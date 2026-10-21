"""Spark side of SPC: window aggregation and the full stream over a temp lakehouse."""

from datetime import date, datetime, timedelta

import numpy as np
import pytest

pytestmark = pytest.mark.spark

T0 = datetime(2026, 1, 5, 8, 0)


def silver_rows(minutes, per_minute=60, station="L1-S1", ir_step_at=None, ir_step=0.0, seed=1):
    rng = np.random.default_rng(seed)
    rows = []
    for m in range(minutes):
        for i in range(per_minute):
            ts = T0 + timedelta(minutes=m, seconds=i * 60 / per_minute)
            ir = rng.normal(1.2, 0.05) + (ir_step if ir_step_at is not None and m >= ir_step_at else 0.0)
            rows.append((f"e-{station}-{m}-{i}", f"C-{m}-{i}", "LOT-1", station[:2], station, "A", ts,
                         float(rng.normal(3.65, 0.01)), float(ir), float(rng.normal(5.0, 0.05)), 25.0, "PASS",
                         date(2026, 1, 5), 0, m * per_minute + i, ts))
    return rows


def test_window_aggregates_to_long_rows(spark):
    from streaming.spc_monitor import to_long, window_aggregates
    from streaming.tables import SILVER_CELL_TESTS

    df = spark.createDataFrame(silver_rows(2, per_minute=30), SILVER_CELL_TESTS)
    long = to_long(window_aggregates(df, "1 minute").toPandas())
    assert len(long) == 2 * 3  # 2 windows x 3 SPC metrics
    assert set(long.metric) == {"internal_resistance_mohm", "voltage_v", "capacity_ah"}
    assert (long.n == 30).all()
    ir = long[long.metric == "internal_resistance_mohm"]
    assert ir["mean"].between(1.15, 1.25).all() and ir["std"].between(0.02, 0.08).all()


def test_spc_stream_builds_limits_scores_windows_and_alerts_on_a_step(spark, lake):
    from streaming.spc_monitor import start_spc
    from streaming.tables import GOLD_TABLES, SILVER_CELL_TESTS, SILVER_TABLES, ensure_tables

    ensure_tables(spark, lake, [*SILVER_TABLES.values(), *GOLD_TABLES.values()])
    # 14 minutes of data: the step at minute 8 only closes once a later batch moves the watermark past 9:00.
    rows = silver_rows(14, ir_step_at=8, ir_step=0.1)
    path = lake.table_path("silver", "cell_tests")
    for chunk in range(0, 14, 2):  # one Delta commit per two minutes so the watermark advances between batches
        part = [r for r in rows if chunk * 60 <= r[14] < (chunk + 2) * 60]
        spark.createDataFrame(part, SILVER_CELL_TESTS).coalesce(1).write.format("delta").mode("append").save(path)

    for q in start_spc(spark, lake, trigger={"availableNow": True}, source_options={"maxFilesPerTrigger": 1}):
        q.awaitTermination(180)

    read = lambda name: spark.read.format("delta").load(lake.table_path("gold", name)).toPandas()
    limits = read("spc_control_limits")
    assert len(limits) == 3 and (limits.baseline_windows == 5).all()
    ir_limit = limits[limits.metric == "internal_resistance_mohm"].iloc[0]
    assert ir_limit.center == pytest.approx(1.2, abs=0.01)

    windows = read("spc_window_stats")
    assert windows.z.isna().sum() == 3 * 5  # baseline windows stay unscored
    alerts = read("spc_alerts")
    ir_alerts = alerts[alerts.metric == "internal_resistance_mohm"]
    assert not ir_alerts.empty
    assert ir_alerts.window_start.min() == T0 + timedelta(minutes=8)
    assert "WE1" in set(ir_alerts.rule_triggered)
    assert alerts.alert_id.is_unique
