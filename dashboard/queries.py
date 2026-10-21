"""Every query the dashboard runs. DuckDB reads the Delta tables and the dbt Parquet marts directly."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from config.settings import Settings
from scripts import detection, recall_lookup
from scripts.lakehouse import connect, query, table_exists

SPC_METRIC_LABELS = {
    "internal_resistance_mohm": "Internal resistance (mOhm)",
    "voltage_v": "Voltage (V)",
    "capacity_ah": "Capacity (Ah)",
}


def dbt_model(settings: Settings, model: str) -> Path:
    return settings.lakehouse_root / "gold" / "dbt" / f"{model}.parquet"


def _parquet(settings: Settings, model: str, sql: str) -> pd.DataFrame | None:
    """Run sql against a dbt mart exposed as `m`, or None before dbt has run."""
    path = dbt_model(settings, model)
    if not path.exists():
        return None
    con = connect()
    try:
        return con.execute(sql.replace("{m}", f"read_parquet('{path.as_posix()}')")).df()
    finally:
        con.close()


def stations(settings: Settings) -> list[str]:
    if not table_exists(settings, "gold", "spc_window_stats"):
        return []
    df = query("SELECT DISTINCT station_id FROM {gold.spc_window_stats} ORDER BY 1", settings)
    return list(df.station_id)


CONTROL_CHART_SQL = """
WITH latest AS (SELECT max(window_start) AS t FROM {gold.spc_window_stats})
SELECT w.window_start, w.window_end, w.n, w.mean, w.z,
       l.center, l.sigma / sqrt(w.n) AS sigma_xbar, w.z IS NULL AS in_baseline
FROM {gold.spc_window_stats} w
LEFT JOIN {gold.spc_control_limits} l ON l.station_id = w.station_id AND l.metric = w.metric
WHERE w.station_id = ? AND w.metric = ?
  AND w.window_start >= (SELECT t FROM latest) - to_minutes(CAST(? AS BIGINT))
ORDER BY w.window_start
"""

SERIES_ALERTS_SQL = """
SELECT window_start, value, z, rule_triggered, rule_description, detected_at
FROM {gold.spc_alerts}
WHERE station_id = ? AND metric = ? AND window_start >= ?
ORDER BY window_start
"""


def control_chart(settings: Settings, station: str, metric: str, last_minutes: int = 90):
    windows = query(CONTROL_CHART_SQL, settings, [station, metric, last_minutes])
    if windows.empty or not table_exists(settings, "gold", "spc_alerts"):
        return windows, pd.DataFrame(columns=["window_start", "value", "z", "rule_triggered"])
    alerts = query(SERIES_ALERTS_SQL, settings, [station, metric, windows.window_start.min()])
    return windows, alerts


RECENT_ALERTS_SQL = """
WITH latest AS (SELECT max(window_start) AS t FROM {gold.spc_window_stats})
SELECT station_id, metric, count(*) AS alerts, max(window_start) AS last_alert_window
FROM {gold.spc_alerts}
WHERE window_start >= (SELECT t FROM latest) - to_minutes(CAST(? AS BIGINT))
GROUP BY station_id, metric
ORDER BY alerts DESC
"""


def recent_alerts(settings: Settings, last_minutes: int = 15) -> pd.DataFrame:
    if not table_exists(settings, "gold", "spc_alerts"):
        return pd.DataFrame()
    return query(RECENT_ALERTS_SQL, settings, [last_minutes])


FPY_TREND_SQL = """
SELECT time_bucket(to_minutes(CAST(? AS BIGINT)), ts) AS bucket, line_id,
       count(*) AS cells_tested, avg(CASE WHEN result = 'PASS' THEN 1.0 ELSE 0.0 END) AS first_pass_yield
FROM {silver.cell_tests}
GROUP BY 1, 2
ORDER BY 1, 2
"""


def fpy_trend(settings: Settings, bucket_minutes: int = 5) -> pd.DataFrame:
    if not table_exists(settings, "silver", "cell_tests"):
        return pd.DataFrame()
    return query(FPY_TREND_SQL, settings, [bucket_minutes])


def fpy_by_station(settings: Settings) -> pd.DataFrame | None:
    return _parquet(settings, "fct_first_pass_yield", """
        SELECT station_id, line_id, sum(cells_tested) AS cells_tested,
               sum(cells_passed) / sum(cells_tested) AS first_pass_yield
        FROM {m} GROUP BY station_id, line_id ORDER BY first_pass_yield""")


def defect_pareto(settings: Settings) -> pd.DataFrame | None:
    return _parquet(settings, "fct_defect_pareto", """
        WITH c AS (SELECT failure_code, sum(failures) AS failures FROM {m} GROUP BY 1)
        SELECT failure_code, failures, failures / sum(failures) OVER () AS share,
               sum(failures) OVER (ORDER BY failures DESC, failure_code) / sum(failures) OVER () AS cumulative_share
        FROM c ORDER BY failures DESC, failure_code""")


def lot_quality(settings: Settings) -> pd.DataFrame | None:
    return _parquet(settings, "fct_lot_quality", """
        SELECT lot_id, n_cells, capacity_mean, ir_mean, fail_rate, capacity_z, is_outlier, outlier_metric
        FROM {m} ORDER BY lot_id""")


def lot_ids(settings: Settings) -> list[str]:
    if not table_exists(settings, "silver", "cell_tests"):
        return []
    return list(query("SELECT DISTINCT lot_id FROM {silver.cell_tests} ORDER BY 1", settings).lot_id)


def time_to_detect(settings: Settings) -> pd.DataFrame:
    if not (table_exists(settings, "silver", "fault_log") and table_exists(settings, "gold", "spc_alerts")):
        return pd.DataFrame()
    return detection.time_to_detect(settings)


def alerts(settings: Settings) -> pd.DataFrame:
    if not (table_exists(settings, "silver", "fault_log") and table_exists(settings, "gold", "spc_alerts")):
        return pd.DataFrame()
    return detection.alert_attribution(settings).sort_values("detected_at", ascending=False)


def recall(settings: Settings, lot_id: str) -> recall_lookup.RecallResult:
    return recall_lookup.recall(lot_id, settings)
