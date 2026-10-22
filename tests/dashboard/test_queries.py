"""Dashboard queries over small Delta tables and dbt-style Parquet marts."""

from datetime import datetime, timedelta

import pandas as pd
import pytest

from tests.conftest import requires_duckdb_delta

deltalake = pytest.importorskip("deltalake")
pytestmark = requires_duckdb_delta

T0 = datetime(2026, 1, 5, 8, 0)


def test_control_chart_fills_limits_for_baseline_windows(lake):
    from dashboard import queries

    rows = [{"station_id": "L1-S1", "metric": "voltage_v", "window_start": T0 + timedelta(minutes=i),
             "window_end": T0 + timedelta(minutes=i + 1), "n": 100, "mean": 3.65, "std": 0.01,
             "center": None if i < 2 else 3.65, "sigma_xbar": None if i < 2 else 0.001,
             "z": None if i < 2 else 0.0, "scored_at": T0} for i in range(4)]
    deltalake.write_deltalake(lake.table_path("gold", "spc_window_stats"), pd.DataFrame(rows))
    deltalake.write_deltalake(lake.table_path("gold", "spc_control_limits"), pd.DataFrame([{
        "station_id": "L1-S1", "metric": "voltage_v", "center": 3.65, "sigma": 0.01, "baseline_windows": 2,
        "baseline_n": 200, "computed_at": T0}]))
    windows, alerts = queries.control_chart(lake, "L1-S1", "voltage_v", 90)
    assert len(windows) == 4 and windows.center.notna().all()
    assert windows.sigma_xbar.iloc[0] == pytest.approx(0.001)
    assert list(windows.in_baseline) == [True, True, False, False]
    assert alerts.empty
    assert queries.stations(lake) == ["L1-S1"]


def test_dbt_marts_are_none_until_dbt_runs_then_aggregate(lake):
    from dashboard import queries

    assert queries.defect_pareto(lake) is None
    path = queries.dbt_model(lake, "fct_defect_pareto")
    path.parent.mkdir(parents=True)
    pd.DataFrame({"failure_code": ["IR_HIGH", "CAPACITY_LOW", "IR_HIGH"], "failures": [5, 30, 5]}).to_parquet(path)
    pareto = queries.defect_pareto(lake)
    assert list(pareto.failure_code) == ["CAPACITY_LOW", "IR_HIGH"]
    assert pareto.cumulative_share.iloc[-1] == pytest.approx(1.0)


def _spc_tables(lake, z_by_station):
    rows = []
    for station, zs in z_by_station.items():
        for i, z in enumerate(zs):
            rows.append({"station_id": station, "metric": "voltage_v", "window_start": T0 + timedelta(minutes=i),
                         "window_end": T0 + timedelta(minutes=i + 1), "n": 100, "mean": 3.65, "std": 0.01,
                         "center": 3.65, "sigma_xbar": 0.001, "z": z, "scored_at": T0})
    deltalake.write_deltalake(lake.table_path("gold", "spc_window_stats"), pd.DataFrame(rows))


def test_station_status_uses_latest_z_and_alerts_in_recent_windows(lake):
    from dashboard import queries

    _spc_tables(lake, {"L1-S1": [0.1, 0.2, 0.3, -0.4], "L2-S3": [0.5, 3.5, 4.1, 5.0]})
    deltalake.write_deltalake(lake.table_path("gold", "spc_alerts"), pd.DataFrame([
        {"alert_id": f"a{i}", "station_id": "L2-S3", "metric": "voltage_v", "rule_triggered": "WE1",
         "rule_description": "", "window_start": T0 + timedelta(minutes=i), "window_end": T0 + timedelta(minutes=i + 1),
         "value": 3.7, "limit": 3.66, "z": 4.0, "detected_at": T0} for i in (1, 2, 3)]))
    status = queries.station_status(lake).set_index("station_id")
    assert status.loc["L1-S1"].z == pytest.approx(-0.4) and status.loc["L1-S1"].recent_alerts == 0
    assert status.loc["L2-S3"].z == pytest.approx(5.0) and status.loc["L2-S3"].recent_alerts == 2  # windows 2-3


def test_overview_counts_cells_yield_and_alerting_series(lake):
    from dashboard import queries

    tests = [{"event_id": f"e{i}", "lot_id": "LOT-1", "station_id": "L1-S1", "line_id": "L1",
              "ts": T0 + timedelta(seconds=i), "result": "PASS" if i % 4 else "FAIL"} for i in range(40)]
    deltalake.write_deltalake(lake.table_path("silver", "cell_tests"), pd.DataFrame(tests))
    deltalake.write_deltalake(lake.table_path("silver", "cell_tests_quarantine"),
                              pd.DataFrame([{"event_id": "q1", "quarantine_reason": "null_voltage_v"}]))
    o = queries.overview(lake)
    assert o["cells_tested"] == 40 and o["quarantined"] == 1
    assert o["first_pass_yield"] == pytest.approx(0.75)
    assert o["last_event"] == T0 + timedelta(seconds=39)


def test_pack_summary_groups_recalled_cells_by_pack():
    from dashboard.queries import pack_summary

    cells = pd.DataFrame({
        "cell_id": ["C1", "C2", "C3", "C4"], "result": ["PASS", "PASS", "PASS", "FAIL"],
        "module_id": ["M1", "M1", "M2", None], "pack_id": ["P1", "P1", "P2", None],
    })
    s = pack_summary(cells)
    assert list(s.pack_id) == ["P1", "P2"]
    assert list(s.cells_from_lot) == [2, 1] and list(s.modules) == [1, 1]
