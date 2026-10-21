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
