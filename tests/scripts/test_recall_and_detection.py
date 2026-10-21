"""Recall lookup and time-to-detect over small Delta tables written with delta-rs."""

from datetime import datetime, timedelta

import pandas as pd
import pytest

deltalake = pytest.importorskip("deltalake")

from tests.conftest import requires_duckdb_delta  # noqa: E402

pytestmark = requires_duckdb_delta

T0 = datetime(2026, 1, 5, 8, 0)


def write(lake, layer, name, rows):
    deltalake.write_deltalake(lake.table_path(layer, name), pd.DataFrame(rows))


def cell_test(cell, lot, result="PASS", minute=0):
    return {"event_id": f"e-{cell}", "cell_id": cell, "lot_id": lot, "line_id": "L1", "station_id": "L1-S1",
            "shift": "A", "ts": T0 + timedelta(minutes=minute), "voltage_v": 3.65,
            "internal_resistance_mohm": 1.2, "capacity_ah": 4.86 if lot == "LOT-BAD" else 5.0, "temp_c": 25.0,
            "result": result}


def version(cell, module, pack, start, end=None):
    return {"cell_id": cell, "module_id": module, "pack_id": pack, "line_id": "L1",
            "valid_from": T0 + timedelta(seconds=start),
            "valid_to": None if end is None else T0 + timedelta(seconds=end),
            "is_current": end is None, "change_reason": "PLACED", "source_event_id": f"a-{cell}-{start}"}


@pytest.fixture
def recall_lake(lake):
    write(lake, "silver", "cell_tests", [
        cell_test("C1", "LOT-BAD"), cell_test("C2", "LOT-BAD"), cell_test("C3", "LOT-BAD", result="FAIL"),
        cell_test("C4", "LOT-BAD"), cell_test("C9", "LOT-OK"),
    ])
    write(lake, "silver", "cell_genealogy", [
        version("C1", "M1", "P1", 1, end=50),  # reworked out of M1 ...
        version("C1", "M2", "P2", 50),         # ... into M2
        version("C2", "M1", "P1", 2),
        version("C9", "M1", "P1", 3),
        # C3 failed and was never placed; C4 passed but is not in a module yet
    ])
    return lake


def test_recall_returns_current_packs_modules_and_cells(recall_lake):
    from scripts.recall_lookup import recall

    r = recall("LOT-BAD", recall_lake)
    assert r.packs == ["P1", "P2"]
    assert r.modules == ["M1", "M2"]
    assert sorted(r.cells.cell_id) == ["C1", "C2", "C3", "C4"]
    c1 = r.cells.set_index("cell_id").loc["C1"]
    assert (c1.module_id, c1.pack_id, c1.module_history, c1.versions) == ("M2", "P2", "M1 > M2", 2)
    assert r.counts == {"cells": 4, "failed_at_test": 1, "not_yet_assembled": 1, "in_packs": 2}
    assert r.elapsed_s > 0


def test_unknown_lot_returns_nothing(recall_lake):
    from scripts.recall_lookup import recall

    r = recall("LOT-NOPE", recall_lake)
    assert r.cells.empty and r.packs == []


def alert(station, metric, rule, minute, detected_minute):
    ws = T0 + timedelta(minutes=minute)
    return {"alert_id": f"{station}{metric}{minute}{rule}", "station_id": station, "metric": metric,
            "rule_triggered": rule, "rule_description": "", "window_start": ws, "window_end": ws + timedelta(minutes=1),
            "value": 1.3, "limit": 1.25, "z": 4.0, "detected_at": T0 + timedelta(minutes=detected_minute)}


def test_time_to_detect_uses_first_matching_alert_after_fault_start(lake):
    from scripts.detection import alert_attribution, time_to_detect

    write(lake, "silver", "fault_log", [
        {"fault_id": "f1", "run_id": "r", "fault_type": "STATION_DRIFT", "target": "L2-S3",
         "metric": "internal_resistance_mohm", "started_at": T0 + timedelta(minutes=10), "magnitude": 0.25,
         "details": "", "ingest_ts": T0},
        {"fault_id": "f2", "run_id": "r", "fault_type": "BAD_LOT", "target": "LOT-5", "metric": "capacity_ah",
         "started_at": T0 + timedelta(minutes=6, seconds=40), "magnitude": -0.12, "details": "", "ingest_ts": T0},
    ])
    write(lake, "gold", "spc_alerts", [
        alert("L2-S3", "internal_resistance_mohm", "WE1", 8, 9),     # before the drift: a false alarm
        alert("L1-S1", "internal_resistance_mohm", "WE1", 11, 12),   # wrong station: a false alarm
        alert("L2-S3", "internal_resistance_mohm", "WE2", 11, 13),
        alert("L2-S3", "internal_resistance_mohm", "WE1", 12, 13.5),
        alert("L3-S1", "capacity_ah", "WE1", 6, 8),                   # bad lot on any station counts
        alert("L1-S2", "capacity_ah", "WE1", 40, 41),                 # long after the lot: a false alarm
    ])
    ttd = time_to_detect(lake).set_index("fault_type")
    drift = ttd.loc["STATION_DRIFT"]
    assert (drift.first_rule, drift.first_alert_station) == ("WE2", "L2-S3")
    assert drift.time_to_detect_s == pytest.approx(180)
    assert drift.event_time_to_detect_s == pytest.approx(120)
    lot = ttd.loc["BAD_LOT"]
    assert lot.first_alert_station == "L3-S1"
    assert lot.time_to_detect_s == pytest.approx(80)

    attributed = alert_attribution(lake, lot_duration_s=100)
    assert attributed.attributed.tolist().count(False) == 3


def test_lookback_rules_after_a_bad_lot_are_attributed_to_it(lake):
    from scripts.detection import alert_attribution

    write(lake, "silver", "fault_log", [
        {"fault_id": "f2", "run_id": "r", "fault_type": "BAD_LOT", "target": "LOT-5", "metric": "capacity_ah",
         "started_at": T0 + timedelta(minutes=10), "magnitude": -0.12, "details": "", "ingest_ts": T0}])
    write(lake, "gold", "spc_alerts", [
        alert("L1-S1", "capacity_ah", "WE4", 16, 17),  # its 8-window lookback (9..16) covers the lot at 10-11
        alert("L1-S1", "capacity_ah", "WE1", 16, 17),  # a single point 5 minutes after the lot: unexplained
        alert("L1-S1", "capacity_ah", "WE4", 19, 20),  # lookback 12..19 starts after the lot ended
    ])
    a = alert_attribution(lake, lot_duration_s=100).sort_values(["window_start", "rule_triggered"])
    assert list(zip(a.rule_triggered, a.attributed)) == [("WE1", False), ("WE4", True), ("WE4", False)]
