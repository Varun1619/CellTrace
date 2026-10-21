"""Behavior of the simulated test stations, fault injection and assembly flow."""

from collections import Counter
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import numpy as np
import pytest

from config.settings import load_settings
from generator.simulator import NOMINAL, Simulator, drift_delta, iso, run_simulation, shift_for

START = datetime(2026, 1, 5, 8, 0, tzinfo=timezone.utc)
METRICS = ("voltage_v", "internal_resistance_mohm", "capacity_ah", "temp_c")


def cfg(**overrides):
    base = load_settings(env={}).generator
    clean = replace(
        base,
        sensor_dropout_rate=0.0,
        duplicate_rate=0.0,
        late_rate=0.0,
        rework_rate=0.0,
        drift_start_after_s=1e9,
        bad_lot_index=10_000,
    )
    return replace(clean, **overrides)


def cell_tests_of(messages):
    return [m.value for m in messages if m.topic == "cell_tests"]


def assembly_of(messages):
    return [m.value for m in messages if m.topic == "assembly_events"]


def test_event_schema_and_station_layout():
    events = cell_tests_of(run_simulation(cfg(), n_cells=120, start=START))
    expected = {
        "event_id", "cell_id", "lot_id", "line_id", "station_id", "shift", "ts",
        "voltage_v", "internal_resistance_mohm", "capacity_ah", "temp_c", "result",
    }
    assert set(events[0]) == expected
    assert {e["station_id"] for e in events} == {f"L{l}-S{s}" for l in (1, 2, 3) for s in (1, 2, 3, 4)}
    assert all(e["station_id"].startswith(e["line_id"] + "-") for e in events)
    assert {e["result"] for e in events} <= {"PASS", "FAIL"}


def test_distributions_match_nominal():
    events = cell_tests_of(run_simulation(cfg(), n_cells=12_000, start=START))
    for metric in METRICS:
        values = np.array([e[metric] for e in events])
        mean, sd = NOMINAL[metric]
        assert values.mean() == pytest.approx(mean, abs=sd * 0.1), metric
        assert values.std() == pytest.approx(sd, rel=0.15), metric


def test_first_pass_yield_is_realistic_without_faults():
    events = cell_tests_of(run_simulation(cfg(), n_cells=12_000, start=START))
    fpy = sum(e["result"] == "PASS" for e in events) / len(events)
    assert 0.94 < fpy < 0.99


def test_drift_ramp_shape():
    c = cfg(drift_start_after_s=100, drift_ramp_s=200, drift_ir_max_delta_mohm=0.3)
    assert drift_delta(99, c) == 0.0
    assert drift_delta(200, c) == pytest.approx(0.15)
    assert drift_delta(10_000, c) == pytest.approx(0.3)


def test_station_drift_raises_ir_only_on_target_station():
    c = cfg(rate=100, drift_start_after_s=30, drift_ramp_s=30, drift_ir_max_delta_mohm=0.3)
    messages = run_simulation(c, n_cells=12_000, start=START)
    late = [e for e in cell_tests_of(messages) if e["ts"] >= iso(START + timedelta(seconds=60))]
    drifted = np.mean([e["internal_resistance_mohm"] for e in late if e["station_id"] == c.drift_station])
    others = np.mean([e["internal_resistance_mohm"] for e in late if e["station_id"] != c.drift_station])
    assert drifted - others == pytest.approx(0.3, abs=0.03)


def test_drift_start_is_written_to_fault_log_once():
    c = cfg(rate=100, drift_start_after_s=30)
    faults = [m.value for m in run_simulation(c, n_cells=6_000, start=START) if m.topic == "fault_log"]
    drift = [f for f in faults if f["fault_type"] == "STATION_DRIFT"]
    assert len(drift) == 1
    assert drift[0]["target"] == c.drift_station
    assert drift[0]["metric"] == "internal_resistance_mohm"
    assert drift[0]["started_at"] == iso(START + timedelta(seconds=30))


def test_bad_lot_has_lower_capacity_and_is_logged():
    c = cfg(cells_per_lot=1000, bad_lot_index=3, bad_lot_capacity_delta_ah=-0.12)
    messages = run_simulation(c, n_cells=5_000, start=START)
    events = cell_tests_of(messages)
    bad = np.mean([e["capacity_ah"] for e in events if e["lot_id"].endswith("-0003")])
    good = np.mean([e["capacity_ah"] for e in events if not e["lot_id"].endswith("-0003")])
    assert good - bad == pytest.approx(0.12, abs=0.01)
    faults = [m.value for m in messages if m.topic == "fault_log" and m.value["fault_type"] == "BAD_LOT"]
    assert [f["target"] for f in faults] == ["LOT-01050800-0003"]


def test_sensor_dropout_rate_and_shape():
    events = cell_tests_of(run_simulation(cfg(sensor_dropout_rate=0.05), n_cells=10_000, start=START))
    broken = [e for e in events if any(e[m] is None or e[m] == 0.0 for m in METRICS)]
    assert len(broken) / len(events) == pytest.approx(0.05, abs=0.01)
    assert any(any(e[m] is None for m in METRICS) for e in broken)
    assert any(any(e[m] == 0.0 for m in METRICS) for e in broken)
    assert all(e["result"] == "FAIL" for e in broken)


def test_duplicates_repeat_event_id_with_identical_payload():
    events = cell_tests_of(run_simulation(cfg(duplicate_rate=0.05), n_cells=5_000, start=START))
    counts = Counter(e["event_id"] for e in events)
    dup_ids = [k for k, v in counts.items() if v > 1]
    assert len(dup_ids) / len(counts) == pytest.approx(0.05, abs=0.012)
    by_id = {}
    for e in events:
        assert by_id.setdefault(e["event_id"], e) == e


def test_late_events_arrive_after_newer_events():
    c = cfg(late_rate=0.05, late_max_delay_s=120)
    messages = run_simulation(c, n_cells=5_000, start=START)
    ts = [m.value["ts"] for m in messages if m.topic == "cell_tests"]
    out_of_order = sum(1 for a, b in zip(ts, ts[1:]) if b < a)
    assert out_of_order / len(ts) == pytest.approx(0.05, abs=0.015)
    assert len(ts) == 5_000


def test_assembly_builds_modules_and_packs_from_passing_cells():
    c = cfg(cells_per_module=10, modules_per_pack=2, lines=1, stations_per_line=2)
    messages = run_simulation(c, n_cells=1_000, start=START)
    passed = {e["cell_id"] for e in cell_tests_of(messages) if e["result"] == "PASS"}
    events = assembly_of(messages)
    placed = [e for e in events if e["event_type"] == "CELL_TO_MODULE"]
    assert {e["cell_id"] for e in placed} == passed
    per_module = Counter(e["module_id"] for e in placed)
    full_modules = [m for m, n in per_module.items() if n == 10]
    assert len(full_modules) >= len(per_module) - 1
    packed = [e for e in events if e["event_type"] == "MODULE_TO_PACK"]
    per_pack = Counter(e["pack_id"] for e in packed)
    assert per_pack and all(n == 2 for n in per_pack.values())
    assert {e["module_id"] for e in packed} <= set(full_modules)


def test_rework_moves_a_cell_from_a_previous_module():
    c = cfg(rework_rate=0.05, cells_per_module=10, lines=1)
    events = assembly_of(run_simulation(c, n_cells=2_000, start=START))
    reworks = [e for e in events if e["event_type"] == "CELL_REWORK"]
    assert reworks
    home = {}
    for e in events:
        if e["event_type"] == "CELL_TO_MODULE":
            home[e["cell_id"]] = e["module_id"]
        elif e["event_type"] == "CELL_REWORK":
            assert home[e["cell_id"]] == e["previous_module_id"]
            assert e["module_id"] != e["previous_module_id"]
            home[e["cell_id"]] = e["module_id"]


def test_same_seed_same_output():
    a = run_simulation(cfg(duplicate_rate=0.02, late_rate=0.02), n_cells=500, start=START)
    b = run_simulation(cfg(duplicate_rate=0.02, late_rate=0.02), n_cells=500, start=START)
    assert a == b


def test_shift_boundaries():
    assert shift_for(datetime(2026, 1, 5, 6, 0, tzinfo=timezone.utc)) == "A"
    assert shift_for(datetime(2026, 1, 5, 14, 0, tzinfo=timezone.utc)) == "B"
    assert shift_for(datetime(2026, 1, 5, 23, 59, tzinfo=timezone.utc)) == "C"
    assert shift_for(datetime(2026, 1, 5, 5, 59, tzinfo=timezone.utc)) == "C"


def test_simulator_rejects_bad_config():
    with pytest.raises(ValueError):
        Simulator(cfg(cells_per_module=0), start=START)
