from config.specs import DROPOUT_CODE, failure_codes, is_plausible, load_specs

GOOD = {"voltage_v": 3.65, "internal_resistance_mohm": 1.2, "capacity_ah": 5.0, "temp_c": 25.0}


def test_load_specs_from_seed_csv():
    specs = {s.metric: s for s in load_specs()}
    assert specs["internal_resistance_mohm"].upper == 1.32
    assert specs["internal_resistance_mohm"].lower is None
    assert specs["capacity_ah"].lower == 4.88


def test_good_cell_has_no_failure_codes():
    assert failure_codes(GOOD, load_specs()) == []


def test_each_violation_reports_its_code():
    specs = load_specs()
    assert failure_codes({**GOOD, "internal_resistance_mohm": 1.4}, specs) == ["IR_HIGH"]
    assert failure_codes({**GOOD, "capacity_ah": 4.8, "temp_c": 30.0}, specs) == ["CAPACITY_LOW", "TEMP_HIGH"]


def test_missing_reading_is_a_dropout():
    assert failure_codes({**GOOD, "voltage_v": None}, load_specs()) == [DROPOUT_CODE]


def test_plausible_ranges_reject_stuck_sensor():
    assert is_plausible("voltage_v", 3.65)
    assert not is_plausible("voltage_v", 0.0)
    assert not is_plausible("internal_resistance_mohm", None)
