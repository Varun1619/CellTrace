"""The batch seeder reuses the pure cores to build a lakehouse without Kafka or Spark."""

import pytest

from config.settings import load_settings

deltalake = pytest.importorskip("deltalake")


@pytest.fixture(scope="module")
def seeded(tmp_path_factory):
    from scripts.seed_lakehouse import seed

    settings = load_settings(env={
        "LAKEHOUSE_ROOT": str(tmp_path_factory.mktemp("seeded")),
        "GENERATOR_RATE": "20", "CELLS_PER_LOT": "1000", "BAD_LOT_INDEX": "5",
        "DRIFT_START_AFTER_S": "420", "DRIFT_RAMP_S": "60", "DRIFT_IR_MAX_DELTA_MOHM": "0.3",
        "SENSOR_DROPOUT_RATE": "0.01", "SPC_BASELINE_WINDOWS": "5", "DUPLICATE_RATE": "0.02", "REWORK_RATE": "0.01",
    })
    summary = seed(settings, minutes=10)
    return settings, summary


def read(settings, layer, name):
    return deltalake.DeltaTable(settings.table_path(layer, name)).to_pandas()


def test_silver_reconciles_with_generated_events(seeded):
    settings, summary = seeded
    silver, quarantine = read(settings, "silver", "cell_tests"), read(settings, "silver", "cell_tests_quarantine")
    assert silver.event_id.is_unique
    assert len(silver) + quarantine.event_id.nunique() == summary["distinct_cell_tests"]
    assert summary["cell_test_messages"] > summary["distinct_cell_tests"]  # duplicates were generated
    assert quarantine.quarantine_reason.str.match(r"^(null|out_of_range)_").all()


def test_genealogy_has_one_current_version_per_placed_cell(seeded):
    settings, _ = seeded
    g = read(settings, "silver", "cell_genealogy")
    assert g.groupby("cell_id").is_current.sum().eq(1).all()
    assert (g.change_reason == "REWORK").any()
    assert g[g.is_current].pack_id.notna().any()


def test_spc_flags_the_drift_and_faults_are_logged(seeded):
    settings, _ = seeded
    faults = read(settings, "silver", "fault_log")
    assert set(faults.fault_type) == {"STATION_DRIFT", "BAD_LOT"}
    limits = read(settings, "gold", "spc_control_limits")
    assert len(limits) == 12 * 3
    alerts = read(settings, "gold", "spc_alerts")
    drift = alerts[(alerts.station_id == settings.generator.drift_station)
                   & (alerts.metric == "internal_resistance_mohm")]
    assert not drift.empty
    assert alerts.alert_id.is_unique
