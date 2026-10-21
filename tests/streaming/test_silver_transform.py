import pytest

from tests.streaming.helpers import bronze_df, cell

pytestmark = pytest.mark.spark


def reasons_by_event(spark, payloads):
    from streaming.silver_transform import split_cell_tests

    clean, quarantine = split_cell_tests(bronze_df(spark, payloads))
    return {r.event_id: r.quarantine_reason for r in quarantine.collect()}, clean


def test_good_row_is_clean_and_typed(spark):
    reasons, clean = reasons_by_event(spark, [cell()])
    assert reasons == {}
    row = clean.collect()[0]
    assert row.ts.year == 2026 and str(row.test_date) == "2026-01-05"
    assert row.kafka_offset == 0


def test_quarantine_reasons(spark):
    payloads = [
        cell(event_id="null-v", voltage_v=None),
        cell(event_id="zero-ir", internal_resistance_mohm=0.0),
        cell(event_id="no-cell", cell_id=None),
        cell(event_id="bad-ts", ts="yesterday"),
        cell(event_id="bad-result", result="MAYBE"),
        cell(event_id="two-problems", capacity_ah=None, temp_c=99.0),
    ]
    reasons, clean = reasons_by_event(spark, payloads)
    assert clean.count() == 0
    assert reasons == {
        "null-v": "null_voltage_v",
        "zero-ir": "out_of_range_internal_resistance_mohm",
        "no-cell": "null_cell_id",
        "bad-ts": "bad_ts",
        "bad-result": "bad_result",
        "two-problems": "null_capacity_ah;out_of_range_temp_c",
    }


def test_unparseable_payload_is_quarantined_with_raw_text(spark):
    from streaming.silver_transform import split_cell_tests

    _, quarantine = split_cell_tests(bronze_df(spark, ["{not json"]))
    row = quarantine.collect()[0]
    assert row.quarantine_reason == "unparseable_json"
    assert row.payload == "{not json"


def test_dedupe_keeps_one_row_per_event_id(spark):
    from streaming.silver_transform import dedupe, split_cell_tests

    clean, _ = split_cell_tests(bronze_df(spark, [cell(), cell(), cell(event_id="e-2")]))
    assert sorted(r.event_id for r in dedupe(clean, "10 minutes").collect()) == ["e-1", "e-2"]


def test_quality_checks_flag_violations(spark):
    from streaming.quality import cell_test_checks, failed_checks
    from streaming.silver_transform import split_cell_tests

    clean, _ = split_cell_tests(bronze_df(spark, [cell(), cell(event_id="e-2")]))
    assert failed_checks(clean, cell_test_checks()) == []
    doubled = clean.unionByName(clean)
    assert failed_checks(doubled, cell_test_checks()) == ["unique_event_id"]


def test_assembly_validation(spark):
    from streaming.silver_transform import split_assembly

    base = {"event_id": "a1", "event_type": "CELL_TO_MODULE", "line_id": "L1", "cell_id": "C-1",
            "module_id": "M-1", "pack_id": None, "previous_module_id": None, "ts": "2026-01-05T08:00:00.000+00:00"}
    payloads = [
        base,
        {**base, "event_id": "a2", "event_type": "MODULE_TO_PACK", "cell_id": None},
        {**base, "event_id": "a3", "event_type": "TELEPORT"},
        {**base, "event_id": "a4", "event_type": "CELL_REWORK"},
    ]
    clean, quarantine = split_assembly(bronze_df(spark, payloads, topic="assembly_events"))
    assert [r.event_id for r in clean.collect()] == ["a1"]
    assert {r.event_id: r.quarantine_reason for r in quarantine.collect()} == {
        "a2": "null_pack_id",
        "a3": "bad_event_type",
        "a4": "null_previous_module_id",
    }
