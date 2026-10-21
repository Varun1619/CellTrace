"""SCD Type 2 genealogy logic. Pure pandas, no Spark."""

from datetime import datetime, timedelta

import pandas as pd

from streaming.genealogy_rules import GENEALOGY_COLS, apply_assembly_events

T0 = datetime(2026, 1, 5, 8, 0)
NO_ROWS = pd.DataFrame(columns=GENEALOGY_COLS)


def ev(kind, offset, cell=None, module=None, pack=None, prev=None, line="L1"):
    return {"event_id": f"a{offset}", "event_type": kind, "line_id": line, "cell_id": cell, "module_id": module,
            "pack_id": pack, "previous_module_id": prev, "ts": T0 + timedelta(seconds=offset),
            "kafka_partition": 0, "kafka_offset": offset}


def run(events, current=NO_ROWS, module_pack=None):
    return apply_assembly_events(current, module_pack or {}, pd.DataFrame(events))


def current_of(rows):
    return {r.cell_id: (r.module_id, r.pack_id) for r in rows.itertuples() if r.is_current}


def test_placement_opens_a_current_row_without_pack():
    out = run([ev("CELL_TO_MODULE", 1, cell="C1", module="M1")])
    row = out.rows.iloc[0]
    assert (row.cell_id, row.module_id, row.is_current, row.change_reason) == ("C1", "M1", True, "PLACED")
    assert pd.isna(row.pack_id) and pd.isna(row.valid_to)
    assert row.valid_from == T0 + timedelta(seconds=1)


def test_pack_assignment_fills_pack_in_place_without_new_version():
    out = run([
        ev("CELL_TO_MODULE", 1, cell="C1", module="M1"),
        ev("CELL_TO_MODULE", 2, cell="C2", module="M1"),
        ev("MODULE_TO_PACK", 3, module="M1", pack="P1"),
    ])
    assert len(out.rows) == 2
    assert current_of(out.rows) == {"C1": ("M1", "P1"), "C2": ("M1", "P1")}
    assert out.module_packs.to_dict("records")[0]["pack_id"] == "P1"


def test_rework_closes_old_version_and_opens_new_one():
    out = run([
        ev("CELL_TO_MODULE", 1, cell="C1", module="M1"),
        ev("MODULE_TO_PACK", 2, module="M1", pack="P1"),
        ev("CELL_REWORK", 5, cell="C1", module="M2", prev="M1"),
    ])
    versions = out.rows.sort_values("valid_from")
    assert list(versions.module_id) == ["M1", "M2"]
    old, new = versions.iloc[0], versions.iloc[1]
    assert not old.is_current and old.valid_to == T0 + timedelta(seconds=5) and old.pack_id == "P1"
    assert new.is_current and pd.isna(new.valid_to) and new.change_reason == "REWORK" and pd.isna(new.pack_id)


def test_rework_into_a_module_packed_earlier_takes_its_pack():
    out = run([ev("CELL_REWORK", 5, cell="C1", module="M2", prev="M1")],
              current=pd.DataFrame([{"cell_id": "C1", "module_id": "M1", "pack_id": "P1", "line_id": "L1",
                                     "valid_from": T0, "valid_to": None, "is_current": True,
                                     "change_reason": "PLACED", "source_event_id": "a0"}]),
              module_pack={"M2": "P9"})
    assert current_of(out.rows) == {"C1": ("M2", "P9")}
    assert len(out.rows) == 2  # the closed M1 version is returned so the merge can update it


def test_events_apply_in_kafka_order_not_input_order():
    events = [
        ev("CELL_REWORK", 5, cell="C1", module="M2", prev="M1"),
        ev("CELL_TO_MODULE", 1, cell="C1", module="M1"),
    ]
    assert current_of(run(events).rows) == {"C1": ("M2", None)}


def test_replayed_placement_is_idempotent():
    first = run([ev("CELL_TO_MODULE", 1, cell="C1", module="M1")])
    again = run([ev("CELL_TO_MODULE", 1, cell="C1", module="M1")], current=first.rows)
    assert len(again.rows) == 0


def test_existing_open_rows_get_pack_from_a_later_batch():
    first = run([ev("CELL_TO_MODULE", 1, cell="C1", module="M1")])
    second = run([ev("MODULE_TO_PACK", 9, module="M1", pack="P1")], current=first.rows)
    assert current_of(second.rows) == {"C1": ("M1", "P1")}
