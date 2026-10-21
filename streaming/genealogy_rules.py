"""SCD Type 2 cell genealogy: cell -> module -> pack, with full history of reworks.

Pure pandas so the versioning rules can be tested without Spark. genealogy.py feeds it
each micro-batch of assembly events plus the current rows they touch, then MERGEs the
returned rows into silver.cell_genealogy on (cell_id, valid_from).

Versioning rules:
- CELL_TO_MODULE opens the first version for a cell.
- CELL_REWORK closes the current version (valid_to = event ts) and opens a new one.
- MODULE_TO_PACK fills pack_id on the open versions of that module's cells. The cell
  did not move; the pack only became known, so this is a Type 1 fill, not a new version.
Events apply in Kafka order (partition, offset). Each line's events share one
partition, which preserves their order.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Mapping

import pandas as pd

GENEALOGY_COLS = ["cell_id", "module_id", "pack_id", "line_id", "valid_from", "valid_to", "is_current",
                  "change_reason", "source_event_id"]
MODULE_PACK_COLS = ["module_id", "pack_id", "line_id", "packed_at", "source_event_id"]


@dataclass(frozen=True)
class GenealogyChanges:
    rows: pd.DataFrame          # every version created or changed in this batch
    module_packs: pd.DataFrame  # module-to-pack assignments seen in this batch


def _missing(value) -> bool:
    return value is None or (isinstance(value, float) and pd.isna(value))


class _State:
    def __init__(self, current: pd.DataFrame, module_pack: Mapping[str, str]):
        self.versions: dict[tuple, dict] = {}
        self.current: dict[str, tuple] = {}
        self.members: dict[str, set] = defaultdict(set)
        self.packs = dict(module_pack)
        self.touched: set[tuple] = set()
        for row in current.to_dict("records"):
            key = (row["cell_id"], row["valid_from"])
            self.versions[key] = {**row, "pack_id": None if _missing(row["pack_id"]) else row["pack_id"],
                                  "valid_to": None if _missing(row["valid_to"]) else row["valid_to"]}
            if row["is_current"]:
                self.current[row["cell_id"]] = key
                self.members[row["module_id"]].add(row["cell_id"])

    def open(self, cell: str, module: str, line: str, ts: datetime, reason: str, event_id: str) -> None:
        key = (cell, ts)
        self.versions[key] = {
            "cell_id": cell, "module_id": module, "pack_id": self.packs.get(module), "line_id": line,
            "valid_from": ts, "valid_to": None, "is_current": True, "change_reason": reason,
            "source_event_id": event_id,
        }
        self.current[cell] = key
        self.members[module].add(cell)
        self.touched.add(key)

    def close(self, cell: str, ts: datetime) -> None:
        key = self.current.pop(cell)
        row = self.versions[key]
        self.versions[key] = {**row, "valid_to": ts, "is_current": False}
        self.members[row["module_id"]].discard(cell)
        self.touched.add(key)

    def move(self, e, reason: str) -> None:
        key = self.current.get(e.cell_id)
        if key is not None and self.versions[key]["module_id"] == e.module_id:
            return  # replay of an event already applied
        if key is not None:
            self.close(e.cell_id, e.ts)
        self.open(e.cell_id, e.module_id, e.line_id, e.ts, reason, e.event_id)

    def pack(self, e) -> None:
        self.packs[e.module_id] = e.pack_id
        for cell in self.members[e.module_id]:
            key = self.current[cell]
            if self.versions[key]["pack_id"] is None:
                self.versions[key] = {**self.versions[key], "pack_id": e.pack_id}
                self.touched.add(key)


def apply_assembly_events(current: pd.DataFrame, module_pack: Mapping[str, str],
                          events: pd.DataFrame) -> GenealogyChanges:
    """Apply a batch of assembly events to the current genealogy rows of the cells and modules involved."""
    state = _State(current, module_pack)
    packed = []
    for e in events.sort_values(["kafka_partition", "kafka_offset"]).itertuples(index=False):
        if e.event_type == "MODULE_TO_PACK":
            state.pack(e)
            packed.append({"module_id": e.module_id, "pack_id": e.pack_id, "line_id": e.line_id,
                           "packed_at": e.ts, "source_event_id": e.event_id})
        elif e.event_type == "CELL_TO_MODULE":
            state.move(e, "PLACED")
        elif e.event_type == "CELL_REWORK":
            state.move(e, "REWORK")
    rows = [state.versions[k] for k in sorted(state.touched, key=lambda k: (k[0], k[1]))]
    return GenealogyChanges(
        rows=pd.DataFrame(rows, columns=GENEALOGY_COLS),
        module_packs=pd.DataFrame(packed, columns=MODULE_PACK_COLS),
    )
