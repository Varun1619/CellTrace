"""Given a lot_id, list every affected pack, module and cell with its test results.

    python -m scripts.recall_lookup LOT-10211557-0005
    python -m scripts.recall_lookup --bad-lot          # the lot the generator logged as bad
"""

from __future__ import annotations

import argparse
import time
from dataclasses import dataclass

import pandas as pd

from config.settings import Settings, get_settings
from scripts.lakehouse import query

RECALL_SQL = """
WITH tests AS (
    SELECT cell_id, lot_id, station_id, result, ts AS tested_at,
           voltage_v, internal_resistance_mohm, capacity_ah, temp_c
    FROM {silver.cell_tests}
    WHERE lot_id = ?
),
versions AS (
    SELECT * FROM {silver.cell_genealogy} WHERE cell_id IN (SELECT cell_id FROM tests)
),
history AS (
    SELECT cell_id, string_agg(module_id, ' > ' ORDER BY valid_from) AS module_history, count(*) AS versions
    FROM versions GROUP BY cell_id
)
SELECT t.*, c.module_id, c.pack_id, c.valid_from AS placed_at, h.module_history,
       coalesce(h.versions, 0) AS versions
FROM tests t
LEFT JOIN versions c ON c.cell_id = t.cell_id AND c.is_current
LEFT JOIN history h ON h.cell_id = t.cell_id
ORDER BY c.pack_id NULLS LAST, c.module_id, t.cell_id
"""

BAD_LOT_SQL = "SELECT target FROM {silver.fault_log} WHERE fault_type = 'BAD_LOT' ORDER BY started_at DESC LIMIT 1"


@dataclass(frozen=True)
class RecallResult:
    lot_id: str
    cells: pd.DataFrame
    packs: list[str]
    modules: list[str]
    counts: dict[str, int]
    elapsed_s: float


def _ids(series: pd.Series) -> list[str]:
    return sorted(v for v in series.dropna().unique())


def recall(lot_id: str, settings: Settings | None = None) -> RecallResult:
    t0 = time.perf_counter()
    cells = query(RECALL_SQL, settings, [lot_id])
    elapsed = time.perf_counter() - t0
    packs = _ids(cells.pack_id)
    counts = {
        "cells": len(cells),
        "failed_at_test": int((cells.result == "FAIL").sum()),
        "not_yet_assembled": int(((cells.result == "PASS") & cells.module_id.isna()).sum()),
        "in_packs": int(cells.pack_id.notna().sum()),
    }
    return RecallResult(lot_id, cells, packs, _ids(cells.module_id), counts, elapsed)


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("lot_id", nargs="?")
    p.add_argument("--bad-lot", action="store_true", help="look up the lot the generator logged as bad")
    p.add_argument("--show", type=int, default=10, help="cell rows to print")
    p.add_argument("--csv", help="write every affected cell to this CSV path")
    args = p.parse_args(argv)
    settings = get_settings()

    lot_id = args.lot_id
    if args.bad_lot:
        found = query(BAD_LOT_SQL, settings)
        if found.empty:
            p.error("no BAD_LOT entry in silver.fault_log yet")
        lot_id = found.target[0]
    if not lot_id:
        p.error("give a lot_id or --bad-lot")

    r = recall(lot_id, settings)
    print(f"lot {r.lot_id}: {r.counts['cells']} cells tested, {r.counts['failed_at_test']} failed at test, "
          f"{r.counts['not_yet_assembled']} not yet assembled")
    print(f"affected: {len(r.packs)} packs, {len(r.modules)} modules, {r.counts['in_packs']} cells inside packs")
    print(f"lookup time: {r.elapsed_s * 1000:.0f} ms\n")
    print("packs:", ", ".join(r.packs) or "none")
    cols = ["cell_id", "result", "capacity_ah", "station_id", "module_id", "pack_id", "module_history"]
    print(r.cells[cols].head(args.show).to_string(index=False))
    if args.csv:
        r.cells.to_csv(args.csv, index=False)
        print(f"\nwrote {len(r.cells)} rows to {args.csv}")


if __name__ == "__main__":
    main()
