"""OPTIMIZE (compact small files) and VACUUM the Delta tables in one layer, with delta-rs.

    python -m scripts.maintain_delta --layer silver

Streaming writes leave many small files. Compaction commits with dataChange=false, so
the Spark streams reading these tables ignore it. VACUUM keeps 7 days of history by
default, far longer than any stream's checkpoint lag.
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from pathlib import Path

from deltalake import DeltaTable
from deltalake.exceptions import CommitFailedError

from config.settings import Settings, get_settings

log = logging.getLogger("maintain_delta")


def maintain_table(path: str, vacuum_retention_hours: int, attempts: int = 5) -> dict:
    for attempt in range(1, attempts + 1):
        try:
            dt = DeltaTable(path)
            metrics = dt.optimize.compact()
            break
        except CommitFailedError:
            # A streaming append committed first; compaction is safe to redo from the new version.
            if attempt == attempts:
                raise
            log.warning("optimize conflict on %s (attempt %d/%d), retrying", path, attempt, attempts)
            time.sleep(attempt)
    # enforce_retention_duration makes delta-rs refuse anything under 7 days, which could
    # delete files a lagging stream still needs.
    vacuumed = DeltaTable(path).vacuum(retention_hours=vacuum_retention_hours, dry_run=False,
                                       enforce_retention_duration=True)
    return {
        "files_added": metrics.get("numFilesAdded", 0),
        "files_removed": metrics.get("numFilesRemoved", 0),
        "files_vacuumed": len(vacuumed),
    }


def maintain_layer(settings: Settings, layer: str, vacuum_retention_hours: int = 168) -> dict[str, dict]:
    root = Path(settings.table_path(layer, "_")).parent
    results = {}
    if not root.is_dir():
        return results
    for table_dir in sorted(p for p in root.iterdir() if (p / "_delta_log").is_dir()):
        results[table_dir.name] = maintain_table(table_dir.as_posix(), vacuum_retention_hours)
        log.info("%s.%s %s", layer, table_dir.name, results[table_dir.name])
    return results


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--layer", default="silver", choices=["bronze", "silver", "gold"])
    p.add_argument("--vacuum-retention-hours", type=int, default=168)
    args = p.parse_args(argv)
    print(json.dumps(maintain_layer(get_settings(), args.layer, args.vacuum_retention_hours), indent=2))


if __name__ == "__main__":
    main()
