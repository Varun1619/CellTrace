"""Publish dbt's rpt_daily_quality to the Delta table gold.daily_quality_report.

    python -m scripts.publish_daily_report [--date 2026-10-21]

Upserts on test_date, so rerunning a day (an Airflow retry or backfill) replaces that
day's row instead of duplicating it.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from config.settings import Settings, get_settings


def report_parquet(settings: Settings) -> Path:
    return settings.lakehouse_root / "gold" / "dbt" / "rpt_daily_quality.parquet"


def publish(settings: Settings, source: Path | None = None, only_date: date | None = None) -> int:
    """Upsert report rows (optionally one day) and return how many were written."""
    rows = pd.read_parquet(source or report_parquet(settings))
    rows["test_date"] = pd.to_datetime(rows["test_date"]).dt.date
    if only_date is not None:
        rows = rows[rows.test_date == only_date]
    if rows.empty:
        return 0
    rows = rows.assign(published_at=datetime.now(timezone.utc).replace(tzinfo=None))
    table = pa.Table.from_pandas(rows, preserve_index=False)
    path = settings.table_path("gold", "daily_quality_report")
    if not DeltaTable.is_deltatable(path):
        write_deltalake(path, table)
    else:
        (DeltaTable(path).merge(table, predicate="t.test_date = s.test_date", source_alias="s", target_alias="t")
         .when_matched_update_all().when_not_matched_insert_all().execute())
    return len(rows)


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--date", type=date.fromisoformat, help="publish only this test date (default: all)")
    args = p.parse_args(argv)
    n = publish(get_settings(), only_date=args.date)
    print(f"published {n} day(s) to gold.daily_quality_report")


if __name__ == "__main__":
    main()
