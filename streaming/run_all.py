"""Run every streaming stage in one Spark driver (one JVM is lighter than four on a laptop).

    python -m streaming.run_all [--only bronze,silver]
"""

from __future__ import annotations

import argparse
import logging

from config.settings import get_settings
from streaming.bronze_ingest import start_bronze
from streaming.silver_transform import start_silver
from streaming.spark_session import build_spark
from streaming.tables import BRONZE_TABLES, SILVER_TABLES, ensure_tables

log = logging.getLogger("run_all")

STAGES = {
    "bronze": start_bronze,
    "silver": start_silver,
}


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--only", help=f"comma-separated subset of {','.join(STAGES)}")
    args = p.parse_args(argv)
    stages = args.only.split(",") if args.only else list(STAGES)
    unknown = set(stages) - set(STAGES)
    if unknown:
        p.error(f"unknown stages: {sorted(unknown)}")

    settings = get_settings()
    spark = build_spark("celltrace-streaming", settings)
    ensure_tables(spark, settings, [*BRONZE_TABLES.values(), *SILVER_TABLES.values()])
    for stage in stages:
        queries = STAGES[stage](spark, settings)
        log.info("started %s: %s", stage, ", ".join(q.name for q in queries))
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
