"""Silver assembly events to silver.cell_genealogy (SCD Type 2) and silver.module_pack.

    python -m streaming.genealogy
"""

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from config.settings import Settings, get_settings
from streaming.bronze_ingest import default_trigger
from streaming.delta_io import append, read_pdf, to_spark, utcnow, with_conflict_retry
from streaming.genealogy_rules import GENEALOGY_COLS, MODULE_PACK_COLS, apply_assembly_events
from streaming.spark_session import build_spark
from streaming.tables import SILVER_GENEALOGY, SILVER_MODULE_PACK, SILVER_TABLES, ensure_tables

QUERY = "silver_genealogy"


def _ids(series) -> list[str]:
    return sorted({v for v in series.dropna()})


def merge_versions(spark: SparkSession, path: str, rows: DataFrame) -> None:
    """Upsert version rows on their SCD2 key. Closing a version is an update; a new version is an insert."""
    (DeltaTable.forPath(spark, path).alias("t")
     .merge(rows.alias("s"), "t.cell_id = s.cell_id AND t.valid_from = s.valid_from")
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute())


def genealogy_writer(settings: Settings):
    genealogy_path = settings.table_path("silver", "cell_genealogy")
    module_pack_path = settings.table_path("silver", "module_pack")

    def write(batch: DataFrame, batch_id: int) -> None:
        events = batch.toPandas()
        if events.empty:
            return
        spark = batch.sparkSession
        cells = _ids(events.cell_id)
        modules = _ids(events.module_id) + _ids(events.previous_module_id)
        current = read_pdf(spark, genealogy_path, GENEALOGY_COLS,
                           F.col("is_current") & (F.col("cell_id").isin(cells) | F.col("module_id").isin(modules)))
        known = read_pdf(spark, module_pack_path, MODULE_PACK_COLS, F.col("module_id").isin(modules))
        changes = apply_assembly_events(current, dict(zip(known.module_id, known.pack_id)), events)

        if not changes.rows.empty:
            rows = to_spark(spark, changes.rows.assign(updated_at=utcnow()), SILVER_GENEALOGY)
            with_conflict_retry(lambda: merge_versions(spark, genealogy_path, rows))
        if not changes.module_packs.empty:
            packs = to_spark(spark, changes.module_packs, SILVER_MODULE_PACK)
            with_conflict_retry(lambda: append(packs, module_pack_path, f"{QUERY}_module_pack", batch_id))

    return write


def start_genealogy(spark: SparkSession, settings: Settings, trigger: dict | None = None,
                    source_options: dict | None = None) -> list[StreamingQuery]:
    reader = spark.readStream.format("delta")
    for k, v in (source_options or {}).items():
        reader = reader.option(k, v)
    query = (
        reader.load(settings.table_path("silver", "assembly_events"))
        .writeStream.queryName(QUERY)
        .option("checkpointLocation", settings.checkpoint_path(QUERY))
        .trigger(**(trigger or default_trigger(settings)))
        .foreachBatch(genealogy_writer(settings))
        .start()
    )
    return [query]


def main() -> None:
    settings = get_settings()
    spark = build_spark("celltrace-genealogy", settings)
    ensure_tables(spark, settings, SILVER_TABLES.values())
    start_genealogy(spark, settings)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
