"""Silver cell tests to SPC windows, control limits and Western Electric alerts.

    python -m streaming.spc_monitor

A watermarked tumbling-window aggregation emits each 1-minute window per station once
it closes. foreachBatch hands the closed windows to spc_rules.process_batch, which
holds all the statistics, and appends what comes back to three gold tables.
"""

from __future__ import annotations

import uuid
from datetime import timedelta

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from config.settings import Settings, get_settings
from streaming.bronze_ingest import default_trigger
from streaming.delta_io import BatchTimer, append, read_pdf, to_spark, utcnow, with_conflict_retry
from streaming.spark_session import build_spark
from streaming.spc_rules import LIMIT_COLS, SPC_METRICS, WINDOW_COLS, process_batch
from streaming.tables import (
    GOLD_SPC_ALERTS, GOLD_SPC_LIMITS, GOLD_SPC_WINDOWS, GOLD_TABLES, SILVER_TABLES, ensure_tables,
)

QUERY = "gold_spc"
HISTORY_LOOKBACK = timedelta(hours=2)


def window_aggregates(silver: DataFrame, window: str) -> DataFrame:
    aggs = [F.count("*").alias("n")]
    for m in SPC_METRICS:
        aggs += [F.avg(m).alias(f"{m}__mean"), F.stddev_samp(m).alias(f"{m}__std")]
    return (
        silver.groupBy(F.window("ts", window).alias("w"), "station_id")
        .agg(*aggs)
        .select(F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"), "station_id", "n",
                *[c for m in SPC_METRICS for c in (f"{m}__mean", f"{m}__std")])
    )


def to_long(wide: pd.DataFrame) -> pd.DataFrame:
    """One row per station, window and metric."""
    parts = [
        wide[["station_id", "window_start", "window_end", "n"]].assign(
            metric=m, mean=wide[f"{m}__mean"], std=wide[f"{m}__std"])
        for m in SPC_METRICS
    ]
    return pd.concat(parts, ignore_index=True)[["station_id", "metric", "window_start", "window_end", "n", "mean", "std"]]


def _alert_id(row) -> str:
    key = f"{row.station_id}|{row.metric}|{row.rule_triggered}|{row.window_start.isoformat()}"
    return str(uuid.uuid5(uuid.NAMESPACE_URL, key))


def spc_writer(settings: Settings):
    paths = {name: settings.table_path("gold", name) for name in GOLD_TABLES}

    def write(batch: DataFrame, batch_id: int) -> None:
        timer = BatchTimer(QUERY, batch_id)
        wide = batch.toPandas()
        timer.mark("to_pandas")
        if wide.empty:
            return
        spark = batch.sparkSession
        new = to_long(wide)
        since = new.window_start.min() - HISTORY_LOOKBACK
        history = read_pdf(spark, paths["spc_window_stats"], WINDOW_COLS,
                        (F.col("window_start") >= F.lit(since)) | F.col("z").isNull())
        limits = read_pdf(spark, paths["spc_control_limits"], LIMIT_COLS)
        now = utcnow()
        timer.mark("read_history")
        result = process_batch(new, history, limits, settings.spc.baseline_windows, detected_at=now)
        timer.mark("rules")

        outputs = [
            (result.new_limits.assign(computed_at=now), GOLD_SPC_LIMITS, "spc_control_limits", "limits"),
            (result.windows.assign(scored_at=now), GOLD_SPC_WINDOWS, "spc_window_stats", "windows"),
            (result.alerts.assign(alert_id=[_alert_id(r) for r in result.alerts.itertuples()]),
             GOLD_SPC_ALERTS, "spc_alerts", "alerts"),
        ]
        for pdf, schema, table, suffix in outputs:
            if not pdf.empty:
                df = to_spark(spark, pdf, schema)
                with_conflict_retry(lambda: append(df, paths[table], f"{QUERY}_{suffix}", batch_id))
        timer.mark("write")
        timer.log(len(new))

    return write


def start_spc(spark: SparkSession, settings: Settings, trigger: dict | None = None,
              source_options: dict | None = None) -> list[StreamingQuery]:
    reader = spark.readStream.format("delta")
    for k, v in (source_options or {}).items():
        reader = reader.option(k, v)
    silver = reader.load(settings.table_path("silver", "cell_tests")).withWatermark("ts", settings.spc.watermark)
    query = (
        window_aggregates(silver, settings.spc.window)
        .writeStream.queryName(QUERY).outputMode("append")
        .option("checkpointLocation", settings.checkpoint_path(QUERY))
        .trigger(**(trigger or default_trigger(settings)))
        .foreachBatch(spc_writer(settings))
        .start()
    )
    return [query]


def main() -> None:
    settings = get_settings()
    spark = build_spark("celltrace-spc", settings)
    ensure_tables(spark, settings, [*SILVER_TABLES.values(), *GOLD_TABLES.values()])
    start_spc(spark, settings)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
