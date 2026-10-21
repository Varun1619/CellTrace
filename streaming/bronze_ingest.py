"""Kafka to bronze: raw payloads plus Kafka metadata, no parsing.

    python -m streaming.bronze_ingest
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from config.settings import Settings, get_settings
from streaming.spark_session import build_spark
from streaming.tables import BRONZE_TABLES, ensure_tables


def default_trigger(settings: Settings) -> dict:
    return {"processingTime": f"{settings.spark.trigger_seconds} seconds"}


def kafka_stream(spark: SparkSession, settings: Settings, topic: str) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 100_000)
        .load()
    )


def to_bronze(kafka_df: DataFrame) -> DataFrame:
    return kafka_df.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("payload"),
        "topic",
        "partition",
        "offset",
        F.col("timestamp").alias("kafka_ts"),
        F.current_timestamp().alias("ingest_ts"),
    ).withColumn("ingest_date", F.to_date("ingest_ts"))


def start_bronze(spark: SparkSession, settings: Settings, trigger: dict | None = None) -> list[StreamingQuery]:
    trigger = trigger or default_trigger(settings)
    topics = {
        "cell_tests": settings.topics.cell_tests,
        "assembly_events": settings.topics.assembly_events,
        "fault_log": settings.topics.fault_log,
    }
    queries = []
    for table, topic in topics.items():
        name = f"bronze_{table}"
        queries.append(
            to_bronze(kafka_stream(spark, settings, topic))
            .writeStream.format("delta")
            .queryName(name)
            .outputMode("append")
            .option("checkpointLocation", settings.checkpoint_path(name))
            .trigger(**trigger)
            .start(settings.table_path("bronze", table))
        )
    return queries


def main() -> None:
    settings = get_settings()
    spark = build_spark("celltrace-bronze", settings)
    ensure_tables(spark, settings, BRONZE_TABLES.values())
    start_bronze(spark, settings)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
