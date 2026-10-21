"""SparkSession factory with Delta Lake and the Kafka connector."""

from __future__ import annotations

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from config.settings import Settings, get_settings

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"


def build_spark(app_name: str, settings: Settings | None = None, local_test: bool = False) -> SparkSession:
    """Create (or reuse) a local SparkSession.

    local_test trims shuffle partitions and disables the UI so unit tests start fast.
    """
    s = settings or get_settings()
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", s.spark.driver_memory)
        .config("spark.sql.shuffle.partitions", "1" if local_test else str(s.spark.shuffle_partitions))
        .config("spark.ui.enabled", "false" if local_test else "true")
        .config("spark.sql.streaming.stateStore.stateSchemaCheck", "true")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "false")
    )
    spark = configure_spark_with_delta_pip(builder, extra_packages=[KAFKA_PACKAGE]).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
