"""Schemas for every Delta table and a helper that creates them before streams start.

Creating tables up front means a downstream stream never races an upstream one
for the first commit, and schemas are explicit instead of inferred.
"""

from __future__ import annotations

from dataclasses import dataclass

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType, DateType, DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType,
)

from config.settings import Settings


def _schema(*cols: tuple[str, object]) -> StructType:
    return StructType([StructField(name, dtype, True) for name, dtype in cols])


S, D, L, I, T, B = StringType(), DoubleType(), LongType(), IntegerType(), TimestampType(), BooleanType()

# Raw JSON as it arrives from the generator. ts stays a string here and is cast in silver.
CELL_TEST_JSON = _schema(
    ("event_id", S), ("cell_id", S), ("lot_id", S), ("line_id", S), ("station_id", S), ("shift", S), ("ts", S),
    ("voltage_v", D), ("internal_resistance_mohm", D), ("capacity_ah", D), ("temp_c", D), ("result", S),
)
ASSEMBLY_JSON = _schema(
    ("event_id", S), ("event_type", S), ("line_id", S), ("cell_id", S), ("module_id", S), ("pack_id", S),
    ("previous_module_id", S), ("ts", S),
)
FAULT_JSON = _schema(
    ("fault_id", S), ("run_id", S), ("fault_type", S), ("target", S), ("metric", S), ("started_at", S),
    ("magnitude", D), ("details", S),
)

KAFKA_META = (("kafka_partition", I), ("kafka_offset", L), ("ingest_ts", T))

BRONZE = _schema(
    ("kafka_key", S), ("payload", S), ("topic", S), ("partition", I), ("offset", L),
    ("kafka_ts", T), ("ingest_ts", T), ("ingest_date", DateType()),
)
SILVER_CELL_TESTS = _schema(
    ("event_id", S), ("cell_id", S), ("lot_id", S), ("line_id", S), ("station_id", S), ("shift", S), ("ts", T),
    ("voltage_v", D), ("internal_resistance_mohm", D), ("capacity_ah", D), ("temp_c", D), ("result", S),
    ("test_date", DateType()), *KAFKA_META,
)
SILVER_QUARANTINE = _schema(
    ("quarantine_reason", S), ("event_id", S), ("station_id", S), ("ts_raw", S), ("payload", S),
    *KAFKA_META, ("quarantined_at", T),
)
SILVER_ASSEMBLY = _schema(
    ("event_id", S), ("event_type", S), ("line_id", S), ("cell_id", S), ("module_id", S), ("pack_id", S),
    ("previous_module_id", S), ("ts", T), *KAFKA_META,
)
SILVER_FAULT_LOG = _schema(
    ("fault_id", S), ("run_id", S), ("fault_type", S), ("target", S), ("metric", S), ("started_at", T),
    ("magnitude", D), ("details", S), ("ingest_ts", T),
)
SILVER_DQ_METRICS = _schema(
    ("query", S), ("batch_id", L), ("rows", L), ("checks_passed", B), ("failed_checks", S), ("checked_at", T),
)

GOLD_SPC_WINDOWS = _schema(
    ("station_id", S), ("metric", S), ("window_start", T), ("window_end", T), ("n", L), ("mean", D), ("std", D),
    ("center", D), ("sigma_xbar", D), ("z", D), ("scored_at", T),
)
GOLD_SPC_LIMITS = _schema(
    ("station_id", S), ("metric", S), ("center", D), ("sigma", D), ("baseline_windows", L), ("baseline_n", L),
    ("computed_at", T),
)
GOLD_SPC_ALERTS = _schema(
    ("alert_id", S), ("station_id", S), ("metric", S), ("rule_triggered", S), ("rule_description", S),
    ("window_start", T), ("window_end", T), ("value", D), ("limit", D), ("z", D), ("detected_at", T),
)


@dataclass(frozen=True)
class TableDef:
    layer: str
    name: str
    schema: StructType
    partition_by: tuple[str, ...] = ()


BRONZE_TABLES = {
    topic: TableDef("bronze", topic, BRONZE, ("ingest_date",))
    for topic in ("cell_tests", "assembly_events", "fault_log")
}
SILVER_TABLES = {
    "cell_tests": TableDef("silver", "cell_tests", SILVER_CELL_TESTS, ("test_date",)),
    "cell_tests_quarantine": TableDef("silver", "cell_tests_quarantine", SILVER_QUARANTINE),
    "assembly_events": TableDef("silver", "assembly_events", SILVER_ASSEMBLY),
    "assembly_events_quarantine": TableDef("silver", "assembly_events_quarantine", SILVER_QUARANTINE),
    "fault_log": TableDef("silver", "fault_log", SILVER_FAULT_LOG),
    "dq_batch_metrics": TableDef("silver", "dq_batch_metrics", SILVER_DQ_METRICS),
}

GOLD_TABLES = {
    "spc_window_stats": TableDef("gold", "spc_window_stats", GOLD_SPC_WINDOWS),
    "spc_control_limits": TableDef("gold", "spc_control_limits", GOLD_SPC_LIMITS),
    "spc_alerts": TableDef("gold", "spc_alerts", GOLD_SPC_ALERTS),
}


def ensure_table(spark: SparkSession, settings: Settings, table: TableDef) -> str:
    path = settings.table_path(table.layer, table.name)
    builder = DeltaTable.createIfNotExists(spark).location(path).addColumns(table.schema)
    if table.partition_by:
        builder = builder.partitionedBy(*table.partition_by)
    builder.execute()
    return path


def ensure_tables(spark: SparkSession, settings: Settings, tables) -> None:
    for table in tables:
        ensure_table(spark, settings, table)
