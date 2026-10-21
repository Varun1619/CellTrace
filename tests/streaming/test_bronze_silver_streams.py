"""Run the real bronze-to-silver streaming queries over a temp lakehouse with availableNow."""

from datetime import datetime

import pytest

from tests.streaming.helpers import bronze_rows, cell

pytestmark = pytest.mark.spark


def test_to_bronze_keeps_payload_and_kafka_metadata(spark):
    from pyspark.sql.types import BinaryType, IntegerType, LongType, StringType, StructField, StructType, TimestampType

    from streaming.bronze_ingest import to_bronze

    kafka_schema = StructType([
        StructField("key", BinaryType()), StructField("value", BinaryType()), StructField("topic", StringType()),
        StructField("partition", IntegerType()), StructField("offset", LongType()),
        StructField("timestamp", TimestampType()), StructField("timestampType", IntegerType()),
    ])
    raw = spark.createDataFrame([(b"L1-S1", b'{"a":1}', "cell_tests", 2, 41, datetime(2026, 1, 5, 8), 0)], kafka_schema)
    row = to_bronze(raw).collect()[0]
    assert (row.kafka_key, row.payload, row.topic, row.partition, row.offset) == ("L1-S1", '{"a":1}', "cell_tests", 2, 41)
    assert row.ingest_ts is not None and row.ingest_date is not None


def test_silver_stream_dedupes_quarantines_and_records_dq(spark, lake):
    from streaming.silver_transform import start_silver
    from streaming.tables import BRONZE, BRONZE_TABLES, SILVER_TABLES, ensure_tables

    ensure_tables(spark, lake, [*BRONZE_TABLES.values(), *SILVER_TABLES.values()])
    payloads = [cell(event_id="e-1"), cell(event_id="e-1"), cell(event_id="e-2"), cell(event_id="bad", voltage_v=None)]
    spark.createDataFrame(bronze_rows(payloads), BRONZE).write.format("delta").mode("append").save(
        lake.table_path("bronze", "cell_tests"))

    for q in start_silver(spark, lake, trigger={"availableNow": True}):
        q.awaitTermination(120)

    read = lambda name: spark.read.format("delta").load(lake.table_path("silver", name))
    assert sorted(r.event_id for r in read("cell_tests").collect()) == ["e-1", "e-2"]
    assert [r.quarantine_reason for r in read("cell_tests_quarantine").collect()] == ["null_voltage_v"]
    dq = read("dq_batch_metrics").collect()
    assert dq and all(r.checks_passed for r in dq)
    assert sum(r.rows for r in dq if r.query == "silver_cell_tests") == 2
