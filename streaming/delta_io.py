"""Small helpers shared by the foreachBatch sinks that round-trip through pandas."""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Callable, TypeVar

import numpy as np
import pandas as pd
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.types import StructType

log = logging.getLogger("delta_io")
T = TypeVar("T")


def utcnow() -> datetime:
    """Naive UTC, matching the Spark session time zone."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _py(value):
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    return value.item() if isinstance(value, np.generic) else value


def to_spark(spark: SparkSession, pdf: pd.DataFrame, schema: StructType) -> DataFrame:
    """pandas to Spark with NaN/NaT written as null, so SQL readers see NULL rather than NaN."""
    cols = schema.fieldNames()
    clean = pdf[cols].astype(object).where(pdf[cols].notna(), None)
    return spark.createDataFrame([tuple(_py(v) for v in row) for row in clean.itertuples(index=False)], schema)


def read_pdf(spark: SparkSession, path: str, columns: list[str], where: Column | None = None) -> pd.DataFrame:
    df = spark.read.format("delta").load(path)
    if where is not None:
        df = df.filter(where)
    pdf = df.select(*columns).toPandas()
    return pdf if not pdf.empty else pd.DataFrame(columns=columns)


def append(df: DataFrame, path: str, app_id: str, batch_id: int) -> None:
    """Idempotent append: Delta skips a (txnAppId, txnVersion) pair it has already committed."""
    df.write.format("delta").mode("append").option("txnAppId", app_id).option("txnVersion", batch_id).save(path)


class BatchTimer:
    """Per-stage wall time for one foreachBatch call, logged as a single line."""

    def __init__(self, query: str, batch_id: int):
        self.query, self.batch_id = query, batch_id
        self.stages: list[tuple[str, float]] = []
        self._last = time.perf_counter()

    def mark(self, stage: str) -> None:
        now = time.perf_counter()
        self.stages.append((stage, now - self._last))
        self._last = now

    def log(self, rows: int) -> None:
        parts = ", ".join(f"{name} {secs:.1f}s" for name, secs in self.stages)
        log.info("%s batch %d: %d rows, %s", self.query, self.batch_id, rows, parts)


def with_conflict_retry(fn: Callable[[], T], attempts: int = 5, base_delay_s: float = 1.0) -> T:
    """Retry a Delta write that lost an optimistic-concurrency race (for example against OPTIMIZE)."""
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except Exception as exc:  # delta raises py4j-wrapped Java exceptions without a stable Python type
            if "Concurrent" not in type(exc).__name__ + str(exc)[:500] or attempt == attempts:
                raise
            log.warning("delta write conflict (attempt %d/%d), retrying", attempt, attempts)
            time.sleep(base_delay_s * attempt)
    raise AssertionError("unreachable")
