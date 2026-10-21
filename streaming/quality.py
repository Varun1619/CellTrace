"""Data quality assertions on silver micro-batches.

Row checks compile into one aggregation so a batch costs one Spark job plus one
for the uniqueness check.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import reduce

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from config.specs import METRICS, PLAUSIBLE_RANGES


class DataQualityError(RuntimeError):
    pass


@dataclass(frozen=True)
class RowCheck:
    name: str
    violation: Column  # true for a row that breaks the rule


@dataclass(frozen=True)
class UniqueCheck:
    name: str
    keys: tuple[str, ...]


def not_null(*cols: str) -> RowCheck:
    return RowCheck(f"not_null_{'_'.join(cols)}", reduce(lambda a, b: a | b, [F.col(c).isNull() for c in cols]))


def cell_test_checks() -> list:
    in_range = [
        RowCheck(f"range_{m}", ~F.col(m).between(*PLAUSIBLE_RANGES[m]) | F.col(m).isNull()) for m in METRICS
    ]
    return [
        not_null("event_id", "cell_id", "lot_id", "line_id", "station_id", "ts"),
        RowCheck("accepted_result", ~F.col("result").isin("PASS", "FAIL") | F.col("result").isNull()),
        *in_range,
        UniqueCheck("unique_event_id", ("event_id",)),
    ]


def assembly_checks() -> list:
    return [not_null("event_id", "event_type", "line_id", "module_id", "ts"), UniqueCheck("unique_event_id", ("event_id",))]


def failed_checks(df: DataFrame, checks: list) -> list[str]:
    """Names of checks with at least one violating row, in the order given."""
    row_checks = [c for c in checks if isinstance(c, RowCheck)]
    counts = {}
    if row_checks:
        agg = df.agg(*[F.sum(F.when(c.violation, 1).otherwise(0)).alias(c.name) for c in row_checks]).collect()[0]
        counts = {c.name: agg[c.name] or 0 for c in row_checks}
    failed = []
    for c in checks:
        if isinstance(c, RowCheck) and counts[c.name] > 0:
            failed.append(c.name)
        elif isinstance(c, UniqueCheck) and df.groupBy(*c.keys).count().filter("count > 1").limit(1).count() > 0:
            failed.append(c.name)
    return failed
