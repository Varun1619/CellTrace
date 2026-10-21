"""Builders for bronze-shaped test data."""

import json
from datetime import date, datetime

GOOD_CELL = {
    "event_id": "e-1", "cell_id": "C-1", "lot_id": "LOT-1", "line_id": "L1", "station_id": "L1-S1",
    "shift": "A", "ts": "2026-01-05T08:00:00.000+00:00", "voltage_v": 3.65,
    "internal_resistance_mohm": 1.2, "capacity_ah": 5.0, "temp_c": 25.0, "result": "PASS",
}


def cell(**overrides):
    return {**GOOD_CELL, **overrides}


def bronze_rows(payloads, topic="cell_tests"):
    """Wrap dicts (or raw strings) the way bronze stores Kafka records."""
    now = datetime(2026, 1, 5, 8, 0, 5)
    rows = []
    for offset, p in enumerate(payloads):
        raw = p if isinstance(p, str) else json.dumps(p)
        rows.append(("k", raw, topic, 0, offset, now, now, date(2026, 1, 5)))
    return rows


def bronze_df(spark, payloads, topic="cell_tests"):
    from streaming.tables import BRONZE

    return spark.createDataFrame(bronze_rows(payloads, topic), BRONZE)
