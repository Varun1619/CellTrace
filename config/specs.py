"""Spec limits (PASS/FAIL) and physical plausibility ranges (quarantine).

Spec limits live in dbt/seeds/spec_limits.csv so the generator and the dbt models
read the same numbers. Plausibility ranges are wider: a value outside them is a
sensor fault, not a bad cell.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Mapping

from config.settings import REPO_ROOT

SPEC_CSV = REPO_ROOT / "dbt" / "seeds" / "spec_limits.csv"
DROPOUT_CODE = "SENSOR_DROPOUT"

PLAUSIBLE_RANGES: dict[str, tuple[float, float]] = {
    "voltage_v": (2.5, 4.3),
    "internal_resistance_mohm": (0.1, 10.0),
    "capacity_ah": (1.0, 7.0),
    "temp_c": (5.0, 60.0),
}
METRICS = tuple(PLAUSIBLE_RANGES)


@dataclass(frozen=True)
class Spec:
    metric: str
    lower: float | None
    upper: float | None
    failure_code: str

    def violated_by(self, value: float) -> bool:
        return (self.lower is not None and value < self.lower) or (self.upper is not None and value > self.upper)


def _num(raw: str) -> float | None:
    return float(raw) if raw.strip() else None


@lru_cache(maxsize=4)
def load_specs(path: Path = SPEC_CSV) -> tuple[Spec, ...]:
    with open(path, newline="") as fh:
        return tuple(
            Spec(row["metric"], _num(row["lower_limit"]), _num(row["upper_limit"]), row["failure_code"])
            for row in csv.DictReader(fh)
        )


def is_plausible(metric: str, value: float | None) -> bool:
    if value is None:
        return False
    low, high = PLAUSIBLE_RANGES[metric]
    return low <= value <= high


def failure_codes(readings: Mapping[str, float | None], specs: tuple[Spec, ...]) -> list[str]:
    """Codes for every violated spec, in spec order. Any unreadable sensor short-circuits to a dropout."""
    if any(not is_plausible(m, readings.get(m)) for m in METRICS):
        return [DROPOUT_CODE]
    return [s.failure_code for s in specs if s.violated_by(readings[s.metric])]
