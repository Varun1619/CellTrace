"""Central configuration. Every path, port and tunable comes from the environment.

Values load from the process environment first, then from .env at the repo root.
Defaults match .env.example so tests and fresh checkouts work without a .env file.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Mapping

from dotenv import dotenv_values

REPO_ROOT = Path(__file__).resolve().parent.parent
LAYERS = ("bronze", "silver", "gold")


@dataclass(frozen=True)
class Topics:
    cell_tests: str
    assembly_events: str
    fault_log: str


@dataclass(frozen=True)
class GeneratorConfig:
    rate: float
    seed: int
    lines: int
    stations_per_line: int
    cells_per_lot: int
    cells_per_module: int
    modules_per_pack: int
    drift_station: str
    drift_start_after_s: float
    drift_ramp_s: float
    drift_ir_max_delta_mohm: float
    bad_lot_index: int
    bad_lot_capacity_delta_ah: float
    sensor_dropout_rate: float
    duplicate_rate: float
    late_rate: float
    late_max_delay_s: float
    rework_rate: float


@dataclass(frozen=True)
class SparkConfig:
    driver_memory: str
    shuffle_partitions: int
    trigger_seconds: int
    silver_dedup_watermark: str


@dataclass(frozen=True)
class SpcConfig:
    window: str
    watermark: str
    baseline_windows: int


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap_servers: str
    topics: Topics
    lakehouse_root: Path
    generator: GeneratorConfig
    spark: SparkConfig
    spc: SpcConfig
    dashboard_port: int

    def table_path(self, layer: str, name: str) -> str:
        if layer not in LAYERS:
            raise ValueError(f"unknown lakehouse layer {layer!r}, expected one of {LAYERS}")
        return (self.lakehouse_root / layer / name).as_posix()

    def checkpoint_path(self, query_name: str) -> str:
        return (self.lakehouse_root / "_checkpoints" / query_name).as_posix()


def _resolve_root(raw: str) -> Path:
    path = Path(raw)
    return path if path.is_absolute() else (REPO_ROOT / path).resolve()


def load_settings(env: Mapping[str, str] | None = None) -> Settings:
    """Build Settings from a mapping. With env=None, read os.environ layered over .env."""
    if env is None:
        file_values = {k: v for k, v in dotenv_values(REPO_ROOT / ".env").items() if v is not None}
        env = {**file_values, **os.environ}

    def get(key: str, default: str) -> str:
        return env.get(key, default)

    generator = GeneratorConfig(
        rate=float(get("GENERATOR_RATE", "50")),
        seed=int(get("GENERATOR_SEED", "42")),
        lines=int(get("GENERATOR_LINES", "3")),
        stations_per_line=int(get("GENERATOR_STATIONS_PER_LINE", "4")),
        cells_per_lot=int(get("CELLS_PER_LOT", "5000")),
        cells_per_module=int(get("CELLS_PER_MODULE", "46")),
        modules_per_pack=int(get("MODULES_PER_PACK", "4")),
        drift_station=get("DRIFT_STATION", "L2-S3"),
        drift_start_after_s=float(get("DRIFT_START_AFTER_S", "1560")),
        drift_ramp_s=float(get("DRIFT_RAMP_S", "600")),
        drift_ir_max_delta_mohm=float(get("DRIFT_IR_MAX_DELTA_MOHM", "0.25")),
        bad_lot_index=int(get("BAD_LOT_INDEX", "15")),
        bad_lot_capacity_delta_ah=float(get("BAD_LOT_CAPACITY_DELTA_AH", "-0.12")),
        sensor_dropout_rate=float(get("SENSOR_DROPOUT_RATE", "0.005")),
        duplicate_rate=float(get("DUPLICATE_RATE", "0.01")),
        late_rate=float(get("LATE_RATE", "0.01")),
        late_max_delay_s=float(get("LATE_MAX_DELAY_S", "300")),
        rework_rate=float(get("REWORK_RATE", "0.002")),
    )
    return Settings(
        kafka_bootstrap_servers=get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        topics=Topics(
            cell_tests=get("TOPIC_CELL_TESTS", "cell_tests"),
            assembly_events=get("TOPIC_ASSEMBLY_EVENTS", "assembly_events"),
            fault_log=get("TOPIC_FAULT_LOG", "fault_log"),
        ),
        lakehouse_root=_resolve_root(get("LAKEHOUSE_ROOT", "./lakehouse")),
        generator=generator,
        spark=SparkConfig(
            driver_memory=get("SPARK_DRIVER_MEMORY", "3g"),
            shuffle_partitions=int(get("SPARK_SHUFFLE_PARTITIONS", "4")),
            trigger_seconds=int(get("STREAM_TRIGGER_SECONDS", "10")),
            silver_dedup_watermark=get("SILVER_DEDUP_WATERMARK", "10 minutes"),
        ),
        spc=SpcConfig(
            window=get("SPC_WINDOW", "1 minute"),
            watermark=get("SPC_WATERMARK", "1 minute"),
            baseline_windows=int(get("SPC_BASELINE_WINDOWS", "20")),
        ),
        dashboard_port=int(get("DASHBOARD_PORT", "8501")),
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return load_settings()
