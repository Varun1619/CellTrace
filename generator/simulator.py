"""Deterministic simulation of 3 lines x 4 end-of-line test stations.

Pure Python: no Kafka, no wall clock. The producer feeds it timestamps and ships
the returned messages; tests drive it with a simulated clock.
"""

from __future__ import annotations

import heapq
import uuid
from dataclasses import fields
from datetime import datetime, timedelta
from typing import NamedTuple

import numpy as np

from config.settings import GeneratorConfig
from config.specs import METRICS, Spec, failure_codes, load_specs
from generator.assembly import AssemblyTracker

# (mean, standard deviation) per reading for an in-control cell.
NOMINAL: dict[str, tuple[float, float]] = {
    "voltage_v": (3.65, 0.010),
    "internal_resistance_mohm": (1.20, 0.050),
    "capacity_ah": (5.00, 0.050),
    "temp_c": (25.0, 0.8),
}
DECIMALS = {"voltage_v": 4, "internal_resistance_mohm": 4, "capacity_ah": 4, "temp_c": 2}
RATE_FIELDS = ("sensor_dropout_rate", "duplicate_rate", "late_rate", "rework_rate")
COUNT_FIELDS = ("lines", "stations_per_line", "cells_per_lot", "cells_per_module", "modules_per_pack")


class Message(NamedTuple):
    topic: str  # logical topic: cell_tests, assembly_events or fault_log
    key: str
    value: dict


def iso(ts: datetime) -> str:
    return ts.isoformat(timespec="milliseconds")


def shift_for(ts: datetime) -> str:
    """Three 8-hour shifts: A 06-14, B 14-22, C 22-06 (UTC)."""
    if 6 <= ts.hour < 14:
        return "A"
    return "B" if 14 <= ts.hour < 22 else "C"


def drift_delta(elapsed_s: float, cfg: GeneratorConfig) -> float:
    """IR offset on the drifting station: zero, then a linear ramp, then a plateau."""
    if elapsed_s < cfg.drift_start_after_s:
        return 0.0
    if cfg.drift_ramp_s <= 0:
        return cfg.drift_ir_max_delta_mohm
    frac = min(1.0, (elapsed_s - cfg.drift_start_after_s) / cfg.drift_ramp_s)
    return cfg.drift_ir_max_delta_mohm * frac


def _validate(cfg: GeneratorConfig) -> None:
    if cfg.rate <= 0:
        raise ValueError("rate must be positive")
    for name in COUNT_FIELDS:
        if getattr(cfg, name) < 1:
            raise ValueError(f"{name} must be at least 1")
    for name in RATE_FIELDS:
        if not 0.0 <= getattr(cfg, name) <= 1.0:
            raise ValueError(f"{name} must be between 0 and 1")


class Simulator:
    def __init__(self, cfg: GeneratorConfig, start: datetime, run_tag: str | None = None,
                 specs: tuple[Spec, ...] | None = None):
        _validate(cfg)
        self.cfg = cfg
        self.start = start
        self.run_tag = run_tag or start.strftime("%m%d%H%M")
        self._specs = specs or load_specs()
        self._rng = np.random.default_rng(cfg.seed)
        self.run_id = self._new_id()
        self.stations = [f"L{l}-S{s}" for l in range(1, cfg.lines + 1) for s in range(1, cfg.stations_per_line + 1)]
        # Small fixed calibration offset per station, a tenth of the process spread.
        self._bias = {st: {m: self._rng.normal(0, sd * 0.1) for m, (_, sd) in NOMINAL.items()} for st in self.stations}
        self._assembly = AssemblyTracker(cfg, self._rng, self.run_tag, self._new_id)
        self._cell_n = 0
        self._pending: list[tuple[datetime, int, Message]] = []
        self._seq = 0
        self._logged: set[str] = set()

    def _new_id(self) -> str:
        return str(uuid.UUID(bytes=self._rng.bytes(16), version=4))

    def emit_cell(self, now: datetime) -> list[Message]:
        """Test one cell at `now`. Returns messages due immediately; late and duplicate copies are held."""
        cfg, n = self.cfg, self._cell_n
        self._cell_n += 1
        line_id = f"L{n % cfg.lines + 1}"
        station_id = f"{line_id}-S{(n // cfg.lines) % cfg.stations_per_line + 1}"
        lot_no = n // cfg.cells_per_lot + 1
        lot_id = f"LOT-{self.run_tag}-{lot_no:04d}"
        elapsed = (now - self.start).total_seconds()

        out = self._fault_messages(now, elapsed, lot_no, lot_id)
        readings = self._dropout(self._sample(station_id, lot_no, elapsed))
        result = "FAIL" if failure_codes(readings, self._specs) else "PASS"
        event = {
            "event_id": self._new_id(),
            "cell_id": f"C-{self.run_tag}-{n + 1:08d}",
            "lot_id": lot_id,
            "line_id": line_id,
            "station_id": station_id,
            "shift": shift_for(now),
            "ts": iso(now),
            **readings,
            "result": result,
        }
        out += self._route(Message("cell_tests", station_id, event), now)
        if result == "PASS":
            out += [Message("assembly_events", line_id, e) for e in self._assembly.place(event["cell_id"], line_id, iso(now))]
        return out

    def release_due(self, now: datetime) -> list[Message]:
        out = []
        while self._pending and self._pending[0][0] <= now:
            out.append(heapq.heappop(self._pending)[2])
        return out

    def flush(self) -> list[Message]:
        out = [m for _, _, m in sorted(self._pending)]
        self._pending = []
        return out

    def _sample(self, station_id: str, lot_no: int, elapsed: float) -> dict[str, float | None]:
        cfg = self.cfg
        values = {m: self._rng.normal(mean, sd) + self._bias[station_id][m] for m, (mean, sd) in NOMINAL.items()}
        if station_id == cfg.drift_station:
            values["internal_resistance_mohm"] += drift_delta(elapsed, cfg)
        if lot_no == cfg.bad_lot_index:
            values["capacity_ah"] += cfg.bad_lot_capacity_delta_ah
        return {m: round(float(v), DECIMALS[m]) for m, v in values.items()}

    def _dropout(self, readings: dict[str, float | None]) -> dict[str, float | None]:
        """A dropped sensor reports nothing (75%) or sticks at zero (25%)."""
        if self._rng.random() >= self.cfg.sensor_dropout_rate:
            return readings
        metric = METRICS[int(self._rng.integers(len(METRICS)))]
        broken = None if self._rng.random() < 0.75 else 0.0
        return {**readings, metric: broken}

    def _route(self, msg: Message, now: datetime) -> list[Message]:
        cfg = self.cfg
        out = []
        if self._rng.random() < cfg.late_rate:
            delay = self._rng.uniform(min(10.0, cfg.late_max_delay_s), cfg.late_max_delay_s)
            self._hold(now + timedelta(seconds=delay), msg)
        else:
            out.append(msg)
        if self._rng.random() < cfg.duplicate_rate:
            self._hold(now + timedelta(seconds=self._rng.uniform(0.5, 30.0)), msg)
        return out

    def _hold(self, release_at: datetime, msg: Message) -> None:
        self._seq += 1
        heapq.heappush(self._pending, (release_at, self._seq, msg))

    def _fault_messages(self, now: datetime, elapsed: float, lot_no: int, lot_id: str) -> list[Message]:
        cfg = self.cfg
        out = []
        if elapsed >= cfg.drift_start_after_s and "drift" not in self._logged and cfg.drift_station in self.stations:
            self._logged.add("drift")
            started = self.start + timedelta(seconds=cfg.drift_start_after_s)
            out.append(self._fault("STATION_DRIFT", cfg.drift_station, "internal_resistance_mohm", started,
                                   cfg.drift_ir_max_delta_mohm, f"linear ramp over {cfg.drift_ramp_s:.0f}s"))
        if lot_no == cfg.bad_lot_index and "bad_lot" not in self._logged:
            self._logged.add("bad_lot")
            out.append(self._fault("BAD_LOT", lot_id, "capacity_ah", now, cfg.bad_lot_capacity_delta_ah,
                                   f"{cfg.cells_per_lot} cells"))
        return out

    def _fault(self, fault_type: str, target: str, metric: str, started: datetime, magnitude: float, details: str) -> Message:
        value = {
            "fault_id": self._new_id(),
            "run_id": self.run_id,
            "fault_type": fault_type,
            "target": target,
            "metric": metric,
            "started_at": iso(started),
            "magnitude": magnitude,
            "details": details,
        }
        return Message("fault_log", fault_type, value)


def run_simulation(cfg: GeneratorConfig, n_cells: int, start: datetime, run_tag: str | None = None) -> list[Message]:
    """Run n_cells through a simulated clock at cfg.rate and return every message in emit order."""
    sim = Simulator(cfg, start=start, run_tag=run_tag)
    step = timedelta(seconds=1.0 / cfg.rate)
    out: list[Message] = []
    for i in range(n_cells):
        now = start + step * i
        out += sim.release_due(now)
        out += sim.emit_cell(now)
    return out + sim.flush()


def config_summary(cfg: GeneratorConfig) -> dict:
    return {f.name: getattr(cfg, f.name) for f in fields(cfg)}
