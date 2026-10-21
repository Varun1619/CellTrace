"""Turns passing cells into cell-to-module and module-to-pack assembly events."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

import numpy as np

from config.settings import GeneratorConfig


@dataclass
class _LineState:
    module_seq: int = 0
    pack_seq: int = 0
    open_module: str = ""
    open_count: int = 0
    open_members: list[str] = field(default_factory=list)
    full_unpacked: list[str] = field(default_factory=list)
    last_full_module: str | None = None
    last_full_members: list[str] = field(default_factory=list)


class AssemblyTracker:
    """Fills modules per line in arrival order. FAIL cells never reach this class."""

    def __init__(self, cfg: GeneratorConfig, rng: np.random.Generator, run_tag: str, new_id: Callable[[], str]):
        self._cfg = cfg
        self._rng = rng
        self._tag = run_tag
        self._new_id = new_id
        self._lines: dict[str, _LineState] = {}

    def place(self, cell_id: str, line_id: str, ts: str) -> list[dict]:
        state = self._lines.get(line_id) or self._open_line(line_id)
        events = []
        if self._cfg.rework_rate > 0 and state.last_full_members and self._rng.random() < self._cfg.rework_rate:
            events.append(self._rework(state, line_id, ts))
        events.append(self._event("CELL_TO_MODULE", line_id, ts, cell_id=cell_id, module_id=state.open_module))
        state.open_count += 1
        state.open_members.append(cell_id)
        if state.open_count == self._cfg.cells_per_module:
            self._close_module(state, line_id)
        if len(state.full_unpacked) == self._cfg.modules_per_pack:
            events.extend(self._build_pack(state, line_id, ts))
        return events

    def _open_line(self, line_id: str) -> _LineState:
        state = _LineState()
        self._lines[line_id] = state
        self._start_module(state, line_id)
        return state

    def _start_module(self, state: _LineState, line_id: str) -> None:
        state.module_seq += 1
        state.open_module = f"M-{self._tag}-{line_id}-{state.module_seq:05d}"
        state.open_count = 0
        state.open_members = []

    def _close_module(self, state: _LineState, line_id: str) -> None:
        state.full_unpacked.append(state.open_module)
        state.last_full_module = state.open_module
        state.last_full_members = state.open_members
        self._start_module(state, line_id)

    def _build_pack(self, state: _LineState, line_id: str, ts: str) -> list[dict]:
        state.pack_seq += 1
        pack_id = f"P-{self._tag}-{line_id}-{state.pack_seq:05d}"
        events = [self._event("MODULE_TO_PACK", line_id, ts, module_id=m, pack_id=pack_id) for m in state.full_unpacked]
        state.full_unpacked = []
        return events

    def _rework(self, state: _LineState, line_id: str, ts: str) -> dict:
        """Pull one cell out of the last finished module and put it in the open one."""
        idx = int(self._rng.integers(len(state.last_full_members)))
        cell_id = state.last_full_members.pop(idx)
        state.open_members.append(cell_id)
        return self._event(
            "CELL_REWORK", line_id, ts,
            cell_id=cell_id, module_id=state.open_module, previous_module_id=state.last_full_module,
        )

    def _event(self, event_type: str, line_id: str, ts: str, **fields) -> dict:
        base = {
            "event_id": self._new_id(),
            "event_type": event_type,
            "line_id": line_id,
            "cell_id": None,
            "module_id": None,
            "pack_id": None,
            "previous_module_id": None,
            "ts": ts,
        }
        return {**base, **fields}
