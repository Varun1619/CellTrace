"""Publish simulated cell tests, assembly events and fault log entries to Kafka.

    python -m generator.cell_test_producer --rate 50 --duration 900
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import Callable, Protocol

from config.settings import GeneratorConfig, Settings, get_settings
from generator.simulator import Simulator

log = logging.getLogger("generator")
TICK_S = 0.05
REPORT_EVERY_S = 10.0


class Sink(Protocol):
    def send(self, topic: str, key: str, value: dict) -> None: ...
    def flush(self) -> None: ...


class KafkaSink:
    def __init__(self, bootstrap_servers: str):
        from confluent_kafka import Producer  # imported lazily so tests need no broker client

        self._producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 20,
            "compression.type": "lz4",
        })

    def send(self, topic: str, key: str, value: dict) -> None:
        payload = json.dumps(value, separators=(",", ":")).encode()
        while True:
            try:
                self._producer.produce(topic, key=key.encode(), value=payload)
                break
            except BufferError:
                self._producer.poll(0.1)
        self._producer.poll(0)

    def flush(self) -> None:
        self._producer.flush(30)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--rate", type=float, help="cell tests per second (default from GENERATOR_RATE)")
    p.add_argument("--duration", type=float, help="stop after this many seconds (default: run until Ctrl+C)")
    p.add_argument("--seed", type=int, help="random seed (default from GENERATOR_SEED)")
    p.add_argument("--no-faults", action="store_true", help="disable all fault injection")
    return p.parse_args(argv)


def apply_cli(cfg: GeneratorConfig, args: argparse.Namespace) -> GeneratorConfig:
    if args.rate is not None:
        cfg = replace(cfg, rate=args.rate)
    if args.seed is not None:
        cfg = replace(cfg, seed=args.seed)
    if args.no_faults:
        cfg = replace(cfg, sensor_dropout_rate=0.0, duplicate_rate=0.0, late_rate=0.0, rework_rate=0.0,
                      drift_start_after_s=1e9, bad_lot_index=-1)
    return cfg


def run(settings: Settings, sink: Sink, duration_s: float | None = None,
        clock: Callable[[], float] = time.monotonic, sleep: Callable[[float], None] = time.sleep,
        start_wall: datetime | None = None, cfg: GeneratorConfig | None = None) -> dict:
    """Emit cells at cfg.rate until duration_s elapses, then flush held late/duplicate messages."""
    cfg = cfg or settings.generator
    topics = {"cell_tests": settings.topics.cell_tests, "assembly_events": settings.topics.assembly_events,
              "fault_log": settings.topics.fault_log}
    start_wall = start_wall or datetime.now(timezone.utc)
    sim = Simulator(cfg, start=start_wall)
    t0 = last_report = clock()
    stats = {"cells": 0, "messages": 0}

    def ship(messages):
        for m in messages:
            sink.send(topics[m.topic], m.key, m.value)
        stats["messages"] += len(messages)

    log.info("run %s started, tag %s, %.0f cells/s, drift on %s after %.0fs",
             sim.run_id, sim.run_tag, cfg.rate, cfg.drift_station, cfg.drift_start_after_s)
    try:
        while duration_s is None or clock() - t0 < duration_s:
            elapsed = clock() - t0
            now = start_wall + timedelta(seconds=elapsed)
            ship(sim.release_due(now))
            for _ in range(int(elapsed * cfg.rate) - stats["cells"]):
                ship(sim.emit_cell(now))
                stats["cells"] += 1
            if clock() - last_report >= REPORT_EVERY_S:
                last_report = clock()
                log.info("%d cells, %d messages, %.1f cells/s", stats["cells"], stats["messages"],
                         stats["cells"] / max(elapsed, 1e-9))
            sleep(TICK_S)
    except KeyboardInterrupt:
        log.info("interrupted")
    finally:
        ship(sim.flush())
        sink.flush()
    log.info("done: %d cells, %d messages", stats["cells"], stats["messages"])
    return stats


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = parse_args(argv)
    settings = get_settings()
    cfg = apply_cli(settings.generator, args)
    run(settings, KafkaSink(settings.kafka_bootstrap_servers), duration_s=args.duration, cfg=cfg)


if __name__ == "__main__":
    main()
