from datetime import datetime, timezone

from config.settings import load_settings
from generator.cell_test_producer import apply_cli, parse_args, run


class FakeClock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t

    def sleep(self, seconds):
        self.t += seconds


class ListSink:
    def __init__(self):
        self.sent = []
        self.flushed = False

    def send(self, topic, key, value):
        self.sent.append((topic, key, value))

    def flush(self):
        self.flushed = True


START = datetime(2026, 1, 5, 8, 0, tzinfo=timezone.utc)


def test_run_paces_to_rate_and_maps_topic_names():
    settings = load_settings(env={"TOPIC_CELL_TESTS": "ct_custom", "GENERATOR_RATE": "40"})
    clock, sink = FakeClock(), ListSink()
    stats = run(settings, sink, duration_s=10, clock=clock, sleep=clock.sleep, start_wall=START)
    tests = [s for s in sink.sent if s[0] == "ct_custom"]
    assert 390 <= stats["cells"] <= 410
    assert len(tests) >= stats["cells"]  # duplicates can only add
    assert {s[0] for s in sink.sent} <= {"ct_custom", "assembly_events", "fault_log"}
    assert sink.flushed


def test_event_timestamps_follow_the_clock():
    settings = load_settings(env={"GENERATOR_RATE": "10", "LATE_RATE": "0", "DUPLICATE_RATE": "0"})
    clock, sink = FakeClock(), ListSink()
    run(settings, sink, duration_s=5, clock=clock, sleep=clock.sleep, start_wall=START)
    ts = [v["ts"] for t, _, v in sink.sent if t == "cell_tests"]
    assert ts[0].startswith("2026-01-05T08:00:00")
    assert ts[-1] < "2026-01-05T08:00:05.1"


def test_cli_overrides_and_no_faults():
    settings = load_settings(env={})
    args = parse_args(["--rate", "120", "--seed", "7", "--no-faults", "--duration", "30"])
    cfg = apply_cli(settings.generator, args)
    assert cfg.rate == 120 and cfg.seed == 7
    assert cfg.sensor_dropout_rate == 0 and cfg.duplicate_rate == 0 and cfg.late_rate == 0
    assert cfg.drift_start_after_s > 1e8
    assert args.duration == 30
