from pathlib import Path

from config.settings import REPO_ROOT, load_settings


def test_defaults_without_env():
    s = load_settings(env={})
    assert s.kafka_bootstrap_servers == "localhost:9092"
    assert s.topics.cell_tests == "cell_tests"
    assert s.generator.rate == 50
    assert s.generator.cells_per_module == 46
    assert s.spc.baseline_windows == 5


def test_env_overrides_and_types():
    s = load_settings(env={"GENERATOR_RATE": "200", "DUPLICATE_RATE": "0.05", "KAFKA_BOOTSTRAP_SERVERS": "kafka:29092"})
    assert s.generator.rate == 200
    assert s.generator.duplicate_rate == 0.05
    assert s.kafka_bootstrap_servers == "kafka:29092"


def test_relative_lakehouse_root_resolves_against_repo():
    s = load_settings(env={"LAKEHOUSE_ROOT": "./lakehouse"})
    assert s.lakehouse_root == REPO_ROOT / "lakehouse"


def test_table_and_checkpoint_paths(tmp_path: Path):
    s = load_settings(env={"LAKEHOUSE_ROOT": str(tmp_path)})
    assert s.table_path("silver", "cell_tests") == (tmp_path / "silver" / "cell_tests").as_posix()
    assert s.checkpoint_path("bronze_cell_tests") == (tmp_path / "_checkpoints" / "bronze_cell_tests").as_posix()


def test_unknown_layer_rejected(tmp_path: Path):
    s = load_settings(env={"LAKEHOUSE_ROOT": str(tmp_path)})
    try:
        s.table_path("platinum", "x")
    except ValueError as exc:
        assert "platinum" in str(exc)
    else:
        raise AssertionError("expected ValueError")
