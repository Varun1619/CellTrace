import pytest

from config.settings import load_settings


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    pytest.importorskip("pyspark")
    from streaming.spark_session import build_spark

    settings = load_settings(env={"LAKEHOUSE_ROOT": str(tmp_path_factory.mktemp("lake"))})
    session = build_spark("celltrace-tests", settings, local_test=True)
    yield session
    session.stop()


@pytest.fixture
def lake(tmp_path):
    """Settings pointing at an empty, per-test lakehouse."""
    return load_settings(env={"LAKEHOUSE_ROOT": str(tmp_path / "lake"), "STREAM_TRIGGER_SECONDS": "1"})
