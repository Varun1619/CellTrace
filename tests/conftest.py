import pytest

from config.settings import load_settings


def duckdb_delta_available() -> bool:
    try:
        from scripts.lakehouse import connect

        connect().close()
        return True
    except Exception:
        return False


requires_duckdb_delta = pytest.mark.skipif(
    not duckdb_delta_available(),
    reason="DuckDB delta extension unavailable (not built for Windows); runs in Docker",
)


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
