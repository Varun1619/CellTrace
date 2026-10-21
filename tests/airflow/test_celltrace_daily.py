"""DAG structure and callbacks. Runs inside the Airflow image: make test-airflow."""

import json
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

pytest.importorskip("airflow.operators.bash")

DAG_DIR = Path(__file__).resolve().parents[2] / "airflow" / "dags"


@pytest.fixture(scope="module")
def dag():
    from airflow.models import DagBag

    bag = DagBag(dag_folder=str(DAG_DIR), include_examples=False)
    assert bag.import_errors == {}
    return bag.get_dag("celltrace_daily")


def test_task_order(dag):
    assert [t.task_id for t in dag.topological_sort()] == [
        "dbt_build", "optimize_vacuum_silver", "publish_daily_quality_report"]


def test_retries_sla_and_callbacks_on_every_task(dag):
    for task in dag.tasks:
        assert task.retries == 2
        assert task.sla == timedelta(minutes=30)
        assert task.on_failure_callback is not None
    assert dag.sla_miss_callback is not None
    assert dag.catchup is False and dag.max_active_runs == 1


def test_report_task_publishes_the_logical_date(dag):
    assert "--date {{ ds }}" in dag.get_task("publish_daily_quality_report").bash_command


def test_failure_callback_appends_a_json_line(tmp_path, monkeypatch):
    import celltrace_daily

    monkeypatch.setattr(celltrace_daily, "FAILURE_LOG", tmp_path / "failures.jsonl")
    ti = SimpleNamespace(dag_id="celltrace_daily", task_id="dbt_build", try_number=3, log_url="http://x")
    celltrace_daily.log_failure({"task_instance": ti, "run_id": "manual__1", "exception": ValueError("boom")})
    record = json.loads((tmp_path / "failures.jsonl").read_text().strip())
    assert record["task_id"] == "dbt_build" and record["try_number"] == 3
    assert "boom" in record["exception"]
