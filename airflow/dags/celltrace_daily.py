"""Daily CellTrace batch: dbt build, then Delta OPTIMIZE + VACUUM on silver, then the daily report.

Each task shells out to the CellTrace virtualenv (dbt and delta-rs), keeping its
dependencies apart from Airflow's. Failures and SLA misses are logged and appended as
JSON lines to CELLTRACE_FAILURE_LOG for later review.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator

HOME = os.environ.get("CELLTRACE_HOME", "/opt/celltrace")
PYTHON = os.environ.get("CELLTRACE_PYTHON", "/opt/airflow/celltrace-venv/bin/python")
FAILURE_LOG = Path(os.environ.get("CELLTRACE_FAILURE_LOG", "/opt/airflow/logs/celltrace_failures.jsonl"))

log = logging.getLogger("celltrace_daily")


def _append(record: dict) -> None:
    FAILURE_LOG.parent.mkdir(parents=True, exist_ok=True)
    with FAILURE_LOG.open("a") as fh:
        fh.write(json.dumps(record) + "\n")


def failure_record(context: dict) -> dict:
    ti = context["task_instance"]
    return {
        "event": "task_failed",
        "dag_id": ti.dag_id,
        "task_id": ti.task_id,
        "run_id": context.get("run_id"),
        "try_number": ti.try_number,
        "logical_date": str(context.get("logical_date")),
        "exception": repr(context.get("exception")),
        "log_url": getattr(ti, "log_url", None),
        "logged_at": datetime.now(timezone.utc).isoformat(),
    }


def log_failure(context: dict) -> None:
    record = failure_record(context)
    log.error("celltrace task failed: %s", json.dumps(record))
    _append(record)


def log_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
    record = {
        "event": "sla_miss",
        "dag_id": dag.dag_id,
        "tasks": [str(s.task_id) for s in slas],
        "blocking": [str(t) for t in blocking_tis],
        "logged_at": datetime.now(timezone.utc).isoformat(),
    }
    log.warning("celltrace SLA missed: %s", json.dumps(record))
    _append(record)


def task_command(module_args: str) -> str:
    return f"cd {HOME} && {PYTHON} -m {module_args}"


default_args = {
    "owner": "celltrace",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=20),
    "sla": timedelta(minutes=30),
    "on_failure_callback": log_failure,
}

with DAG(
    dag_id="celltrace_daily",
    description="dbt build, Delta maintenance and the daily quality report",
    schedule="0 2 * * *",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    sla_miss_callback=log_sla_miss,
    tags=["celltrace"],
    doc_md=__doc__,
) as dag:
    dbt_build = BashOperator(task_id="dbt_build", bash_command=task_command("scripts.run_dbt build"))
    maintain_silver = BashOperator(
        task_id="optimize_vacuum_silver",
        bash_command=task_command("scripts.maintain_delta --layer silver --vacuum-retention-hours 168"),
    )
    publish_report = BashOperator(
        task_id="publish_daily_quality_report",
        bash_command=task_command("scripts.publish_daily_report --date {{ ds }}"),
    )

    dbt_build >> maintain_silver >> publish_report
