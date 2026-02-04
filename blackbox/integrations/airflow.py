from __future__ import annotations

from typing import Any

from blackbox import Recorder, Run


def airflow_tags(context: dict[str, Any]) -> dict[str, str]:
    return {
        "airflow_dag_id": str(context.get("dag").dag_id) if context.get("dag") else "",
        "airflow_task_id": str(context.get("task").task_id) if context.get("task") else "",
        "airflow_run_id": str(context.get("run_id") or ""),
        "airflow_execution_date": str(context.get("execution_date") or ""),
    }


def start_airflow_run(rec: Recorder, context: dict[str, Any], *, project: str, dataset: str) -> Run:
    tags = {"engine": "airflow", **airflow_tags(context)}
    return rec.start_run(tags=tags)
