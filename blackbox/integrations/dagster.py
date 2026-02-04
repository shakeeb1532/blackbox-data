from __future__ import annotations

from typing import Any

from blackbox import Recorder, Run


def dagster_tags(context: Any) -> dict[str, str]:
    return {
        "dagster_job": getattr(context, "job_name", "") or "",
        "dagster_run_id": getattr(context, "run_id", "") or "",
        "dagster_op": getattr(context, "op", None).name if getattr(context, "op", None) else "",
    }


def start_dagster_run(rec: Recorder, context: Any, *, project: str, dataset: str) -> Run:
    tags = {"engine": "dagster", **dagster_tags(context)}
    return rec.start_run(tags=tags)
