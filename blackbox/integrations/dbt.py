from __future__ import annotations

import json
from typing import Any


def load_dbt_run_results(path: str) -> dict[str, Any]:
    """
    Load dbt run_results.json and return a small summary that can be stored
    as run metadata or tags.
    """
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    results = data.get("results") or []
    statuses = {}
    for r in results:
        status = r.get("status") or "unknown"
        statuses[status] = statuses.get(status, 0) + 1
    return {
        "dbt_status_counts": statuses,
        "dbt_execution_time": data.get("elapsed_time"),
        "dbt_generated_at": data.get("generated_at"),
    }
