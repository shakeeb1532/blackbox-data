from __future__ import annotations

import datetime as _dt
import os
from typing import Any

from blackbox.store import Store, LocalStore
from blackbox.seal import verify_chain_with_payloads


def _parse_dt(ts: str) -> _dt.datetime | None:
    try:
        return _dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None


def compute_stats(store: Store, *, max_runs: int | None = None) -> dict[str, Any]:
    projects = store.list_dirs("")
    run_count = 0
    runs_per_day: dict[str, int] = {}
    verify_ok = 0
    verify_fail = 0
    latency_ms: list[float] = []
    churn_by_dataset: dict[str, list[float]] = {}
    size_mb_total = 0.0

    for project in projects:
        datasets = store.list_dirs(project)
        for dataset in datasets:
            runs = store.list_dirs(f"{project}/{dataset}")
            for run_id in runs:
                if max_runs and run_count >= max_runs:
                    break
                prefix = f"{project}/{dataset}/{run_id}"
                try:
                    run_obj = store.get_json(f"{prefix}/run.json")
                    chain_obj = store.get_json(f"{prefix}/chain.json")
                except Exception:
                    continue

                run_count += 1
                created_at = run_obj.get("created_at")
                finished_at = run_obj.get("finished_at")
                if created_at:
                    day = created_at.split("T")[0]
                    runs_per_day[day] = runs_per_day.get(day, 0) + 1

                if created_at and finished_at:
                    d1 = _parse_dt(created_at)
                    d2 = _parse_dt(finished_at)
                    if d1 and d2:
                        latency_ms.append((d2 - d1).total_seconds() * 1000.0)

                ok, _ = verify_chain_with_payloads(chain_obj, store, run_prefix=prefix)
                if ok:
                    verify_ok += 1
                else:
                    verify_fail += 1

                steps = run_obj.get("steps") or []
                churn_vals: list[float] = []
                for st in steps:
                    path = st.get("path")
                    if not path:
                        continue
                    try:
                        step_obj = store.get_json(f"{prefix}/{path}")
                    except Exception:
                        continue
                    diff = step_obj.get("diff") or {}
                    summary = diff.get("summary") or {}
                    added = float(summary.get("added") or 0)
                    removed = float(summary.get("removed") or 0)
                    inp = (step_obj.get("input") or {}).get("n_rows") or 0
                    denom = max(float(inp) or 1.0, 1.0)
                    churn_vals.append((added + removed) / denom)
                if churn_vals:
                    key = f"{project}/{dataset}"
                    churn_by_dataset.setdefault(key, []).append(sum(churn_vals) / len(churn_vals))

                if isinstance(store, LocalStore):
                    try:
                        total_bytes = 0
                        for key in store.list(prefix):
                            path = store._path(key)
                            total_bytes += os.path.getsize(path)
                        size_mb_total += total_bytes / (1024 * 1024)
                    except Exception:
                        pass

    avg_latency = sum(latency_ms) / len(latency_ms) if latency_ms else 0.0
    top_churn = sorted(
        ((k, sum(v) / len(v)) for k, v in churn_by_dataset.items()),
        key=lambda x: x[1],
        reverse=True,
    )[:5]

    return {
        "runs_total": run_count,
        "runs_per_day": runs_per_day,
        "verify_pass_rate": (verify_ok / max(1, verify_ok + verify_fail)),
        "verify_ok": verify_ok,
        "verify_fail": verify_fail,
        "avg_latency_ms": round(avg_latency, 2),
        "top_datasets_by_churn": top_churn,
        "storage_mb_total": round(size_mb_total, 2),
    }
