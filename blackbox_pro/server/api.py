from __future__ import annotations

import os
from typing import Any

from fastapi import APIRouter, HTTPException

from blackbox.store import Store
from blackbox.seal import verify_chain_with_payloads

router = APIRouter()


def get_store() -> Store:
    # Use the same store layout as blackbox CLI
    root = os.environ.get("BLACKBOX_PRO_ROOT", "./.blackbox_store")
    return Store.local(root)


@router.get("/runs")
def list_runs(project: str, dataset: str) -> dict[str, Any]:
    store = get_store()
    base = f"{project}/{dataset}".rstrip("/")

    # store.list returns file keys; infer run_ids from first segment after base/
    try:
        keys = store.list(base)
    except Exception:
        keys = store.list(f"{base}/")

    run_ids: set[str] = set()
    for k in keys:
        k = str(k).lstrip("/")
        if not k.startswith(base + "/"):
            continue
        rest = k[len(base) + 1 :]
        if not rest:
            continue
        rid = rest.split("/", 1)[0]
        if rid:
            run_ids.add(rid)

    return {"ok": True, "project": project, "dataset": dataset, "runs": sorted(run_ids)}


@router.get("/verify")
def verify_run(project: str, dataset: str, run_id: str) -> dict[str, Any]:
    store = get_store()
    prefix = f"{project}/{dataset}/{run_id}"

    try:
        chain_obj = store.get_json(f"{prefix}/chain.json")
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Run not found")

    ok, msg = verify_chain_with_payloads(chain_obj, store, run_prefix=prefix)

    return {
        "ok": bool(ok),
        "message": msg,
        "project": project,
        "dataset": dataset,
        "run_id": run_id,
        "chain_entries": len(chain_obj.get("entries", [])),
        "chain_head": chain_obj.get("head"),
    }


@router.get("/report")
def report_run(project: str, dataset: str, run_id: str) -> dict[str, Any]:
    store = get_store()
    prefix = f"{project}/{dataset}/{run_id}"

    try:
        run_obj = store.get_json(f"{prefix}/run.json")
        chain_obj = store.get_json(f"{prefix}/chain.json")
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Run not found")

    ok, msg = verify_chain_with_payloads(chain_obj, store, run_prefix=prefix)

    return {
        "ok": bool(ok),
        "verify_message": msg,
        "project": project,
        "dataset": dataset,
        "run_id": run_id,
        "run": run_obj,
        "chain": {"entries": len(chain_obj.get("entries", [])), "head": chain_obj.get("head")},
    }


def report_verbose_impl(project: str, dataset: str, run_id: str, show_keys: str, max_keys: int) -> dict[str, Any]:
    """
    Shared implementation used by /report_verbose and the HTML UI.
    """
    store = get_store()
    prefix = f"{project}/{dataset}/{run_id}"

    try:
        run_obj = store.get_json(f"{prefix}/run.json")
        chain_obj = store.get_json(f"{prefix}/chain.json")
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Run not found")

    ok, msg = verify_chain_with_payloads(chain_obj, store, run_prefix=prefix)

    verbose_steps: list[dict[str, Any]] = []
    for s in (run_obj.get("steps") or []):
        step_path = s.get("path")
        if not step_path:
            continue
        try:
            step_obj = store.get_json(f"{prefix}/{step_path}")
        except FileNotFoundError:
            # If a step json is missing, skip it
            continue

        # Optional: if diff_payload contains large key lists, keep only head/tail
        dp = step_obj.get("diff_payload")
        if isinstance(dp, dict) and show_keys == "head":
            for keyset in ("added_keys", "removed_keys", "changed_keys"):
                ks = dp.get(keyset)
                if isinstance(ks, dict):
                    items = ks.get("items")
                    if isinstance(items, list) and len(items) > max_keys:
                        ks["items"] = items[:max_keys]
                        ks["truncated"] = True

        verbose_steps.append(step_obj)

    return {
        "ok": bool(ok),
        "verify_message": msg,
        "project": project,
        "dataset": dataset,
        "run_id": run_id,
        "run": run_obj,
        "chain": {"entries": len(chain_obj.get("entries", [])), "head": chain_obj.get("head")},
        "verbose_steps": verbose_steps,
        "notes": {"show_keys": show_keys, "max_keys": max_keys},
    }


@router.get("/report_verbose")
def report_verbose(project: str, dataset: str, run_id: str, show_keys: str = "head", max_keys: int = 10) -> dict[str, Any]:
    return report_verbose_impl(project, dataset, run_id, show_keys, max_keys)

