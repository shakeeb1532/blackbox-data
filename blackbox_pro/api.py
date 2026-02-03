from __future__ import annotations

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Any

from blackbox.store import Store
from blackbox.seal import verify_chain_with_payloads


def _run_prefix(project: str, dataset: str, run_id: str) -> str:
    return f"{project}/{dataset}/{run_id}"


def _load_json(store: Store, key: str) -> dict[str, Any]:
    return store.get_json(key)


def _join(prefix: str, rel: str) -> str:
    rel = rel.lstrip("/")
    return f"{prefix}/{rel}"


def _infer_run_ids(store: Store, base: str) -> list[str]:
    base = base.rstrip("/")
    keys = store.list(base) or store.list(f"{base}/")
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

    return sorted(run_ids)


def _infer_step_dir_from_path(path: str) -> str:
    parts = path.replace("\\", "/").split("/")
    if len(parts) >= 2:
        return "/".join(parts[:-1])
    return ""


def _load_step(store: Store, prefix: str, st: dict[str, Any]) -> dict[str, Any] | None:
    # inline step payload
    if any(k in st for k in ("code", "input", "output", "diff", "schema_diff")) and "path" not in st:
        return st

    path = st.get("path")
    if not path:
        return None

    try:
        return store.get_json(_join(prefix, path))
    except FileNotFoundError:
        return None


def _load_diff_payload(store: Store, prefix: str, step_obj: dict[str, Any], step_path: str | None) -> dict[str, Any] | None:
    diff = step_obj.get("diff")
    if not isinstance(diff, dict):
        return None

    artifact = diff.get("artifact")
    if not isinstance(artifact, str) or not artifact:
        return None

    candidates: list[str] = []
    if step_path:
        step_dir = _infer_step_dir_from_path(step_path)
        candidates.append(_join(prefix, f"{step_dir}/{artifact}"))
    candidates.append(_join(prefix, artifact))

    for k in candidates:
        try:
            return store.get_json(k)
        except FileNotFoundError:
            continue
    return None


class ProConfig(BaseModel):
    root: str = ".blackbox_store"


def create_app(config: ProConfig) -> FastAPI:
    app = FastAPI(title="Blackbox Data Pro", version="0.1.0")

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {"ok": True, "service": "blackbox-pro", "version": "0.1.0"}

    @app.get("/runs/{project}/{dataset}")
    def list_runs(project: str, dataset: str) -> dict[str, Any]:
        store = Store.local(config.root)
        base = f"{project}/{dataset}"
        return {"ok": True, "root": config.root, "project": project, "dataset": dataset, "run_ids": _infer_run_ids(store, base)}

    @app.get("/report/{project}/{dataset}/{run_id}")
    def report(project: str, dataset: str, run_id: str, verbose: bool = False) -> dict[str, Any]:
        store = Store.local(config.root)
        prefix = _run_prefix(project, dataset, run_id)

        try:
            run_obj = _load_json(store, f"{prefix}/run.json")
            chain_obj = _load_json(store, f"{prefix}/chain.json")
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail={"ok": False, "error": "run_not_found", "prefix": prefix})

        ok, msg = verify_chain_with_payloads(chain_obj, store, run_prefix=prefix)

        steps_idx = run_obj.get("steps", [])
        steps: list[dict[str, Any]] = []
        for st in steps_idx:
            if not isinstance(st, dict):
                continue
            step_path = st.get("path")
            step_obj = _load_step(store, prefix, st) or {"status": "missing_step_json", "name": st.get("name"), "ordinal": st.get("ordinal")}
            if verbose and isinstance(step_obj, dict):
                dp = _load_diff_payload(store, prefix, step_obj, step_path)
                if isinstance(dp, dict):
                    step_obj = dict(step_obj)
                    step_obj["diff_payload"] = dp
            steps.append(step_obj)

        return {
            "ok": bool(ok),
            "verify_message": msg,
            "root": config.root,
            "prefix": prefix,
            "project": project,
            "dataset": dataset,
            "run_id": run_id,
            "run": run_obj,
            "chain": {"entries": len(chain_obj.get("entries", [])), "head": chain_obj.get("head")},
            "steps": steps,
        }

    return app

