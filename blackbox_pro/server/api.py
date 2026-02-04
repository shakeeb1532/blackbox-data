from __future__ import annotations

import os
import logging
import json
import hmac
import hashlib
import subprocess  # nosec B404 - local gpg integration
import tempfile
import shutil
from typing import Any

from fastapi import APIRouter, HTTPException, Request, UploadFile, File
from pydantic import constr
from fastapi.responses import Response

from blackbox.store import Store
from blackbox.seal import verify_chain_with_payloads
from blackbox.util import utc_now_iso
from blackbox_pro.server.auth import require_role, require_project_access
from blackbox_pro.server.audit import export_siem, read_audit_events
from blackbox_pro.server.stats import compute_stats

router = APIRouter()
logger = logging.getLogger("blackbox-pro")

NameType = constr(min_length=1, pattern=r"^[A-Za-z0-9._-]+$")


def _resolve_executable(name: str) -> str:
    path = shutil.which(name)
    if not path:
        raise FileNotFoundError(f"Executable not found: {name}")
    return path


def get_store() -> Store:
    root = os.environ.get("BLACKBOX_PRO_ROOT", "./.blackbox_store")
    return Store.local(root)


def _validation_detail(field: str, msg: str) -> list[dict[str, Any]]:
    return [{"loc": ["query", field], "msg": msg, "type": "value_error"}]


def _sanitize_component(value: str, *, field: str) -> str:
    s = (value or "").strip()
    if not s:
        raise HTTPException(status_code=422, detail=_validation_detail(field, "is required"))
    try:
        s.encode("utf-8", errors="strict")
    except Exception:
        raise HTTPException(status_code=422, detail=_validation_detail(field, "is invalid"))
    return s


@router.get("/runs")
def list_runs(request: Request, project: NameType, dataset: NameType) -> dict[str, Any]:
    project = _sanitize_component(project, field="project")
    dataset = _sanitize_component(dataset, field="dataset")
    require_project_access(request, project)
    store = get_store()
    base = f"{project}/{dataset}".rstrip("/")

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
def verify_run(request: Request, project: NameType, dataset: NameType, run_id: NameType) -> dict[str, Any]:
    project = _sanitize_component(project, field="project")
    dataset = _sanitize_component(dataset, field="dataset")
    run_id = _sanitize_component(run_id, field="run_id")
    require_project_access(request, project)
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
        "verification_evidence": {
            "verified_at": utc_now_iso(),
            "chain_entries": len(chain_obj.get("entries", [])),
            "chain_head": chain_obj.get("head"),
        },
        "chain_entries": len(chain_obj.get("entries", [])),
        "chain_head": chain_obj.get("head"),
    }


@router.get("/report")
def report_run(request: Request, project: NameType, dataset: NameType, run_id: NameType) -> dict[str, Any]:
    project = _sanitize_component(project, field="project")
    dataset = _sanitize_component(dataset, field="dataset")
    run_id = _sanitize_component(run_id, field="run_id")
    require_project_access(request, project)
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


@router.get("/report_verbose")
def report_verbose(
    request: Request,
    project: NameType,
    dataset: NameType,
    run_id: NameType,
    show_keys: str = "head",
    max_keys: int = 10,
) -> dict[str, Any]:
    project = _sanitize_component(project, field="project")
    dataset = _sanitize_component(dataset, field="dataset")
    run_id = _sanitize_component(run_id, field="run_id")
    require_project_access(request, project)
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
            continue
        verbose_steps.append(step_obj)

    return {
        "ok": bool(ok),
        "verify_message": msg,
        "project": project,
        "dataset": dataset,
        "run_id": run_id,
        "run": run_obj,
        "steps": verbose_steps,
        "chain": {"entries": len(chain_obj.get("entries", [])), "head": chain_obj.get("head")},
        "show_keys": show_keys,
        "max_keys": max_keys,
    }


@router.get("/audit")
def audit_log(request: Request) -> dict[str, Any]:
    require_role(request, {"admin"})
    return {"events": read_audit_events()}


@router.get("/siem")
def siem_export(request: Request) -> Response:
    require_role(request, {"admin"})
    payload = export_siem()
    return Response(content=payload, media_type="application/jsonl")


@router.get("/stats")
def stats(request: Request) -> dict[str, Any]:
    require_role(request, {"admin", "viewer"})
    store = get_store()
    max_runs = os.environ.get("BLACKBOX_PRO_STATS_MAX_RUNS")
    max_runs_int = int(max_runs) if max_runs and max_runs.isdigit() else None
    return compute_stats(store, max_runs=max_runs_int)


@router.get("/evidence")
def evidence_bundle(request: Request, project: NameType, dataset: NameType, run_id: NameType) -> Response:
    project = _sanitize_component(project, field="project")
    dataset = _sanitize_component(dataset, field="dataset")
    run_id = _sanitize_component(run_id, field="run_id")
    require_role(request, {"admin"})
    require_project_access(request, project)
    store = get_store()
    prefix = f"{project}/{dataset}/{run_id}"
    try:
        run_obj = store.get_json(f"{prefix}/run.json")
        chain_obj = store.get_json(f"{prefix}/chain.json")
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Run not found")

    ok, msg = verify_chain_with_payloads(chain_obj, store, run_prefix=prefix)
    verification = {
        "verified_at": utc_now_iso(),
        "ok": bool(ok),
        "message": msg,
        "chain_entries": len(chain_obj.get("entries", [])),
        "chain_head": chain_obj.get("head"),
    }

    buf = tempfile.SpooledTemporaryFile()
    run_bytes = json.dumps(run_obj, ensure_ascii=False, indent=2).encode("utf-8")
    chain_bytes = json.dumps(chain_obj, ensure_ascii=False, indent=2).encode("utf-8")
    verification_bytes = json.dumps(verification, ensure_ascii=False, indent=2).encode("utf-8")
    meta_bytes = json.dumps({"project": project, "dataset": dataset, "run_id": run_id}, ensure_ascii=False, indent=2).encode("utf-8")

    manifest = {
        "run.json": hashlib.sha256(run_bytes).hexdigest(),
        "chain.json": hashlib.sha256(chain_bytes).hexdigest(),
        "verification.json": hashlib.sha256(verification_bytes).hexdigest(),
        "meta.json": hashlib.sha256(meta_bytes).hexdigest(),
    }
    manifest_bytes = json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8")

    signature = None
    key = os.environ.get("BLACKBOX_PRO_EVIDENCE_HMAC_KEY")
    if key:
        signature = hmac.new(key.encode("utf-8"), manifest_bytes, hashlib.sha256).hexdigest()
    pgp_sig = None
    gpg_key = os.environ.get("BLACKBOX_PRO_GPG_KEY_ID")
    if gpg_key:
        try:
            with tempfile.NamedTemporaryFile("wb", delete=False) as tmp:
                tmp.write(manifest_bytes)
                tmp_path = tmp.name
            gpg_path = _resolve_executable("gpg")
            cmd = [gpg_path, "--batch", "--yes", "--armor", "--local-user", gpg_key, "--detach-sign", tmp_path]
            subprocess.check_call(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)  # nosec B603
            asc_path = tmp_path + ".asc"
            with open(asc_path, "rb") as f:
                pgp_sig = f.read()
            try:
                os.remove(tmp_path)
                os.remove(asc_path)
            except Exception:
                pass
        except Exception:
            pgp_sig = None

    import zipfile
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("run.json", run_bytes)
        zf.writestr("chain.json", chain_bytes)
        zf.writestr("verification.json", verification_bytes)
        zf.writestr("meta.json", meta_bytes)
        zf.writestr("manifest.json", manifest_bytes)
        if signature:
            sig_obj = {"algo": "HMAC-SHA256", "signature": signature}
            zf.writestr("signature.json", json.dumps(sig_obj, ensure_ascii=False, indent=2))
        if pgp_sig:
            zf.writestr("manifest.json.asc", pgp_sig)
    buf.seek(0)
    return Response(
        content=buf.read(),
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename=evidence_{run_id}.zip"},
    )


@router.post("/evidence/verify")
def verify_evidence_bundle(request: Request, file: bytes = File(..., min_length=1)):
    require_role(request, {"admin"})
    data = file
    import zipfile
    import io as _io

    try:
        zf = zipfile.ZipFile(_io.BytesIO(data), "r")
    except Exception:
        raise HTTPException(status_code=400, detail="invalid_zip")

    try:
        manifest = json.loads(zf.read("manifest.json"))
    except Exception:
        raise HTTPException(status_code=400, detail="manifest_missing_or_invalid")

    for name, expected in manifest.items():
        if name not in zf.namelist():
            raise HTTPException(status_code=400, detail=f"missing_{name}")
        with zf.open(name, "r") as f:
            actual = hashlib.sha256(f.read()).hexdigest()
        if actual != expected:
            raise HTTPException(status_code=400, detail=f"hash_mismatch_{name}")

    result: dict[str, Any] = {"ok": True, "hashes": "ok"}

    if "signature.json" in zf.namelist():
        key = os.environ.get("BLACKBOX_PRO_EVIDENCE_HMAC_KEY")
        if not key:
            result["hmac"] = "missing_key"
        else:
            sig_obj = json.loads(zf.read("signature.json"))
            sig = sig_obj.get("signature")
            calc = hmac.new(key.encode("utf-8"), zf.read("manifest.json"), hashlib.sha256).hexdigest()
            if calc != sig:
                raise HTTPException(status_code=400, detail="hmac_signature_mismatch")
            result["hmac"] = "ok"

    if "manifest.json.asc" in zf.namelist():
        if os.environ.get("BLACKBOX_PRO_GPG_VERIFY") == "1":
            try:
                import tempfile
                with tempfile.NamedTemporaryFile("wb", delete=False) as m:
                    m.write(zf.read("manifest.json"))
                    m_path = m.name
                with tempfile.NamedTemporaryFile("wb", delete=False) as s:
                    s.write(zf.read("manifest.json.asc"))
                    s_path = s.name
                gpg_path = _resolve_executable("gpg")
                subprocess.check_call([gpg_path, "--verify", s_path, m_path], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)  # nosec B603
                result["pgp"] = "ok"
            except Exception:
                raise HTTPException(status_code=400, detail="pgp_signature_verify_failed")
            finally:
                try:
                    os.remove(m_path)
                    os.remove(s_path)
                except Exception:
                    pass
        else:
            result["pgp"] = "present_not_verified"

    return result
