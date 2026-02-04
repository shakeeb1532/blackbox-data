from __future__ import annotations

import json
import os
import hashlib
import time
import logging
from typing import Any

from blackbox.util import utc_now_iso

_logger = logging.getLogger("blackbox-pro")


def _audit_path() -> str:
    # Default to store root under .blackbox_store
    root = os.environ.get("BLACKBOX_PRO_ROOT", "./.blackbox_store")
    return os.environ.get("BLACKBOX_PRO_AUDIT_LOG", os.path.join(root, "_audit.log.jsonl"))


def _audit_rotate() -> None:
    """
    Rotate audit log if size exceeds limit or if older than retention days.
    Retention controlled by:
      BLACKBOX_PRO_AUDIT_ROTATE_MB (default 10)
      BLACKBOX_PRO_AUDIT_RETENTION_DAYS (default 30)
    """
    path = _audit_path()
    if not os.path.exists(path):
        return
    try:
        max_mb = float(os.environ.get("BLACKBOX_PRO_AUDIT_ROTATE_MB", "10"))
        retention_days = int(os.environ.get("BLACKBOX_PRO_AUDIT_RETENTION_DAYS", "30"))
    except Exception as e:
        _logger.debug("Invalid audit rotation config: %s", e)
        max_mb = 10.0
        retention_days = 30

    size_mb = os.path.getsize(path) / (1024 * 1024)
    age_days = (time.time() - os.path.getmtime(path)) / 86400.0
    if size_mb < max_mb and age_days < retention_days:
        return

    ts = time.strftime("%Y%m%d%H%M%S", time.gmtime())
    rotated = f"{path}.{ts}"
    try:
        os.rename(path, rotated)
    except Exception as e:
        _logger.debug("Audit log rotation failed: %s", e)
        return

    # cleanup older rotated logs
    if retention_days > 0:
        cutoff = time.time() - (retention_days * 86400.0)
        base = os.path.basename(path)
        dirn = os.path.dirname(path)
        for name in os.listdir(dirn or "."):
            if not name.startswith(base + "."):
                continue
            full = os.path.join(dirn, name)
            try:
                if os.path.getmtime(full) < cutoff:
                    os.remove(full)
            except Exception as e:
                _logger.debug("Audit log cleanup failed for %s: %s", full, e)


def _safe_json_load(line: str) -> dict[str, Any] | None:
    try:
        return json.loads(line)
    except Exception as e:
        _logger.debug("Skipping invalid audit line: %s", e)
        return None


def write_audit_event(event: dict[str, Any]) -> None:
    path = _audit_path()
    _audit_rotate()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    event.setdefault("ts", utc_now_iso())
    prev_hash = None
    if os.path.exists(path):
        try:
            with open(path, "rb") as f:
                # read last non-empty line for hash chaining
                lines = f.read().splitlines()
            if lines:
                last = lines[-1].decode("utf-8", errors="ignore")
                if last:
                    last_obj = json.loads(last)
                    prev_hash = last_obj.get("hash")
        except Exception as e:
            _logger.debug("Failed to read last audit hash: %s", e)
            prev_hash = None
    payload = dict(event)
    payload["prev_hash"] = prev_hash
    digest = hashlib.sha256((str(prev_hash) + json.dumps(event, ensure_ascii=False)).encode("utf-8")).hexdigest()
    payload["hash"] = digest
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def read_audit_events() -> list[dict[str, Any]]:
    path = _audit_path()
    if not os.path.exists(path):
        return []
    events: list[dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = _safe_json_load(line)
            if obj is not None:
                events.append(obj)
    return events


def _compute_audit_hash(event: dict[str, Any], prev_hash: str | None) -> str:
    return hashlib.sha256((str(prev_hash) + json.dumps(event, ensure_ascii=False)).encode("utf-8")).hexdigest()


def verify_audit_log(path: str | None = None) -> tuple[bool, int, str]:
    path = path or _audit_path()
    if not os.path.exists(path):
        return False, 0, "audit log not found"
    prev_hash = None
    count = 0
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                return False, count, "invalid json line"
            expected_prev = obj.get("prev_hash")
            if expected_prev != prev_hash:
                return False, count, "prev_hash mismatch"
            stored_hash = obj.get("hash")
            event = {k: v for k, v in obj.items() if k not in ("hash", "prev_hash")}
            calc_hash = _compute_audit_hash(event, prev_hash)
            if stored_hash != calc_hash:
                return False, count, "hash mismatch"
            prev_hash = stored_hash
            count += 1
    return True, count, "ok"


def _to_cef(event: dict[str, Any]) -> str:
    # Minimal CEF mapping for SIEM ingestion
    sig = event.get("event", "request")
    name = event.get("path", "blackbox")
    sev = 3 if event.get("status", 200) < 400 else 6
    ext = []
    for k in ("path", "method", "status", "role", "token_id", "ip", "user_agent", "duration_ms", "detail"):
        v = event.get(k)
        if v is None:
            continue
        ext.append(f"{k}={v}")
    return f"CEF:0|Blackbox|DataPro|1.0|{sig}|{name}|{sev}|{' '.join(ext)}"


def export_siem(format: str = "jsonl") -> str:
    events = read_audit_events()
    if format == "cef":
        return "\n".join(_to_cef(e) for e in events)
    return "\n".join(json.dumps(e, ensure_ascii=False) for e in events)
