from __future__ import annotations
import json
import os
import platform
import socket
import sys
from datetime import datetime, timezone
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def canonical_json_bytes(obj: Any) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def get_runtime_info() -> dict[str, Any]:
    return {
        "python": f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
        "platform": platform.platform(),
    }


def get_host_info() -> dict[str, Any]:
    return {
        "hostname": socket.gethostname(),
        "os": os.name,
        "arch": platform.machine(),
    }


def safe_path_component(value: str, *, max_len: int = 64) -> str:
    """
    Normalize user-provided identifiers for safe filesystem usage.
    """
    if not isinstance(value, str):
        value = str(value)
    cleaned = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in value)
    return cleaned[:max_len]
