from __future__ import annotations

import os
from typing import Dict


def collect_dbt_artifacts(root: str) -> Dict[str, bytes]:
    """
    Collect dbt artifacts (run_results.json, manifest.json) if present.
    Returns dict of filename -> bytes.
    """
    out: Dict[str, bytes] = {}
    target = os.path.join(root, "target")
    if not os.path.isdir(target):
        return out
    for name in ("run_results.json", "manifest.json"):
        path = os.path.join(target, name)
        if os.path.exists(path):
            with open(path, "rb") as f:
                out[name] = f.read()
    return out
