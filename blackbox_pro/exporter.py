from __future__ import annotations

import hashlib
import json
import os
import zipfile
from typing import Iterable, Tuple

from blackbox.store import Store
from blackbox.util import utc_now_iso


def _list_run_keys(store: Store, prefix: str) -> list[str]:
    keys = store.list(prefix.rstrip("/"))
    out: list[str] = []
    for k in keys:
        k = str(k).lstrip("/")
        if k.startswith(prefix.rstrip("/") + "/"):
            out.append(k)
    out.sort()
    return out


def _resolve_run_location(store: Store, run_id: str, project: str | None, dataset: str | None) -> Tuple[str, str]:
    if project and dataset:
        return project, dataset

    # Try to find a unique run_id across store
    projects = store.list_dirs("")
    hits: list[Tuple[str, str]] = []
    for proj in projects:
        datasets = store.list_dirs(proj)
        for ds in datasets:
            runs = store.list_dirs(f"{proj}/{ds}")
            if run_id in runs:
                hits.append((proj, ds))
    if len(hits) == 1:
        return hits[0]
    if not hits:
        raise FileNotFoundError("Run not found")
    raise ValueError("Run ID is not unique; provide --project and --dataset")


def export_run_bundle(
    *,
    store: Store,
    project: str | None,
    dataset: str | None,
    run_id: str,
    out_path: str,
) -> None:
    project, dataset = _resolve_run_location(store, run_id, project, dataset)
    prefix = f"{project}/{dataset}/{run_id}"
    keys = _list_run_keys(store, prefix)
    if not keys:
        raise FileNotFoundError("Run not found")

    manifest: dict[str, str] = {}
    meta = {
        "project": project,
        "dataset": dataset,
        "run_id": run_id,
        "exported_at": utc_now_iso(),
    }

    with zipfile.ZipFile(out_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for key in keys:
            rel = key[len(prefix) + 1 :]
            data = store.get_bytes(key)
            zf.writestr(rel, data)
            manifest[rel] = hashlib.sha256(data).hexdigest()

        meta_bytes = json.dumps(meta, ensure_ascii=False, indent=2).encode("utf-8")
        manifest_bytes = json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8")
        zf.writestr("meta.json", meta_bytes)
        zf.writestr("manifest.json", manifest_bytes)
