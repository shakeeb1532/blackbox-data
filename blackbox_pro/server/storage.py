from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any


@dataclass
class LocalProStore:
    """
    Pro MVP store: local filesystem layout

      <root>/<project>/<dataset>/<run_id>/
        run.json
        chain.json (optional)

    This is intentionally simple for v0.0.1.
    """
    root: str

    def _base(self, project: str, dataset: str, run_id: str | None = None) -> str:
        base = os.path.join(self.root, project, dataset)
        return os.path.join(base, run_id) if run_id else base

    def put_run_meta(
        self,
        *,
        project: str,
        dataset: str,
        run_id: str,
        run_json_bytes: bytes,
        chain_json_bytes: bytes | None,
    ) -> None:
        base = self._base(project, dataset, run_id)
        os.makedirs(base, exist_ok=True)

        with open(os.path.join(base, "run.json"), "wb") as f:
            f.write(run_json_bytes)

        if chain_json_bytes is not None:
            with open(os.path.join(base, "chain.json"), "wb") as f:
                f.write(chain_json_bytes)

    def get_run_meta(self, *, project: str, dataset: str, run_id: str) -> dict[str, Any] | None:
        base = self._base(project, dataset, run_id)
        run_path = os.path.join(base, "run.json")
        if not os.path.exists(run_path):
            return None

        with open(run_path, "rb") as f:
            run_obj = json.loads(f.read().decode("utf-8"))

        chain_path = os.path.join(base, "chain.json")
        chain_head = None
        if os.path.exists(chain_path):
            with open(chain_path, "rb") as f:
                chain_obj = json.loads(f.read().decode("utf-8"))
            chain_head = chain_obj.get("head")

        return {"run": run_obj, "chain_head": chain_head}

    def list_runs(self, *, project: str, dataset: str) -> list[str]:
        base = self._base(project, dataset)
        if not os.path.exists(base):
            return []
        out: list[str] = []
        for name in os.listdir(base):
            p = os.path.join(base, name)
            if os.path.isdir(p):
                out.append(name)
        out.sort()
        return out

