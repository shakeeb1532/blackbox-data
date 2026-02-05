from __future__ import annotations
from dataclasses import dataclass
from typing import Any
import traceback
import uuid
import logging
import os

import pandas as pd

from .config import DiffConfig, SnapshotConfig, SealConfig, RecorderConfig
from .store import Store, LocalStore
from .util import utc_now_iso, get_host_info, get_runtime_info, safe_path_component
from .hashing import schema_fingerprint, content_fingerprint_rowhash, diff_rowhash, schema_diff
from .seal import payload_digest, chain_digest, verify_chain_with_payloads

_logger = logging.getLogger("blackbox")


def _safe_name(s: str) -> str:
    return safe_path_component(s, max_len=64)


def _new_run_id() -> str:
    ts = utc_now_iso().replace("-", "").replace(":", "").replace(".", "")
    return f"run_{ts}_{uuid.uuid4().hex[:6]}"


def _rel_under(prefix: str, key: str) -> str:
    return key.split(f"{prefix}/")[-1] if key.startswith(f"{prefix}/") else key


@dataclass
class StepContext:
    run: "Run"
    name: str
    ordinal: int
    input_df: pd.DataFrame | None = None
    metadata: dict[str, Any] | None = None

    _started_at: str | None = None
    _output_df: pd.DataFrame | None = None

    def __enter__(self) -> "StepContext":
        self._started_at = utc_now_iso()
        self.run._current_step = self
        return self

    def capture_output(self, df: pd.DataFrame) -> None:
        self._output_df = df

    def add_metadata(self, **kwargs: Any) -> None:
        if self.metadata is None:
            self.metadata = {}
        self.metadata.update(kwargs)

    def __exit__(self, exc_type, exc, tb) -> bool:
        finished_at = utc_now_iso()
        status = "ok" if exc_type is None else "error"

        if self.run.recorder.config.enforce_explicit_output and exc_type is None and self._output_df is None:
            raise RuntimeError("Step finished without capture_output(df). v0.1 requires explicit output capture.")

        # Normalize dataframe-like inputs to pandas
        from .engines import to_pandas, is_dataframe_like
        if self.input_df is not None and is_dataframe_like(self.input_df):
            self.input_df = to_pandas(self.input_df)
        if self._output_df is not None and is_dataframe_like(self._output_df):
            self._output_df = to_pandas(self._output_df)

        step_key = self.run._step_prefix(self.ordinal, self.name)
        artifacts_prefix = f"{step_key}/artifacts"

        step_obj: dict[str, Any] = {
            "version": "0.1",
            "ordinal": self.ordinal,
            "name": self.name,
            "started_at": self._started_at,
            "finished_at": finished_at,
            "status": status,
            "metadata": self.metadata or {},
            "input": None,
            "output": None,
            "schema_diff": None,
            "diff": None,
            "code": self.run._code_hint(),
            "seal": None,
        }

        try:
            if self.input_df is not None:
                step_obj["input"] = self.run._maybe_write_df_artifact(
                    f"{artifacts_prefix}/input.bbdata", self.input_df
                )

            if self._output_df is not None:
                step_obj["output"] = self.run._maybe_write_df_artifact(
                    f"{artifacts_prefix}/output.bbdata", self._output_df
                )

            if (self.input_df is not None) and (self._output_df is not None):
                step_obj["schema_diff"] = schema_diff(self.input_df, self._output_df)

            if (
                self.run.recorder.diff.mode != "none"
                and (self.input_df is not None)
                and (self._output_df is not None)
            ):
                input_meta = step_obj.get("input") if isinstance(step_obj.get("input"), dict) else None
                output_meta = step_obj.get("output") if isinstance(step_obj.get("output"), dict) else None
                adaptive = bool(self.run.recorder.diff.adaptive)

                schema_same = False
                content_same = False
                if input_meta and output_meta:
                    schema_same = input_meta.get("schema_fp") == output_meta.get("schema_fp")
                    content_same = input_meta.get("content_fp") == output_meta.get("content_fp")

                diff_mode = self.run.recorder.diff.diff_mode

                if diff_mode == "schema":
                    step_obj["diff"] = {
                        "mode": "schema",
                        "status": "skipped",
                        "reason": "schema_only",
                        "ui_hint": "diff_schema_only",
                        "summary": {"added": 0, "removed": 0, "changed": 0},
                    }
                elif (
                    adaptive
                    and self.run.recorder.diff.skip_if_fingerprint_match
                    and schema_same
                    and content_same
                ):
                    step_obj["diff"] = {
                        "mode": "rowhash",
                        "status": "skipped",
                        "reason": "fingerprint_match",
                        "ui_hint": "diff_skipped_fingerprint_match",
                        "summary": {"added": 0, "removed": 0, "changed": 0},
                    }
                else:
                    total_keys_hint = None
                    if input_meta and output_meta:
                        try:
                            total_keys_hint = max(int(input_meta.get("n_rows") or 0), int(output_meta.get("n_rows") or 0))
                        except Exception:
                            total_keys_hint = None

                    diff_payload, summary = diff_rowhash(
                        self.input_df,
                        self._output_df,
                        order_sensitive=self.run.recorder.diff.order_sensitive,
                        sample_rows=self.run.recorder.diff.sample_rows,
                        primary_key=self.run.recorder.diff.primary_key,
                        summary_only_threshold=(
                            self.run.recorder.diff.summary_only_threshold if adaptive else None
                        ),
                        total_keys_hint=total_keys_hint,
                        diff_mode=diff_mode,
                        chunk_rows=self.run.recorder.diff.chunk_rows,
                        hash_group_size=self.run.recorder.diff.hash_group_size,
                        parallel_groups=self.run.recorder.diff.parallel_groups,
                        auto_parallel_wide=self.run.recorder.diff.auto_parallel_wide,
                        auto_parallel_threshold_cols=self.run.recorder.diff.auto_parallel_threshold_cols,
                        auto_parallel_workers=self.run.recorder.diff.auto_parallel_workers,
                        auto_hash_group_size=self.run.recorder.diff.auto_hash_group_size,
                        cache_rowhash=self.run.recorder.diff.cache_rowhash,
                    )
                    diff_ref = f"{artifacts_prefix}/diff.bbdelta"
                    self.run.store.put_json(diff_ref, diff_payload)
                    step_obj["diff"] = {
                        "mode": diff_mode,
                        "artifact": "artifacts/diff.bbdelta",
                        "summary": {"added": summary.added, "removed": summary.removed, "changed": summary.changed},
                        "summary_only": bool(diff_payload.get("summary_only")),
                        "ui_hint": diff_payload.get("ui_hint"),
                    }

                    # Expose the diff payload inline for CLI report (optional but helpful).
                    step_obj["diff_payload"] = diff_payload
                    step_obj["diff_summary"] = {"added": summary.added, "removed": summary.removed, "changed": summary.changed}

            if exc_type is not None:
                step_obj["error"] = {
                    "type": str(exc_type.__name__),
                    "message": str(exc),
                    "traceback": "".join(traceback.format_exception(exc_type, exc, tb))[:20000],
                }

            step_json_key = f"{step_key}/step.json"
            for field_name in ("input", "output"):
                meta = step_obj.get(field_name)
                if isinstance(meta, dict) and "_pending_writes" in meta:
                    pending = meta.pop("_pending_writes")
                    self.run._register_pending_writes(step_json_key, field_name, pending)
            self.run.store.put_json(step_json_key, step_obj)

            if self.run.recorder.seal.mode == "chain":
                self.run._append_chain_entry(
                    typ="step",
                    ts=finished_at,
                    payload_ref=step_json_key,
                    payload_obj=step_obj,
                )

        finally:
            self.run._current_step = None

        return False


@dataclass
class Recorder:
    store: Store
    project: str
    dataset: str
    diff: DiffConfig = DiffConfig()
    snapshot: SnapshotConfig = SnapshotConfig()
    seal: SealConfig = SealConfig()
    config: RecorderConfig = RecorderConfig()

    def start_run(
        self,
        run_id: str | None = None,
        tags: dict[str, str] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> "Run":
        rid = run_id or _new_run_id()
        run = Run(
            recorder=self,
            store=self.store,
            run_id=rid,
            tags=tags or {},
            metadata=metadata or {},
        )
        run._init_run()
        return run

    def start_stream(
        self,
        run_id: str | None = None,
        tags: dict[str, str] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> "StreamRun":
        run = self.start_run(run_id=run_id, tags=tags, metadata=metadata)
        return StreamRun(run)


@dataclass
class Run:
    recorder: Recorder
    store: Store
    run_id: str
    tags: dict[str, str]
    metadata: dict[str, Any]

    _run_prefix: str | None = None
    _step_counter: int = 0
    _chain: dict[str, Any] | None = None
    _current_step: StepContext | None = None
    _events: list[dict[str, Any]] | None = None
    _pending_writes: list[dict[str, Any]] | None = None
    _snapshot_executor: Any | None = None

    def _prefix(self) -> str:
        if self._run_prefix is None:
            self._run_prefix = f"{_safe_name(self.recorder.project)}/{_safe_name(self.recorder.dataset)}/{self.run_id}"
        return self._run_prefix

    def _run_json_key(self) -> str:
        return f"{self._prefix()}/run.json"

    def _run_start_key(self) -> str:
        return f"{self._prefix()}/run_start.json"

    def _run_finish_key(self) -> str:
        return f"{self._prefix()}/run_finish.json"

    def _chain_key(self) -> str:
        return f"{self._prefix()}/chain.json"

    def _events_key(self) -> str:
        return f"{self._prefix()}/events.jsonl"

    def _step_prefix(self, ordinal: int, name: str) -> str:
        return f"{self._prefix()}/steps/{ordinal:04d}_{_safe_name(name)}"

    # Feature: callsite hints
    def _code_hint(self) -> dict[str, Any]:
        import inspect
        try:
            for frameinfo in inspect.stack()[2:]:
                fn = frameinfo.function
                file = frameinfo.filename
                norm = file.replace("\\", "/")
                if "/blackbox/" in norm:
                    continue
                mod = inspect.getmodule(frameinfo.frame)
                return {
                    "module": getattr(mod, "__name__", None),
                    "function": fn,
                    "file": file,
                    "line": int(frameinfo.lineno),
                }
        except Exception:
            pass
        return {"module": None, "function": None, "file": None, "line": None}

    def _init_run(self) -> None:
        created = utc_now_iso()
        self._events = []

        base: dict[str, Any] = {
            "version": "0.1",
            "run_id": self.run_id,
            "project": self.recorder.project,
            "dataset": self.recorder.dataset,
            "created_at": created,
            "finished_at": None,
            "status": "running",
            "tags": self.tags,
            "metadata": self.metadata,
            "host": get_host_info(),
            "runtime": get_runtime_info(),
            "steps": [],
            "seal": {"mode": self.recorder.seal.mode, "chain_path": "chain.json"}
            if self.recorder.seal.mode != "none"
            else {"mode": "none"},
        }

        self.store.put_json(self._run_json_key(), dict(base))
        self.store.put_json(self._run_start_key(), dict(base))

        if self.recorder.seal.mode == "chain":
            self._chain = {
                "version": "0.1",
                "run_id": self.run_id,
                "algo": self.recorder.seal.algo,
                "entries": [],
                "head": None,
            }
            self._append_chain_entry("run_start", created, self._run_start_key(), dict(base))
            self.store.put_json(self._chain_key(), self._chain)

    def step(self, name: str, *, input_df: pd.DataFrame | None = None, metadata: dict[str, Any] | None = None) -> StepContext:
        self._step_counter += 1
        return StepContext(run=self, name=name, ordinal=self._step_counter, input_df=input_df, metadata=metadata)

    # Buffered events
    def add_event(self, kind: str, message: str, *, data: dict[str, Any] | None = None) -> None:
        if self._events is None:
            self._events = []
        self._events.append({"ts": utc_now_iso(), "kind": kind, "message": message, "data": data or {}})

    # Fingerprints
    def _df_fingerprints(self, df: pd.DataFrame) -> dict[str, Any]:
        group_size = self.recorder.diff.hash_group_size
        parallel_groups = self.recorder.diff.parallel_groups
        if self.recorder.diff.auto_parallel_wide and group_size == 0 and parallel_groups == 0:
            if int(df.shape[1]) >= int(self.recorder.diff.auto_parallel_threshold_cols):
                group_size = int(self.recorder.diff.auto_hash_group_size)
                parallel_groups = int(self.recorder.diff.auto_parallel_workers)
        return {
            "schema_fp": schema_fingerprint(df),
            "content_fp": content_fingerprint_rowhash(
                df,
                order_sensitive=self.recorder.diff.order_sensitive,
                sample_rows=self.recorder.diff.sample_rows,
                hash_group_size=group_size,
                parallel_groups=parallel_groups,
                cache_rowhash=self.recorder.diff.cache_rowhash,
            ),
            "n_rows": int(len(df)),
            "n_cols": int(df.shape[1]),
        }

    def _estimate_df_mb(self, df: pd.DataFrame) -> float:
        """
        Cheap estimate used to decide skip BEFORE Parquet serialization.
        Uses memory_usage(deep=True) which is usually a good proxy for snapshot size.
        """
        try:
            bytes_used = int(df.memory_usage(index=True, deep=True).sum())
        except Exception:
            # fallback: rough estimate
            bytes_used = int(df.shape[0] * max(df.shape[1], 1) * 8)

        mb = bytes_used / (1024 * 1024)
        mult = float(getattr(self.recorder.config, "size_estimate_multiplier", 1.0) or 1.0)
        return float(mb * mult)

    def _sample_df(self, df: pd.DataFrame) -> pd.DataFrame:
        n = int(self.recorder.snapshot.sample_rows)
        if n <= 0:
            n = 2000
        dfx = df.head(n)

        cap_cols = int(self.recorder.snapshot.sample_cols)
        if cap_cols and cap_cols > 0 and dfx.shape[1] > cap_cols:
            dfx = dfx.iloc[:, :cap_cols]
        return dfx

    def _get_snapshot_executor(self):
        if self._snapshot_executor is None:
            from concurrent.futures import ThreadPoolExecutor
            self._snapshot_executor = ThreadPoolExecutor(
                max_workers=int(self.recorder.config.snapshot_async_workers or 2)
            )
        return self._snapshot_executor

    def _submit_parquet_write(self, key: str, df: pd.DataFrame):
        compression = self.recorder.config.parquet_compression
        if compression == "none":
            compression = None
        ex = self._get_snapshot_executor()
        return ex.submit(self.store.put_parquet_df, key, df, compression=compression)

    def _write_parquet(self, key: str, df: pd.DataFrame) -> float:
        compression = self.recorder.config.parquet_compression
        if compression == "none":
            compression = None
        return self.store.put_parquet_df(key, df, compression=compression)

    def _maybe_write_df_artifact(self, key: str, df: pd.DataFrame) -> dict[str, Any]:
        """
        MVP Snapshot policy:
          1) Compute fingerprints (audit signal) always.
          2) If mode=none => no artifacts.
          3) If mode=auto => cheap size estimate first. If over max_mb:
               - skip full artifact
               - optionally write a small sample artifact (head N rows)
               - DO NOT Parquet-serialize full df
          4) Else serialize to Parquet and store.
        """
        from .engines import to_pandas
        if not isinstance(df, pd.DataFrame):
            df = to_pandas(df)
        fp = self._df_fingerprints(df)
        mode = self.recorder.snapshot.mode
        max_mb = float(self.recorder.snapshot.max_mb)

        if mode == "none":
            fp["artifact"] = None
            return fp

        # --- Cheap estimate first (avoid expensive Parquet serialization) ---
        est_mb = self._estimate_df_mb(df)

        if mode == "auto" and est_mb > max_mb:
            fp["artifact"] = None
            fp["snapshot_skipped"] = {"reason": "size_estimate", "est_mb": round(est_mb, 3), "max_mb": max_mb}

            if self.recorder.snapshot.sample_on_skip:
                sample_key = key.replace(".bbdata", ".sample.bbdata")
                try:
                    dfx = self._sample_df(df)
                    if self.recorder.config.snapshot_async:
                        future = self._submit_parquet_write(sample_key, dfx)
                        fp["sample_artifact"] = _rel_under(self._prefix(), sample_key)
                        fp["sample_size_mb"] = None
                        fp["sample_rows"] = int(len(dfx))
                        fp["sample_pending"] = True
                        fp.setdefault("_pending_writes", []).append(
                            {"future": future, "size_field": "sample_size_mb"}
                        )
                    else:
                        sample_mb = self._write_parquet(sample_key, dfx)
                        fp["sample_artifact"] = _rel_under(self._prefix(), sample_key)
                        fp["sample_size_mb"] = round(sample_mb, 3)
                        fp["sample_rows"] = int(len(dfx))
                except Exception as e:
                    fp["sample_artifact"] = None
                    fp["sample_error"] = str(e)

            return fp

        # --- Store full artifact (auto below threshold OR always mode) ---
        if self.recorder.config.snapshot_async:
            future = self._submit_parquet_write(key, df)
            size_mb = None
            fp.setdefault("_pending_writes", []).append(
                {"future": future, "size_field": "snapshot_size_mb"}
            )
        else:
            size_mb = self._write_parquet(key, df)

        # If auto and actual parquet is larger than max_mb, skip storing the full artifact
        # (This prevents false negatives from estimate).
        if mode == "auto" and size_mb is not None and size_mb > max_mb:
            fp["artifact"] = None
            fp["snapshot_skipped"] = {"reason": "size", "size_mb": round(size_mb, 3), "max_mb": max_mb}

            if self.recorder.snapshot.sample_on_skip:
                sample_key = key.replace(".bbdata", ".sample.bbdata")
                try:
                    dfx = self._sample_df(df)
                    if self.recorder.config.snapshot_async:
                        future = self._submit_parquet_write(sample_key, dfx)
                        fp["sample_artifact"] = _rel_under(self._prefix(), sample_key)
                        fp["sample_size_mb"] = None
                        fp["sample_rows"] = int(len(dfx))
                        fp["sample_pending"] = True
                        fp.setdefault("_pending_writes", []).append(
                            {"future": future, "size_field": "sample_size_mb"}
                        )
                    else:
                        sample_mb = self._write_parquet(sample_key, dfx)
                        fp["sample_artifact"] = _rel_under(self._prefix(), sample_key)
                        fp["sample_size_mb"] = round(sample_mb, 3)
                        fp["sample_rows"] = int(len(dfx))
                except Exception as e:
                    fp["sample_artifact"] = None
                    fp["sample_error"] = str(e)

            return fp

        # Store full artifact (already written in _write_parquet)
        fp["artifact"] = _rel_under(self._prefix(), key)
        fp["snapshot_size_mb"] = round(size_mb, 3) if size_mb is not None else None
        fp["snapshot_est_mb"] = round(est_mb, 3)
        return fp

    def _register_pending_writes(self, step_json_key: str, field_name: str, pending: list[dict[str, Any]]) -> None:
        if not pending:
            return
        if self._pending_writes is None:
            self._pending_writes = []
        for p in pending:
            self._pending_writes.append(
                {
                    "step_json_key": step_json_key,
                    "field_name": field_name,
                    "future": p["future"],
                    "size_field": p["size_field"],
                }
            )

    def _flush_pending_writes(self) -> None:
        if not self._pending_writes:
            return
        for item in self._pending_writes:
            future = item["future"]
            try:
                size_mb = future.result()
                size_mb = round(float(size_mb), 3)
                step_obj = self.store.get_json(item["step_json_key"])
                field = step_obj.get(item["field_name"])
                if isinstance(field, dict):
                    field[item["size_field"]] = size_mb
                    field["snapshot_pending"] = False
                    field["sample_pending"] = False
                    step_obj[item["field_name"]] = field
                    self.store.put_json(item["step_json_key"], step_obj)
            except Exception as e:
                try:
                    step_obj = self.store.get_json(item["step_json_key"])
                    field = step_obj.get(item["field_name"])
                    if isinstance(field, dict):
                        field["snapshot_error"] = str(e)
                        step_obj[item["field_name"]] = field
                        self.store.put_json(item["step_json_key"], step_obj)
                except Exception:
                    pass

        self._pending_writes = []

    def _append_chain_entry(self, typ: str, ts: str, payload_ref: str, payload_obj: dict[str, Any]) -> None:
        if self._chain is None:
            raise RuntimeError("Chain not initialized")
        entries = self._chain["entries"]
        idx = len(entries)
        prev = entries[-1]["digest"] if entries else None
        pdig = payload_digest(payload_obj)
        dig = chain_digest(prev, pdig, typ, ts)
        entries.append({
            "index": idx,
            "type": typ,
            "ts": ts,
            "payload_ref": _rel_under(self._prefix(), payload_ref),
            "payload_digest": pdig,
            "prev": prev,
            "digest": dig,
        })
        self._chain["head"] = dig
        self.store.put_json(self._chain_key(), self._chain)

    def finish(self, *, status: str = "ok", error: str | None = None) -> None:
        finished = utc_now_iso()

        # Ensure async snapshot writes are completed before finalizing run metadata.
        if self.recorder.config.snapshot_async:
            self._flush_pending_writes()
            if self._snapshot_executor is not None:
                try:
                    self._snapshot_executor.shutdown(wait=True, cancel_futures=False)
                except Exception:
                    pass

        run_summary = self.store.get_json(self._run_json_key())
        run_summary["finished_at"] = finished
        run_summary["status"] = status
        if error:
            run_summary["error"] = error

        # Policy checks (enterprise)
        policy: dict[str, Any] = run_summary.get("policy") or {}
        violations: list[str] = policy.get("violations", [])

        # Max run size enforcement (LocalStore only)
        max_run_mb = self.recorder.config.max_run_mb
        if isinstance(self.store, LocalStore) and max_run_mb:
            try:
                total_bytes = 0
                for key in self.store.list(self._prefix()):
                    path = self.store._path(key)
                    if os.path.exists(path):
                        total_bytes += os.path.getsize(path)
                total_mb = total_bytes / (1024 * 1024)
                policy["total_run_mb"] = round(total_mb, 3)
                policy["max_run_mb"] = float(max_run_mb)
                if total_mb > float(max_run_mb):
                    violations.append("max_run_size_exceeded")
            except Exception as e:
                _logger.debug("Failed to compute run size: %s", e)

        # Require verification for production runs
        if self.recorder.config.require_verify_for_prod:
            env = (self.tags or {}).get("env")
            if env in ("prod", "production"):
                ok, msg = self.verify()
                policy["prod_verify_ok"] = bool(ok)
                policy["prod_verify_message"] = msg
                if not ok:
                    violations.append("prod_verification_failed")
                    run_summary["status"] = "verify_failed"

        if violations:
            policy["violations"] = sorted(set(violations))
        if policy:
            run_summary["policy"] = policy

        step_keys = self.store.list(f"{self._prefix()}/steps")
        step_jsons = [k for k in step_keys if k.endswith("/step.json")]
        step_jsons.sort()

        steps = []
        for k in step_jsons:
            folder = k.split("/")[-2]
            ordinal = int(folder.split("_", 1)[0])
            name = folder.split("_", 1)[1]
            steps.append({"ordinal": ordinal, "name": name, "path": _rel_under(self._prefix(), k)})
        run_summary["steps"] = steps

        if self._events:
            import json
            lines = "".join(json.dumps(e, ensure_ascii=False) + "\n" for e in self._events).encode("utf-8")
            self.store.put_bytes(self._events_key(), lines, content_type="application/jsonl")

        # Immutable evidence run_finish.json without seal.head
        run_finish = dict(run_summary)
        if isinstance(run_finish.get("seal"), dict):
            run_finish["seal"] = dict(run_finish["seal"])
            run_finish["seal"].pop("head", None)
        self.store.put_json(self._run_finish_key(), run_finish)

        if self.recorder.seal.mode == "chain":
            self._append_chain_entry("run_finish", finished, self._run_finish_key(), run_finish)
            chain_obj = self.store.get_json(self._chain_key())
            run_summary.setdefault("seal", {})
            if isinstance(run_summary["seal"], dict):
                run_summary["seal"]["head"] = chain_obj.get("head")

        self.store.put_json(self._run_json_key(), run_summary)

    def verify(self) -> tuple[bool, str]:
        if self.recorder.seal.mode != "chain":
            return True, "seal disabled"
        chain_obj = self.store.get_json(self._chain_key())
        ok, msg = verify_chain_with_payloads(chain_obj, self.store, run_prefix=self._prefix())
        return ok, msg


class StreamRun:
    """
    Minimal streaming helper for micro-batch pipelines.
    Each batch is recorded as a step, diffed against the previous batch output.
    """

    def __init__(self, run: Run) -> None:
        self._run = run
        self._batch_index = 0
        self._last_df: pd.DataFrame | None = None

    @property
    def run_id(self) -> str:
        return self._run.run_id

    def push(
        self,
        step: str,
        df: pd.DataFrame,
        *,
        metadata: dict[str, Any] | None = None,
        window: dict[str, Any] | None = None,
    ) -> None:
        self._batch_index += 1
        meta = {"stream": True, "batch_index": self._batch_index}
        if metadata:
            meta.update(metadata)
        if window:
            meta["window"] = window

        with self._run.step(step, input_df=self._last_df, metadata=meta) as st:
            st.capture_output(df)
        self._last_df = df

    def finish(self, *, status: str = "ok", error: str | None = None) -> None:
        self._run.finish(status=status, error=error)

    def verify(self) -> tuple[bool, str]:
        return self._run.verify()
