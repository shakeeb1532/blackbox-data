from __future__ import annotations
from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class DiffConfig:
    mode: Literal["none", "rowhash"] = "rowhash"
    diff_mode: Literal["rows", "schema", "keys-only"] = "rows"
    primary_key: list[str] | None = None
    order_sensitive: bool = False
    sample_rows: int = 0  # 0 = all rows
    # Adaptive diffing: skip or summarize based on cheap signals.
    adaptive: bool = True
    # If schema unchanged AND content fingerprint matches, skip deep diff.
    skip_if_fingerprint_match: bool = True
    # If (added+removed)/max(n_rows) >= threshold, return summary only.
    # Set to 0 to disable summary-only mode.
    summary_only_threshold: float = 0.2
    # Diff chunking (rows per chunk). 0 disables chunked diff.
    chunk_rows: int = 0
    # Hashing controls for wide frames.
    hash_group_size: int = 0
    parallel_groups: int = 0
    # Cache rowhashes on DataFrame attrs for reuse across steps.
    cache_rowhash: bool = True


@dataclass(frozen=True)
class SnapshotConfig:
    """
    Snapshot policy:
      - none: never store data artifacts
      - auto: store only if estimated/actual size <= max_mb
      - always: always store full artifacts (may be large)
    """
    mode: Literal["none", "auto", "always"] = "auto"

    # Allow floats (you are using 0.6 in practice)
    max_mb: float = 50.0

    # When we skip full snapshot (auto), write a small sample artifact instead.
    sample_on_skip: bool = True

    # Number of rows for sample snapshots (head rows).
    sample_rows: int = 2000

    # For extremely wide frames, you may want to cap columns in sample too.
    # 0 means "keep all columns".
    sample_cols: int = 0


@dataclass(frozen=True)
class SealConfig:
    mode: Literal["none", "chain"] = "chain"
    algo: Literal["sha256"] = "sha256"


@dataclass(frozen=True)
class RecorderConfig:
    # v0.1: keep explicit; no magic inference
    enforce_explicit_output: bool = True

    # Parquet write options
    parquet_compression: Literal["snappy", "zstd", "gzip", "lz4", "none"] = "snappy"
    # Async snapshot writes (waited at run.finish).
    snapshot_async: bool = False
    snapshot_async_workers: int = 2

    # Size estimate guardrail:
    # We estimate size using memory_usage(deep=True). If object columns are heavy,
    # we add a multiplier to reduce false negatives.
    size_estimate_multiplier: float = 1.0

    # Policy controls (enterprise)
    # Maximum total run size (artifacts + metadata). None disables check.
    max_run_mb: float | None = 1024.0
    # Require verification for prod-tagged runs.
    require_verify_for_prod: bool = True
    # Retention window in days (used by cleanup tooling).
    retention_days: int | None = 30
