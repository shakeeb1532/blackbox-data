from __future__ import annotations

from dataclasses import dataclass
from typing import Any
import weakref

import pandas as pd
import numpy as np


def _is_polars_df(obj: Any) -> bool:
    mod = getattr(obj.__class__, "__module__", "")
    return mod.startswith("polars")


def _polars_hash_series(df: Any, cols: list[str]) -> list[int]:
    import polars as pl  # type: ignore
    if not cols:
        return [0] * df.height
    try:
        series = df.select(pl.struct(cols).hash().alias("_h"))["_h"]
    except Exception:
        series = df.select(pl.all().hash_rows().alias("_h"))["_h"]
    return [int(x) for x in series.to_list()]


def _polars_pk_series(df: Any, pk: list[str]) -> list[str]:
    import polars as pl  # type: ignore
    if len(pk) == 1:
        series = df.select(pl.col(pk[0]).cast(pl.Utf8).alias("_k"))["_k"]
        return [str(x) for x in series.to_list()]
    parts = [pl.col(c).cast(pl.Utf8) for c in pk]
    series = df.select(pl.concat_str(parts, separator="|").alias("_k"))["_k"]
    return [str(x) for x in series.to_list()]


# ----------------------------
# Schema fingerprint + diff
# ----------------------------

def schema_fingerprint(df: pd.DataFrame) -> dict[str, Any]:
    """
    Stable-ish schema fingerprint:
      - column order
      - dtype strings
    """
    cols = list(map(str, df.columns.tolist()))
    dtypes = {str(c): str(df[c].dtype) for c in df.columns}
    return {"cols": cols, "dtypes": dtypes}


def schema_diff(a: pd.DataFrame, b: pd.DataFrame) -> dict[str, Any]:
    """
    Returns:
      {
        "added_cols": [...],
        "removed_cols": [...],
        "dtype_changed": [{"col": "...", "from": "...", "to": "..."}]
      }
    """
    a_cols = [str(c) for c in a.columns]
    b_cols = [str(c) for c in b.columns]

    a_set = set(a_cols)
    b_set = set(b_cols)

    added = [c for c in b_cols if c not in a_set]
    removed = [c for c in a_cols if c not in b_set]

    dtype_changed: list[dict[str, Any]] = []
    common = [c for c in a_cols if c in b_set]
    for c in common:
        ad = str(a[c].dtype)
        bd = str(b[c].dtype)
        if ad != bd:
            dtype_changed.append({"col": c, "from": ad, "to": bd})

    return {"added_cols": added, "removed_cols": removed, "dtype_changed": dtype_changed}


# ----------------------------
# Row hashing + fingerprints
# ----------------------------

def _normalize_pk_series(s: pd.Series) -> pd.Series:
    # Normalize PK values to strings for stable JSON artifacts
    # (also avoids issues with numpy scalar types).
    return s.astype("string")


_ROW_HASH_CACHE: dict[int, dict[str, Any]] = {}
_ROW_HASH_REFS: dict[int, weakref.ref] = {}


def _get_rowhash_cache(df: pd.DataFrame) -> dict[str, Any]:
    key = id(df)
    ref = _ROW_HASH_REFS.get(key)
    if ref is None or ref() is not df:
        def _cleanup(_):
            _ROW_HASH_CACHE.pop(key, None)
            _ROW_HASH_REFS.pop(key, None)

        _ROW_HASH_CACHE[key] = {}
        _ROW_HASH_REFS[key] = weakref.ref(df, _cleanup)
    return _ROW_HASH_CACHE[key]


def _auto_parallel_settings(
    cols_count: int,
    group_size: int,
    parallel_groups: int,
    *,
    auto_parallel: bool,
    threshold_cols: int,
    workers: int,
    group_size_default: int,
) -> tuple[int, int]:
    if not auto_parallel:
        return group_size, parallel_groups
    if group_size or parallel_groups:
        return group_size, parallel_groups
    if cols_count >= threshold_cols:
        return group_size_default, max(2, int(workers))
    return group_size, parallel_groups


def _rowhash_cache_key(cols: list[str], *, group_size: int) -> str:
    return f"cols={','.join(cols)}|group={group_size}"


def _hash_frame(df: pd.DataFrame, cols: list[str]) -> pd.Series:
    x = df[cols].copy(deep=False)
    obj_cols = [c for c in cols if x[c].dtype == "object"]
    for c in obj_cols:
        x[c] = x[c].astype("string")
    return pd.util.hash_pandas_object(x, index=False)


def _rowhash_series(
    df: pd.DataFrame,
    cols: list[str],
    *,
    group_size: int = 0,
    parallel_groups: int = 0,
    cache_rowhash: bool = False,
) -> pd.Series:
    """
    Uses pandas built-in hashing for speed; returns uint64 hashes.
    """
    if not cols:
        return pd.Series([0] * len(df), index=df.index, dtype="uint64")

    cache_key = _rowhash_cache_key(cols, group_size=group_size)
    if cache_rowhash:
        cache = _get_rowhash_cache(df)
        cached = cache.get(cache_key)
        if isinstance(cached, pd.Series) and len(cached) == len(df):
            return cached

    if group_size and group_size > 0 and len(cols) > group_size:
        groups: list[list[str]] = []
        for i in range(0, len(cols), group_size):
            groups.append(cols[i : i + group_size])

        if parallel_groups and parallel_groups > 1 and len(groups) > 1:
            from concurrent.futures import ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=parallel_groups) as ex:
                parts = list(ex.map(lambda g: _hash_frame(df, g), groups))
        else:
            parts = [_hash_frame(df, g) for g in groups]

        h = parts[0]
        for p in parts[1:]:
            h = h ^ p
    else:
        h = _hash_frame(df, cols)

    if cache_rowhash:
        cache = _get_rowhash_cache(df)
        cache[cache_key] = h
    return h


def content_fingerprint_rowhash(
    df: pd.DataFrame,
    *,
    order_sensitive: bool = False,
    sample_rows: int = 0,
    hash_group_size: int = 0,
    parallel_groups: int = 0,
    cache_rowhash: bool = False,
    native_polars: bool = False,
) -> dict[str, Any]:
    """
    Lightweight content fingerprint:
      - hashes rows over all columns
      - aggregates into a small representative sample
    Note: Not cryptographic; sealing handles tamper evidence.
    """
    if hasattr(df, "shape") and df.shape[0] == 0:
        return {"mode": "rowhash", "label": "h64", "sample": [], "n": 0}

    dfx = df
    if sample_rows and sample_rows > 0 and len(df) > sample_rows:
        dfx = df.head(int(sample_rows))

    if native_polars and _is_polars_df(dfx):
        cols = [str(c) for c in dfx.columns]
        hashes = _polars_hash_series(dfx, cols)
        if not order_sensitive:
            vals = np.asarray(hashes, dtype="uint64")
            k = min(10, len(vals))
            if k == 0:
                take = []
            elif len(vals) <= k:
                take = sorted(vals.tolist())
            else:
                part = np.partition(vals, k - 1)[:k]
                take = sorted(part.tolist())
        else:
            take = list(map(int, hashes[: min(10, len(hashes))]))
        return {"mode": "rowhash", "label": "h64", "sample": take, "n": int(len(dfx))}

    cols = [str(c) for c in dfx.columns]
    hashes = _rowhash_series(
        dfx,
        cols,
        group_size=hash_group_size,
        parallel_groups=parallel_groups,
        cache_rowhash=cache_rowhash,
    )

    if not order_sensitive:
        vals = hashes.to_numpy(copy=False)
        k = min(10, len(vals))
        if k == 0:
            take = []
        elif len(vals) <= k:
            take = sorted(vals.astype("uint64").tolist())
        else:
            # Faster than full sort for large frames: partition to k smallest.
            part = np.partition(vals, k - 1)[:k]
            take = sorted(part.astype("uint64").tolist())
    else:
        take = hashes.head(min(10, len(hashes))).astype("uint64").tolist()
    return {"mode": "rowhash", "label": "h64", "sample": take, "n": int(len(dfx))}


# ----------------------------
# Diff (PK mode)
# ----------------------------

@dataclass(frozen=True)
class DiffSummary:
    added: int
    removed: int
    changed: int


def diff_rowhash(
    a: pd.DataFrame,
    b: pd.DataFrame,
    *,
    order_sensitive: bool = False,
    sample_rows: int = 0,
    primary_key: list[str] | None = None,
    treat_schema_add_remove_as_change: bool = False,
    summary_only_threshold: float | None = None,
    total_keys_hint: int | None = None,
    diff_mode: str = "rows",
    chunk_rows: int = 0,
    hash_group_size: int = 0,
    parallel_groups: int = 0,
    auto_parallel_wide: bool = False,
    auto_parallel_threshold_cols: int = 40,
    auto_parallel_workers: int = 4,
    auto_hash_group_size: int = 8,
    cache_rowhash: bool = False,
    native_polars: bool = False,
) -> tuple[dict[str, Any], DiffSummary]:
    """
    PK-based diff (rowhash mode).

    Default MVP semantics:
      - Hash only SHARED non-PK columns between a and b.
      - Record schema-only columns in notes.
      - Optionally: treat schema add/remove as "all common keys changed".
    """
    if a.shape[1] == 0 or b.shape[1] == 0:
        raise ValueError("diff_rowhash requires both dataframes to have at least one column")

    if primary_key is None:
        if "id" in a.columns and "id" in b.columns:
            pk = ["id"]
        else:
            pk = [str(a.columns[0])]
    else:
        pk = [str(x) for x in primary_key]

    a_cols = [str(c) for c in a.columns]
    b_cols = [str(c) for c in b.columns]
    a_set = set(a_cols)
    b_set = set(b_cols)

    missing_pk = [c for c in pk if c not in a_set or c not in b_set]
    if missing_pk:
        raise ValueError(f"Primary key columns missing from one or both dataframes: {missing_pk}")

    cols_only_in_left = [c for c in a_cols if c not in b_set]
    cols_only_in_right = [c for c in b_cols if c not in a_set]

    common_cols = [c for c in a_cols if c in b_set]

    pk_set = set(pk)
    cols_hashed = [c for c in common_cols if c not in pk_set]

    # Auto-parallelize wide frames unless user provided explicit settings.
    hash_group_size, parallel_groups = _auto_parallel_settings(
        len(cols_hashed),
        hash_group_size,
        parallel_groups,
        auto_parallel=auto_parallel_wide,
        threshold_cols=auto_parallel_threshold_cols,
        workers=auto_parallel_workers,
        group_size_default=auto_hash_group_size,
    )

    if sample_rows and sample_rows > 0:
        aa = a.head(int(sample_rows)).copy()
        bb = b.head(int(sample_rows)).copy()
    else:
        aa = a
        bb = b

    if native_polars and _is_polars_df(aa) and _is_polars_df(bb):
        # Polars-native hashing path (experimental)
        pk = [str(x) for x in (primary_key or (["id"] if "id" in a.columns and "id" in b.columns else [str(a.columns[0])]))]
        cols_hashed = [c for c in common_cols if c not in set(pk)]
        a_keys = _polars_pk_series(aa, pk)
        b_keys = _polars_pk_series(bb, pk)
        if len(a_keys) != len(set(a_keys)):
            raise ValueError("Primary key values must be unique in 'a'")
        if len(b_keys) != len(set(b_keys)):
            raise ValueError("Primary key values must be unique in 'b'")
        if keys_only or not cols_hashed:
            a_map = {k: 0 for k in a_keys}
            b_map = {k: 0 for k in b_keys}
        else:
            a_hash = _polars_hash_series(aa, cols_hashed)
            b_hash = _polars_hash_series(bb, cols_hashed)
            a_map = {k: int(v) for k, v in zip(a_keys, a_hash)}
            b_map = {k: int(v) for k, v in zip(b_keys, b_hash)}
        a_set = set(a_map.keys())
        b_set = set(b_map.keys())
        added_keys = sorted(list(b_set - a_set))
        removed_keys = sorted(list(a_set - b_set))
        common_keys = a_set & b_set
        if keys_only:
            changed_keys = []
        else:
            changed_keys = sorted([k for k in common_keys if a_map[k] != b_map[k]])
        added_count = len(added_keys)
        removed_count = len(removed_keys)
        changed_count = len(changed_keys)
        total_keys = int(total_keys_hint or max(len(a_set), len(b_set)))
        summary_only = False
        if summary_only_threshold is not None and summary_only_threshold > 0:
            ratio = (added_count + removed_count) / max(total_keys, 1)
            if ratio >= summary_only_threshold:
                summary_only = True
                added_keys = []
                removed_keys = []
                changed_keys = []

        payload: dict[str, Any] = {
            "version": "0.1",
            "mode": "rowhash",
            "hash": {"algo": "polars_hash", "label": "h64"},
            "primary_key": pk,
            "cols_hashed": cols_hashed,
            "added_rowhashes": [],
            "removed_rowhashes": [],
            "added_keys": added_keys,
            "removed_keys": removed_keys,
            "changed_keys": changed_keys,
            "summary_only": bool(summary_only),
            "summary": {"added": added_count, "removed": removed_count, "changed": changed_count},
            "ui_hint": "summary_only_high_churn" if summary_only else None,
            "diff_mode": diff_mode,
            "notes": {
                "order_sensitive": bool(order_sensitive),
                "sample_rows": int(sample_rows or 0),
                "hash_cols_mode": "shared",
                "schema_changed": schema_changed,
                "cols_only_in_left": cols_only_in_left,
                "cols_only_in_right": cols_only_in_right,
                "treat_schema_add_remove_as_change": bool(treat_schema_add_remove_as_change),
                "chunk_rows": int(chunk_rows or 0),
                "hash_group_size": int(hash_group_size or 0),
                "parallel_groups": int(parallel_groups or 0),
                "native_polars": True,
            },
        }
        summary = DiffSummary(added=added_count, removed=removed_count, changed=changed_count)
        return payload, summary

    single_pk = len(pk) == 1
    if single_pk:
        # Avoid string conversions for performance; convert to string only for output.
        a_pk = aa[pk[0]]
        b_pk = bb[pk[0]]
    else:
        a_pk = aa[pk].astype("string").agg("|".join, axis=1)
        b_pk = bb[pk].astype("string").agg("|".join, axis=1)

    if a_pk.duplicated(keep=False).any():
        dup = a_pk[a_pk.duplicated(keep=False)].astype("string")
        sample = sorted(set(dup.head(5).tolist()))
        raise ValueError(f"Primary key values must be unique in 'a'; duplicates found (sample={sample})")

    if b_pk.duplicated(keep=False).any():
        dup = b_pk[b_pk.duplicated(keep=False)].astype("string")
        sample = sorted(set(dup.head(5).tolist()))
        raise ValueError(f"Primary key values must be unique in 'b'; duplicates found (sample={sample})")

    keys_only = diff_mode == "keys-only"

    def _build_map_chunked(df: pd.DataFrame) -> dict[str, int]:
        mapping: dict[str, int] = {}
        keys_seen: set[str] = set()
        n = len(df)
        step = int(chunk_rows) if chunk_rows and chunk_rows > 0 else n
        for start in range(0, n, step):
            end = min(start + step, n)
            dfx = df.iloc[start:end]
            if len(pk) == 1:
                pk_series = _normalize_pk_series(dfx[pk[0]])
            else:
                pk_series = dfx[pk].astype("string").agg("|".join, axis=1)

            if pk_series.duplicated(keep=False).any():
                dup = pk_series[pk_series.duplicated(keep=False)].astype("string")
                sample = sorted(set(dup.head(5).tolist()))
                raise ValueError(f"Primary key values must be unique; duplicates found (sample={sample})")

            pk_values = pk_series.astype("string").tolist()
            for k in pk_values:
                if k in keys_seen:
                    raise ValueError(f"Primary key values must be unique; duplicate found: {k}")
                keys_seen.add(k)

            if keys_only or not cols_hashed:
                for k in pk_values:
                    mapping[k] = 0
            else:
                h = _rowhash_series(
                    dfx,
                    cols_hashed,
                    group_size=hash_group_size,
                    parallel_groups=parallel_groups,
                    cache_rowhash=cache_rowhash,
                )
                for k, v in zip(pk_values, h.values):
                    mapping[k] = int(v)
        return mapping

    if chunk_rows and chunk_rows > 0:
        a_map = _build_map_chunked(aa)
        b_map = _build_map_chunked(bb)
        a_keys = set(a_map.keys())
        b_keys = set(b_map.keys())
        added_count = int(len(b_keys - a_keys))
        removed_count = int(len(a_keys - b_keys))
        common_keys = a_keys & b_keys
    else:
        if keys_only or not cols_hashed:
            a_hash = pd.Series([0] * len(aa), index=aa.index, dtype="uint64")
            b_hash = pd.Series([0] * len(bb), index=bb.index, dtype="uint64")
        else:
            a_hash = _rowhash_series(
                aa,
                cols_hashed,
                group_size=hash_group_size,
                parallel_groups=parallel_groups,
                cache_rowhash=cache_rowhash,
            )
            b_hash = _rowhash_series(
                bb,
                cols_hashed,
                group_size=hash_group_size,
                parallel_groups=parallel_groups,
                cache_rowhash=cache_rowhash,
            )

        a_map = pd.Series(a_hash.values, index=pd.Index(a_pk.values))
        b_map = pd.Series(b_hash.values, index=pd.Index(b_pk.values))

        a_idx = a_map.index
        b_idx = b_map.index
        added_idx = b_idx.difference(a_idx, sort=False)
        removed_idx = a_idx.difference(b_idx, sort=False)
        common_idx = a_idx.intersection(b_idx, sort=False)
        added_count = int(added_idx.size)
        removed_count = int(removed_idx.size)
        common_keys = set(common_idx.tolist())

    if keys_only:
        changed_count = 0
        changed_mask = None
    else:
        if chunk_rows and chunk_rows > 0:
            changed_count = 0
            for k in common_keys:
                if a_map[k] != b_map[k]:
                    changed_count += 1
            changed_mask = None
        else:
            if common_idx.size:
                a_vals = a_map.reindex(common_idx).values
                b_vals = b_map.reindex(common_idx).values
                changed_mask = a_vals != b_vals
                changed_count = int(changed_mask.sum())
            else:
                changed_mask = None
                changed_count = 0

    if chunk_rows and chunk_rows > 0:
        total_keys = int(total_keys_hint or max(len(a_keys), len(b_keys)))
    else:
        total_keys = int(total_keys_hint or max(len(a_idx), len(b_idx)))
    summary_only = False
    if summary_only_threshold is not None and summary_only_threshold > 0:
        ratio = (added_count + removed_count) / max(total_keys, 1)
        if ratio >= summary_only_threshold:
            summary_only = True

    if not summary_only:
        if chunk_rows and chunk_rows > 0:
            added_keys = sorted(list(b_keys - a_keys))
            removed_keys = sorted(list(a_keys - b_keys))
            if keys_only:
                changed_keys = []
            else:
                changed_keys = sorted([k for k in common_keys if a_map[k] != b_map[k]])
        else:
            added_keys = sorted([str(x) for x in added_idx.tolist()])
            removed_keys = sorted([str(x) for x in removed_idx.tolist()])
            if common_idx.size and changed_mask is not None:
                changed_keys = [str(x) for x in common_idx[changed_mask].tolist()]
            else:
                changed_keys = []
    else:
        added_keys = []
        removed_keys = []
        changed_keys = []

    schema_changed = bool(cols_only_in_left or cols_only_in_right)
    if treat_schema_add_remove_as_change and schema_changed:
        changed_keys = sorted([str(k) for k in common_keys])
        changed_count = len(common_keys)

    payload: dict[str, Any] = {
        "version": "0.1",
        "mode": "rowhash",
        "hash": {"algo": "pandas_hash_pandas_object", "label": "h64"},
        "primary_key": pk,
        "cols_hashed": cols_hashed,
        "added_rowhashes": [],    # reserved for later
        "removed_rowhashes": [],  # reserved for later
        "added_keys": added_keys,
        "removed_keys": removed_keys,
        "changed_keys": changed_keys,
        "summary_only": bool(summary_only),
        "summary": {"added": added_count, "removed": removed_count, "changed": changed_count},
        "ui_hint": "summary_only_high_churn" if summary_only else None,
        "diff_mode": diff_mode,
        "notes": {
            "order_sensitive": bool(order_sensitive),
            "sample_rows": int(sample_rows or 0),
            "hash_cols_mode": "shared",
            "schema_changed": schema_changed,
            "cols_only_in_left": cols_only_in_left,
            "cols_only_in_right": cols_only_in_right,
            "treat_schema_add_remove_as_change": bool(treat_schema_add_remove_as_change),
            "chunk_rows": int(chunk_rows or 0),
            "hash_group_size": int(hash_group_size or 0),
            "parallel_groups": int(parallel_groups or 0),
        },
    }

    summary = DiffSummary(added=added_count, removed=removed_count, changed=changed_count)
    return payload, summary
