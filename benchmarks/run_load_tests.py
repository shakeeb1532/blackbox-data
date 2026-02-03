from __future__ import annotations

import csv
import os
import time
from dataclasses import dataclass

import pandas as pd

from blackbox.hashing import diff_rowhash, content_fingerprint_rowhash
from blackbox import Recorder, Store, DiffConfig, SnapshotConfig, SealConfig


@dataclass
class LoadResult:
    name: str
    iterations: int
    rows: int
    mean_ms: float
    rows_per_sec: float


def _now_ms() -> float:
    return time.perf_counter() * 1000.0


def _make_df(n: int) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id": range(1, n + 1),
            "email": [f"user{i}@example.com" for i in range(1, n + 1)],
            "country": ["US"] * n,
            "active": [True] * n,
            "score": list(range(n)),
        }
    )


def _mutate_df(df: pd.DataFrame) -> pd.DataFrame:
    n = len(df)
    out = df.copy()
    out.loc[out["id"] % 100 == 0, "active"] = False
    out.loc[out["id"] % 100 == 0, "score"] = out.loc[out["id"] % 100 == 0, "score"] + 123
    out = pd.concat([out, pd.DataFrame({"id": range(n + 1, n + 501), "email": ["x"] * 500, "country": ["US"] * 500, "active": [True] * 500, "score": [1] * 500})], ignore_index=True)
    return out


def run_load(name: str, fn, *, rows: int, iterations: int) -> LoadResult:
    times = []
    for _ in range(iterations):
        t0 = _now_ms()
        fn()
        t1 = _now_ms()
        times.append(t1 - t0)
    mean_ms = sum(times) / len(times)
    rows_per_sec = rows / (mean_ms / 1000.0) if mean_ms > 0 else 0.0
    return LoadResult(name=name, iterations=iterations, rows=rows, mean_ms=mean_ms, rows_per_sec=rows_per_sec)


def main() -> int:
    rows = 1_000_000
    iterations = 5
    df = _make_df(rows)
    df2 = _mutate_df(df)

    results = [
        run_load(
            "content_fingerprint_rowhash_load",
            lambda: content_fingerprint_rowhash(df, order_sensitive=False, sample_rows=0),
            rows=rows,
            iterations=iterations,
        ),
        run_load(
            "diff_rowhash_load",
            lambda: diff_rowhash(df, df2, primary_key=["id"], order_sensitive=False),
            rows=rows,
            iterations=iterations,
        ),
    ]

    # Snapshot load: write sample to temp store repeatedly
    store = Store.local(os.path.join("benchmarks", "load_store"))
    rec = Recorder(
        store=store,
        project="load",
        dataset="snapshot",
        diff=DiffConfig(mode="rowhash", primary_key=["id"]),
        snapshot=SnapshotConfig(mode="auto", max_mb=0.6),
        seal=SealConfig(mode="none"),
    )
    run = rec.start_run()
    results.append(
        run_load(
            "snapshot_maybe_write_df_artifact_load",
            lambda: run._maybe_write_df_artifact("steps/0001_x/artifacts/input.bbdata", df),
            rows=rows,
            iterations=3,
        )
    )

    out = os.path.join("benchmarks", "load_results.csv")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["name", "iterations", "rows", "mean_ms", "rows_per_sec"])
        for r in results:
            writer.writerow([r.name, r.iterations, r.rows, f"{r.mean_ms:.2f}", f"{r.rows_per_sec:.2f}"])
    print("Load test results written to:", out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
