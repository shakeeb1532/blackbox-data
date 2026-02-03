from __future__ import annotations

import csv
import os
import time
from dataclasses import dataclass

import pandas as pd

from blackbox.hashing import diff_rowhash, content_fingerprint_rowhash


@dataclass
class StressResult:
    name: str
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
    out.loc[out["id"] % 1000 == 0, "active"] = False
    out.loc[out["id"] % 1000 == 0, "score"] = out.loc[out["id"] % 1000 == 0, "score"] + 9999
    add_df = pd.DataFrame(
        {
            "id": range(n + 1, n + 2001),
            "email": ["x"] * 2000,
            "country": ["US"] * 2000,
            "active": [True] * 2000,
            "score": [1] * 2000,
        }
    )
    out = pd.concat([out, add_df], ignore_index=True)
    return out


def _run(fn, rows: int, iterations: int = 2) -> StressResult:
    times = []
    for _ in range(iterations):
        t0 = _now_ms()
        fn()
        t1 = _now_ms()
        times.append(t1 - t0)
    mean_ms = sum(times) / len(times)
    rows_per_sec = rows / (mean_ms / 1000.0) if mean_ms > 0 else 0.0
    return StressResult(fn.__name__, rows, mean_ms, rows_per_sec)


def main() -> int:
    rows = 2_000_000
    df = _make_df(rows)
    df2 = _mutate_df(df)

    results = [
        _run(lambda: content_fingerprint_rowhash(df, order_sensitive=False, sample_rows=0), rows),
        _run(lambda: diff_rowhash(df, df2, primary_key=["id"], order_sensitive=False), rows),
    ]

    out = os.path.join("benchmarks", "stress_results.csv")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["name", "rows", "mean_ms", "rows_per_sec"])
        for r in results:
            writer.writerow([r.name, r.rows, f"{r.mean_ms:.2f}", f"{r.rows_per_sec:.2f}"])
    print("Stress test results written to:", out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
