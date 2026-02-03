from __future__ import annotations

import argparse
import gc
import json
import os
import statistics
import tempfile
import time
from dataclasses import asdict, dataclass
from typing import Any, Callable

import pandas as pd

from blackbox import Recorder, Store, DiffConfig, SnapshotConfig, SealConfig
from blackbox.hashing import diff_rowhash, content_fingerprint_rowhash


@dataclass
class BenchmarkResult:
    name: str
    n_rows: int
    n_cols: int
    runs: int
    warmup: int
    min_ms: float
    max_ms: float
    mean_ms: float
    median_ms: float
    p95_ms: float
    rows_per_sec: float


def _p95(values: list[float]) -> float:
    if not values:
        return 0.0
    vs = sorted(values)
    idx = int(0.95 * (len(vs) - 1))
    return float(vs[idx])


def _time_one(fn: Callable[[], Any]) -> float:
    t0 = time.perf_counter()
    fn()
    t1 = time.perf_counter()
    return (t1 - t0) * 1000.0


def _run_bench(
    name: str,
    n_rows: int,
    n_cols: int,
    fn: Callable[[], Any],
    *,
    warmup: int,
    runs: int,
    rows_for_rate: int,
) -> BenchmarkResult:
    for _ in range(warmup):
        fn()
        gc.collect()

    times: list[float] = []
    for _ in range(runs):
        times.append(_time_one(fn))
        gc.collect()

    return BenchmarkResult(
        name=name,
        n_rows=n_rows,
        n_cols=n_cols,
        runs=runs,
        warmup=warmup,
        min_ms=min(times),
        max_ms=max(times),
        mean_ms=statistics.mean(times),
        median_ms=statistics.median(times),
        p95_ms=_p95(times),
        rows_per_sec=(rows_for_rate / (statistics.mean(times) / 1000.0)) if statistics.mean(times) > 0 else 0.0,
    )


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


def _make_wide_df(n: int, *, wide_cols: int) -> pd.DataFrame:
    base = _make_df(n)
    if wide_cols <= 0:
        return base
    for i in range(wide_cols):
        base[f"extra_{i:03d}"] = i
    return base


def _mutate_df(df: pd.DataFrame) -> pd.DataFrame:
    n = len(df)
    remove_ids = set(range(1, min(301, n + 1)))
    change_ids = set(range(min(10_000, n), min(10_400, n)))
    add_ids = list(range(n + 1, n + 251))

    out = df[~df["id"].isin(remove_ids)].copy()
    if change_ids:
        out.loc[out["id"].isin(change_ids), "active"] = False
        out.loc[out["id"].isin(change_ids), "score"] = out.loc[
            out["id"].isin(change_ids), "score"
        ] + 9999

    add_df = pd.DataFrame(
        {
            "id": add_ids,
            "email": [f"new{i}@example.com" for i in add_ids],
            "country": ["US"] * len(add_ids),
            "active": [True] * len(add_ids),
            "score": [1] * len(add_ids),
        }
    )
    out = pd.concat([out, add_df], ignore_index=True)
    return out


def _benchmarks_for_size(n: int, warmup: int, runs: int, *, wide_cols: int = 0) -> list[BenchmarkResult]:
    if wide_cols:
        df = _make_wide_df(n, wide_cols=wide_cols)
    else:
        df = _make_df(n)
    df2 = _mutate_df(df)

    results: list[BenchmarkResult] = []

    results.append(
        _run_bench(
            "content_fingerprint_rowhash" + (f"_wide{wide_cols}" if wide_cols else ""),
            n_rows=len(df),
            n_cols=df.shape[1],
            fn=lambda: content_fingerprint_rowhash(df, order_sensitive=False, sample_rows=0),
            warmup=warmup,
            runs=runs,
            rows_for_rate=len(df),
        )
    )

    results.append(
        _run_bench(
            "diff_rowhash" + (f"_wide{wide_cols}" if wide_cols else ""),
            n_rows=len(df),
            n_cols=df.shape[1],
            fn=lambda: diff_rowhash(df, df2, primary_key=["id"], order_sensitive=False),
            warmup=warmup,
            runs=runs,
            rows_for_rate=len(df),
        )
    )

    with tempfile.TemporaryDirectory() as td:
        store = Store.local(td)
        rec = Recorder(
            store=store,
            project="bench",
            dataset="snapshot",
            diff=DiffConfig(mode="rowhash", primary_key=["id"]),
            snapshot=SnapshotConfig(mode="auto", max_mb=0.6),
            seal=SealConfig(mode="none"),
        )
        run = rec.start_run()

        results.append(
            _run_bench(
            "snapshot_maybe_write_df_artifact" + (f"_wide{wide_cols}" if wide_cols else ""),
                n_rows=len(df),
                n_cols=df.shape[1],
                fn=lambda: run._maybe_write_df_artifact("steps/0001_x/artifacts/input.bbdata", df),
                warmup=warmup,
                runs=runs,
                rows_for_rate=len(df),
            )
        )

    return results


def _print_table(results: list[BenchmarkResult]) -> None:
    headers = [
        "benchmark",
        "rows",
        "cols",
        "mean_ms",
        "median_ms",
        "p95_ms",
        "min_ms",
        "max_ms",
        "rows/sec",
    ]
    print("\t".join(headers))
    for r in results:
        print(
            "\t".join(
                [
                    r.name,
                    str(r.n_rows),
                    str(r.n_cols),
                    f"{r.mean_ms:.2f}",
                    f"{r.median_ms:.2f}",
                    f"{r.p95_ms:.2f}",
                    f"{r.min_ms:.2f}",
                    f"{r.max_ms:.2f}",
                    f"{r.rows_per_sec:,.0f}",
                ]
            )
        )


def _write_csv(path: str, results: list[BenchmarkResult]) -> None:
    import csv
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "name",
                "n_rows",
                "n_cols",
                "runs",
                "warmup",
                "min_ms",
                "max_ms",
                "mean_ms",
                "median_ms",
                "p95_ms",
                "rows_per_sec",
            ]
        )
        for r in results:
            writer.writerow(
                [
                    r.name,
                    r.n_rows,
                    r.n_cols,
                    r.runs,
                    r.warmup,
                    f"{r.min_ms:.6f}",
                    f"{r.max_ms:.6f}",
                    f"{r.mean_ms:.6f}",
                    f"{r.median_ms:.6f}",
                    f"{r.p95_ms:.6f}",
                    f"{r.rows_per_sec:.6f}",
                ]
            )


def main() -> int:
    parser = argparse.ArgumentParser(description="Blackbox Data micro-benchmarks")
    parser.add_argument(
        "--sizes",
        default="10000,100000,250000,500000,1000000",
        help="Comma-separated row counts to benchmark",
    )
    parser.add_argument(
        "--wide-cols",
        type=int,
        default=0,
        help="If >0, add this many extra columns for a wide-frame benchmark",
    )
    parser.add_argument("--warmup", type=int, default=2)
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument(
        "--output",
        default=os.path.join("benchmarks", "results.json"),
        help="Output JSON path",
    )
    parser.add_argument(
        "--output-csv",
        default=os.path.join("benchmarks", "results.csv"),
        help="Output CSV path",
    )
    args = parser.parse_args()

    sizes = [int(s.strip()) for s in args.sizes.split(",") if s.strip()]
    all_results: list[BenchmarkResult] = []

    for n in sizes:
        all_results.extend(_benchmarks_for_size(n, warmup=args.warmup, runs=args.runs))
        if args.wide_cols and args.wide_cols > 0:
            all_results.extend(
                _benchmarks_for_size(n, warmup=args.warmup, runs=args.runs, wide_cols=args.wide_cols)
            )

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    payload = [asdict(r) for r in all_results]
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    _write_csv(args.output_csv, all_results)

    print("Benchmark results written to:", args.output)
    print("CSV results written to:", args.output_csv)
    _print_table(all_results)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
