# examples/simple_pipeline.py
from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

from blackbox import Recorder, Store


def make_source_df(n: int = 50_000) -> pd.DataFrame:
    # Simple, deterministic-ish demo data
    return pd.DataFrame(
        {
            "id": range(1, n + 1),
            "email": [f"user{i}@example.com" for i in range(1, n + 1)],
            "country": ["AU" if i % 3 == 0 else "US" if i % 3 == 1 else "GB" for i in range(1, n + 1)],
            "active": [(i % 2) == 0 for i in range(1, n + 1)],
            "score": [(i * 7) % 100 for i in range(1, n + 1)],
        }
    )


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["email"] = out["email"].astype("string").str.lower().str.strip()
    out["country"] = out["country"].astype("string")
    out["score"] = pd.to_numeric(out["score"], errors="coerce").fillna(0).astype("int64")
    return out


def pk_mutations(df: pd.DataFrame) -> pd.DataFrame:
    """
    Simulate realistic row-level changes:
    - remove some rows
    - add some new rows
    - modify some existing rows
    """
    out = df.copy()

    # Remove 200 rows (ids 1..200)
    out = out[out["id"] > 200].copy()

    # Modify 300 rows (ids 10_001..10_300) - change score
    mask = (out["id"] >= 10_001) & (out["id"] <= 10_300)
    out.loc[mask, "score"] = (out.loc[mask, "score"] + 17) % 100

    # Add 150 new rows
    max_id = int(out["id"].max())
    add_n = 150
    new_ids = range(max_id + 1, max_id + add_n + 1)
    df_new = pd.DataFrame(
        {
            "id": list(new_ids),
            "email": [f"new{i}@example.com" for i in new_ids],
            "country": ["AU"] * add_n,
            "active": [True] * add_n,
            "score": [42] * add_n,
        }
    )

    out = pd.concat([out, df_new], ignore_index=True)
    return out


def big_feature(df: pd.DataFrame) -> pd.DataFrame:
    """
    Simulate a feature engineering step that:
    - adds columns (schema drift)
    - does not necessarily change rows
    """
    out = df.copy()

    # Add a bucket (schema change)
    out["score_bucket"] = pd.cut(
        out["score"],
        bins=[-1, 20, 50, 80, 100],
        labels=["low", "mid", "high", "top"],
    ).astype("string")

    # Add a simple (non-crypto) hash-like column for demo purposes
    out["email_hash"] = out["email"].astype("string").map(lambda s: str(abs(hash(s)) % (10**12)))

    # Add a "blob" column to show why snapshots might be skipped in auto mode
    out["blob"] = ("x" * 256)

    return out


def main() -> int:
    store_root = Path("./.blackbox_store")
    store_root.mkdir(parents=True, exist_ok=True)

    store = Store.local(str(store_root))
    rec = Recorder(store=store, project="demo", dataset="simple_pipeline")

    df_raw = make_source_df(n=int(os.environ.get("BB_N", "50000")))

    run = rec.start_run(tags={"purpose": "readme_example"})

    with run.step("normalize", input_df=df_raw) as step:
        df1 = normalize(df_raw)
        step.capture_output(df1)

    with run.step("pk_mutations", input_df=df1) as step:
        df2 = pk_mutations(df1)
        step.capture_output(df2)

    with run.step("big_feature", input_df=df2) as step:
        df3 = big_feature(df2)
        step.capture_output(df3)

    run.finish()

    ok, msg = run.verify()

    print("\n=== BLACKBOX DATA: EXAMPLE COMPLETE ===")
    print("run_id:", run.run_id)
    print("verify:", ok, msg)
    print("\nNext commands:")
    print(f"  blackbox --root {store_root} list --project demo --dataset simple_pipeline")
    print(f"  blackbox --root {store_root} report --project demo --dataset simple_pipeline --run-id {run.run_id}")
    print(f"  blackbox --root {store_root} report --project demo --dataset simple_pipeline --run-id {run.run_id} -v")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

