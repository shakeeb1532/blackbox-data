import time
import shutil
from pathlib import Path
import pandas as pd

from blackbox import Recorder, Store, DiffConfig, SnapshotConfig, SealConfig


def _now_ms() -> float:
    return time.perf_counter() * 1000.0


def test_mvp_effectiveness_report():
    """
    Run:
      python3 -m pytest -q -s tests/test_mvp_effectiveness_report.py

    Writes artifacts to a stable directory:
      ./.blackbox_store

    So you can run:
      blackbox --root ./.blackbox_store list --project acme-data --dataset mvp_eval
      blackbox --root ./.blackbox_store report --project acme-data --dataset mvp_eval --run-id <real_run_id>
    """
    store_root = Path(".blackbox_store").resolve()

    # Clean old runs (optional, but reduces confusion)
    if store_root.exists():
        shutil.rmtree(store_root)
    store_root.mkdir(parents=True, exist_ok=True)

    store = Store.local(str(store_root))

    rec = Recorder(
        store=store,
        project="acme-data",
        dataset="mvp_eval",
        diff=DiffConfig(mode="rowhash", primary_key=["id"], order_sensitive=False),
        snapshot=SnapshotConfig(mode="auto", max_mb=0.6),
        seal=SealConfig(mode="chain"),
    )

    n = 120_000
    df = pd.DataFrame({
        "id": range(1, n + 1),
        "email": [f"User{i}@Example.COM" for i in range(1, n + 1)],
        "country": ["AU"] * n,
        "active": [True] * n,
        "score": list(range(n)),
    })

    run = rec.start_run(tags={"suite": "mvp_effectiveness", "rows": str(n)})

    # Step 1: normalization (must change all rows)
    t0 = _now_ms()
    with run.step("normalize", input_df=df) as st:
        out = df.copy()
        out["email"] = out["email"].str.lower()
        st.capture_output(out)
        df = out
    t1 = _now_ms()

    # Step 2: targeted PK changes
    remove_ids = set(range(1, 301))
    change_ids = set(range(10_000, 10_400))
    add_ids = list(range(n + 1, n + 251))

    t2 = _now_ms()
    with run.step("pk_mutations", input_df=df) as st:
        out = df[~df["id"].isin(remove_ids)].copy()
        out.loc[out["id"].isin(change_ids), "active"] = False
        out.loc[out["id"].isin(change_ids), "score"] = out.loc[out["id"].isin(change_ids), "score"] + 9999

        add_df = pd.DataFrame({
            "id": add_ids,
            "email": [f"new{i}@example.com" for i in add_ids],
            "country": ["AU"] * len(add_ids),
            "active": [True] * len(add_ids),
            "score": [1] * len(add_ids),
        })
        out = pd.concat([out, add_df], ignore_index=True)
        st.capture_output(out)
        df = out
    t3 = _now_ms()

    # Step 3: schema evolution + large column (forces snapshot skip)
    big_text = "X" * 500
    t4 = _now_ms()
    with run.step("big_feature", input_df=df) as st:
        out = df.copy()
        out["score_bucket"] = (out["score"] // 1000).astype("int32")
        out["email_hash"] = out["email"].str.len().astype("int16")
        out["blob"] = big_text
        st.capture_output(out)
        df = out
    t5 = _now_ms()

    run.finish(status="ok")

    ok, msg = run.verify()
    assert ok, msg

    run_prefix = f"acme-data/mvp_eval/{run.run_id}"
    run_obj = store.get_json(f"{run_prefix}/run.json")
    chain_obj = store.get_json(f"{run_prefix}/chain.json")

    print("\n================ MVP EFFECTIVENESS REPORT (STABLE STORE ROOT) ================")
    print("STORE_ROOT:", str(store_root))
    print("run_id:", run.run_id)
    print("rows_initial:", n)
    print("status:", run_obj.get("status"))
    print("created_at:", run_obj.get("created_at"))
    print("finished_at:", run_obj.get("finished_at"))
    print("chain_entries:", len(chain_obj.get("entries", [])))
    print("chain_head:", chain_obj.get("head"))
    print("verify:", ok, msg)

    print("\n--- Performance (rough, local) ---")
    print(f"normalize step ms:   {t1 - t0:,.1f}")
    print(f"pk_mutations ms:     {t3 - t2:,.1f}")
    print(f"big_feature ms:      {t5 - t4:,.1f}")
    print("=============================================================================\n")

