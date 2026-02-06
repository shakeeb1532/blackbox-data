import pandas as pd
from blackbox import Recorder, Store, DiffConfig, SnapshotConfig, SealConfig


def main() -> None:
    store = Store.local("./.blackbox_store")
    rec = Recorder(
        store=store,
        project="acme-data",
        dataset="demo",
        diff=DiffConfig(mode="rowhash", primary_key=["id"], diff_mode="rows"),
        snapshot=SnapshotConfig(mode="auto", max_mb=5),
        seal=SealConfig(mode="chain"),
    )

    run = rec.start_run(tags={"env": "local", "demo": "true"})
    df = pd.DataFrame({"id": [1, 2, 3], "score": [10, 20, 30]})

    with run.step("normalize", input_df=df) as st:
        out = df.copy()
        out["score"] = out["score"] / 10.0
        st.capture_output(out)

    # Mutations to demonstrate added/removed/changed keys + schema changes.
    with run.step("mutate_rows", input_df=out) as st:
        out2 = out.copy()
        # change an existing value
        out2.loc[out2["id"] == 2, "score"] = 9.9
        # remove a row
        out2 = out2[out2["id"] != 3].copy()
        # add a new row
        out2 = pd.concat(
            [out2, pd.DataFrame([{"id": 4, "score": 4.4}])],
            ignore_index=True,
        )
        st.capture_output(out2)

    with run.step("add_columns", input_df=out2) as st:
        out3 = out2.copy()
        out3["tier"] = pd.cut(out3["score"], bins=[-1, 3, 7, 11], labels=["low", "mid", "high"])
        out3["score_bucket"] = (out3["score"] * 10).round(0).astype(int)
        st.capture_output(out3)

    with run.step("schema_only", input_df=out3) as st:
        # schema change without row changes (rename column)
        out4 = out3.rename(columns={"score_bucket": "score_band"})
        st.capture_output(out4)

    run.finish()
    ok, msg = run.verify()
    print("run_id:", run.run_id)
    print("verify:", ok, msg)

    # High-churn demo: force summary-only diffs
    rec_churn = Recorder(
        store=store,
        project="acme-data",
        dataset="demo",
        diff=DiffConfig(mode="rowhash", primary_key=["id"], diff_mode="rows", summary_only_threshold=0.1),
        snapshot=SnapshotConfig(mode="auto", max_mb=5),
        seal=SealConfig(mode="chain"),
    )
    run_churn = rec_churn.start_run(tags={"env": "local", "demo": "true", "scenario": "high_churn"})
    df_big = pd.DataFrame({"id": list(range(1, 101)), "score": list(range(100))})
    with run_churn.step("high_churn", input_df=df_big) as st:
        # remove many rows + add many rows
        out_big = df_big[df_big["id"] % 2 == 0].copy()
        add_df = pd.DataFrame({"id": list(range(200, 260)), "score": [5] * 60})
        out_big = pd.concat([out_big, add_df], ignore_index=True)
        st.capture_output(out_big)
    run_churn.finish()
    print("run_id (high_churn):", run_churn.run_id)

    # Failed run demo
    run_fail = rec.start_run(tags={"env": "local", "demo": "true", "scenario": "failure"})
    try:
        with run_fail.step("failing_step", input_df=df) as st:
            raise RuntimeError("demo failure")
    except Exception:
        pass
    run_fail.finish()
    print("run_id (failure):", run_fail.run_id)

    # Tamper demo: modify run_finish.json to break chain
    run_tamper = rec.start_run(tags={"env": "local", "demo": "true", "scenario": "tamper"})
    with run_tamper.step("clean_step", input_df=df) as st:
        st.capture_output(df.copy())
    run_tamper.finish()
    prefix = f"acme-data/demo/{run_tamper.run_id}"
    run_key = f"{prefix}/run_finish.json"
    obj = store.get_json(run_key)
    obj["status"] = "tampered"
    store.put_json(run_key, obj)
    ok2, msg2 = run_tamper.verify()
    print("run_id (tamper):", run_tamper.run_id)
    print("verify (tamper):", ok2, msg2)


if __name__ == "__main__":
    main()
