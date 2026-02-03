import json
import pandas as pd
from blackbox import Recorder, Store, DiffConfig, SnapshotConfig, SealConfig

def test_high_level_report(tmp_path):
    root = str(tmp_path)
    store = Store.local(root)

    rec = Recorder(
        store=store,
        project="acme-data",
        dataset="pipeline_forensics",
        diff=DiffConfig(mode="rowhash", primary_key=["id"], order_sensitive=False),
        snapshot=SnapshotConfig(mode="auto", max_mb=0),  # force snapshots to skip by size
        seal=SealConfig(mode="chain"),
    )

    run = rec.start_run(tags={"env": "test", "suite": "high_level"})

    # Step 1: small DF (snapshot will still be skipped because max_mb=0)
    df = pd.DataFrame({"id": [1, 2, 3], "val": [10, 20, 30]})
    with run.step("small_transform", input_df=df) as st:
        out = df.copy()
        out["val"] = out["val"] + 1
        run.add_event("metric", "rows_in", data={"rows": len(df)})
        st.capture_output(out)
        df = out

    # Step 2: PK change detection
    with run.step("pk_change", input_df=df) as st:
        out = df.copy()
        out.loc[out["id"] == 2, "val"] = 999          # changed key '2'
        out = pd.concat([out, pd.DataFrame({"id": [4], "val": [40]})], ignore_index=True)  # added key '4'
        out = out[out["id"] != 1]                     # removed key '1'
        run.add_event("info", "introduced pk changes", data={})
        st.capture_output(out)
        df = out

    # Step 3: larger DF (ensure snapshot skip path stable)
    big = pd.DataFrame({"id": range(1, 200000), "val": range(1, 200000)})
    with run.step("big_step", input_df=big) as st:
        out = big.copy()
        out["val"] = out["val"] * 2
        st.capture_output(out)

    run.finish(status="ok")

    ok, msg = run.verify()
    assert ok, msg

    # Load run + steps for report
    run_key = f"acme-data/pipeline_forensics/{run.run_id}/run.json"
    run_obj = store.get_json(run_key)

    steps = []
    for s in run_obj.get("steps", []):
        step_obj = store.get_json(f"acme-data/pipeline_forensics/{run.run_id}/" + s["path"])
        steps.append(step_obj)

    print("\n=== BLACKBOX HIGH-LEVEL REPORT ===")
    print("run_id:", run.run_id)
    print("status:", run_obj.get("status"), "created:", run_obj.get("created_at"), "finished:", run_obj.get("finished_at"))
    print("seal_head:", (run_obj.get("seal") or {}).get("head"))

    for st in steps:
        name = st["name"]
        inp = st.get("input") or {}
        outp = st.get("output") or {}
        d = st.get("diff") or {}
        print(f"\n-- step: {name} --")
        print(" code:", st.get("code"))
        print(" input rows/cols:", inp.get("n_rows"), inp.get("n_cols"), "artifact:", inp.get("artifact"), "skip:", inp.get("snapshot_skipped"))
        print(" output rows/cols:", outp.get("n_rows"), outp.get("n_cols"), "artifact:", outp.get("artifact"), "skip:", outp.get("snapshot_skipped"))
        if d:
            print(" diff summary:", d.get("summary"), "artifact:", d.get("artifact"))
            # open diff payload for pk step only
            if name == "pk_change":
                diff_key = f"acme-data/pipeline_forensics/{run.run_id}/steps/0002_pk_change/artifacts/diff.bbdelta"
                diff_payload = store.get_json(diff_key)
                print(" pk diff keys:", {
                    "added_keys": diff_payload.get("added_keys"),
                    "removed_keys": diff_payload.get("removed_keys"),
                    "changed_keys": diff_payload.get("changed_keys"),
                })

    # Events sanity
    ev_key = f"acme-data/pipeline_forensics/{run.run_id}/events.jsonl"
    ev_raw = store.get_bytes(ev_key).decode("utf-8").strip().splitlines()
    events = [json.loads(line) for line in ev_raw if line.strip()]
    print("\nEvents:", len(events), "sample:", events[:2])

