import pandas as pd
from blackbox import Recorder, Store, DiffConfig, SnapshotConfig, SealConfig

def test_end_to_end_local(tmp_path):
    store = Store.local(str(tmp_path))
    rec = Recorder(
        store=store,
        project="acme-data",
        dataset="users_daily",
        diff=DiffConfig(mode="rowhash"),
        snapshot=SnapshotConfig(mode="auto", max_mb=50),
        seal=SealConfig(mode="chain"),
    )

    run = rec.start_run(tags={"env":"test"})

    df = pd.DataFrame({"user_id":[1,2,3], "email":["a@x","b@x", None]})

    with run.step("normalize", input_df=df) as st:
        out = df.copy()
        out["email"] = out["email"].fillna("")
        st.capture_output(out)
        df = out

    run.finish(status="ok")

    # ensure files exist
    keys = store.list("acme-data/users_daily/" + run.run_id)
    assert any(k.endswith("/run.json") for k in keys)
    assert any(k.endswith("/chain.json") for k in keys)
    assert any(k.endswith("/step.json") for k in keys)
    assert any(k.endswith("/input.bbdata") for k in keys)
    assert any(k.endswith("/output.bbdata") for k in keys)
    assert any(k.endswith("/diff.bbdelta") for k in keys)

    ok, msg = run.verify()
    assert ok, msg

