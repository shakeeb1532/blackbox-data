import pandas as pd

from blackbox import Recorder, Store, SealConfig


def test_chain_detects_tamper(tmp_path):
    store = Store.local(str(tmp_path))
    rec = Recorder(store=store, project="p", dataset="d", seal=SealConfig(mode="chain"))
    run = rec.start_run()

    df = pd.DataFrame({"x": [1, 2]})
    with run.step("s1", input_df=df) as st:
        df2 = pd.DataFrame({"x": [1, 2, 3]})
        st.capture_output(df2)

    run.finish()

    ok, msg = run.verify()
    assert ok, msg

    # Tamper with an IMMUTABLE evidence file that is chained
    # (run_finish.json is written once and referenced by the chain entry run_finish)
    run_key = f"p/d/{run.run_id}/run_finish.json"
    run_obj = store.get_json(run_key)
    run_obj["status"] = "tampered"
    store.put_json(run_key, run_obj)

    ok2, msg2 = run.verify()
    assert not ok2
    assert "Payload digest mismatch" in msg2

