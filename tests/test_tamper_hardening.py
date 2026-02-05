import json
import os

import pandas as pd

from blackbox import Recorder, Store, SealConfig, DiffConfig
from blackbox.seal import verify_chain_with_payloads


def _create_run(root: str) -> str:
    store = Store.local(root)
    rec = Recorder(
        store=store,
        project="p",
        dataset="d",
        diff=DiffConfig(mode="rowhash", primary_key=["id"]),
        seal=SealConfig(mode="chain"),
    )
    run = rec.start_run()
    df = pd.DataFrame({"id": [1, 2], "x": [10, 20]})
    with run.step("s1", input_df=df) as st:
        out = df.copy()
        out["x"] = out["x"] + 1
        st.capture_output(out)
    run.finish()
    return run.run_id


def test_chain_breaks_on_edit(tmp_path):
    root = str(tmp_path)
    store = Store.local(root)
    run_id = _create_run(root)

    chain_key = f"p/d/{run_id}/chain.json"
    chain_obj = store.get_json(chain_key)
    chain_obj["entries"][0]["payload_digest"] = "sha256:deadbeef"
    store.put_json(chain_key, chain_obj)

    prefix = f"p/d/{run_id}"
    chain_obj = store.get_json(f"{prefix}/chain.json")
    ok, msg = verify_chain_with_payloads(chain_obj, store, run_prefix=prefix)
    assert not ok
    assert "Payload digest mismatch" in msg


def test_verify_detects_missing(tmp_path):
    root = str(tmp_path)
    store = Store.local(root)
    run_id = _create_run(root)

    missing_path = os.path.join(root, "p", "d", run_id, "run_finish.json")
    os.remove(missing_path)

    prefix = f"p/d/{run_id}"
    chain_obj = store.get_json(f"{prefix}/chain.json")
    ok, msg = verify_chain_with_payloads(chain_obj, store, run_prefix=prefix)
    assert not ok
    assert "Failed to load payload" in msg


def test_step_artifact_tamper(tmp_path):
    root = str(tmp_path)
    store = Store.local(root)
    run_id = _create_run(root)

    run_obj = store.get_json(f"p/d/{run_id}/run.json")
    step_path = run_obj["steps"][0]["path"]
    step_key = f"p/d/{run_id}/{step_path}"
    step_obj = store.get_json(step_key)
    step_obj["status"] = "tampered"
    store.put_json(step_key, step_obj)

    prefix = f"p/d/{run_id}"
    chain_obj = store.get_json(f"{prefix}/chain.json")
    ok, msg = verify_chain_with_payloads(chain_obj, store, run_prefix=prefix)
    assert not ok
    assert "Payload digest mismatch" in msg
