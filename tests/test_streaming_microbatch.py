import pandas as pd

from blackbox import Recorder, Store, DiffConfig, SnapshotConfig, SealConfig


def test_streaming_microbatch(tmp_path):
    store = Store.local(str(tmp_path))
    rec = Recorder(
        store=store,
        project="acme-data",
        dataset="stream_demo",
        diff=DiffConfig(mode="rowhash", primary_key=["id"]),
        snapshot=SnapshotConfig(mode="none"),
        seal=SealConfig(mode="chain"),
    )

    stream = rec.start_stream(tags={"env": "test"})

    batch1 = pd.DataFrame({"id": [1, 2, 3], "score": [10, 20, 30]})
    stream.push("ingest", batch1, window={"start": "t0", "end": "t1"})

    batch2 = pd.DataFrame({"id": [2, 3, 4], "score": [22, 30, 44]})
    stream.push("ingest", batch2, window={"start": "t1", "end": "t2"})

    stream.finish()
    ok, msg = stream.verify()
    assert ok, msg

    keys = store.list("acme-data/stream_demo/" + stream.run_id)
    assert any(k.endswith("/step.json") for k in keys)
    assert any(k.endswith("/diff.bbdelta") for k in keys)
