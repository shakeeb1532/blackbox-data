import pandas as pd
from blackbox import Recorder, Store, DiffConfig, SnapshotConfig, SealConfig


def main() -> None:
    store = Store.local("./.blackbox_store")
    rec = Recorder(
        store=store,
        project="acme-data",
        dataset="stream_demo",
        diff=DiffConfig(mode="rowhash", primary_key=["id"]),
        snapshot=SnapshotConfig(mode="auto", max_mb=5),
        seal=SealConfig(mode="chain"),
    )

    stream = rec.start_stream(tags={"env": "local", "demo": "true", "stream": "true"})

    batch1 = pd.DataFrame({"id": [1, 2, 3], "score": [10, 20, 30]})
    stream.push("ingest", batch1, window={"start": "2026-02-04T00:00:00Z", "end": "2026-02-04T00:05:00Z"})

    batch2 = pd.DataFrame({"id": [2, 3, 4], "score": [22, 30, 44]})
    stream.push("ingest", batch2, window={"start": "2026-02-04T00:05:00Z", "end": "2026-02-04T00:10:00Z"})

    batch3 = pd.DataFrame({"id": [2, 4, 5], "score": [23, 40, 55]})
    stream.push("ingest", batch3, window={"start": "2026-02-04T00:10:00Z", "end": "2026-02-04T00:15:00Z"})

    stream.finish()
    ok, msg = stream.verify()
    print("run_id:", stream.run_id)
    print("verify:", ok, msg)


if __name__ == "__main__":
    main()
