# Streaming (Micro‑Batch) Guide

Blackbox Data v1.0 is batch‑first, but you can model streaming via micro‑batches.
Each batch becomes a step and is diffed against the previous batch output.

## Example
```python
import pandas as pd
from blackbox import Recorder, Store, DiffConfig, SnapshotConfig, SealConfig

store = Store.local("./.blackbox_store")
rec = Recorder(
    store=store,
    project="acme-data",
    dataset="stream_demo",
    diff=DiffConfig(mode="rowhash", primary_key=["id"]),
    snapshot=SnapshotConfig(mode="auto", max_mb=50),
    seal=SealConfig(mode="chain"),
)

stream = rec.start_stream(tags={"env": "dev"})

batch1 = pd.DataFrame({"id": [1, 2, 3], "score": [10, 20, 30]})
stream.push("ingest", batch1, window={"start": "2026-02-04T00:00:00Z", "end": "2026-02-04T00:05:00Z"})

batch2 = pd.DataFrame({"id": [2, 3, 4], "score": [22, 30, 44]})
stream.push("ingest", batch2, window={"start": "2026-02-04T00:05:00Z", "end": "2026-02-04T00:10:00Z"})

stream.finish()
ok, msg = stream.verify()
print(ok, msg)
```

## Notes
- The first batch has no input and therefore no row diff.
- Subsequent batches show added/removed/changed keys.
- Use `summary_only_threshold` for high‑churn streams.
