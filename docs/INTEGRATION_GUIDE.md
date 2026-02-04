# Integration Guide

This guide shows how to integrate Blackbox Data into existing pandas pipelines.

## Minimal Integration
```python
import pandas as pd
from blackbox import Recorder, Store, DiffConfig, SnapshotConfig, SealConfig

store = Store.local("./.blackbox_store")
rec = Recorder(
    store=store,
    project="acme-data",
    dataset="users_daily",
    diff=DiffConfig(mode="rowhash", primary_key=["id"]),
    snapshot=SnapshotConfig(mode="auto", max_mb=50),
    seal=SealConfig(mode="chain"),
)

run = rec.start_run(tags={"env": "prod"})

df0 = pd.DataFrame({"id": [1, 2, 3], "score": [10, 20, 30]})
with run.step("normalize", input_df=df0) as st:
    df1 = df0.copy()
    df1["score"] = df1["score"] / 10.0
    st.capture_output(df1)

run.finish()
ok, msg = run.verify()
print(ok, msg)
```

## Production Checklist
- Use stable `project` and `dataset` names.
- Set `primary_key` for reliable row diffs.
- Enable `snapshot_async` for low‑latency steps.
- Use `summary_only_threshold` to skip deep diff on high‑churn steps.
- Use `chunk_rows` for large datasets.

## Spark / Distributed Engines (Experimental)
For Spark/Polars/DuckDB, pass dataframe objects to steps and they will be converted
to pandas at step boundaries (see `docs/SPARK_GUIDE.md`).

## CLI and API Usage
List runs:
```bash
blackbox --root ./.blackbox_store list --project acme-data --dataset users_daily
```

Report:
```bash
blackbox --root ./.blackbox_store report --project acme-data --dataset users_daily --run-id <RUN_ID>
```

API (token):
```bash
curl -H "Authorization: Bearer dev-secret-token" \
  "http://127.0.0.1:8088/runs?project=acme-data&dataset=users_daily"
```
