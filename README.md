# Blackbox Data

Blackbox Data is a lightweight, tamper-evident forensic recorder for pandas data pipelines. It captures what changed between steps, stores optional artifacts, and produces a verifiable audit trail.

## Why It Exists
- You need step-by-step change evidence for datasets.
- You want reproducible, investigation-friendly logs.
- You need tamper-evident integrity for pipeline output.

## Core Features
- Run and step metadata (`run.json`, `step.json`).
- Schema diff (added, removed, dtype changes).
- Rowhash diff (added, removed, changed keys).
- Snapshot artifacts (Parquet) with size controls.
- Tamper-evident hash chain.
- CLI for listing, verification, and reports.

## Quickstart
```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -U pip
python3 -m pip install -e .
```

```python
import pandas as pd
from blackbox import Recorder, Store, DiffConfig, SnapshotConfig, SealConfig, RecorderConfig

store = Store.local("./.blackbox_store")
rec = Recorder(
    store=store,
    project="acme-data",
    dataset="demo",
    diff=DiffConfig(mode="rowhash", primary_key=["id"], diff_mode="rows"),
    snapshot=SnapshotConfig(mode="auto", max_mb=5),
    seal=SealConfig(mode="chain"),
    config=RecorderConfig(parquet_compression="zstd"),
)

run = rec.start_run(tags={"env": "local"})

df0 = pd.DataFrame({"id": [1, 2, 3], "score": [10, 20, 30]})
with run.step("normalize", input_df=df0) as st:
    df1 = df0.copy()
    df1["score"] = df1["score"] / 10.0
    st.capture_output(df1)

run.finish()
ok, msg = run.verify()
print(ok, msg)
```

## CLI
List runs:
```bash
blackbox --root ./.blackbox_store list --project acme-data --dataset demo
```

Report:
```bash
blackbox --root ./.blackbox_store report --project acme-data --dataset demo --run-id <RUN_ID>
```

Verify:
```bash
blackbox --root ./.blackbox_store verify --project acme-data --dataset demo --run-id <RUN_ID>
```

## Diff Controls
- `DiffConfig.diff_mode`: `rows` (default), `schema`, `keys-only`.
- `DiffConfig.summary_only_threshold`: summarize only when churn is high.
- `DiffConfig.chunk_rows`: chunked diffing for large datasets.
- `DiffConfig.hash_group_size` and `DiffConfig.parallel_groups`: wide-frame hashing.
- `DiffConfig.cache_rowhash`: reuse rowhashes between adjacent steps.

## Snapshot Controls
- `SnapshotConfig.mode`: `none`, `auto`, `always`.
- `SnapshotConfig.max_mb`: max artifact size when `auto`.
- `RecorderConfig.parquet_compression`: `snappy`, `zstd`, `gzip`, `lz4`, `none`.
- `RecorderConfig.snapshot_async`: background snapshot writes for smoother UX.

## Performance Envelope
Optimized for up to ~1M rows per step. Larger datasets are supported via chunking.
Use `DiffConfig.chunk_rows` (for example, `250_000`) to reduce peak memory and improve progress reporting.

## Positioning vs Adjacent Tools
Blackbox Data focuses on forensic change evidence for pandas pipelines. It is complementary to adjacent categories such as:
- Data validation suites (for example, Great Expectations).
- Data version control tools (for example, DVC, lakeFS).
- Lineage/event standards (for example, OpenLineage).
- Data observability platforms.

If you already use any of these, Blackbox Data can act as the step-level evidence layer in your pipeline.

## Local Store Layout
```
<root>/<project>/<dataset>/<run_id>/
  run.json
  run_start.json
  run_finish.json
  chain.json
  events.jsonl (optional)
  steps/<ordinal>_<name>/
    step.json
    artifacts/
      input*.bbdata
      output*.bbdata
      diff.bbdelta
```

## Benchmarks
See `benchmarks/README.md` for benchmark, load, stress, and security runs and CSV outputs.
