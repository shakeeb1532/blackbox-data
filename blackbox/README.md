# blackbox (core engine)

Core library for recording, diffing, snapshotting, and sealing pipeline steps.

Key areas:
- `recorder.py`: run/step lifecycle and artifact writing
- `hashing.py`: row hashing + fingerprints + diffing
- `seal.py`: hash chain integrity verification
- `store.py`: storage backends (local + S3)
- `engines.py`: dataframe conversions (pandas + adapters)

Use this package when embedding Blackbox into Python pipelines.

DuckDB helper:
```python
import duckdb
from blackbox import Recorder, Store

conn = duckdb.connect()
conn.execute("create table t as select 1 as id")
run = Recorder(Store.local("./.blackbox_store"), "p", "d").start_run()
run.step_sql("duckdb_query", conn=conn, sql="select * from t")
run.finish()
```
