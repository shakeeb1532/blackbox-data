# Spark / Distributed Engines (Experimental)

Blackbox Data v1.0 is pandas‑first, but you can use Spark/Polars/DuckDB by
converting results to pandas at step boundaries. This enables diffs and
audit trails while you validate demand for full distributed support.

## Spark (micro‑batch style)
```python
from pyspark.sql import SparkSession
import pandas as pd
from blackbox import Recorder, Store, DiffConfig, SnapshotConfig, SealConfig

spark = SparkSession.builder.getOrCreate()
store = Store.local("./.blackbox_store")
rec = Recorder(
    store=store,
    project="acme-data",
    dataset="spark_demo",
    diff=DiffConfig(mode="rowhash", primary_key=["id"]),
    snapshot=SnapshotConfig(mode="auto", max_mb=50),
    seal=SealConfig(mode="chain"),
)

run = rec.start_run(tags={"engine": "spark"})

df0 = spark.createDataFrame([(1, 10), (2, 20), (3, 30)], ["id", "score"])
with run.step("normalize", input_df=df0) as st:
    df1 = df0.withColumn("score", df0.score / 10.0)
    st.capture_output(df1)

run.finish()
ok, msg = run.verify()
print(ok, msg)
```

## How it works
Non‑pandas dataframes are converted using common methods:
- Spark: `toPandas()`
- Polars: `to_pandas()`
- DuckDB: `to_df()`

## Notes and caveats
- Converting large Spark dataframes to pandas is expensive.
- For large datasets, use chunked diffing and summary‑only thresholds.
- Full distributed engine support is on the roadmap.
