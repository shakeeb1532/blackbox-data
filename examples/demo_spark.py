from __future__ import annotations

try:
    from pyspark.sql import SparkSession
except Exception as e:
    raise SystemExit(
        "PySpark is required for this demo. Install it with:\n"
        "  pip install pyspark\n"
        f"Original error: {e}"
    )

from blackbox import Recorder, Store, DiffConfig, SnapshotConfig, SealConfig


def main() -> None:
    spark = SparkSession.builder.appName("blackbox-spark-demo").getOrCreate()

    store = Store.local("./.blackbox_store")
    rec = Recorder(
        store=store,
        project="acme-data",
        dataset="spark_demo",
        diff=DiffConfig(mode="rowhash", primary_key=["id"]),
        snapshot=SnapshotConfig(mode="auto", max_mb=5),
        seal=SealConfig(mode="chain"),
    )

    run = rec.start_run(tags={"env": "local", "demo": "true", "engine": "spark"})

    df0 = spark.createDataFrame([(1, 10), (2, 20), (3, 30)], ["id", "score"])
    with run.step("normalize", input_df=df0) as st:
        df1 = df0.withColumn("score", df0.score / 10.0)
        st.capture_output(df1)

    with run.step("mutate_rows", input_df=df1) as st:
        df2 = df1.filter("id != 3").union(
            spark.createDataFrame([(4, 4.4)], ["id", "score"])
        )
        st.capture_output(df2)

    run.finish()
    ok, msg = run.verify()
    print("run_id:", run.run_id)
    print("verify:", ok, msg)

    spark.stop()


if __name__ == "__main__":
    main()
