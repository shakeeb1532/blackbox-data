import pandas as pd
import pytest

from blackbox import Recorder, Store, DiffConfig, SnapshotConfig, SealConfig
from blackbox.engines import duckdb_sql_to_pandas


class DummyFrame:
    def __init__(self, df: pd.DataFrame) -> None:
        self._df = df

    def to_pandas(self) -> pd.DataFrame:
        return self._df


def test_engine_conversion_to_pandas(tmp_path):
    store = Store.local(str(tmp_path))
    rec = Recorder(
        store=store,
        project="acme-data",
        dataset="engine_demo",
        diff=DiffConfig(mode="rowhash", primary_key=["id"]),
        snapshot=SnapshotConfig(mode="none"),
        seal=SealConfig(mode="chain"),
    )

    run = rec.start_run(tags={"env": "test"})
    df = pd.DataFrame({"id": [1, 2, 3], "score": [10, 20, 30]})
    input_df = DummyFrame(df)

    with run.step("normalize", input_df=input_df) as st:
        out = df.copy()
        out["score"] = out["score"] / 10.0
        st.capture_output(DummyFrame(out))

    run.finish()
    ok, msg = run.verify()
    assert ok, msg


def test_engine_polars_arrow_duckdb(tmp_path):
    polars = pytest.importorskip("polars")
    pyarrow = pytest.importorskip("pyarrow")
    duckdb = pytest.importorskip("duckdb")

    store = Store.local(str(tmp_path))
    rec = Recorder(
        store=store,
        project="acme-data",
        dataset="engine_demo",
        diff=DiffConfig(mode="rowhash", primary_key=["id"]),
        snapshot=SnapshotConfig(mode="none"),
        seal=SealConfig(mode="chain"),
    )

    run = rec.start_run(tags={"env": "test"})
    pdf = pd.DataFrame({"id": [1, 2, 3], "score": [10, 20, 30]})

    # Polars DataFrame
    pl_df = polars.DataFrame(pdf)
    with run.step("polars_df", input_df=pl_df) as st:
        st.capture_output(pl_df)

    # Polars LazyFrame
    pl_lazy = pl_df.lazy()
    with run.step("polars_lazy", input_df=pl_lazy) as st:
        st.capture_output(pl_lazy)

    # PyArrow Table
    pa_table = pyarrow.Table.from_pandas(pdf)
    with run.step("arrow_table", input_df=pa_table) as st:
        st.capture_output(pa_table)

    # PyArrow RecordBatch
    pa_batch = pyarrow.RecordBatch.from_pandas(pdf)
    with run.step("arrow_batch", input_df=pa_batch) as st:
        st.capture_output(pa_batch)

    # DuckDB relation via SQL
    conn = duckdb.connect()
    conn.register("t", pdf)
    out = duckdb_sql_to_pandas(conn, "select * from t")
    with run.step("duckdb_sql", input_df=pdf) as st:
        st.capture_output(out)

    run.finish()
    ok, msg = run.verify()
    assert ok, msg


def test_cli_wrap(tmp_path):
    from blackbox.cli import cmd_wrap
    import sys
    class Args:
        root = str(tmp_path)
        project = "acme"
        dataset = "demo"
        run_id = None
        name = "cmd"
        cmd = [sys.executable, "-c", "print('hello')"]
    rc = cmd_wrap(Args())
    assert rc == 0
