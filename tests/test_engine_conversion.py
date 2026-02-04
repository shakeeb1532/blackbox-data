import pandas as pd

from blackbox import Recorder, Store, DiffConfig, SnapshotConfig, SealConfig


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
