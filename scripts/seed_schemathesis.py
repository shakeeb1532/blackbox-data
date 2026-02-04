from __future__ import annotations

import os
import pandas as pd

from blackbox import Recorder, Store, DiffConfig, SnapshotConfig, SealConfig


def main() -> int:
    root = os.environ.get("BLACKBOX_PRO_ROOT", "./.blackbox_store")
    project = os.environ.get("SCHEMA_PROJECT", "acme-data")
    dataset = os.environ.get("SCHEMA_DATASET", "demo")

    store = Store.local(root)
    rec = Recorder(
        store=store,
        project=project,
        dataset=dataset,
        diff=DiffConfig(mode="rowhash", primary_key=["id"]),
        snapshot=SnapshotConfig(mode="auto", max_mb=5),
        seal=SealConfig(mode="chain"),
    )

    run = rec.start_run(tags={"env": "schemathesis"})
    with run.step("seed") as st:
        df = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        st.capture_output(df)
    run.finish()

    print(f"SCHEMA_PROJECT={project}")
    print(f"SCHEMA_DATASET={dataset}")
    print(f"SCHEMA_RUN_ID={run.run_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
