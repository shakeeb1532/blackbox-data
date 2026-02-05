import os
import json

import pandas as pd

from blackbox import Recorder, Store, DiffConfig, SnapshotConfig, SealConfig
from blackbox.integrations.airflow import blackbox_task
from blackbox.integrations.dagster import blackbox_op
from blackbox.integrations.dbt import collect_dbt_artifacts
from blackbox.seal import verify_chain_with_payloads


def _latest_run_id(store: Store, project: str, dataset: str) -> str:
    runs = store.list_dirs(f"{project}/{dataset}")
    assert runs, "No runs found"
    return sorted(runs)[-1]


def test_airflow_wrapper_records_run(tmp_path):
    store = Store.local(str(tmp_path))
    rec = Recorder(
        store=store,
        project="acme",
        dataset="airflow",
        diff=DiffConfig(mode="rowhash", primary_key=["id"]),
        snapshot=SnapshotConfig(mode="none"),
        seal=SealConfig(mode="chain"),
    )

    def _task():
        return pd.DataFrame({"id": [1, 2], "x": [10, 20]})

    wrapped = blackbox_task(rec, "task1", _task)
    out = wrapped()
    assert isinstance(out, pd.DataFrame)

    run_id = _latest_run_id(store, "acme", "airflow")
    prefix = f"acme/airflow/{run_id}"
    chain = store.get_json(f"{prefix}/chain.json")
    ok, msg = verify_chain_with_payloads(chain, store, run_prefix=prefix)
    assert ok, msg


def test_dagster_wrapper_records_run(tmp_path):
    store = Store.local(str(tmp_path))
    rec = Recorder(
        store=store,
        project="acme",
        dataset="dagster",
        diff=DiffConfig(mode="rowhash", primary_key=["id"]),
        snapshot=SnapshotConfig(mode="none"),
        seal=SealConfig(mode="chain"),
    )

    def _op():
        return pd.DataFrame({"id": [1, 2], "x": [1, 2]})

    wrapped = blackbox_op(rec, "op1", _op)
    out = wrapped()
    assert isinstance(out, pd.DataFrame)

    run_id = _latest_run_id(store, "acme", "dagster")
    prefix = f"acme/dagster/{run_id}"
    chain = store.get_json(f"{prefix}/chain.json")
    ok, msg = verify_chain_with_payloads(chain, store, run_prefix=prefix)
    assert ok, msg


def test_dbt_artifact_collection(tmp_path):
    root = str(tmp_path)
    target = os.path.join(root, "target")
    os.makedirs(target, exist_ok=True)
    run_results = {"metadata": {"generated_at": "now"}}
    manifest = {"metadata": {"project_name": "demo"}}
    with open(os.path.join(target, "run_results.json"), "w", encoding="utf-8") as f:
        json.dump(run_results, f)
    with open(os.path.join(target, "manifest.json"), "w", encoding="utf-8") as f:
        json.dump(manifest, f)

    artifacts = collect_dbt_artifacts(root)
    assert "run_results.json" in artifacts
    assert "manifest.json" in artifacts
