# Integrations (Airflow, Dagster, dbt)

This page shows lightweight integration helpers included in v1.0.

## Airflow
```python
from blackbox import Recorder, Store, DiffConfig, SnapshotConfig, SealConfig
from blackbox.integrations.airflow import start_airflow_run

rec = Recorder(
    store=Store.local("./.blackbox_store"),
    project="acme-data",
    dataset="users_daily",
    diff=DiffConfig(mode="rowhash", primary_key=["id"]),
    snapshot=SnapshotConfig(mode="auto", max_mb=50),
    seal=SealConfig(mode="chain"),
)

def task_fn(**context):
    run = start_airflow_run(rec, context, project="acme-data", dataset="users_daily")
    # use run.step(...) inside your task
    run.finish()
```

## Dagster
```python
from blackbox.integrations.dagster import start_dagster_run

def run_with_dagster(rec, context):
    run = start_dagster_run(rec, context, project="acme-data", dataset="users_daily")
    # use run.step(...) inside ops
    run.finish()
```

## dbt
```python
from blackbox.integrations.dbt import load_dbt_run_results

summary = load_dbt_run_results("target/run_results.json")
# store summary in run metadata or tags
```
