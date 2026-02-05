# Integration Guide (Non‑Technical)

This guide lets non‑technical users run Blackbox without editing code.

## 1) Start the server
```bash
blackbox-pro start
```

## 2) Run your pipeline with one command
```bash
blackbox --root ./.blackbox_store wrap --project acme-data --dataset demo -- python pipeline.py
```

## 3) Open the UI
```
http://127.0.0.1:8088/ui/home
```

### Screenshots
![UI Home](screens/ui_home.png)
![Run Viewer](screens/ui_run.png)

---

## Airflow (quick wrapper)
```python
from blackbox import Recorder, Store
from blackbox.integrations.airflow import blackbox_task

rec = Recorder(store=Store.local("./.blackbox_store"), project="acme", dataset="prod")

@blackbox_task(rec, "daily_sync", lambda: run_my_task())
def run_my_task():
    ...
```

## Dagster (quick wrapper)
```python
from blackbox import Recorder, Store
from blackbox.integrations.dagster import blackbox_op

rec = Recorder(store=Store.local("./.blackbox_store"), project="acme", dataset="prod")

@blackbox_op(rec, "daily_asset", lambda: build_asset())
def build_asset():
    ...
```

## dbt (no‑code)
```bash
blackbox --root ./.blackbox_store wrap --project acme --dataset prod -- dbt run
```
Blackbox will capture `target/run_results.json` and `target/manifest.json`.
