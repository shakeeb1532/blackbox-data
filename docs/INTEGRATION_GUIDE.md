# Integration Guide (Non‑Technical)

This guide lets non‑technical users run Blackbox without editing code.

## 1) Start the server
```bash
blackbox-pro start
```

Windows:
```powershell
blackbox-pro start
```

## 2) Run your pipeline with one command
```bash
blackbox --root ./.blackbox_store wrap --project acme-data --dataset demo -- python pipeline.py
```

Windows:
```powershell
blackbox --root .\.blackbox_store wrap --project acme-data --dataset demo -- python pipeline.py
```

## 3) Open the UI
```
http://127.0.0.1:8088/ui/home
```

## Demo Scenarios
- Detect silent data poisoning before model training
- Audit trail for financial reporting dataset

### Screenshots
![UI Home](screens/ui_home.png)
![Run Viewer](screens/ui_run.png)

---

## Airflow (quick wrapper)
```python
from blackbox import Recorder, Store
from blackbox.integrations.airflow import blackbox_task, blackbox_task_in_run

rec = Recorder(store=Store.local("./.blackbox_store"), project="acme", dataset="prod")

@blackbox_task(rec, "daily_sync", lambda: run_my_task())
def run_my_task():
    ...
```

Single run for multiple tasks:
```python
run = rec.start_run(tags={"source": "airflow"})
task_a = blackbox_task_in_run(run, "task_a", lambda: do_a())
task_b = blackbox_task_in_run(run, "task_b", lambda: do_b())
task_a(); task_b()
run.finish()
```

## Dagster (quick wrapper)
```python
from blackbox import Recorder, Store
from blackbox.integrations.dagster import blackbox_op, blackbox_op_in_run

rec = Recorder(store=Store.local("./.blackbox_store"), project="acme", dataset="prod")

@blackbox_op(rec, "daily_asset", lambda: build_asset())
def build_asset():
    ...
```

Single run for multiple ops:
```python
run = rec.start_run(tags={"source": "dagster"})
op_a = blackbox_op_in_run(run, "op_a", lambda: do_a())
op_b = blackbox_op_in_run(run, "op_b", lambda: do_b())
op_a(); op_b()
run.finish()
```

## dbt (no‑code)
```bash
blackbox --root ./.blackbox_store wrap --project acme --dataset prod -- dbt run
```
Blackbox will capture `target/run_results.json` and `target/manifest.json`.
