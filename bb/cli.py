from __future__ import annotations
import typer
from rich import print
from rich.table import Table

from blackbox.store import Store
from blackbox.seal import verify_chain_with_payloads
from blackbox.util import safe_path_component

app = typer.Typer(add_completion=False)

def _store_local(root: str) -> Store:
    return Store.local(root)

@app.command()
def list_runs(root: str, project: str, dataset: str):
    store = _store_local(root)
    prefix = f"{safe_path_component(project)}/{safe_path_component(dataset)}/"
    keys = store.list(prefix)
    runs = sorted({k.split("/")[2] for k in keys if k.startswith(prefix) and len(k.split("/")) >= 3})
    for r in runs:
        print(r)

@app.command()
def inspect(root: str, project: str, dataset: str, run_id: str):
    store = _store_local(root)
    run_key = f"{safe_path_component(project)}/{safe_path_component(dataset)}/{run_id}/run.json"
    run = store.get_json(run_key)
    table = Table(title=f"Run {run_id}")
    table.add_column("Field")
    table.add_column("Value", overflow="fold")
    for k in ["status", "created_at", "finished_at"]:
        table.add_row(k, str(run.get(k)))
    print(table)

    steps = run.get("steps", [])
    if steps:
        st = Table(title="Steps")
        st.add_column("Ordinal")
        st.add_column("Name")
        st.add_column("Path")
        for s in steps:
            st.add_row(str(s["ordinal"]), s["name"], s["path"])
        print(st)

@app.command()
def verify(root: str, project: str, dataset: str, run_id: str):
    store = _store_local(root)
    prefix = f"{safe_path_component(project)}/{safe_path_component(dataset)}/{run_id}"
    run = store.get_json(f"{prefix}/run.json")
    seal_mode = (run.get("seal") or {}).get("mode", "none")
    if seal_mode == "none":
        print("ok=True msg=seal disabled")
        return
    chain = store.get_json(f"{prefix}/chain.json")
    ok, msg = verify_chain_with_payloads(chain, store, run_prefix=prefix)
    print(f"ok={ok} msg={msg}")

if __name__ == "__main__":
    app()
