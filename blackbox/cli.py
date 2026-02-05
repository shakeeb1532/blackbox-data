from __future__ import annotations

import argparse
import json
import datetime
import shutil
from typing import Any
import subprocess
import os

from rich.progress import (
    Progress,
    SpinnerColumn,
    BarColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from .store import Store
from .config import RecorderConfig, DiffConfig, SnapshotConfig, SealConfig
from .recorder import Recorder
from .integrations.dbt import collect_dbt_artifacts
from .seal import verify_chain_with_payloads
from .util import safe_path_component


# -----------------------------
# Helpers
# -----------------------------

def _run_prefix(project: str, dataset: str, run_id: str) -> str:
    safe_project = safe_path_component(project)
    safe_dataset = safe_path_component(dataset)
    return f"{safe_project}/{safe_dataset}/{run_id}"


def _load_json(store: Store, key: str) -> dict[str, Any]:
    return store.get_json(key)


def _join(prefix: str, rel: str) -> str:
    """Join a run prefix with a relative path safely."""
    rel = rel.lstrip("/")
    return f"{prefix}/{rel}"


def _truncate_list(values: list[Any], show: str, max_items: int) -> dict[str, Any]:
    """
    show:
      - none: count only
      - head: first N
      - headtail: first N + last N (if longer than N)
      - all: full list (dangerous)
    """
    n = len(values)
    out: dict[str, Any] = {"count": n}

    if show == "none" or max_items <= 0:
        return out

    if show == "all":
        out["items"] = values
        return out

    if show == "head":
        out["items"] = values[:max_items]
        out["truncated"] = n > max_items
        return out

    # headtail
    if n <= max_items:
        out["items"] = values
        out["truncated"] = False
        return out

    out["head"] = values[:max_items]
    out["tail"] = values[-max_items:]
    out["truncated"] = True
    return out


def _truncate_payload_lists(payload: dict[str, Any], *, show: str, max_items: int) -> dict[str, Any]:
    """
    Truncate the big lists that explode report output.
    Works for payloads that include:
      - added_keys / removed_keys / changed_keys
      - added_rowhashes / removed_rowhashes
    """
    out = dict(payload)
    for k in ("added_keys", "removed_keys", "changed_keys", "added_rowhashes", "removed_rowhashes"):
        if isinstance(out.get(k), list):
            out[k] = _truncate_list(out[k], show=show, max_items=max_items)
    return out

def _print_section(title: str) -> None:
    print()
    print(f"=== {title} ===")

def _print_kv(label: str, value: Any) -> None:
    print(f"{label:16s} {value}")

def _load_step_object(store: Store, prefix: str, st: dict[str, Any]) -> dict[str, Any] | None:
    """
    Supports both:
    - run.json['steps'] entries that are already full step payloads (contain 'code'/'input' etc)
    - run.json['steps'] entries that are references (contain 'path')
    """
    # Already a full step payload?
    if any(k in st for k in ("code", "input", "output", "diff", "schema_diff")) and "path" not in st:
        return st

    path = st.get("path")
    if not path:
        return None

    step_key = _join(prefix, path)
    try:
        return store.get_json(step_key)
    except FileNotFoundError:
        return None


def _infer_step_dir_from_path(path: str) -> str:
    """
    path looks like:
      steps/0001_name/step.json
    so step_dir becomes:
      steps/0001_name
    """
    parts = path.replace("\\", "/").split("/")
    if len(parts) >= 2:
        return "/".join(parts[:-1])
    return ""


def _load_diff_payload_for_step(
    store: Store, prefix: str, step_obj: dict[str, Any], step_path: str | None
) -> dict[str, Any] | None:
    """
    Supports:
    - step_obj['diff_payload'] embedded (older debug mode)
    - step_obj['diff']['artifact'] relative to the step folder (current format)
    """
    if isinstance(step_obj.get("diff_payload"), dict):
        return step_obj["diff_payload"]

    diff = step_obj.get("diff")
    if not isinstance(diff, dict):
        return None

    artifact = diff.get("artifact")
    if not isinstance(artifact, str) or not artifact:
        return None

    candidate_keys: list[str] = []

    if step_path:
        step_dir = _infer_step_dir_from_path(step_path)
        candidate_keys.append(_join(prefix, f"{step_dir}/{artifact}"))

    # fallback guess (covers some earlier layouts)
    candidate_keys.append(_join(prefix, artifact))

    for k in candidate_keys:
        try:
            return store.get_json(k)
        except FileNotFoundError:
            continue

    return None


def _compact_step_summary(step_obj: dict[str, Any]) -> dict[str, Any]:
    """
    Produce a concise, readable summary per step (default output).
    """
    out: dict[str, Any] = {
        "ordinal": step_obj.get("ordinal"),
        "name": step_obj.get("name"),
        "status": step_obj.get("status"),
        "started_at": step_obj.get("started_at"),
        "finished_at": step_obj.get("finished_at"),
    }

    inp = step_obj.get("input")
    outp = step_obj.get("output")
    if isinstance(inp, dict):
        out["input"] = {
            "rows": inp.get("n_rows", inp.get("rows")),
            "cols": inp.get("n_cols", inp.get("cols")),
            "artifact": inp.get("artifact"),
            "sample_artifact": inp.get("sample_artifact"),
            "skip": inp.get("snapshot_skipped", inp.get("skip")),
        }
    if isinstance(outp, dict):
        out["output"] = {
            "rows": outp.get("n_rows", outp.get("rows")),
            "cols": outp.get("n_cols", outp.get("cols")),
            "artifact": outp.get("artifact"),
            "sample_artifact": outp.get("sample_artifact"),
            "skip": outp.get("snapshot_skipped", outp.get("skip")),
        }

    if isinstance(step_obj.get("schema_diff"), dict):
        out["schema_diff"] = step_obj["schema_diff"]

    if isinstance(step_obj.get("diff"), dict):
        d = step_obj["diff"]
        summary = d.get("summary") if isinstance(d.get("summary"), dict) else None
        out["diff"] = {
            "mode": d.get("mode"),
            "summary": summary,
            "artifact": d.get("artifact"),
            "summary_only": d.get("summary_only"),
            "ui_hint": d.get("ui_hint"),
        }

    if isinstance(step_obj.get("evidence"), dict):
        out["evidence"] = step_obj["evidence"]

    return out


# -----------------------------
# Commands
# -----------------------------

def cmd_list(args: argparse.Namespace) -> int:
    store = Store.local(args.root)
    base = f"{safe_path_component(args.project)}/{safe_path_component(args.dataset)}".rstrip("/")

    # Prefer store.list_dirs() (correct and cheap). Fallback to inference via list().
    run_ids: list[str] = []
    try:
        run_ids = store.list_dirs(base)
    except Exception:
        run_ids = []

    if not run_ids:
        # fallback to inference from keys
        try:
            keys = store.list(base)
        except Exception:
            keys = store.list(f"{base}/")

        run_set: set[str] = set()
        for k in keys:
            k = str(k).lstrip("/")
            if not k.startswith(base + "/"):
                continue
            rest = k[len(base) + 1 :]
            if not rest:
                continue
            rid = rest.split("/", 1)[0]
            if rid:
                run_set.add(rid)
        run_ids = sorted(run_set)

    if run_ids:
        for rid in run_ids:
            print(rid)
    else:
        _print_section("No Runs Found")
        _print_kv("root", args.root)
        _print_kv("prefix", f"{base}/<run_id>/run.json")
    return 0


def cmd_verify(args: argparse.Namespace) -> int:
    store = Store.local(args.root)
    prefix = _run_prefix(args.project, args.dataset, args.run_id)

    try:
        run_obj = _load_json(store, f"{prefix}/run.json")
    except FileNotFoundError:
        payload = {
            "ok": False,
            "error": "run_not_found",
            "root": args.root,
            "prefix": prefix,
            "hint": f"blackbox --root {args.root} list --project {args.project} --dataset {args.dataset}",
        }
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True))
        else:
            print("FAIL: run not found.")
            print("root:", args.root)
            print("prefix:", prefix)
            print("Try:")
            print(f"  {payload['hint']}")
        return 2

    seal_mode = (run_obj.get("seal") or {}).get("mode", "none")
    if seal_mode == "none":
        payload = {
            "ok": True,
            "message": "seal disabled",
            "root": args.root,
            "prefix": prefix,
            "chain_entries": 0,
            "chain_head": None,
        }
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True))
        else:
            print("OK: seal disabled.")
        return 0

    try:
        chain_obj = _load_json(store, f"{prefix}/chain.json")
    except FileNotFoundError:
        payload = {
            "ok": False,
            "error": "chain_not_found",
            "root": args.root,
            "prefix": prefix,
        }
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True))
        else:
            print("FAIL: chain.json not found.")
            print("root:", args.root)
            print("prefix:", prefix)
        return 2

    ok, msg = verify_chain_with_payloads(chain_obj, store, run_prefix=prefix)

    payload = {
        "ok": bool(ok),
        "message": msg,
        "root": args.root,
        "prefix": prefix,
        "chain_entries": len(chain_obj.get("entries", [])),
        "chain_head": chain_obj.get("head"),
    }

    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        _print_section("Verify")
        _print_kv("result", "OK" if ok else "FAIL")
        _print_kv("message", msg)

    return 0 if ok else 1


def cmd_report(args: argparse.Namespace) -> int:
    store = Store.local(args.root)
    prefix = _run_prefix(args.project, args.dataset, args.run_id)

    try:
        run_obj = _load_json(store, f"{prefix}/run.json")
    except FileNotFoundError:
        payload = {
            "ok": False,
            "error": "run_not_found",
            "root": args.root,
            "prefix": prefix,
            "hint": f"blackbox --root {args.root} list --project {args.project} --dataset {args.dataset}",
        }
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True))
        else:
            print("Run not found.")
            print("root:", args.root)
            print("prefix:", prefix)
            print("List available runs with:")
            print(f"  {payload['hint']}")
        return 2

    seal_mode = (run_obj.get("seal") or {}).get("mode", "none")
    chain_obj: dict[str, Any] | None = None
    if seal_mode != "none":
        try:
            chain_obj = _load_json(store, f"{prefix}/chain.json")
        except FileNotFoundError:
            payload = {
                "ok": False,
                "error": "chain_not_found",
                "root": args.root,
                "prefix": prefix,
            }
            if args.json:
                print(json.dumps(payload, indent=2, sort_keys=True))
            else:
                print("Run found, but chain.json missing.")
                print("root:", args.root)
                print("prefix:", prefix)
            return 2

    if chain_obj is None:
        ok, msg = True, "seal disabled"
    else:
        ok, msg = verify_chain_with_payloads(chain_obj, store, run_prefix=prefix)

    steps_index = run_obj.get("steps", [])
    total_steps = len(steps_index)

    step_summaries: list[dict[str, Any]] = []
    verbose_steps: list[dict[str, Any]] = []

    # Progress bar is only meaningful for human output; JSON output should be clean.
    use_progress = (not args.json) and total_steps > 0

    def _process_one_step(i: int, st: Any) -> None:
        step_path = st.get("path") if isinstance(st, dict) else None
        step_obj = _load_step_object(store, prefix, st) if isinstance(st, dict) else None

        if step_obj is None:
            step_summaries.append(
                {
                    "ordinal": i,
                    "name": st.get("name") if isinstance(st, dict) else None,
                    "status": "missing_step_json",
                }
            )
            return

        summary = _compact_step_summary(step_obj)
        if args.diff_mode == "schema":
            summary.pop("diff", None)
        step_summaries.append(summary)

        if args.verbose:
            diff_payload = None
            if args.diff_mode != "schema":
                diff_payload = _load_diff_payload_for_step(store, prefix, step_obj, step_path)
            if isinstance(diff_payload, dict):
                if args.diff_mode == "keys-only":
                    diff_payload["added_keys"] = []
                    diff_payload["removed_keys"] = []
                    diff_payload["changed_keys"] = []
                if args.summary_threshold is not None:
                    total_keys_hint = None
                    inp = step_obj.get("input") if isinstance(step_obj.get("input"), dict) else None
                    outp = step_obj.get("output") if isinstance(step_obj.get("output"), dict) else None
                    if inp and outp:
                        try:
                            total_keys_hint = max(int(inp.get("n_rows") or 0), int(outp.get("n_rows") or 0))
                        except Exception:
                            total_keys_hint = None
                    added = int((diff_payload.get("summary") or {}).get("added") or 0)
                    removed = int((diff_payload.get("summary") or {}).get("removed") or 0)
                    total = max(int(total_keys_hint or 0), 1)
                    ratio = (added + removed) / total
                    if ratio >= float(args.summary_threshold):
                        diff_payload["summary_only"] = True
                        diff_payload["ui_hint"] = "summary_only_high_churn"
                        diff_payload["added_keys"] = []
                        diff_payload["removed_keys"] = []
                        diff_payload["changed_keys"] = []
                diff_payload = _truncate_payload_lists(diff_payload, show=args.show_keys, max_items=args.max_keys)

            verbose_steps.append(
                {
                    "step": summary,
                    "code": step_obj.get("code"),
                    "raw_input": step_obj.get("input"),
                    "raw_output": step_obj.get("output"),
                    "raw_schema_diff": step_obj.get("schema_diff"),
                    "raw_diff": step_obj.get("diff"),
                    "raw_evidence": step_obj.get("evidence"),
                    "diff_payload": diff_payload,
                }
            )

    if use_progress:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("{task.completed}/{task.total}"),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            transient=True,
        ) as prog:
            task = prog.add_task("Rendering report", total=total_steps)
            for i, st in enumerate(steps_index, start=1):
                _process_one_step(i, st)
                prog.advance(task, 1)
    else:
        for i, st in enumerate(steps_index, start=1):
            _process_one_step(i, st)

    report_obj: dict[str, Any] = {
        "ok": bool(ok),
        "verify_message": msg,
        "root": args.root,
        "prefix": prefix,
        "project": args.project,
        "dataset": args.dataset,
        "run_id": args.run_id,
        "run": {
            "status": run_obj.get("status"),
            "created_at": run_obj.get("created_at"),
            "finished_at": run_obj.get("finished_at"),
            "tags": run_obj.get("tags"),
            "metadata": run_obj.get("metadata"),
        },
        "chain": {
            "entries": len(chain_obj.get("entries", [])) if chain_obj else 0,
            "head": chain_obj.get("head") if chain_obj else None,
        },
        "steps": step_summaries,
    }
    if args.verbose:
        report_obj["verbose_steps"] = verbose_steps

    if args.json:
        print(json.dumps(report_obj, indent=2, sort_keys=True))
        return 0 if ok else 1

    # Human-friendly output
    _print_section("Run")
    _print_kv("root", args.root)
    _print_kv("prefix", prefix)
    _print_kv("project", args.project)
    _print_kv("dataset", args.dataset)
    _print_kv("run_id", args.run_id)
    _print_kv("status", run_obj.get("status"))
    _print_kv("created_at", run_obj.get("created_at"))
    _print_kv("finished_at", run_obj.get("finished_at"))
    _print_kv("verify", f"{ok} ({msg})")
    _print_kv("chain_entries", len(chain_obj.get("entries", [])) if chain_obj else 0)
    _print_kv("chain_head", chain_obj.get("head") if chain_obj else None)

    for idx, s in enumerate(step_summaries, start=1):
        name = s.get("name", f"step_{idx}")
        status = s.get("status", "ok")
        _print_section(f"Step {idx} · {name} [{status}]")

        inp = s.get("input")
        outp = s.get("output")
        if isinstance(inp, dict):
            _print_kv(
                "input",
                f"rows={inp.get('rows')} cols={inp.get('cols')} artifact={inp.get('artifact') or inp.get('sample_artifact') or None}",
            )
            if inp.get("skip"):
                _print_kv("input_skip", inp["skip"])
        if isinstance(outp, dict):
            _print_kv(
                "output",
                f"rows={outp.get('rows')} cols={outp.get('cols')} artifact={outp.get('artifact') or outp.get('sample_artifact') or None}",
            )
            if outp.get("skip"):
                _print_kv("output_skip", outp["skip"])

        if isinstance(s.get("schema_diff"), dict):
            _print_kv("schema_diff", s["schema_diff"])

        diff = s.get("diff")
        if isinstance(diff, dict):
            _print_kv("diff", diff.get("summary") or diff)
            if diff.get("ui_hint"):
                _print_kv("diff_hint", diff.get("ui_hint"))

        if args.verbose:
            v = verbose_steps[idx - 1] if (idx - 1) < len(verbose_steps) else None
            if v:
                if v.get("code"):
                    _print_kv("code", v["code"])
                if v.get("raw_evidence"):
                    _print_kv("evidence", v["raw_evidence"])
                if v.get("diff_payload"):
                    # Helpful MVP note: make schema-only changes explicit
                    notes = v["diff_payload"].get("notes") if isinstance(v["diff_payload"], dict) else None
                    if isinstance(notes, dict) and notes.get("schema_changed"):
                        left = notes.get("cols_only_in_left") or []
                        right = notes.get("cols_only_in_right") or []
                        _print_kv("diff_notes", {"only_in_left": left, "only_in_right": right})
                    _print_kv("diff_payload", json.dumps(v["diff_payload"], indent=2, sort_keys=True))

    return 0 if ok else 1


def cmd_cleanup(args: argparse.Namespace) -> int:
    store = Store.local(args.root)
    retention_days = float(args.retention_days)
    cutoff = datetime.datetime.utcnow() - datetime.timedelta(days=retention_days)
    removed = 0
    kept = 0
    projects = store.list_dirs("")
    for project in projects:
        datasets = store.list_dirs(project)
        for dataset in datasets:
            runs = store.list_dirs(f"{project}/{dataset}")
            for run_id in runs:
                run_key = f"{project}/{dataset}/{run_id}/run.json"
                try:
                    run_obj = store.get_json(run_key)
                except Exception:
                    continue
                created_at = run_obj.get("created_at") or run_obj.get("finished_at")
                if not created_at:
                    kept += 1
                    continue
                try:
                    ts = str(created_at).replace("Z", "+00:00")
                    dt = datetime.datetime.fromisoformat(ts)
                except Exception:
                    kept += 1
                    continue
                if dt < cutoff:
                    path = store._path(f"{project}/{dataset}/{run_id}")
                    if args.dry_run:
                        print("DRY RUN remove", path)
                    else:
                        shutil.rmtree(path, ignore_errors=True)
                    removed += 1
                else:
                    kept += 1
    print(f"cleanup complete: removed={removed} kept={kept}")
    return 0


def cmd_wrap(args: argparse.Namespace) -> int:
    cmd = list(args.cmd or [])
    if cmd and cmd[0] == "--":
        cmd = cmd[1:]
    if not cmd:
        print("ERROR: wrap requires a command. Example: blackbox --root ./.blackbox_store wrap --project p --dataset d -- python pipeline.py")
        return 2

    store = Store.local(args.root)
    rec = Recorder(
        store=store,
        project=args.project,
        dataset=args.dataset,
        diff=DiffConfig(mode="none"),
        snapshot=SnapshotConfig(mode="none"),
        seal=SealConfig(mode="chain"),
        config=RecorderConfig(enforce_explicit_output=False),
    )
    run = rec.start_run(run_id=args.run_id, tags={"source": "wrap"})

    with run.step(args.name) as st:
        proc = subprocess.run(cmd, capture_output=True, text=True)
        stdout = proc.stdout or ""
        stderr = proc.stderr or ""
        exit_code = int(proc.returncode)

        step_key = run._step_prefix(1, args.name)
        artifacts_prefix = f"{step_key}/artifacts"
        if stdout:
            store.put_bytes(f"{artifacts_prefix}/stdout.txt", stdout.encode("utf-8"))
        if stderr:
            store.put_bytes(f"{artifacts_prefix}/stderr.txt", stderr.encode("utf-8"))

        dbt_artifacts = collect_dbt_artifacts(os.getcwd())
        for name, payload in dbt_artifacts.items():
            store.put_bytes(f"{artifacts_prefix}/{name}", payload)

        st.add_metadata(
            command=" ".join(cmd),
            exit_code=exit_code,
            stdout_artifact=f"{artifacts_prefix}/stdout.txt" if stdout else None,
            stderr_artifact=f"{artifacts_prefix}/stderr.txt" if stderr else None,
            dbt_artifacts=list(dbt_artifacts.keys()) if dbt_artifacts else [],
        )

    run.finish()
    ok, msg = run.verify()
    print(f"run_id: {run.run_id}")
    print(f"verify: {ok} {msg}")
    return 0 if proc.returncode == 0 else proc.returncode


# -----------------------------
# Parser
# -----------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="blackbox", description="Blackbox Data Forensic Recorder (MVP)")
    p.add_argument("--root", required=True, help="Root directory of the store")

    sub = p.add_subparsers(dest="cmd", required=True)

    p_list = sub.add_parser("list", help="List run IDs for a project/dataset")
    p_list.add_argument("--project", required=True)
    p_list.add_argument("--dataset", required=True)
    p_list.set_defaults(func=cmd_list)

    p_verify = sub.add_parser("verify", help="Verify the tamper-evident chain for a run")
    p_verify.add_argument("--project", required=True)
    p_verify.add_argument("--dataset", required=True)
    p_verify.add_argument("--run-id", required=True)
    p_verify.add_argument("--json", action="store_true", help="Machine-readable JSON output")
    p_verify.set_defaults(func=cmd_verify)

    p_report = sub.add_parser("report", help="Human-friendly summary of a run")
    p_report.add_argument("--project", required=True)
    p_report.add_argument("--dataset", required=True)
    p_report.add_argument("--run-id", required=True)
    p_report.add_argument("-v", "--verbose", action="store_true", help="Include step details and diff payload (truncated)")
    p_report.add_argument("--json", action="store_true", help="Machine-readable JSON output")
    p_report.add_argument(
        "--show-keys",
        choices=["none", "head", "headtail", "all"],
        default="headtail",
        help="How to display large lists in verbose diff payloads",
    )
    p_report.add_argument(
        "--max-keys",
        type=int,
        default=20,
        help="Max items to show per list when show-keys=head/headtail",
    )
    p_report.add_argument(
        "--summary-threshold",
        type=float,
        default=None,
        help="Override summary-only threshold for report rendering (ratio of added+removed / rows)",
    )
    p_report.add_argument(
        "--diff-mode",
        choices=["rows", "schema", "keys-only"],
        default=None,
        help="Override diff rendering: rows (default), schema only, or keys-only",
    )
    p_report.set_defaults(func=cmd_report)

    p_cleanup = sub.add_parser("cleanup", help="Delete runs older than retention window (local store)")
    p_cleanup.add_argument("--retention-days", required=True, type=float)
    p_cleanup.add_argument("--dry-run", action="store_true", help="Only print what would be removed")
    p_cleanup.set_defaults(func=cmd_cleanup)

    p_wrap = sub.add_parser("wrap", help="Run a command and capture logs (no-code)")
    p_wrap.add_argument("--project", required=True)
    p_wrap.add_argument("--dataset", required=True)
    p_wrap.add_argument("--run-id", default=None)
    p_wrap.add_argument("--name", default="command")
    p_wrap.add_argument("cmd", nargs=argparse.REMAINDER, help="Command to run (use -- before the command)")
    p_wrap.set_defaults(func=cmd_wrap)

    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except Exception as e:
        if getattr(args, "json", False):
            print(json.dumps({"ok": False, "error": "unexpected_exception", "message": str(e)}, indent=2, sort_keys=True))
        else:
            print("ERROR:", e)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
