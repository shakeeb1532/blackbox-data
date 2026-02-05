from __future__ import annotations

import argparse
import os
import secrets


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="blackbox-pro", description="Blackbox Data Pro (local server MVP)")
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("serve", help="Run the local Pro API server")
    s.add_argument("--host", default="127.0.0.1")
    s.add_argument("--port", type=int, default=8088)
    s.add_argument("--root", default="./.blackbox_store", help="Storage root for Pro server")
    s.add_argument("--token-file", default=None, help="Token file with role:token lines")
    s.add_argument("--tokens", default=None, help="Comma-separated role:token pairs")
    s.add_argument(
        "--token",
        default=os.environ.get("BLACKBOX_PRO_TOKEN"),
        help="Bearer token expected by server (also read from env BLACKBOX_PRO_TOKEN).",
    )
    s.add_argument(
        "--allow-dev-token",
        action="store_true",
        help="Enable the legacy dev-secret-token fallback (not recommended).",
    )
    s.add_argument("--reload", action="store_true", help="Enable reload (dev)")

    k = sub.add_parser("apikey", help="Generate an API key for the Pro server")
    k.add_argument("--role", default="viewer", choices=["viewer", "admin"])
    k.add_argument("--tenants", default="*", help="Pipe-separated tenant list (e.g. acme|demo)")
    k.add_argument("--token-file", default=None, help="Append key to token file")
    k.add_argument("--show-line", action="store_true", help="Print the token file line format")

    e = sub.add_parser("export", help="Export a full run bundle")
    e.add_argument("--root", default="./.blackbox_store", help="Storage root for Pro server")
    e.add_argument("--project", default=None)
    e.add_argument("--dataset", default=None)
    e.add_argument("--run", dest="run_id", required=True, help="Run ID to export")
    e.add_argument("--format", default="zip", choices=["zip"])
    e.add_argument("--out", default=None, help="Output file path")
    return p


def cmd_serve(args: argparse.Namespace) -> int:
    # Set env before importing app
    os.environ["BLACKBOX_PRO_ROOT"] = args.root
    if args.token:
        os.environ["BLACKBOX_PRO_TOKEN"] = args.token
    if args.tokens:
        os.environ["BLACKBOX_PRO_TOKENS"] = args.tokens
    if args.token_file:
        os.environ["BLACKBOX_PRO_TOKEN_FILE"] = args.token_file
    if args.allow_dev_token:
        os.environ["BLACKBOX_PRO_ALLOW_DEV_TOKEN"] = "1"

    try:
        import uvicorn
    except Exception:
        print("ERROR: uvicorn not installed. Install Pro extras:")
        print('  pip install -e ".[pro]"')
        return 2

    # NOTE: must pass import string for reload to work
    uvicorn.run(
        "blackbox_pro.server.main:app",
        host=args.host,
        port=args.port,
        reload=bool(args.reload),
        log_level="info",
    )
    return 0


def _write_token_file_line(path: str, line: str) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(line.rstrip() + "\n")
    try:
        os.chmod(path, 0o600)
    except Exception:
        pass


def cmd_apikey(args: argparse.Namespace) -> int:
    token = secrets.token_urlsafe(32)
    tenant_part = args.tenants.strip() if args.tenants else "*"
    line = f"{args.role}@{tenant_part}:{token}"
    print("API key:", token)
    if args.show_line:
        print("Token file line:", line)
    if args.token_file:
        _write_token_file_line(args.token_file, line)
        print(f"Appended to token file: {args.token_file}")
    return 0


def cmd_export(args: argparse.Namespace) -> int:
    from blackbox.store import Store
    from blackbox_pro.exporter import export_run_bundle

    store = Store.local(args.root)
    out_path = args.out or f"run_{args.run_id}.zip"
    export_run_bundle(
        store=store,
        project=args.project,
        dataset=args.dataset,
        run_id=args.run_id,
        out_path=out_path,
    )
    print(f"Exported run bundle to {out_path}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.cmd == "serve":
        return cmd_serve(args)
    if args.cmd == "apikey":
        return cmd_apikey(args)
    if args.cmd == "export":
        return cmd_export(args)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
