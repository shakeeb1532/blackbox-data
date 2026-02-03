from __future__ import annotations

import argparse
import os


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="blackbox-pro", description="Blackbox Data Pro (local server MVP)")
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("serve", help="Run the local Pro API server")
    s.add_argument("--host", default="127.0.0.1")
    s.add_argument("--port", type=int, default=8088)
    s.add_argument("--root", default="./.blackbox_store", help="Storage root for Pro server")
    s.add_argument(
        "--token",
        default=os.environ.get("BLACKBOX_PRO_TOKEN", "dev-secret-token"),
        help="Bearer token expected by server (also read from env BLACKBOX_PRO_TOKEN).",
    )
    s.add_argument("--reload", action="store_true", help="Enable reload (dev)")
    return p


def cmd_serve(args: argparse.Namespace) -> int:
    # Set env before importing app
    os.environ["BLACKBOX_PRO_ROOT"] = args.root
    os.environ["BLACKBOX_PRO_TOKEN"] = args.token

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


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.cmd == "serve":
        return cmd_serve(args)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())

