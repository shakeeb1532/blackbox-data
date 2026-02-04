from __future__ import annotations

import os
import subprocess
import sys


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: scripts/run_schemathesis.py <openapi_url>")
        return 2

    openapi_url = sys.argv[1]
    token = os.environ.get("BLACKBOX_PRO_TOKEN", "dev-secret-token")

    env = os.environ.copy()
    env["SCHEMATHESIS_HOOKS"] = "scripts.schemathesis_hooks"

    cmd = [
        sys.executable,
        "-m",
        "schemathesis",
        "run",
        openapi_url,
        "--checks",
        "all",
        "--workers",
        "1",
        "--header",
        f"Authorization: Bearer {token}",
        "--report",
        "junit,vcr,har,ndjson",
        "--report-dir",
        "reports/schemathesis",
    ]
    return subprocess.call(cmd, env=env)


if __name__ == "__main__":
    raise SystemExit(main())
