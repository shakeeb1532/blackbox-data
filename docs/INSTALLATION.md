# Installation Guide

## Supported Versions
- Python: 3.10, 3.11, 3.12
- OS: macOS, Linux (tested)

This guide covers local and Docker installation for the v1.0 developer‑tooling release.

## Local (Recommended for development)
```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -U pip
python3 -m pip install -e ".[pro]"
```

Generate demo data:
```bash
python examples/demo_pro.py
```

Generate streaming demo data:
```bash
python examples/demo_streaming.py
```

Generate Spark demo data (requires PySpark):
```bash
python examples/demo_spark.py
```

Start the UI/API server:
```bash
export BLACKBOX_PRO_TOKEN=dev-secret-token
blackbox-pro serve --root ./.blackbox_store --host 127.0.0.1 --port 8088
```

Role-based tokens (optional):
```bash
export BLACKBOX_PRO_TOKENS="admin:dev-secret-token,viewer:viewer-token"
```

Multi-tenant tokens (optional):
```bash
export BLACKBOX_PRO_TOKENS="admin@acme-data:dev-secret-token,viewer@acme-data|beta:viewer-token"
```

Open the UI:
```
http://127.0.0.1:8088/ui/home?token=dev-secret-token
```

## Docker (Self‑contained)
Build:
```bash
docker build -t blackbox-data .
```

Run:
```bash
docker run -p 8088:8088 \
  -e BLACKBOX_PRO_TOKEN=dev-secret-token \
  -v $(pwd)/.blackbox_store:/data/.blackbox_store \
  blackbox-data
```

Open the UI:
```
http://127.0.0.1:8088/ui/home?token=dev-secret-token
```

## Docker Compose
```bash
docker compose up -d
```

## Environment Variables
- `BLACKBOX_PRO_TOKEN`: bearer token for API + UI
- `BLACKBOX_PRO_TOKENS`: multi-tenant tokens (`role@tenant1|tenant2:token`)
- `BLACKBOX_PRO_ROOT`: store root path
- `BLACKBOX_PRO_TOKEN_FILE`: read token(s) from file

## Token File (Optional)
```bash
export BLACKBOX_PRO_TOKEN_FILE=/path/to/token
```
