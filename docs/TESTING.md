# Testing

## Zero-Warning Schemathesis Run

This produces a clean Schemathesis run with seeded data and custom deserializers
for JSONL and ZIP responses.

1) Start the API server (in one terminal):

```bash
export BLACKBOX_PRO_TOKEN=dev-secret-token
.venv/bin/python -m blackbox_pro.cli serve --host 127.0.0.1 --port 8089
```

2) Seed a real run for Schemathesis parameters:

```bash
export BLACKBOX_PRO_ROOT=./.blackbox_store
.venv/bin/python scripts/seed_schemathesis.py | tee /tmp/schemathesis.env
source /tmp/schemathesis.env
```

3) Run Schemathesis with hooks + deserializers:

```bash
export SCHEMATHESIS_HOOKS=scripts.schemathesis_hooks
.venv/bin/python scripts/run_schemathesis.py http://127.0.0.1:8089/openapi.json
```

Reports are written to `reports/schemathesis/`.
