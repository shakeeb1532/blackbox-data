# Troubleshooting

## Docker build fails: `uvicorn not installed`
Cause: pro extras were not installed in the image.  
Fix: ensure the Dockerfile installs `.[pro]` (already included in this repo).

## `pip install -e ".[pro]"` fails with Python version error
Cause: system Python is below 3.10.  
Fix: use a venv with Python 3.10+:
```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -e ".[pro]"
```

## UI shows 401 Unauthorized
Cause: token missing.  
Fix: add `?token=...` to UI URLs or set Authorization header for API calls.

Example:
```
http://127.0.0.1:8088/ui/home?token=dev-secret-token
```

## OpenAPI button doesn't load
Cause: token not applied to the link.  
Fix: click **Apply Token** in the UI, or open:
```
http://127.0.0.1:8088/docs?token=dev-secret-token
```

## No diffs shown in UI
Cause: only one step or no changes between steps.  
Fix: run the demo with mutations:
```bash
python examples/demo_pro.py
```

## Evidence bundle signature verify fails
Cause: missing HMAC key or GPG key.  
Fix:
- For HMAC: `blackbox evidence-verify --path evidence.zip --hmac-key <key>`
- For PGP: ensure `gpg` is installed and key is available, then use `--gpg`.

## GPG key not found
Cause: GPG key not imported.  
Fix:
```bash
blackbox gpg-list
blackbox gpg-import --path /path/to/key.asc
```

## Retention cleanup
Use the cleanup command to enforce retention windows:
```bash
blackbox --root ./.blackbox_store cleanup --retention-days 30
```
Use `--dry-run` to preview deletions.
