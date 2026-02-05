# blackbox_pro (Pro server + UI)

FastAPI server and HTML UI for browsing runs, diffs, exports, and evidence.

Key areas:
- `server/main.py`: app wiring + middleware
- `server/api.py`: authenticated API routes
- `server/ui.py`: HTML UI rendering
- `server/auth.py`: token, RBAC, OIDC/JWT helpers
- `cli.py`: `blackbox-pro` CLI (serve, apikey, export)

Use this package to run the local Pro server or integrate with your infra.

Notes:
- UI supports login sessions (cookie-based) and `?token=` fallback.
- Roles: viewer (read-only) and admin (verify/export).
- Evidence ZIP includes `diff_summaries.json` and optional signatures.
