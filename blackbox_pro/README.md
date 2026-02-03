# Blackbox Data Pro (Local Server)

Blackbox Data Pro provides a local API server and a clean HTML UI for browsing runs, verifying integrity, and inspecting diffs.

## Run the Server
```bash
blackbox-pro serve --root ./.blackbox_store --host 127.0.0.1 --port 8088
```

Optional token:
```bash
BLACKBOX_PRO_TOKEN=dev-secret-token blackbox-pro serve --root ./.blackbox_store
```

## UI
Open in a browser:
- `http://127.0.0.1:8088/ui/home`

## API
- `GET /report`
- `GET /report_verbose`
- `GET /verify`

These endpoints are documented in the auto-generated API docs:
- `http://127.0.0.1:8088/docs`

## Notes
- This is a local-first MVP. Authentication is a simple bearer token.
- The UI reads from the same storage root as the CLI.
