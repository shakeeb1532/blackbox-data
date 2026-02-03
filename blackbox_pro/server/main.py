from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from blackbox_pro.server.api import router as api_router
from blackbox_pro.server.ui import router as ui_router
from blackbox_pro.server.auth import expected_token, require_token

app = FastAPI(title="Blackbox Data Pro", version="0.1.0")

# Anything a browser loads directly must be public (no Authorization header)
_PUBLIC_PATH_PREFIXES = (
    "/",                 # your ui.py defines "/" redirect -> /ui/home
    "/health",
    "/openapi.json",
    "/docs",
    "/redoc",
    "/docs/oauth2-redirect",
    "/ui",               # UI pages
    "/static",           # if you add assets later
    "/favicon.ico",
)


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    """
    Keep docs/openapi/ui public so browsers can load them.
    Protect all API endpoints via Bearer token.
    """
    path = request.url.path

    for prefix in _PUBLIC_PATH_PREFIXES:
        if path == prefix or path.startswith(prefix + "/"):
            return await call_next(request)

    try:
        await require_token(request)
    except Exception as e:
        status_code = getattr(e, "status_code", 500)
        detail = getattr(e, "detail", "Unauthorized")
        headers = getattr(e, "headers", None) or {}
        return JSONResponse({"detail": detail}, status_code=status_code, headers=headers)

    return await call_next(request)


@app.get("/health")
def health():
    return {"ok": True, "service": "blackbox-pro", "version": "0.1.0"}


# Mount routers
app.include_router(ui_router)   # <-- THIS is what you’re missing
app.include_router(api_router)  # /runs /report /verify /report_verbose


@app.on_event("startup")
async def _startup_debug() -> None:
    print("[blackbox-pro] main.py =", __file__)
    print("[blackbox-pro] expected token =", expected_token())
    print("[blackbox-pro] routes:")
    for r in app.routes:
        path = getattr(r, "path", None)
        methods = getattr(r, "methods", None)
        print("  ", path, methods)

