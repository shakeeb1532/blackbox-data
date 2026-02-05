from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse, RedirectResponse
import time
from contextlib import asynccontextmanager

from blackbox_pro.server.api import router as api_router
from blackbox_pro.server.ui import router as ui_router
from blackbox_pro.server.auth import expected_token, require_token
from blackbox_pro.server.metrics import snapshot_text, record_request
from blackbox_pro.server.audit import write_audit_event

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("[blackbox-pro] main.py =", __file__)
    print("[blackbox-pro] expected token =", expected_token())
    print("[blackbox-pro] routes:")
    for r in app.routes:
        path = getattr(r, "path", None)
        methods = getattr(r, "methods", None)
        print("  ", path, methods)
    yield


app = FastAPI(title="Blackbox Data Pro", version="0.1.0", lifespan=lifespan)

# Public paths (no auth)
_PUBLIC_PATH_PREFIXES = (
    "/health",
    "/openapi.json",
    "/docs",
    "/redoc",
    "/docs/oauth2-redirect",
    "/favicon.ico",
    "/ui/login",
    "/ui/logout",
)


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    path = request.url.path

    if any(path == p or path.startswith(p + "/") for p in _PUBLIC_PATH_PREFIXES):
        return await call_next(request)

    is_ui = path.startswith("/ui")
    try:
        await require_token(request, allow_query_token=is_ui)
    except Exception as e:
        status_code = getattr(e, "status_code", 500)
        detail = getattr(e, "detail", "Unauthorized")
        headers = getattr(e, "headers", None) or {}
        if status_code in (401, 403):
            try:
                write_audit_event(
                    {
                        "event": "auth_failure",
                        "path": path,
                        "method": request.method,
                        "status": status_code,
                        "detail": detail,
                        "ip": request.client.host if request.client else None,
                        "user_agent": request.headers.get("user-agent"),
                    }
                )
            except Exception:
                pass
        if is_ui and path not in ("/ui/login", "/ui/logout"):
            return RedirectResponse(url="/ui/login", status_code=302)
        return JSONResponse({"detail": detail}, status_code=status_code, headers=headers)

    start = time.perf_counter()
    response = await call_next(request)
    elapsed_ms = (time.perf_counter() - start) * 1000.0
    try:
        record_request(request.method, path, response.status_code, elapsed_ms)
        write_audit_event(
            {
                "event": "request",
                "path": path,
                "method": request.method,
                "status": response.status_code,
                "role": getattr(request.state, "auth_role", None),
                "token_id": getattr(request.state, "auth_token_id", None),
                "ip": request.client.host if request.client else None,
                "user_agent": request.headers.get("user-agent"),
                "duration_ms": round(elapsed_ms, 2),
            }
        )
    except Exception:
        pass
    return response


@app.get("/health")
def health():
    return {"ok": True, "service": "blackbox-pro", "version": "0.1.0"}


@app.get("/metrics")
def metrics():
    return PlainTextResponse(snapshot_text(), media_type="text/plain")


# Mount routers
app.include_router(ui_router)
app.include_router(api_router)


#
