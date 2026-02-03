from __future__ import annotations

import os
from typing import Tuple, Dict, Optional

from fastapi import HTTPException, Request


def expected_token() -> str:
    """
    Single shared expected token for this server process.
    Priority:
      1) BLACKBOX_PRO_TOKEN env var
      2) (fallback) dev-secret-token
    """
    return os.environ.get("BLACKBOX_PRO_TOKEN") or "dev-secret-token"


def _extract_bearer_token(request: Request) -> Optional[str]:
    auth = request.headers.get("authorization") or request.headers.get("Authorization")
    if not auth:
        return None
    if not auth.lower().startswith("bearer "):
        return None
    return auth.split(" ", 1)[1].strip() or None


def verify_request_token(
    request: Request,
    *,
    allow_query_token: bool,
) -> tuple[bool, str, dict]:
    """
    Returns: (ok, detail, headers)
    """
    token = _extract_bearer_token(request)

    if token is None and allow_query_token:
        token = request.query_params.get("token") or None
        if token is not None:
            token = token.strip() or None

    if not token:
        return False, "Missing Authorization: Bearer <token>", {"WWW-Authenticate": "Bearer"}

    if token != expected_token():
        return False, "Invalid token", {"WWW-Authenticate": "Bearer"}

    return True, "ok", {}


async def require_token(request: Request) -> None:
    ok, detail, headers = verify_request_token(request, allow_query_token=False)
    if not ok:
        raise HTTPException(status_code=401, detail=detail, headers=headers)

