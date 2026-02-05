from __future__ import annotations

import os
import hashlib
import logging
import secrets
from dataclasses import dataclass
from typing import Optional

from fastapi import HTTPException, Request

_logger = logging.getLogger("blackbox-pro")


def expected_token() -> str:
    """
    Single shared expected token for this server process.
    Priority:
      1) BLACKBOX_PRO_TOKEN env var
      2) (fallback) dev-secret-token
    """
    reg = token_registry()
    if reg:
        return next(iter(reg.keys()))
    return ""


def _hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()[:12]


@dataclass(frozen=True)
class TokenInfo:
    role: str
    tenants: list[str]


def _jwt_verify(token: str) -> Optional[TokenInfo]:
    public_key = os.environ.get("BLACKBOX_PRO_JWT_PUBLIC_KEY")
    if not public_key:
        return None
    try:
        import jwt  # type: ignore
    except Exception as e:
        _logger.debug("JWT import failed: %s", e)
        return None
    algs = os.environ.get("BLACKBOX_PRO_JWT_ALGORITHMS", "RS256,HS256")
    algorithms = [a.strip() for a in algs.split(",") if a.strip()]
    try:
        claims = jwt.decode(token, public_key, algorithms=algorithms, options={"verify_aud": False})
    except Exception as e:
        _logger.debug("JWT verification failed: %s", e)
        return None

    role = claims.get("role")
    if not role:
        roles = claims.get("roles")
        if isinstance(roles, list) and roles:
            role = str(roles[0])
    role = role or "viewer"

    tenants: list[str] = ["*"]
    if "tenant" in claims:
        tenants = [str(claims.get("tenant"))]
    elif "tenants" in claims and isinstance(claims.get("tenants"), list):
        tenants = [str(x) for x in claims.get("tenants") if str(x).strip()] or ["*"]

    return TokenInfo(role=role, tenants=tenants)


def _oidc_verify(token: str) -> Optional[TokenInfo]:
    issuer = os.environ.get("BLACKBOX_PRO_OIDC_ISSUER")
    jwks_url = os.environ.get("BLACKBOX_PRO_OIDC_JWKS_URL")
    audience = os.environ.get("BLACKBOX_PRO_OIDC_AUDIENCE")
    if not issuer and not jwks_url:
        return None
    try:
        import jwt  # type: ignore
        from jwt import PyJWKClient  # type: ignore
    except Exception as e:
        _logger.debug("OIDC jwt import failed: %s", e)
        return None

    try:
        if not jwks_url and issuer:
            jwks_url = issuer.rstrip("/") + "/.well-known/jwks.json"
        if not jwks_url:
            return None
        jwk_client = PyJWKClient(jwks_url)
        signing_key = jwk_client.get_signing_key_from_jwt(token)
        options = {"verify_aud": bool(audience)}
        claims = jwt.decode(
            token,
            signing_key.key,
            algorithms=["RS256", "ES256"],
            audience=audience,
            issuer=issuer if issuer else None,
            options=options,
        )
    except Exception as e:
        _logger.debug("OIDC verification failed: %s", e)
        return None

    role = claims.get("role") or claims.get("roles")
    if isinstance(role, list):
        role = role[0] if role else "viewer"
    role = str(role) if role else "viewer"

    tenants: list[str] = ["*"]
    if "tenant" in claims:
        tenants = [str(claims.get("tenant"))]
    elif "tenants" in claims and isinstance(claims.get("tenants"), list):
        tenants = [str(x) for x in claims.get("tenants") if str(x).strip()] or ["*"]

    return TokenInfo(role=role, tenants=tenants)


def _proxy_sso_info(request: Request) -> Optional[TokenInfo]:
    if os.environ.get("BLACKBOX_PRO_TRUST_PROXY") != "1":
        return None
    user_header = os.environ.get("BLACKBOX_PRO_SSO_HEADER", "x-auth-user")
    role_header = os.environ.get("BLACKBOX_PRO_SSO_ROLE_HEADER", "x-auth-role")
    tenant_header = os.environ.get("BLACKBOX_PRO_SSO_TENANT_HEADER", "x-auth-tenant")

    user = request.headers.get(user_header)
    if not user:
        return None
    role = request.headers.get(role_header) or "viewer"
    tenants_raw = request.headers.get(tenant_header) or "*"
    tenants = [t.strip() for t in tenants_raw.replace(",", "|").split("|") if t.strip()] or ["*"]
    return TokenInfo(role=role, tenants=tenants)


def _parse_token_lines(lines: list[str]) -> dict[str, TokenInfo]:
    """
    Parse token lines with optional roles:
      admin:token
      viewer:token
      admin@tenant1|tenant2:token
    If role is missing, defaults to admin.
    """
    out: dict[str, TokenInfo] = {}
    for raw in lines:
        s = raw.strip()
        if not s or s.startswith("#"):
            continue
        role = "admin"
        tenants = ["*"]
        token: str | None = None
        if ":" in s:
            left, token = s.split(":", 1)
            left = left.strip()
            token = token.strip()
            if "@" in left:
                role_part, tenant_part = left.split("@", 1)
                role = role_part.strip() or "admin"
                tenant_part = tenant_part.strip()
                tenants = [t.strip() for t in tenant_part.split("|") if t.strip()] or ["*"]
            else:
                role = left.strip() or "admin"
        else:
            token = s
        if token:
            out[token] = TokenInfo(role=role, tenants=tenants)
    return out


_FALLBACK_TOKEN: str | None = None


def _fallback_token() -> str:
    global _FALLBACK_TOKEN
    if _FALLBACK_TOKEN is None:
        _FALLBACK_TOKEN = secrets.token_urlsafe(32)
        _logger.warning(
            "No tokens configured; generated an ephemeral token for this session only."
        )
    return _FALLBACK_TOKEN


def token_registry() -> dict[str, TokenInfo]:
    """
    Returns token -> role mapping.
    Priority:
      1) BLACKBOX_PRO_TOKENS env (comma-separated role:token pairs)
      2) BLACKBOX_PRO_TOKEN_FILE (single token or role:token per line)
      3) BLACKBOX_PRO_TOKEN (single token, admin)
    """
    env_tokens = os.environ.get("BLACKBOX_PRO_TOKENS")
    if env_tokens:
        parts = [p.strip() for p in env_tokens.split(",") if p.strip()]
        reg = _parse_token_lines(parts)
        if reg:
            return reg

    token_file = os.environ.get("BLACKBOX_PRO_TOKEN_FILE")
    if token_file:
        try:
            with open(token_file, "r", encoding="utf-8") as f:
                lines = f.read().splitlines()
            reg = _parse_token_lines(lines)
            if reg:
                return reg
        except Exception as e:
            _logger.debug("Failed to read token registry: %s", e)

    tok = os.environ.get("BLACKBOX_PRO_TOKEN")
    if tok:
        return {tok: TokenInfo(role="admin", tenants=["*"])}

    if os.environ.get("BLACKBOX_PRO_ALLOW_DEV_TOKEN") == "1":
        return {"dev-secret-token": TokenInfo(role="admin", tenants=["*"])}

    # No configured tokens; generate a session token.
    return {_fallback_token(): TokenInfo(role="admin", tenants=["*"])}


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
) -> tuple[bool, int, str, dict, Optional[str], Optional[str], Optional[list[str]]]:
    """
    Returns: (ok, status_code, detail, headers, role, token_id, tenants)
    """
    token = _extract_bearer_token(request)

    if token is None and allow_query_token:
        token = request.query_params.get("token") or None
        if token is not None:
            token = token.strip() or None

    if not token:
        proxy_info = _proxy_sso_info(request)
        if proxy_info is not None:
            return True, 200, "ok", {}, proxy_info.role, _hash_token("proxy:" + proxy_info.role), proxy_info.tenants
        return False, 401, "Missing Authorization: Bearer <token>", {"WWW-Authenticate": "Bearer"}, None, None, None

    registry = token_registry()
    info = registry.get(token)
    if info is None:
        oidc_info = _oidc_verify(token)
        if oidc_info is not None:
            return True, 200, "ok", {}, oidc_info.role, _hash_token(token), oidc_info.tenants
        jwt_info = _jwt_verify(token)
        if jwt_info is not None:
            return True, 200, "ok", {}, jwt_info.role, _hash_token(token), jwt_info.tenants
        return False, 403, "Invalid token", {"WWW-Authenticate": "Bearer"}, None, _hash_token(token), None

    return True, 200, "ok", {}, info.role, _hash_token(token), info.tenants


async def require_token(request: Request, *, allow_query_token: bool) -> None:
    ok, status, detail, headers, role, token_id, tenants = verify_request_token(request, allow_query_token=allow_query_token)
    if not ok:
        raise HTTPException(status_code=status, detail=detail, headers=headers)
    request.state.auth_role = role or "admin"
    request.state.auth_token_id = token_id or "unknown"
    request.state.auth_tenants = tenants or ["*"]


def require_role(request: Request, roles: set[str]) -> None:
    role = getattr(request.state, "auth_role", None) or "viewer"
    if role not in roles:
        raise HTTPException(status_code=403, detail="Insufficient role")


def require_project_access(request: Request, project: str) -> None:
    tenants = getattr(request.state, "auth_tenants", None) or ["*"]
    if "*" in tenants:
        return
    if project not in tenants:
        raise HTTPException(status_code=403, detail="Project not allowed for this token")
