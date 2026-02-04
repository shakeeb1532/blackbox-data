# Security, SSO, and SIEM

## Token Auth (default)
Use a single token:
```bash
export BLACKBOX_PRO_TOKEN=dev-secret-token
```

Use role‑based and multi‑tenant tokens:
```bash
export BLACKBOX_PRO_TOKENS="admin@acme-data:admin-token,viewer@acme-data|beta:viewer-token"
```

## OIDC JWT (optional)
If you want OIDC‑style tokens, provide a public key:
```bash
export BLACKBOX_PRO_JWT_PUBLIC_KEY="-----BEGIN PUBLIC KEY-----\n...\n-----END PUBLIC KEY-----"
export BLACKBOX_PRO_JWT_ALGORITHMS="RS256,HS256"
```
JWT claims used:
- `role` or `roles[0]`
- `tenant` or `tenants[]`

## SAML / Proxy SSO (optional)
Enable trusted proxy headers:
```bash
export BLACKBOX_PRO_TRUST_PROXY=1
export BLACKBOX_PRO_SSO_HEADER="x-auth-user"
export BLACKBOX_PRO_SSO_ROLE_HEADER="x-auth-role"
export BLACKBOX_PRO_SSO_TENANT_HEADER="x-auth-tenant"
```
This mode assumes an upstream SSO proxy injects identity headers.

## Audit Log + SIEM Export
Audit log JSONL:
```
GET /audit
```

SIEM export formats:
```
GET /siem?format=jsonl
GET /siem?format=cef
```

## Evidence Bundle Export
Download a portable evidence pack:
```
GET /evidence?project=<p>&dataset=<d>&run_id=<id>
```
Includes `run.json`, `chain.json`, and `verification.json`.

## Evidence Bundle Signing (HMAC)
If you set a signing key, the evidence bundle includes `manifest.json` and `signature.json`:
```bash
export BLACKBOX_PRO_EVIDENCE_HMAC_KEY="super-secret-key"
```
Signature covers the manifest (SHA‑256 hashes of bundle files).

## Evidence Bundle Signing (PGP)
To include a PGP signature on the manifest:
```bash
export BLACKBOX_PRO_GPG_KEY_ID="your-key-id"
```
This will add `manifest.json.asc` to the evidence bundle.

## Evidence Bundle Verification (API)
```bash
curl -H "Authorization: Bearer <token>" \
  -F "file=@evidence.zip" \
  http://127.0.0.1:8088/evidence/verify
```
