# Security Posture

## Threat Model
Blackbox Data is designed for **tamper‑evident auditability**, not full confidentiality.

We assume:
- Attackers may attempt to alter stored run artifacts.
- Operators need cryptographic evidence that a run has not been modified.

We provide:
- Hash‑chained evidence for run artifacts (`chain.json`).
- Verification APIs and UI evidence summaries.
- Optional audit log with hash chaining.

## Data Handling
- Default storage is **metadata + diffs**; full snapshots are optional.
- No network transmission unless you explicitly export.
- Supports self‑hosted deployment for data residency.

## Integrity Guarantees
- Each step is hashed and linked via a chain.
- Verification checks all payloads against the chain head.
- Exported reports include verification evidence.

## Audit Log
- Request audit log is append‑only JSONL.
- Each audit entry includes a hash and previous hash (`prev_hash`) for tamper evidence.
- Rotate logs by moving the JSONL file and archiving it.

## Audit Log Rotation
Set rotation and retention via:
```bash
export BLACKBOX_PRO_AUDIT_ROTATE_MB=10
export BLACKBOX_PRO_AUDIT_RETENTION_DAYS=30
```

## Not Confidentiality
This tool does **not** encrypt your stored artifacts. If you need encryption at rest:
- Use encrypted volumes.
- Restrict store filesystem permissions.
