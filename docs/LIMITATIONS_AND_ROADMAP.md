# Limitations and Roadmap (v1.0)

This document clarifies the current scope and the intentional trade-offs of the v1.0 developer‑tooling release, and outlines the next planned expansions.

## Current Focus (v1.0)
- **Pandas-first batch pipelines**: Optimized for pandas-based, step-wise batch workflows.
- **Developer‑tooling product**: Self-hosted (local or Docker) with token‑protected UI and APIs.
- **Forensic clarity over UI polish**: The UI prioritizes correctness and auditability. Enterprise UX is a near‑term roadmap item.
- **Metadata‑first storage**: Stores cryptographic metadata, diffs, and optional artifacts rather than full dataset copies.
- **Single‑tenant auth**: Bearer token for local/teams with basic access control.
- **Integrity-first**: Verification adds minimal overhead, acceptable for audit‑grade guarantees.

## Trade-offs (Why These Are OK for v1.0)
- **Pandas wedge** validates demand quickly before expanding to distributed engines.
- **Batch focus** aligns with audit-heavy workflows where post‑hoc inspection matters most.
- **Metadata‑only storage** lowers compliance risk and reduces storage cost.
- **MVP UI** accelerates feedback cycles for real users.

## Roadmap (Next 3–6 Months)
### Engines and scale
- Spark/DuckDB/Polars adapters (engine interface + plug‑in strategy).
- Chunked + parallel diff for larger datasets (beyond ~1M rows/step).

### Streaming support
- Micro‑batch snapshots and windowed diffs.
- Event‑level verification for incremental pipelines.

### Security and enterprise readiness
- Multi‑tenant access control and RBAC.
- SSO integration and audit log export.

### UI and collaboration
- Enterprise UI polish (themes, layout consistency, guided analysis).
- Shareable reports + role‑based views.

## Performance Envelope (v1.0)
- Optimized for **~1M rows per step**.
- Larger datasets supported via `DiffConfig.chunk_rows` to reduce peak memory.
- Use `DiffConfig.summary_only_threshold` to skip deep diff on high‑churn steps.

## Summary
v1.0 is designed to **prove value fast** in the most common audit‑grade pandas workflows, while setting the architecture up for distributed engines and hosted SaaS later.
