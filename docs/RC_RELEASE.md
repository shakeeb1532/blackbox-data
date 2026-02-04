# RC Release Checklist

## Quality Gates
- [ ] `pytest` clean (`.venv/bin/python -m pytest`)
- [ ] Schemathesis clean with seeded data (see `docs/TESTING.md`)
- [ ] `bandit` clean (`.venv/bin/bandit -r blackbox blackbox_pro -f json`)
- [ ] `pip-audit` clean (`.venv/bin/pip-audit -f json`)
- [ ] Benchmarks updated (`benchmarks/run_benchmarks.py`)

## Packaging
- [ ] Docker build succeeds
- [ ] README quickstart verified (local + docker)
- [ ] Demo run created and visible in UI

## Security
- [ ] Evidence bundle sign/verify path validated
- [ ] Audit log verify path validated
- [ ] `BLACKBOX_PRO_TOKEN`/`BLACKBOX_PRO_TOKENS` tested

## Release Notes (Short)
Template:
```
RC Summary:
- Stability: <ok/notes>
- Performance: <rows/sec highlights>
- Security: <SAST/audit status>
- API: <Schemathesis status>

Known Limitations:
- <if any>
```

## Executive Summary (Shareable)
Template:
```
Blackbox Data Pro RC delivers audit-grade pipeline diffing with evidence bundles,
tamper verification, and a production-ready UI. Internal tests and fuzzing are
clean, with performance improvements in wide-frame diffing and stable load
characteristics. Security checks (SAST + dependency audit) pass with no critical
findings. The product is ready for limited beta and internal adoption.
```
