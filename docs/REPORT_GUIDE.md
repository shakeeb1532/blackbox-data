# How to Read a Blackbox Data Report

This guide explains the report fields and how to interpret changes between steps.

## Key Concepts
- **Schema diff**: structural changes (added/removed columns, dtype changes).
- **Row diff**: content changes (added/removed/changed keys).
- **Snapshot**: stored Parquet artifacts when enabled and below size limits.
- **Seal / chain**: tamper-evident hash chain across run artifacts.

## Step Summary
Each step shows:
- **Rows**: row counts before and after the step.
- **Columns**: column counts before and after the step.
- **Schema summary**: +added / −removed / dtype changes.
- **Row summary**: +added / −removed / changed keys.

## Diff Hints
Some diffs are intentionally abbreviated:
- `summary_only_high_churn`: the change ratio is high, so the report shows only counts.
- `diff_skipped_fingerprint_match`: schema and fingerprints match; deep diff skipped.
- `diff_schema_only`: schema summary only, no row diff.

## Practical Interpretation
- **Schema changes without row changes** often indicate a projection/rename step.
- **Row changes with stable schema** often indicate filtering, joins, or updates.
- **Large churn** likely means a full refresh or major data source change.

## When to Use Chunked Diff
For large datasets (>1M rows), enable chunking:
```python
DiffConfig(chunk_rows=250_000)
```
This lowers peak memory and keeps reports responsive.
