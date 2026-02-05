# Benchmarks (Buyer-Friendly Summary)

## Purpose
This page summarizes performance and integrity overhead for Blackbox Data Pro.
All results below are representative runs; re-run on your hardware to confirm.

## Environment
- CPU: Not available in sandbox (sysctl blocked)
- RAM: Not available in sandbox (sysctl blocked)
- OS: macOS 26.2 (arm64)
- Python: 3.11.9
- Pandas: 3.0.0
- PyArrow: 23.0.0
- Blackbox Data Pro version: 0.1.0

## How to Run
```bash
python benchmarks/run_benchmarks.py --sizes 1000000
python benchmarks/run_benchmarks.py --sizes 1000000 --wide-cols 50
```

Optional profiling (macOS/Linux):
```bash
/usr/bin/time -l python benchmarks/run_benchmarks.py --sizes 1000000
```

## Results
| Test | Rows | Cols | Time (s) | Rows/s | Notes |
|---|---:|---:|---:|---:|---|
| content_fingerprint_rowhash | 1,000,000 | 5 | 0.517 | 1,935,865 | mean over 3 runs |
| diff_rowhash | 1,000,000 | 5 | 1.039 | 962,071 | mean over 3 runs |
| snapshot_maybe_write_df_artifact | 1,000,000 | 5 | 0.010 | 102,893,450 | snapshot skipped by size guard |
| content_fingerprint_rowhash_wide50 | 1,000,000 | 55 | 0.616 | 1,624,082 | mean over 3 runs |
| diff_rowhash_wide50 | 1,000,000 | 55 | 1.645 | 607,920 | mean over 3 runs |
| snapshot_maybe_write_df_artifact_wide50 | 1,000,000 | 55 | 0.013 | 77,173,561 | snapshot skipped by size guard |

## Performance Observations (Feb 5, 2026)
- Row‑diffing stays under ~2s for 1M rows even with 55 columns, which is fast enough for most batch pipelines.
- Wide tables reduce diff throughput (~0.61M rows/sec) but stay within acceptable SLA for batch jobs.
- Fingerprint hashing is ~0.52–0.62s per 1M rows (improved), enabling very fast no‑op detection.
- Snapshot timings are extremely low here because `SnapshotConfig(max_mb=0.6)` skips full writes at 1M rows; measure full disk writes with `SnapshotConfig(mode="always")` for true storage cost.

## Comparison vs Previous Run (before fingerprint optimization)
Previous 1M‑row results:
- content_fingerprint_rowhash: 0.823s (1.22M rows/sec)
- diff_rowhash: 1.328s (0.75M rows/sec)
- snapshot_maybe_write_df_artifact: 0.183s (5.45M rows/sec)
- content_fingerprint_rowhash_wide50: 0.974s (1.03M rows/sec)
- diff_rowhash_wide50: 1.773s (0.56M rows/sec)
- snapshot_maybe_write_df_artifact_wide50: 0.129s (7.75M rows/sec)

Changes (new run vs previous):
- `content_fingerprint_rowhash` improved by ~37% (0.823s → 0.517s).
- `content_fingerprint_rowhash_wide50` improved by ~37% (0.974s → 0.616s).
- `diff_rowhash` improved by ~22% (1.328s → 1.039s).
- `diff_rowhash_wide50` improved by ~7% (1.773s → 1.645s).
- Snapshot timings are not comparable due to size‑guard skipping in the latest run.
## Integrity Overhead
Verification re-hashes only the metadata/chain payloads (not full datasets).
Typical overhead is minimal compared to data transforms and I/O.

## Notes for Buyers
- Optimized for medium-to-large batch workloads (up to ~1M rows per step).
- Wide-frame hashing auto-parallelizes when column counts are high.
- Diffing can skip deep scans when fingerprints match (no-op runs).
- Evidence bundles include cryptographic manifests for portability.
