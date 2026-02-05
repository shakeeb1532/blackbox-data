# Benchmarks (Buyer-Friendly Summary)

## Purpose
This page summarizes performance and integrity overhead for Blackbox Data Pro.
All results below are representative runs; re-run on your hardware to confirm.

## Environment
- CPU:
- RAM:
- OS:
- Python:
- Pandas:
- PyArrow:
- Blackbox Data Pro version:

## How to Run
```bash
python benchmarks/run_benchmarks.py --sizes 1000000
python benchmarks/run_benchmarks.py --sizes 1000000 --wide-cols 50
```

Optional profiling (macOS/Linux):
```bash
/usr/bin/time -l python benchmarks/run_benchmarks.py --sizes 1000000
```

## Results (Example Table)
| Test | Rows | Cols | Time (s) | Rows/s | Notes |
|---|---:|---:|---:|---:|---|
| diff_rowhash baseline | 1,000,000 | 10 |  |  |  |
| diff_rowhash wide | 1,000,000 | 50 |  |  |  |
| snapshot write | 1,000,000 | 10 |  |  | parquet + compression |

## Integrity Overhead
Verification re-hashes only the metadata/chain payloads (not full datasets).
Typical overhead is minimal compared to data transforms and I/O.

## Notes for Buyers
- Optimized for medium-to-large batch workloads (up to ~1M rows per step).
- Wide-frame hashing auto-parallelizes when column counts are high.
- Diffing can skip deep scans when fingerprints match (no-op runs).
- Evidence bundles include cryptographic manifests for portability.
