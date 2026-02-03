# Benchmarks

This folder contains repeatable benchmark, load, stress, and security tests for Blackbox Data.

## Benchmark Suite
Runs micro-benchmarks across multiple row counts and wide frames.

```bash
.venv/bin/python benchmarks/run_benchmarks.py \
  --sizes 10000,100000,250000,500000,1000000 \
  --wide-cols 50 --warmup 1 --runs 3 \
  --output benchmarks/results_full.json \
  --output-csv benchmarks/results_full.csv
```

Outputs:
- `benchmarks/results_full.json`
- `benchmarks/results_full.csv`

## Load Test
Repeated runs at 1M rows for consistent throughput measurements.

```bash
.venv/bin/python benchmarks/run_load_tests.py
```

Output:
- `benchmarks/load_results.csv`

## Stress Test
Single large dataset run at 2M rows.

```bash
.venv/bin/python benchmarks/run_stress_tests.py
```

Output:
- `benchmarks/stress_results.csv`

## Security Test
Validates hash-chain tamper detection.

```bash
.venv/bin/python benchmarks/run_security_tests.py
```

Output:
- `benchmarks/security_results.csv`
