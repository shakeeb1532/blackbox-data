# benchmarks

Micro-benchmarks for hashing, diffing, and snapshot writing.

Run:
```bash
.venv/bin/python -m benchmarks.run_benchmarks --sizes 1000000 --wide-cols 50 --runs 3 --warmup 1 \
  --output reports/benchmarks.json --output-csv reports/benchmarks.csv
```

Force full snapshot writes (no size guard):
```bash
.venv/bin/python -m benchmarks.run_benchmarks --sizes 1000000 --force-snapshot
```

Summary results live in `BENCHMARKS.md` at repo root.
