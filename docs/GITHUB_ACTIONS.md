# GitHub Actions Integration Example

Basic example to run a pipeline and upload the evidence bundle as an artifact.

```yaml
name: blackbox-audit
on: [push]

jobs:
  audit:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - run: pip install -e ".[pro]"
      - run: blackbox-pro start
      - run: blackbox --root ./.blackbox_store wrap --project acme --dataset demo -- python pipeline.py
      - run: blackbox-pro export --project acme --dataset demo --run $(ls .blackbox_store/acme/demo | tail -1) --format zip --out evidence.zip
      - uses: actions/upload-artifact@v4
        with:
          name: evidence
          path: evidence.zip
```
