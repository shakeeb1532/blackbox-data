from __future__ import annotations

import csv
import os
import tempfile
import time
from dataclasses import dataclass

import pandas as pd

from blackbox import Recorder, Store, SealConfig


@dataclass
class TestResult:
    name: str
    status: str
    duration_ms: float
    message: str


def _now_ms() -> float:
    return time.perf_counter() * 1000.0


def test_chain_tamper() -> TestResult:
    t0 = _now_ms()
    with tempfile.TemporaryDirectory() as td:
        store = Store.local(td)
        rec = Recorder(store=store, project="sec", dataset="chain", seal=SealConfig(mode="chain"))
        run = rec.start_run()
        df = pd.DataFrame({"x": [1, 2]})
        with run.step("s1", input_df=df) as st:
            st.capture_output(pd.DataFrame({"x": [1, 2, 3]}))
        run.finish()

        ok, _ = run.verify()
        if not ok:
            return TestResult("chain_tamper", "fail", _now_ms() - t0, "initial verify failed")

        run_key = f"sec/chain/{run.run_id}/run_finish.json"
        run_obj = store.get_json(run_key)
        run_obj["status"] = "tampered"
        store.put_json(run_key, run_obj)

        ok2, msg2 = run.verify()
        if ok2:
            return TestResult("chain_tamper", "fail", _now_ms() - t0, "tamper not detected")
        return TestResult("chain_tamper", "pass", _now_ms() - t0, msg2)


def main() -> int:
    results = [test_chain_tamper()]
    out = os.path.join("benchmarks", "security_results.csv")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["name", "status", "duration_ms", "message"])
        for r in results:
            writer.writerow([r.name, r.status, f"{r.duration_ms:.2f}", r.message])
    print("Security test results written to:", out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
