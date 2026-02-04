import os
import json

from blackbox_pro.server.audit import write_audit_event, verify_audit_log


def test_audit_verify(tmp_path, monkeypatch):
    path = os.path.join(str(tmp_path), "audit.jsonl")
    monkeypatch.setenv("BLACKBOX_PRO_AUDIT_LOG", path)

    write_audit_event({"event": "request", "path": "/runs", "status": 200})
    write_audit_event({"event": "request", "path": "/report", "status": 200})

    ok, count, msg = verify_audit_log(path)
    assert ok is True
    assert count == 2
    assert msg == "ok"

    # Tamper with last line
    with open(path, "r", encoding="utf-8") as f:
        lines = f.read().splitlines()
    obj = json.loads(lines[-1])
    obj["status"] = 500
    lines[-1] = json.dumps(obj)
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

    ok, count, msg = verify_audit_log(path)
    assert ok is False
