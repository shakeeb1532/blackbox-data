import os
import json
import zipfile
import hmac
import hashlib

from fastapi.testclient import TestClient

from blackbox_pro.server.main import app
from blackbox import Recorder, Store, DiffConfig, SnapshotConfig, SealConfig


def _create_run(root: str) -> str:
    store = Store.local(root)
    rec = Recorder(
        store=store,
        project="acme-data",
        dataset="demo",
        diff=DiffConfig(mode="rowhash", primary_key=["id"]),
        snapshot=SnapshotConfig(mode="auto", max_mb=5),
        seal=SealConfig(mode="chain"),
    )
    run = rec.start_run(tags={"env": "test"})
    run.finish()
    return run.run_id


def test_evidence_bundle_signature(tmp_path, monkeypatch):
    root = str(tmp_path)
    token = "admin-token"
    hmac_key = "secret"
    monkeypatch.setenv("BLACKBOX_PRO_ROOT", root)
    monkeypatch.setenv("BLACKBOX_PRO_TOKENS", f"admin:{token}")
    monkeypatch.setenv("BLACKBOX_PRO_EVIDENCE_HMAC_KEY", hmac_key)
    run_id = _create_run(root)
    client = TestClient(app)

    r = client.get(
        f"/evidence?project=acme-data&dataset=demo&run_id={run_id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert r.status_code == 200
    bundle_path = os.path.join(root, "evidence.zip")
    with open(bundle_path, "wb") as f:
        f.write(r.content)

    with zipfile.ZipFile(bundle_path, "r") as zf:
        manifest = json.loads(zf.read("manifest.json"))
        sig_obj = json.loads(zf.read("signature.json"))
        manifest_bytes = zf.read("manifest.json")
        calc = hmac.new(hmac_key.encode("utf-8"), manifest_bytes, hashlib.sha256).hexdigest()
        assert sig_obj["signature"] == calc
        for name, expected in manifest.items():
            with zf.open(name) as f:
                data = f.read()
            assert hashlib.sha256(data).hexdigest() == expected
