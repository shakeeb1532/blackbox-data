import os
import json
import pandas as pd
import pytest
pytest.importorskip("httpx")
from fastapi.testclient import TestClient

from blackbox import Recorder, Store, DiffConfig, SnapshotConfig, SealConfig
from blackbox_pro.server.main import app


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
    df = pd.DataFrame({"id": [1, 2], "x": [10, 20]})
    with run.step("s1", input_df=df) as st:
        out = df.copy()
        out["x"] = out["x"] + 1
        st.capture_output(out)
    run.finish()
    return run.run_id


@pytest.fixture()
def client_run(tmp_path, monkeypatch):
    root = str(tmp_path)
    token = "test-token"
    monkeypatch.setenv("BLACKBOX_PRO_ROOT", root)
    monkeypatch.setenv("BLACKBOX_PRO_TOKEN", token)
    run_id = _create_run(root)
    client = TestClient(app)
    return client, run_id, token, root


def test_runs_missing_token(client_run):
    client, _, _, _ = client_run
    r = client.get("/runs?project=acme-data&dataset=demo")
    assert r.status_code == 401
    assert r.headers.get("WWW-Authenticate") == "Bearer"


def test_runs_bad_token(client_run):
    client, _, _, _ = client_run
    r = client.get(
        "/runs?project=acme-data&dataset=demo",
        headers={"Authorization": "Bearer bad"},
    )
    assert r.status_code == 403


def test_runs_good_token(client_run):
    client, _, token, _ = client_run
    r = client.get(
        "/runs?project=acme-data&dataset=demo",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert r.status_code == 200


def test_docs_public(client_run):
    client, _, _, _ = client_run
    assert client.get("/docs").status_code == 200
    assert client.get("/openapi.json").status_code == 200


def test_ui_home_requires_token(client_run):
    client, _, token, _ = client_run
    assert client.get("/ui/home").status_code == 401
    assert client.get(f"/ui/home?token={token}").status_code == 200


def test_ui_run_valid(client_run):
    client, run_id, token, _ = client_run
    r = client.get(f"/ui?project=acme-data&dataset=demo&run_id={run_id}&token={token}")
    assert r.status_code == 200


def test_ui_run_missing(client_run):
    client, _, token, _ = client_run
    r = client.get(f"/ui?project=acme-data&dataset=demo&run_id=missing&token={token}")
    assert r.status_code == 200
    assert "Run not found" in r.text


def test_ui_docs(client_run):
    client, _, token, _ = client_run
    r = client.get(f"/ui/docs?token={token}")
    assert r.status_code == 200


def test_ui_metrics(client_run):
    client, _, token, _ = client_run
    r = client.get(f"/ui/metrics?token={token}")
    assert r.status_code == 200


def test_metrics_requires_auth(client_run):
    client, _, token, _ = client_run
    assert client.get("/metrics").status_code == 401
    r = client.get("/metrics", headers={"Authorization": f"Bearer {token}"})
    assert r.status_code == 200


def test_stats_endpoint(client_run):
    client, _, token, _ = client_run
    r = client.get("/stats", headers={"Authorization": f"Bearer {token}"})
    assert r.status_code == 200
    assert "runs_total" in r.json()


def test_ui_exports(client_run):
    client, run_id, token, _ = client_run
    r = client.get(f"/ui/export_json?project=acme-data&dataset=demo&run_id={run_id}&token={token}")
    assert r.status_code == 200
    r = client.get(f"/ui/export_html?project=acme-data&dataset=demo&run_id={run_id}&token={token}")
    assert r.status_code == 200
    r = client.get(f"/ui/export_evidence_json?project=acme-data&dataset=demo&run_id={run_id}&token={token}")
    assert r.status_code == 200


def test_ui_diff_keys_download(client_run):
    client, run_id, token, _ = client_run
    r = client.get(
        f"/ui/diff_keys?project=acme-data&dataset=demo&run_id={run_id}&ordinal=1&kind=added&token={token}"
    )
    assert r.status_code == 200


def test_verify_ok(client_run):
    client, run_id, token, _ = client_run
    r = client.get(
        f"/verify?project=acme-data&dataset=demo&run_id={run_id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert r.status_code == 200
    assert r.json().get("ok") is True


def test_verify_tamper(client_run):
    client, run_id, token, root = client_run
    run_key = os.path.join(root, "acme-data", "demo", run_id, "run_finish.json")
    with open(run_key, "r", encoding="utf-8") as f:
        obj = json.load(f)
    obj["status"] = "tampered"
    with open(run_key, "w", encoding="utf-8") as f:
        json.dump(obj, f)

    r = client.get(
        f"/verify?project=acme-data&dataset=demo&run_id={run_id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert r.status_code == 200
    assert r.json().get("ok") is False


def test_audit_export_admin_viewer(tmp_path, monkeypatch):
    root = str(tmp_path)
    admin_token = "admin-token"
    viewer_token = "viewer-token"
    monkeypatch.setenv("BLACKBOX_PRO_ROOT", root)
    monkeypatch.setenv("BLACKBOX_PRO_TOKENS", f"admin:{admin_token},viewer:{viewer_token}")
    run_id = _create_run(root)
    client = TestClient(app)

    # Generate at least one request so audit log exists.
    r = client.get(
        f"/runs?project=acme-data&dataset=demo",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert r.status_code == 200

    r = client.get("/audit", headers={"Authorization": f"Bearer {admin_token}"})
    assert r.status_code == 200
    assert "request" in r.text

    r = client.get("/siem?format=cef", headers={"Authorization": f"Bearer {admin_token}"})
    assert r.status_code == 200


def test_evidence_bundle(tmp_path, monkeypatch):
    root = str(tmp_path)
    token = "admin-token"
    monkeypatch.setenv("BLACKBOX_PRO_ROOT", root)
    monkeypatch.setenv("BLACKBOX_PRO_TOKENS", f"admin:{token}")
    run_id = _create_run(root)
    client = TestClient(app)

    r = client.get(
        f"/evidence?project=acme-data&dataset=demo&run_id={run_id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert r.status_code == 200
    assert r.headers.get("Content-Disposition", "").startswith("attachment; filename=evidence_")

    # verify via API endpoint
    r2 = client.post(
        "/evidence/verify",
        headers={"Authorization": f"Bearer {token}"},
        files={"file": ("evidence.zip", r.content, "application/zip")},
    )
    assert r2.status_code == 200
    assert r2.json().get("ok") is True


def test_tenant_restriction(tmp_path, monkeypatch):
    root = str(tmp_path)
    token = "viewer-token"
    monkeypatch.setenv("BLACKBOX_PRO_ROOT", root)
    monkeypatch.setenv("BLACKBOX_PRO_TOKENS", f"viewer@other-project:{token}")
    _create_run(root)
    client = TestClient(app)

    r = client.get(
        "/runs?project=acme-data&dataset=demo",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert r.status_code == 403
