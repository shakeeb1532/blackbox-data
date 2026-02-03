import pandas as pd
import pytest
from blackbox.hashing import diff_rowhash

def test_diff_rowhash_added_removed():
    a = pd.DataFrame({"x":[1,2,3]})
    b = pd.DataFrame({"x":[1,3,4]})
    payload, summary = diff_rowhash(a, b, order_sensitive=False)
    assert summary.added == 1
    assert summary.removed == 1
    assert payload["mode"] == "rowhash"

def test_diff_rowhash_missing_pk():
    a = pd.DataFrame({"id":[1,2], "x":[10, 20]})
    b = pd.DataFrame({"x":[10, 20]})
    with pytest.raises(ValueError, match="Primary key columns missing"):
        diff_rowhash(a, b, primary_key=["id"])

def test_diff_rowhash_duplicate_pk():
    a = pd.DataFrame({"id":[1,1,2], "x":[10, 11, 20]})
    b = pd.DataFrame({"id":[1,2,3], "x":[10, 20, 30]})
    with pytest.raises(ValueError, match="Primary key values must be unique"):
        diff_rowhash(a, b, primary_key=["id"])

def test_diff_rowhash_summary_only_threshold():
    a = pd.DataFrame({"id":[1,2,3,4], "x":[10, 20, 30, 40]})
    b = pd.DataFrame({"id":[10,11,12,13], "x":[10, 20, 30, 40]})
    payload, summary = diff_rowhash(a, b, primary_key=["id"], summary_only_threshold=0.5)
    assert summary.added == 4
    assert summary.removed == 4
    assert payload["summary_only"] is True
    assert payload["added_keys"] == []

def test_diff_rowhash_keys_only():
    a = pd.DataFrame({"id":[1,2,3], "x":[10, 20, 30]})
    b = pd.DataFrame({"id":[1,2,3], "x":[999, 20, 30]})
    payload, summary = diff_rowhash(a, b, primary_key=["id"], diff_mode="keys-only")
    assert summary.added == 0
    assert summary.removed == 0
    assert summary.changed == 0
    assert payload["diff_mode"] == "keys-only"

def test_diff_rowhash_chunked_matches():
    a = pd.DataFrame({"id":[1,2,3,4], "x":[10, 20, 30, 40]})
    b = pd.DataFrame({"id":[1,3,4,5], "x":[10, 99, 40, 50]})
    payload1, summary1 = diff_rowhash(a, b, primary_key=["id"])
    payload2, summary2 = diff_rowhash(a, b, primary_key=["id"], chunk_rows=2)
    assert summary1 == summary2
    assert payload1["summary"] == payload2["summary"]
