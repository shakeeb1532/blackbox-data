from __future__ import annotations
from dataclasses import dataclass
import hashlib
from typing import Any, Protocol, Tuple

from .util import canonical_json_bytes

def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def digest_label(hexstr: str) -> str:
    return f"sha256:{hexstr}"

def payload_digest(obj: Any) -> str:
    return digest_label(sha256_hex(canonical_json_bytes(obj)))

def chain_digest(prev_digest: str | None, payload_digest_: str, typ: str, ts: str) -> str:
    prev = prev_digest or ""
    material = (prev + "\n" + payload_digest_ + "\n" + typ + "\n" + ts).encode("utf-8")
    return digest_label(sha256_hex(material))

@dataclass
class ChainEntry:
    index: int
    type: str
    ts: str
    payload_ref: str
    payload_digest: str
    prev: str | None
    digest: str

class PayloadReader(Protocol):
    def get_json(self, key: str) -> dict[str, Any]: ...

def verify_chain_structure(chain: dict) -> Tuple[bool, str]:
    """
    Verifies only the hash-chain linkage (prev -> digest), assuming payload_digests are correct.
    """
    entries = chain.get("entries", [])
    prev = None
    for i, e in enumerate(entries):
        if e.get("index") != i:
            return False, f"Bad index at {i}"
        expected = chain_digest(prev, e["payload_digest"], e["type"], e["ts"])
        if e.get("digest") != expected:
            return False, f"Digest mismatch at {i}"
        if e.get("prev") != prev:
            return False, f"Prev mismatch at {i}"
        prev = e.get("digest")
    head = chain.get("head")
    if entries and head != entries[-1]["digest"]:
        return False, "Head mismatch"
    return True, "ok"

def verify_chain_with_payloads(
    chain: dict,
    reader: PayloadReader,
    *,
    run_prefix: str,
) -> Tuple[bool, str]:
    """
    Full verification:
      1) Recompute each payload's digest from stored JSON and compare to chain entry payload_digest
      2) Verify chain linkage using those (existing) payload_digest fields
    """
    entries = chain.get("entries", [])
    if not isinstance(entries, list):
        return False, "Invalid chain entries"

    # 1) payload integrity
    for i, e in enumerate(entries):
        ref = e.get("payload_ref")
        pd_expected = e.get("payload_digest")
        if not ref or not pd_expected:
            return False, f"Missing payload_ref/payload_digest at {i}"

        # chain stores payload_ref relative to run root; reconstruct key
        key = f"{run_prefix}/{ref}".replace("//", "/")
        try:
            obj = reader.get_json(key)
        except Exception as ex:
            return False, f"Failed to load payload at {i}: {ref} ({ex})"

        pd_actual = payload_digest(obj)
        if pd_actual != pd_expected:
            return False, f"Payload digest mismatch at {i}: {ref}"

    # 2) chain linkage
    return verify_chain_structure(chain)

