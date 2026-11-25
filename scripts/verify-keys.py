#!/usr/bin/env python3
"""Verify the 4 derived validator Ed25519 pubkeys appear in the localdev
genesis validator-registry storage (0x3600...0002). Pure-stdlib Ed25519.
Reuses derive_seed_bytes() from gen-keys.py (already self-checked vs new.rs)."""
import hashlib
import json
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)
from gen_keys import derive_seed_bytes, ed25519_pubkey  # noqa: E402  (gen_keys.py)


def pubkey(seed: bytes) -> bytes:
    return ed25519_pubkey(seed)


def main() -> int:
    poc_dir = os.environ.get(
        "POC_DIR", os.path.normpath(os.path.join(_HERE, "..")))
    genesis = json.load(open(os.path.join(
        poc_dir, "net", "shared", "genesis.json")))
    registry = "0x3600000000000000000000000000000000000002"
    storage = genesis["alloc"].get(registry, {}).get("storage", {})
    values = {v.lower() for v in storage.values()}
    ok = True
    for node in range(4):
        seed = derive_seed_bytes(2 + node)
        pk = pubkey(seed)
        found = ("0x" + pk.hex()) in values
        print(f"node{node} pubkey 0x{pk.hex()} in-registry-storage: {found}")
        ok = ok and found
    print("ALL 4 PUBKEYS IN GENESIS REGISTRY" if ok else "MISMATCH — keys do NOT match genesis")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
