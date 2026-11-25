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
from gen_keys import derive_seed_bytes  # noqa: E402  (gen_keys.py)

Q = (1 << 255) - 19
D = (-121665 * pow(121666, Q - 2, Q)) % Q
I = pow(2, (Q - 1) // 4, Q)


def xrecover(y):
    xx = (y * y - 1) * pow(D * y * y + 1, Q - 2, Q) % Q
    x = pow(xx, (Q + 3) // 8, Q)
    if (x * x - xx) % Q != 0:
        x = (x * I) % Q
    if x % 2 != 0:
        x = Q - x
    return x


BY = (4 * pow(5, Q - 2, Q)) % Q
B = (xrecover(BY), BY)


def edwards(P, Q_):
    x1, y1 = P
    x2, y2 = Q_
    x3 = (x1 * y2 + x2 * y1) * pow(1 + D * x1 * x2 * y1 * y2, Q - 2, Q) % Q
    # a = -1, so y3 = (y1*y2 - a*x1*x2)/(1 - d*x1*x2*y1*y2) = (y1*y2 + x1*x2)/(...)
    y3 = (y1 * y2 + x1 * x2) * pow(1 - D * x1 * x2 * y1 * y2, Q - 2, Q) % Q
    return (x3, y3)


def scalarmult(P, e):
    Q_ = (0, 1)
    while e > 0:
        if e & 1:
            Q_ = edwards(Q_, P)
        P = edwards(P, P)
        e >>= 1
    return Q_


def pubkey(seed: bytes) -> bytes:
    h = hashlib.sha512(seed).digest()
    a = int.from_bytes(h[:32], "little")
    a &= (1 << 254) - 8
    a |= 1 << 254
    x, y = scalarmult(B, a)
    return (y | ((x & 1) << 255)).to_bytes(32, "little")


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
