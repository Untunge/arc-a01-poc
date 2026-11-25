#!/usr/bin/env python3
"""Derive the 4 deterministic localdev validator Ed25519 keys and write
priv_validator_key.json files (CometBFT format) for poc-a01 net nodes 0..3.

Construction (must match BOTH:
 - arc-node crates/malachite-cli/src/new.rs: derive_bip39_child_sk_bytes(2, n)
   (bip32 crate, standard BIP32, HMAC key "Bitcoin seed")
 - scripts/genesis/AccountCreator.ts validators(): BIP44 m/44'/60'/0'/1/{i+2}
   from the same test mnemonic, Ed25519 pubkey from the 32-byte seed)
Key file JSON: {"type": "tendermint/PrivKeyEd25519", "value": "<base64 seed>"}.

Self-check: indices 2,3,4 must equal the well-known seeds asserted in
new.rs::derives_child_keys_well_known. Exits non-zero on mismatch.
"""
import base64
import hashlib
import hmac
import json
import os
import sys

MNEMONIC = "test test test test test test test test test test test junk"
PASSPHRASE = ""
PATH_PREFIX = [44, 60, 0, 1]  # m/44'/60'/0'/1, all hardened
START_INDEX = 2
NODES = 4

EXPECTED = {
    2: "93ac6a66e7b27d3b21eb05c3edf07d3380019460b761ee117cdca9d3215e1b2d",
    3: "1a864302982c12335b26a63fd7b841c6491e58530fc2f25c23a4191a7ea31c90",
    4: "a8bf2e57dbee36fcc50f072cd71ee0f885e8b36cad256c927048fd5474b6ad56",
}

def hmac_sha512(key: bytes, msg: bytes) -> bytes:
    return hmac.new(key, msg, hashlib.sha512).digest()


SECP256K1_N = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141
SECP256K1_P = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEFFFFFC2F
SECP256K1_G = (
    0x79BE667EF9DCBBAC55A06295CE870B07029BFCDB2DCE28D959F2815B16F81798,
    0x483ADA7726A3C4655DA4FBFC0E1108A8FD17B448A68554199C47D08FFB10D4B8,
)


def _ec_add(p, q):
    if p is None:
        return q
    if q is None:
        return p
    x1, y1 = p
    x2, y2 = q
    if x1 == x2 and y1 != y2:
        return None
    if p == q:
        lam = (3 * x1 * x1) * pow(2 * y1, -1, SECP256K1_P) % SECP256K1_P
    else:
        lam = (y2 - y1) * pow(x2 - x1, -1, SECP256K1_P) % SECP256K1_P
    x3 = (lam * lam - x1 - x2) % SECP256K1_P
    return x3, (lam * (x1 - x3) - y1) % SECP256K1_P


def _ec_mul(k, point=SECP256K1_G):
    result = None
    addend = point
    while k:
        if k & 1:
            result = _ec_add(result, addend)
        addend = _ec_add(addend, addend)
        k >>= 1
    return result


def _compressed_pubkey(k: int) -> bytes:
    x, y = _ec_mul(k)
    return (b"\x03" if y & 1 else b"\x02") + x.to_bytes(32, "big")


def derive_seed_bytes(index: int) -> bytes:
    """BIP32 derivation for m/44'/60'/0'/1/{index} where only 44,60,0 are
    hardened; the '1' and the final index are NORMAL (ChildNumber::new(idx, false))."""
    seed = hashlib.pbkdf2_hmac(
        "sha512", MNEMONIC.encode(), ("mnemonic" + PASSPHRASE).encode(), 2048, 64
    )
    master = hmac_sha512(b"Bitcoin seed", seed)
    k, c = int.from_bytes(master[:32], "big"), master[32:]
    for element, hardened in [(44, True), (60, True), (0, True), (1, False), (index, False)]:
        if hardened:
            data = b"\x00" + k.to_bytes(32, "big") + (element | 0x80000000).to_bytes(4, "big")
        else:
            data = _compressed_pubkey(k) + element.to_bytes(4, "big")
        i = hmac_sha512(c, data)
        k = (int.from_bytes(i[:32], "big") + k) % SECP256K1_N
        c = i[32:]
    return k.to_bytes(32, "big")


def main() -> int:
    poc = os.environ.get(
        "POC_DIR",
        os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")))
    for idx, expected in EXPECTED.items():
        got = derive_seed_bytes(idx).hex()
        if got != expected:
            print(f"SELF-CHECK FAIL index {idx}: got {got} want {expected}", file=sys.stderr)
            return 1
    print("self-check OK: indices 2,3,4 match new.rs well-known seeds")
    for node in range(NODES):
        idx = START_INDEX + node
        seed = derive_seed_bytes(idx)
        keyfile = os.path.join(poc, "net", f"node{node}", "malachite", "config", "priv_validator_key.json")
        os.makedirs(os.path.dirname(keyfile), exist_ok=True)
        with open(keyfile, "w") as f:
            json.dump(
                {"type": "tendermint/PrivKeyEd25519", "value": base64.b64encode(seed).decode()},
                f, indent=2,
            )
            f.write("\n")
        os.chmod(keyfile, 0o600)
        print(f"node{node}: path m/44'/60'/0'/1/{idx} seed {seed.hex()} -> {keyfile}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
