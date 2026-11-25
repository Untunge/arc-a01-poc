# Arc A-01 localnet PoC (C2-01)

Localnet proof-of-concept for the re-evaluation of HackerOne report **A-01
(#4229458)** — arc-node pins malachite **v0.8.0**, in which 13
resource-exhaustion vectors were fixed in v0.8.1/v0.8.2. The report was closed
as Informative for lack of a practical exploitation scenario; this repo
produces execution evidence on an **unmodified** arc-node, on a localnet only.

**Lead vector — C2-01:** WAL replay drops own votes past the 10-round
lookahead (`malachite v0.8.0`, `core-consensus/src/handle/vote.rs:80-96`,
no replay exemption; `wal_replay()` replays through the same `process_input`
path on a clean driver). After a >10-round stall + crash + restart, the
validator can lose its lock/votes and **equivocate** — sign a conflicting vote
at the same (height, round). Fixed upstream in v0.8.1/v0.8.2
(`!state.is_replaying_wal() && exceeds_future_round_lookahead(...)`).

## Safety

- Localnet only: everything binds `127.0.0.1`; no mainnet/testnet traffic.
- Public code only: `circlefin/arc-node` pinned to commit
  `6e764023ee6515fe70573e123ed2db912a7207b4` (main HEAD, 25 Sep 2026).
- Validator keys are derived from the **public** test mnemonic shipped in
  arc-node itself (`test test ... junk`) — no real secrets.
- Nothing in this repo submits anything to HackerOne. The re-evaluation
  comment is drafted by a human afterwards, from the evidence artifacts.
- Honesty note: release v0.8.6 (18 Nov 2026) already bumped the pin to
  malachite v0.8.2, so the triager may still close the report as fixed.
  The PoC proves the bug existed in the pinned v0.8.0 code, not that it is
  still present in the latest release.

## Layout

- `.github/workflows/poc-c2-01.yml` — GitHub Actions workflow (ubuntu-24.04):
  build EL+CL, set up a 4-validator localnet, run the PoC, upload evidence.
- `scripts/net-setup.sh` — create the 4-node layout (dirs, shared genesis +
  JWT, per-node launch scripts). Port table: CL p2p 27000+i, CL metrics
  29000+i, CL RPC 31000+i, EL HTTP 8545+i, EL WS 8645+i, EL authrpc 8551+i,
  EL metrics 9001+i.
- `scripts/gen_keys.py` — derive the 4 deterministic validator Ed25519 keys
  (BIP39 `m/44'/60'/0'/1/{2..5}`); self-checks against the well-known seeds
  asserted in `arc-node/crates/malachite-cli/src/new.rs`.
- `scripts/verify-keys.py` — verify the 4 pubkeys are in the localdev genesis
  validator-registry storage (`0x3600...0002`).
- `scripts/run-poc.sh` — the C2-01 sequence + evidence collection.
- `RUNBOOK-C2-01.md` — the manual runbook (same sequence, by hand).

## Run it

```bash
gh workflow run poc-c2-01.yml
```

Or locally (needs a built arc-node checkout at `./arc-node`, Rust 1.93.0,
`libclang-dev` for bindgen):

```bash
cargo build -p arc-node-execution -p arc-node-consensus   # in ./arc-node
bash scripts/net-setup.sh
python3 scripts/gen_keys.py
python3 scripts/verify-keys.py
bash scripts/run-poc.sh
```

Evidence lands in `evidence/c2-01/` (WAL + store.db snapshots, CL logs,
metrics before/after, `VERDICT.md`, `RUNLOG.md`) and is uploaded as a
workflow artifact.

## Verdict scale

- **PROVEN** — two conflicting signed votes from the target at the same
  (H, R) (e.g. nil pre-crash vs value post-restart).
- **PARTIAL** — vote loss shown (e.g. `dropped_future_round_votes` rose
  across the restart) but no conflicting pair captured.
- **FAILED** — no vote loss, or the net never stalled past round 10.
