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
- `scripts/net-setup.sh` — create the 5-node layout (dirs, shared genesis +
  JWT, per-node launch scripts). Port table: CL p2p 27000+i, CL metrics
  29000+i, CL RPC 31000+i, EL HTTP 8545+i, EL WS 8645+i, EL authrpc 8551+i,
  EL metrics 9001+i, EL p2p 30303+i (i = 0..4).
- `scripts/gen_keys.py` — derive the 5 deterministic validator Ed25519 keys
  (BIP39 `m/44'/60'/0'/1/{2..6}`) + libp2p PeerIds; self-checks against the
  well-known seeds asserted in `arc-node/crates/malachite-cli/src/new.rs`
  and against node0's PeerId observed in the Actions run-1 CL log.
- `scripts/verify-keys.py` — verify the 4 pubkeys are in the localdev genesis
  validator-registry storage (`0x3600...0002`).
- `scripts/run-poc.sh` — the C2-01 sequence + evidence collection
  (5 nodes; run-5 freeze–jump–crash design). Stall mechanism: SIGSTOP all
  **ELs** — stopping CLs drops online power below the 67/100 quorum, so no
  polka ever forms and the Prevote timeout is never armed (malachite
  schedules it only on PolkaAny, `core-state-machine/src/state_machine.rs`
  L34/L35), so the net halts at round 0 by design (run 3). With the ELs
  frozen, the CLs keep voting (80/100), no proposer can build a block, and
  every round ends polka-nil → precommit nil → precommit timeout → next
  round, at the same height (run 4: H51 rounds 0→21). The target is frozen
  early (SIGSTOP its CL) so its WAL lacks the skipped rounds' Timeout
  entries; it resumes, jumps to the high round via a round certificate
  (which is NOT persisted to the WAL, `engine/src/wal/entry.rs`), votes
  NIL there, is crashed, and on restart the replay drops that vote
  (round > replay-round + 10) — then it prevotes the VALUE at the same
  round. All 5 genesis validators run so the net keeps quorum while the
  target is frozen.
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

## Vector (c) — gossipsub subscription flood

- `.github/workflows/poc-gossipsub-flood.yml` — same build/launch pattern,
  but a **single node** (node0 CL+EL; no quorum needed) plus a plain
  non-validator libp2p peer that subscribes to many distinct gossipsub
  topics in stages (default 5k / 20k / 50k at 1000/s).
- `harness/flood-peer/` — the flooding peer (Rust, libp2p 0.56.0 /
  libp2p-gossipsub 0.49.4, the exact versions the target uses; ephemeral
  keypair, no validator key/proof). Subscribes through the public
  `gossipsub::Behaviour::subscribe` API, one SUBSCRIBE RPC per topic.
- `scripts/run-flood.sh` — start node0, sample CL RSS every 1s (30s
  baseline → flood → 60s observe), count accepted subscriptions in
  `cl.log` (`SUBSCRIPTION: Adding gossip peer to topic`, one debug line per
  new topic). Evidence in `evidence/gossipsub-flood/` (`VERDICT.md`,
  `RUNLOG.md`, `rss.csv`, `rss-stats.txt`, logs, metrics before/after).
- Static verification: `../poc-a01/STATIC-gossipsub-flood.md` (kept outside
  this repo with the other A-01 notes). Mechanism: v0.8.0 builds the
  behaviour with plain `gossipsub::Behaviour::new` (default
  `AllowAllSubscriptionFilter`, no cap); each SUBSCRIBE lands in the peer's
  unbounded `PeerDetails.topics: BTreeSet<TopicHash>`. Fixed in v0.8.2 by
  `WhitelistSubscriptionFilter` (`network/src/behaviour.rs`).
