# Runbook — PoC C2-01 (WAL replay drops own votes past 10-round lookahead)

Target: arc-node @ 6e76402 (main tip, malachite v0.8.0), unmodified, localnet only.
Vector: after a >10-round stall + crash + restart, the validator re-votes at a
(H, R) where it already voted differently before the crash (equivocation).

## Preconditions (done)
- [x] EL binary build running; CL binary to build after (`cargo build --offline -p ...`, jobs=1)
- [x] Keys: `gen_keys.py` — BIP39 m/44'/60'/0'/1/{2..5}; self-check vs new.rs well-known seeds OK
- [x] Keys verified: all 4 Ed25519 pubkeys present in genesis registry storage (0x3600...0002) — `verify-keys.py` OK
- [x] Net layout: `net-setup.sh` (dirs, shared genesis.json + jwt.hex + reth.toml, per-node run-el.sh/run-cl.sh)
- [ ] Port fix applied: EL WS 8645+i (design table had WS/HTTP collision)

## Mechanism (verified in v0.8.0 source, fixed in v0.8.1/v0.8.2)
- `malachite-v080/code/crates/core-consensus/src/handle/vote.rs:80-96`:
  `vote_round > consensus_round + MAX_FUTURE_ROUND_LOOKAHEAD (10)` → drop +
  `metrics.dropped_future_round_votes.inc()`. No replay exemption.
- `malachite-v080/code/crates/engine/src/consensus.rs:967` `wal_replay()` feeds WAL
  entries through the SAME `process_input` path on a clean driver — so replayed
  votes hit the lookahead check. v0.8.2 fix (`!state.is_replaying_wal() && ...`,
  `is_replaying_wal` in `state.rs`): "Replayed votes are exempt: they were
  admitted and applied by an earlier run, and a round certificate can carry them
  arbitrarily far ahead of the round replay has reached."
- v0.8.1 highlights: "Cross-check each vote and proposal re-derived during WAL
  replay against the node's own WAL record" + "Remove the WAL replay delay".
- Observables: metric `dropped_future_round_votes` (scrape 2900x before/after
  restart) + log line `Received vote for round beyond the future-round lookahead,
  dropping` (debug level) during replay + a conflicting re-vote at same (H, R).

## 1. Start (per node i=0..3: EL first, then CL)
```bash
cd ./net
for i in 0 1 2 3; do nohup ./node$i/run-el.sh & done
# wait: curl eth_blockNumber on 8545..8548 returns 0x0, authrpc up
for i in 0 1 2 3; do nohup ./node$i/run-cl.sh & done
```
Health: CL logs show height advancing (`node$i/logs/cl.log`); EL `eth_blockNumber` rises.
Record: PIDs (`pgrep -f 'node[0-3]/(run-)?(el|cl)'`), start time, first finalized height H0.

## 2. Baseline votes
- Grep each CL log for vote lines (exact strings TBD at runtime — malachite logs
  prevote/precommit broadcasts; update this section with the real strings).
- Record each node's latest vote at current (H, R): height, round, vote type, value/nil.
- Scrape metrics: `curl -s 127.0.0.1:2900$i/metrics` → save to `evidence/c2-01/metrics-before/`.

## 3. Stall (target = node0)
```bash
kill -STOP $(pgrep -f 'node2/run-cl') $(pgrep -f 'node3/run-cl')   # CLs only, ELs keep running
```
Remaining voting power: node0+node1 = 40/100 < 2/3 quorum → no progress; rounds
advance on timeouts only. Wait until node0's round at height H is > 10 past the
round of its last recorded vote. Check via CL log round lines / metrics.
Record: stall start time, H, R_last_voted, R_current when threshold passed.

## 4. Pre-crash snapshot (node0)
```bash
E=./evidence/c2-01
mkdir -p $E/pre-crash
cp ./net/node0/malachite/wal/consensus.wal $E/pre-crash/
cp ./net/node0/malachite/store.db $E/pre-crash/
cp ./net/node0/logs/cl.log $E/pre-crash/cl-before.log
grep -i "vote" $E/pre-crash/cl-before.log | tail -n 50 > $E/pre-crash/votes-before.txt
```
Record node0's latest vote at (H, R*) before the crash, verbatim log lines.

## 5. Crash + restart (node0 CL only)
```bash
kill -KILL $(pgrep -f 'node0/run-cl')   # SIGKILL the CL; EL keeps running
# restart with the SAME --home (WAL replay, WAL_REPLAY_DELAY=5s)
nohup ./net/node0/run-cl.sh &
```

## 6. Resume the mesh
```bash
kill -CONT $(pgrep -f 'node2/run-cl') $(pgrep -f 'node3/run-cl')
```
A proposal should now appear at a far round (R >> R*). Wait for node0 to vote again
at height H. Record its post-restart vote at the same (H, R*) if it re-votes there,
or at the new round.

## 7. Compare (the verdict)
- Extract node0's post-restart vote(s) at (H, R*): `grep -i vote node0/logs/cl.log`.
- Cross-check on a peer: node1's store/log should contain node0's pre-crash vote
  at (H, R*) (it gossiped before the crash) AND the post-restart vote.
- **PROVEN**: two conflicting signed votes from node0 at the same (H, R*)
  (e.g. nil pre-crash vs value post-restart, or different values). Quote both log
  lines + WAL/store evidence.
- **PARTIAL**: vote loss shown (pre-crash vote absent from post-restart state /
  re-vote differs in a way that is not a conflicting pair at the same (H,R)),
  but no conflicting pair captured.
- **FAILED**: no vote loss, or the net never stalled past round 10, or no re-vote.

## 8. Evidence to collect (evidence/c2-01/)
- `pre-crash/consensus.wal`, `pre-crash/store.db`, `pre-crash/cl-before.log`, `pre-crash/votes-before.txt`
- `post-restart/cl-after.log`, `post-restart/votes-after.txt`, metrics before/after
- peer (node1) `cl.log` excerpts showing both votes
- `RUNLOG.md`: timestamps for every step, PIDs, heights/rounds observed

## Notes / risks
- 4×(CL+EL) RAM untested on ~6 GB available; if OOM, drop to fewer nodes is NOT
  possible for C2-01 (needs 4 for quorum math) — fall back to C2-07 instead.
- If no proposal appears at a far round after resume, vary resume timing / wait longer.
- WAL replay delay is 5s; allow ≥30s after restart before judging.
- Never touch mainnet/testnet; all ports are 127.0.0.1; discovery OFF.
