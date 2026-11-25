#!/usr/bin/env bash
# run-far-future.sh — execute the far-future proposal PoC (A-01 vector (a))
# on a 5-node localnet and collect evidence.
#
# Vector: malachite v0.8.0 `on_proposal` has NO future-round lookahead bound
# (unlike `on_vote`, which drops rounds > current + 10). A proposal that
# passes proposer + signature verification is stored in the
# `FullProposalKeeper` (2 per round, but NO cap on the number of rounds, no
# eviction) and WAL-appended UNCONDITIONALLY — even an exact duplicate of an
# already-stored proposal. Fix in v0.8.2: `exceeds_future_round_lookahead`.
#
# Why 5 nodes (not single-node): rounds only advance when a polka forms
# (>= 67% voting power). A single node holds 20/100, so it can never form a
# polka and its Prevote timeout is never armed (state_machine.rs L34/L35) —
# it halts at round 0 by design (C2-01 run 3 lesson). With all 5 CLs up and
# all 5 ELs SIGSTOPped, no block can be built, so the height stalls while
# rounds skip via polka-nil (C2-01 run 4: H51 rounds 0->21; run 6: height 121
# rounds 0->27). The target is node0's CL; the crafted peer connects to it.
#
# Sequence:
# - Start 5 ELs + 5 CLs (node0 WITHOUT --p2p.persistent-peers-only, so the
#   non-validator crafted peer is accepted — same strip as run-flood.sh;
#   nodes 1-4 keep the flag, they only peer with each other + node0).
# - Baseline 60s (healthy, blocks advancing) -> SIGSTOP all 5 ELs -> wait
#   until node0 reaches round >= 5 at the stalled height -> stalled-baseline
#   60s (background WAL growth from votes/timeouts, no attack).
# - Launch proposal-peer: V1 = 600 proposals, one per round owned by a
#   single Byzantine validator (address-sorted index 1), rounds starting at
#   current+11; V2 = 600 replays of the exact same first proposal bytes.
# - Observe 60s -> stop CLs -> copy WALs + store.db -> stats + VERDICT.md.
#
# Observables: WAL file size per phase (stat, all 5 nodes), node0 RSS,
# node0 cl.log "Received proposal" lines (only logged AFTER verify passes)
# with proposal.round - consensus.round gap, cap-reject lines (expect 0),
# WAL "Wrote log entry ... type = Consensus(Proposal)" debug lines with
# entry.size, Prometheus metrics snapshots.
#
# Env:
#   POC_DIR             — repo root (default: parent of this script's dir)
#   V1_COUNT            — distinct-round proposals, default 600
#   V2_COUNT            — duplicate replays, default 600
#   RATE                — proposals/sec, default 100
#   OBSERVE_SECS        — peer gossip-observe window, default 20
#   V1_SETTLE_SECS      — settle between V1 and V2, default 10
#   HOLD_SECS           — peer hold after V2, default 30
#   BASELINE_SECS       — default 60
#   STALLED_BASELINE_SECS — default 60
#   OBSERVE_AFTER_SECS  — default 60
#   STALL_TARGET_ROUND  — round to reach at stalled height, default 5
#   STALL_WAIT_SECS     — max wait for stall rounds, default 300
#   VALIDATOR_INDEX     — address-sorted validator index to sign as, default 1
#   EVIDENCE_DIR        — default: $POC_DIR/evidence/far-future-proposals
#
# Exit code: 0 if the sequence ran to completion (whatever the verdict);
# non-zero only on infrastructure failure. The verdict lives in
# $EVIDENCE_DIR/VERDICT.md.
set -u
POC_DIR="${POC_DIR:-$(cd "$(dirname "$0")/.." && pwd)}"
V1_COUNT="${V1_COUNT:-600}"
V2_COUNT="${V2_COUNT:-600}"
RATE="${RATE:-100}"
OBSERVE_SECS="${OBSERVE_SECS:-20}"
V1_SETTLE_SECS="${V1_SETTLE_SECS:-10}"
HOLD_SECS="${HOLD_SECS:-30}"
BASELINE_SECS="${BASELINE_SECS:-60}"
STALLED_BASELINE_SECS="${STALLED_BASELINE_SECS:-60}"
OBSERVE_AFTER_SECS="${OBSERVE_AFTER_SECS:-60}"
STALL_TARGET_ROUND="${STALL_TARGET_ROUND:-5}"
STALL_WAIT_SECS="${STALL_WAIT_SECS:-300}"
VALIDATOR_INDEX="${VALIDATOR_INDEX:-1}"
NET="$POC_DIR/net"
EVIDENCE_DIR="${EVIDENCE_DIR:-$POC_DIR/evidence/far-future-proposals}"
RUNLOG="$EVIDENCE_DIR/RUNLOG.md"
PEER_BIN="$POC_DIR/harness/proposal-peer/target/release/proposal-peer"
mkdir -p "$EVIDENCE_DIR" "$EVIDENCE_DIR/wal-final" "$EVIDENCE_DIR/node-logs"

log() { echo "[$(date -u +%H:%M:%S)] $*" | tee -a "$RUNLOG"; }
phase() { echo "$(date +%s) $1" >> "$EVIDENCE_DIR/phases.tsv"; log "PHASE: $1"; }
wal_size() { stat -c%s "$NET/node$1/malachite/wal/consensus.wal" 2>/dev/null || echo 0; }

: > "$RUNLOG"
: > "$EVIDENCE_DIR/phases.tsv"
log "PoC far-future-proposals run starting. POC_DIR=$POC_DIR V1=$V1_COUNT V2=$V2_COUNT RATE=$RATE/s"
log "Runner resources:"; { df -h /; free -m; } >> "$RUNLOG" 2>&1

pid_of() { cat "$NET/node$1/$2.pid" 2>/dev/null; }
eth_block_number() {
  curl -s --max-time 10 -X POST -H 'Content-Type: application/json' \
    --data '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' \
    "http://127.0.0.1:$1" 2>/dev/null | python3 -c \
    'import sys,json
try:
    print(int(json.load(sys.stdin)["result"], 16))
except Exception:
    print("")'
}
current_height() { # $1 = node index -> latest height from "Starting new height" in CL log
  local f="$NET/node$1/logs/cl.log"
  [[ -f "$f" ]] || { echo ""; return; }
  grep -aoE 'Starting new height height=[0-9]+' "$f" 2>/dev/null \
    | grep -oE '[0-9]+$' | tail -n 1
}
round_at_height() { # $1 = node index, $2 = height -> max round seen at that height in CL log
  local f="$NET/node$1/logs/cl.log"
  [[ -f "$f" ]] || { echo ""; return; }
  grep -aoE "consensus\\{height=$2 round=[0-9]+\\}" "$f" 2>/dev/null \
    | grep -oE 'round=[0-9]+' | grep -oE '[0-9]+$' | sort -n | tail -n 1
}
peer_count() { # $1 = node index -> distinct peers seen in CL log
  local f="$NET/node$1/logs/cl.log"
  [[ -f "$f" ]] || { echo "?"; return; }
  grep -aoE 'Received identity from [A-Za-z0-9]+' "$f" 2>/dev/null | awk '{print $4}' | sort -u | wc -l
}

cleanup() {
  log "Cleaning up node processes..."
  [[ -n "${SAMPLER_PID:-}" ]] && kill "$SAMPLER_PID" 2>/dev/null || true
  [[ -n "${WAL_SAMPLER_PID:-}" ]] && kill "$WAL_SAMPLER_PID" 2>/dev/null || true
  for i in 0 1 2 3 4; do
    for kind in cl el; do
      local p; p="$(pid_of "$i" "$kind")"
      if [[ -n "$p" ]]; then kill -CONT "$p" 2>/dev/null || true; kill "$p" 2>/dev/null || true; fi
    done
  done
  sleep 2
  pkill -f 'arc-node-consensus start' 2>/dev/null || true
  pkill -f 'arc-node-execution node' 2>/dev/null || true
  pkill -f 'proposal-peer --target' 2>/dev/null || true
}
trap cleanup EXIT

# ---------- preconditions ----------
for b in arc-node-execution arc-node-consensus; do
  [[ -x "$POC_DIR/arc-node/target/debug/$b" ]] || { log "FATAL: binary missing: $b"; exit 2; }
done
[[ -x "$PEER_BIN" ]] || { log "FATAL: proposal-peer binary missing: $PEER_BIN"; exit 2; }
for i in 0 1 2 3 4; do
  [[ -f "$NET/node$i/malachite/config/priv_validator_key.json" ]] || { log "FATAL: key missing for node$i"; exit 2; }
done
[[ -f "$NET/shared/peer_ids.txt" ]] || { log "FATAL: peer_ids.txt missing"; exit 2; }
NODE0_PEER_ID="$(sed -n '1p' "$NET/shared/peer_ids.txt")"
[[ -n "$NODE0_PEER_ID" ]] || { log "FATAL: peer_ids.txt empty"; exit 2; }
log "Preconditions OK. node0 peer id: $NODE0_PEER_ID"

# ---------- 1. start ELs ----------
for i in 0 1 2 3 4; do
  nohup "$NET/node$i/run-el.sh" > /dev/null 2>&1 &
  echo $! > "$NET/node$i/el.pid"
done
log "ELs started. Waiting for eth RPC on 8545..8549..."
for i in 0 1 2 3 4; do
  for _ in $(seq 1 24); do
    if ! kill -0 "$(pid_of "$i" el)" 2>/dev/null; then
      log "FATAL: node$i EL died. Last EL log lines:"; tail -n 25 "$NET/node$i/logs/el.log" >> "$RUNLOG" 2>&1 || true
      exit 3
    fi
    [[ -n "$(eth_block_number $((8545 + i)))" ]] && break
    sleep 5
  done
  log "  node$i EL blockNumber: $(eth_block_number $((8545 + i)))"
done

# ---------- 2. start CLs (node0 WITHOUT --p2p.persistent-peers-only) ----------
# The crafted peer is a non-persistent inbound peer; the flag would make
# node0 reject it ~1ms after identify (flood run 5). Nodes 1-4 keep the
# flag: they only ever peer with each other + node0.
# RUST_LOG adds WAL debug lines (malachitebft_engine::wal) and the proposal
# handler debug line on top of the arc/circle debug defaults; the global
# default stays INFO so "Received proposal" (info) is visible.
export RUST_LOG="info,arc=debug,circle=debug,libp2p_swarm=debug,malachitebft_engine::wal=debug,malachitebft_core_consensus::handle::proposal=debug"
sed '/--p2p.persistent-peers-only/d' "$NET/node0/run-cl.sh" > "$NET/node0/run-cl-ff.sh"
chmod +x "$NET/node0/run-cl-ff.sh"
nohup "$NET/node0/run-cl-ff.sh" > /dev/null 2>&1 &
echo $! > "$NET/node0/cl.pid"
for i in 1 2 3 4; do
  nohup "$NET/node$i/run-cl.sh" > /dev/null 2>&1 &
  echo $! > "$NET/node$i/cl.pid"
done
log "CLs started. Waiting 60s for peering, then for block production..."
sleep 60
for i in 0 1 2 3 4; do
  if ! kill -0 "$(pid_of "$i" cl)" 2>/dev/null; then
    log "FATAL: node$i CL died. Last CL log lines:"; tail -n 25 "$NET/node$i/logs/cl.log" >> "$RUNLOG" 2>&1 || true
    exit 3
  fi
done
log "Peer report after 60s (expect 4 each):"
for i in 0 1 2 3 4; do log "  node$i distinct peers seen: $(peer_count "$i")"; done
H_BEFORE=""
for _ in $(seq 1 60); do
  ok=1
  for i in 0 1 2 3 4; do
    h="$(eth_block_number $((8545 + i)))"
    [[ -n "$h" && "$h" -gt 1 ]] || ok=0
  done
  [[ "$ok" == 1 ]] && break
  sleep 5
done
for i in 0 1 2 3 4; do log "  node$i height: $(eth_block_number $((8545 + i)))"; done
H_BEFORE="$(eth_block_number 8545)"
if [[ -z "$H_BEFORE" || "$H_BEFORE" -le 1 ]]; then
  log "FATAL: net did not start producing blocks."
  exit 3
fi
curl -s --max-time 10 "http://127.0.0.1:29000/metrics" > "$EVIDENCE_DIR/metrics-healthy.prom" 2>/dev/null || true
log "Net healthy at EL height $H_BEFORE. Metrics snapshot saved."

# ---------- 3. samplers (RSS 1s, WAL 2s) ----------
CL_PID="$(pid_of 0 cl)"
( while true; do
    rss="$(ps -o rss= -p "$CL_PID" 2>/dev/null | tr -d ' ')"
    echo "$(date +%s) ${rss:-0}" >> "$EVIDENCE_DIR/rss.csv"
    sleep 1
  done ) &
SAMPLER_PID=$!
( while true; do
    echo "$(date +%s) $(wal_size 0) $(wal_size 1) $(wal_size 2) $(wal_size 3) $(wal_size 4)" >> "$EVIDENCE_DIR/wal.csv"
    sleep 2
  done ) &
WAL_SAMPLER_PID=$!
log "Samplers started (RSS PID $SAMPLER_PID, WAL PID $WAL_SAMPLER_PID)."

# ---------- 4. baseline (healthy) ----------
phase "baseline"
sleep "$BASELINE_SECS"
log "Baseline ${BASELINE_SECS}s done. node0 WAL: $(wal_size 0) bytes."

# ---------- 5. stall: SIGSTOP all 5 ELs; wait for rounds to advance ----------
for i in 0 1 2 3 4; do kill -STOP "$(pid_of "$i" el)" 2>/dev/null || true; done
log "SIGSTOP all 5 ELs at $(date -u +%H:%M:%S). No block can be built; rounds should skip via polka-nil."
# NOTE (run 1 lesson): do NOT pin the stalled height from a single read at
# SIGSTOP time. The height in flight at that moment (239) is decided once
# more (240) before the freeze takes effect, and rounds then skip at 240
# while 239 stays at round 0 forever. So re-read the LATEST height every
# poll and accept only when the latest height itself has reached the target
# round — that can only happen once a height has genuinely stalled.
H_STALL=""
R_STALL=""
ITER=0
for _ in $(seq 1 $((STALL_WAIT_SECS / 5))); do
  ITER=$((ITER + 1))
  H_NOW="$(current_height 0)"
  if [[ -n "$H_NOW" ]]; then
    R_NOW="$(round_at_height 0 "$H_NOW")"
    if [[ $((ITER % 12)) == 0 ]]; then log "  stall wait: latest height=${H_NOW} round=${R_NOW:-?} (target round $STALL_TARGET_ROUND)"; fi
    if [[ -n "$R_NOW" && "$R_NOW" -ge "$STALL_TARGET_ROUND" ]]; then
      H_STALL="$H_NOW"; R_STALL="$R_NOW"; break
    fi
  fi
  sleep 5
done
if [[ -z "$H_STALL" || -z "$R_STALL" || "$R_STALL" -lt "$STALL_TARGET_ROUND" ]]; then
  log "FATAL: stall failed — height=${H_STALL:-?} round=${R_STALL:-?} after ${STALL_WAIT_SECS}s (target round $STALL_TARGET_ROUND)."
  log "Last node0 cl.log lines:"; tail -n 25 "$NET/node0/logs/cl.log" >> "$RUNLOG" 2>&1 || true
  exit 3
fi
log "Stalled at height $H_STALL round $R_STALL. EL height still: $(eth_block_number 8545 || echo '? (EL stopped, RPC frozen as expected)')."
curl -s --max-time 10 "http://127.0.0.1:29000/metrics" > "$EVIDENCE_DIR/metrics-stalled.prom" 2>/dev/null || true

# ---------- 6. stalled baseline (background WAL growth, no attack) ----------
phase "stalled-baseline"
WAL_SB_BEFORE="$(wal_size 0)"
sleep "$STALLED_BASELINE_SECS"
WAL_SB_AFTER="$(wal_size 0)"
log "Stalled baseline ${STALLED_BASELINE_SECS}s done. node0 WAL: $WAL_SB_BEFORE -> $WAL_SB_AFTER bytes (delta $((WAL_SB_AFTER - WAL_SB_BEFORE)))."

# ---------- 7. launch proposal-peer ----------
SEEDS_HEX="$(python3 - "$NET" <<'PYEOF'
import sys, json, base64
net = sys.argv[1]
seeds = []
for i in range(5):
    d = json.load(open(f"{net}/node{i}/malachite/config/priv_validator_key.json"))
    seeds.append(base64.b64decode(d["value"]).hex())
print(",".join(seeds))
PYEOF
)"
[[ -n "$SEEDS_HEX" ]] || { log "FATAL: could not derive validator seeds"; exit 2; }
TARGET="/ip4/127.0.0.1/tcp/27000/p2p/$NODE0_PEER_ID"
phase "v1"
log "Starting proposal-peer -> $TARGET (height=$H_STALL start-round=$R_STALL v1=$V1_COUNT v2=$V2_COUNT rate=$RATE/s validator-index=$VALIDATOR_INDEX)"
RUST_LOG="debug" "$PEER_BIN" --target "$TARGET" --validator-seeds-hex "$SEEDS_HEX" \
  --validator-index "$VALIDATOR_INDEX" --height "$H_STALL" --start-round "$R_STALL" \
  --v1-count "$V1_COUNT" --v2-count "$V2_COUNT" --rate "$RATE" \
  --observe-secs "$OBSERVE_SECS" --v1-settle-secs "$V1_SETTLE_SECS" --hold-secs "$HOLD_SECS" \
  > "$EVIDENCE_DIR/proposal-peer.log" 2> "$EVIDENCE_DIR/proposal-peer.stderr" &
PEER_PID=$!
log "proposal-peer PID $PEER_PID"
PEER_OK=""
V2_PHASE_LOGGED=0
for _ in $(seq 1 600); do
  if ! kill -0 "$PEER_PID" 2>/dev/null; then wait "$PEER_PID"; PEER_OK=$?; break; fi
  if [[ "$V2_PHASE_LOGGED" == 0 ]] && grep -q '^V1_DONE' "$EVIDENCE_DIR/proposal-peer.log" 2>/dev/null; then
    phase "v1-settle-v2"
    V2_PHASE_LOGGED=1
  fi
  sleep 1
done
if kill -0 "$PEER_PID" 2>/dev/null; then
  log "FATAL: proposal-peer still running after 600s; killing."
  kill "$PEER_PID" 2>/dev/null || true
  PEER_OK=124
fi
log "proposal-peer exited with code ${PEER_OK:-?}."
grep -E "^(CONNECTED|READY|JOINED|OBSERVED|TARGET|V1_DONE|V2_DONE|DONE|FATAL)" "$EVIDENCE_DIR/proposal-peer.log" | tee -a "$RUNLOG" || true
if [[ "$PEER_OK" != 0 ]]; then
  log "FATAL: proposal-peer failed. stderr tail:"
  tail -n 25 "$EVIDENCE_DIR/proposal-peer.stderr" >> "$RUNLOG" 2>&1 || true
  # fall through to partial collection, then exit 4
  PEER_FAILED=1
else
  PEER_FAILED=0
fi

# ---------- 8. mid-check: were proposals accepted? ----------
RECEIVED_N0="$(grep -c 'Received proposal' "$NET/node0/logs/cl.log" 2>/dev/null || true)"; RECEIVED_N0="${RECEIVED_N0:-0}"
log "Node0 'Received proposal' log lines so far: $RECEIVED_N0"
for i in 1 2 3 4; do
  log "  node$i 'Received proposal' lines: $(grep -c 'Received proposal' "$NET/node$i/logs/cl.log" 2>/dev/null || echo 0)"
done
if [[ "$PEER_FAILED" == 0 && "$RECEIVED_N0" == 0 ]]; then
  log "FATAL: peer exited 0 but node0 logged ZERO accepted proposals — signing/format/proposer wrong. See diagnosis in VERDICT."
  PEER_FAILED=1
fi

# ---------- 9. observe ----------
phase "observe"
sleep "$OBSERVE_AFTER_SECS"
log "Observe ${OBSERVE_AFTER_SECS}s done."
kill "$SAMPLER_PID" 2>/dev/null || true
kill "$WAL_SAMPLER_PID" 2>/dev/null || true
sleep 1

# ---------- 10. collect evidence ----------
phase "collect"
curl -s --max-time 10 "http://127.0.0.1:29000/metrics" > "$EVIDENCE_DIR/metrics-after.prom" 2>/dev/null || true
cp "$NET/node0/logs/cl.log" "$EVIDENCE_DIR/cl.log" 2>/dev/null || true
cp "$NET/node0/logs/el.log" "$EVIDENCE_DIR/el.log" 2>/dev/null || true
for i in 1 2 3 4; do cp "$NET/node$i/logs/cl.log" "$EVIDENCE_DIR/node-logs/node$i-cl.log" 2>/dev/null || true; done
# Stop CLs first, then copy WALs + store.db while stable (run-poc.sh pattern).
for i in 0 1 2 3 4; do
  p="$(pid_of "$i" cl)"; [[ -n "$p" ]] && kill "$p" 2>/dev/null || true
done
sleep 3
for i in 0 1 2 3 4; do
  cp "$NET/node$i/malachite/wal/consensus.wal" "$EVIDENCE_DIR/wal-final/node$i-consensus.wal" 2>/dev/null || log "WARN: WAL copy failed for node$i"
done
cp "$NET/node0/malachite/store.db" "$EVIDENCE_DIR/wal-final/node0-store.db" 2>/dev/null || log "WARN: store.db copy failed"
for i in 0 1 2 3 4; do
  log "  final WAL node$i: $(stat -c%s "$EVIDENCE_DIR/wal-final/node$i-consensus.wal" 2>/dev/null || echo ?) bytes"
done

# ---------- 11. stats + verdict (python3, stdlib only) ----------
python3 - "$EVIDENCE_DIR" "$H_STALL" "$V1_COUNT" "$V2_COUNT" "$WAL_SB_BEFORE" "$WAL_SB_AFTER" "$STALLED_BASELINE_SECS" <<'PYEOF' > "$EVIDENCE_DIR/stats.txt" 2>&1 || echo "stats failed" >> "$EVIDENCE_DIR/stats.txt"
import sys, statistics, re, os
d, h_stall, v1, v2 = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4])
sb_before, sb_after, sb_secs = int(sys.argv[5]), int(sys.argv[6]), int(sys.argv[7])

def read_csv(path, cols):
    rows = []
    try:
        for line in open(path):
            p = line.split()
            if len(p) >= cols:
                rows.append([int(x) for x in p[:cols]])
    except FileNotFoundError:
        pass
    return rows

phases = []
for line in open(f"{d}/phases.tsv"):
    p = line.split()
    if len(p) == 2:
        phases.append((int(p[0]), p[1]))
phases.sort()
bounds = [(phases[i][1], phases[i][0], phases[i+1][0] if i+1 < len(phases) else 10**18)
          for i in range(len(phases))]

# RSS per phase (node0 CL)
rss = read_csv(f"{d}/rss.csv", 2)
print("== RSS node0 CL (KiB) ==")
print("phase,n_samples,mean,median,min,max")
for name, t0, t1 in bounds:
    vals = [r for (t, r) in rss if t0 <= t < t1 and r > 0]
    if not vals:
        print(f"{name},0,-,-,-,-"); continue
    print(f"{name},{len(vals)},{statistics.mean(vals):.0f},{statistics.median(vals):.0f},{min(vals)},{max(vals)}")

# WAL per phase (all 5 nodes, 2s samples): first/last sample in phase.
wal = read_csv(f"{d}/wal.csv", 6)
print("== WAL size (bytes) per phase: first -> last sample, delta ==")
print("phase,node0_first,node0_last,node0_delta,node1_delta,node2_delta,node3_delta,node4_delta")
for name, t0, t1 in bounds:
    seg = [row for row in wal if t0 <= row[0] < t1]
    if len(seg) < 2:
        print(f"{name},-,-,-,-,-,-,-"); continue
    f, l = seg[0], seg[-1]
    print(f"{name},{f[1]},{l[1]},{l[1]-f[1]},{l[2]-f[2]},{l[3]-f[3]},{l[4]-f[4]},{l[5]-f[5]}")

# Background rate from the explicit stalled-baseline snapshot.
bg_rate = (sb_after - sb_before) / sb_secs if sb_secs else 0
print(f"== stalled-baseline background: WAL node0 {sb_before} -> {sb_after} in {sb_secs}s = {bg_rate:.1f} B/s ==")

# cl.log analysis (node0).
cl = open(f"{d}/cl.log", errors="ignore").read() if os.path.exists(f"{d}/cl.log") else ""
received = [ln for ln in cl.splitlines() if "Received proposal" in ln
            and "proposal.round" in ln and "dropping" not in ln and "queuing" not in ln]
gaps = []
for ln in received:
    m1 = re.search(r"consensus\.round=(\d+)", ln)
    m2 = re.search(r"proposal\.round=(\d+)", ln)
    m3 = re.search(r"proposal\.height=(\d+)", ln)
    if m1 and m2:
        gaps.append((int(m2.group(1)) - int(m1.group(1)), int(m2.group(1)),
                     int(m3.group(1)) if m3 else -1))
print(f"== node0 cl.log: 'Received proposal' (post-verify) lines: {len(received)} (expect ~{v1+v2}) ==")
if gaps:
    gs = sorted(g[0] for g in gaps)
    print(f"proposal.round - consensus.round: min={gs[0]} median={statistics.median(gs):.0f} max={gs[-1]}; "
          f"gap>=11: {sum(1 for g in gs if g >= 11)}/{len(gs)}; gap<11: {sum(1 for g in gs if g < 11)}")
    print(f"proposal rounds seen: min={min(g[1] for g in gaps)} max={max(g[1] for g in gaps)}; "
          f"heights: {sorted(set(g[2] for g in gaps))}")
cap = cl.count("Rejecting proposal: per-(height, round) cap reached")
lower = cl.count("Received proposal for lower height")
higher = cl.count("Received proposal for higher height")
print(f"cap-reject lines: {cap} (expect 0); lower-height drops: {lower}; higher-height queued: {higher}")
prop_entries = [ln for ln in cl.splitlines() if "Consensus(Proposal)" in ln and "Wrote log entry" in ln]
sizes = []
for ln in prop_entries:
    m = re.search(r"entry\.size\s*=\s*(\d+)", ln)
    if m:
        sizes.append(int(m.group(1)))
print(f"WAL 'Wrote log entry' Consensus(Proposal) lines: {len(prop_entries)}; "
      f"entry.size total={sum(sizes)} bytes over {len(sizes)} entries "
      f"(mean {statistics.mean(sizes):.1f} B)" if sizes else "(no entry.size parsed)")
flush = re.findall(r"Flushed WAL to disk.*?wal\.entries\s*=\s*(\d+).*?wal\.size\s*=\s*(\d+)", cl)
if flush:
    print(f"last WAL flush: entries={flush[-1][0]} size={flush[-1][1]} bytes; flush lines: {len(flush)}")
for i in range(1, 5):
    p = f"{d}/node-logs/node{i}-cl.log"
    if os.path.exists(p):
        c = open(p, errors="ignore").read()
        print(f"node{i} 'Received proposal' lines: {c.count('Received proposal')}; "
              f"cap-reject: {c.count('Rejecting proposal: per-(height, round) cap reached')}")
PYEOF
cat "$EVIDENCE_DIR/stats.txt" | tee -a "$RUNLOG"

# ---------- 12. verdict (observables only) ----------
V1_SENT="$(grep -E '^V1_DONE' "$EVIDENCE_DIR/proposal-peer.log" | grep -oE 'sent=[0-9]+' | grep -oE '[0-9]+' | tail -n 1)"
V2_SENT="$(grep -E '^V2_DONE' "$EVIDENCE_DIR/proposal-peer.log" | grep -oE 'sent=[0-9]+' | grep -oE '[0-9]+' | tail -n 1)"
{
  echo "# far-future-proposals preliminary verdict (observables — human confirms TERBUKTI/GAGAL)"
  echo ""
  echo "- Target: node0 CL (malachite v0.8.0, arc-node @ 6e76402), 5-node localnet, ELs SIGSTOPped, localnet only"
  echo "- Threat model: ONE Byzantine validator (address-sorted index $VALIDATOR_INDEX); peer signs with that validator's deterministic test key"
  echo "- Stalled height: $H_STALL (round $R_STALL at stall; EL height frozen at $H_BEFORE)"
  echo "- V1 distinct-round proposals SENT by peer (V1_DONE marker): ${V1_SENT:-?} (requested: $V1_COUNT)"
  echo "- V2 duplicate replays SENT by peer (V2_DONE marker): ${V2_SENT:-?} (requested: $V2_COUNT)"
  echo "- Proposals ACCEPTED by node0 (cl.log 'Received proposal', post-verify): $RECEIVED_N0"
  echo "- Stalled-baseline background WAL growth: $WAL_SB_BEFORE -> $WAL_SB_AFTER bytes in ${STALLED_BASELINE_SECS}s"
  echo "- WAL/RSS per phase + log analysis: see stats.txt (wal.csv, rss.csv, phases.tsv)"
  echo ""
  echo "TERBUKTI iff: V1_SENT == $V1_COUNT AND V2_SENT == $V2_COUNT AND"
  echo "RECEIVED ~= V1+V2 AND WAL growth during v1/v2 phases is clearly above the"
  echo "stalled-baseline background rate (bytes/proposal >> background B/s) AND"
  echo "cap-reject lines == 0 AND min(proposal.round - consensus.round) >= 11."
  echo "GAGAL iff: RECEIVED == 0 (signing/format/proposer wrong), or WAL flat during"
  echo "flood phases, or cap-rejects / drops appear."
  echo ""
  echo "Files: cl.log, el.log, proposal-peer.log, proposal-peer.stderr,"
  echo "node-logs/node1-4-cl.log, wal-final/node*-consensus.wal + node0-store.db,"
  echo "rss.csv, wal.csv, phases.tsv, stats.txt,"
  echo "metrics-healthy.prom, metrics-stalled.prom, metrics-after.prom, RUNLOG.md"
} > "$EVIDENCE_DIR/VERDICT.md"
cat "$EVIDENCE_DIR/VERDICT.md" | tee -a "$RUNLOG"
log "PoC sequence complete. Evidence in $EVIDENCE_DIR"
{ df -h /; free -m; } >> "$RUNLOG" 2>&1
if [[ "$PEER_FAILED" == 1 ]]; then
  log "Exiting 4 (peer/acceptance failure — see VERDICT.md + stats.txt)."
  exit 4
fi
exit 0
