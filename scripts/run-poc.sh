#!/usr/bin/env bash
# run-poc.sh — execute the C2-01 sequence on the 4-node localnet and collect evidence.
#
# Vector: WAL replay drops own votes past the 10-round lookahead
# (malachite v0.8.0, core-consensus/src/handle/vote.rs:80-96, no replay
# exemption; fixed in v0.8.1/v0.8.2) -> validator can lose lock/votes and
# equivocate after restart.
#
# Sequence: start 4 nodes -> SIGSTOP 2 CLs (stall, rounds advance on timeouts)
# -> snapshot target WAL/store -> SIGKILL target CL -> restart same --home
# -> SIGCONT stopped CLs -> compare target's pre/post-restart votes at same (H,R).
#
# Env:
#   POC_DIR      — repo root (default: parent of this script's dir)
#   STALL_ROUNDS — rounds past the last voted round before crashing (default 12)
#   EVIDENCE_DIR — where to write evidence (default: $POC_DIR/evidence/c2-01)
#
# Exit code: 0 if the sequence ran to completion (whatever the verdict);
# non-zero only on infrastructure failure (net would not start).
# The verdict itself lives in $EVIDENCE_DIR/VERDICT.md — PROVEN/FAILED is
# decided by a human reading the evidence, this script only reports observables.
set -u
POC_DIR="${POC_DIR:-$(cd "$(dirname "$0")/.." && pwd)}"
STALL_ROUNDS="${STALL_ROUNDS:-12}"
NET="$POC_DIR/net"
EVIDENCE_DIR="${EVIDENCE_DIR:-$POC_DIR/evidence/c2-01}"
RUNLOG="$EVIDENCE_DIR/RUNLOG.md"
mkdir -p "$EVIDENCE_DIR/metrics-before" "$EVIDENCE_DIR/metrics-after" "$EVIDENCE_DIR/pre-crash" "$EVIDENCE_DIR/post-restart"

log() { echo "[$(date -u +%H:%M:%S)] $*" | tee -a "$RUNLOG"; }
note() { echo "$*" >> "$RUNLOG"; }

: > "$RUNLOG"
log "PoC C2-01 run starting. POC_DIR=$POC_DIR STALL_ROUNDS=$STALL_ROUNDS"
log "Runner resources:"; { df -h /; free -m; } >> "$RUNLOG" 2>&1

# ---------- helpers ----------
eth_block_number() { # $1 = http port -> decimal height, or empty on failure
  curl -s --max-time 10 -X POST -H 'Content-Type: application/json' \
    --data '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' \
    "http://127.0.0.1:$1" 2>/dev/null | python3 -c \
    'import sys,json
try:
    print(int(json.load(sys.stdin)["result"], 16))
except Exception:
    print("")'
}
dropped_votes() { # $1 = node index -> counter value or empty
  curl -s --max-time 10 "http://127.0.0.1:$((29000 + $1))/metrics" 2>/dev/null \
    | grep -E '^malachite_consensus_dropped_future_round_votes' | awk '{print $2}' | head -n 1
}
max_round_in_log() { # $1 = node index -> max round number seen in CL log (tolerant grep)
  local f="$NET/node$1/logs/cl.log"
  [[ -f "$f" ]] || { echo ""; return; }
  grep -aoE '(consensus\.round|round)[=: "]+[0-9]+' "$f" 2>/dev/null \
    | grep -oE '[0-9]+$' | sort -n | tail -n 1
}
pid_of() { cat "$NET/node$1/$2.pid" 2>/dev/null; }

cleanup() {
  log "Cleaning up node processes..."
  for i in 0 1 2 3; do
    for kind in cl el; do
      local p; p="$(pid_of "$i" "$kind")"
      [[ -n "$p" ]] && kill "$p" 2>/dev/null || true
    done
  done
  sleep 2
  pkill -f 'arc-node-consensus start' 2>/dev/null || true
  pkill -f 'arc-node-execution node' 2>/dev/null || true
}
trap cleanup EXIT

# ---------- preconditions ----------
for b in arc-node-execution arc-node-consensus; do
  [[ -x "$POC_DIR/arc-node/target/debug/$b" ]] || { log "FATAL: binary missing: $b"; exit 2; }
done
for i in 0 1 2 3; do
  [[ -f "$NET/node$i/malachite/config/priv_validator_key.json" ]] || { log "FATAL: key missing for node$i"; exit 2; }
done
log "Preconditions OK (binaries + 4 validator keys)."

# ---------- 1. start ELs ----------
for i in 0 1 2 3; do
  nohup "$NET/node$i/run-el.sh" > /dev/null 2>&1 &
  echo $! > "$NET/node$i/el.pid"
done
log "ELs started. Waiting for eth RPC on 8545..8548..."
for i in 0 1 2 3; do
  for _ in $(seq 1 24); do
    # fail fast if the EL process died (e.g. port collision) instead of waiting out the loop
    if ! kill -0 "$(pid_of "$i" el)" 2>/dev/null; then
      log "FATAL: node$i EL process died. Last EL log lines:"
      tail -n 25 "$NET/node$i/logs/el.log" >> "$RUNLOG" 2>&1 || true
      exit 3
    fi
    [[ -n "$(eth_block_number $((8545 + i)))" ]] && break
    sleep 5
  done
  log "  node$i EL blockNumber: $(eth_block_number $((8545 + i)))"
done

# ---------- 2. start CLs ----------
for i in 0 1 2 3; do
  nohup "$NET/node$i/run-cl.sh" > /dev/null 2>&1 &
  echo $! > "$NET/node$i/cl.pid"
done
log "CLs started. Waiting for block production (height rising on all ELs)..."
H_BEFORE=""
for _ in $(seq 1 60); do
  ok=1
  for i in 0 1 2 3; do
    if ! kill -0 "$(pid_of "$i" cl)" 2>/dev/null; then
      log "FATAL: node$i CL process died. Last CL log lines:"
      tail -n 25 "$NET/node$i/logs/cl.log" >> "$RUNLOG" 2>&1 || true
      exit 3
    fi
    h="$(eth_block_number $((8545 + i)))"
    [[ -n "$h" && "$h" -gt 1 ]] || ok=0
  done
  [[ "$ok" == 1 ]] && break
  sleep 5
done
for i in 0 1 2 3; do log "  node$i height: $(eth_block_number $((8545 + i)))"; done
H_STALL="$(eth_block_number 8545)"
if [[ -z "$H_STALL" || "$H_STALL" -le 1 ]]; then
  log "FATAL: net did not start producing blocks. EL/CL logs:"
  tail -n 30 "$NET/node0/logs/el.log" >> "$RUNLOG" 2>&1 || true
  tail -n 30 "$NET/node0/logs/cl.log" >> "$RUNLOG" 2>&1 || true
  exit 3
fi
log "Net is producing blocks. Stall height H=$H_STALL."

# ---------- 3. baseline ----------
for i in 0 1 2 3; do
  curl -s --max-time 10 "http://127.0.0.1:$((29000 + i))/metrics" > "$EVIDENCE_DIR/metrics-before/node$i.prom" 2>/dev/null || true
  log "  node$i dropped_future_round_votes (before): $(dropped_votes "$i")"
done
R_FIRST="$(max_round_in_log 0)"
log "node0 max round in log at baseline: ${R_FIRST:-unknown}"
grep -i "vote" "$NET/node0/logs/cl.log" 2>/dev/null | tail -n 50 > "$EVIDENCE_DIR/pre-crash/votes-baseline.txt" || true

# ---------- 4. stall: SIGSTOP node2 + node3 CLs ----------
kill -STOP "$(pid_of 2 cl)" "$(pid_of 3 cl)"
log "SIGSTOP node2+node3 CLs at $(date -u +%H:%M:%S). Remaining power 40/100 < 2/3 quorum; rounds advance on timeouts."
log "Waiting for node0 round to advance >= $STALL_ROUNDS past baseline round ${R_FIRST:-?} (max 600s)..."
R_TARGET=""
if [[ -n "$R_FIRST" ]]; then R_TARGET=$((R_FIRST + STALL_ROUNDS)); fi
STALL_OK=0
for _ in $(seq 1 120); do
  R_NOW="$(max_round_in_log 0)"
  if [[ -n "$R_NOW" && -n "$R_TARGET" && "$R_NOW" -ge "$R_TARGET" ]]; then STALL_OK=1; break; fi
  sleep 5
done
R_STALL="$(max_round_in_log 0)"
log "Stall phase done. node0 max round now: ${R_STALL:-unknown} (target >= ${R_TARGET:-?}, reached=$STALL_OK)."
log "Height during stall (should be stuck at H=$H_STALL): $(eth_block_number 8545)"

# ---------- 5. pre-crash snapshot (node0 = target) ----------
cp "$NET/node0/malachite/wal/consensus.wal" "$EVIDENCE_DIR/pre-crash/" 2>/dev/null || log "WARN: WAL copy failed"
cp "$NET/node0/malachite/store.db" "$EVIDENCE_DIR/pre-crash/" 2>/dev/null || log "WARN: store.db copy failed"
cp "$NET/node0/logs/cl.log" "$EVIDENCE_DIR/pre-crash/cl-before.log" 2>/dev/null || true
grep -i "vote" "$EVIDENCE_DIR/pre-crash/cl-before.log" 2>/dev/null | tail -n 100 > "$EVIDENCE_DIR/pre-crash/votes-before.txt" || true
DROPPED_BEFORE="$(dropped_votes 0)"
log "node0 dropped_future_round_votes before crash: ${DROPPED_BEFORE:-unknown}"
log "Pre-crash snapshot saved (WAL bytes: $(stat -c%s "$EVIDENCE_DIR/pre-crash/consensus.wal" 2>/dev/null || echo ?))."

# ---------- 6. crash + restart node0 CL ----------
kill -KILL "$(pid_of 0 cl)"
sleep 2
nohup "$NET/node0/run-cl.sh" > /dev/null 2>&1 &
echo $! > "$NET/node0/cl.pid"
log "SIGKILL node0 CL, restarted with same --home (new PID $(pid_of 0 cl)). WAL replay delay is 5s."

# ---------- 7. resume the mesh ----------
kill -CONT "$(pid_of 2 cl)" "$(pid_of 3 cl)"
log "SIGCONT node2+node3 CLs at $(date -u +%H:%M:%S). Waiting for height to advance past H=$H_STALL (max 600s)..."
RECOVERED=0
for _ in $(seq 1 120); do
  h="$(eth_block_number 8545)"
  if [[ -n "$h" && "$h" -gt "$H_STALL" ]]; then RECOVERED=1; break; fi
  sleep 5
done
log "Recovery: height now $(eth_block_number 8545) (recovered=$RECOVERED)."
sleep 30  # let votes propagate after resume

# ---------- 8. post-restart evidence ----------
for i in 0 1 2 3; do
  curl -s --max-time 10 "http://127.0.0.1:$((29000 + i))/metrics" > "$EVIDENCE_DIR/metrics-after/node$i.prom" 2>/dev/null || true
done
cp "$NET/node0/logs/cl.log" "$EVIDENCE_DIR/post-restart/cl-after.log" 2>/dev/null || true
cp "$NET/node1/logs/cl.log" "$EVIDENCE_DIR/post-restart/node1-cl-after.log" 2>/dev/null || true
grep -i "vote" "$EVIDENCE_DIR/post-restart/cl-after.log" 2>/dev/null | tail -n 100 > "$EVIDENCE_DIR/post-restart/votes-after.txt" || true
grep -i "vote" "$EVIDENCE_DIR/post-restart/node1-cl-after.log" 2>/dev/null | tail -n 100 > "$EVIDENCE_DIR/post-restart/node1-votes-after.txt" || true
grep -i "beyond the future-round lookahead" "$EVIDENCE_DIR/post-restart/cl-after.log" 2>/dev/null | tail -n 30 > "$EVIDENCE_DIR/post-restart/lookahead-drops.txt" || true
DROPPED_AFTER="$(dropped_votes 0)"
log "node0 dropped_future_round_votes after restart: ${DROPPED_AFTER:-unknown} (before: ${DROPPED_BEFORE:-unknown})"

# ---------- 9. preliminary verdict (observables only) ----------
{
  echo "# C2-01 preliminary verdict (observables — human confirms PROVEN/FAILED)"
  echo ""
  echo "- Stall height H: $H_STALL"
  echo "- node0 round baseline -> stall: ${R_FIRST:-?} -> ${R_STALL:-?} (stall reached: $STALL_OK)"
  echo "- Height recovered after restart: $RECOVERED (now $(eth_block_number 8545))"
  echo "- node0 dropped_future_round_votes: before=${DROPPED_BEFORE:-?} after=${DROPPED_AFTER:-?}"
  if [[ -n "$DROPPED_BEFORE" && -n "$DROPPED_AFTER" && "$DROPPED_AFTER" -gt "$DROPPED_BEFORE" ]]; then
    echo "- REPLAY DROP OBSERVED: counter rose by $((DROPPED_AFTER - DROPPED_BEFORE)) across the restart."
  else
    echo "- No replay-drop counter movement observed."
  fi
  echo "- Lookahead-drop log lines during/after replay: $(wc -l < "$EVIDENCE_DIR/post-restart/lookahead-drops.txt" 2>/dev/null || echo 0) (see post-restart/lookahead-drops.txt)"
  echo "- Vote excerpts: pre-crash/votes-before.txt, post-restart/votes-after.txt, post-restart/node1-votes-after.txt"
  echo "- Conflicting-vote check (same (H,R), different value): MANUAL — compare the excerpts above."
  echo ""
  echo "Files: pre-crash/consensus.wal, pre-crash/store.db, pre-crash/cl-before.log,"
  echo "post-restart/cl-after.log, metrics-before/, metrics-after/, RUNLOG.md"
} > "$EVIDENCE_DIR/VERDICT.md"
cat "$EVIDENCE_DIR/VERDICT.md" | tee -a "$RUNLOG"
log "PoC sequence complete. Evidence in $EVIDENCE_DIR"
{ df -h /; free -m; } >> "$RUNLOG" 2>&1
exit 0
