#!/usr/bin/env bash
# run-poc.sh — execute the C2-01 sequence on the 5-node localnet and collect evidence.
#
# Vector: WAL replay drops own votes past the 10-round lookahead
# (malachite v0.8.0, core-consensus/src/handle/vote.rs:80-96, no replay
# exemption; fixed in v0.8.1/v0.8.2) -> validator can lose lock/votes and
# equivocate after restart.
#
# Sequence (run-5 design; runs 3-4 proved the naive versions cannot work):
# - Run 3: stopping CLs drops online power below 67/100, so no polka ever
#   forms and the Prevote timeout is never armed (scheduled only on PolkaAny,
#   state_machine.rs L34/L35) — the net halts at round 0 by design.
# - Run 4: stopping all ELs DOES make rounds skip via polka-nil (H51 rounds
#   0->21), but the WAL holds the Timeout entries for every skipped round, so
#   the replay walks rounds in lockstep and drops nothing (counter stays 0).
#   The drop needs the target's WAL to LACK the intervening timeout entries —
#   i.e. the target must JUMP to the high round via a round certificate, which
#   is NOT persisted to the WAL (engine/src/wal/entry.rs: RoundCertificate is
#   rejected as "not persisted"), while the votes it then casts ARE persisted.
# - So: freeze node0's CL early in height H, let the other 4 CLs (80/100)
#   skip 15 rounds at H with the ELs stopped, resume node0 so it jumps via
#   certificate and votes NIL at the high round R, freeze the network's round
#   at R (stop 2 more CLs -> 60/100 < 67), crash node0, restart it (replay
#   drops the R vote: R > replay-round + 10), resume the ELs so a proposal
#   appears at R and node0 prevotes the VALUE at R -> conflicting pair.
# This needs all 5 genesis validators running: with only 4/5, freezing node0
# leaves 60/100 < 67 and the skipping phase cannot form polkas.
#
# Env:
#   POC_DIR      — repo root (default: parent of this script's dir)
#   STALL_ROUNDS — rounds past the freeze round before resuming node0 (default 15)
#   EVIDENCE_DIR — where to write evidence (default: $POC_DIR/evidence/c2-01)
#
# Exit code: 0 if the sequence ran to completion (whatever the verdict);
# non-zero only on infrastructure failure (net would not start).
# The verdict itself lives in $EVIDENCE_DIR/VERDICT.md — PROVEN/FAILED is
# decided by a human reading the evidence, this script only reports observables.
set -u
POC_DIR="${POC_DIR:-$(cd "$(dirname "$0")/.." && pwd)}"
STALL_ROUNDS="${STALL_ROUNDS:-15}"
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
    | grep -E '^malachitebft_core_consensus_dropped_future_round_votes_total' | awk '{print $2}' | head -n 1
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
pid_of() { cat "$NET/node$1/$2.pid" 2>/dev/null; }
peer_count() { # $1 = node index -> distinct peers seen in CL log
  local f="$NET/node$1/logs/cl.log"
  [[ -f "$f" ]] || { echo "?"; return; }
  grep -aoE 'Received identity from [A-Za-z0-9]+' "$f" 2>/dev/null | awk '{print $4}' | sort -u | wc -l
}
peer_report() {
  for i in 0 1 2 3 4; do
    log "  node$i distinct peers seen: $(peer_count "$i") (ERROR lines: $(grep -c -i ' error ' "$NET/node$i/logs/cl.log" 2>/dev/null || echo 0), WARN: $(grep -c -i ' warn ' "$NET/node$i/logs/cl.log" 2>/dev/null || echo 0))"
  done
}
# latest nil-prevote round of node0 in its CL log (live votes only; empty if none)
node0_nil_prevote_round() {
  grep -aoE 'Voting vote_type=Prevote value=Nil round=[0-9]+' "$NET/node0/logs/cl.log" 2>/dev/null \
    | grep -oE '[0-9]+$' | tail -n 1
}

cleanup() {
  log "Cleaning up node processes..."
  for i in 0 1 2 3 4; do
    for kind in cl el; do
      local p; p="$(pid_of "$i" "$kind")"
      if [[ -n "$p" ]]; then kill -CONT "$p" 2>/dev/null || true; kill "$p" 2>/dev/null || true; fi
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
for i in 0 1 2 3 4; do
  [[ -f "$NET/node$i/malachite/config/priv_validator_key.json" ]] || { log "FATAL: key missing for node$i"; exit 2; }
done
log "Preconditions OK (binaries + 5 validator keys)."

# ---------- 1. start ELs ----------
for i in 0 1 2 3 4; do
  nohup "$NET/node$i/run-el.sh" > /dev/null 2>&1 &
  echo $! > "$NET/node$i/el.pid"
done
log "ELs started. Waiting for eth RPC on 8545..8549..."
for i in 0 1 2 3 4; do
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
for i in 0 1 2 3 4; do
  nohup "$NET/node$i/run-cl.sh" > /dev/null 2>&1 &
  echo $! > "$NET/node$i/cl.pid"
done
log "CLs started. Waiting 60s for peering, then for block production..."
sleep 60
log "Peer report after 60s (expect 4 each):"
peer_report
log "Waiting for block production (height rising on all ELs)..."
H_BEFORE=""
for _ in $(seq 1 60); do
  ok=1
  for i in 0 1 2 3 4; do
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
for i in 0 1 2 3 4; do log "  node$i height: $(eth_block_number $((8545 + i)))"; done
H_STALL="$(eth_block_number 8545)"
if [[ -z "$H_STALL" || "$H_STALL" -le 1 ]]; then
  log "FATAL: net did not start producing blocks. Per-node diagnostics:"
  peer_report
  for i in 0 1 2 3 4; do
    log "--- node$i cl.log (last 8 lines) ---"
    tail -n 8 "$NET/node$i/logs/cl.log" >> "$RUNLOG" 2>&1 || true
    log "--- node$i el.log (last 8 lines) ---"
    tail -n 8 "$NET/node$i/logs/el.log" >> "$RUNLOG" 2>&1 || true
  done
  exit 3
fi
log "Net is producing blocks. Stall height H=$H_STALL."

# ---------- 3. baseline ----------
for i in 0 1 2 3 4; do
  curl -s --max-time 10 "http://127.0.0.1:$((29000 + i))/metrics" > "$EVIDENCE_DIR/metrics-before/node$i.prom" 2>/dev/null || true
  log "  node$i dropped_future_round_votes (before): $(dropped_votes "$i")"
done
H_CL="$(current_height 0)"
R_FIRST="$(round_at_height 0 "$H_CL")"
log "node0 CL height at baseline: ${H_CL:-unknown}, round at that height: ${R_FIRST:-unknown}"
grep -i "vote" "$NET/node0/logs/cl.log" 2>/dev/null | tail -n 50 > "$EVIDENCE_DIR/pre-crash/votes-baseline.txt" || true

# ---------- 4a. freeze node0 CL early in height H_CL ----------
kill -STOP "$(pid_of 0 cl)"
R_FREEZE="$(round_at_height 0 "$H_CL")"
log "SIGSTOP node0 CL at $(date -u +%H:%M:%S) (frozen at height $H_CL round ${R_FREEZE:-?}). Its WAL at $H_CL holds only low-round entries."

# ---------- 4b. stall: SIGSTOP all 5 ELs; node1-4 skip rounds via polka-nil ----------
for i in 0 1 2 3 4; do kill -STOP "$(pid_of "$i" el)"; done
R_TARGET=""
if [[ -n "$R_FREEZE" ]]; then R_TARGET=$((R_FREEZE + STALL_ROUNDS)); fi
log "SIGSTOP all 5 ELs at $(date -u +%H:%M:%S). node1-4 (80/100) keep voting; no block can be built, so rounds at height $H_CL skip via polka-nil."
log "Waiting for node1 round at height $H_CL to reach >= ${R_TARGET:-?} (max 400s)..."
STALL_OK=0
for n in $(seq 1 80); do
  R_NOW="$(round_at_height 1 "$H_CL")"
  H_NOW="$(current_height 1)"
  if [[ -n "$R_NOW" && -n "$R_TARGET" && "$R_NOW" -ge "$R_TARGET" ]]; then STALL_OK=1; break; fi
  if [[ "$((n % 12))" == 0 ]]; then log "  ... after $((n * 5))s: node1 height=${H_NOW:-?} round at H_CL=${R_NOW:-?} (target >= ${R_TARGET:-?})"; fi
  sleep 5
done
R_STALL="$(round_at_height 1 "$H_CL")"
log "Stall phase done. node1 round at height $H_CL: ${R_STALL:-unknown} (target >= ${R_TARGET:-?}, reached=$STALL_OK)."

# ---------- 4c. resume node0 CL; wait for its nil vote at the high round ----------
kill -CONT "$(pid_of 0 cl)"
log "SIGCONT node0 CL at $(date -u +%H:%M:%S). Waiting for it to jump via round certificate and prevote NIL at a round >= ${R_TARGET:-?} (max 120s, poll 0.5s)..."
R_JUMP=""
for _ in $(seq 1 240); do
  r="$(node0_nil_prevote_round)"
  if [[ -n "$r" && -n "$R_TARGET" && "$r" -ge "$R_TARGET" ]]; then R_JUMP="$r"; break; fi
  sleep 0.5
done
if [[ -z "$R_JUMP" ]]; then
  log "WARN: node0 never prevoted nil at a round >= $R_TARGET after resume; last nil-prevote round: $(node0_nil_prevote_round). Continuing anyway (verdict will show FAILED)."
  R_JUMP="${R_STALL:-0}"
fi
log "node0 prevoted NIL at round $R_JUMP (height $H_CL)."

# ---------- 4d. freeze the network round at R_JUMP + crash node0 (fast!) ----------
# SIGSTOP node3+node4 CLs first: online power drops to 60/100 < 67, no more
# polkas, rounds freeze at R_JUMP. Then SIGKILL node0 before its precommit
# timeout at R_JUMP fires, so its WAL holds the R_JUMP votes but no
# Timeout(Precommit,R_JUMP) entry — the replay cannot walk past R_JUMP.
kill -STOP "$(pid_of 3 cl)" "$(pid_of 4 cl)"
kill -KILL "$(pid_of 0 cl)"
sleep 2
log "SIGSTOP node3+node4 CLs; SIGKILL node0 CL at $(date -u +%H:%M:%S)."

# ---------- 5. pre-crash snapshot (node0 = target) ----------
cp "$NET/node0/malachite/wal/consensus.wal" "$EVIDENCE_DIR/pre-crash/" 2>/dev/null || log "WARN: WAL copy failed"
cp "$NET/node0/malachite/store.db" "$EVIDENCE_DIR/pre-crash/" 2>/dev/null || log "WARN: store.db copy failed"
cp "$NET/node0/logs/cl.log" "$EVIDENCE_DIR/pre-crash/cl-before.log" 2>/dev/null || true
grep -i "vote" "$EVIDENCE_DIR/pre-crash/cl-before.log" 2>/dev/null | tail -n 100 > "$EVIDENCE_DIR/pre-crash/votes-before.txt" || true
grep -E "Prevote\(height: $H_CL, round: $R_JUMP, value: Nil, from: 0x66d496cb" "$EVIDENCE_DIR/pre-crash/cl-before.log" 2>/dev/null | tail -n 5 > "$EVIDENCE_DIR/pre-crash/nil-vote-at-R.txt" || true
DROPPED_BEFORE="$(dropped_votes 0)"
log "node0 dropped_future_round_votes before crash: ${DROPPED_BEFORE:-unknown}"
log "Pre-crash snapshot saved (WAL bytes: $(stat -c%s "$EVIDENCE_DIR/pre-crash/consensus.wal" 2>/dev/null || echo ?); last WAL flush: $(grep -aoE 'Flushed WAL to disk wal.entries=[0-9]+ wal.size=[0-9]+' "$NET/node0/logs/cl.log" 2>/dev/null | tail -n 1))."
log "Pre-crash nil-vote lines at (H=$H_CL, R=$R_JUMP) captured: $(wc -l < "$EVIDENCE_DIR/pre-crash/nil-vote-at-R.txt" 2>/dev/null || echo 0) (see pre-crash/nil-vote-at-R.txt)."

# ---------- 6. restart node0 CL (ELs still stopped, node3/4 still frozen) ----------
RESTART_LINE="$(wc -l < "$NET/node0/logs/cl.log" 2>/dev/null || echo 0)"
nohup "$NET/node0/run-cl.sh" > /dev/null 2>&1 &
echo $! > "$NET/node0/cl.pid"
log "Restarted node0 CL with same --home (new PID $(pid_of 0 cl)). WAL replay delay is 5s; ELs still stopped, node3/4 still frozen."
log "Waiting for replay to finish (max 120s)..."
REPLAYED=0
for _ in $(seq 1 240); do
  if tail -n +"$((RESTART_LINE + 1))" "$NET/node0/logs/cl.log" 2>/dev/null | grep -q "Phase transition prev=Recovering new=Running"; then REPLAYED=1; break; fi
  sleep 0.5
done
log "Replay finished: $REPLAYED."
DROPPED_AFTER_REPLAY="$(dropped_votes 0)"
log "node0 dropped_future_round_votes right after replay: ${DROPPED_AFTER_REPLAY:-unknown} (before crash: ${DROPPED_BEFORE:-unknown})."
tail -n +"$((RESTART_LINE + 1))" "$NET/node0/logs/cl.log" 2>/dev/null | grep -i "beyond the future-round lookahead" | tail -n 30 > "$EVIDENCE_DIR/post-restart/lookahead-drops.txt" || true
log "Lookahead-drop lines in replay: $(wc -l < "$EVIDENCE_DIR/post-restart/lookahead-drops.txt" 2>/dev/null || echo 0)."

# ---------- 7. rejoin at R_JUMP, then resume block production ----------
log "Waiting for node0 to reach round $R_JUMP again post-restart (via round certificate, max 120s, poll 0.5s)..."
REJOINED=0
for _ in $(seq 1 240); do
  if tail -n +"$((RESTART_LINE + 1))" "$NET/node0/logs/cl.log" 2>/dev/null | grep -q "Starting new round height=$H_CL round=$R_JUMP"; then REJOINED=1; break; fi
  sleep 0.5
done
log "node0 rejoined round $R_JUMP post-restart: $REJOINED."
for i in 0 1 2 3 4; do kill -CONT "$(pid_of "$i" el)"; done
log "SIGCONT all 5 ELs at $(date -u +%H:%M:%S). A proposal should now appear at round $R_JUMP; waiting for node0 to prevote a VALUE there (max 120s, poll 0.5s)..."
VALUE_VOTE=0
for _ in $(seq 1 240); do
  if tail -n +"$((RESTART_LINE + 1))" "$NET/node0/logs/cl.log" 2>/dev/null | grep -qE "Voting vote_type=Prevote value=[0-9a-f]{64} round=$R_JUMP"; then VALUE_VOTE=1; break; fi
  sleep 0.5
done
log "node0 prevoted a VALUE at round $R_JUMP post-restart: $VALUE_VOTE."
kill -CONT "$(pid_of 3 cl)" "$(pid_of 4 cl)"
log "SIGCONT node3+node4 CLs. Waiting for height to advance past H=$H_STALL (max 600s)..."
RECOVERED=0
for _ in $(seq 1 120); do
  h="$(eth_block_number 8545)"
  if [[ -n "$h" && "$h" -gt "$H_STALL" ]]; then RECOVERED=1; break; fi
  sleep 5
done
log "Recovery: height now $(eth_block_number 8545) (recovered=$RECOVERED)."
sleep 30  # let votes propagate after resume

# ---------- 8. post-restart evidence ----------
for i in 0 1 2 3 4; do
  curl -s --max-time 10 "http://127.0.0.1:$((29000 + i))/metrics" > "$EVIDENCE_DIR/metrics-after/node$i.prom" 2>/dev/null || true
done
cp "$NET/node0/logs/cl.log" "$EVIDENCE_DIR/post-restart/cl-after.log" 2>/dev/null || true
cp "$NET/node1/logs/cl.log" "$EVIDENCE_DIR/post-restart/node1-cl-after.log" 2>/dev/null || true
grep -i "vote" "$EVIDENCE_DIR/post-restart/cl-after.log" 2>/dev/null | tail -n 100 > "$EVIDENCE_DIR/post-restart/votes-after.txt" || true
grep -i "vote" "$EVIDENCE_DIR/post-restart/node1-cl-after.log" 2>/dev/null | tail -n 100 > "$EVIDENCE_DIR/post-restart/node1-votes-after.txt" || true
tail -n +"$((RESTART_LINE + 1))" "$EVIDENCE_DIR/post-restart/cl-after.log" 2>/dev/null | grep -i "beyond the future-round lookahead" | tail -n 30 > "$EVIDENCE_DIR/post-restart/lookahead-drops.txt" || true
grep -E "Prevote\(height: $H_CL, round: $R_JUMP, value: [0-9a-f]{64}, from: 0x66d496cb" "$EVIDENCE_DIR/post-restart/cl-after.log" 2>/dev/null | tail -n 5 > "$EVIDENCE_DIR/post-restart/value-vote-at-R.txt" || true
grep -E "Prevote\(height: $H_CL, round: $R_JUMP, value: [0-9a-f]{64}, from: 0x66d496cb" "$EVIDENCE_DIR/post-restart/node1-cl-after.log" 2>/dev/null | tail -n 5 > "$EVIDENCE_DIR/post-restart/value-vote-at-R-node1.txt" || true
DROPPED_AFTER="$(dropped_votes 0)"
log "node0 dropped_future_round_votes after restart: ${DROPPED_AFTER:-unknown} (before: ${DROPPED_BEFORE:-unknown})"
log "Post-restart VALUE-vote lines at (H=$H_CL, R=$R_JUMP): node0 log $(wc -l < "$EVIDENCE_DIR/post-restart/value-vote-at-R.txt" 2>/dev/null || echo 0), node1 log $(wc -l < "$EVIDENCE_DIR/post-restart/value-vote-at-R-node1.txt" 2>/dev/null || echo 0)."

# ---------- 9. preliminary verdict (observables only) ----------
{
  echo "# C2-01 preliminary verdict (observables — human confirms PROVEN/FAILED)"
  echo ""
  echo "- Stall height (CL): $H_CL; EL height at baseline: $H_STALL"
  echo "- node0 frozen at round: ${R_FREEZE:-?}; network skipped to round: ${R_STALL:-?} (target >= ${R_TARGET:-?}, reached=$STALL_OK)"
  echo "- Jump round R_JUMP: $R_JUMP (node0 nil-prevote there pre-crash: see pre-crash/nil-vote-at-R.txt)"
  echo "- Replay finished: $REPLAYED; rejoined round R_JUMP post-restart: $REJOINED"
  echo "- Height recovered after restart: $RECOVERED (now $(eth_block_number 8545))"
  echo "- node0 dropped_future_round_votes: before=${DROPPED_BEFORE:-?} after-replay=${DROPPED_AFTER_REPLAY:-?} after=${DROPPED_AFTER:-?}"
  if [[ -n "$DROPPED_BEFORE" && -n "$DROPPED_AFTER_REPLAY" && "$DROPPED_AFTER_REPLAY" -gt "$DROPPED_BEFORE" ]]; then
    echo "- REPLAY DROP OBSERVED: counter rose by $((DROPPED_AFTER_REPLAY - DROPPED_BEFORE)) across the restart (replay phase)."
  elif [[ -n "$DROPPED_BEFORE" && -n "$DROPPED_AFTER" && "$DROPPED_AFTER" -gt "$DROPPED_BEFORE" ]]; then
    echo "- REPLAY DROP OBSERVED: counter rose by $((DROPPED_AFTER - DROPPED_BEFORE)) across the restart (final reading)."
  else
    echo "- No replay-drop counter movement observed."
  fi
  echo "- Lookahead-drop log lines during/after replay: $(wc -l < "$EVIDENCE_DIR/post-restart/lookahead-drops.txt" 2>/dev/null || echo 0) (see post-restart/lookahead-drops.txt)"
  echo "- node0 VALUE prevote at (H=$H_CL, R=$R_JUMP) post-restart: $VALUE_VOTE (see post-restart/value-vote-at-R.txt; node1 witness: value-vote-at-R-node1.txt)"
  echo "- Conflicting-vote check (same (H,R), different value): MANUAL — compare pre-crash/nil-vote-at-R.txt (NIL pre-crash) with post-restart/value-vote-at-R.txt (VALUE post-restart)."
  echo "  PROVEN iff both files hold a signed Prevote by node0 (0x66d496cb...) at (H=$H_CL, R=$R_JUMP), one Nil and one a block hash."
  echo ""
  echo "Files: pre-crash/consensus.wal, pre-crash/store.db, pre-crash/cl-before.log,"
  echo "post-restart/cl-after.log, post-restart/node1-cl-after.log, metrics-before/, metrics-after/, RUNLOG.md"
} > "$EVIDENCE_DIR/VERDICT.md"
cat "$EVIDENCE_DIR/VERDICT.md" | tee -a "$RUNLOG"
log "PoC sequence complete. Evidence in $EVIDENCE_DIR"
{ df -h /; free -m; } >> "$RUNLOG" 2>&1
exit 0
