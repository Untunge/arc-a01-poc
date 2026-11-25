#!/usr/bin/env bash
# run-flood.sh — execute the gossipsub subscription-flood PoC (A-01 vector (c))
# on a SINGLE node (node0 CL + EL) and collect evidence.
#
# Vector: malachite v0.8.0 uses plain `gossipsub::Behaviour::new` (default
# AllowAllSubscriptionFilter); any connected peer (no validator key) can
# advertise arbitrarily many topics and each one lands in that peer's
# unbounded `PeerDetails.topics: BTreeSet<TopicHash>` set -> memory DoS.
# Fix in v0.8.2: WhitelistSubscriptionFilter (network/src/behaviour.rs).
#
# Sequence:
# - Start node0 EL (CL exits after 30s without a reachable EL — run-5 lesson),
#   then node0 CL. No quorum needed: a single node is enough, the flood peer
#   only talks gossipsub to it. Single node also keeps the per-IP inbound
#   limit (5, ip_limits.rs) far away: the flood peer is the only inbound
#   connection from 127.0.0.1.
# - The flood peer first joins the real /consensus topic (shared mesh keeps
#   the connection alive), then floods junk topics in stages. Run 4 showed a
#   bare peer (identify "flood-peer/1.0.0", no validator-proof protocol, no
#   shared topic) gets its connection closed ~1ms after identify; run 5
#   proved the logged cause once the protocol matched: the node rejects
#   non-persistent peers because the localnet passes
#   --p2p.persistent-peers-only (discovery/src/handlers/identify.rs,
#   handle_new_peer). That flag is a localnet test config (CLI default
#   false), so the flood run strips it from node0's launch line — the
#   vector is about post-connect behaviour, and production nodes accept
#   arbitrary inbound peers.
# - Baseline RSS 30s -> flood peer subscribes to LEVELS topics in stages
#   (default 5000/20000/50000 at 1000/s, 10s settle per level) -> observe 60s.
# - Observables: CL RSS sampled every 1s; node log count of
#   "SUBSCRIPTION: Adding gossip peer to topic" (one debug line per accepted
#   NEW topic); flood-peer stdout (LEVEL_DONE markers = sent counts).
#
# Env:
#   POC_DIR       — repo root (default: parent of this script's dir)
#   LEVELS        — comma list, default "5000,20000,50000"
#   RATE          — subscriptions/sec, default 1000
#   SETTLE_SECS   — settle after each level, default 10
#   HOLD_SECS     — hold after last level, default 60
#   BASELINE_SECS — default 30
#   OBSERVE_SECS  — default 60
#   EVIDENCE_DIR  — default: $POC_DIR/evidence/gossipsub-flood
#
# Exit code: 0 if the sequence ran to completion (whatever the verdict);
# non-zero only on infrastructure failure (net would not start / flood
# peer failed to connect). The verdict lives in $EVIDENCE_DIR/VERDICT.md.
set -u
POC_DIR="${POC_DIR:-$(cd "$(dirname "$0")/.." && pwd)}"
LEVELS="${LEVELS:-5000,20000,50000}"
RATE="${RATE:-1000}"
SETTLE_SECS="${SETTLE_SECS:-10}"
HOLD_SECS="${HOLD_SECS:-60}"
BASELINE_SECS="${BASELINE_SECS:-30}"
OBSERVE_SECS="${OBSERVE_SECS:-60}"
NET="$POC_DIR/net"
EVIDENCE_DIR="${EVIDENCE_DIR:-$POC_DIR/evidence/gossipsub-flood}"
RUNLOG="$EVIDENCE_DIR/RUNLOG.md"
FLOOD_BIN="$POC_DIR/harness/flood-peer/target/release/flood-peer"
mkdir -p "$EVIDENCE_DIR"

log() { echo "[$(date -u +%H:%M:%S)] $*" | tee -a "$RUNLOG"; }
phase() { echo "$(date +%s) $1" >> "$EVIDENCE_DIR/phases.tsv"; log "PHASE: $1"; }

: > "$RUNLOG"
: > "$EVIDENCE_DIR/phases.tsv"
log "PoC gossipsub-flood run starting. POC_DIR=$POC_DIR LEVELS=$LEVELS RATE=$RATE/s"
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
cl_alive() { kill -0 "$(pid_of 0 cl)" 2>/dev/null; }

cleanup() {
  log "Cleaning up node processes..."
  [[ -n "${SAMPLER_PID:-}" ]] && kill "$SAMPLER_PID" 2>/dev/null || true
  for kind in cl el; do
    local p; p="$(pid_of 0 "$kind")"
    [[ -n "$p" ]] && kill "$p" 2>/dev/null || true
  done
  sleep 2
  pkill -f 'arc-node-consensus start' 2>/dev/null || true
  pkill -f 'arc-node-execution node' 2>/dev/null || true
  pkill -f 'flood-peer --target' 2>/dev/null || true
}
trap cleanup EXIT

# ---------- preconditions ----------
for b in arc-node-execution arc-node-consensus; do
  [[ -x "$POC_DIR/arc-node/target/debug/$b" ]] || { log "FATAL: binary missing: $b"; exit 2; }
done
[[ -x "$FLOOD_BIN" ]] || { log "FATAL: flood-peer binary missing: $FLOOD_BIN"; exit 2; }
[[ -f "$NET/node0/malachite/config/priv_validator_key.json" ]] || { log "FATAL: node0 key missing"; exit 2; }
NODE0_PEER_ID="$(sed -n '1p' "$NET/shared/peer_ids.txt")"
[[ -n "$NODE0_PEER_ID" ]] || { log "FATAL: peer_ids.txt missing/empty"; exit 2; }
log "Preconditions OK. node0 peer id: $NODE0_PEER_ID"

# ---------- 1. start node0 EL ----------
nohup "$NET/node0/run-el.sh" > /dev/null 2>&1 &
echo $! > "$NET/node0/el.pid"
log "EL started. Waiting for eth RPC on 8545..."
for _ in $(seq 1 24); do
  if ! kill -0 "$(pid_of 0 el)" 2>/dev/null; then
    log "FATAL: node0 EL died. Last EL log lines:"; tail -n 25 "$NET/node0/logs/el.log" >> "$RUNLOG" 2>&1 || true
    exit 3
  fi
  [[ -n "$(eth_block_number 8545)" ]] && break
  sleep 5
done
log "  node0 EL blockNumber: $(eth_block_number 8545)"

# ---------- 2. start node0 CL (WITHOUT --p2p.persistent-peers-only) ----------
# Run 5 (45609362664) proved the flag is a hard gate for this PoC: the node
# logs "Rejecting connection from non-persistent peer as persistent_peers_only
# mode is on" (discovery/src/handlers/identify.rs, handle_new_peer) and
# closes the connection ~1ms after identify. The flag is a localnet test
# config (CLI default false, start.rs:523); production nodes accept inbound
# connections from arbitrary peers subject only to connection/ip limits.
# The vector is about what happens AFTER a peer connects, so the flood run
# launches node0 with the flag stripped (same binary, same args otherwise).
sed '/--p2p.persistent-peers-only/d' "$NET/node0/run-cl.sh" > "$NET/node0/run-cl-flood.sh"
chmod +x "$NET/node0/run-cl-flood.sh"
nohup "$NET/node0/run-cl-flood.sh" > /dev/null 2>&1 &
echo $! > "$NET/node0/cl.pid"
log "CL started. Waiting 60s for startup, then checking liveness..."
sleep 60
if ! cl_alive; then
  log "FATAL: node0 CL died. Last CL log lines:"; tail -n 25 "$NET/node0/logs/cl.log" >> "$RUNLOG" 2>&1 || true
  exit 3
fi
curl -s --max-time 10 "http://127.0.0.1:29000/metrics" > "$EVIDENCE_DIR/metrics-before.prom" 2>/dev/null || true
log "CL alive (PID $(pid_of 0 cl)), metrics snapshot saved ($(wc -c < "$EVIDENCE_DIR/metrics-before.prom") bytes)."
log "CL RSS at start: $(ps -o rss= -p "$(pid_of 0 cl)" | tr -d ' ') KiB"

# ---------- 3. RSS sampler (background, 1s) ----------
CL_PID="$(pid_of 0 cl)"
( while true; do
    rss="$(ps -o rss= -p "$CL_PID" 2>/dev/null | tr -d ' ')"
    echo "$(date +%s) ${rss:-0}" >> "$EVIDENCE_DIR/rss.csv"
    sleep 1
  done ) &
SAMPLER_PID=$!
log "RSS sampler started (PID $SAMPLER_PID, 1s interval)."

# ---------- 4. baseline ----------
phase "baseline"
sleep "$BASELINE_SECS"
log "Baseline ${BASELINE_SECS}s done."

# ---------- 5. flood ----------
phase "flood"
TARGET="/ip4/127.0.0.1/tcp/27000/p2p/$NODE0_PEER_ID"
log "Starting flood-peer -> $TARGET (levels=$LEVELS rate=$RATE/s settle=${SETTLE_SECS}s hold=${HOLD_SECS}s)"
RUST_LOG="${RUST_LOG:-debug}" "$FLOOD_BIN" --target "$TARGET" --levels "$LEVELS" --rate "$RATE" \
  --settle-secs "$SETTLE_SECS" --hold-secs "$HOLD_SECS" \
  > "$EVIDENCE_DIR/flood-peer.log" 2> "$EVIDENCE_DIR/flood-peer.stderr" &
FLOOD_PID=$!
log "flood-peer PID $FLOOD_PID"
FLOOD_OK=0
for _ in $(seq 1 600); do
  if ! kill -0 "$FLOOD_PID" 2>/dev/null; then wait "$FLOOD_PID"; FLOOD_OK=$?; break; fi
  sleep 1
done
if kill -0 "$FLOOD_PID" 2>/dev/null; then
  log "FATAL: flood-peer still running after 600s; killing."
  kill "$FLOOD_PID" 2>/dev/null || true
  FLOOD_OK=124
fi
log "flood-peer exited with code $FLOOD_OK."
grep -E "LEVEL_DONE|DONE|FATAL|CONNECTED|READY" "$EVIDENCE_DIR/flood-peer.log" | tee -a "$RUNLOG" || true
if [[ "$FLOOD_OK" != 0 ]]; then
  log "FATAL: flood-peer failed. stderr tail:"
  tail -n 25 "$EVIDENCE_DIR/flood-peer.stderr" >> "$RUNLOG" 2>&1 || true
  exit 4
fi

# ---------- 6. observe ----------
phase "observe"
sleep "$OBSERVE_SECS"
log "Observe ${OBSERVE_SECS}s done."
kill "$SAMPLER_PID" 2>/dev/null || true
sleep 1

# ---------- 7. collect evidence ----------
phase "collect"
curl -s --max-time 10 "http://127.0.0.1:29000/metrics" > "$EVIDENCE_DIR/metrics-after.prom" 2>/dev/null || true
cp "$NET/node0/logs/cl.log" "$EVIDENCE_DIR/cl.log" 2>/dev/null || true
cp "$NET/node0/logs/el.log" "$EVIDENCE_DIR/el.log" 2>/dev/null || true
ACCEPTED="$(grep -c 'SUBSCRIPTION: Adding gossip peer to topic' "$EVIDENCE_DIR/cl.log" 2>/dev/null || true)"; ACCEPTED="${ACCEPTED:-0}"
UNKNOWN_SUBS="$(grep -c 'tried to subscribe to unknown topic' "$EVIDENCE_DIR/cl.log" 2>/dev/null || true)"; UNKNOWN_SUBS="${UNKNOWN_SUBS:-0}"
log "Accepted-subscription log lines in cl.log: $ACCEPTED"
log "Unknown-topic trace lines in cl.log: $UNKNOWN_SUBS"
grep 'SUBSCRIPTION: Adding gossip peer to topic' "$EVIDENCE_DIR/cl.log" 2>/dev/null | head -n 5 > "$EVIDENCE_DIR/accepted-sample.txt" || true
grep 'SUBSCRIPTION: Adding gossip peer to topic' "$EVIDENCE_DIR/cl.log" 2>/dev/null | tail -n 5 >> "$EVIDENCE_DIR/accepted-sample.txt" || true

# RSS stats per phase (python3, stdlib only)
python3 - "$EVIDENCE_DIR" <<'PYEOF' > "$EVIDENCE_DIR/rss-stats.txt" 2>&1 || echo "rss stats failed" >> "$EVIDENCE_DIR/rss-stats.txt"
import sys, statistics
d = sys.argv[1]
rows = []
for line in open(f"{d}/rss.csv"):
    p = line.split()
    if len(p) == 2:
        rows.append((int(p[0]), int(p[1])))
phases = []
for line in open(f"{d}/phases.tsv"):
    p = line.split()
    if len(p) == 2:
        phases.append((int(p[0]), p[1]))
phases.sort()
bounds = [(phases[i][1], phases[i][0], phases[i+1][0] if i+1 < len(phases) else 10**18)
          for i in range(len(phases))]
print("phase,n_samples,mean_rss_kib,median_rss_kib,min_rss_kib,max_rss_kib")
for name, t0, t1 in bounds:
    vals = [r for (t, r) in rows if t0 <= t < t1 and r > 0]
    if not vals:
        print(f"{name},0,-,-,-,-")
        continue
    print(f"{name},{len(vals)},{statistics.mean(vals):.0f},{statistics.median(vals):.0f},{min(vals)},{max(vals)}")
PYEOF
cat "$EVIDENCE_DIR/rss-stats.txt" | tee -a "$RUNLOG"

# ---------- 8. verdict (observables only) ----------
LAST_LEVEL="$(echo "$LEVELS" | awk -F, '{print $NF}')"
SENT="$(grep -E '^DONE' "$EVIDENCE_DIR/flood-peer.log" | grep -oE 'total_sent=[0-9]+' | grep -oE '[0-9]+' | tail -n 1)"
{
  echo "# gossipsub-flood preliminary verdict (observables — human confirms TERBUKTI/GAGAL)"
  echo ""
  echo "- Target: node0 CL (malachite v0.8.0, arc-node @ 6e76402), single node + EL, localnet only"
  echo "- Flood peer: plain libp2p peer, ephemeral keypair, NO validator key/proof"
  echo "- Levels requested: $LEVELS (rate ${RATE}/s, settle ${SETTLE_SECS}s, hold ${HOLD_SECS}s)"
  echo "- Subscriptions SENT by flood peer (DONE marker): ${SENT:-?} (last level requested: $LAST_LEVEL)"
  echo "- Subscriptions ACCEPTED by node (cl.log 'SUBSCRIPTION: Adding gossip peer to topic'): $ACCEPTED"
  echo "- Unknown-topic trace lines at app layer (ignored, no disconnect): $UNKNOWN_SUBS"
  echo "- RSS per phase (KiB): see rss-stats.txt"
  echo ""
  echo "TERBUKTI iff: SENT == $LAST_LEVEL AND ACCEPTED == SENT AND RSS grows"
  echo "monotonically across levels (baseline < 5k < 20k < 50k) with no cap/"
  echo "rejection visible in logs."
  echo "GAGAL iff: ACCEPTED << SENT, or connection rejected, or RSS flat."
  echo ""
  echo "Files: cl.log, el.log, flood-peer.log, flood-peer.stderr, rss.csv,"
  echo "rss-stats.txt, phases.tsv, metrics-before.prom, metrics-after.prom, RUNLOG.md"
} > "$EVIDENCE_DIR/VERDICT.md"
cat "$EVIDENCE_DIR/VERDICT.md" | tee -a "$RUNLOG"
log "PoC sequence complete. Evidence in $EVIDENCE_DIR"
{ df -h /; free -m; } >> "$RUNLOG" 2>&1
exit 0
