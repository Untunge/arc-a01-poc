#!/usr/bin/env bash
# net-setup.sh — create the 4-node localnet layout (no Docker).
# Creates dirs, shared genesis + JWT, per-node launch scripts.
# Key generation is a separate step (scripts/gen_keys.py).
#
# Env:
#   POC_DIR      — repo root (default: parent of this script's dir)
#   ARC_NODE_DIR — arc-node checkout (default: $POC_DIR/arc-node)
#
# Port note: EL WS is 8645+i. The original design table had WS 8546+i which
# collides with the next node's HTTP port.
set -euo pipefail

POC_DIR="${POC_DIR:-$(cd "$(dirname "$0")/.." && pwd)}"
ARC_NODE_DIR="${ARC_NODE_DIR:-$POC_DIR/arc-node}"
NET="$POC_DIR/net"
BIN_DIR="$ARC_NODE_DIR/target/debug"

mkdir -p "$NET/shared"
for i in 0 1 2 3 4; do
  mkdir -p "$NET/node$i/malachite/config" "$NET/node$i/reth" "$NET/node$i/logs"
done

cp "$ARC_NODE_DIR/assets/localdev/genesis.json" "$NET/shared/genesis.json"
cp "$ARC_NODE_DIR/assets/localdev/reth.toml" "$NET/shared/reth.toml"

if [[ ! -f "$NET/shared/jwt.hex" ]]; then
  openssl rand -hex 32 > "$NET/shared/jwt.hex"
fi

# Fee recipient: Hardhat/test-mnemonic account 0 (prefunded in localdev genesis).
FEE_RECIPIENT="0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"

for i in 0 1 2 3 4; do
  CL_P2P=$((27000 + i))
  CL_METRICS=$((29000 + i))
  CL_RPC=$((31000 + i))
  EL_HTTP=$((8545 + i))
  EL_WS=$((8645 + i))
  EL_AUTH=$((8551 + i))
  EL_METRICS=$((9001 + i))
  EL_P2P=$((30303 + i))

  # Persistent peers are resolved at CL launch from net/shared/peer_ids.txt
  # (written by gen_keys.py) — see run-cl.sh below. Run 2 showed bare
  # /ip4/.../tcp/2700j multiaddrs leave node0 peered only with node2, so the
  # multiaddrs now carry explicit /p2p/<PeerId> suffixes.

  cat > "$NET/node$i/run-el.sh" <<EOF
#!/usr/bin/env bash
# EL (reth) for node $i
exec "$BIN_DIR/arc-node-execution" node \\
  --chain="$NET/shared/genesis.json" \\
  --config="$NET/shared/reth.toml" \\
  --datadir="$NET/node$i/reth" \\
  --http --http.addr=127.0.0.1 --http.port=$EL_HTTP --http.api=all \\
  --ws --ws.addr=127.0.0.1 --ws.port=$EL_WS --ws.api=all \\
  --authrpc.addr=127.0.0.1 --authrpc.port=$EL_AUTH --authrpc.jwtsecret="$NET/shared/jwt.hex" \\
  --metrics=127.0.0.1:$EL_METRICS \\
  --port=$EL_P2P --discovery.port=$EL_P2P \\
  --disable-discovery --ipcdisable \\
  --arc-rpc-upstream-url=http://127.0.0.1:$CL_RPC \\
  >> "$NET/node$i/logs/el.log" 2>&1
EOF

  cat > "$NET/node$i/run-cl.sh" <<EOF
#!/usr/bin/env bash
# CL (malachite consensus) for node $i
export ARC_GENESIS_FILE_PATH="$NET/shared/genesis.json"
[[ -s "$NET/shared/peer_ids.txt" ]] || { echo "FATAL: peer_ids.txt missing" >&2; exit 2; }
PEERS=""
j=0
while IFS= read -r pid; do
  if [[ "\$j" != "$i" && -n "\$pid" ]]; then
    PEERS="\$PEERS --p2p.persistent-peers=/ip4/127.0.0.1/tcp/\$((27000 + j))/p2p/\$pid"
  fi
  j=\$((j + 1))
done < "$NET/shared/peer_ids.txt"
exec "$BIN_DIR/arc-node-consensus" start \\
  --home="$NET/node$i/malachite" \\
  --moniker="node$i" \\
  --log-level=debug \\
  --p2p.addr=/ip4/127.0.0.1/tcp/$CL_P2P \\
  \$PEERS \\
  --p2p.persistent-peers-only \\
  --validator --suggested-fee-recipient=$FEE_RECIPIENT \\
  --eth-rpc-endpoint=http://127.0.0.1:$EL_HTTP \\
  --execution-endpoint=http://127.0.0.1:$EL_AUTH \\
  --execution-ws-endpoint=ws://127.0.0.1:$EL_WS \\
  --execution-jwt="$NET/shared/jwt.hex" \\
  --rpc.addr=127.0.0.1:$CL_RPC \\
  --metrics=127.0.0.1:$CL_METRICS \\
  >> "$NET/node$i/logs/cl.log" 2>&1
EOF
  chmod +x "$NET/node$i/run-el.sh" "$NET/node$i/run-cl.sh"
done

echo "net layout ready under $NET"
echo "next: python3 scripts/gen_keys.py && python3 scripts/verify-keys.py"
ls "$NET/shared"
