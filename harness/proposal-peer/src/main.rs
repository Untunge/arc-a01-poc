//! proposal-peer — a libp2p peer that sends far-future round proposals,
//! signed as ONE Byzantine validator, to a malachite v0.8.0 node.
//!
//! Purpose: PoC for Arc A-01 vector (a) — malachite v0.8.0 `on_proposal`
//! (`core-consensus/src/handle/proposal.rs`) has no future-round lookahead
//! bound (unlike `on_vote`, which drops rounds > current + 10). Every
//! proposal that passes proposer + signature verification is stored in the
//! `FullProposalKeeper` (capped at 2 per round, but with NO cap on the number
//! of rounds and no eviction) and WAL-appended UNCONDITIONALLY — even an
//! exact duplicate of an already-stored proposal (`DuplicateIgnored` still
//! reaches `Effect::WalAppend`). See `STATIC-far-future-proposals.md`.
//!
//! Two variants, both in one run:
//!   V1 — distinct rounds: one proposal per round owned by our validator
//!        (round-robin proposer `(height-1+round) % n`), rounds starting at
//!        current_round + 11 (i.e. beyond the v0.8.2 fix threshold of +10).
//!   V2 — duplicate replay: the exact same proposal bytes (first V1
//!        proposal) republished many times; each copy must be re-verified
//!        and re-WAL-appended.
//!
//! Connection survival reuses the flood-peer lessons (phase 3, proven):
//! identify protocol `/malachitebft-core-consensus/v1beta1`, validator-proof
//! responder (never sends a proof), `/ax/broadcast/1.0.0` via libp2p-scatter,
//! join the real `/consensus` gossipsub topic first.
//!
//! NOTE (run 2 lesson): malachite uses **identity-hash** gossipsub topics
//! (`Channel::to_gossipsub_topic` returns `gossipsub::IdentTopic`).
//! Subscribing/publishing with `Sha256Topic` silently addresses a DIFFERENT
//! topic hash: the node records the subscription under the unknown sha256
//! hash, the local JOIN finds 0 mesh peers, zero messages arrive, and
//! `publish` fails with `NoPeersSubscribedToTopic`. (Phase-3 flood-peer has
//! the same latent mismatch, harmless there because it only ever sent
//! SUBSCRIBE frames, which need no mesh.) This peer uses `IdentTopic`.
//!
//! Wire format (verified against arc-node @ 6e76402 source):
//! - SSZ sign bytes = `Proposal::to_sign_bytes()` = `as_ssz_bytes()`:
//!   container height:u64 LE | round:Option<u32> (0x00 | 0x01+LE) |
//!   value:[u8;32] | pol_round:Option<u32> | validator_address:[u8;20],
//!   with 4-byte LE offsets for the two variable (Option) fields.
//!   Hand-rolled here (no ssz dependency); unit tests pin two vectors.
//! - Signature: Ed25519 (ed25519-consensus 2.1.0, same as malachite) over
//!   the SSZ bytes, with the validator's deterministic localnet seed.
//! - Envelope: 0x01 version byte + protobuf SignedMessage{proposal, signature}
//!   (hand-rolled protobuf; unit test pins the exact bytes), published to
//!   gossipsub topic `/consensus`.
//! - Validator set order is address-sorted (`ValidatorSet::new` sorts by
//!   address); the peer derives all validator addresses from the supplied
//!   seeds, sorts them, and uses the validator at `--validator-index`
//!   (default 1) in that sorted order — so the proposer-round math holds
//!   regardless of node-numbering vs address order.
//!
//! Stdout protocol (parsed by run-far-future.sh):
//!   CONNECTED peer=<PeerId>
//!   READY
//!   JOINED topics=<n>
//!   OBSERVED height=<H> round=<R>          (max seen on gossipsub, on advance)
//!   TARGET height=<H> base_round=<R> first_round=<R> validator_index=<I> validator_address=0x...
//!   V1_DONE sent=<N> first_round=<R> last_round=<R> elapsed_s=<secs>
//!   V2_DONE sent=<N> elapsed_s=<secs>
//!   DONE total_sent=<N> elapsed_s=<secs>
//!   FATAL <reason>            (non-zero exit)
//!
//! Usage:
//!   proposal-peer --target /ip4/127.0.0.1/tcp/27000/p2p/<node0 PeerId>
//!     --validator-seeds-hex <seed1,seed2,...> --validator-index 1
//!     --height 123 --start-round 5
//!     [--v1-count 600] [--v2-count 600] [--rate 100]
//!     [--observe-secs 20] [--v1-settle-secs 10] [--hold-secs 30]
//!
//! Safety: localnet only, deterministic TEST keys only (the public
//! `test test ... junk` mnemonic derivation used by the localnet itself).
//! The peer signs as a genuine genesis validator — that IS the threat
//! model (Byzantine validator), not a forgery: no external attacker
//! without a validator key can do this.

use std::env;
use std::io;
use std::time::{Duration, Instant};

use futures::StreamExt;
use libp2p::{
    gossipsub, identify, noise, ping, request_response, swarm::NetworkBehaviour,
    swarm::SwarmEvent, tcp, yamux, Multiaddr, PeerId, StreamProtocol, SwarmBuilder,
};
use libp2p::futures::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use sha3::Digest as _;

/// The node's consensus protocol name on localdev (malachite defaults).
const DEFAULT_IDENTIFY_PROTOCOL: &str = "/malachitebft-core-consensus/v1beta1";
/// The node's validator-proof protocol name on localdev.
const VALIDATOR_PROOF_PROTOCOL: &str = "/malachitebft-validator-proof/v1";
/// Max proof bytes the node's codec accepts.
const MAX_PROOF_SIZE: usize = 1024;
/// Gossipsub topic carrying consensus messages.
const CONSENSUS_TOPIC: &str = "/consensus";

/// Minimal replica of the node's validator-proof codec
/// (`malachite/.../network/src/validator_proof/codec.rs`):
/// request = unsigned-varint length + proof bytes, response = zero bytes.
/// The peer only ever ANSWERS proofs; it never sends one.
#[derive(Clone, Default)]
struct ProofCodec;

#[async_trait::async_trait]
impl request_response::Codec for ProofCodec {
    type Protocol = StreamProtocol;
    type Request = Vec<u8>;
    type Response = ();

    async fn read_request<T>(&mut self, _: &Self::Protocol, io: &mut T) -> io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        let mut len: usize = 0;
        let mut shift = 0u32;
        loop {
            let mut b = [0u8; 1];
            io.read_exact(&mut b).await?;
            len |= ((b[0] & 0x7f) as usize) << shift;
            if b[0] & 0x80 == 0 {
                break;
            }
            shift += 7;
            if shift > 28 {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "varint too long"));
            }
        }
        if len > MAX_PROOF_SIZE {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "proof too large"));
        }
        let mut buf = vec![0u8; len];
        io.read_exact(&mut buf).await?;
        Ok(buf)
    }

    async fn read_response<T>(
        &mut self,
        _: &Self::Protocol,
        _: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        Ok(())
    }

    async fn write_request<T>(
        &mut self,
        _: &Self::Protocol,
        _: &mut T,
        _: Self::Request,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        Ok(())
    }

    async fn write_response<T>(
        &mut self,
        _: &Self::Protocol,
        _: &mut T,
        _: Self::Response,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        Ok(())
    }
}

#[derive(NetworkBehaviour)]
struct ProposalBehaviour {
    gossipsub: gossipsub::Behaviour,
    identify: identify::Behaviour,
    ping: ping::Behaviour,
    validator_proof: request_response::Behaviour<ProofCodec>,
    broadcast: libp2p_scatter::Behaviour,
}

// ---------------------------------------------------------------------------
// Wire encoding: SSZ sign bytes + protobuf envelope (hand-rolled, unit-tested)
// ---------------------------------------------------------------------------

/// SSZ encoding of `Proposal{height, round, value, pol_round, validator_address}`
/// exactly as `arc-node` `crates/types/src/proposal.rs` derives it:
/// height is a transparent u64 (8 LE bytes); round / pol_round use
/// `ssz_round`, i.e. a transparent `Option<u32>` (0x00 for Nil, 0x01 + 4 LE
/// bytes for Some) — both variable-length, so the container carries 4-byte
/// LE offsets for them in the fixed part (8 + 4 + 32 + 4 + 20 = 68 bytes);
/// value is a transparent 32-byte block hash; validator_address is a
/// transparent 20-byte address.
fn ssz_proposal_bytes(
    height: u64,
    round: u32,
    value: &[u8; 32],
    pol_round: Option<u32>,
    address: &[u8; 20],
) -> Vec<u8> {
    let mut round_enc = Vec::with_capacity(5);
    round_enc.push(1u8);
    round_enc.extend_from_slice(&round.to_le_bytes());
    let mut pol_enc = Vec::with_capacity(5);
    match pol_round {
        Some(r) => {
            pol_enc.push(1u8);
            pol_enc.extend_from_slice(&r.to_le_bytes());
        }
        None => pol_enc.push(0u8),
    }
    const FIXED_LEN: usize = 8 + 4 + 32 + 4 + 20; // 68
    let round_off = FIXED_LEN as u32;
    let pol_off = (FIXED_LEN + round_enc.len()) as u32;
    let mut out = Vec::with_capacity(FIXED_LEN + round_enc.len() + pol_enc.len());
    out.extend_from_slice(&height.to_le_bytes());
    out.extend_from_slice(&round_off.to_le_bytes());
    out.extend_from_slice(value);
    out.extend_from_slice(&pol_off.to_le_bytes());
    out.extend_from_slice(address);
    out.extend_from_slice(&round_enc);
    out.extend_from_slice(&pol_enc);
    out
}

fn put_varint(mut v: u64, out: &mut Vec<u8>) {
    loop {
        let b = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(b);
            break;
        }
        out.push(b | 0x80);
    }
}

fn put_varint_field(field: u32, v: u64, out: &mut Vec<u8>) {
    put_varint(((field << 3) | 0) as u64, out);
    put_varint(v, out);
}

fn put_len_field(field: u32, payload: &[u8], out: &mut Vec<u8>) {
    put_varint(((field << 3) | 2) as u64, out);
    put_varint(payload.len() as u64, out);
    out.extend_from_slice(payload);
}

/// Protobuf `Proposal{height=1, round=2, value=3{block_hash=1},
/// pol_round=4 (omitted when Nil), validator_address=5{value=1}}`.
fn encode_proposal(height: u64, round: u32, value: &[u8; 32], address: &[u8; 20]) -> Vec<u8> {
    let mut out = Vec::new();
    put_varint_field(1, height, &mut out);
    put_varint_field(2, round as u64, &mut out);
    let mut v = Vec::new();
    put_len_field(1, value, &mut v);
    put_len_field(3, &v, &mut out);
    // pol_round: Nil -> field absent (proto3 optional).
    let mut a = Vec::new();
    put_len_field(1, address, &mut a);
    put_len_field(5, &a, &mut out);
    out
}

/// Envelope: 0x01 version byte + protobuf
/// `SignedMessage{proposal=1, signature=3{bytes=1}}`.
fn encode_signed_message(proposal: &[u8], signature: &[u8; 64]) -> Vec<u8> {
    let mut sig = Vec::new();
    put_len_field(1, signature, &mut sig);
    let mut out = Vec::with_capacity(1 + proposal.len() + sig.len() + 4);
    out.push(0x01);
    put_len_field(1, proposal, &mut out);
    put_len_field(3, &sig, &mut out);
    out
}

fn read_varint(data: &[u8], pos: &mut usize) -> Option<u64> {
    let mut v: u64 = 0;
    let mut shift = 0u32;
    loop {
        let b = *data.get(*pos)?;
        *pos += 1;
        v |= ((b & 0x7f) as u64) << shift;
        if b & 0x80 == 0 {
            return Some(v);
        }
        shift += 7;
        if shift > 63 {
            return None;
        }
    }
}

/// Parse a received gossipsub payload (0x01 + SignedMessage protobuf).
/// Returns (height, round) of the inner Proposal (field 1) or Vote (field 2).
/// Vote proto: vote_type=1, height=2, round=3.
fn parse_signed_message(data: &[u8]) -> Option<(u64, u32)> {
    if data.first() != Some(&0x01) {
        return None;
    }
    let mut pos = 1usize;
    while pos < data.len() {
        let tag = read_varint(data, &mut pos)?;
        let field = (tag >> 3) as u32;
        let wire = (tag & 7) as u32;
        if wire != 2 {
            return None;
        }
        let len = read_varint(data, &mut pos)? as usize;
        if pos + len > data.len() {
            return None;
        }
        let msg = &data[pos..pos + len];
        pos += len;
        match field {
            1 => return parse_proposal_fields(msg),
            2 => return parse_vote_fields(msg),
            _ => {}
        }
    }
    None
}

fn parse_proposal_fields(msg: &[u8]) -> Option<(u64, u32)> {
    parse_varint_fields(msg, 1, 2)
}

fn parse_vote_fields(msg: &[u8]) -> Option<(u64, u32)> {
    parse_varint_fields(msg, 2, 3)
}

fn parse_varint_fields(msg: &[u8], height_field: u32, round_field: u32) -> Option<(u64, u32)> {
    let mut pos = 0usize;
    let mut height: Option<u64> = None;
    let mut round: Option<u32> = None;
    while pos < msg.len() {
        let tag = read_varint(msg, &mut pos)?;
        let field = (tag >> 3) as u32;
        let wire = (tag & 7) as u32;
        match wire {
            0 => {
                let v = read_varint(msg, &mut pos)?;
                if field == height_field {
                    height = Some(v);
                } else if field == round_field {
                    round = Some(v as u32);
                }
            }
            2 => {
                let len = read_varint(msg, &mut pos)? as usize;
                pos = pos.checked_add(len)?;
                if pos > msg.len() {
                    return None;
                }
            }
            _ => return None,
        }
    }
    Some((height?, round?))
}

/// Round-robin proposer: `(height - 1 + round) % n`
/// (`arc-node/crates/types/src/proposer.rs`).
fn proposer_index(height: u64, round: u64, n: u64) -> u64 {
    (height - 1 + round) % n
}

fn hex_decode(s: &str) -> Option<Vec<u8>> {
    let s = s.trim();
    if s.len() % 2 != 0 {
        return None;
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    let b = s.as_bytes();
    for i in (0..b.len()).step_by(2) {
        let hi = (b[i] as char).to_digit(16)?;
        let lo = (b[i + 1] as char).to_digit(16)?;
        out.push(((hi << 4) | lo) as u8);
    }
    Some(out)
}

/// Derive (signing key, validator address) from a 32-byte Ed25519 seed.
/// Address = first 20 bytes of Keccak256(pubkey)
/// (`arc-node/crates/types/src/address.rs`).
fn validator_from_seed(seed_hex: &str) -> Option<(ed25519_consensus::SigningKey, [u8; 20])> {
    let seed = hex_decode(seed_hex)?;
    let seed32: [u8; 32] = seed.try_into().ok()?;
    let sk = ed25519_consensus::SigningKey::from(seed32);
    let vk = ed25519_consensus::VerificationKey::from(&sk);
    let mut hasher = sha3::Keccak256::new();
    hasher.update(vk.as_bytes());
    let hash = hasher.finalize();
    let mut address = [0u8; 20];
    address.copy_from_slice(&hash[..20]);
    Some((sk, address))
}

fn usage() -> ! {
    eprintln!(
        "usage: proposal-peer --target <multiaddr> --validator-seeds-hex <seed,...> \
         [--validator-index 1] --height <H> --start-round <R> \
         [--v1-count 600] [--v2-count 600] [--rate 100] [--observe-secs 20] \
         [--v1-settle-secs 10] [--hold-secs 30] \
         [--identify-protocol /malachitebft-core-consensus/v1beta1] \
         [--join-topics /consensus] [--join-settle-secs 5]"
    );
    std::process::exit(2);
}

fn arg_value(args: &[String], name: &str, default: &str) -> String {
    for w in args.windows(2) {
        if w[0] == name {
            return w[1].clone();
        }
    }
    default.to_string()
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    WaitReady,
    WaitJoin,
    JoinSettle,
    Observe,
    FloodV1,
    SettleV1,
    FloodV2,
    Hold,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args: Vec<String> = env::args().skip(1).collect();
    let target: Multiaddr = match arg_value(&args, "--target", "").parse() {
        Ok(m) if !arg_value(&args, "--target", "").is_empty() => m,
        _ => usage(),
    };
    let seeds_hex = arg_value(&args, "--validator-seeds-hex", "");
    if seeds_hex.is_empty() {
        usage();
    }
    let validator_index: usize = arg_value(&args, "--validator-index", "1")
        .parse()
        .unwrap_or_else(|_| usage());
    let arg_height: u64 = arg_value(&args, "--height", "0")
        .parse()
        .unwrap_or_else(|_| usage());
    let arg_start_round: u32 = arg_value(&args, "--start-round", "0")
        .parse()
        .unwrap_or_else(|_| usage());
    let v1_count: usize = arg_value(&args, "--v1-count", "600")
        .parse()
        .unwrap_or_else(|_| usage());
    let v2_count: usize = arg_value(&args, "--v2-count", "600")
        .parse()
        .unwrap_or_else(|_| usage());
    let rate: usize = arg_value(&args, "--rate", "100")
        .parse()
        .unwrap_or_else(|_| usage());
    let observe_secs: u64 = arg_value(&args, "--observe-secs", "20")
        .parse()
        .unwrap_or_else(|_| usage());
    let v1_settle_secs: u64 = arg_value(&args, "--v1-settle-secs", "10")
        .parse()
        .unwrap_or_else(|_| usage());
    let hold_secs: u64 = arg_value(&args, "--hold-secs", "30")
        .parse()
        .unwrap_or_else(|_| usage());
    let identify_protocol =
        arg_value(&args, "--identify-protocol", DEFAULT_IDENTIFY_PROTOCOL);
    let join_topics: Vec<String> = arg_value(&args, "--join-topics", CONSENSUS_TOPIC)
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    let join_settle_secs: u64 = arg_value(&args, "--join-settle-secs", "5")
        .parse()
        .unwrap_or_else(|_| usage());
    if arg_height == 0 || v1_count == 0 || rate == 0 {
        usage();
    }

    // --- validator identities: sort by address, pick --validator-index ---
    let seed_list: Vec<String> = seeds_hex.split(',').map(|s| s.trim().to_string()).collect();
    let mut validators: Vec<(ed25519_consensus::SigningKey, [u8; 20])> = Vec::new();
    for s in &seed_list {
        match validator_from_seed(s) {
            Some(v) => validators.push(v),
            None => {
                eprintln!("FATAL bad validator seed hex: {s}");
                std::process::exit(2);
            }
        }
    }
    if validators.is_empty() || validator_index >= validators.len() {
        eprintln!(
            "FATAL validator-index {validator_index} out of range for {} seeds",
            validators.len()
        );
        std::process::exit(2);
    }
    validators.sort_by(|a, b| a.1.cmp(&b.1));
    let n_validators = validators.len() as u64;
    let (signing_key, validator_address) = &validators[validator_index];
    let validator_address_hex = validator_address.iter().map(|b| format!("{b:02x}")).collect::<String>();
    tracing::info!(
        "validator set: {n_validators} validators (address-sorted); \
         signing as index {validator_index} address 0x{validator_address_hex}"
    );
    tracing::info!("target height fallback: {arg_height}, start round fallback: {arg_start_round}");

    let keypair = libp2p::identity::Keypair::generate_ed25519();
    let local_peer_id = PeerId::from(keypair.public());
    tracing::info!("proposal-peer local id: {local_peer_id}");

    let gossipsub_config = gossipsub::ConfigBuilder::default()
        .heartbeat_interval(Duration::from_secs(120))
        .validation_mode(gossipsub::ValidationMode::Strict)
        .build()
        .expect("gossipsub config");
    let proof_protocol = StreamProtocol::new(VALIDATOR_PROOF_PROTOCOL);
    let mut swarm = SwarmBuilder::with_existing_identity(keypair.clone())
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )
        .expect("tcp transport")
        .with_behaviour(|key| ProposalBehaviour {
            gossipsub: gossipsub::Behaviour::new(
                gossipsub::MessageAuthenticity::Signed(key.clone()),
                gossipsub_config,
            )
            .expect("gossipsub behaviour"),
            identify: identify::Behaviour::new(
                identify::Config::new(identify_protocol, key.public())
                    .with_agent_version("moniker=proposal-peer".to_string()),
            ),
            ping: ping::Behaviour::new(ping::Config::new()),
            validator_proof: request_response::Behaviour::with_codec(
                ProofCodec,
                [(proof_protocol, request_response::ProtocolSupport::Inbound)],
                request_response::Config::default(),
            ),
            broadcast: libp2p_scatter::Behaviour::new(
                libp2p_scatter::Config::default(),
            ),
        })
        .expect("behaviour")
        .build();

    swarm.dial(target.clone()).expect("dial target");
    tracing::info!("dialing {target}");

    let start = Instant::now();
    let mut connected = false;
    let mut phase = Phase::WaitReady;
    let mut phase_until: Option<Instant> = None;
    let mut observed: Option<(u64, u32)> = None;
    let mut observed_printed: Option<(u64, u32)> = None;
    let mut msgs_seen: u64 = 0;
    let mut parsed_seen: u64 = 0;

    // V1 plan, computed at the end of Observe.
    let mut target_height: u64 = arg_height;
    let mut v1_envelopes: Vec<Vec<u8>> = Vec::new();
    let mut v1_first_round: u64 = 0;
    let mut v1_last_round: u64 = 0;
    let mut v1_sent: usize = 0;
    let mut v1_started_at: Option<Instant> = None;
    let mut v2_sent: usize = 0;
    let mut v2_started_at: Option<Instant> = None;

    let mut ticker = tokio::time::interval(Duration::from_millis(100));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            event = swarm.next() => {
                match event {
                    Some(SwarmEvent::ConnectionEstablished { peer_id, .. }) => {
                        tracing::info!("connected to {peer_id}");
                        if !connected {
                            connected = true;
                            println!("CONNECTED peer={peer_id}");
                            phase_until = Some(Instant::now() + Duration::from_secs(3));
                        }
                    }
                    Some(SwarmEvent::ConnectionClosed { peer_id, cause, .. }) => {
                        tracing::error!("connection to {peer_id} closed: {cause:?}");
                        println!("FATAL connection closed cause={cause:?}");
                        std::process::exit(1);
                    }
                    Some(SwarmEvent::OutgoingConnectionError { peer_id, error, .. }) => {
                        tracing::error!("dial to {peer_id:?} failed: {error}");
                        println!("FATAL dial failed error={error}");
                        std::process::exit(1);
                    }
                    Some(SwarmEvent::Behaviour(event)) => {
                        match event {
                            ProposalBehaviourEvent::Identify(identify::Event::Received { peer_id, info, .. }) => {
                                tracing::info!("identify received from {peer_id}: protocol={:?} agent={:?}", info.protocol_version, info.agent_version);
                            }
                            ProposalBehaviourEvent::Identify(identify::Event::Sent { peer_id, .. }) => {
                                tracing::info!("identify sent to {peer_id}");
                            }
                            ProposalBehaviourEvent::Gossipsub(gossipsub::Event::Message { message, .. }) => {
                                msgs_seen += 1;
                                if let Some((h, r)) = parse_signed_message(&message.data) {
                                    parsed_seen += 1;
                                    let better = match observed {
                                        None => true,
                                        Some((oh, or_)) => (h, r) > (oh, or_),
                                    };
                                    if better {
                                        observed = Some((h, r));
                                    }
                                }
                            }
                            ProposalBehaviourEvent::Gossipsub(gossipsub::Event::Subscribed { peer_id, topic, .. }) => {
                                tracing::info!("peer {peer_id} subscribed to {topic}");
                            }
                            ProposalBehaviourEvent::ValidatorProof(request_response::Event::Message { peer, message, .. }) => {
                                if let request_response::Message::Request { request, channel, .. } = message {
                                    tracing::info!("validator proof received from {peer}: {} bytes (answering empty response)", request.len());
                                    if swarm.behaviour_mut().validator_proof.send_response(channel, ()).is_err() {
                                        tracing::warn!("failed to answer validator proof (channel closed)");
                                    }
                                }
                            }
                            ProposalBehaviourEvent::Broadcast(event) => {
                                tracing::info!("broadcast event: {event:?}");
                            }
                            _ => {}
                        }
                    }
                    Some(other) => {
                        tracing::debug!("swarm event: {other:?}");
                    }
                    None => {
                        tracing::error!("swarm stream ended");
                        println!("FATAL swarm ended");
                        std::process::exit(1);
                    }
                }
            }
            _ = ticker.tick() => {
                if !connected {
                    continue;
                }
                // Report newly observed max (height, round).
                if observed != observed_printed {
                    if let Some((h, r)) = observed {
                        println!("OBSERVED height={h} round={r} msgs_seen={msgs_seen} parsed={parsed_seen}");
                        tracing::info!("observed consensus height={h} round={r} ({msgs_seen} gossip msgs, {parsed_seen} parsed)");
                        observed_printed = observed;
                    }
                }
                match phase {
                    Phase::WaitReady => {
                        if let Some(t) = phase_until {
                            if Instant::now() >= t {
                                phase_until = None;
                                println!("READY");
                                phase = Phase::WaitJoin;
                            }
                        }
                    }
                    Phase::WaitJoin => {
                        for topic in &join_topics {
                            match swarm
                                .behaviour_mut()
                                .gossipsub
                                .subscribe(&gossipsub::IdentTopic::new(topic.clone()))
                            {
                                Ok(_) => tracing::info!("subscribed to real topic {topic}"),
                                Err(e) => {
                                    tracing::error!("failed to subscribe to real topic {topic}: {e}");
                                    println!("FATAL join failed topic={topic} error={e}");
                                    std::process::exit(1);
                                }
                            }
                        }
                        println!("JOINED topics={}", join_topics.len());
                        phase = Phase::JoinSettle;
                        phase_until = Some(Instant::now() + Duration::from_secs(join_settle_secs));
                    }
                    Phase::JoinSettle => {
                        if let Some(t) = phase_until {
                            if Instant::now() >= t {
                                phase_until = None;
                                phase = Phase::Observe;
                                phase_until = Some(Instant::now() + Duration::from_secs(observe_secs));
                                tracing::info!("join settle done, observing gossip for {observe_secs}s");
                            }
                        }
                    }
                    Phase::Observe => {
                        if let Some(t) = phase_until {
                            if Instant::now() >= t {
                                // --- decide target height / base round ---
                                // Mesh guard (run 2 lesson): if we received
                                // ZERO gossipsub messages in the whole
                                // observe window we are not in the node's
                                // mesh (e.g. wrong topic hash) — fail fast
                                // instead of dying later at publish time.
                                if msgs_seen == 0 {
                                    println!("FATAL no gossipsub messages received during observe window (mesh not established)");
                                    std::process::exit(1);
                                }
                                let mut base_round: u64 = arg_start_round as u64;
                                match observed {
                                    Some((oh, or_)) if oh > arg_height => {
                                        target_height = oh;
                                        base_round = or_ as u64;
                                        tracing::info!("adopting observed height {oh} (ahead of arg {arg_height})");
                                    }
                                    Some((oh, or_)) if oh == arg_height => {
                                        base_round = base_round.max(or_ as u64);
                                    }
                                    _ => {
                                        tracing::warn!("no usable gossip observation; using CLI height/round fallback");
                                    }
                                }
                                // First V1 round: >= base+11 and owned by our validator.
                                let mut r = base_round + 11;
                                while proposer_index(target_height, r, n_validators) != validator_index as u64 {
                                    r += 1;
                                }
                                v1_first_round = r;
                                // Precompute all V1 envelopes (sign once per round).
                                let t0 = Instant::now();
                                let mut round = r;
                                while v1_envelopes.len() < v1_count {
                                    if proposer_index(target_height, round, n_validators) == validator_index as u64 {
                                        let mut value = [0xABu8; 32];
                                        value[28..32].copy_from_slice(&(round as u32).to_le_bytes());
                                        let sign_bytes = ssz_proposal_bytes(
                                            target_height,
                                            round as u32,
                                            &value,
                                            None,
                                            validator_address,
                                        );
                                        let sig = signing_key.sign(&sign_bytes);
                                        let sig_bytes = sig.to_bytes();
                                        let proposal = encode_proposal(
                                            target_height,
                                            round as u32,
                                            &value,
                                            validator_address,
                                        );
                                        v1_envelopes.push(encode_signed_message(&proposal, &sig_bytes));
                                        v1_last_round = round;
                                    }
                                    round += 1;
                                }
                                println!(
                                    "TARGET height={target_height} base_round={base_round} first_round={v1_first_round} last_round={v1_last_round} validator_index={validator_index} validator_address=0x{validator_address_hex} precompute_ms={}",
                                    t0.elapsed().as_millis()
                                );
                                tracing::info!(
                                    "V1 plan: {v1_count} proposals, rounds {v1_first_round}..{v1_last_round} (step 5, ours only)"
                                );
                                phase = Phase::FloodV1;
                                v1_started_at = Some(Instant::now());
                            }
                        }
                    }
                    Phase::FloodV1 => {
                        let per_tick = rate.div_ceil(10).max(1);
                        let topic = gossipsub::IdentTopic::new(CONSENSUS_TOPIC.to_string());
                        for _ in 0..per_tick {
                            if v1_sent >= v1_envelopes.len() {
                                break;
                            }
                            let data = v1_envelopes[v1_sent].clone();
                            match swarm.behaviour_mut().gossipsub.publish(topic.clone(), data) {
                                Ok(_) => v1_sent += 1,
                                Err(e) => {
                                    tracing::error!("publish failed at v1_sent={v1_sent}: {e}");
                                    println!("FATAL publish failed at={v1_sent} error={e}");
                                    std::process::exit(1);
                                }
                            }
                        }
                        if v1_sent >= v1_envelopes.len() {
                            let elapsed = v1_started_at.map(|t| t.elapsed().as_secs_f64()).unwrap_or(0.0);
                            println!(
                                "V1_DONE sent={v1_sent} first_round={v1_first_round} last_round={v1_last_round} elapsed_s={elapsed:.1}"
                            );
                            tracing::info!("V1 complete: {v1_sent} proposals in {elapsed:.1}s");
                            phase = Phase::SettleV1;
                            phase_until = Some(Instant::now() + Duration::from_secs(v1_settle_secs));
                        }
                    }
                    Phase::SettleV1 => {
                        if let Some(t) = phase_until {
                            if Instant::now() >= t {
                                phase_until = None;
                                phase = Phase::FloodV2;
                                v2_started_at = Some(Instant::now());
                                tracing::info!("V1 settle done, starting V2 duplicate replay");
                            }
                        }
                    }
                    Phase::FloodV2 => {
                        if v1_envelopes.is_empty() {
                            println!("FATAL no V1 envelope to replay");
                            std::process::exit(1);
                        }
                        let per_tick = rate.div_ceil(10).max(1);
                        let topic = gossipsub::IdentTopic::new(CONSENSUS_TOPIC.to_string());
                        for _ in 0..per_tick {
                            if v2_sent >= v2_count {
                                break;
                            }
                            let data = v1_envelopes[0].clone();
                            match swarm.behaviour_mut().gossipsub.publish(topic.clone(), data) {
                                Ok(_) => v2_sent += 1,
                                Err(e) => {
                                    tracing::error!("publish failed at v2_sent={v2_sent}: {e}");
                                    println!("FATAL publish failed at={v2_sent} error={e}");
                                    std::process::exit(1);
                                }
                            }
                        }
                        if v2_sent >= v2_count {
                            let elapsed = v2_started_at.map(|t| t.elapsed().as_secs_f64()).unwrap_or(0.0);
                            println!("V2_DONE sent={v2_sent} elapsed_s={elapsed:.1}");
                            tracing::info!("V2 complete: {v2_sent} duplicate replays in {elapsed:.1}s");
                            phase = Phase::Hold;
                            phase_until = Some(Instant::now() + Duration::from_secs(hold_secs));
                        }
                    }
                    Phase::Hold => {
                        if let Some(t) = phase_until {
                            if Instant::now() >= t {
                                let total = v1_sent + v2_sent;
                                println!("DONE total_sent={total} elapsed_s={:.1}", start.elapsed().as_secs_f64());
                                tracing::info!("proposal flood complete: {total} sent in {:.1}s", start.elapsed().as_secs_f64());
                                std::process::exit(0);
                            }
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex_bytes(s: &str) -> Vec<u8> {
        hex_decode(s).expect("valid test hex")
    }

    /// SSZ vector mirrored from arc-node `crates/types/src/proposal.rs`
    /// test_proposal_ssz_encode_decode: height 100, round 5, pol_round Some(3),
    /// value 0xAA*32, address 0xBB*20. Fixed part 68 bytes, offsets 68 and 73.
    #[test]
    fn ssz_proposal_with_pol_round() {
        let got = ssz_proposal_bytes(100, 5, &[0xAA; 32], Some(3), &[0xBB; 20]);
        let mut exp = Vec::new();
        exp.extend_from_slice(&100u64.to_le_bytes());
        exp.extend_from_slice(&68u32.to_le_bytes());
        exp.extend_from_slice(&[0xAA; 32]);
        exp.extend_from_slice(&73u32.to_le_bytes());
        exp.extend_from_slice(&[0xBB; 20]);
        exp.extend_from_slice(&[1, 5, 0, 0, 0]); // round Some(5)
        exp.extend_from_slice(&[1, 3, 0, 0, 0]); // pol_round Some(3)
        assert_eq!(got.len(), 78);
        assert_eq!(got, exp);
    }

    /// Same proposal with pol_round Nil: 74 bytes, pol encoding is one 0x00.
    #[test]
    fn ssz_proposal_nil_pol_round() {
        let got = ssz_proposal_bytes(100, 5, &[0xAA; 32], None, &[0xBB; 20]);
        let mut exp = Vec::new();
        exp.extend_from_slice(&100u64.to_le_bytes());
        exp.extend_from_slice(&68u32.to_le_bytes());
        exp.extend_from_slice(&[0xAA; 32]);
        exp.extend_from_slice(&73u32.to_le_bytes());
        exp.extend_from_slice(&[0xBB; 20]);
        exp.extend_from_slice(&[1, 5, 0, 0, 0]); // round Some(5)
        exp.push(0); // pol_round Nil
        assert_eq!(got.len(), 74);
        assert_eq!(got, exp);
    }

    /// Exact protobuf envelope bytes for height 100, round 5, value 0xAA*32,
    /// address 0xBB*20, signature 0xCC*64 (hand-computed from consensus.proto).
    #[test]
    fn protobuf_envelope_exact_bytes() {
        let proposal = encode_proposal(100, 5, &[0xAA; 32], &[0xBB; 20]);
        let envelope = encode_signed_message(&proposal, &[0xCC; 64]);
        let mut exp_proposal = Vec::new();
        exp_proposal.extend_from_slice(&[0x08, 0x64]); // height = 100
        exp_proposal.extend_from_slice(&[0x10, 0x05]); // round = 5
        exp_proposal.extend_from_slice(&[0x1a, 0x22, 0x0a, 0x20]); // value{block_hash}
        exp_proposal.extend_from_slice(&[0xAA; 32]);
        exp_proposal.extend_from_slice(&[0x2a, 0x16, 0x0a, 0x14]); // validator_address{value}
        exp_proposal.extend_from_slice(&[0xBB; 20]);
        assert_eq!(proposal, exp_proposal);
        let mut exp = Vec::new();
        exp.push(0x01); // version V1
        exp.extend_from_slice(&[0x0a, 0x40]); // SignedMessage.proposal, len 64
        exp.extend_from_slice(&exp_proposal);
        exp.extend_from_slice(&[0x1a, 0x42, 0x0a, 0x40]); // signature{bytes}, len 66/64
        exp.extend_from_slice(&[0xCC; 64]);
        assert_eq!(envelope.len(), 135);
        assert_eq!(envelope, exp);
        // And the parser must recover (height, round) from our own envelope.
        assert_eq!(parse_signed_message(&envelope), Some((100, 5)));
    }

    /// Vote parsing: hand-built SignedMessage{Vote{height=7, round=9}}.
    #[test]
    fn parse_vote_message() {
        // Vote: field1 vote_type=1 (varint), field2 height=7, field3 round=9.
        let mut vote = Vec::new();
        put_varint_field(1, 1, &mut vote);
        put_varint_field(2, 7, &mut vote);
        put_varint_field(3, 9, &mut vote);
        let mut env = vec![0x01];
        put_len_field(2, &vote, &mut env);
        assert_eq!(parse_signed_message(&env), Some((7, 9)));
    }

    #[test]
    fn proposer_round_robin() {
        // height 121, n=5: round r is ours (index 1) iff (120 + r) % 5 == 1.
        assert_eq!(proposer_index(121, 6, 5), 1);
        assert_eq!(proposer_index(121, 11, 5), 1);
        assert_eq!(proposer_index(121, 5, 5), 0);
    }

    #[test]
    fn hex_decode_roundtrip() {
        assert_eq!(hex_bytes("00ff10"), vec![0x00, 0xff, 0x10]);
        assert_eq!(hex_decode("zz"), None);
    }
}
