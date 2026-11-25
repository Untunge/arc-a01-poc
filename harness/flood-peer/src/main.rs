//! flood-peer — a plain libp2p peer (NO validator key) that subscribes to a large
//! number of distinct gossipsub topics on a target node.
//!
//! Purpose: PoC for Arc A-01 vector (c) — malachite v0.8.0 uses plain
//! `gossipsub::Behaviour::new` (default `AllowAllSubscriptionFilter`), so every
//! SUBSCRIBE a connected peer sends is recorded in that peer's unbounded
//! `PeerDetails.topics: BTreeSet<TopicHash>` set. This peer demonstrates that
//! growth and measures it at several subscription levels.
//!
//! To stay connected long enough to flood, the peer presents itself the way an
//! ordinary compatible arc peer does (all verified in malachite v0.8.0 source):
//! - identify protocol version = the node's consensus protocol
//!   (`/malachitebft-core-consensus/v1beta1` on localdev), otherwise the node
//!   logs "incompatible protocol version" and never registers the peer
//!   (`network/src/lib.rs` identify handler);
//! - a validator-proof `request_response` responder for
//!   `/malachitebft-validator-proof/v1` (uvi-length-prefixed proof bytes in,
//!   empty response out — same wire format as
//!   `network/src/validator_proof/codec.rs`), because the node sends its proof
//!   on first connection and the send otherwise fails with "remote supports
//!   none of the requested protocols". The peer NEVER sends a proof: it holds
//!   no validator key, so it can never be classified as a validator;
//! - it first subscribes to a real topic (`/consensus`), which puts both sides
//!   in the same gossipsub mesh — the gossipsub handler only keeps a
//!   connection alive while `in_mesh`.
//!
//! Usage:
//!   flood-peer --target /ip4/127.0.0.1/tcp/27000/p2p/<node PeerId>
//!              --levels 5000,20000,50000 --rate 1000 --settle-secs 10 --hold-secs 60
//!
//! Stdout protocol (parsed by run-flood.sh):
//!   CONNECTED peer=<PeerId>
//!   READY
//!   JOINED topics=<n>
//!   LEVEL_DONE level=<cumulative> sent=<cumulative> elapsed_s=<secs since start>
//!   DONE total_sent=<N> elapsed_s=<secs>
//!   FATAL <reason>            (non-zero exit)
//!
//! Design notes:
//! - Same libp2p (0.56.0) / libp2p-gossipsub (0.49.4) versions as the target,
//!   so protocol negotiation (/meshsub/1.2.0) and RPC encoding match exactly.
//! - Subscriptions go through the public `gossipsub::Behaviour::subscribe` API:
//!   one SUBSCRIBE RPC per topic, sent immediately to all connected peers —
//!   exactly what a real peer advertising many topics does on the wire.
//! - Flood-side heartbeat is stretched to 120 s: with 50k subscribed topics the
//!   1 s default heartbeat would iterate all topics every second for no benefit.
//! - Tracing goes to stderr (RUST_LOG env, default info); stdout carries only
//!   the machine-readable markers above.

use std::env;
use std::io;
use std::time::{Duration, Instant};

use futures::StreamExt;
use libp2p::{
    gossipsub, identify, noise, ping, request_response, swarm::NetworkBehaviour,
    swarm::SwarmEvent, tcp, yamux, Multiaddr, PeerId, StreamProtocol, SwarmBuilder,
};
use libp2p::futures::io::{AsyncRead, AsyncReadExt, AsyncWrite};

/// The node's consensus protocol name on localdev (malachite defaults).
/// See arc-node `crates/malachite-app/src/hardcoded_config.rs::p2p::protocol_names`.
const DEFAULT_IDENTIFY_PROTOCOL: &str = "/malachitebft-core-consensus/v1beta1";
/// The node's validator-proof protocol name on localdev.
const VALIDATOR_PROOF_PROTOCOL: &str = "/malachitebft-validator-proof/v1";
/// Max proof bytes the node's codec accepts.
const MAX_PROOF_SIZE: usize = 1024;

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
        // Unsigned-varint length prefix.
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
        // Never called: this peer never sends a proof.
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
        // Zero-length response, exactly like the node's codec.
        Ok(())
    }
}

#[derive(NetworkBehaviour)]
struct FloodBehaviour {
    gossipsub: gossipsub::Behaviour,
    identify: identify::Behaviour,
    ping: ping::Behaviour,
    validator_proof: request_response::Behaviour<ProofCodec>,
}

fn usage() -> ! {
    eprintln!(
        "usage: flood-peer --target <multiaddr> [--levels 5000,20000,50000] \
         [--rate 1000] [--settle-secs 10] [--hold-secs 60] \
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

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
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
    let levels: Vec<usize> = arg_value(&args, "--levels", "5000,20000,50000")
        .split(',')
        .map(|s| s.trim().parse::<usize>().unwrap_or_else(|_| usage()))
        .collect();
    let rate: usize = arg_value(&args, "--rate", "1000")
        .parse()
        .unwrap_or_else(|_| usage());
    let settle_secs: u64 = arg_value(&args, "--settle-secs", "10")
        .parse()
        .unwrap_or_else(|_| usage());
    let hold_secs: u64 = arg_value(&args, "--hold-secs", "60")
        .parse()
        .unwrap_or_else(|_| usage());
    let identify_protocol =
        arg_value(&args, "--identify-protocol", DEFAULT_IDENTIFY_PROTOCOL);
    let join_topics: Vec<String> = arg_value(&args, "--join-topics", "/consensus")
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    let join_settle_secs: u64 = arg_value(&args, "--join-settle-secs", "5")
        .parse()
        .unwrap_or_else(|_| usage());
    if levels.is_empty() || levels.windows(2).any(|w| w[0] >= w[1]) || rate == 0 {
        usage();
    }

    let keypair = libp2p::identity::Keypair::generate_ed25519();
    let local_peer_id = PeerId::from(keypair.public());
    tracing::info!("flood-peer local id: {local_peer_id}");
    tracing::info!("identify protocol: {identify_protocol}, join topics: {join_topics:?}");

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
        .with_behaviour(|key| FloodBehaviour {
            gossipsub: gossipsub::Behaviour::new(
                gossipsub::MessageAuthenticity::Signed(key.clone()),
                gossipsub_config,
            )
            .expect("gossipsub behaviour"),
            identify: identify::Behaviour::new(
                identify::Config::new(identify_protocol, key.public())
                    .with_agent_version("moniker=flood-peer".to_string()),
            ),
            ping: ping::Behaviour::new(ping::Config::new()),
            validator_proof: request_response::Behaviour::with_codec(
                ProofCodec,
                [(proof_protocol, request_response::ProtocolSupport::Inbound)],
                request_response::Config::default(),
            ),
        })
        .expect("behaviour")
        .build();

    swarm.dial(target.clone()).expect("dial target");
    tracing::info!("dialing {target}");

    let start = Instant::now();
    let mut connected = false;
    let mut ready_at: Option<Instant> = None;
    let mut joined = false;
    let mut join_settle_until: Option<Instant> = None;
    let mut subscribed: usize = 0;
    let mut level_idx: usize = 0;
    let mut settle_until: Option<Instant> = None;
    let mut hold_until: Option<Instant> = None;
    let mut done_printed = false;

    // 10 ticks/second; each tick subscribes up to rate/10 topics.
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
                            // Give identify + gossipsub negotiation a moment before
                            // the first SUBSCRIBE (subscribe() opens the substream
                            // lazily, so this is just belt-and-braces).
                            ready_at = Some(Instant::now() + Duration::from_secs(3));
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
                            FloodBehaviourEvent::Identify(identify::Event::Received { peer_id, info, .. }) => {
                                tracing::info!("identify received from {peer_id}: protocol={:?} agent={:?}", info.protocol_version, info.agent_version);
                            }
                            FloodBehaviourEvent::Identify(identify::Event::Sent { peer_id, .. }) => {
                                tracing::info!("identify sent to {peer_id}");
                            }
                            FloodBehaviourEvent::Gossipsub(gossipsub::Event::Subscribed { peer_id, topic, .. }) => {
                                tracing::info!("peer {peer_id} subscribed to {topic}");
                            }
                            FloodBehaviourEvent::Gossipsub(gossipsub::Event::Unsubscribed { peer_id, topic, .. }) => {
                                tracing::info!("peer {peer_id} unsubscribed from {topic}");
                            }
                            FloodBehaviourEvent::Gossipsub(gossipsub::Event::Message { message, .. }) => {
                                tracing::debug!("gossipsub message on {}", message.topic);
                            }
                            FloodBehaviourEvent::ValidatorProof(request_response::Event::Message { peer, message, .. }) => {
                                match message {
                                    request_response::Message::Request { request, channel, .. } => {
                                        tracing::info!("validator proof received from {peer}: {} bytes (answering empty response, never sending our own proof)", request.len());
                                        if swarm.behaviour_mut().validator_proof.send_response(channel, ()).is_err() {
                                            tracing::warn!("failed to answer validator proof (channel closed)");
                                        }
                                    }
                                    request_response::Message::Response { .. } => {
                                        tracing::debug!("validator proof response from {peer} (unexpected)");
                                    }
                                }
                            }
                            FloodBehaviourEvent::ValidatorProof(request_response::Event::OutboundFailure { peer, error, .. }) => {
                                tracing::warn!("validator proof outbound failure to {peer}: {error}");
                            }
                            FloodBehaviourEvent::ValidatorProof(request_response::Event::InboundFailure { peer, error, .. }) => {
                                tracing::warn!("validator proof inbound failure from {peer}: {error}");
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
                if let Some(t) = ready_at {
                    if Instant::now() < t {
                        continue;
                    }
                    ready_at = None;
                    println!("READY");
                    tracing::info!("ready, joining real topics first: {join_topics:?}");
                }
                if !joined {
                    // Join the node's real topics first: shared mesh membership
                    // keeps the connection alive on both sides (gossipsub
                    // keep-alive is true only while in_mesh).
                    for topic in &join_topics {
                        match swarm
                            .behaviour_mut()
                            .gossipsub
                            .subscribe(&gossipsub::Sha256Topic::new(topic.clone()))
                        {
                            Ok(_) => tracing::info!("subscribed to real topic {topic}"),
                            Err(e) => {
                                tracing::error!("failed to subscribe to real topic {topic}: {e}");
                                println!("FATAL join failed topic={topic} error={e}");
                                std::process::exit(1);
                            }
                        }
                    }
                    joined = true;
                    println!("JOINED topics={}", join_topics.len());
                    join_settle_until = Some(Instant::now() + Duration::from_secs(join_settle_secs));
                    continue;
                }
                if let Some(until) = join_settle_until {
                    if Instant::now() < until {
                        continue;
                    }
                    join_settle_until = None;
                    tracing::info!("join settle done, starting flood");
                }
                if let Some(until) = settle_until {
                    if Instant::now() < until {
                        continue;
                    }
                    settle_until = None;
                }
                if let Some(until) = hold_until {
                    if Instant::now() >= until && !done_printed {
                        done_printed = true;
                        println!("DONE total_sent={} elapsed_s={:.1}", subscribed, start.elapsed().as_secs_f64());
                        tracing::info!("flood complete: {subscribed} subscriptions in {:.1}s", start.elapsed().as_secs_f64());
                        std::process::exit(0);
                    }
                    continue;
                }
                if level_idx >= levels.len() {
                    hold_until = Some(Instant::now() + Duration::from_secs(hold_secs));
                    continue;
                }
                let target_n = levels[level_idx];
                let per_tick = rate.div_ceil(10).max(1);
                let batch = per_tick.min(target_n.saturating_sub(subscribed));
                for _ in 0..batch {
                    let topic = format!("flood/{subscribed:06}");
                    match swarm
                        .behaviour_mut()
                        .gossipsub
                        .subscribe(&gossipsub::Sha256Topic::new(topic))
                    {
                        Ok(_) => subscribed += 1,
                        Err(e) => {
                            tracing::error!("subscribe failed at {subscribed}: {e}");
                            println!("FATAL subscribe failed at={subscribed} error={e}");
                            std::process::exit(1);
                        }
                    }
                }
                if subscribed >= target_n {
                    println!(
                        "LEVEL_DONE level={} sent={} elapsed_s={:.1}",
                        target_n,
                        subscribed,
                        start.elapsed().as_secs_f64()
                    );
                    tracing::info!("level {target_n} reached ({subscribed} sent)");
                    level_idx += 1;
                    if level_idx >= levels.len() {
                        hold_until = Some(Instant::now() + Duration::from_secs(hold_secs));
                    } else {
                        settle_until = Some(Instant::now() + Duration::from_secs(settle_secs));
                    }
                }
            }
        }
    }
}
