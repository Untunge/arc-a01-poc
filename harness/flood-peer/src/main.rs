//! flood-peer — a plain libp2p peer (NO validator key) that subscribes to a large
//! number of distinct gossipsub topics on a target node.
//!
//! Purpose: PoC for Arc A-01 vector (c) — malachite v0.8.0 uses plain
//! `gossipsub::Behaviour::new` (default `AllowAllSubscriptionFilter`), so every
//! SUBSCRIBE a connected peer sends is recorded in that peer's unbounded
//! `PeerDetails.topics: BTreeSet<TopicHash>` set. This peer demonstrates that
//! growth and measures it at several subscription levels.
//!
//! Usage:
//!   flood-peer --target /ip4/127.0.0.1/tcp/27000/p2p/<node PeerId>
//!              --levels 5000,20000,50000 --rate 1000 --settle-secs 10 --hold-secs 60
//!
//! Stdout protocol (parsed by run-flood.sh):
//!   CONNECTED peer=<PeerId>
//!   READY
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
//!   1 s default heartbeat would iterate all topics every second for no benefit
//!   (the node never subscribes to flood topics, so no mesh ever forms).
//! - The peer never presents a validator proof and holds no validator key.

use std::env;
use std::time::{Duration, Instant};

use futures::StreamExt;
use libp2p::{
    gossipsub, identify, noise, ping, swarm::NetworkBehaviour, swarm::SwarmEvent, tcp, yamux,
    Multiaddr, PeerId, SwarmBuilder,
};

#[derive(NetworkBehaviour)]
struct FloodBehaviour {
    gossipsub: gossipsub::Behaviour,
    identify: identify::Behaviour,
    ping: ping::Behaviour,
}

fn usage() -> ! {
    eprintln!(
        "usage: flood-peer --target <multiaddr> [--levels 5000,20000,50000] \
         [--rate 1000] [--settle-secs 10] [--hold-secs 60]"
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
    if levels.is_empty() || levels.windows(2).any(|w| w[0] >= w[1]) || rate == 0 {
        usage();
    }

    let keypair = libp2p::identity::Keypair::generate_ed25519();
    let local_peer_id = PeerId::from(keypair.public());
    eprintln!("flood-peer local id: {local_peer_id}");

    let gossipsub_config = gossipsub::ConfigBuilder::default()
        .heartbeat_interval(Duration::from_secs(120))
        .validation_mode(gossipsub::ValidationMode::Strict)
        .build()
        .expect("gossipsub config");
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
                identify::Config::new("flood-peer/1.0.0".to_string(), key.public()),
            ),
            ping: ping::Behaviour::new(ping::Config::new()),
        })
        .expect("behaviour")
        .build();

    swarm.dial(target.clone()).expect("dial target");
    eprintln!("dialing {target}");

    let start = Instant::now();
    let mut connected = false;
    let mut ready_at: Option<Instant> = None;
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
                        eprintln!("connected to {peer_id}");
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
                        eprintln!("FATAL connection to {peer_id} closed: {cause:?}");
                        println!("FATAL connection closed cause={cause:?}");
                        std::process::exit(1);
                    }
                    Some(SwarmEvent::OutgoingConnectionError { peer_id, error, .. }) => {
                        eprintln!("FATAL dial to {peer_id:?} failed: {error}");
                        println!("FATAL dial failed error={error}");
                        std::process::exit(1);
                    }
                    Some(_) => {}
                    None => {
                        eprintln!("FATAL swarm stream ended");
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
                        eprintln!("flood complete: {subscribed} subscriptions in {:.1}s", start.elapsed().as_secs_f64());
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
                        .subscribe(&gossipsub::Topic::new(topic))
                    {
                        Ok(_) => subscribed += 1,
                        Err(e) => {
                            eprintln!("FATAL subscribe failed at {subscribed}: {e}");
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
                    eprintln!("level {target_n} reached ({subscribed} sent)");
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
