use anyhow::Context;
use clap::Parser;
use std::path::PathBuf;
use std::sync::Arc;

mod auth;
mod azure;
mod cache;
mod cluster;
mod config;
mod error;
mod fetcher;
mod fuse_fs;
mod nic;
mod stats;
mod transport;

use crate::auth::Credential;
use crate::azure::BlobClient;
use crate::cache::DiskCache;
use crate::cluster::{Membership, NodeInfo, NodeState};
use crate::config::Config;
use crate::fetcher::Fetcher;
use crate::stats::Stats;
use crate::transport::PeerService;

#[derive(Parser, Debug)]
#[command(name = "blobcached", about = "Distributed FUSE blob cache daemon")]
struct Args {
    #[arg(short, long)]
    config: PathBuf,
    #[arg(long, default_value = "info")]
    log: String,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    init_tracing(&args.log);

    let cfg = Config::from_path(&args.config).context("load config")?;
    tracing::info!(?cfg.cache.dir, "starting blobcached");

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("blobcached")
        .build()?;

    let stats = Stats::new();
    let cache = DiskCache::open(cfg.cache.dir.clone(), cfg.cache.max_bytes)?;

    let cred = Credential::resolve(&cfg.mounts[0].account, cfg.mounts[0].sas_token.as_deref())
        .context("resolve credentials")?;
    tracing::info!("credential resolved: {}", match &cred {
        Credential::SharedKey { .. } => "SharedKey",
        Credential::Sas { .. } => "SAS",
        Credential::Bearer { .. } => "Bearer (managed identity)",
        Credential::Anonymous => "Anonymous",
    });
    let blob = Arc::new(BlobClient::new(cred)?);

    let cluster_hash = cfg.cluster_hash();
    let cluster_hash_hex = hex32(&cluster_hash);
    tracing::info!(cluster_hash = %cluster_hash_hex, "config hash computed");

    let node_id = cfg.node_id.clone().unwrap_or_else(|| {
        hostname().unwrap_or_else(|| format!("node-{}", std::process::id()))
    });

    let advertise = cfg.transport.advertise.first().cloned().unwrap_or_else(|| {
        let bind = &cfg.transport.bind;
        if let Some((host, port)) = bind.rsplit_once(':') {
            if host == "0.0.0.0" || host.is_empty() {
                let local = nic::enumerate(true).into_iter()
                    .filter(|a| matches!(a.ip, std::net::IpAddr::V4(_)))
                    .next();
                match local {
                    Some(a) => format!("http://{}:{}", a.ip, port),
                    None => format!("http://127.0.0.1:{port}"),
                }
            } else {
                format!("http://{bind}")
            }
        } else { format!("http://{bind}") }
    });

    let me = NodeInfo {
        id: node_id.clone(),
        transport_url: advertise.clone(),
        cluster_hash: cluster_hash_hex.clone(),
        last_seen_unix: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs(),
        state: NodeState::Alive,
        incarnation: 1,
    };
    let membership = Membership::new(me, stats.cluster_stats.clone());

    let fetcher = Arc::new(Fetcher::new(
        cache.clone(), blob.clone(), membership.clone(),
        stats.clone(), cfg.cache.chunk_size,
    ));

    let peer_svc = PeerService::new(cache.clone(), cluster_hash, stats.peer_stats.clone());
    let transport_addr: std::net::SocketAddr = cfg.transport.bind.parse()
        .context(format!("parse transport.bind {}", cfg.transport.bind))?;
    let cluster_addr: std::net::SocketAddr = cfg.cluster.bind.parse()
        .context(format!("parse cluster.bind {}", cfg.cluster.bind))?;
    let stats_addr: std::net::SocketAddr = cfg.stats.bind.parse()
        .context(format!("parse stats.bind {}", cfg.stats.bind))?;

    rt.spawn({
        let svc = peer_svc.clone();
        async move {
            if let Err(e) = svc.serve(transport_addr).await {
                tracing::error!(error = %e, "peer transport server died");
            }
        }
    });
    rt.spawn({
        let m = membership.clone();
        let s = cluster::GossipServer::new(m);
        async move {
            if let Err(e) = s.serve(cluster_addr).await {
                tracing::error!(error = %e, "gossip server died");
            }
        }
    });
    rt.spawn(cluster::run_gossip_loop(membership.clone(), cfg.cluster.seeds.clone()));
    rt.spawn(stats::serve(stats.clone(), stats_addr, cache.clone(), membership.clone()));

    let handle = rt.handle().clone();
    let mut sessions = Vec::new();
    for mount in &cfg.mounts {
        std::fs::create_dir_all(&mount.mountpoint)
            .with_context(|| format!("create mountpoint {}", mount.mountpoint.display()))?;
        let fs = fuse_fs::BlobFs::new(mount.clone(), blob.clone(), fetcher.clone(), handle.clone());
        let opts = vec![
            fuser::MountOption::FSName(format!("blobcache-{}", mount.name)),
            fuser::MountOption::Subtype("blobcache".into()),
            fuser::MountOption::AutoUnmount,
            fuser::MountOption::AllowOther,
            fuser::MountOption::DefaultPermissions,
            fuser::MountOption::RO,
        ];
        let session = fuser::spawn_mount2(fs, &mount.mountpoint, &opts)
            .with_context(|| format!("mount {} at {}", mount.name, mount.mountpoint.display()))?;
        tracing::info!(mount=%mount.name, mountpoint=%mount.mountpoint.display(), "mounted");
        sessions.push(session);
    }

    rt.block_on(wait_signal());
    tracing::info!("shutting down");
    drop(sessions);
    Ok(())
}

async fn wait_signal() {
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("sigterm");
    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()).expect("sigint");
    tokio::select! { _ = sigterm.recv() => {}, _ = sigint.recv() => {} }
}

fn init_tracing(level: &str) {
    use tracing_subscriber::{fmt, EnvFilter};
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level));
    fmt().with_env_filter(filter).with_target(false).init();
}

fn hostname() -> Option<String> {
    std::fs::read_to_string("/etc/hostname").ok().map(|s| s.trim().to_string()).filter(|s| !s.is_empty())
}

fn hex32(b: &[u8; 32]) -> String {
    const C: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(64);
    for x in b { s.push(C[(x >> 4) as usize] as char); s.push(C[(x & 0xf) as usize] as char); }
    s
}
