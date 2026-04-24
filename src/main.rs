use anyhow::Context;
#[cfg(feature = "ucx")]
use base64::prelude::{Engine as _, BASE64_STANDARD};
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
#[cfg(feature = "ucx")]
mod transport_ucx;

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

    // C4: resolve credentials per mount. Each mount may target a different
    // account/container with its own auth (SAS for one, MSI bearer for
    // another); a shared client would attach the wrong Authorization header.
    let mut blobs: std::collections::HashMap<String, Arc<BlobClient>> =
        std::collections::HashMap::new();
    for m in &cfg.mounts {
        let cred = Credential::resolve(&m.account, m.sas_token.as_deref())
            .with_context(|| format!("resolve credentials for mount {}", m.name))?;
        tracing::info!(
            mount = %m.name,
            account = %m.account,
            credential = %match &cred {
                Credential::SharedKey { .. } => "SharedKey",
                Credential::Sas { .. } => "SAS",
                Credential::Bearer(_) => "Bearer (managed identity)",
                Credential::Anonymous => "Anonymous",
            },
            "credential resolved"
        );
        blobs.insert(m.name.clone(), Arc::new(BlobClient::new(cred)?));
    }
    let blobs = Arc::new(blobs);

    let cluster_hash = cfg.cluster_hash();
    let cluster_hash_hex = hex32(&cluster_hash);
    tracing::info!(cluster_hash = %cluster_hash_hex, "config hash computed");

    let node_id = cfg
        .node_id
        .clone()
        .unwrap_or_else(|| hostname().unwrap_or_else(|| format!("node-{}", std::process::id())));

    let advertise = cfg.transport.advertise.first().cloned().unwrap_or_else(|| {
        let bind = &cfg.transport.bind;
        if let Some((host, port)) = bind.rsplit_once(':') {
            if host == "0.0.0.0" || host.is_empty() {
                let local = nic::enumerate(true)
                    .into_iter()
                    .filter(|a| matches!(a.ip, std::net::IpAddr::V4(_)))
                    .next();
                match local {
                    Some(a) => format!("http://{}:{}", a.ip, port),
                    None => format!("http://127.0.0.1:{port}"),
                }
            } else {
                format!("http://{bind}")
            }
        } else {
            format!("http://{bind}")
        }
    });

    let gossip_advertise = cfg.cluster.advertise.clone().unwrap_or_else(|| {
        let bind = &cfg.cluster.bind;
        if let Some((host, port)) = bind.rsplit_once(':') {
            if host == "0.0.0.0" || host.is_empty() {
                let local = nic::enumerate(true)
                    .into_iter()
                    .filter(|a| matches!(a.ip, std::net::IpAddr::V4(_)))
                    .next();
                match local {
                    Some(a) => format!("http://{}:{}", a.ip, port),
                    None => format!("http://127.0.0.1:{port}"),
                }
            } else {
                format!("http://{bind}")
            }
        } else {
            format!("http://{bind}")
        }
    });

    #[cfg(feature = "ucx")]
    let mut rdma_bootstrap = None;

    let ucx_worker_addr_b64 = match cfg.transport.kind.as_str() {
        "tcp" => None,
        "rdma" => {
            #[cfg(feature = "ucx")]
            {
                let transport_addr: std::net::SocketAddr = cfg
                    .transport
                    .bind
                    .parse()
                    .context(format!("parse transport.bind {}", cfg.transport.bind))?;
                let (service, client, worker_addr_blob) =
                    crate::transport_ucx::RdmaPeerService::start(
                        cache.clone(),
                        transport_addr,
                        stats.peer_stats.clone(),
                        node_id.clone(),
                        8 + cfg.cache.chunk_size as usize,
                        cfg.transport.chunk_concurrency as usize,
                    )
                    .context("start RDMA peer service")?;
                let encoded = BASE64_STANDARD.encode(worker_addr_blob.as_slice());
                rdma_bootstrap = Some((service, client));
                Some(encoded)
            }
            #[cfg(not(feature = "ucx"))]
            {
                anyhow::bail!("transport.kind=\"rdma\" requires --features ucx at build time");
            }
        }
        other => anyhow::bail!("unknown transport.kind {other:?}"),
    };

    let me = NodeInfo {
        id: node_id.clone(),
        transport_url: advertise.clone(),
        gossip_url: gossip_advertise.clone(),
        cluster_hash: cluster_hash_hex.clone(),
        ucx_worker_addr_b64,
        last_seen_unix: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
        state: NodeState::Alive,
        incarnation: 1,
    };
    #[allow(unused_mut)]
    let mut membership = Membership::new(me, stats.cluster_stats.clone());

    let peers: Arc<crate::transport::PeerClient> = match cfg.transport.kind.as_str() {
        "tcp" => Arc::new(crate::transport::PeerClient::tcp()),
        "rdma" => {
            #[cfg(feature = "ucx")]
            {
                let (_service, client) = rdma_bootstrap
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("missing RDMA bootstrap state"))?;
                Arc::new(crate::transport::PeerClient::rdma(client.clone()))
            }
            #[cfg(not(feature = "ucx"))]
            {
                anyhow::bail!("transport.kind=\"rdma\" requires --features ucx at build time");
            }
        }
        other => anyhow::bail!("unknown transport.kind {other:?}"),
    };

    #[cfg(feature = "ucx")]
    if matches!(cfg.transport.kind.as_str(), "rdma") {
        let peers_for_hook = peers.clone();
        membership.set_rdma_peer_update_hook(move |node| {
            let Some(encoded) = &node.ucx_worker_addr_b64 else {
                return;
            };
            let decoded = match BASE64_STANDARD.decode(encoded) {
                Ok(decoded) => decoded,
                Err(e) => {
                    tracing::warn!(peer = %node.id, error = %e, "invalid UCX worker address in gossip payload");
                    return;
                }
            };
            if let Err(e) = peers_for_hook.update_peer(&node.id, &decoded) {
                tracing::warn!(peer = %node.id, error = %e, "failed to update RDMA peer address from gossip");
            }
        });
    }

    tracing::info!(kind = %cfg.transport.kind, "peer transport client initialised");

    let fetcher = Arc::new(Fetcher::new(
        cache.clone(),
        blobs.clone(),
        peers.clone(),
        membership.clone(),
        stats.clone(),
        cfg.cache.chunk_size,
        cfg.transport.chunk_concurrency,
    ));

    let transport_addr: std::net::SocketAddr = cfg
        .transport
        .bind
        .parse()
        .context(format!("parse transport.bind {}", cfg.transport.bind))?;
    let cluster_addr: std::net::SocketAddr = cfg
        .cluster
        .bind
        .parse()
        .context(format!("parse cluster.bind {}", cfg.cluster.bind))?;
    let stats_addr: std::net::SocketAddr = cfg
        .stats
        .bind
        .parse()
        .context(format!("parse stats.bind {}", cfg.stats.bind))?;

    match cfg.transport.kind.as_str() {
        "tcp" => {
            let peer_svc = PeerService::new(cache.clone(), cluster_hash, stats.peer_stats.clone());
            rt.spawn({
                let svc = peer_svc.clone();
                async move {
                    if let Err(e) = svc.serve(transport_addr).await {
                        tracing::error!(error = %e, "peer transport server died");
                    }
                }
            });
        }
        "rdma" => {
            #[cfg(feature = "ucx")]
            {
                let _ = transport_addr;
            }
            #[cfg(not(feature = "ucx"))]
            {
                anyhow::bail!("transport.kind=\"rdma\" requires --features ucx at build time");
            }
        }
        _ => unreachable!(),
    }
    rt.spawn({
        let m = membership.clone();
        let s = cluster::GossipServer::new(m);
        async move {
            if let Err(e) = s.serve(cluster_addr).await {
                tracing::error!(error = %e, "gossip server died");
            }
        }
    });
    rt.spawn(cluster::run_gossip_loop(
        membership.clone(),
        cfg.cluster.seeds.clone(),
    ));
    rt.spawn(stats::serve(
        stats.clone(),
        stats_addr,
        cache.clone(),
        membership.clone(),
    ));

    let handle = rt.handle().clone();
    let mut sessions = Vec::new();
    for mount in &cfg.mounts {
        std::fs::create_dir_all(&mount.mountpoint)
            .with_context(|| format!("create mountpoint {}", mount.mountpoint.display()))?;
        let blob = blobs
            .get(&mount.name)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("missing blob client for mount {}", mount.name))?;
        let fs = fuse_fs::BlobFs::new(mount.clone(), blob, fetcher.clone(), handle.clone());
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
    #[cfg(feature = "ucx")]
    drop(rdma_bootstrap);
    Ok(())
}

async fn wait_signal() {
    let mut sigterm =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("sigterm");
    let mut sigint =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()).expect("sigint");
    tokio::select! { _ = sigterm.recv() => {}, _ = sigint.recv() => {} }
}

fn init_tracing(level: &str) {
    use tracing_subscriber::{fmt, EnvFilter};
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level));
    fmt().with_env_filter(filter).with_target(false).init();
}

fn hostname() -> Option<String> {
    std::fs::read_to_string("/etc/hostname")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn hex32(b: &[u8; 32]) -> String {
    const C: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(64);
    for x in b {
        s.push(C[(x >> 4) as usize] as char);
        s.push(C[(x & 0xf) as usize] as char);
    }
    s
}
