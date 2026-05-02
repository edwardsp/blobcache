use anyhow::Context;
#[cfg(feature = "ucx")]
use base64::prelude::{Engine as _, BASE64_STANDARD};
use clap::Parser;
use std::path::PathBuf;
use std::sync::Arc;

mod auth;
mod azure;
mod blob_fetcher_pool;
mod cache;
mod clear;
mod cluster;
mod config;
mod error;
mod fetcher;
mod fuse_fs;
mod http_util;
mod hydrate;
mod hydrate_jobs;
mod nic;
mod peerindex;
mod request_id;
mod stats;
mod transport;
#[cfg(feature = "ucx")]
mod transport_ucx;

use crate::blob_fetcher_pool::BlobFetcherPool;
use crate::cache::DiskCache;
use crate::cluster::{Membership, NodeInfo, NodeState};
use crate::config::{Config, MountConfig};
use crate::fetcher::Fetcher;
use crate::peerindex::PeerIndex;
use crate::stats::Stats;
use crate::transport::{ChunkProvider, PeerService};

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

    for d in crate::nic::enumerate(false) {
        tracing::info!(
            iface = %d.iface,
            ip = %d.ip,
            is_likely_infiniband = crate::nic::is_likely_infiniband(&d.iface),
            "detected NIC"
        );
    }

    let main_worker_threads = cfg.azure.main_worker_threads;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(main_worker_threads)
        .enable_all()
        .thread_name("blobcached")
        .build()?;
    tracing::info!(main_worker_threads, "main tokio runtime started");

    let stats = Stats::new();
    let cache = DiskCache::open(cfg.cache.dir.clone(), cfg.cache.max_bytes)?;

    let pool = BlobFetcherPool::new(&cfg.mounts, &cfg.azure, stats.clone(), cfg.azure.workers)
        .context("build blob fetcher pool")?;
    tracing::info!(
        workers = pool.worker_count(),
        block_size = cfg.azure.block_size,
        "blob fetcher pool ready"
    );
    let blobs = pool.view();

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
                    .find(|a| matches!(a.ip, std::net::IpAddr::V4(_)));
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
                    .find(|a| matches!(a.ip, std::net::IpAddr::V4(_)));
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
        bloom_version: 0,
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

    let peer_index = PeerIndex::new(node_id.clone(), cfg.transport.bloom_bits);

    // Wire bloom-version propagation: PeerIndex notifies Membership of version
    // bumps so peers see the new digest in the next gossip round and pull it.
    peer_index.set_on_version_change({
        let m = membership.clone();
        move |v| m.set_bloom_version(v)
    });

    // Wire dead-peer cleanup: when sweep marks a node Dead, immediately drop
    // its bloom from PeerIndex so we stop routing reads to it (closes the
    // 30s gap between Dead-transition and the next bloom-pull cycle).
    membership.set_on_peer_dead({
        let pi = peer_index.clone();
        move |id: &str| pi.drop_remote(id)
    });

    let mounts: Arc<std::collections::HashMap<String, MountConfig>> = Arc::new(
        cfg.mounts
            .iter()
            .map(|m| (m.name.clone(), m.clone()))
            .collect(),
    );

    let fetcher = Arc::new(Fetcher::new(
        cache.clone(),
        pool.clone(),
        peers.clone(),
        membership.clone(),
        stats.clone(),
        cfg.cache.chunk_size,
        cfg.azure.block_size,
        cfg.transport.chunk_concurrency,
        cfg.transport.prefetch_depth,
        cfg.transport.prefetch_threshold,
        cfg.transport.prefetch_concurrency,
        cfg.transport.prefetch_origin_only,
        cfg.cache.cache_on_peer_fetch,
        cfg.cache.peer_lru_bytes,
        peer_index.clone(),
        cfg.transport.peer_max_candidates,
        cfg.transport.peer_max_yes_attempts,
        cfg.transport.peer_max_maybe_attempts,
        cfg.transport.stampede_wait_ms,
        mounts.clone(),
        cfg.transport.peer_concurrency,
    ));

    // ChunkProvider wraps Fetcher::serve_peer_chunk so the peer-service
    // (TCP or RDMA) can route stampede-follower wait_ms requests back into
    // the local singleflight + leader fetch path. Built AFTER Fetcher exists.
    let provider: ChunkProvider = {
        let f = fetcher.clone();
        Arc::new(move |key, len, wait_ms| {
            let f = f.clone();
            Box::pin(async move { f.serve_peer_chunk(key, len, wait_ms).await })
        })
    };

    #[cfg(feature = "ucx")]
    if matches!(cfg.transport.kind.as_str(), "rdma") {
        if let Some((service, _client)) = rdma_bootstrap.as_ref() {
            service.set_chunk_provider(provider.clone());
        }
    }

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
            let peer_svc = PeerService::new(cache.clone(), cluster_hash, stats.peer_stats.clone())
                .with_chunk_provider(provider.clone());
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
        let s = cluster::GossipServer::new(m).with_peer_index(peer_index.clone());
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
    rt.spawn(run_bloom_rebuild_loop(
        peer_index.clone(),
        cache.clone(),
        membership.clone(),
        stats.clone(),
        cfg.transport.bloom_rebuild_secs,
    ));
    rt.spawn(run_bloom_pull_loop(
        peer_index.clone(),
        membership.clone(),
        stats.clone(),
        cfg.transport.bloom_pull_secs,
    ));
    rt.spawn(stats::serve(
        stats.clone(),
        stats_addr,
        cache.clone(),
        membership.clone(),
        fetcher.clone(),
        blobs.clone(),
        mounts.clone(),
        cfg.cache.chunk_size,
        node_id.clone(),
        advertise.clone(),
        cfg.admin.token.clone(),
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

async fn run_bloom_rebuild_loop(
    peer_index: Arc<PeerIndex>,
    cache: Arc<DiskCache>,
    membership: Membership,
    stats: Arc<stats::Stats>,
    period_secs: u64,
) {
    let period = std::time::Duration::from_secs(period_secs.max(1));
    let mut tick = tokio::time::interval(period);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        let pi = peer_index.clone();
        let c = cache.clone();
        let t_start = std::time::Instant::now();
        // Reconcile the in-memory entries map against the filesystem BEFORE
        // snapshotting live_keys() for the rebuild. Without this, files that
        // vanished externally (NVMe loss, manual rm, external wipe between
        // ticks) stay in entries{}, get re-hashed into the new bloom, and we
        // keep advertising chunks we cannot serve until the daemon restarts.
        let res = tokio::task::spawn_blocking(move || {
            let (dropped, bytes) = c.reconcile_with_disk();
            if dropped > 0 {
                tracing::warn!(
                    dropped_entries = dropped,
                    bytes_reclaimed = bytes,
                    "bloom rebuild: reconciled stale cache entries before snapshot"
                );
            }
            pi.rebuild_local_from_cache(&c);
            pi.local_version()
        })
        .await;
        let elapsed = t_start.elapsed();
        stats.bloom_rebuild_seconds.observe(elapsed.as_secs_f64());
        match res {
            Ok(v) => {
                membership.set_bloom_version(v);
                tracing::debug!(
                    version = v,
                    elapsed_ms = elapsed.as_millis() as u64,
                    "bloom rebuild complete"
                );
            }
            Err(e) => tracing::warn!(error = %e, "bloom rebuild task panicked"),
        }
    }
}

async fn run_bloom_pull_loop(
    peer_index: Arc<PeerIndex>,
    membership: Membership,
    stats: Arc<Stats>,
    period_secs: u64,
) {
    let period = std::time::Duration::from_secs(period_secs.max(1));
    let mut tick = tokio::time::interval(period);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let client = match reqwest::Client::builder()
        .pool_idle_timeout(std::time::Duration::from_secs(30))
        .timeout(std::time::Duration::from_secs(15))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "bloom pull client build failed");
            return;
        }
    };
    loop {
        tick.tick().await;
        let alive: Vec<NodeInfo> = membership
            .members_all()
            .into_iter()
            .filter(|n| matches!(n.state, NodeState::Alive))
            .collect();
        let me = peer_index.me_id.clone();
        let mut alive_ids = std::collections::HashSet::new();
        for n in alive.iter() {
            alive_ids.insert(n.id.clone());
        }
        // Forget bloom state for peers that have left the cluster so we don't
        // route to a dead node based on a stale digest.
        for entry in membership.members_all() {
            if !alive_ids.contains(&entry.id) {
                peer_index.drop_remote(&entry.id);
            }
        }
        for n in alive.iter() {
            if n.id == me {
                continue;
            }
            if n.bloom_version == 0 {
                continue;
            }
            let have = peer_index.remote_version(&n.id).unwrap_or(0);
            if n.bloom_version <= have {
                continue;
            }
            let url = format!("{}/cluster/bloom", n.gossip_url.trim_end_matches('/'));
            let req = client.get(&url).send().await;
            match req {
                Ok(resp) if resp.status().is_success() => {
                    let v_hdr = resp
                        .headers()
                        .get("x-blobcache-bloom-version")
                        .and_then(|v| v.to_str().ok())
                        .and_then(|s| s.parse::<u64>().ok())
                        .unwrap_or(n.bloom_version);
                    match resp.bytes().await {
                        Ok(body) => {
                            if peer_index.ingest_remote(&n.id, v_hdr, &body) {
                                stats.peer_bloom_pulls_total.inc();
                                tracing::debug!(peer=%n.id, version=v_hdr, bytes=body.len(), "ingested peer bloom");
                            } else {
                                stats.peer_bloom_pull_errors_total.inc();
                                tracing::warn!(peer=%n.id, "peer bloom payload rejected");
                            }
                        }
                        Err(e) => {
                            stats.peer_bloom_pull_errors_total.inc();
                            tracing::warn!(peer=%n.id, error=%e, "peer bloom body read failed");
                        }
                    }
                }
                Ok(resp) => {
                    stats.peer_bloom_pull_errors_total.inc();
                    tracing::debug!(peer=%n.id, status=%resp.status(), "peer bloom non-2xx");
                }
                Err(e) => {
                    stats.peer_bloom_pull_errors_total.inc();
                    tracing::debug!(peer=%n.id, error=%e, "peer bloom GET failed");
                }
            }
        }
    }
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
