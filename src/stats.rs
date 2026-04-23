use prometheus::{Encoder, IntCounter, IntGauge, Registry, TextEncoder};
use std::sync::Arc;

pub struct Stats {
    pub registry: Registry,
    pub cache_hits: IntCounter,
    pub cache_misses: IntCounter,
    pub cache_evictions: IntCounter,
    pub cache_inserts: IntCounter,
    pub cache_bytes: IntGauge,
    pub blob_fetches: IntCounter,
    pub blob_fetch_bytes: IntCounter,
    pub peer_fetches_ok: IntCounter,
    pub peer_fetches_miss: IntCounter,
    pub peer_fetches_err: IntCounter,
    pub peer_fetch_bytes: IntCounter,
    pub fuse_reads: IntCounter,
    pub fuse_read_bytes: IntCounter,
    pub singleflight_waits: IntCounter,
    pub peer_stats: Arc<PeerStats>,
    pub cluster_stats: Arc<ClusterStats>,
    pub members_alive: IntGauge,
    pub members_dead: IntGauge,
}

pub struct PeerStats {
    pub chunk_requests: IntCounter,
    pub chunk_bytes_served: IntCounter,
}
pub struct ClusterStats {
    pub gossip_rounds: IntCounter,
    pub joins: IntCounter,
    pub failures: IntCounter,
    pub config_mismatches: IntCounter,
}

impl Stats {
    pub fn new() -> Arc<Self> {
        let r = Registry::new();
        let cache_hits = IntCounter::new("blobcache_cache_hits_total", "cache hits").unwrap();
        let cache_misses = IntCounter::new("blobcache_cache_misses_total", "cache misses").unwrap();
        let cache_evictions =
            IntCounter::new("blobcache_cache_evictions_total", "cache evictions").unwrap();
        let cache_inserts =
            IntCounter::new("blobcache_cache_inserts_total", "cache inserts").unwrap();
        let cache_bytes = IntGauge::new("blobcache_cache_bytes", "cache bytes in use").unwrap();
        let blob_fetches = IntCounter::new("blobcache_blob_fetches_total", "blob fetches").unwrap();
        let blob_fetch_bytes =
            IntCounter::new("blobcache_blob_fetch_bytes_total", "blob fetched bytes").unwrap();
        let peer_fetches_ok =
            IntCounter::new("blobcache_peer_fetches_ok_total", "peer fetches ok").unwrap();
        let peer_fetches_miss =
            IntCounter::new("blobcache_peer_fetches_miss_total", "peer fetch misses").unwrap();
        let peer_fetches_err =
            IntCounter::new("blobcache_peer_fetches_err_total", "peer fetch errors").unwrap();
        let peer_fetch_bytes =
            IntCounter::new("blobcache_peer_fetch_bytes_total", "peer fetched bytes").unwrap();
        let fuse_reads = IntCounter::new("blobcache_fuse_reads_total", "fuse read calls").unwrap();
        let fuse_read_bytes =
            IntCounter::new("blobcache_fuse_read_bytes_total", "fuse read bytes").unwrap();
        let singleflight_waits = IntCounter::new(
            "blobcache_singleflight_waits_total",
            "fetches deduped via singleflight",
        )
        .unwrap();
        let chunk_requests = IntCounter::new(
            "blobcache_peer_chunk_requests_total",
            "chunk requests served",
        )
        .unwrap();
        let chunk_bytes_served = IntCounter::new(
            "blobcache_peer_chunk_bytes_served_total",
            "chunk bytes served",
        )
        .unwrap();
        let gossip_rounds =
            IntCounter::new("blobcache_cluster_gossip_rounds_total", "gossip rounds").unwrap();
        let joins =
            IntCounter::new("blobcache_cluster_joins_total", "peer joins observed").unwrap();
        let failures =
            IntCounter::new("blobcache_cluster_failures_total", "peer failures observed").unwrap();
        let config_mismatches = IntCounter::new(
            "blobcache_cluster_config_mismatches_total",
            "config hash mismatches",
        )
        .unwrap();
        let members_alive =
            IntGauge::new("blobcache_cluster_members_alive", "alive members").unwrap();
        let members_dead = IntGauge::new("blobcache_cluster_members_dead", "dead members").unwrap();

        for m in [
            &cache_hits,
            &cache_misses,
            &cache_evictions,
            &cache_inserts,
            &blob_fetches,
            &blob_fetch_bytes,
            &peer_fetches_ok,
            &peer_fetches_miss,
            &peer_fetches_err,
            &peer_fetch_bytes,
            &fuse_reads,
            &fuse_read_bytes,
            &singleflight_waits,
            &chunk_requests,
            &chunk_bytes_served,
            &gossip_rounds,
            &joins,
            &failures,
            &config_mismatches,
        ] {
            r.register(Box::new(m.clone())).unwrap();
        }
        for g in [&cache_bytes, &members_alive, &members_dead] {
            r.register(Box::new(g.clone())).unwrap();
        }

        Arc::new(Self {
            registry: r,
            cache_hits,
            cache_misses,
            cache_evictions,
            cache_inserts,
            cache_bytes,
            blob_fetches,
            blob_fetch_bytes,
            peer_fetches_ok,
            peer_fetches_miss,
            peer_fetches_err,
            peer_fetch_bytes,
            fuse_reads,
            fuse_read_bytes,
            singleflight_waits,
            members_alive,
            members_dead,
            peer_stats: Arc::new(PeerStats {
                chunk_requests,
                chunk_bytes_served,
            }),
            cluster_stats: Arc::new(ClusterStats {
                gossip_rounds,
                joins,
                failures,
                config_mismatches,
            }),
        })
    }

    pub fn render(&self) -> Vec<u8> {
        let mfs = self.registry.gather();
        let mut buf = Vec::new();
        TextEncoder::new().encode(&mfs, &mut buf).unwrap();
        buf
    }
}

pub async fn serve(
    stats: Arc<Stats>,
    addr: std::net::SocketAddr,
    cache: Arc<crate::cache::DiskCache>,
    membership: crate::cluster::Membership,
) -> crate::error::Result<()> {
    use bytes::Bytes;
    use http_body_util::Full;
    use hyper::body::Incoming;
    use hyper::server::conn::http1;
    use hyper::service::service_fn;
    use hyper::{Method, Request, Response};
    use hyper_util::rt::TokioIo;
    use std::convert::Infallible;
    use std::sync::atomic::Ordering;
    use tokio::net::TcpListener;

    let listener = TcpListener::bind(addr).await?;
    tracing::info!(%addr, "stats endpoint listening");
    loop {
        let (stream, _) = listener.accept().await?;
        let s = stats.clone();
        let cache = cache.clone();
        let membership = membership.clone();
        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let _ = http1::Builder::new()
                .serve_connection(
                    io,
                    service_fn(move |req: Request<Incoming>| {
                        let s = s.clone();
                        let cache = cache.clone();
                        let membership = membership.clone();
                        async move {
                            let resp = match (req.method(), req.uri().path()) {
                                (&Method::GET, "/metrics") => {
                                    let cs = &cache.stats;
                                    s.cache_hits.reset();
                                    s.cache_hits.inc_by(cs.hits.load(Ordering::Relaxed));
                                    s.cache_misses.reset();
                                    s.cache_misses.inc_by(cs.misses.load(Ordering::Relaxed));
                                    s.cache_evictions.reset();
                                    s.cache_evictions
                                        .inc_by(cs.evictions.load(Ordering::Relaxed));
                                    s.cache_inserts.reset();
                                    s.cache_inserts.inc_by(cs.inserts.load(Ordering::Relaxed));
                                    s.cache_bytes
                                        .set(cs.bytes_in_use.load(Ordering::Relaxed) as i64);
                                    let all = membership.members_all();
                                    let alive = all
                                        .iter()
                                        .filter(|n| {
                                            matches!(n.state, crate::cluster::NodeState::Alive)
                                        })
                                        .count();
                                    let dead = all
                                        .iter()
                                        .filter(|n| {
                                            matches!(n.state, crate::cluster::NodeState::Dead)
                                        })
                                        .count();
                                    s.members_alive.set(alive as i64);
                                    s.members_dead.set(dead as i64);
                                    let body = s.render();
                                    Response::builder()
                                        .status(200)
                                        .header("content-type", "text/plain; version=0.0.4")
                                        .body(Full::new(Bytes::from(body)))
                                        .unwrap()
                                }
                                (&Method::GET, "/stats") => {
                                    let cs = &cache.stats;
                                    let v = serde_json::json!({
                                        "cache": {
                                            "hits": cs.hits.load(Ordering::Relaxed),
                                            "misses": cs.misses.load(Ordering::Relaxed),
                                            "evictions": cs.evictions.load(Ordering::Relaxed),
                                            "inserts": cs.inserts.load(Ordering::Relaxed),
                                            "bytes_in_use": cs.bytes_in_use.load(Ordering::Relaxed),
                                        },
                                        "cluster": {
                                            "members": membership.members_all(),
                                        },
                                    });
                                    let body = serde_json::to_vec(&v).unwrap();
                                    Response::builder()
                                        .status(200)
                                        .header("content-type", "application/json")
                                        .body(Full::new(Bytes::from(body)))
                                        .unwrap()
                                }
                                (&Method::GET, "/peers") => {
                                    let v =
                                        serde_json::json!({"members": membership.members_all()});
                                    let body = serde_json::to_vec(&v).unwrap();
                                    Response::builder()
                                        .status(200)
                                        .header("content-type", "application/json")
                                        .body(Full::new(Bytes::from(body)))
                                        .unwrap()
                                }
                                _ => Response::builder()
                                    .status(404)
                                    .body(Full::new(Bytes::new()))
                                    .unwrap(),
                            };
                            Ok::<_, Infallible>(resp)
                        }
                    }),
                )
                .await;
        });
    }
}
