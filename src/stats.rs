use prometheus::{
    Encoder, Histogram, HistogramOpts, IntCounter, IntCounterVec, IntGauge, Opts, Registry,
    TextEncoder,
};
use std::sync::Arc;

pub struct Stats {
    pub registry: Registry,
    pub cache_hits: IntCounter,
    pub cache_misses: IntCounter,
    pub cache_evictions: IntCounter,
    pub cache_inserts: IntCounter,
    pub cache_reconcile_drops: IntCounter,
    pub cache_bytes: IntGauge,
    pub blob_fetches: IntCounter,
    pub blob_fetch_bytes: IntCounter,
    // Throttling visibility: when Azure egress saturates the storage account
    // (Premium block-blob accounts cap around ~200 Gbps), it returns 429/503
    // with a Retry-After hint. The blob client retries silently with exponential
    // backoff. These counters surface that retry traffic so an operator can
    // see throttling pressure without having to tcpdump.
    //
    // - blob_request_status_total{status="200|206|429|503|...|err"} — the
    //   FINAL status of every blob HTTP attempt that returned (i.e., wasn't
    //   followed by another retry). "err" is used when the underlying
    //   reqwest::Error fired (timeout, connect, body) and we gave up.
    // - blob_request_retries_total{status="429|500|502|503|504|net"} —
    //   incremented every time we observed a retryable condition AND
    //   decided to retry. Sum across labels = total retries performed.
    // - blob_request_giveups_total — exhausted max_retries on a request.
    // - blob_retry_sleep_seconds_total — cumulative seconds slept in
    //   exponential-backoff between retry attempts; a high value here while
    //   throughput is low is the smoking gun for storage-side throttling.
    pub blob_request_status_total: IntCounterVec,
    pub blob_request_retries_total: IntCounterVec,
    pub blob_request_giveups_total: IntCounter,
    pub blob_retry_sleep_seconds_total: prometheus::Counter,
    // Per-attempt blob HTTP request wall time (single send().await + body
    // drain). Wide buckets up to 60s because Azure can stall multi-second
    // under throttle. blob_request_seconds_max is a gauge of the longest
    // observed request since process start (monotone) so a single >30s
    // outlier is visible without histogram quantile estimation.
    pub blob_request_seconds: Histogram,
    pub blob_request_seconds_max: prometheus::Gauge,
    pub peer_fetches_ok: IntCounter,
    pub peer_fetches_miss: IntCounter,
    pub peer_fetches_err: IntCounter,
    pub peer_fetch_bytes: IntCounter,
    pub peer_lru_hits: IntCounter,
    pub fuse_reads: IntCounter,
    pub fuse_read_bytes: IntCounter,
    pub singleflight_waits: IntCounter,
    pub prefetch_spawned: IntCounter,
    pub prefetch_skipped_cached: IntCounter,
    pub prefetch_skipped_inflight: IntCounter,
    pub prefetch_skipped_not_origin: IntCounter,
    pub prefetch_completed_ok: IntCounter,
    pub prefetch_completed_err: IntCounter,
    pub peer_bloom_yes: IntCounter,
    pub peer_bloom_no_holder: IntCounter,
    pub peer_bloom_false_positive: IntCounter,
    pub peer_bloom_stale_drops: IntCounter,
    pub peer_bloom_pulls_total: IntCounter,
    pub peer_bloom_pull_errors_total: IntCounter,
    pub peer_stampede_leader: IntCounter,
    pub peer_stampede_follower: IntCounter,
    pub peer_stampede_follower_ok: IntCounter,
    pub peer_stampede_follower_timeout: IntCounter,
    pub peer_stats: Arc<PeerStats>,
    pub cluster_stats: Arc<ClusterStats>,
    pub members_alive: IntGauge,
    pub members_dead: IntGauge,
    pub chunk_total_seconds: Histogram,
    pub chunk_cache_get_seconds: Histogram,
    pub chunk_peer_fetch_seconds: Histogram,
    pub chunk_cache_insert_seconds: Histogram,
    pub fuse_read_seconds: Histogram,
}

pub struct PeerStats {
    pub chunk_requests: IntCounter,
    pub chunk_bytes_served: IntCounter,
    #[cfg(feature = "ucx")]
    pub rdma_non_rdma_lane: IntCounter,
    pub server_handler_seconds: Histogram,
    pub server_cache_get_seconds: Histogram,
    #[allow(dead_code)]
    pub server_send_seconds: Histogram,
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
        let cache_reconcile_drops = IntCounter::new(
            "blobcache_cache_reconcile_drops_total",
            "entries dropped because backing file was missing on disk (would have caused stale-bloom false positives)",
        )
        .unwrap();
        let cache_bytes = IntGauge::new("blobcache_cache_bytes", "cache bytes in use").unwrap();
        let blob_fetches = IntCounter::new("blobcache_blob_fetches_total", "blob fetches").unwrap();
        let blob_fetch_bytes =
            IntCounter::new("blobcache_blob_fetch_bytes_total", "blob fetched bytes").unwrap();
        let blob_request_status_total = IntCounterVec::new(
            Opts::new(
                "blobcache_blob_request_status_total",
                "final HTTP status of blob requests after retries (label: status code or \"err\")",
            ),
            &["status"],
        )
        .unwrap();
        let blob_request_retries_total = IntCounterVec::new(
            Opts::new(
                "blobcache_blob_request_retries_total",
                "blob request retries by retryable condition (label: status code or \"net\")",
            ),
            &["status"],
        )
        .unwrap();
        let blob_request_giveups_total = IntCounter::new(
            "blobcache_blob_request_giveups_total",
            "blob requests that exhausted max_retries",
        )
        .unwrap();
        let blob_retry_sleep_seconds_total = prometheus::Counter::new(
            "blobcache_blob_retry_sleep_seconds_total",
            "cumulative seconds slept in exponential backoff between blob retries",
        )
        .unwrap();
        let blob_request_seconds = Histogram::with_opts(
            HistogramOpts::new(
                "blobcache_blob_request_seconds",
                "wall time for one Azure blob HTTP request attempt (send + body drain)",
            )
            .buckets(vec![
                0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0,
            ]),
        )
        .unwrap();
        let blob_request_seconds_max = prometheus::Gauge::new(
            "blobcache_blob_request_seconds_max",
            "longest observed single blob HTTP request since process start",
        )
        .unwrap();
        let peer_fetches_ok =
            IntCounter::new("blobcache_peer_fetches_ok_total", "peer fetches ok").unwrap();
        let peer_fetches_miss =
            IntCounter::new("blobcache_peer_fetches_miss_total", "peer fetch misses").unwrap();
        let peer_fetches_err =
            IntCounter::new("blobcache_peer_fetches_err_total", "peer fetch errors").unwrap();
        let peer_fetch_bytes =
            IntCounter::new("blobcache_peer_fetch_bytes_total", "peer fetched bytes").unwrap();
        let peer_lru_hits = IntCounter::new(
            "blobcache_peer_lru_hits_total",
            "in-memory peer-LRU hits (cache_on_peer_fetch=false path)",
        )
        .unwrap();
        let fuse_reads = IntCounter::new("blobcache_fuse_reads_total", "fuse read calls").unwrap();
        let fuse_read_bytes =
            IntCounter::new("blobcache_fuse_read_bytes_total", "fuse read bytes").unwrap();
        let singleflight_waits = IntCounter::new(
            "blobcache_singleflight_waits_total",
            "fetches deduped via singleflight",
        )
        .unwrap();
        let prefetch_spawned = IntCounter::new(
            "blobcache_prefetch_spawned_total",
            "prefetch chunk fetches spawned by sequential detector",
        )
        .unwrap();
        let prefetch_skipped_cached = IntCounter::new(
            "blobcache_prefetch_skipped_cached_total",
            "prefetch candidates skipped because already cached",
        )
        .unwrap();
        let prefetch_skipped_inflight = IntCounter::new(
            "blobcache_prefetch_skipped_inflight_total",
            "prefetch candidates skipped because fetch already in flight",
        )
        .unwrap();
        let prefetch_skipped_not_origin = IntCounter::new(
            "blobcache_prefetch_skipped_not_origin_total",
            "prefetch trigger gated off because prefetch_origin_only=true and the stream's recent fetches were not from blob",
        )
        .unwrap();
        let prefetch_completed_ok = IntCounter::new(
            "blobcache_prefetch_completed_ok_total",
            "prefetch chunk fetches that completed successfully",
        )
        .unwrap();
        let prefetch_completed_err = IntCounter::new(
            "blobcache_prefetch_completed_err_total",
            "prefetch chunk fetches that failed",
        )
        .unwrap();
        let peer_bloom_yes = IntCounter::new(
            "blobcache_peer_bloom_yes_total",
            "fetches where at least one peer bloom indicated holder",
        )
        .unwrap();
        let peer_bloom_no_holder = IntCounter::new(
            "blobcache_peer_bloom_no_holder_total",
            "fetches with peers known but no bloom-positive holder",
        )
        .unwrap();
        let peer_bloom_false_positive = IntCounter::new(
            "blobcache_peer_bloom_false_positive_total",
            "bloom-positive peers that returned NotFound",
        )
        .unwrap();
        let peer_bloom_stale_drops = IntCounter::new(
            "blobcache_peer_bloom_stale_drops_total",
            "remote bloom views dropped after a was_yes NotFound (forces refetch on next bloom-pull tick)",
        )
        .unwrap();
        let peer_bloom_pulls_total = IntCounter::new(
            "blobcache_peer_bloom_pulls_total",
            "successful peer bloom pulls",
        )
        .unwrap();
        let peer_bloom_pull_errors_total = IntCounter::new(
            "blobcache_peer_bloom_pull_errors_total",
            "failed peer bloom pulls",
        )
        .unwrap();
        let peer_stampede_leader = IntCounter::new(
            "blobcache_peer_stampede_leader_total",
            "fetches where this node is the HRW-top stampede leader",
        )
        .unwrap();
        let peer_stampede_follower = IntCounter::new(
            "blobcache_peer_stampede_follower_total",
            "fetches where this node routes to HRW-top with wait_ms as a follower",
        )
        .unwrap();
        let peer_stampede_follower_ok = IntCounter::new(
            "blobcache_peer_stampede_follower_ok_total",
            "stampede follower fetches that returned data within wait_ms",
        )
        .unwrap();
        let peer_stampede_follower_timeout = IntCounter::new(
            "blobcache_peer_stampede_follower_timeout_total",
            "stampede follower fetches that fell through to blob (peer didn't deliver in time)",
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
        #[cfg(feature = "ucx")]
        let rdma_non_rdma_lane = IntCounter::new(
            "blobcache_rdma_non_rdma_lane_total",
            "rdma endpoints that resolved to non-RDMA lanes",
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

        let mk_hist = |name: &str, help: &str| {
            Histogram::with_opts(HistogramOpts::new(name, help).buckets(vec![
                0.0001, 0.00025, 0.0005, 0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5,
                1.0,
            ]))
            .unwrap()
        };
        let chunk_total_seconds = mk_hist(
            "blobcache_chunk_fetch_total_seconds",
            "wall time in fetch_chunk()",
        );
        let chunk_cache_get_seconds = mk_hist(
            "blobcache_chunk_cache_get_seconds",
            "wall time in cache.try_get spawn_blocking",
        );
        let chunk_peer_fetch_seconds = mk_hist(
            "blobcache_chunk_peer_fetch_seconds",
            "wall time in peers.fetch_chunk await",
        );
        let chunk_cache_insert_seconds = mk_hist(
            "blobcache_chunk_cache_insert_seconds",
            "wall time in cache.insert spawn_blocking",
        );
        let fuse_read_seconds = mk_hist(
            "blobcache_fuse_read_seconds",
            "wall time in FUSE read callback",
        );
        let server_handler_seconds = mk_hist(
            "blobcache_peer_server_handler_seconds",
            "wall time per inbound peer chunk request handler",
        );
        let server_cache_get_seconds = mk_hist(
            "blobcache_peer_server_cache_get_seconds",
            "wall time in server-side cache.try_get for peer chunk requests",
        );
        let server_send_seconds = mk_hist(
            "blobcache_peer_server_send_seconds",
            "wall time sending response bytes to peer",
        );

        for m in [
            &cache_hits,
            &cache_misses,
            &cache_evictions,
            &cache_inserts,
            &cache_reconcile_drops,
            &blob_fetches,
            &blob_fetch_bytes,
            &blob_request_giveups_total,
            &peer_fetches_ok,
            &peer_fetches_miss,
            &peer_fetches_err,
            &peer_fetch_bytes,
            &peer_lru_hits,
            &fuse_reads,
            &fuse_read_bytes,
            &singleflight_waits,
            &prefetch_spawned,
            &prefetch_skipped_cached,
            &prefetch_skipped_inflight,
            &prefetch_skipped_not_origin,
            &prefetch_completed_ok,
            &prefetch_completed_err,
            &peer_bloom_yes,
            &peer_bloom_no_holder,
            &peer_bloom_false_positive,
            &peer_bloom_stale_drops,
            &peer_bloom_pulls_total,
            &peer_bloom_pull_errors_total,
            &peer_stampede_leader,
            &peer_stampede_follower,
            &peer_stampede_follower_ok,
            &peer_stampede_follower_timeout,
            &chunk_requests,
            &chunk_bytes_served,
            #[cfg(feature = "ucx")]
            &rdma_non_rdma_lane,
            &gossip_rounds,
            &joins,
            &failures,
            &config_mismatches,
        ] {
            r.register(Box::new(m.clone())).unwrap();
        }
        r.register(Box::new(blob_request_status_total.clone()))
            .unwrap();
        r.register(Box::new(blob_request_retries_total.clone()))
            .unwrap();
        r.register(Box::new(blob_retry_sleep_seconds_total.clone()))
            .unwrap();
        r.register(Box::new(blob_request_seconds.clone())).unwrap();
        r.register(Box::new(blob_request_seconds_max.clone()))
            .unwrap();
        for g in [&cache_bytes, &members_alive, &members_dead] {
            r.register(Box::new(g.clone())).unwrap();
        }
        for h in [
            &chunk_total_seconds,
            &chunk_cache_get_seconds,
            &chunk_peer_fetch_seconds,
            &chunk_cache_insert_seconds,
            &fuse_read_seconds,
            &server_handler_seconds,
            &server_cache_get_seconds,
            &server_send_seconds,
        ] {
            r.register(Box::new(h.clone())).unwrap();
        }

        Arc::new(Self {
            registry: r,
            cache_hits,
            cache_misses,
            cache_evictions,
            cache_inserts,
            cache_reconcile_drops,
            cache_bytes,
            blob_fetches,
            blob_fetch_bytes,
            blob_request_status_total,
            blob_request_retries_total,
            blob_request_giveups_total,
            blob_retry_sleep_seconds_total,
            blob_request_seconds,
            blob_request_seconds_max,
            peer_fetches_ok,
            peer_fetches_miss,
            peer_fetches_err,
            peer_fetch_bytes,
            peer_lru_hits,
            fuse_reads,
            fuse_read_bytes,
            singleflight_waits,
            prefetch_spawned,
            prefetch_skipped_cached,
            prefetch_skipped_inflight,
            prefetch_skipped_not_origin,
            prefetch_completed_ok,
            prefetch_completed_err,
            peer_bloom_yes,
            peer_bloom_no_holder,
            peer_bloom_false_positive,
            peer_bloom_stale_drops,
            peer_bloom_pulls_total,
            peer_bloom_pull_errors_total,
            peer_stampede_leader,
            peer_stampede_follower,
            peer_stampede_follower_ok,
            peer_stampede_follower_timeout,
            members_alive,
            members_dead,
            chunk_total_seconds,
            chunk_cache_get_seconds,
            chunk_peer_fetch_seconds,
            chunk_cache_insert_seconds,
            fuse_read_seconds,
            peer_stats: Arc::new(PeerStats {
                chunk_requests,
                chunk_bytes_served,
                #[cfg(feature = "ucx")]
                rdma_non_rdma_lane,
                server_handler_seconds,
                server_cache_get_seconds,
                server_send_seconds,
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

#[allow(clippy::too_many_arguments)]
pub async fn serve(
    stats: Arc<Stats>,
    addr: std::net::SocketAddr,
    cache: Arc<crate::cache::DiskCache>,
    membership: crate::cluster::Membership,
    fetcher: Arc<crate::fetcher::Fetcher>,
    blobs: Arc<std::collections::HashMap<String, Arc<crate::azure::BlobClient>>>,
    mounts: Arc<std::collections::HashMap<String, crate::config::MountConfig>>,
    chunk_size: u64,
    me_id: String,
    me_transport_url: String,
) -> crate::error::Result<()> {
    use bytes::Bytes;
    use http_body_util::{BodyExt, Full};
    use hyper::body::Incoming;
    use hyper::server::conn::http1;
    use hyper::service::service_fn;
    use hyper::{Method, Request, Response};
    use hyper_util::rt::TokioIo;
    use std::convert::Infallible;
    use std::sync::atomic::Ordering;
    use tokio::net::TcpListener;

    let hydrate_http = reqwest::Client::builder()
        .pool_idle_timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|e| crate::error::BcError::Other(format!("hydrate http build: {e}")))?;

    let listener = TcpListener::bind(addr).await?;
    tracing::info!(%addr, "stats endpoint listening");
    loop {
        let (stream, _) = listener.accept().await?;
        let s = stats.clone();
        let cache = cache.clone();
        let membership = membership.clone();
        let fetcher = fetcher.clone();
        let blobs = blobs.clone();
        let mounts = mounts.clone();
        let me_id = me_id.clone();
        let me_url = me_transport_url.clone();
        let hydrate_http = hydrate_http.clone();
        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let _ = http1::Builder::new()
                .serve_connection(
                    io,
                    service_fn(move |req: Request<Incoming>| {
                        let s = s.clone();
                        let cache = cache.clone();
                        let membership = membership.clone();
                        let fetcher = fetcher.clone();
                        let blobs = blobs.clone();
                        let mounts = mounts.clone();
                        let me_id = me_id.clone();
                        let me_url = me_url.clone();
                        let hydrate_http = hydrate_http.clone();
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
                                    s.cache_reconcile_drops.reset();
                                    s.cache_reconcile_drops
                                        .inc_by(cs.reconcile_drops.load(Ordering::Relaxed));
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
                                            "reconcile_drops": cs.reconcile_drops.load(Ordering::Relaxed),
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
                                (&Method::GET, "/healthz") => Response::builder()
                                    .status(200)
                                    .header("content-type", "text/plain")
                                    .body(Full::new(Bytes::from_static(b"ok\n")))
                                    .unwrap(),
                                (&Method::GET, "/readyz") => {
                                    // Ready once we have at least one Alive
                                    // member (which includes self after gossip
                                    // boot). Pre-join we're up but should not
                                    // receive peer traffic, so K8s can keep us
                                    // out of any peer-facing Service until
                                    // membership stabilises.
                                    let alive = membership
                                        .members_all()
                                        .iter()
                                        .filter(|n| {
                                            matches!(n.state, crate::cluster::NodeState::Alive)
                                        })
                                        .count();
                                    if alive >= 1 {
                                        Response::builder()
                                            .status(200)
                                            .header("content-type", "text/plain")
                                            .body(Full::new(Bytes::from_static(b"ready\n")))
                                            .unwrap()
                                    } else {
                                        Response::builder()
                                            .status(503)
                                            .header("content-type", "text/plain")
                                            .body(Full::new(Bytes::from_static(b"joining\n")))
                                            .unwrap()
                                    }
                                }
                                (&Method::POST, "/hydrate") => {
                                    let body = match req.into_body().collect().await {
                                        Ok(c) => c.to_bytes(),
                                        Err(e) => {
                                            return Ok::<_, Infallible>(
                                                Response::builder()
                                                    .status(400)
                                                    .body(Full::new(Bytes::from(format!(
                                                        "body read: {e}"
                                                    ))))
                                                    .unwrap(),
                                            );
                                        }
                                    };
                                    let hreq: crate::hydrate::HydrateRequest =
                                        match serde_json::from_slice(&body) {
                                            Ok(r) => r,
                                            Err(e) => {
                                                return Ok::<_, Infallible>(
                                                    Response::builder()
                                                        .status(400)
                                                        .body(Full::new(Bytes::from(format!(
                                                            "json: {e}"
                                                        ))))
                                                        .unwrap(),
                                                );
                                            }
                                        };
                                    match crate::hydrate::run_coordinator(
                                        hreq,
                                        chunk_size,
                                        fetcher.clone(),
                                        blobs.clone(),
                                        mounts.clone(),
                                        membership.clone(),
                                        me_id.clone(),
                                        me_url.clone(),
                                        hydrate_http.clone(),
                                    )
                                    .await
                                    {
                                        Ok(r) => {
                                            let body = serde_json::to_vec(&r).unwrap();
                                            Response::builder()
                                                .status(200)
                                                .header("content-type", "application/json")
                                                .body(Full::new(Bytes::from(body)))
                                                .unwrap()
                                        }
                                        Err(e) => Response::builder()
                                            .status(500)
                                            .body(Full::new(Bytes::from(format!("{e}"))))
                                            .unwrap(),
                                    }
                                }
                                (&Method::POST, "/clear-cache") => {
                                    match crate::clear::run_coordinator(
                                        fetcher.clone(),
                                        membership.clone(),
                                        me_id.clone(),
                                        hydrate_http.clone(),
                                    )
                                    .await
                                    {
                                        Ok(r) => {
                                            let body = serde_json::to_vec(&r).unwrap();
                                            Response::builder()
                                                .status(200)
                                                .header("content-type", "application/json")
                                                .body(Full::new(Bytes::from(body)))
                                                .unwrap()
                                        }
                                        Err(e) => Response::builder()
                                            .status(500)
                                            .body(Full::new(Bytes::from(format!("{e}"))))
                                            .unwrap(),
                                    }
                                }
                                (&Method::POST, "/clear-cache-shard") => {
                                    let r = crate::clear::run_shard(fetcher.clone()).await;
                                    let body = serde_json::to_vec(&r).unwrap();
                                    Response::builder()
                                        .status(200)
                                        .header("content-type", "application/json")
                                        .body(Full::new(Bytes::from(body)))
                                        .unwrap()
                                }
                                (&Method::POST, "/hydrate-shard") => {
                                    let body = match req.into_body().collect().await {
                                        Ok(c) => c.to_bytes(),
                                        Err(e) => {
                                            return Ok::<_, Infallible>(
                                                Response::builder()
                                                    .status(400)
                                                    .body(Full::new(Bytes::from(format!(
                                                        "body read: {e}"
                                                    ))))
                                                    .unwrap(),
                                            );
                                        }
                                    };
                                    let sreq: crate::hydrate::HydrateShardRequest =
                                        match serde_json::from_slice(&body) {
                                            Ok(r) => r,
                                            Err(e) => {
                                                return Ok::<_, Infallible>(
                                                    Response::builder()
                                                        .status(400)
                                                        .body(Full::new(Bytes::from(format!(
                                                            "json: {e}"
                                                        ))))
                                                        .unwrap(),
                                                );
                                            }
                                        };
                                    let r = crate::hydrate::run_shard(
                                        sreq,
                                        fetcher.clone(),
                                        mounts.clone(),
                                    )
                                    .await;
                                    let body = serde_json::to_vec(&r).unwrap();
                                    Response::builder()
                                        .status(200)
                                        .header("content-type", "application/json")
                                        .body(Full::new(Bytes::from(body)))
                                        .unwrap()
                                }
                                (&Method::POST, "/hydrate-broadcast-shard") => {
                                    let body = match req.into_body().collect().await {
                                        Ok(c) => c.to_bytes(),
                                        Err(e) => {
                                            return Ok::<_, Infallible>(
                                                Response::builder()
                                                    .status(400)
                                                    .body(Full::new(Bytes::from(format!(
                                                        "body read: {e}"
                                                    ))))
                                                    .unwrap(),
                                            );
                                        }
                                    };
                                    let breq: crate::hydrate::HydrateBroadcastShardRequest =
                                        match serde_json::from_slice(&body) {
                                            Ok(r) => r,
                                            Err(e) => {
                                                return Ok::<_, Infallible>(
                                                    Response::builder()
                                                        .status(400)
                                                        .body(Full::new(Bytes::from(format!(
                                                            "json: {e}"
                                                        ))))
                                                        .unwrap(),
                                                );
                                            }
                                        };
                                    let r = crate::hydrate::run_broadcast_shard(
                                        breq,
                                        fetcher.clone(),
                                        mounts.clone(),
                                    )
                                    .await;
                                    let body = serde_json::to_vec(&r).unwrap();
                                    Response::builder()
                                        .status(200)
                                        .header("content-type", "application/json")
                                        .body(Full::new(Bytes::from(body)))
                                        .unwrap()
                                }
                                (&Method::POST, "/hydrate-ring-step") => {
                                    let body = match req.into_body().collect().await {
                                        Ok(c) => c.to_bytes(),
                                        Err(e) => {
                                            return Ok::<_, Infallible>(
                                                Response::builder()
                                                    .status(400)
                                                    .body(Full::new(Bytes::from(format!(
                                                        "body read: {e}"
                                                    ))))
                                                    .unwrap(),
                                            );
                                        }
                                    };
                                    let rreq: crate::hydrate::HydrateRingStepRequest =
                                        match serde_json::from_slice(&body) {
                                            Ok(r) => r,
                                            Err(e) => {
                                                return Ok::<_, Infallible>(
                                                    Response::builder()
                                                        .status(400)
                                                        .body(Full::new(Bytes::from(format!(
                                                            "json: {e}"
                                                        ))))
                                                        .unwrap(),
                                                );
                                            }
                                        };
                                    let r = crate::hydrate::run_ring_step(
                                        rreq,
                                        fetcher.clone(),
                                        mounts.clone(),
                                    )
                                    .await;
                                    let body = serde_json::to_vec(&r).unwrap();
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
