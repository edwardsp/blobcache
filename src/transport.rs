use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use tokio::net::TcpListener;

use crate::cache::{ChunkKey, DiskCache};
use crate::error::{BcError, Result};

pub const PROTO_VERSION: &str = "v1";

// Chunk provider invoked by PeerService when a wait_ms request misses cache.
// Returns Some(bytes) if the chunk could be produced (via singleflight
// subscription or by becoming the leader for a blob fetch) within the
// caller-supplied deadline; None to signal "give up, return 404 to the peer".
pub type ChunkProvider = Arc<
    dyn Fn(ChunkKey, u64, u32) -> Pin<Box<dyn Future<Output = Option<Bytes>> + Send + 'static>>
        + Send
        + Sync,
>;

#[derive(Clone)]
pub struct PeerService {
    cache: Arc<DiskCache>,
    cluster_hash_hex: String,
    pub stats: Arc<crate::stats::PeerStats>,
    chunk_provider: Option<ChunkProvider>,
}

impl PeerService {
    pub fn new(
        cache: Arc<DiskCache>,
        cluster_hash: [u8; 32],
        stats: Arc<crate::stats::PeerStats>,
    ) -> Self {
        Self {
            cache,
            cluster_hash_hex: hex32(&cluster_hash),
            stats,
            chunk_provider: None,
        }
    }

    pub fn with_chunk_provider(mut self, provider: ChunkProvider) -> Self {
        self.chunk_provider = Some(provider);
        self
    }

    pub async fn serve(self, addr: SocketAddr) -> Result<()> {
        let listener = TcpListener::bind(addr).await?;
        tracing::info!(%addr, "peer transport listening");
        loop {
            let (stream, peer) = listener.accept().await?;
            let svc = self.clone();
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let res = http1::Builder::new()
                    .serve_connection(
                        io,
                        service_fn(move |req| {
                            let svc = svc.clone();
                            async move { Ok::<_, Infallible>(svc.handle(req).await) }
                        }),
                    )
                    .await;
                if let Err(e) = res {
                    tracing::debug!(%peer, error=%e, "peer conn closed");
                }
            });
        }
    }

    pub async fn handle(&self, req: Request<Incoming>) -> Response<Full<Bytes>> {
        use tracing::Instrument;
        let rid = req
            .headers()
            .get("x-blobcache-rid")
            .and_then(|v| v.to_str().ok())
            .and_then(crate::request_id::RequestId::from_header)
            .unwrap_or_default();
        let span = tracing::info_span!("peer_server", rid = %rid);
        async move {
            let path = req.uri().path().to_string();
            match (req.method(), path.as_str()) {
                (&Method::GET, "/health") => json_ok(serde_json::json!({
                    "ok": true,
                    "version": PROTO_VERSION,
                    "cluster": &self.cluster_hash_hex,
                })),
                (&Method::GET, p) if p.starts_with("/v1/chunk/") => {
                    self.stats.chunk_requests.inc();
                    self.handle_chunk(req).await
                }
                _ => not_found(),
            }
        }
        .instrument(span)
        .await
    }

    async fn handle_chunk(&self, req: Request<Incoming>) -> Response<Full<Bytes>> {
        let t_total = std::time::Instant::now();
        let parts: Vec<&str> = req.uri().path().splitn(4, '/').collect();
        if parts.len() < 4 {
            return bad_request("path");
        }
        let mount = match urlencoding_decode(parts[3]) {
            Some(s) => s,
            None => return bad_request("mount"),
        };
        let qs: std::collections::HashMap<_, _> =
            url::form_urlencoded::parse(req.uri().query().unwrap_or("").as_bytes())
                .into_owned()
                .collect();
        let blob = match qs.get("blob") {
            Some(s) => s.clone(),
            None => return bad_request("blob"),
        };
        let offset: u64 = match qs.get("offset").and_then(|v| v.parse().ok()) {
            Some(v) => v,
            None => return bad_request("offset"),
        };
        let key = ChunkKey {
            mount,
            blob,
            offset,
        };
        let wait_ms: u32 = qs.get("wait_ms").and_then(|v| v.parse().ok()).unwrap_or(0);
        let expected_len: u64 = qs.get("len").and_then(|v| v.parse().ok()).unwrap_or(0);
        let t_cg = std::time::Instant::now();
        // Offload the synchronous disk read to a blocking thread so a slow
        // NVMe read does not stall the tokio worker and starve other peer
        // requests sharing it. try_get does std::fs::read inline.
        let cache = self.cache.clone();
        let key_bg = key.clone();
        let got = tokio::task::spawn_blocking(move || cache.try_get(&key_bg))
            .await
            .unwrap_or(None);
        self.stats
            .server_cache_get_seconds
            .observe(t_cg.elapsed().as_secs_f64());
        let resolved = match got {
            Some(b) => Some(b),
            None => match (&self.chunk_provider, wait_ms, expected_len) {
                (Some(provider), w, l) if w > 0 && l > 0 => provider(key.clone(), l, w).await,
                _ => None,
            },
        };
        let resp = match resolved {
            Some(b) => {
                self.stats.chunk_bytes_served.inc_by(b.len() as u64);
                Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "application/octet-stream")
                    .header("content-length", b.len().to_string())
                    .body(Full::new(b))
                    .unwrap()
            }
            None => crate::http_util::error_response(StatusCode::NOT_FOUND, "miss"),
        };
        self.stats
            .server_handler_seconds
            .observe(t_total.elapsed().as_secs_f64());
        resp
    }
}

pub struct TcpPeerClient {
    http: reqwest::Client,
}

impl TcpPeerClient {
    pub fn new() -> Self {
        let http = reqwest::Client::builder()
            .pool_max_idle_per_host(64)
            .timeout(std::time::Duration::from_secs(30))
            .http1_only()
            .build()
            .expect("reqwest");
        Self { http }
    }

    pub async fn fetch_chunk(
        &self,
        peer_url: &str,
        key: &ChunkKey,
        expected_len: u32,
        wait_ms: u32,
        rid: Option<&crate::request_id::RequestId>,
    ) -> Result<Bytes> {
        let url = format!(
            "{peer_url}/v1/chunk/{}?blob={}&offset={}&len={}&wait_ms={}",
            urlencoding_encode(&key.mount),
            urlencoding_encode(&key.blob),
            key.offset,
            expected_len,
            wait_ms,
        );
        let mut req = self.http.get(&url);
        if let Some(rid) = rid {
            req = req.header("x-blobcache-rid", rid.as_str());
        }
        let resp = req.send().await?;
        let status = resp.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Err(BcError::NotFound("peer miss".into()));
        }
        if !status.is_success() {
            return Err(BcError::Peer(format!("HTTP {status}")));
        }
        Ok(resp.bytes().await?)
    }

    pub async fn health(&self, peer_url: &str) -> Result<serde_json::Value> {
        let r = self.http.get(format!("{peer_url}/health")).send().await?;
        if !r.status().is_success() {
            return Err(BcError::Peer(format!("health HTTP {}", r.status())));
        }
        Ok(r.json().await?)
    }
}

// Unified client surface for the fetcher. The TCP variant always exists;
// the Rdma variant is gated on the `ucx` feature so non-UCX builds do not
// pull in async-ucx / libucx-dev. `length` on `fetch_chunk` is required by
// the RDMA wire protocol (which pre-allocates a recv buffer); the TCP
// variant ignores it (the HTTP body length is on the response).
#[derive(Clone)]
pub enum PeerClient {
    Tcp(Arc<TcpPeerClient>),
    #[cfg(feature = "ucx")]
    Rdma(crate::transport_ucx::RdmaPeerClient),
}

impl PeerClient {
    pub fn tcp() -> Self {
        Self::Tcp(Arc::new(TcpPeerClient::new()))
    }

    #[cfg(feature = "ucx")]
    pub fn rdma(client: crate::transport_ucx::RdmaPeerClient) -> Self {
        Self::Rdma(client)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn fetch_chunk(
        &self,
        #[cfg(feature = "ucx")] peer_id: &str,
        #[cfg(not(feature = "ucx"))] _peer_id: &str,
        peer_url: &str,
        #[cfg(feature = "ucx")] peer_worker_addr: Option<&[u8]>,
        #[cfg(not(feature = "ucx"))] _peer_worker_addr: Option<&[u8]>,
        key: &ChunkKey,
        length: u32,
        wait_ms: u32,
        rid: Option<&crate::request_id::RequestId>,
    ) -> Result<Bytes> {
        match self {
            Self::Tcp(c) => c.fetch_chunk(peer_url, key, length, wait_ms, rid).await,
            #[cfg(feature = "ucx")]
            Self::Rdma(c) => {
                let worker_addr = peer_worker_addr.ok_or_else(|| {
                    BcError::Peer(format!("missing ucx worker address for peer {peer_id}"))
                })?;
                c.fetch_chunk(peer_id, worker_addr, key, length, wait_ms, rid)
                    .await
            }
        }
    }

    #[allow(dead_code)]
    pub async fn health(
        &self,
        #[cfg(feature = "ucx")] peer_id: &str,
        #[cfg(not(feature = "ucx"))] _peer_id: &str,
        peer_url: &str,
        #[cfg(feature = "ucx")] peer_worker_addr: Option<&[u8]>,
        #[cfg(not(feature = "ucx"))] _peer_worker_addr: Option<&[u8]>,
    ) -> Result<()> {
        match self {
            Self::Tcp(c) => c.health(peer_url).await.map(|_| ()),
            #[cfg(feature = "ucx")]
            Self::Rdma(c) => {
                let worker_addr = peer_worker_addr.ok_or_else(|| {
                    BcError::Peer(format!("missing ucx worker address for peer {peer_id}"))
                })?;
                c.health(peer_id, worker_addr).await
            }
        }
    }

    #[cfg(feature = "ucx")]
    pub fn update_peer(&self, peer_id: &str, peer_worker_addr: &[u8]) -> Result<()> {
        match self {
            Self::Rdma(c) => c.update_peer(peer_id, peer_worker_addr),
            Self::Tcp(_) => Ok(()),
        }
    }
}

fn json_ok(v: serde_json::Value) -> Response<Full<Bytes>> {
    let body = serde_json::to_vec(&v).unwrap_or_default();
    crate::http_util::ok_response(body)
}
fn not_found() -> Response<Full<Bytes>> {
    crate::http_util::error_response(StatusCode::NOT_FOUND, "not found")
}
fn bad_request(why: &str) -> Response<Full<Bytes>> {
    crate::http_util::error_response(StatusCode::BAD_REQUEST, &format!("bad: {why}"))
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

fn urlencoding_encode(s: &str) -> String {
    percent_encoding::utf8_percent_encode(s, percent_encoding::NON_ALPHANUMERIC).to_string()
}
fn urlencoding_decode(s: &str) -> Option<String> {
    percent_encoding::percent_decode_str(s)
        .decode_utf8()
        .ok()
        .map(|c| c.into_owned())
}

impl Default for TcpPeerClient {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(dead_code)]
async fn _drain(b: Incoming) -> Result<Bytes> {
    let body = b
        .collect()
        .await
        .map_err(|e| BcError::Peer(e.to_string()))?;
    Ok(body.to_bytes())
}
