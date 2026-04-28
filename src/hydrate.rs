// Hydrate: pre-warm the cluster cache for a given mount + path. Coordinator
// node lists blobs, enumerates chunks, round-robin shards them across all
// alive cluster members (including itself), and POSTs each peer their
// per-shard chunk batch via /hydrate-shard. Each worker fetches its assigned
// chunks through the local Fetcher (which inserts them into its own cache
// and updates its bloom). The result: aggregate bandwidth scales ~linearly
// with cluster size, since every node pulls a different ~1/N of the data
// in parallel from Azure.

use crate::azure::BlobClient;
use crate::cluster::{Membership, NodeState};
use crate::config::MountConfig;
use crate::error::{BcError, Result};
use crate::fetcher::Fetcher;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkSpec {
    pub blob: String,
    pub offset: u64,
    pub len: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HydrateShardRequest {
    pub mount: String,
    pub chunks: Vec<ChunkSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HydrateShardResponse {
    pub fetched: u64,
    pub bytes: u64,
    pub errors: Vec<String>,
    pub elapsed_ms: u64,
    pub start_unix_ms: u64,
    pub end_unix_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HydrateRequest {
    pub mount: String,
    #[serde(default)]
    pub path: String,
    #[serde(default)]
    pub recursive: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerPeerStats {
    pub node_id: String,
    pub assigned_chunks: u64,
    pub fetched: u64,
    pub bytes: u64,
    pub errors: Vec<String>,
    pub elapsed_ms: u64,
    pub start_unix_ms: u64,
    pub end_unix_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HydrateResponse {
    pub mount: String,
    pub path: String,
    pub total_files: u64,
    pub total_chunks: u64,
    pub total_bytes: u64,
    pub elapsed_ms: u64,
    pub aggregate_mibs: f64,
    pub peers: Vec<PerPeerStats>,
}

/// Worker side: fetch the assigned chunks through the local Fetcher,
/// using the same chunk_concurrency semaphore as normal reads. Errors
/// per chunk are collected (truncated) so partial failure is observable
/// rather than aborting the whole shard.
pub async fn run_shard(
    req: HydrateShardRequest,
    fetcher: Arc<Fetcher>,
    mounts: Arc<HashMap<String, MountConfig>>,
) -> HydrateShardResponse {
    let t0 = Instant::now();
    let start_unix_ms = now_unix_ms();
    let mount = match mounts.get(&req.mount) {
        Some(m) => m.clone(),
        None => {
            return HydrateShardResponse {
                fetched: 0,
                bytes: 0,
                errors: vec![format!("unknown mount {}", req.mount)],
                elapsed_ms: 0,
                start_unix_ms,
                end_unix_ms: now_unix_ms(),
            };
        }
    };
    let mut handles = Vec::with_capacity(req.chunks.len());
    for c in req.chunks {
        let f = fetcher.clone();
        let m = mount.clone();
        handles.push(tokio::spawn(async move {
            let permit = match f.acquire_chunk_permit().await {
                Ok(p) => p,
                Err(e) => return (c, Err(e)),
            };
            let res = f
                .fetch_chunk_origin_only(&m, &c.blob, c.offset, c.len)
                .await;
            drop(permit);
            (c, res)
        }));
    }
    let mut fetched = 0u64;
    let mut bytes = 0u64;
    let mut errors = Vec::new();
    for h in handles {
        match h.await {
            Ok((c, Ok(b))) => {
                fetched += 1;
                bytes += b.len() as u64;
                let _ = c;
            }
            Ok((c, Err(e))) => {
                if errors.len() < 32 {
                    errors.push(format!("{}@{}: {e}", c.blob, c.offset));
                }
            }
            Err(e) => {
                if errors.len() < 32 {
                    errors.push(format!("join: {e}"));
                }
            }
        }
    }
    // Wait for all backgrounded cache.insert tasks to finish writing to NVMe
    // (tmp+fsync+rename) so elapsed_ms reflects on-disk completion, matching
    // azcp's wall-clock semantics (azcp writes synchronously before returning).
    fetcher.await_inserts_drained().await;
    HydrateShardResponse {
        fetched,
        bytes,
        errors,
        elapsed_ms: t0.elapsed().as_millis() as u64,
        start_unix_ms,
        end_unix_ms: now_unix_ms(),
    }
}

/// Coordinator side: list blobs, enumerate chunks, round-robin shard to
/// alive peers (including self), POST each peer their batch in parallel,
/// and aggregate results.
pub async fn run_coordinator(
    req: HydrateRequest,
    chunk_size: u64,
    fetcher: Arc<Fetcher>,
    blobs: Arc<HashMap<String, Arc<BlobClient>>>,
    mounts: Arc<HashMap<String, MountConfig>>,
    membership: Membership,
    me_id: String,
    me_transport_url: String,
    http: reqwest::Client,
) -> Result<HydrateResponse> {
    let t0 = Instant::now();
    let mount = mounts
        .get(&req.mount)
        .cloned()
        .ok_or_else(|| BcError::Other(format!("unknown mount {}", req.mount)))?;
    let blob = blobs
        .get(&req.mount)
        .cloned()
        .ok_or_else(|| BcError::Other(format!("no blob client for mount {}", req.mount)))?;

    let recursive = req.recursive.unwrap_or(true);
    let combined_prefix = if mount.prefix.is_empty() {
        req.path.clone()
    } else if req.path.is_empty() {
        mount.prefix.clone()
    } else {
        format!(
            "{}{}",
            mount.prefix.trim_end_matches('/'),
            if req.path.starts_with('/') {
                req.path.clone()
            } else {
                format!("/{}", req.path)
            }
        )
    };
    let prefix_opt = if combined_prefix.is_empty() {
        None
    } else {
        Some(combined_prefix.as_str())
    };
    let (listed, _prefixes) = blob
        .list_blobs(&mount.account, &mount.container, prefix_opt, recursive)
        .await?;

    let mut all_chunks: Vec<ChunkSpec> = Vec::new();
    let mut total_files = 0u64;
    for b in &listed {
        if b.content_length == 0 {
            continue;
        }
        total_files += 1;
        let blob_path = if !mount.prefix.is_empty() && b.name.starts_with(&mount.prefix) {
            b.name[mount.prefix.len()..]
                .trim_start_matches('/')
                .to_string()
        } else {
            b.name.clone()
        };
        let mut off = 0u64;
        while off < b.content_length {
            let len = chunk_size.min(b.content_length - off);
            all_chunks.push(ChunkSpec {
                blob: blob_path.clone(),
                offset: off,
                len,
            });
            off += len;
        }
    }
    let total_chunks = all_chunks.len() as u64;
    if total_chunks == 0 {
        return Ok(HydrateResponse {
            mount: req.mount,
            path: req.path,
            total_files: 0,
            total_chunks: 0,
            total_bytes: 0,
            elapsed_ms: t0.elapsed().as_millis() as u64,
            aggregate_mibs: 0.0,
            peers: Vec::new(),
        });
    }

    // Collect alive peers (members_alive excludes self by design); add self
    // first so chunk 0 always lands locally and uneven N-distribution is
    // deterministic for benchmarking.
    let mut targets: Vec<(String, Option<String>)> = vec![(me_id.clone(), None)];
    for n in membership.members_all() {
        if matches!(n.state, NodeState::Alive) && n.id != me_id {
            targets.push((n.id.clone(), Some(n.transport_url.clone())));
        }
    }
    let n_targets = targets.len();
    let mut buckets: Vec<Vec<ChunkSpec>> = (0..n_targets).map(|_| Vec::new()).collect();
    for (i, c) in all_chunks.into_iter().enumerate() {
        buckets[i % n_targets].push(c);
    }

    let mut handles = Vec::with_capacity(n_targets);
    for ((node_id, transport_url), chunks) in targets.into_iter().zip(buckets.into_iter()) {
        let assigned = chunks.len() as u64;
        let mount_name = req.mount.clone();
        if transport_url.is_none() {
            // Local shard — call run_shard directly to avoid an HTTP self-call.
            let f = fetcher.clone();
            let m = mounts.clone();
            let me_url = me_transport_url.clone();
            handles.push(tokio::spawn(async move {
                let r = run_shard(
                    HydrateShardRequest {
                        mount: mount_name,
                        chunks,
                    },
                    f,
                    m,
                )
                .await;
                let _ = me_url;
                PerPeerStats {
                    node_id,
                    assigned_chunks: assigned,
                    fetched: r.fetched,
                    bytes: r.bytes,
                    errors: r.errors,
                    elapsed_ms: r.elapsed_ms,
                    start_unix_ms: r.start_unix_ms,
                    end_unix_ms: r.end_unix_ms,
                }
            }));
        } else {
            // Remote shard — POST /hydrate-shard to peer's stats endpoint
            // (port derived from gossip_url's host + the conventional :7773;
            // we reuse transport_url's host since both gossip and transport
            // are on the same node interface).
            let url = transport_url.unwrap();
            let host = url
                .trim_start_matches("http://")
                .split(':')
                .next()
                .unwrap_or("")
                .to_string();
            let endpoint = format!("http://{host}:7773/hydrate-shard");
            let http = http.clone();
            handles.push(tokio::spawn(async move {
                let body = HydrateShardRequest {
                    mount: mount_name,
                    chunks,
                };
                let t0 = Instant::now();
                let post_start_unix_ms = now_unix_ms();
                let resp = http
                    .post(&endpoint)
                    .json(&body)
                    .timeout(std::time::Duration::from_secs(3600))
                    .send()
                    .await;
                match resp {
                    Ok(r) => match r.json::<HydrateShardResponse>().await {
                        Ok(s) => PerPeerStats {
                            node_id,
                            assigned_chunks: assigned,
                            fetched: s.fetched,
                            bytes: s.bytes,
                            errors: s.errors,
                            elapsed_ms: s.elapsed_ms,
                            start_unix_ms: s.start_unix_ms,
                            end_unix_ms: s.end_unix_ms,
                        },
                        Err(e) => PerPeerStats {
                            node_id,
                            assigned_chunks: assigned,
                            fetched: 0,
                            bytes: 0,
                            errors: vec![format!("decode: {e}")],
                            elapsed_ms: t0.elapsed().as_millis() as u64,
                            start_unix_ms: post_start_unix_ms,
                            end_unix_ms: now_unix_ms(),
                        },
                    },
                    Err(e) => PerPeerStats {
                        node_id,
                        assigned_chunks: assigned,
                        fetched: 0,
                        bytes: 0,
                        errors: vec![format!("post: {e}")],
                        elapsed_ms: t0.elapsed().as_millis() as u64,
                        start_unix_ms: post_start_unix_ms,
                        end_unix_ms: now_unix_ms(),
                    },
                }
            }));
        }
    }

    let mut peers = Vec::with_capacity(n_targets);
    // Per-shard remote HTTP timeout is 3600s; cap the whole hydrate at
    // 3700s (override with BLOBCACHE_HYDRATE_TIMEOUT_SECS) so a single
    // wedged local shard or a peer whose HTTP didn't fire can't block
    // /hydrate indefinitely. On timeout we abort outstanding handles and
    // return what completed, marking the rest as errors.
    let global_timeout_secs: u64 = std::env::var("BLOBCACHE_HYDRATE_TIMEOUT_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3700);
    let global_timeout = std::time::Duration::from_secs(global_timeout_secs);
    let abort_handles: Vec<_> = handles.iter().map(|h| h.abort_handle()).collect();
    let join_all = async {
        let mut out = Vec::with_capacity(n_targets);
        for h in handles {
            match h.await {
                Ok(s) => out.push(s),
                Err(e) => out.push(PerPeerStats {
                    node_id: "unknown".into(),
                    assigned_chunks: 0,
                    fetched: 0,
                    bytes: 0,
                    errors: vec![format!("join: {e}")],
                    elapsed_ms: 0,
                    start_unix_ms: 0,
                    end_unix_ms: 0,
                }),
            }
        }
        out
    };
    match tokio::time::timeout(global_timeout, join_all).await {
        Ok(p) => peers = p,
        Err(_) => {
            for ah in abort_handles {
                ah.abort();
            }
            peers.push(PerPeerStats {
                node_id: "coordinator".into(),
                assigned_chunks: 0,
                fetched: 0,
                bytes: 0,
                errors: vec![format!(
                    "coordinator timeout after {global_timeout_secs}s; aborted outstanding shards"
                )],
                elapsed_ms: global_timeout.as_millis() as u64,
                start_unix_ms: 0,
                end_unix_ms: now_unix_ms(),
            });
        }
    }
    let elapsed_ms = t0.elapsed().as_millis() as u64;
    let total_bytes: u64 = peers.iter().map(|p| p.bytes).sum();
    let aggregate_mibs = if elapsed_ms > 0 {
        (total_bytes as f64 / 1024.0 / 1024.0) / (elapsed_ms as f64 / 1000.0)
    } else {
        0.0
    };
    Ok(HydrateResponse {
        mount: req.mount,
        path: req.path,
        total_files,
        total_chunks,
        total_bytes,
        elapsed_ms,
        aggregate_mibs,
        peers,
    })
}
