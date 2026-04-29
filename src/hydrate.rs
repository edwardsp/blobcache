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
use base64::prelude::{Engine as _, BASE64_STANDARD};
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum HydrateMode {
    Default,
    Broadcast,
    Ring,
}

impl Default for HydrateMode {
    fn default() -> Self {
        Self::Default
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HydrateRequest {
    pub mount: String,
    #[serde(default)]
    pub path: String,
    #[serde(default)]
    pub recursive: Option<bool>,
    #[serde(default)]
    pub mode: Option<HydrateMode>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BroadcastSource {
    pub node_id: String,
    pub transport_url: String,
    #[serde(default)]
    pub ucx_worker_addr_b64: Option<String>,
    pub chunks: Vec<ChunkSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HydrateBroadcastShardRequest {
    pub mount: String,
    pub sources: Vec<BroadcastSource>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HydrateBroadcastShardResponse {
    pub fetched: u64,
    pub bytes: u64,
    pub errors: Vec<String>,
    pub elapsed_ms: u64,
    pub start_unix_ms: u64,
    pub end_unix_ms: u64,
}

/// Ring-allgather request: each receiver gets the full sharded plan
/// (sorted-by-id ordering across the cluster) plus its own node id, and
/// derives its rank / prev neighbor independently. After Phase A every
/// node holds exactly `plan[my_rank].chunks`; the ring then performs
/// N-1 sequential pull steps from the previous neighbor, picking up
/// one shard per step until all N shards are local. Pull-based, so it
/// reuses the existing `RdmaPeerClient::fetch_chunk` (with stampede-wait)
/// and needs no separate transport. See `run_ring_shard` for the
/// per-step algorithm.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HydrateRingShardRequest {
    pub mount: String,
    pub plan: Vec<BroadcastSource>,
    pub my_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HydrateRingShardResponse {
    pub fetched: u64,
    pub bytes: u64,
    pub errors: Vec<String>,
    pub elapsed_ms: u64,
    pub start_unix_ms: u64,
    pub end_unix_ms: u64,
    /// Per-step (pulls_ms, drain_ms, bytes) so coordinator can attribute
    /// where ring time was spent.
    pub steps: Vec<RingStepStat>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RingStepStat {
    pub step: u32,
    pub source_rank: u32,
    pub pulls_ms: u64,
    pub drain_ms: u64,
    pub bytes: u64,
    pub n_chunks: u64,
    pub n_errors: u64,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<HydrateMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase_a_elapsed_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase_b_elapsed_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub broadcast_peers: Vec<PerPeerStats>,
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
    let mode = hydrate_mode(req.mode);
    tracing::info!(
        mount = %req.mount,
        path = %req.path,
        mode = ?mode,
        "hydrate request received"
    );
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
            mode: Some(mode),
            phase_a_elapsed_ms: None,
            phase_b_elapsed_ms: None,
            broadcast_peers: Vec::new(),
        });
    }

    // Each target is (node_id, Option<transport_url> -- None means self,
    // Option<ucx_worker_addr_b64>). Self goes first so chunk 0 lands locally
    // and uneven N-distribution is deterministic for benchmarking.
    let alive_members = membership.members_all();
    let me_worker_b64 = alive_members
        .iter()
        .find(|n| n.id == me_id)
        .and_then(|n| n.ucx_worker_addr_b64.clone());
    let mut targets: Vec<(String, Option<String>, Option<String>)> =
        vec![(me_id.clone(), None, me_worker_b64.clone())];
    for n in &alive_members {
        if matches!(n.state, NodeState::Alive) && n.id != me_id {
            targets.push((
                n.id.clone(),
                Some(n.transport_url.clone()),
                n.ucx_worker_addr_b64.clone(),
            ));
        }
    }
    let n_targets = targets.len();
    let mut buckets: Vec<Vec<ChunkSpec>> = (0..n_targets).map(|_| Vec::new()).collect();
    for (i, c) in all_chunks.into_iter().enumerate() {
        buckets[i % n_targets].push(c);
    }

    // Snapshot the full sharding plan before consuming buckets so Phase B
    // (broadcast) can tell each receiver which peer owns each chunk.
    let plan: Vec<BroadcastSource> = targets
        .iter()
        .zip(buckets.iter())
        .map(|((node_id, transport_url, worker_b64), chunks)| BroadcastSource {
            node_id: node_id.clone(),
            transport_url: transport_url
                .clone()
                .unwrap_or_else(|| me_transport_url.clone()),
            ucx_worker_addr_b64: worker_b64.clone(),
            chunks: chunks.clone(),
        })
        .collect();

    let phase_a_t0 = Instant::now();
    let mut handles = Vec::with_capacity(n_targets);
    for ((node_id, transport_url, _), chunks) in targets.into_iter().zip(buckets.into_iter()) {
        let assigned = chunks.len() as u64;
        let mount_name = req.mount.clone();
        if transport_url.is_none() {
            let f = fetcher.clone();
            let m = mounts.clone();
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
    let phase_a_elapsed_ms = phase_a_t0.elapsed().as_millis() as u64;
    let phase_a_bytes: u64 = peers.iter().map(|p| p.bytes).sum();
    let phase_a_mibs = if phase_a_elapsed_ms > 0 {
        (phase_a_bytes as f64 / 1024.0 / 1024.0) / (phase_a_elapsed_ms as f64 / 1000.0)
    } else {
        0.0
    };
    tracing::info!(
        elapsed_ms = phase_a_elapsed_ms,
        bytes = phase_a_bytes,
        aggregate_mibs = phase_a_mibs,
        n_peers = peers.len(),
        n_errors = peers.iter().map(|p| p.errors.len()).sum::<usize>(),
        "hydrate phase A complete"
    );

    let mut broadcast_peers: Vec<PerPeerStats> = Vec::new();
    let mut phase_b_elapsed_ms: Option<u64> = None;
    if mode == HydrateMode::Broadcast && peers.iter().all(|p| p.errors.is_empty()) {
        let phase_b_t0 = Instant::now();
        broadcast_peers = run_broadcast_phase(
            &req.mount,
            &plan,
            &me_id,
            &me_transport_url,
            fetcher.clone(),
            mounts.clone(),
            http.clone(),
            global_timeout,
        )
        .await;
        phase_b_elapsed_ms = Some(phase_b_t0.elapsed().as_millis() as u64);
        let phase_b_bytes: u64 = broadcast_peers.iter().map(|p| p.bytes).sum();
        let phase_b_mibs = if let Some(ms) = phase_b_elapsed_ms {
            if ms > 0 {
                (phase_b_bytes as f64 / 1024.0 / 1024.0) / (ms as f64 / 1000.0)
            } else {
                0.0
            }
        } else {
            0.0
        };
        tracing::info!(
            elapsed_ms = phase_b_elapsed_ms.unwrap_or(0),
            bytes = phase_b_bytes,
            aggregate_mibs = phase_b_mibs,
            n_targets = broadcast_peers.len(),
            n_errors = broadcast_peers
                .iter()
                .map(|p| p.errors.len())
                .sum::<usize>(),
            "hydrate phase B (broadcast) complete"
        );
    } else if mode == HydrateMode::Broadcast {
        tracing::warn!(
            "skipping hydrate Phase B (broadcast) because Phase A reported errors"
        );
    } else if mode == HydrateMode::Ring && peers.iter().all(|p| p.errors.is_empty()) {
        let phase_b_t0 = Instant::now();
        broadcast_peers = run_ring_phase(
            &req.mount,
            &plan,
            &me_id,
            &me_transport_url,
            fetcher.clone(),
            mounts.clone(),
            http.clone(),
            global_timeout,
        )
        .await;
        phase_b_elapsed_ms = Some(phase_b_t0.elapsed().as_millis() as u64);
        let phase_b_bytes: u64 = broadcast_peers.iter().map(|p| p.bytes).sum();
        let phase_b_mibs = if let Some(ms) = phase_b_elapsed_ms {
            if ms > 0 {
                (phase_b_bytes as f64 / 1024.0 / 1024.0) / (ms as f64 / 1000.0)
            } else {
                0.0
            }
        } else {
            0.0
        };
        tracing::info!(
            elapsed_ms = phase_b_elapsed_ms.unwrap_or(0),
            bytes = phase_b_bytes,
            aggregate_mibs = phase_b_mibs,
            n_targets = broadcast_peers.len(),
            n_errors = broadcast_peers.iter().map(|p| p.errors.len()).sum::<usize>(),
            "hydrate phase B (ring) complete"
        );
    } else if mode == HydrateMode::Ring {
        tracing::warn!("skipping hydrate Phase B (ring) because Phase A reported errors");
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
        mode: Some(mode),
        phase_a_elapsed_ms: Some(phase_a_elapsed_ms),
        phase_b_elapsed_ms,
        broadcast_peers,
    })
}

fn hydrate_mode(request_mode: Option<HydrateMode>) -> HydrateMode {
    if let Ok(s) = std::env::var("HYDRATE_MODE") {
        match s.to_ascii_lowercase().as_str() {
            "broadcast" => return HydrateMode::Broadcast,
            "ring" => return HydrateMode::Ring,
            _ => {}
        }
    }
    request_mode.unwrap_or_default()
}

#[allow(clippy::too_many_arguments)]
async fn run_broadcast_phase(
    mount: &str,
    plan: &[BroadcastSource],
    me_id: &str,
    me_transport_url: &str,
    fetcher: Arc<Fetcher>,
    mounts: Arc<HashMap<String, MountConfig>>,
    http: reqwest::Client,
    global_timeout: std::time::Duration,
) -> Vec<PerPeerStats> {
    let n_targets = plan.len();
    let mut handles = Vec::with_capacity(n_targets);
    for receiver in plan.iter() {
        let receiver_id = receiver.node_id.clone();
        let receiver_url = receiver.transport_url.clone();
        let sources: Vec<BroadcastSource> = plan
            .iter()
            .filter(|s| s.node_id != receiver_id)
            .cloned()
            .collect();
        let assigned: u64 = sources.iter().map(|s| s.chunks.len() as u64).sum();
        let body = HydrateBroadcastShardRequest {
            mount: mount.to_string(),
            sources,
        };
        if receiver_id == me_id {
            let f = fetcher.clone();
            let m = mounts.clone();
            handles.push(tokio::spawn(async move {
                let r = run_broadcast_shard(body, f, m).await;
                PerPeerStats {
                    node_id: receiver_id,
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
            let host = receiver_url
                .trim_start_matches("http://")
                .split(':')
                .next()
                .unwrap_or("")
                .to_string();
            let endpoint = format!("http://{host}:7773/hydrate-broadcast-shard");
            let http = http.clone();
            handles.push(tokio::spawn(async move {
                let t0 = Instant::now();
                let post_start_unix_ms = now_unix_ms();
                let resp = http
                    .post(&endpoint)
                    .json(&body)
                    .timeout(std::time::Duration::from_secs(3600))
                    .send()
                    .await;
                match resp {
                    Ok(r) => match r.json::<HydrateBroadcastShardResponse>().await {
                        Ok(s) => PerPeerStats {
                            node_id: receiver_id,
                            assigned_chunks: assigned,
                            fetched: s.fetched,
                            bytes: s.bytes,
                            errors: s.errors,
                            elapsed_ms: s.elapsed_ms,
                            start_unix_ms: s.start_unix_ms,
                            end_unix_ms: s.end_unix_ms,
                        },
                        Err(e) => PerPeerStats {
                            node_id: receiver_id,
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
                        node_id: receiver_id,
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
    let _ = me_transport_url;
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
        Ok(p) => p,
        Err(_) => {
            for ah in abort_handles {
                ah.abort();
            }
            vec![PerPeerStats {
                node_id: "coordinator".into(),
                assigned_chunks: 0,
                fetched: 0,
                bytes: 0,
                errors: vec![format!(
                    "phase-b coordinator timeout after {}s; aborted outstanding shards",
                    global_timeout.as_secs()
                )],
                elapsed_ms: global_timeout.as_millis() as u64,
                start_unix_ms: 0,
                end_unix_ms: now_unix_ms(),
            }]
        }
    }
}

pub async fn run_broadcast_shard(
    req: HydrateBroadcastShardRequest,
    fetcher: Arc<Fetcher>,
    mounts: Arc<HashMap<String, MountConfig>>,
) -> HydrateBroadcastShardResponse {
    let t0 = Instant::now();
    let start_unix_ms = now_unix_ms();
    let mount = match mounts.get(&req.mount) {
        Some(m) => m.clone(),
        None => {
            return HydrateBroadcastShardResponse {
                fetched: 0,
                bytes: 0,
                errors: vec![format!("unknown mount {}", req.mount)],
                elapsed_ms: 0,
                start_unix_ms,
                end_unix_ms: now_unix_ms(),
            };
        }
    };
    // Round-robin chunks across sources so concurrent permits land on
    // different peers (run #9 found that a source-major ordering pinned every
    // receiver to the same source[0] first, serialising 16 sources through one
    // peer endpoint and stalling Phase B at ~2 fetches/min). Each source also
    // gets its own per-source semaphore (≤ chunk_concurrency / sources) to keep
    // any single UCX endpoint from monopolising the puller's runtime.
    struct SourceCtx {
        node_id: String,
        transport_url: String,
        worker_addr: Option<Vec<u8>>,
        per_src_sem: Arc<tokio::sync::Semaphore>,
        chunks: std::vec::IntoIter<ChunkSpec>,
    }
    let n_src = req.sources.len().max(1);
    let chunk_conc = fetcher.chunk_concurrency_limit().max(1);
    let per_src_permits = (chunk_conc / n_src).max(1);
    let mut sources: Vec<SourceCtx> = req
        .sources
        .into_iter()
        .map(|src| {
            let worker_addr: Option<Vec<u8>> = match src.ucx_worker_addr_b64.as_ref() {
                Some(s) => match BASE64_STANDARD.decode(s) {
                    Ok(v) => Some(v),
                    Err(e) => {
                        tracing::warn!(
                            peer = %src.node_id,
                            error = %e,
                            "broadcast source has invalid ucx worker addr; will skip its chunks"
                        );
                        None
                    }
                },
                None => None,
            };
            SourceCtx {
                node_id: src.node_id,
                transport_url: src.transport_url,
                worker_addr,
                per_src_sem: Arc::new(tokio::sync::Semaphore::new(per_src_permits)),
                chunks: src.chunks.into_iter(),
            }
        })
        .collect();
    let mut handles = Vec::new();
    let mut any_left = true;
    while any_left {
        any_left = false;
        for s in sources.iter_mut() {
            if let Some(c) = s.chunks.next() {
                any_left = true;
                let f = fetcher.clone();
                let m = mount.clone();
                let peer_id = s.node_id.clone();
                let transport_url = s.transport_url.clone();
                let wa = s.worker_addr.clone();
                let per_src = s.per_src_sem.clone();
                handles.push(tokio::spawn(async move {
                    let _src_permit = match per_src.acquire_owned().await {
                        Ok(p) => p,
                        Err(_) => {
                            return (
                                peer_id,
                                c,
                            Err(crate::error::BcError::Other(
                                "broadcast per-src semaphore closed".into(),
                            )),
                            );
                        }
                    };
                    let permit = match f.acquire_chunk_permit().await {
                        Ok(p) => p,
                        Err(e) => return (peer_id, c, Err(e)),
                    };
                    let res = f
                        .pull_chunk_from_peer(
                            &m,
                            &c.blob,
                            c.offset,
                            c.len,
                            &peer_id,
                            &transport_url,
                            wa.as_deref(),
                        )
                        .await;
                    drop(permit);
                    drop(_src_permit);
                    (peer_id, c, res)
                }));
            }
        }
    }
    let mut fetched = 0u64;
    let mut bytes = 0u64;
    let mut errors = Vec::new();
    let pulls_t0 = Instant::now();
    for h in handles {
        match h.await {
            Ok((_, _, Ok(b))) => {
                fetched += 1;
                bytes += b.len() as u64;
            }
            Ok((p, c, Err(e))) => {
                if errors.len() < 32 {
                    errors.push(format!("from {p} {}@{}: {e}", c.blob, c.offset));
                }
            }
            Err(e) => {
                if errors.len() < 32 {
                    errors.push(format!("join: {e}"));
                }
            }
        }
    }
    let pulls_elapsed_ms = pulls_t0.elapsed().as_millis() as u64;
    let drain_t0 = Instant::now();
    fetcher.await_inserts_drained().await;
    let drain_elapsed_ms = drain_t0.elapsed().as_millis() as u64;
    tracing::info!(
        n_chunks = fetched,
        bytes,
        pulls_ms = pulls_elapsed_ms,
        drain_ms = drain_elapsed_ms,
        n_errors = errors.len(),
        "broadcast shard complete"
    );
    HydrateBroadcastShardResponse {
        fetched,
        bytes,
        errors,
        elapsed_ms: t0.elapsed().as_millis() as u64,
        start_unix_ms,
        end_unix_ms: now_unix_ms(),
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_ring_phase(
    mount: &str,
    plan: &[BroadcastSource],
    me_id: &str,
    me_transport_url: &str,
    fetcher: Arc<Fetcher>,
    mounts: Arc<HashMap<String, MountConfig>>,
    http: reqwest::Client,
    global_timeout: std::time::Duration,
) -> Vec<PerPeerStats> {
    let mut sorted_plan: Vec<BroadcastSource> = plan.to_vec();
    sorted_plan.sort_by(|a, b| a.node_id.cmp(&b.node_id));
    let n_targets = sorted_plan.len();
    let mut handles = Vec::with_capacity(n_targets);
    for receiver in sorted_plan.iter() {
        let receiver_id = receiver.node_id.clone();
        let receiver_url = receiver.transport_url.clone();
        let assigned: u64 = sorted_plan
            .iter()
            .filter(|s| s.node_id != receiver_id)
            .map(|s| s.chunks.len() as u64)
            .sum();
        let body = HydrateRingShardRequest {
            mount: mount.to_string(),
            plan: sorted_plan.clone(),
            my_id: receiver_id.clone(),
        };
        if receiver_id == me_id {
            let f = fetcher.clone();
            let m = mounts.clone();
            handles.push(tokio::spawn(async move {
                let r = run_ring_shard(body, f, m).await;
                PerPeerStats {
                    node_id: receiver_id,
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
            let host = receiver_url
                .trim_start_matches("http://")
                .split(':')
                .next()
                .unwrap_or("")
                .to_string();
            let endpoint = format!("http://{host}:7773/hydrate-ring-shard");
            let http = http.clone();
            handles.push(tokio::spawn(async move {
                let t0 = Instant::now();
                let post_start_unix_ms = now_unix_ms();
                let resp = http
                    .post(&endpoint)
                    .json(&body)
                    .timeout(std::time::Duration::from_secs(3600))
                    .send()
                    .await;
                match resp {
                    Ok(r) => match r.json::<HydrateRingShardResponse>().await {
                        Ok(s) => PerPeerStats {
                            node_id: receiver_id,
                            assigned_chunks: assigned,
                            fetched: s.fetched,
                            bytes: s.bytes,
                            errors: s.errors,
                            elapsed_ms: s.elapsed_ms,
                            start_unix_ms: s.start_unix_ms,
                            end_unix_ms: s.end_unix_ms,
                        },
                        Err(e) => PerPeerStats {
                            node_id: receiver_id,
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
                        node_id: receiver_id,
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
    let _ = me_transport_url;
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
        Ok(p) => p,
        Err(_) => {
            for ah in abort_handles {
                ah.abort();
            }
            vec![PerPeerStats {
                node_id: "coordinator".into(),
                assigned_chunks: 0,
                fetched: 0,
                bytes: 0,
                errors: vec![format!(
                    "phase-b ring coordinator timeout after {}s; aborted outstanding shards",
                    global_timeout.as_secs()
                )],
                elapsed_ms: global_timeout.as_millis() as u64,
                start_unix_ms: 0,
                end_unix_ms: now_unix_ms(),
            }]
        }
    }
}

pub async fn run_ring_shard(
    req: HydrateRingShardRequest,
    fetcher: Arc<Fetcher>,
    mounts: Arc<HashMap<String, MountConfig>>,
) -> HydrateRingShardResponse {
    let t0 = Instant::now();
    let start_unix_ms = now_unix_ms();
    let mount = match mounts.get(&req.mount) {
        Some(m) => m.clone(),
        None => {
            return HydrateRingShardResponse {
                fetched: 0,
                bytes: 0,
                errors: vec![format!("unknown mount {}", req.mount)],
                elapsed_ms: 0,
                start_unix_ms,
                end_unix_ms: now_unix_ms(),
                steps: Vec::new(),
            };
        }
    };
    let world = req.plan.len();
    if world < 2 {
        return HydrateRingShardResponse {
            fetched: 0,
            bytes: 0,
            errors: Vec::new(),
            elapsed_ms: t0.elapsed().as_millis() as u64,
            start_unix_ms,
            end_unix_ms: now_unix_ms(),
            steps: Vec::new(),
        };
    }
    let my_rank = match req.plan.iter().position(|s| s.node_id == req.my_id) {
        Some(r) => r,
        None => {
            return HydrateRingShardResponse {
                fetched: 0,
                bytes: 0,
                errors: vec![format!("ring: my_id {} not in plan", req.my_id)],
                elapsed_ms: t0.elapsed().as_millis() as u64,
                start_unix_ms,
                end_unix_ms: now_unix_ms(),
                steps: Vec::new(),
            };
        }
    };
    let prev_rank = (my_rank + world - 1) % world;
    let prev = &req.plan[prev_rank];
    let prev_worker_addr: Option<Vec<u8>> = match prev.ucx_worker_addr_b64.as_ref() {
        Some(s) => match BASE64_STANDARD.decode(s) {
            Ok(v) => Some(v),
            Err(e) => {
                return HydrateRingShardResponse {
                    fetched: 0,
                    bytes: 0,
                    errors: vec![format!("ring: prev {} bad worker addr: {e}", prev.node_id)],
                    elapsed_ms: t0.elapsed().as_millis() as u64,
                    start_unix_ms,
                    end_unix_ms: now_unix_ms(),
                    steps: Vec::new(),
                };
            }
        },
        None => None,
    };
    tracing::info!(
        world,
        my_rank,
        prev_rank,
        prev_id = %prev.node_id,
        "ring shard starting"
    );
    let chunk_conc = fetcher.chunk_concurrency_limit().max(1);
    let mut total_fetched = 0u64;
    let mut total_bytes = 0u64;
    let mut total_errors: Vec<String> = Vec::new();
    let mut steps: Vec<RingStepStat> = Vec::new();
    for k in 1..world {
        let source_rank = (my_rank + world - k) % world;
        let source_chunks = &req.plan[source_rank].chunks;
        let n_chunks = source_chunks.len();
        let pulls_t0 = Instant::now();
        let sem = Arc::new(tokio::sync::Semaphore::new(chunk_conc));
        let mut handles = Vec::with_capacity(n_chunks);
        for c in source_chunks.iter().cloned() {
            let f = fetcher.clone();
            let m = mount.clone();
            let peer_id = prev.node_id.clone();
            let transport_url = prev.transport_url.clone();
            let wa = prev_worker_addr.clone();
            let sem = sem.clone();
            handles.push(tokio::spawn(async move {
                let _ring_permit = match sem.acquire_owned().await {
                    Ok(p) => p,
                    Err(_) => {
                        return (
                            c,
                            Err(crate::error::BcError::Other(
                                "ring step semaphore closed".into(),
                            )),
                        );
                    }
                };
                let permit = match f.acquire_chunk_permit().await {
                    Ok(p) => p,
                    Err(e) => return (c, Err(e)),
                };
                let res = f
                    .pull_chunk_from_peer_wait(
                        &m,
                        &c.blob,
                        c.offset,
                        c.len,
                        &peer_id,
                        &transport_url,
                        wa.as_deref(),
                        300_000,
                    )
                    .await;
                drop(permit);
                drop(_ring_permit);
                (c, res)
            }));
        }
        let mut step_fetched = 0u64;
        let mut step_bytes = 0u64;
        let mut step_errors = 0u64;
        for h in handles {
            match h.await {
                Ok((_, Ok(b))) => {
                    step_fetched += 1;
                    step_bytes += b.len() as u64;
                }
                Ok((c, Err(e))) => {
                    step_errors += 1;
                    if total_errors.len() < 32 {
                        total_errors.push(format!(
                            "step {k} from {} {}@{}: {e}",
                            prev.node_id, c.blob, c.offset
                        ));
                    }
                }
                Err(e) => {
                    step_errors += 1;
                    if total_errors.len() < 32 {
                        total_errors.push(format!("step {k} join: {e}"));
                    }
                }
            }
        }
        let pulls_elapsed_ms = pulls_t0.elapsed().as_millis() as u64;
        let drain_t0 = Instant::now();
        fetcher.await_inserts_drained().await;
        let drain_elapsed_ms = drain_t0.elapsed().as_millis() as u64;
        tracing::info!(
            step = k,
            source_rank,
            source_id = %req.plan[source_rank].node_id,
            n_chunks = step_fetched,
            bytes = step_bytes,
            pulls_ms = pulls_elapsed_ms,
            drain_ms = drain_elapsed_ms,
            n_errors = step_errors,
            "ring step complete"
        );
        steps.push(RingStepStat {
            step: k as u32,
            source_rank: source_rank as u32,
            pulls_ms: pulls_elapsed_ms,
            drain_ms: drain_elapsed_ms,
            bytes: step_bytes,
            n_chunks: step_fetched,
            n_errors: step_errors,
        });
        total_fetched += step_fetched;
        total_bytes += step_bytes;
    }
    HydrateRingShardResponse {
        fetched: total_fetched,
        bytes: total_bytes,
        errors: total_errors,
        elapsed_ms: t0.elapsed().as_millis() as u64,
        start_unix_ms,
        end_unix_ms: now_unix_ms(),
        steps,
    }
}
