// Hydrate: pre-warm the cluster cache for a given mount + path. Coordinator
// node lists blobs, enumerates chunks, round-robin shards them across all
// alive cluster members (including itself), and POSTs each peer their
// per-shard chunk batch via /hydrate-shard. Each worker fetches its assigned
// chunks through the local Fetcher (which inserts them into its own cache
// and updates its bloom). The result: aggregate bandwidth scales ~linearly
// with cluster size, since every node pulls a different ~1/N of the data
// in parallel from Azure.
//
//! NOTE(opus-eval-27): Planned split (deferred to a follow-up PR).
//! This file (1478 LOC) mixes coordinator logic, per-phase orchestration,
//! and wire types.  Proposed layout:
//!   - `hydrate/coordinator.rs`  — top-level fan-out and result aggregation
//!   - `hydrate/phases/shard.rs` — shard-mode phase
//!   - `hydrate/phases/broadcast.rs` — broadcast-mode phase
//!   - `hydrate/phases/ring.rs`  — ring-step phase
//!   - `hydrate/wire.rs`         — request/response serde types
//!
//! Rationale for deferral: zero behavioural change, large diff (~600 LOC
//! moved), would block unrelated PRs during review.

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
#[derive(Default)]
pub enum HydrateMode {
    #[default]
    Default,
    Broadcast,
    Ring,
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
    // Total chunks the receiver was asked to pull (sum of req.sources[].chunks).
    // The coordinator already knows this from the request it built, but echoing
    // it back lets the coordinator detect protocol-version skew and lets
    // operators see the assigned/fetched/errors triple at a glance in logs.
    // Defaulted for backward compat with pre-fix daemons mid-rolling-upgrade.
    #[serde(default)]
    pub assigned: u64,
}

/// Ring-allgather coordinator-driven step request: each /hydrate-ring-step
/// call asks one receiver to pull one specific shard from one specific
/// peer (its prev neighbor) for a single step k. The coordinator dispatches
/// all 17 step-k calls in parallel, waits for all to complete (an explicit
/// barrier), then dispatches step k+1. The barrier guarantees that prev
/// has step-k chunks in cache before step-(k+1) requesters ask for them,
/// so we use wait_ms=0 and avoid the stampede-leader / blob-fetch fallback
/// path entirely.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HydrateRingStepRequest {
    pub mount: String,
    pub step: u32,
    pub source_node_id: String,
    pub prev_node_id: String,
    pub prev_transport_url: String,
    #[serde(default)]
    pub prev_ucx_worker_addr_b64: Option<String>,
    pub chunks: Vec<ChunkSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HydrateRingStepResponse {
    pub fetched: u64,
    pub bytes: u64,
    pub errors: Vec<String>,
    pub pulls_ms: u64,
    pub drain_ms: u64,
    pub start_unix_ms: u64,
    pub end_unix_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RingStepStat {
    pub step: u32,
    pub source_rank: u32,
    pub start_unix_ms: u64,
    pub end_unix_ms: u64,
    pub elapsed_ms: u64,
    pub bytes: u64,
    pub n_chunks: u64,
    pub n_errors: u64,
    pub per_pod: Vec<RingStepPerPod>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RingStepPerPod {
    pub node_id: String,
    pub fetched: u64,
    pub bytes: u64,
    pub pulls_ms: u64,
    pub drain_ms: u64,
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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ring_steps: Vec<RingStepStat>,
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
                .fetch_chunk_origin_only(&m, &c.blob, c.offset, c.len, None)
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
    for (k, e) in fetcher.take_insert_failures() {
        if errors.len() < 32 {
            errors.push(format!("insert: {}@{}: {e}", k.blob, k.offset));
        }
    }
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
#[allow(clippy::too_many_arguments)]
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
            ring_steps: Vec::new(),
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
        .map(
            |((node_id, transport_url, worker_b64), chunks)| BroadcastSource {
                node_id: node_id.clone(),
                transport_url: transport_url
                    .clone()
                    .unwrap_or_else(|| me_transport_url.clone()),
                ucx_worker_addr_b64: worker_b64.clone(),
                chunks: chunks.clone(),
            },
        )
        .collect();

    let phase_a_t0 = Instant::now();
    let mut handles = Vec::with_capacity(n_targets);
    for ((node_id, transport_url, _), chunks) in targets.into_iter().zip(buckets) {
        let assigned = chunks.len() as u64;
        let mount_name = req.mount.clone();
        let Some(url) = transport_url else {
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
            continue;
        };
        {
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
    let mut ring_steps: Vec<RingStepStat> = Vec::new();
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
        tracing::warn!("skipping hydrate Phase B (broadcast) because Phase A reported errors");
    } else if mode == HydrateMode::Ring && peers.iter().all(|p| p.errors.is_empty()) {
        let phase_b_t0 = Instant::now();
        let (peers_out, steps_out) = run_ring_phase(
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
        broadcast_peers = peers_out;
        ring_steps = steps_out;
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
            n_steps = ring_steps.len(),
            n_errors = broadcast_peers
                .iter()
                .map(|p| p.errors.len())
                .sum::<usize>(),
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
        ring_steps,
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
                        Ok(s) => {
                            let mut errors = s.errors;
                            // Cross-check the receiver's view against ours.
                            // s.assigned == 0 means the receiver is pre-fix
                            // (serde default) - skip the strict equality but
                            // still apply the fetched+errors < assigned guard
                            // so partial responses don't slip through.
                            if s.assigned != 0 && s.assigned != assigned {
                                errors.insert(0, format!(
                                    "protocol skew: coordinator assigned={assigned} receiver assigned={}",
                                    s.assigned
                                ));
                            }
                            let accounted = s.fetched.saturating_add(errors.len() as u64);
                            if accounted < assigned {
                                let lost = assigned - accounted;
                                errors.push(format!(
                                    "coordinator silent-loss check: assigned={assigned} fetched={} reported_errors={} lost={lost}",
                                    s.fetched,
                                    errors.len()
                                ));
                            }
                            PerPeerStats {
                                node_id: receiver_id,
                                assigned_chunks: assigned,
                                fetched: s.fetched,
                                bytes: s.bytes,
                                errors,
                                elapsed_ms: s.elapsed_ms,
                                start_unix_ms: s.start_unix_ms,
                                end_unix_ms: s.end_unix_ms,
                            }
                        }
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
                assigned: 0,
            };
        }
    };
    let assigned: u64 = req.sources.iter().map(|s| s.chunks.len() as u64).sum();
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
                            None,
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
    // Silent-loss detection. errors is capped at 32 entries above to keep the
    // response payload bounded, so a shard that failed 1000 chunks would have
    // returned errors.len() == 32 with fetched << assigned and the coordinator
    // could not tell that from a normal partial. Synthesize a single
    // counted-error so n_errors > 0 surfaces in coordinator logs and the
    // response carries an honest accounting.
    let accounted = fetched.saturating_add(errors.len() as u64);
    if accounted < assigned {
        let lost = assigned - accounted;
        errors.push(format!(
            "silent loss: assigned={assigned} fetched={fetched} reported_errors={} lost={lost}",
            errors.len()
        ));
    }
    tracing::info!(
        n_chunks = fetched,
        bytes,
        assigned,
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
        assigned,
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
) -> (Vec<PerPeerStats>, Vec<RingStepStat>) {
    let _ = me_transport_url;
    let mut sorted_plan: Vec<BroadcastSource> = plan.to_vec();
    sorted_plan.sort_by(|a, b| a.node_id.cmp(&b.node_id));
    let world = sorted_plan.len();
    if world < 2 {
        return (Vec::new(), Vec::new());
    }
    let phase_t0 = Instant::now();
    let mut per_pod: HashMap<String, PerPeerStats> = HashMap::with_capacity(world);
    for s in &sorted_plan {
        let assigned: u64 = sorted_plan
            .iter()
            .filter(|p| p.node_id != s.node_id)
            .map(|p| p.chunks.len() as u64)
            .sum();
        per_pod.insert(
            s.node_id.clone(),
            PerPeerStats {
                node_id: s.node_id.clone(),
                assigned_chunks: assigned,
                fetched: 0,
                bytes: 0,
                errors: Vec::new(),
                elapsed_ms: 0,
                start_unix_ms: 0,
                end_unix_ms: 0,
            },
        );
    }
    let mut all_steps: Vec<RingStepStat> = Vec::with_capacity(world - 1);
    for k in 1..world {
        if phase_t0.elapsed() > global_timeout {
            for ps in per_pod.values_mut() {
                ps.errors
                    .push(format!("ring coordinator timeout before step {k}"));
            }
            break;
        }
        let step_t0 = Instant::now();
        let step_start_unix_ms = now_unix_ms();
        let mut handles = Vec::with_capacity(world);
        for (recv_idx, receiver) in sorted_plan.iter().enumerate() {
            let prev_idx = (recv_idx + world - 1) % world;
            let prev = &sorted_plan[prev_idx];
            let source_rank = (recv_idx + world - k) % world;
            let source = &sorted_plan[source_rank];
            let body = HydrateRingStepRequest {
                mount: mount.to_string(),
                step: k as u32,
                source_node_id: source.node_id.clone(),
                prev_node_id: prev.node_id.clone(),
                prev_transport_url: prev.transport_url.clone(),
                prev_ucx_worker_addr_b64: prev.ucx_worker_addr_b64.clone(),
                chunks: source.chunks.clone(),
            };
            let receiver_id = receiver.node_id.clone();
            let receiver_url = receiver.transport_url.clone();
            if receiver_id == me_id {
                let f = fetcher.clone();
                let m = mounts.clone();
                handles.push(tokio::spawn(async move {
                    let r = run_ring_step(body, f, m).await;
                    (receiver_id, r)
                }));
            } else {
                let host = receiver_url
                    .trim_start_matches("http://")
                    .split(':')
                    .next()
                    .unwrap_or("")
                    .to_string();
                let endpoint = format!("http://{host}:7773/hydrate-ring-step");
                let http = http.clone();
                handles.push(tokio::spawn(async move {
                    let post_t0 = now_unix_ms();
                    let resp = http
                        .post(&endpoint)
                        .json(&body)
                        .timeout(std::time::Duration::from_secs(1800))
                        .send()
                        .await;
                    let r = match resp {
                        Ok(r) => match r.json::<HydrateRingStepResponse>().await {
                            Ok(s) => s,
                            Err(e) => HydrateRingStepResponse {
                                fetched: 0,
                                bytes: 0,
                                errors: vec![format!("decode: {e}")],
                                pulls_ms: 0,
                                drain_ms: 0,
                                start_unix_ms: post_t0,
                                end_unix_ms: now_unix_ms(),
                            },
                        },
                        Err(e) => HydrateRingStepResponse {
                            fetched: 0,
                            bytes: 0,
                            errors: vec![format!("post: {e}")],
                            pulls_ms: 0,
                            drain_ms: 0,
                            start_unix_ms: post_t0,
                            end_unix_ms: now_unix_ms(),
                        },
                    };
                    (receiver_id, r)
                }));
            }
        }
        let mut step_bytes = 0u64;
        let mut step_chunks = 0u64;
        let mut step_errors = 0u64;
        let mut per_pod_step: Vec<RingStepPerPod> = Vec::with_capacity(world);
        let remaining = global_timeout.saturating_sub(phase_t0.elapsed());
        let join_all = async {
            let mut out = Vec::with_capacity(world);
            for h in handles {
                match h.await {
                    Ok(t) => out.push(t),
                    Err(e) => out.push((
                        "unknown".into(),
                        HydrateRingStepResponse {
                            fetched: 0,
                            bytes: 0,
                            errors: vec![format!("join: {e}")],
                            pulls_ms: 0,
                            drain_ms: 0,
                            start_unix_ms: 0,
                            end_unix_ms: 0,
                        },
                    )),
                }
            }
            out
        };
        let results = match tokio::time::timeout(remaining, join_all).await {
            Ok(r) => r,
            Err(_) => {
                tracing::error!(step = k, "ring step coordinator timeout");
                for ps in per_pod.values_mut() {
                    ps.errors
                        .push(format!("ring coordinator timeout at step {k}"));
                }
                break;
            }
        };
        for (rid, r) in results {
            step_bytes += r.bytes;
            step_chunks += r.fetched;
            step_errors += r.errors.len() as u64;
            per_pod_step.push(RingStepPerPod {
                node_id: rid.clone(),
                fetched: r.fetched,
                bytes: r.bytes,
                pulls_ms: r.pulls_ms,
                drain_ms: r.drain_ms,
                n_errors: r.errors.len() as u64,
            });
            if let Some(ps) = per_pod.get_mut(&rid) {
                ps.fetched += r.fetched;
                ps.bytes += r.bytes;
                if ps.start_unix_ms == 0 || r.start_unix_ms < ps.start_unix_ms {
                    ps.start_unix_ms = r.start_unix_ms;
                }
                if r.end_unix_ms > ps.end_unix_ms {
                    ps.end_unix_ms = r.end_unix_ms;
                }
                for e in r.errors {
                    if ps.errors.len() < 32 {
                        ps.errors.push(e);
                    }
                }
            }
        }
        let step_elapsed_ms = step_t0.elapsed().as_millis() as u64;
        let step_end_unix_ms = now_unix_ms();
        let mibs = if step_elapsed_ms > 0 {
            (step_bytes as f64 / 1024.0 / 1024.0) / (step_elapsed_ms as f64 / 1000.0)
        } else {
            0.0
        };
        tracing::info!(
            step = k,
            elapsed_ms = step_elapsed_ms,
            bytes = step_bytes,
            n_chunks = step_chunks,
            n_errors = step_errors,
            aggregate_mibs = mibs,
            "ring step barrier complete"
        );
        all_steps.push(RingStepStat {
            step: k as u32,
            source_rank: 0,
            start_unix_ms: step_start_unix_ms,
            end_unix_ms: step_end_unix_ms,
            elapsed_ms: step_elapsed_ms,
            bytes: step_bytes,
            n_chunks: step_chunks,
            n_errors: step_errors,
            per_pod: per_pod_step,
        });
    }
    let total_elapsed = phase_t0.elapsed().as_millis() as u64;
    for ps in per_pod.values_mut() {
        if ps.elapsed_ms == 0 {
            ps.elapsed_ms = total_elapsed;
        }
    }
    let mut peers: Vec<PerPeerStats> = per_pod.into_values().collect();
    peers.sort_by(|a, b| a.node_id.cmp(&b.node_id));
    (peers, all_steps)
}

pub async fn run_ring_step(
    req: HydrateRingStepRequest,
    fetcher: Arc<Fetcher>,
    mounts: Arc<HashMap<String, MountConfig>>,
) -> HydrateRingStepResponse {
    let start_unix_ms = now_unix_ms();
    let pulls_t0 = Instant::now();
    let mount = match mounts.get(&req.mount) {
        Some(m) => m.clone(),
        None => {
            return HydrateRingStepResponse {
                fetched: 0,
                bytes: 0,
                errors: vec![format!("unknown mount {}", req.mount)],
                pulls_ms: 0,
                drain_ms: 0,
                start_unix_ms,
                end_unix_ms: now_unix_ms(),
            };
        }
    };
    let prev_worker_addr: Option<Vec<u8>> = match req.prev_ucx_worker_addr_b64.as_ref() {
        Some(s) => match BASE64_STANDARD.decode(s) {
            Ok(v) => Some(v),
            Err(e) => {
                return HydrateRingStepResponse {
                    fetched: 0,
                    bytes: 0,
                    errors: vec![format!("bad prev worker addr: {e}")],
                    pulls_ms: 0,
                    drain_ms: 0,
                    start_unix_ms,
                    end_unix_ms: now_unix_ms(),
                };
            }
        },
        None => None,
    };
    let chunk_conc = fetcher.chunk_concurrency_limit().max(1);
    let sem = Arc::new(tokio::sync::Semaphore::new(chunk_conc));
    let mut handles = Vec::with_capacity(req.chunks.len());
    for c in req.chunks.into_iter() {
        let f = fetcher.clone();
        let m = mount.clone();
        let peer_id = req.prev_node_id.clone();
        let transport_url = req.prev_transport_url.clone();
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
                    0,
                    None,
                )
                .await;
            drop(permit);
            drop(_ring_permit);
            (c, res)
        }));
    }
    let mut fetched = 0u64;
    let mut bytes = 0u64;
    let mut errors: Vec<String> = Vec::new();
    for h in handles {
        match h.await {
            Ok((_, Ok(b))) => {
                fetched += 1;
                bytes += b.len() as u64;
            }
            Ok((c, Err(e))) => {
                if errors.len() < 32 {
                    errors.push(format!(
                        "step {} from {} {}@{}: {e}",
                        req.step, req.prev_node_id, c.blob, c.offset
                    ));
                }
            }
            Err(e) => {
                if errors.len() < 32 {
                    errors.push(format!("step {} join: {e}", req.step));
                }
            }
        }
    }
    let pulls_ms = pulls_t0.elapsed().as_millis() as u64;
    let drain_t0 = Instant::now();
    fetcher.await_inserts_drained().await;
    let drain_ms = drain_t0.elapsed().as_millis() as u64;
    HydrateRingStepResponse {
        fetched,
        bytes,
        errors,
        pulls_ms,
        drain_ms,
        start_unix_ms,
        end_unix_ms: now_unix_ms(),
    }
}

#[cfg(test)]
mod hydrate_mode_tests {
    use super::*;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn with_env<F: FnOnce()>(value: Option<&str>, f: F) {
        let _g = ENV_LOCK.lock().unwrap();
        let prev = std::env::var("HYDRATE_MODE").ok();
        match value {
            Some(v) => std::env::set_var("HYDRATE_MODE", v),
            None => std::env::remove_var("HYDRATE_MODE"),
        }
        f();
        match prev {
            Some(v) => std::env::set_var("HYDRATE_MODE", v),
            None => std::env::remove_var("HYDRATE_MODE"),
        }
    }

    #[test]
    fn default_when_no_env_and_no_request() {
        with_env(None, || {
            assert_eq!(hydrate_mode(None), HydrateMode::Default);
        });
    }

    #[test]
    fn request_mode_used_when_no_env() {
        with_env(None, || {
            assert_eq!(
                hydrate_mode(Some(HydrateMode::Broadcast)),
                HydrateMode::Broadcast
            );
            assert_eq!(hydrate_mode(Some(HydrateMode::Ring)), HydrateMode::Ring);
            assert_eq!(
                hydrate_mode(Some(HydrateMode::Default)),
                HydrateMode::Default
            );
        });
    }

    #[test]
    fn env_broadcast_overrides_request() {
        with_env(Some("broadcast"), || {
            assert_eq!(hydrate_mode(None), HydrateMode::Broadcast);
            assert_eq!(
                hydrate_mode(Some(HydrateMode::Ring)),
                HydrateMode::Broadcast
            );
            assert_eq!(
                hydrate_mode(Some(HydrateMode::Default)),
                HydrateMode::Broadcast
            );
        });
    }

    #[test]
    fn env_ring_overrides_request() {
        with_env(Some("ring"), || {
            assert_eq!(hydrate_mode(None), HydrateMode::Ring);
            assert_eq!(
                hydrate_mode(Some(HydrateMode::Broadcast)),
                HydrateMode::Ring
            );
        });
    }

    #[test]
    fn env_is_case_insensitive() {
        with_env(Some("BROADCAST"), || {
            assert_eq!(hydrate_mode(None), HydrateMode::Broadcast);
        });
        with_env(Some("Ring"), || {
            assert_eq!(hydrate_mode(None), HydrateMode::Ring);
        });
    }

    #[test]
    fn env_unknown_value_falls_back_to_request_mode() {
        with_env(Some("nonsense"), || {
            assert_eq!(hydrate_mode(None), HydrateMode::Default);
            assert_eq!(hydrate_mode(Some(HydrateMode::Ring)), HydrateMode::Ring);
        });
    }

    #[test]
    fn env_empty_string_falls_back() {
        with_env(Some(""), || {
            assert_eq!(
                hydrate_mode(Some(HydrateMode::Broadcast)),
                HydrateMode::Broadcast
            );
        });
    }

    #[test]
    fn hydrate_mode_serde_lowercase_roundtrip() {
        for (mode, json) in &[
            (HydrateMode::Default, "\"default\""),
            (HydrateMode::Broadcast, "\"broadcast\""),
            (HydrateMode::Ring, "\"ring\""),
        ] {
            let s = serde_json::to_string(mode).unwrap();
            assert_eq!(&s, json);
            let back: HydrateMode = serde_json::from_str(json).unwrap();
            assert_eq!(back, *mode);
        }
    }
}
