use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::PathBuf;

use crate::error::{BcError, Result};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub node_id: Option<String>,
    pub cache: CacheConfig,
    #[serde(default)]
    pub azure: AzureConfig,
    pub cluster: ClusterConfig,
    pub transport: TransportConfig,
    pub stats: StatsConfig,
    #[serde(default)]
    pub mounts: Vec<MountConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    pub dir: PathBuf,
    pub max_bytes: u64,
    #[serde(default = "default_chunk_size")]
    pub chunk_size: u64,
    /// When true (default), chunks fetched from a peer are written to the
    /// local on-disk cache so subsequent local reads hit instantly. When
    /// false, peer-fetched chunks are served to the caller but never
    /// inserted, so the local cache only grows from blob fetches (i.e.
    /// hydrate Phase A shards + on-demand blob misses). Useful for
    /// maximising effective cluster cache capacity (no replication) and
    /// for benchmarking peer-fetch throughput without the NVMe-write
    /// path on the critical path. Blob fetches always cache regardless.
    #[serde(default = "default_cache_on_peer_fetch")]
    pub cache_on_peer_fetch: bool,
    /// Bounded in-memory LRU of peer-fetched chunks (bytes). Mirrors the
    /// chunks the disk cache would have held when `cache_on_peer_fetch`
    /// is false, so that within-chunk locality (FUSE issues ~32 sub-reads
    /// per 4 MiB chunk) doesn't trigger a full peer re-fetch per sub-read.
    /// 0 disables. Default 1 GiB. Memory accounting is approximate
    /// (sum of chunk byte lengths); per-entry overhead is negligible.
    #[serde(default = "default_peer_lru_bytes")]
    pub peer_lru_bytes: u64,
}
fn default_chunk_size() -> u64 {
    4 * 1024 * 1024
}
fn default_cache_on_peer_fetch() -> bool {
    true
}
fn default_peer_lru_bytes() -> u64 {
    1024 * 1024 * 1024
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AzureConfig {
    #[serde(default = "default_pool_max_idle_per_host")]
    pub pool_max_idle_per_host: usize,
    /// Bytes per Azure GET. Decoupled from cache.chunk_size: the daemon
    /// issues block-sized GETs to Azure, then slices each block into
    /// `block_size / chunk_size` cache chunks. 0 = use chunk_size.
    /// Must be a positive multiple of cache.chunk_size when non-zero.
    /// Larger blocks reduce per-request overhead and Azure-side throttling
    /// pressure (small chunks at low concurrency see retry storms because
    /// the per-second request rate limit is hit before bandwidth limits).
    #[serde(default)]
    pub block_size: u64,
    /// Number of independent tokio runtimes (each with its own reqwest
    /// connection pool) used to dispatch Azure GETs. Mirrors azcp's
    /// `--workers` knob: a single tokio runtime + reqwest Client tops out
    /// near 28 Gbps regardless of concurrency, so to scale a single node
    /// past that ceiling we need multiple independent runtimes.
    #[serde(default = "default_workers")]
    pub workers: usize,
    /// Worker thread cap for the main tokio runtime that drives FUSE
    /// handlers, gossip, peer-transport server, and stats. Default 8 caps
    /// the work-stealing scheduler atomic-op overhead seen at the
    /// `num_cpus()` default on high-core nodes (≈15 % CPU on GB300 at 128
    /// cores). The blob-fetch runtimes (see `workers`) are sized
    /// independently and not affected by this knob.
    #[serde(default = "default_main_worker_threads")]
    pub main_worker_threads: usize,
}

impl Default for AzureConfig {
    fn default() -> Self {
        Self {
            pool_max_idle_per_host: default_pool_max_idle_per_host(),
            block_size: 0,
            workers: default_workers(),
            main_worker_threads: default_main_worker_threads(),
        }
    }
}

fn default_pool_max_idle_per_host() -> usize {
    512
}

fn default_workers() -> usize {
    1
}

fn default_main_worker_threads() -> usize {
    8
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub bind: String,
    #[serde(default)]
    pub seeds: Vec<String>,
    #[serde(default = "default_advertise")]
    pub advertise: Option<String>,
}
fn default_advertise() -> Option<String> {
    None
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransportConfig {
    pub bind: String,
    #[serde(default)]
    pub advertise: Vec<String>,
    #[serde(default = "default_chunk_concurrency")]
    pub chunk_concurrency: usize,
    #[serde(default = "default_peer_concurrency")]
    pub peer_concurrency: usize,
    #[serde(default = "default_prefetch_depth")]
    pub prefetch_depth: u32,
    #[serde(default = "default_prefetch_threshold")]
    pub prefetch_threshold: u32,
    #[serde(default = "default_prefetch_concurrency")]
    pub prefetch_concurrency: usize,
    /// When true: (1) prefetch only triggers on streams whose recent fetches
    /// came from Azure Blob (a peer-fetch success resets the streak to 0); and
    /// (2) prefetched chunks themselves use the origin-only fetch path,
    /// bypassing peer fan-out and stampede-leader hops. Default false.
    #[serde(default)]
    pub prefetch_origin_only: bool,
    #[serde(default = "default_transport_kind")]
    pub kind: String,
    #[serde(default = "default_bloom_bits")]
    pub bloom_bits: usize,
    #[serde(default = "default_bloom_rebuild_secs")]
    pub bloom_rebuild_secs: u64,
    #[serde(default = "default_bloom_pull_secs")]
    pub bloom_pull_secs: u64,
    #[serde(default = "default_peer_max_candidates")]
    pub peer_max_candidates: usize,
    #[serde(default = "default_peer_max_yes_attempts")]
    pub peer_max_yes_attempts: usize,
    #[serde(default = "default_peer_max_maybe_attempts")]
    pub peer_max_maybe_attempts: usize,
    #[serde(default = "default_stampede_wait_ms")]
    pub stampede_wait_ms: u32,
}
fn default_chunk_concurrency() -> usize {
    32
}
fn default_peer_concurrency() -> usize {
    8
}
fn default_prefetch_depth() -> u32 {
    16
}
fn default_prefetch_threshold() -> u32 {
    3
}
fn default_prefetch_concurrency() -> usize {
    32
}
fn default_transport_kind() -> String {
    "tcp".into()
}
fn default_bloom_bits() -> usize {
    1 << 23
}
fn default_bloom_rebuild_secs() -> u64 {
    30
}
fn default_bloom_pull_secs() -> u64 {
    5
}
fn default_peer_max_candidates() -> usize {
    4
}
fn default_peer_max_yes_attempts() -> usize {
    2
}
fn default_peer_max_maybe_attempts() -> usize {
    2
}
fn default_stampede_wait_ms() -> u32 {
    5000
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatsConfig {
    pub bind: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MountConfig {
    pub name: String,
    pub mountpoint: PathBuf,
    pub account: String,
    pub container: String,
    #[serde(default)]
    pub prefix: String,
    #[serde(default)]
    pub sas_token: Option<String>,
}

impl Config {
    pub fn from_path(path: &std::path::Path) -> Result<Self> {
        let s = std::fs::read_to_string(path)
            .map_err(|e| BcError::Config(format!("read {}: {e}", path.display())))?;
        let cfg: Config = toml::from_str(&s)?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> Result<()> {
        if self.cache.chunk_size == 0 || self.cache.chunk_size % 4096 != 0 {
            return Err(BcError::Config(
                "cache.chunk_size must be multiple of 4096".into(),
            ));
        }
        if self.azure.pool_max_idle_per_host == 0 {
            return Err(BcError::Config(
                "azure.pool_max_idle_per_host must be >= 1".into(),
            ));
        }
        if self.azure.workers == 0 {
            return Err(BcError::Config("azure.workers must be >= 1".into()));
        }
        if self.azure.main_worker_threads == 0 {
            return Err(BcError::Config(
                "azure.main_worker_threads must be >= 1".into(),
            ));
        }
        if self.azure.block_size != 0 {
            if self.azure.block_size < self.cache.chunk_size {
                return Err(BcError::Config(
                    "azure.block_size must be >= cache.chunk_size when set".into(),
                ));
            }
            if self.azure.block_size % self.cache.chunk_size != 0 {
                return Err(BcError::Config(
                    "azure.block_size must be a multiple of cache.chunk_size".into(),
                ));
            }
        }
        if self.mounts.is_empty() {
            return Err(BcError::Config("at least one mount required".into()));
        }
        for m in &self.mounts {
            if m.name.is_empty() || m.account.is_empty() || m.container.is_empty() {
                return Err(BcError::Config(format!(
                    "mount {}: name/account/container required",
                    m.name
                )));
            }
        }
        if !["tcp", "rdma"].contains(&self.transport.kind.as_str()) {
            return Err(BcError::Config(format!(
                "transport.kind must be \"tcp\" or \"rdma\", got {:?}",
                self.transport.kind
            )));
        }
        Ok(())
    }

    pub fn cluster_hash(&self) -> [u8; 32] {
        let mut h = Sha256::new();
        h.update(self.cache.chunk_size.to_le_bytes());
        h.update(self.transport.kind.as_bytes());
        h.update(b"\0");
        let mut mounts = self.mounts.clone();
        mounts.sort_by(|a, b| a.name.cmp(&b.name));
        for m in &mounts {
            h.update(m.name.as_bytes());
            h.update(b"\0");
            h.update(m.account.as_bytes());
            h.update(b"\0");
            h.update(m.container.as_bytes());
            h.update(b"\0");
            h.update(m.prefix.as_bytes());
            h.update(b"\0");
        }
        h.finalize().into()
    }
}
