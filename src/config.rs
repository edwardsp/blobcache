use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::PathBuf;

use crate::error::{BcError, Result};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub node_id: Option<String>,
    pub cache: CacheConfig,
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
}
fn default_chunk_size() -> u64 {
    4 * 1024 * 1024
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
