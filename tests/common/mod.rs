//! Shared test helpers. Kept minimal and dependency-free so that any
//! integration test file can `mod common;` it without dragging in extra
//! crates.
//!
//! Conventions:
//! - Helpers that build config or fake cluster state must use **deterministic
//!   values** so failure modes reproduce. Random values belong in proptest
//!   harnesses, not here.
//! - No `unwrap()` on filesystem ops in helpers - return `tempfile::TempDir`
//!   so the caller controls cleanup.

#![allow(dead_code)]

use std::path::PathBuf;

use blobcache::cluster::{NodeInfo, NodeState};
use blobcache::config::{
    AdminConfig, AzureConfig, CacheConfig, ClusterConfig, Config, MountConfig, StatsConfig,
    TransportConfig,
};

/// Build a minimal valid `Config` rooted at `cache_dir`. Single mount, tcp
/// transport, no seeds. Tests that need to mutate fields should clone this
/// and override only what they care about, so unrelated defaults stay in
/// lock-step with production defaults.
pub fn minimal_config(cache_dir: PathBuf) -> Config {
    Config {
        node_id: Some("test-node".into()),
        cache: CacheConfig {
            dir: cache_dir,
            max_bytes: 1024 * 1024 * 1024,
            chunk_size: 4 * 1024 * 1024,
            cache_on_peer_fetch: true,
            peer_lru_bytes: 0,
        },
        azure: AzureConfig::default(),
        cluster: ClusterConfig {
            bind: "127.0.0.1:0".into(),
            seeds: vec![],
            advertise: None,
        },
        transport: TransportConfig {
            bind: "127.0.0.1:0".into(),
            advertise: vec![],
            chunk_concurrency: 4,
            peer_concurrency: 2,
            prefetch_depth: 0,
            prefetch_threshold: 0,
            prefetch_concurrency: 0,
            prefetch_origin_only: false,
            kind: "tcp".into(),
            bloom_bits: 1 << 16,
            bloom_rebuild_secs: 30,
            bloom_pull_secs: 5,
            peer_max_candidates: 4,
            peer_max_yes_attempts: 2,
            peer_max_maybe_attempts: 2,
            stampede_wait_ms: 1000,
        },
        stats: StatsConfig {
            bind: "127.0.0.1:0".into(),
        },
        admin: AdminConfig::default(),
        mounts: vec![MountConfig {
            name: "models".into(),
            mountpoint: PathBuf::from("/tmp/blobcache-test"),
            account: "acct".into(),
            container: "ctr".into(),
            prefix: String::new(),
            sas_token: None,
        }],
    }
}

/// Build a minimal `NodeInfo` for tests. Only `id` is meaningful for HRW
/// scoring and bloom routing; the URL fields are placeholders that the
/// pure-logic tests never dial.
pub fn node(id: &str) -> NodeInfo {
    NodeInfo {
        id: id.into(),
        transport_url: format!("http://{id}:7772"),
        gossip_url: format!("http://{id}:7771"),
        cluster_hash: "x".into(),
        ucx_worker_addr_b64: None,
        last_seen_unix: 0,
        state: NodeState::Alive,
        incarnation: 1,
        bloom_version: 0,
        admin_url: None,
    }
}
