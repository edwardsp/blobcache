mod common;

use blobcache::config::{Config, MountConfig};
use std::path::PathBuf;
use tempfile::tempdir;

fn cfg() -> Config {
    common::minimal_config(PathBuf::from("/tmp/blobcache-test-cache"))
}

#[test]
fn minimal_config_is_valid() {
    cfg().validate().expect("baseline must validate");
}

#[test]
fn rejects_zero_chunk_size() {
    let mut c = cfg();
    c.cache.chunk_size = 0;
    assert!(c.validate().is_err());
}

#[test]
fn rejects_chunk_size_not_multiple_of_4096() {
    let mut c = cfg();
    c.cache.chunk_size = 4097;
    assert!(c.validate().is_err());
    c.cache.chunk_size = 8192;
    c.validate().expect("8192 is multiple of 4096");
}

#[test]
fn rejects_zero_pool_max_idle() {
    let mut c = cfg();
    c.azure.pool_max_idle_per_host = 0;
    assert!(c.validate().is_err());
}

#[test]
fn rejects_zero_workers() {
    let mut c = cfg();
    c.azure.workers = 0;
    assert!(c.validate().is_err());
}

#[test]
fn rejects_zero_main_worker_threads() {
    let mut c = cfg();
    c.azure.main_worker_threads = 0;
    assert!(c.validate().is_err());
}

#[test]
fn block_size_zero_is_allowed_means_use_chunk_size() {
    let mut c = cfg();
    c.azure.block_size = 0;
    c.validate().expect("0 is the documented sentinel");
}

#[test]
fn block_size_must_be_ge_chunk_size() {
    let mut c = cfg();
    c.cache.chunk_size = 4 * 1024 * 1024;
    c.azure.block_size = 1024 * 1024;
    assert!(c.validate().is_err());
}

#[test]
fn block_size_must_be_multiple_of_chunk_size() {
    let mut c = cfg();
    c.cache.chunk_size = 4 * 1024 * 1024;
    c.azure.block_size = 5 * 1024 * 1024;
    assert!(c.validate().is_err());
    c.azure.block_size = 8 * 1024 * 1024;
    c.validate().expect("8 MiB is multiple of 4 MiB");
}

#[test]
fn rejects_no_mounts() {
    let mut c = cfg();
    c.mounts.clear();
    assert!(c.validate().is_err());
}

#[test]
fn rejects_empty_mount_field() {
    for &(name, account, container) in
        &[("", "a", "c"), ("n", "", "c"), ("n", "a", ""), ("", "", "")]
    {
        let mut c = cfg();
        c.mounts[0].name = name.into();
        c.mounts[0].account = account.into();
        c.mounts[0].container = container.into();
        assert!(
            c.validate().is_err(),
            "expected error for ({name:?}, {account:?}, {container:?})"
        );
    }
}

#[test]
fn rejects_unknown_transport_kind() {
    let mut c = cfg();
    c.transport.kind = "udp".into();
    assert!(c.validate().is_err());
    c.transport.kind = "TCP".into();
    assert!(c.validate().is_err(), "case-sensitive: TCP != tcp");
    c.transport.kind = "tcp".into();
    c.validate().unwrap();
    c.transport.kind = "rdma".into();
    c.validate().unwrap();
}

#[test]
fn config_serde_roundtrip_preserves_all_fields() {
    let c = cfg();
    let s = toml::to_string(&c).expect("serialize");
    let back: Config = toml::from_str(&s).expect("deserialize");
    back.validate().expect("roundtripped config still valid");
    assert_eq!(c.cache.chunk_size, back.cache.chunk_size);
    assert_eq!(c.cache.cache_on_peer_fetch, back.cache.cache_on_peer_fetch);
    assert_eq!(c.cache.peer_lru_bytes, back.cache.peer_lru_bytes);
    assert_eq!(c.azure.workers, back.azure.workers);
    assert_eq!(c.azure.block_size, back.azure.block_size);
    assert_eq!(c.azure.main_worker_threads, back.azure.main_worker_threads);
    assert_eq!(c.transport.kind, back.transport.kind);
    assert_eq!(c.transport.bloom_bits, back.transport.bloom_bits);
    assert_eq!(
        c.transport.peer_max_yes_attempts,
        back.transport.peer_max_yes_attempts
    );
    assert_eq!(
        c.transport.peer_max_maybe_attempts,
        back.transport.peer_max_maybe_attempts
    );
    assert_eq!(
        c.transport.stampede_wait_ms,
        back.transport.stampede_wait_ms
    );
    assert_eq!(c.mounts.len(), back.mounts.len());
}

#[test]
fn omitted_optional_fields_use_documented_defaults() {
    let toml_src = r#"
        node_id = "n1"
        [cache]
        dir = "/tmp/c"
        max_bytes = 1073741824
        [cluster]
        bind = "127.0.0.1:0"
        [transport]
        bind = "127.0.0.1:0"
        [stats]
        bind = "127.0.0.1:0"
        [[mounts]]
        name = "m"
        mountpoint = "/tmp/m"
        account = "a"
        container = "c"
    "#;
    let c: Config = toml::from_str(toml_src).expect("parse");
    assert_eq!(c.cache.chunk_size, 4 * 1024 * 1024);
    assert!(c.cache.cache_on_peer_fetch);
    assert_eq!(c.cache.peer_lru_bytes, 1024 * 1024 * 1024);
    assert_eq!(c.azure.workers, 1);
    assert_eq!(c.azure.block_size, 0);
    assert_eq!(c.azure.main_worker_threads, 8);
    assert_eq!(c.azure.pool_max_idle_per_host, 512);
    assert_eq!(c.transport.kind, "tcp");
    assert_eq!(c.transport.chunk_concurrency, 32);
    assert_eq!(c.transport.peer_concurrency, 8);
    assert_eq!(c.transport.bloom_bits, 1 << 23);
    assert_eq!(c.transport.bloom_rebuild_secs, 30);
    assert_eq!(c.transport.bloom_pull_secs, 5);
    assert_eq!(c.transport.peer_max_candidates, 4);
    assert_eq!(c.transport.peer_max_yes_attempts, 2);
    assert_eq!(c.transport.peer_max_maybe_attempts, 2);
    assert_eq!(c.transport.stampede_wait_ms, 5000);
    assert_eq!(c.transport.prefetch_depth, 16);
    assert_eq!(c.transport.prefetch_threshold, 3);
    assert_eq!(c.transport.prefetch_concurrency, 32);
    assert!(!c.transport.prefetch_origin_only);
    c.validate().expect("defaults must be valid");
}

#[test]
fn from_path_reads_and_validates() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("cfg.toml");
    let src = format!(
        r#"
        node_id = "n1"
        [cache]
        dir = "{}"
        max_bytes = 1073741824
        [cluster]
        bind = "127.0.0.1:0"
        [transport]
        bind = "127.0.0.1:0"
        [stats]
        bind = "127.0.0.1:0"
        [[mounts]]
        name = "m"
        mountpoint = "/tmp/m"
        account = "a"
        container = "c"
        "#,
        dir.path().display()
    );
    std::fs::write(&path, src).unwrap();
    let c = Config::from_path(&path).expect("load");
    assert_eq!(c.mounts.len(), 1);
}

#[test]
fn from_path_rejects_invalid() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("cfg.toml");
    std::fs::write(&path, "not = valid = toml").unwrap();
    assert!(Config::from_path(&path).is_err());
}

#[test]
fn cluster_hash_deterministic() {
    let c = cfg();
    let h1 = c.cluster_hash();
    let h2 = c.cluster_hash();
    assert_eq!(h1, h2);
}

#[test]
fn cluster_hash_changes_with_chunk_size() {
    let mut a = cfg();
    let mut b = cfg();
    a.cache.chunk_size = 4 * 1024 * 1024;
    b.cache.chunk_size = 8 * 1024 * 1024;
    assert_ne!(a.cluster_hash(), b.cluster_hash());
}

#[test]
fn cluster_hash_changes_with_transport_kind() {
    let mut a = cfg();
    let mut b = cfg();
    a.transport.kind = "tcp".into();
    b.transport.kind = "rdma".into();
    assert_ne!(a.cluster_hash(), b.cluster_hash());
}

#[test]
fn cluster_hash_changes_with_mounts() {
    let mut a = cfg();
    let mut b = cfg();
    b.mounts[0].container = "different".into();
    assert_ne!(a.cluster_hash(), b.cluster_hash());
    a.mounts[0].account = "x".into();
    b = cfg();
    assert_ne!(a.cluster_hash(), b.cluster_hash());
    a = cfg();
    a.mounts[0].name = "alpha".into();
    b = cfg();
    b.mounts[0].name = "beta".into();
    assert_ne!(a.cluster_hash(), b.cluster_hash());
    a = cfg();
    a.mounts[0].prefix = "p1".into();
    b = cfg();
    b.mounts[0].prefix = "p2".into();
    assert_ne!(a.cluster_hash(), b.cluster_hash());
}

#[test]
fn cluster_hash_invariant_to_mount_order() {
    let mut a = cfg();
    let mut b = cfg();
    let m2 = MountConfig {
        name: "data".into(),
        mountpoint: PathBuf::from("/tmp/data"),
        account: "acct".into(),
        container: "data".into(),
        prefix: String::new(),
        sas_token: None,
    };
    a.mounts.push(m2.clone());
    b.mounts.insert(0, m2);
    assert_eq!(
        a.cluster_hash(),
        b.cluster_hash(),
        "mount ordering must not affect hash; cluster_hash sorts by name"
    );
}

#[test]
fn cluster_hash_ignores_local_tuning_fields() {
    let baseline = cfg().cluster_hash();

    type Mut = Box<dyn Fn(&mut Config)>;
    let cases: Vec<(&str, Mut)> = vec![
        ("azure.workers", Box::new(|c| c.azure.workers = 8)),
        (
            "azure.pool_max_idle_per_host",
            Box::new(|c| c.azure.pool_max_idle_per_host = 1024),
        ),
        (
            "azure.block_size",
            Box::new(|c| c.azure.block_size = 8 * 1024 * 1024),
        ),
        (
            "azure.main_worker_threads",
            Box::new(|c| c.azure.main_worker_threads = 16),
        ),
        ("cache.max_bytes", Box::new(|c| c.cache.max_bytes = 999)),
        (
            "cache.cache_on_peer_fetch",
            Box::new(|c| c.cache.cache_on_peer_fetch = false),
        ),
        (
            "cache.peer_lru_bytes",
            Box::new(|c| c.cache.peer_lru_bytes = 12345),
        ),
        (
            "cache.dir",
            Box::new(|c| c.cache.dir = PathBuf::from("/different")),
        ),
        (
            "transport.chunk_concurrency",
            Box::new(|c| c.transport.chunk_concurrency = 99),
        ),
        (
            "transport.peer_concurrency",
            Box::new(|c| c.transport.peer_concurrency = 99),
        ),
        (
            "transport.bloom_bits",
            Box::new(|c| c.transport.bloom_bits = 1 << 20),
        ),
        (
            "transport.bloom_rebuild_secs",
            Box::new(|c| c.transport.bloom_rebuild_secs = 1),
        ),
        (
            "transport.bloom_pull_secs",
            Box::new(|c| c.transport.bloom_pull_secs = 1),
        ),
        (
            "transport.peer_max_candidates",
            Box::new(|c| c.transport.peer_max_candidates = 99),
        ),
        (
            "transport.peer_max_yes_attempts",
            Box::new(|c| c.transport.peer_max_yes_attempts = 99),
        ),
        (
            "transport.peer_max_maybe_attempts",
            Box::new(|c| c.transport.peer_max_maybe_attempts = 99),
        ),
        (
            "transport.stampede_wait_ms",
            Box::new(|c| c.transport.stampede_wait_ms = 99),
        ),
        (
            "transport.prefetch_depth",
            Box::new(|c| c.transport.prefetch_depth = 99),
        ),
        (
            "transport.prefetch_threshold",
            Box::new(|c| c.transport.prefetch_threshold = 99),
        ),
        (
            "transport.prefetch_concurrency",
            Box::new(|c| c.transport.prefetch_concurrency = 99),
        ),
        (
            "transport.prefetch_origin_only",
            Box::new(|c| c.transport.prefetch_origin_only = true),
        ),
        (
            "transport.bind",
            Box::new(|c| c.transport.bind = "0.0.0.0:9999".into()),
        ),
        (
            "transport.advertise",
            Box::new(|c| c.transport.advertise = vec!["x".into()]),
        ),
        (
            "cluster.bind",
            Box::new(|c| c.cluster.bind = "0.0.0.0:9999".into()),
        ),
        (
            "cluster.seeds",
            Box::new(|c| c.cluster.seeds = vec!["s".into()]),
        ),
        (
            "cluster.advertise",
            Box::new(|c| c.cluster.advertise = Some("a".into())),
        ),
        (
            "stats.bind",
            Box::new(|c| c.stats.bind = "0.0.0.0:9999".into()),
        ),
        ("node_id", Box::new(|c| c.node_id = Some("other".into()))),
        (
            "mount.mountpoint",
            Box::new(|c| c.mounts[0].mountpoint = PathBuf::from("/other")),
        ),
        (
            "mount.sas_token",
            Box::new(|c| c.mounts[0].sas_token = Some("?sig=...".into())),
        ),
    ];

    for (label, mutate) in cases {
        let mut c = cfg();
        mutate(&mut c);
        assert_eq!(
            baseline,
            c.cluster_hash(),
            "{label} must not affect cluster_hash"
        );
    }
}
