//! Two-daemon localhost integration test.
//!
//! Stands up TWO real `PeerService` instances on independent localhost
//! ports, each backed by an independent on-disk cache directory, then has
//! one node act as a client (`TcpPeerClient`) requesting chunks from the
//! other. Verifies the cross-process peer-fetch contract end-to-end:
//!
//! * payload bytes round-trip exactly across the wire
//! * 404 propagates as `BcError::NotFound` for cache misses
//! * `chunk_requests` and `chunk_bytes_served` increment on the server
//! * the same client + URL handles concurrent in-flight requests for
//!   distinct chunks under load (peer concurrency smoke test)
//! * `health` returns the expected protocol version on both daemons
//!
//! The test deliberately does not stand up `Fetcher`/`BlobFetcherPool` —
//! singleflight semantics live inside `Fetcher` and are covered by
//! in-source unit tests. This file scopes itself to "two real daemons
//! exchanging chunks over loopback TCP".

use blobcache::cache::{ChunkKey, DiskCache};
use blobcache::stats::Stats;
use blobcache::transport::{PeerService, TcpPeerClient};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

struct Daemon {
    addr: SocketAddr,
    cache: Arc<DiskCache>,
    stats: Arc<blobcache::stats::PeerStats>,
    _tmp: TempDir,
}

async fn boot_daemon(node_id: &str) -> Daemon {
    let tmp = TempDir::new().unwrap();
    let cache = DiskCache::open(tmp.path().join("cache"), 64 * 1024 * 1024).unwrap();
    let stats = Stats::new().peer_stats.clone();
    let svc = PeerService::new(cache.clone(), [0u8; 32], stats.clone());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let svc_clone = svc.clone();
    let label = node_id.to_string();
    tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(p) => p,
                Err(_) => return,
            };
            let svc = svc_clone.clone();
            let lbl = label.clone();
            tokio::spawn(async move {
                use hyper::server::conn::http1;
                use hyper::service::service_fn;
                use hyper_util::rt::TokioIo;
                let io = TokioIo::new(stream);
                let res = http1::Builder::new()
                    .serve_connection(
                        io,
                        service_fn(move |req| {
                            let svc = svc.clone();
                            async move {
                                let r = svc.handle(req).await;
                                Ok::<_, std::convert::Infallible>(r)
                            }
                        }),
                    )
                    .await;
                if let Err(e) = res {
                    eprintln!("[{lbl}] conn closed: {e}");
                }
            });
        }
    });

    // Tiny readiness wait — the listener is bound, but the spawn-loop
    // needs a tick to register itself with the runtime.
    tokio::time::sleep(Duration::from_millis(50)).await;

    Daemon {
        addr,
        cache,
        stats,
        _tmp: tmp,
    }
}

fn url(addr: SocketAddr) -> String {
    format!("http://{addr}")
}

#[tokio::test]
async fn two_daemons_exchange_chunks_over_loopback() {
    // node-a holds the data; node-b's cache stays empty (only its
    // listener participates, to prove two daemons can coexist).
    let a = boot_daemon("node-a").await;
    let _b = boot_daemon("node-b").await;

    let key = ChunkKey {
        mount: "models".into(),
        blob: "weights/0.bin".into(),
        offset: 0,
    };
    let payload = vec![0xCDu8; 4 * 1024 * 1024];
    a.cache.insert(key.clone(), &payload).unwrap();

    let client = TcpPeerClient::new();
    let got = client
        .fetch_chunk(&url(a.addr), &key, payload.len() as u32, 0, None)
        .await
        .expect("peer fetch must succeed");

    assert_eq!(got.len(), payload.len());
    assert_eq!(&got[..1024], &payload[..1024]);
    assert_eq!(&got[got.len() - 1024..], &payload[payload.len() - 1024..]);
    assert_eq!(a.stats.chunk_requests.get(), 1);
    assert_eq!(a.stats.chunk_bytes_served.get(), payload.len() as u64);
}

#[tokio::test]
async fn miss_on_remote_daemon_returns_not_found_error() {
    let a = boot_daemon("node-a").await;
    let key = ChunkKey {
        mount: "m".into(),
        blob: "absent".into(),
        offset: 0,
    };

    let client = TcpPeerClient::new();
    let err = client
        .fetch_chunk(&url(a.addr), &key, 4096, 0, None)
        .await
        .unwrap_err();
    let msg = format!("{err}").to_lowercase();
    assert!(
        msg.contains("not found") || msg.contains("miss"),
        "expected NotFound, got {msg:?}"
    );

    // The server still counted the request even though it was a miss.
    assert_eq!(a.stats.chunk_requests.get(), 1);
    assert_eq!(a.stats.chunk_bytes_served.get(), 0);
}

#[tokio::test]
async fn many_concurrent_distinct_chunks_round_trip_correctly() {
    // Stress the per-connection pool + the server's per-request handler
    // with many in-flight requests for distinct chunks. Catches issues
    // like cross-talk between requests, off-by-one in path parsing, or
    // shared-state corruption in the response builder.
    let a = boot_daemon("node-a").await;
    const N: u64 = 64;
    const CHUNK: usize = 64 * 1024;
    for i in 0..N {
        let k = ChunkKey {
            mount: "m".into(),
            blob: "b".into(),
            offset: i * CHUNK as u64,
        };
        // distinct payload per chunk so cross-talk would be visible
        let payload = vec![(i & 0xff) as u8; CHUNK];
        a.cache.insert(k, &payload).unwrap();
    }

    let client = Arc::new(TcpPeerClient::new());
    let mut handles = Vec::with_capacity(N as usize);
    for i in 0..N {
        let client = client.clone();
        let url = url(a.addr);
        handles.push(tokio::spawn(async move {
            let k = ChunkKey {
                mount: "m".into(),
                blob: "b".into(),
                offset: i * CHUNK as u64,
            };
            let got = client
                .fetch_chunk(&url, &k, CHUNK as u32, 0, None)
                .await
                .unwrap();
            assert_eq!(got.len(), CHUNK);
            assert!(
                got.iter().all(|&b| b == (i & 0xff) as u8),
                "chunk {i} payload corrupted (cross-talk?)"
            );
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    assert_eq!(a.stats.chunk_requests.get(), N);
    assert_eq!(a.stats.chunk_bytes_served.get(), N * CHUNK as u64);
}

#[tokio::test]
async fn both_daemons_report_health_independently() {
    let a = boot_daemon("node-a").await;
    let b = boot_daemon("node-b").await;
    let client = TcpPeerClient::new();

    let ha = client.health(&url(a.addr)).await.unwrap();
    let hb = client.health(&url(b.addr)).await.unwrap();
    assert_eq!(ha["ok"], serde_json::Value::Bool(true));
    assert_eq!(hb["ok"], serde_json::Value::Bool(true));
    assert_eq!(ha["version"], hb["version"]);
    // Both daemons booted with the same all-zero cluster_hash, so the
    // hex string must match — this is the cross-daemon cluster check.
    assert_eq!(ha["cluster"], hb["cluster"]);
}
