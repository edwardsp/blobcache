use blobcache::cache::{ChunkKey, DiskCache};
use blobcache::stats::Stats;
use blobcache::transport::{PeerService, TcpPeerClient};
use bytes::Bytes;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

async fn boot_server(
    chunk_provider: Option<blobcache::transport::ChunkProvider>,
) -> (SocketAddr, Arc<DiskCache>, TempDir) {
    let td = TempDir::new().unwrap();
    let cache = DiskCache::open(td.path().to_path_buf(), 16 * 1024 * 1024).unwrap();
    let stats = Stats::new().peer_stats.clone();
    let mut svc = PeerService::new(cache.clone(), [0u8; 32], stats);
    if let Some(p) = chunk_provider {
        svc = svc.with_chunk_provider(p);
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(p) => p,
                Err(_) => return,
            };
            let svc = svc.clone();
            tokio::spawn(async move {
                use hyper::server::conn::http1;
                use hyper::service::service_fn;
                use hyper_util::rt::TokioIo;
                let io = TokioIo::new(stream);
                let _ = http1::Builder::new()
                    .serve_connection(
                        io,
                        service_fn(move |req| {
                            let svc = svc.clone();
                            async move {
                                let resp = svc.handle(req).await;
                                Ok::<_, std::convert::Infallible>(resp)
                            }
                        }),
                    )
                    .await;
            });
        }
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    (addr, cache, td)
}

fn url_for(addr: SocketAddr) -> String {
    format!("http://{addr}")
}

#[tokio::test]
async fn health_returns_200_with_protocol_version() {
    let (addr, _c, _td) = boot_server(None).await;
    let client = TcpPeerClient::new();
    let v = client.health(&url_for(addr)).await.expect("health ok");
    assert_eq!(v["ok"], serde_json::Value::Bool(true));
    assert_eq!(v["version"], serde_json::Value::String("v1".into()));
    assert!(v["cluster"].is_string());
}

#[tokio::test]
async fn fetch_chunk_returns_cached_payload() {
    let (addr, cache, _td) = boot_server(None).await;
    let key = ChunkKey {
        mount: "models".into(),
        blob: "weights/0.bin".into(),
        offset: 0,
    };
    let payload = vec![0xCDu8; 4096];
    cache.insert(key.clone(), &payload).unwrap();

    let client = TcpPeerClient::new();
    let got = client
        .fetch_chunk(&url_for(addr), &key, payload.len() as u32, 0, None)
        .await
        .expect("hit");
    assert_eq!(&got[..], &payload[..]);
}

#[tokio::test]
async fn fetch_chunk_miss_returns_not_found_error() {
    let (addr, _c, _td) = boot_server(None).await;
    let key = ChunkKey {
        mount: "m".into(),
        blob: "absent".into(),
        offset: 0,
    };
    let client = TcpPeerClient::new();
    let err = client
        .fetch_chunk(&url_for(addr), &key, 4096, 0, None)
        .await
        .unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.to_lowercase().contains("not found") || msg.to_lowercase().contains("miss"),
        "expected NotFound-style error, got {msg:?}"
    );
}

#[tokio::test]
async fn fetch_chunk_url_encodes_special_chars_in_mount_and_blob() {
    let (addr, cache, _td) = boot_server(None).await;
    let key = ChunkKey {
        mount: "m space/slash".into(),
        blob: "path with spaces & symbols/0.bin".into(),
        offset: 1024,
    };
    let payload = b"weird-name-payload".to_vec();
    cache.insert(key.clone(), &payload).unwrap();

    let client = TcpPeerClient::new();
    let got = client
        .fetch_chunk(&url_for(addr), &key, payload.len() as u32, 0, None)
        .await
        .expect("hit despite special chars");
    assert_eq!(&got[..], &payload[..]);
}

#[tokio::test]
async fn fetch_chunk_uses_chunk_provider_on_miss_when_wait_ms_set() {
    let provider: blobcache::transport::ChunkProvider = Arc::new(|_key, _len, _wait_ms| {
        Box::pin(async move { Some(Bytes::from_static(b"from-provider")) })
    });
    let (addr, _c, _td) = boot_server(Some(provider)).await;
    let key = ChunkKey {
        mount: "m".into(),
        blob: "miss".into(),
        offset: 0,
    };
    let client = TcpPeerClient::new();
    let got = client
        .fetch_chunk(&url_for(addr), &key, 13, 1000, None)
        .await
        .expect("provider fills the miss");
    assert_eq!(&got[..], b"from-provider");
}

#[tokio::test]
async fn fetch_chunk_skips_provider_when_wait_ms_zero() {
    let provider: blobcache::transport::ChunkProvider = Arc::new(|_, _, _| {
        Box::pin(async move {
            panic!("provider must NOT be called when wait_ms=0");
        })
    });
    let (addr, _c, _td) = boot_server(Some(provider)).await;
    let key = ChunkKey {
        mount: "m".into(),
        blob: "miss".into(),
        offset: 0,
    };
    let client = TcpPeerClient::new();
    let err = client
        .fetch_chunk(&url_for(addr), &key, 4096, 0, None)
        .await
        .unwrap_err();
    let msg = format!("{err}").to_lowercase();
    assert!(msg.contains("not found") || msg.contains("miss"));
}

#[tokio::test]
async fn unknown_path_returns_404() {
    let (addr, _c, _td) = boot_server(None).await;
    let r = reqwest::get(format!("{}/no/such/path", url_for(addr)))
        .await
        .unwrap();
    assert_eq!(r.status(), 404);
}

#[tokio::test]
async fn malformed_chunk_path_returns_400() {
    let (addr, _c, _td) = boot_server(None).await;
    let r = reqwest::get(format!("{}/v1/chunk/", url_for(addr)))
        .await
        .unwrap();
    assert_eq!(r.status(), 400, "missing mount segment");
    let r = reqwest::get(format!("{}/v1/chunk/m?offset=0", url_for(addr)))
        .await
        .unwrap();
    assert_eq!(r.status(), 400, "missing blob query param");
    let r = reqwest::get(format!("{}/v1/chunk/m?blob=b", url_for(addr)))
        .await
        .unwrap();
    assert_eq!(r.status(), 400, "missing offset query param");
    let r = reqwest::get(format!("{}/v1/chunk/m?blob=b&offset=notanumber", url_for(addr)))
        .await
        .unwrap();
    assert_eq!(r.status(), 400, "non-numeric offset");
}

#[tokio::test]
async fn rid_header_is_accepted() {
    let (addr, cache, _td) = boot_server(None).await;
    let key = ChunkKey {
        mount: "m".into(),
        blob: "b".into(),
        offset: 0,
    };
    cache.insert(key.clone(), &vec![1u8; 1024]).unwrap();

    let rid = blobcache::request_id::RequestId::new();
    let client = TcpPeerClient::new();
    let got = client
        .fetch_chunk(&url_for(addr), &key, 1024, 0, Some(&rid))
        .await
        .expect("hit");
    assert_eq!(got.len(), 1024);
}
