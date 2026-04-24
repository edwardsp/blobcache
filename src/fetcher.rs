use base64::prelude::{Engine as _, BASE64_STANDARD};
use bytes::Bytes;
use parking_lot::Mutex;
use rand::seq::SliceRandom;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, Semaphore};

use crate::azure::BlobClient;
use crate::cache::{ChunkKey, DiskCache};
use crate::cluster::Membership;
use crate::config::MountConfig;
use crate::error::{BcError, Result};
use crate::stats::Stats;
use crate::transport::PeerClient;
// Hard cap on a single fetch_range call. Prevents an unbounded readahead from
// fanning out millions of chunk tasks if a caller passes a bogus length.
const MAX_READ_BYTES: u64 = 64 * 1024 * 1024;

// Singleflight dedupes concurrent fetches of the same chunk: the first caller
// runs the fetch and broadcasts the result (cloned Bytes is cheap) to all
// followers waiting on that key. Without this, N concurrent FUSE reads landing
// on the same uncached chunk would each issue an independent peer/blob fetch.
type InflightTx = broadcast::Sender<std::result::Result<Bytes, String>>;

pub struct Fetcher {
    pub cache: Arc<DiskCache>,
    // Per-mount BlobClient: each mount may have its own resolved credential
    // (one container could be SAS-token, another MSI-bearer). A single shared
    // client would attach the wrong Authorization header to half the calls.
    pub blobs: Arc<HashMap<String, Arc<BlobClient>>>,
    pub peers: Arc<PeerClient>,
    pub membership: Membership,
    pub stats: Arc<Stats>,
    pub chunk_size: u64,
    inflight: Arc<Mutex<HashMap<ChunkKey, InflightTx>>>,
    chunk_sem: Arc<Semaphore>,
}

// RAII guard for the leader slot in `inflight`. On drop (panic, cancel, or
// normal completion if `disarm` was not called) it removes the inflight entry
// and broadcasts an error so followers don't hang forever.
struct LeaderGuard {
    inflight: Arc<Mutex<HashMap<ChunkKey, InflightTx>>>,
    key: ChunkKey,
    armed: bool,
}

impl LeaderGuard {
    fn disarm(mut self) -> Option<InflightTx> {
        self.armed = false;
        let mut g = self.inflight.lock();
        g.remove(&self.key)
    }
}

impl Drop for LeaderGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let mut g = self.inflight.lock();
        if let Some(tx) = g.remove(&self.key) {
            let _ = tx.send(Err("leader cancelled".into()));
        }
    }
}

impl Fetcher {
    pub fn new(
        cache: Arc<DiskCache>,
        blobs: Arc<HashMap<String, Arc<BlobClient>>>,
        peers: Arc<PeerClient>,
        membership: Membership,
        stats: Arc<Stats>,
        chunk_size: u64,
        chunk_concurrency: usize,
    ) -> Self {
        let permits = chunk_concurrency.max(1);
        Self {
            cache,
            blobs,
            peers,
            membership,
            stats,
            chunk_size,
            inflight: Arc::new(Mutex::new(HashMap::new())),
            chunk_sem: Arc::new(Semaphore::new(permits)),
        }
    }

    fn blob_for(&self, mount_name: &str) -> Result<&Arc<BlobClient>> {
        self.blobs
            .get(mount_name)
            .ok_or_else(|| BcError::Other(format!("no blob client for mount {mount_name}")))
    }

    pub async fn fetch_chunk(
        &self,
        mount: &MountConfig,
        blob_path: &str,
        offset: u64,
        expected_len: u64,
    ) -> Result<Bytes> {
        let t_total = std::time::Instant::now();
        let res = self
            .fetch_chunk_inner(mount, blob_path, offset, expected_len)
            .await;
        self.stats
            .chunk_total_seconds
            .observe(t_total.elapsed().as_secs_f64());
        res
    }

    async fn fetch_chunk_inner(
        &self,
        mount: &MountConfig,
        blob_path: &str,
        offset: u64,
        expected_len: u64,
    ) -> Result<Bytes> {
        let key = ChunkKey {
            mount: mount.name.clone(),
            blob: blob_path.to_string(),
            offset,
        };

        // Cache lookup is sync I/O (read + stat); push it to the blocking pool.
        let cache_for_get = self.cache.clone();
        let key_for_get = key.clone();
        let t_get = std::time::Instant::now();
        let cached = tokio::task::spawn_blocking(move || cache_for_get.try_get(&key_for_get))
            .await
            .map_err(|e| BcError::Other(format!("cache get join: {e}")))?;
        self.stats
            .chunk_cache_get_seconds
            .observe(t_get.elapsed().as_secs_f64());
        if let Some(b) = cached {
            if b.len() as u64 == expected_len {
                return Ok(b);
            }
            // Cached chunk has the wrong size (truncated write, partial peer
            // response, or chunk_size config drift). Evict and refetch.
            tracing::warn!(
                key = ?key,
                got = b.len(),
                want = expected_len,
                "evicting cached chunk with wrong length"
            );
            let cache_for_rm = self.cache.clone();
            let key_for_rm = key.clone();
            let _ = tokio::task::spawn_blocking(move || cache_for_rm.remove(&key_for_rm)).await;
        }

        let (leader, mut rx_opt) = {
            let mut g = self.inflight.lock();
            if let Some(tx) = g.get(&key) {
                (false, Some(tx.subscribe()))
            } else {
                let (tx, _rx) = broadcast::channel::<std::result::Result<Bytes, String>>(1);
                g.insert(key.clone(), tx);
                (true, None)
            }
        };

        if !leader {
            self.stats.singleflight_waits.inc();
            let rx = rx_opt.as_mut().expect("follower must have receiver");
            match rx.recv().await {
                Ok(Ok(data)) => {
                    if data.len() as u64 != expected_len {
                        return Err(BcError::Other(format!(
                            "singleflight result wrong len: got {} want {}",
                            data.len(),
                            expected_len
                        )));
                    }
                    return Ok(data);
                }
                Ok(Err(e)) => return Err(BcError::Other(e)),
                Err(_) => {
                    // Leader dropped without sending - retry from cache, then
                    // bail. Caller (fetch_range) can retry the whole chunk.
                    let cache_for_get = self.cache.clone();
                    let key_for_get = key.clone();
                    if let Ok(Some(b)) =
                        tokio::task::spawn_blocking(move || cache_for_get.try_get(&key_for_get))
                            .await
                    {
                        if b.len() as u64 == expected_len {
                            return Ok(b);
                        }
                    }
                    return Err(BcError::Other(
                        "singleflight leader dropped without result".into(),
                    ));
                }
            }
        }

        // We are the leader. The guard ensures the inflight slot is cleared
        // and followers are notified even if do_fetch panics or this future
        // is cancelled.
        let guard = LeaderGuard {
            inflight: self.inflight.clone(),
            key: key.clone(),
            armed: true,
        };
        let result = self.do_fetch(mount, blob_path, &key, expected_len).await;
        if let Some(tx) = guard.disarm() {
            let msg = match &result {
                Ok(b) => Ok(b.clone()),
                Err(e) => Err(e.to_string()),
            };
            let _ = tx.send(msg);
        }
        result
    }

    async fn do_fetch(
        &self,
        mount: &MountConfig,
        blob_path: &str,
        key: &ChunkKey,
        expected_len: u64,
    ) -> Result<Bytes> {
        let alive = self.membership.members_alive();
        let mut shuffled = alive;
        shuffled.shuffle(&mut rand::thread_rng());
        for peer in shuffled.iter().take(3) {
            let worker_addr = match &peer.ucx_worker_addr_b64 {
                Some(encoded) => match BASE64_STANDARD.decode(encoded) {
                    Ok(decoded) => Some(decoded),
                    Err(e) => {
                        tracing::warn!(
                            peer = %peer.id,
                            transport = %peer.transport_url,
                            error = %e,
                            "peer advertised invalid UCX worker address; skipping"
                        );
                        continue;
                    }
                },
                None => match &*self.peers {
                    PeerClient::Tcp(_) => None,
                    #[cfg(feature = "ucx")]
                    PeerClient::Rdma(_) => {
                        tracing::warn!(
                            peer = %peer.id,
                            transport = %peer.transport_url,
                            "rdma peer missing UCX worker address; skipping"
                        );
                        continue;
                    }
                },
            };
            let t_peer = std::time::Instant::now();
            let peer_res = self
                .peers
                .fetch_chunk(
                    &peer.id,
                    &peer.transport_url,
                    worker_addr.as_deref(),
                    key,
                    expected_len as u32,
                )
                .await;
            self.stats
                .chunk_peer_fetch_seconds
                .observe(t_peer.elapsed().as_secs_f64());
            match peer_res
            {
                Ok(data) => {
                    if data.len() as u64 != expected_len {
                        // Peer served a chunk of the wrong size. Don't poison
                        // our cache with it; try the next peer.
                        tracing::warn!(
                            peer = %peer.transport_url,
                            key = ?key,
                            got = data.len(),
                            want = expected_len,
                            "peer chunk wrong length, skipping"
                        );
                        self.stats.peer_fetches_err.inc();
                        continue;
                    }
                    self.stats.peer_fetches_ok.inc();
                    self.stats.peer_fetch_bytes.inc_by(data.len() as u64);
                    let cache = self.cache.clone();
                    let k = key.clone();
                    let d = data.clone();
                    let t_ins = std::time::Instant::now();
                    let _ = tokio::task::spawn_blocking(move || cache.insert(k, &d)).await;
                    self.stats
                        .chunk_cache_insert_seconds
                        .observe(t_ins.elapsed().as_secs_f64());
                    return Ok(data);
                }
                Err(BcError::NotFound(_)) => {
                    self.stats.peer_fetches_miss.inc();
                    continue;
                }
                Err(e) => {
                    tracing::debug!(peer=%peer.transport_url, error=%e, "peer fetch err");
                    self.stats.peer_fetches_err.inc();
                    continue;
                }
            }
        }

        let real_path = if mount.prefix.is_empty() {
            blob_path.to_string()
        } else {
            format!("{}/{}", mount.prefix.trim_end_matches('/'), blob_path)
        };
        let blob_client = self.blob_for(&mount.name)?;
        let data = blob_client
            .get_blob_range(
                &mount.account,
                &mount.container,
                &real_path,
                key.offset,
                self.chunk_size,
            )
            .await?;
        if data.len() as u64 != expected_len {
            // Origin returned a short chunk. This is normal only at EOF, but
            // expected_len was already clipped to file_size by the caller.
            return Err(BcError::Other(format!(
                "blob returned wrong length: got {} want {} for {}@{}",
                data.len(),
                expected_len,
                blob_path,
                key.offset
            )));
        }
        self.stats.blob_fetches.inc();
        self.stats.blob_fetch_bytes.inc_by(data.len() as u64);
        let cache = self.cache.clone();
        let k = key.clone();
        let d = data.clone();
        let t_ins = std::time::Instant::now();
        let _ = tokio::task::spawn_blocking(move || cache.insert(k, &d)).await;
        self.stats
            .chunk_cache_insert_seconds
            .observe(t_ins.elapsed().as_secs_f64());
        Ok(data)
    }

    pub async fn fetch_range(
        &self,
        mount: &MountConfig,
        blob_path: &str,
        offset: u64,
        length: u64,
        file_size: u64,
    ) -> Result<Bytes> {
        if length == 0 {
            return Ok(Bytes::new());
        }
        if length > MAX_READ_BYTES {
            return Err(BcError::Other(format!(
                "read length {} exceeds MAX_READ_BYTES {}",
                length, MAX_READ_BYTES
            )));
        }
        let cs = self.chunk_size;
        let end = offset.saturating_add(length).min(file_size);
        if end <= offset {
            return Ok(Bytes::new());
        }
        let first_chunk = (offset / cs) * cs;
        let last_chunk = ((end - 1) / cs) * cs;

        let mut tasks = Vec::new();
        let mut o = first_chunk;
        while o <= last_chunk {
            let chunk_len = cs.min(file_size - o);
            let mc = mount.clone();
            let bp = blob_path.to_string();
            let me = self.clone_handles();
            let sem = self.chunk_sem.clone();
            tasks.push(tokio::spawn(async move {
                // Bound concurrent chunk fetches across the whole Fetcher.
                let _permit = sem
                    .acquire_owned()
                    .await
                    .map_err(|e| BcError::Other(format!("sem closed: {e}")))?;
                me.fetch_chunk(&mc, &bp, o, chunk_len).await.map(|b| (o, b))
            }));
            o = match o.checked_add(cs) {
                Some(n) => n,
                None => break,
            };
        }
        let mut chunks = Vec::with_capacity(tasks.len());
        for t in tasks {
            let (co, data) = t
                .await
                .map_err(|e| BcError::Other(format!("join: {e}")))??;
            chunks.push((co, data));
        }
        chunks.sort_by_key(|(o, _)| *o);

        let mut out = Vec::with_capacity((end - offset) as usize);
        for (co, data) in chunks {
            let chunk_end = co + data.len() as u64;
            let take_start = offset.max(co) - co;
            let take_end = end.min(chunk_end) - co;
            if take_end > take_start {
                out.extend_from_slice(&data[take_start as usize..take_end as usize]);
            }
        }
        Ok(Bytes::from(out))
    }

    fn clone_handles(&self) -> Self {
        Self {
            cache: self.cache.clone(),
            blobs: self.blobs.clone(),
            peers: self.peers.clone(),
            membership: self.membership.clone(),
            stats: self.stats.clone(),
            chunk_size: self.chunk_size,
            inflight: self.inflight.clone(),
            chunk_sem: self.chunk_sem.clone(),
        }
    }
}
