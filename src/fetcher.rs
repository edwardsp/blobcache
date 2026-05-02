use base64::prelude::{Engine as _, BASE64_STANDARD};
use bytes::Bytes;
use dashmap::DashMap;
use lru::LruCache;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, Semaphore};

use crate::blob_fetcher_pool::BlobFetcherPool;
use crate::cache::{ChunkKey, DiskCache};
use crate::cluster::Membership;
use crate::config::MountConfig;
use crate::error::{BcError, Result};
use crate::peerindex::{key_digest, PeerIndex};
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

// Byte-bounded LRU of peer-fetched chunks. Used when cache_on_peer_fetch=false
// to keep within-chunk locality (FUSE issues ~32 sub-reads per 4 MiB chunk)
// without writing to disk. lru::LruCache is entry-bounded; we wrap it with
// running byte accounting and evict from the back until under the cap.
struct PeerLru {
    inner: LruCache<ChunkKey, Bytes>,
    bytes: u64,
    cap_bytes: u64,
}

impl PeerLru {
    fn new(cap_bytes: u64) -> Self {
        Self {
            inner: LruCache::unbounded(),
            bytes: 0,
            cap_bytes,
        }
    }

    fn get(&mut self, key: &ChunkKey) -> Option<Bytes> {
        self.inner.get(key).cloned()
    }

    fn put(&mut self, key: ChunkKey, val: Bytes) {
        let new_len = val.len() as u64;
        if new_len > self.cap_bytes {
            return;
        }
        if let Some(prev) = self.inner.put(key, val) {
            self.bytes = self.bytes.saturating_sub(prev.len() as u64);
        }
        self.bytes = self.bytes.saturating_add(new_len);
        while self.bytes > self.cap_bytes {
            match self.inner.pop_lru() {
                Some((_, v)) => self.bytes = self.bytes.saturating_sub(v.len() as u64),
                None => break,
            }
        }
    }

    fn clear(&mut self) {
        self.inner.clear();
        self.bytes = 0;
    }
}

pub struct Fetcher {
    pub cache: Arc<DiskCache>,
    pub pool: Arc<BlobFetcherPool>,
    pub peers: Arc<PeerClient>,
    pub membership: Membership,
    pub stats: Arc<Stats>,
    pub chunk_size: u64,
    pub block_size: u64,
    pub peer_index: Arc<PeerIndex>,
    pub peer_max_candidates: usize,
    pub peer_max_yes_attempts: usize,
    pub peer_max_maybe_attempts: usize,
    pub stampede_wait_ms: u32,
    // Mount lookup table so the stampede-leader path (serve_peer_chunk) can
    // reconstruct the blob path from a ChunkKey when no MountConfig is on
    // the call stack (the request arrived from a peer, not from FUSE).
    pub mounts: Arc<HashMap<String, MountConfig>>,
    inflight: Arc<DashMap<ChunkKey, InflightTx>>,
    chunk_sem: Arc<Semaphore>,
    chunk_concurrency: usize,
    inflight_writes: Arc<DashMap<ChunkKey, Bytes>>,
    inflight_writes_drained: Arc<tokio::sync::Notify>,
    insert_failures: Arc<Mutex<Vec<(ChunkKey, String)>>>,
    seq_state: Arc<DashMap<String, SeqState>>,
    prefetch_sem: Arc<Semaphore>,
    prefetch_depth: u32,
    prefetch_threshold: u32,
    prefetch_origin_only: bool,
    cache_on_peer_fetch: bool,
    peer_lru: Option<Arc<Mutex<PeerLru>>>,
    peer_sems: Arc<DashMap<String, Arc<Semaphore>>>,
    peer_concurrency: usize,
}

#[derive(Clone, Copy)]
struct SeqState {
    last_end: u64,
    consecutive: u32,
    blob_streak: u32,
}

// RAII guard for the leader slot in `inflight`. On drop (panic, cancel, or
// normal completion if `disarm` was not called) it removes the inflight entry
// and broadcasts an error so followers don't hang forever.
struct LeaderGuard {
    inflight: Arc<DashMap<ChunkKey, InflightTx>>,
    stats: Arc<Stats>,
    key: ChunkKey,
    armed: bool,
}

impl LeaderGuard {
    fn disarm(mut self) -> Option<InflightTx> {
        self.armed = false;
        let removed = self.inflight.remove(&self.key).map(|(_, v)| v);
        if removed.is_some() {
            self.stats.singleflight_inflight.dec();
        }
        removed
    }
}

impl Drop for LeaderGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if let Some((_, tx)) = self.inflight.remove(&self.key) {
            self.stats.singleflight_inflight.dec();
            let _ = tx.send(Err("leader cancelled".into()));
        }
    }
}

// RAII wrapper around an OwnedSemaphorePermit that increments
// stats.chunk_permits_in_use on creation and decrements on drop, so the
// gauge stays consistent across cancellation, panic, and normal completion.
pub struct ChunkPermit {
    _permit: tokio::sync::OwnedSemaphorePermit,
    stats: Arc<Stats>,
}

impl Drop for ChunkPermit {
    fn drop(&mut self) {
        self.stats.chunk_permits_in_use.dec();
    }
}

impl Fetcher {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cache: Arc<DiskCache>,
        pool: Arc<BlobFetcherPool>,
        peers: Arc<PeerClient>,
        membership: Membership,
        stats: Arc<Stats>,
        chunk_size: u64,
        configured_block_size: u64,
        chunk_concurrency: usize,
        prefetch_depth: u32,
        prefetch_threshold: u32,
        prefetch_concurrency: usize,
        prefetch_origin_only: bool,
        cache_on_peer_fetch: bool,
        peer_lru_bytes: u64,
        peer_index: Arc<PeerIndex>,
        peer_max_candidates: usize,
        peer_max_yes_attempts: usize,
        peer_max_maybe_attempts: usize,
        stampede_wait_ms: u32,
        mounts: Arc<HashMap<String, MountConfig>>,
        peer_concurrency: usize,
    ) -> Self {
        let permits = chunk_concurrency.max(1);
        let pf_permits = prefetch_concurrency.max(1);
        let block_size = if configured_block_size == 0 {
            chunk_size
        } else {
            configured_block_size
        };
        Self {
            cache,
            pool,
            peers,
            membership,
            stats,
            chunk_size,
            block_size,
            peer_index,
            peer_max_candidates: peer_max_candidates.max(1),
            peer_max_yes_attempts: peer_max_yes_attempts.max(1),
            peer_max_maybe_attempts: peer_max_maybe_attempts.max(1),
            stampede_wait_ms,
            mounts,
            inflight: Arc::new(DashMap::new()),
            chunk_sem: Arc::new(Semaphore::new(permits)),
            chunk_concurrency: permits,
            inflight_writes: Arc::new(DashMap::new()),
            inflight_writes_drained: Arc::new(tokio::sync::Notify::new()),
            insert_failures: Arc::new(Mutex::new(Vec::new())),
            seq_state: Arc::new(DashMap::new()),
            prefetch_sem: Arc::new(Semaphore::new(pf_permits)),
            prefetch_depth,
            prefetch_threshold: prefetch_threshold.max(1),
            prefetch_origin_only,
            cache_on_peer_fetch,
            peer_lru: if peer_lru_bytes > 0 {
                Some(Arc::new(Mutex::new(PeerLru::new(peer_lru_bytes))))
            } else {
                None
            },
            peer_sems: Arc::new(DashMap::new()),
            peer_concurrency: peer_concurrency.max(1),
        }
    }

    pub async fn fetch_chunk(
        &self,
        mount: &MountConfig,
        blob_path: &str,
        offset: u64,
        expected_len: u64,
        rid: Option<&crate::request_id::RequestId>,
    ) -> Result<Bytes> {
        let t_total = std::time::Instant::now();
        let res = self
            .fetch_chunk_inner(mount, blob_path, offset, expected_len, false, rid)
            .await;
        self.stats
            .chunk_total_seconds
            .observe(t_total.elapsed().as_secs_f64());
        res
    }

    /// Origin-only fetch: cache lookup → singleflight → direct Azure GET,
    /// bypassing the peer-pull and stampede-leader coordination paths in
    /// `do_fetch`. Used by hydrate, where every node already has a disjoint
    /// shard assignment from the coordinator: routing to peers would cause
    /// (a) ~2x cache duplication (peer serves a chunk it was about to fetch
    /// itself, then the requester also writes through to its own cache), and
    /// (b) up to `stampede_wait_ms` of dead time per chunk while the HRW-top
    /// peer's `serve_peer_chunk` blocks waiting for a cold cache that never
    /// fills (because the HRW peer is also fetching its own shard, not this
    /// one). The cache.insert + bloom update still happen, so subsequent
    /// FUSE reads on other nodes find the chunk via the normal peer path.
    pub async fn fetch_chunk_origin_only(
        &self,
        mount: &MountConfig,
        blob_path: &str,
        offset: u64,
        expected_len: u64,
        rid: Option<&crate::request_id::RequestId>,
    ) -> Result<Bytes> {
        let t_total = std::time::Instant::now();
        let res = self
            .fetch_chunk_inner(mount, blob_path, offset, expected_len, true, rid)
            .await;
        self.stats
            .chunk_total_seconds
            .observe(t_total.elapsed().as_secs_f64());
        res
    }

    /// Acquire a `chunk_concurrency` permit. Used by hydrate to gate the
    /// per-shard fan-out (which spawns one task per assigned chunk and would
    /// otherwise issue an unbounded burst of in-flight blob GETs through a
    /// single reqwest pool — see `hydrate::run_shard`). The FUSE read fan-out
    /// (`fetch_chunk_range`) and the prefetch worker acquire the same
    /// semaphore directly, so all three paths share the per-pod `chunk_sem`
    /// budget.
    pub async fn acquire_chunk_permit(&self) -> Result<ChunkPermit> {
        let permit = self
            .chunk_sem
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| BcError::Other(format!("chunk_sem closed: {e}")))?;
        self.stats.chunk_permits_in_use.inc();
        Ok(ChunkPermit {
            _permit: permit,
            stats: self.stats.clone(),
        })
    }

    pub fn chunk_concurrency_limit(&self) -> usize {
        self.chunk_concurrency
    }

    /// Like `fetch_chunk` but returns only `[sub_offset, sub_offset+sub_len)`
    /// of the chunk. On a cache hit it issues a single pread() of just the
    /// requested slice, avoiding the 32x read amplification when FUSE splits
    /// a 4 MiB read into 32 x 128 KiB sub-reads (each previously caused a
    /// full 4 MiB cache read).
    #[allow(clippy::too_many_arguments)]
    pub async fn fetch_chunk_range(
        &self,
        mount: &MountConfig,
        blob_path: &str,
        chunk_offset: u64,
        sub_offset: u64,
        sub_len: u64,
        expected_chunk_len: u64,
        rid: Option<&crate::request_id::RequestId>,
    ) -> Result<Bytes> {
        let t_total = std::time::Instant::now();
        let res = self
            .fetch_chunk_range_inner(
                mount,
                blob_path,
                chunk_offset,
                sub_offset,
                sub_len,
                expected_chunk_len,
                rid,
            )
            .await;
        self.stats
            .chunk_total_seconds
            .observe(t_total.elapsed().as_secs_f64());
        res
    }

    #[allow(clippy::too_many_arguments)]
    async fn fetch_chunk_range_inner(
        &self,
        mount: &MountConfig,
        blob_path: &str,
        chunk_offset: u64,
        sub_offset: u64,
        sub_len: u64,
        expected_chunk_len: u64,
        rid: Option<&crate::request_id::RequestId>,
    ) -> Result<Bytes> {
        if sub_len == 0 {
            return Ok(Bytes::new());
        }
        let key = ChunkKey {
            mount: mount.name.clone(),
            blob: blob_path.to_string(),
            offset: chunk_offset,
        };

        if let Some(b) = self.inflight_writes.get(&key) {
            let bytes = b.value().clone();
            let end = (sub_offset + sub_len) as usize;
            if end <= bytes.len() {
                return Ok(bytes.slice(sub_offset as usize..end));
            }
        }

        if let Some(lru) = &self.peer_lru {
            let hit = lru.lock().get(&key);
            if let Some(bytes) = hit {
                if bytes.len() as u64 == expected_chunk_len {
                    let end = (sub_offset + sub_len) as usize;
                    if end <= bytes.len() {
                        self.stats.peer_lru_hits.inc();
                        return Ok(bytes.slice(sub_offset as usize..end));
                    }
                }
            }
        }

        let cache_for_get = self.cache.clone();
        let key_for_get = key.clone();
        let t_get = std::time::Instant::now();
        let cached = tokio::task::spawn_blocking(move || {
            cache_for_get.try_get_range(&key_for_get, sub_offset, sub_len)
        })
        .await
        .map_err(|e| BcError::Other(format!("cache get join: {e}")))?;
        self.stats
            .chunk_cache_get_seconds
            .observe(t_get.elapsed().as_secs_f64());
        if let Some(b) = cached {
            return Ok(b);
        }

        // Slice miss: full-chunk fetch (will populate cache for subsequent
        // sub-reads), then slice.
        let full = self
            .fetch_chunk_inner(
                mount,
                blob_path,
                chunk_offset,
                expected_chunk_len,
                false,
                rid,
            )
            .await?;
        let end = (sub_offset + sub_len) as usize;
        if end > full.len() {
            return Err(BcError::Other(format!(
                "fetched chunk shorter than requested slice: have {} need {}",
                full.len(),
                end
            )));
        }
        Ok(full.slice(sub_offset as usize..end))
    }

    fn note_fetch_origin(&self, mount_name: &str, blob_path: &str, from_blob: bool) {
        let key = format!("{}\0{}", mount_name, blob_path);
        let mut e = self.seq_state.entry(key).or_insert(SeqState {
            last_end: 0,
            consecutive: 0,
            blob_streak: 0,
        });
        if from_blob {
            e.blob_streak = e.blob_streak.saturating_add(1);
        } else {
            e.blob_streak = 0;
        }
    }

    async fn fetch_chunk_inner(
        &self,
        mount: &MountConfig,
        blob_path: &str,
        offset: u64,
        expected_len: u64,
        bypass_peers: bool,
        rid: Option<&crate::request_id::RequestId>,
    ) -> Result<Bytes> {
        let key = ChunkKey {
            mount: mount.name.clone(),
            blob: blob_path.to_string(),
            offset,
        };

        if let Some(b) = self.inflight_writes.get(&key) {
            let bytes = b.value().clone();
            if bytes.len() as u64 == expected_len {
                return Ok(bytes);
            }
        }

        if let Some(lru) = &self.peer_lru {
            let hit = lru.lock().get(&key);
            if let Some(bytes) = hit {
                if bytes.len() as u64 == expected_len {
                    self.stats.peer_lru_hits.inc();
                    return Ok(bytes);
                }
            }
        }

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
            use dashmap::mapref::entry::Entry;
            match self.inflight.entry(key.clone()) {
                Entry::Occupied(o) => (false, Some(o.get().subscribe())),
                Entry::Vacant(v) => {
                    let (tx, _rx) = broadcast::channel::<std::result::Result<Bytes, String>>(1);
                    v.insert(tx);
                    self.stats.singleflight_inflight.inc();
                    (true, None)
                }
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
            stats: self.stats.clone(),
            key: key.clone(),
            armed: true,
        };
        let result = self
            .do_fetch(mount, blob_path, &key, expected_len, bypass_peers, rid)
            .await;
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
        bypass_peers: bool,
        rid: Option<&crate::request_id::RequestId>,
    ) -> Result<Bytes> {
        if !bypass_peers {
            let alive = self.membership.members_alive_same_cluster();
            let candidates = self.peer_index.rank_candidates(
                key,
                &alive,
                self.peer_max_yes_attempts,
                self.peer_max_maybe_attempts,
            );
            let yes_count = candidates.yes.len();
            if yes_count > 0 {
                self.stats.peer_bloom_yes.inc();
            } else if !alive.is_empty() {
                self.stats.peer_bloom_no_holder.inc();
            }
            // peer_max_candidates is a HARD ceiling on combined yes+maybe attempts;
            // peer_max_yes_attempts and peer_max_maybe_attempts are SOFT caps within
            // their respective pools.  Without the combined cap, pathological alive
            // sets could exceed the operator's budget (Action 3 from opus_code_eval).
            //
            // Try yes-set first (peers whose advertised bloom contains this
            // chunk), then maybe-set (peers with no bloom yet, e.g. just joined).
            // Each set has its own budget so a flood of false-positives in yes
            // can't starve the maybe-set.
            let yes_cap = self.peer_max_yes_attempts.min(self.peer_max_candidates);
            let mut yes_used = 0usize;
            for peer in candidates.yes.iter().take(yes_cap) {
                yes_used += 1;
                if let Some(data) = self
                    .try_peer_fetch(peer, key, expected_len, true, 0, rid)
                    .await?
                {
                    return Ok(data);
                }
            }
            let maybe_cap = self
                .peer_max_maybe_attempts
                .min(self.peer_max_candidates.saturating_sub(yes_used));
            for peer in candidates.maybe.iter().take(maybe_cap) {
                if let Some(data) = self
                    .try_peer_fetch(peer, key, expected_len, false, 0, rid)
                    .await?
                {
                    return Ok(data);
                }
            }

            // v2.6.0 stampede-leader cold-start coordination: if no peer claims
            // (or might claim) the chunk, route to the cluster-wide HRW-top
            // owner with wait_ms so that the first to reach blob becomes the
            // leader and everyone else piggybacks on its singleflight. This
            // avoids the cold-start herd: 8 nodes reading the same model file
            // would otherwise each issue an independent blob GET because their
            // blooms are still empty.
            if yes_count == 0 && self.stampede_wait_ms > 0 && !alive.is_empty() {
                let hrw_top_id = self.peer_index.hrw_top_id(key, &alive);
                if hrw_top_id != self.peer_index.me_id {
                    if let Some(peer) = alive.iter().find(|n| n.id == hrw_top_id) {
                        self.stats.peer_stampede_follower.inc();
                        if let Some(data) = self
                            .try_peer_fetch(
                                peer,
                                key,
                                expected_len,
                                false,
                                self.stampede_wait_ms,
                                rid,
                            )
                            .await?
                        {
                            self.stats.peer_stampede_follower_ok.inc();
                            return Ok(data);
                        }
                        self.stats.peer_stampede_follower_timeout.inc();
                    }
                } else {
                    self.stats.peer_stampede_leader.inc();
                }
            }
        }

        let real_path = if mount.prefix.is_empty() {
            blob_path.to_string()
        } else {
            format!("{}/{}", mount.prefix.trim_end_matches('/'), blob_path)
        };
        let data = self
            .pool
            .get_blob_range(
                &mount.name,
                &mount.account,
                &mount.container,
                &real_path,
                key.offset,
                self.chunk_size,
            )
            .await?;
        if data.len() as u64 != expected_len {
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
        self.spawn_insert(key.clone(), data.clone());
        self.note_fetch_origin(&key.mount, &key.blob, true);
        Ok(data)
    }

    async fn try_peer_fetch(
        &self,
        peer: &crate::cluster::NodeInfo,
        key: &ChunkKey,
        expected_len: u64,
        was_yes: bool,
        wait_ms: u32,
        rid: Option<&crate::request_id::RequestId>,
    ) -> Result<Option<Bytes>> {
        // Per-peer concurrency cap (Action 2 from opus_code_eval): without this,
        // a hot peer can be flooded by parallel fetches from this node, causing
        // HoL blocking on the peer's own chunk_sem.  peer_concurrency (default 8)
        // gates how many in-flight requests we issue to any single peer.
        let peer_id = peer.id.clone();
        let sem = self
            .peer_sems
            .entry(peer_id)
            .or_insert_with(|| Arc::new(Semaphore::new(self.peer_concurrency)))
            .clone();
        // Semaphores are only closed by explicit Semaphore::close(), which we
        // never call, so acquire_owned() cannot return Err in practice.
        let _permit = sem
            .acquire_owned()
            .await
            .expect("peer semaphore never closed");

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
                    return Ok(None);
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
                    return Ok(None);
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
                wait_ms,
                rid,
            )
            .await;
        self.stats
            .chunk_peer_fetch_seconds
            .observe(t_peer.elapsed().as_secs_f64());
        match peer_res {
            Ok(data) => {
                if data.len() as u64 != expected_len {
                    tracing::warn!(
                        peer = %peer.transport_url,
                        key = ?key,
                        got = data.len(),
                        want = expected_len,
                        "peer chunk wrong length, skipping"
                    );
                    self.stats.peer_fetches_err.inc();
                    return Ok(None);
                }
                self.stats.peer_fetches_ok.inc();
                self.stats.peer_fetch_bytes.inc_by(data.len() as u64);
                if self.cache_on_peer_fetch {
                    self.spawn_insert(key.clone(), data.clone());
                } else if let Some(lru) = &self.peer_lru {
                    lru.lock().put(key.clone(), data.clone());
                }
                self.note_fetch_origin(&key.mount, &key.blob, false);
                Ok(Some(data))
            }
            Err(BcError::NotFound(_)) => {
                self.stats.peer_fetches_miss.inc();
                if was_yes {
                    self.stats.peer_bloom_false_positive.inc();
                    // Self-correction: our cached view of this peer's bloom said
                    // it had this chunk, but the peer says it doesn't. Either
                    // the peer evicted (stale bits) or its on-disk state
                    // diverged from its bloom (stale entries pre-Fix-1). Drop
                    // our remote view; the next bloom-pull tick refetches the
                    // current version. Without this, every subsequent
                    // rank_candidates call keeps routing to the same liar peer
                    // until bloom_pull_secs elapses.
                    self.peer_index.drop_remote(&peer.id);
                    self.stats.peer_bloom_stale_drops.inc();
                }
                Ok(None)
            }
            Err(e) => {
                tracing::debug!(peer=%peer.transport_url, error=%e, "peer fetch err");
                self.stats.peer_fetches_err.inc();
                Ok(None)
            }
        }
    }

    // Stampede-leader entry point: invoked by PeerService when a remote peer
    // sends a fetch with wait_ms>0 and we cache-miss. Subscribes to our own
    // singleflight if a leader is already running for this key, otherwise
    // becomes leader and fetches from blob (skipping the peer-fan-out step
    // to avoid a recursive RDMA roundtrip back to the original requester).
    pub async fn serve_peer_chunk(
        &self,
        key: ChunkKey,
        expected_len: u64,
        wait_ms: u32,
    ) -> Option<Bytes> {
        let cache_for_get = self.cache.clone();
        let key_for_get = key.clone();
        if let Ok(Some(b)) =
            tokio::task::spawn_blocking(move || cache_for_get.try_get(&key_for_get)).await
        {
            if b.len() as u64 == expected_len {
                return Some(b);
            }
        }

        let (leader, mut rx_opt) = {
            use dashmap::mapref::entry::Entry;
            match self.inflight.entry(key.clone()) {
                Entry::Occupied(o) => (false, Some(o.get().subscribe())),
                Entry::Vacant(v) => {
                    let (tx, _rx) = broadcast::channel::<std::result::Result<Bytes, String>>(1);
                    v.insert(tx);
                    self.stats.singleflight_inflight.inc();
                    (true, None)
                }
            }
        };

        if !leader {
            let rx = rx_opt.as_mut().expect("follower must have receiver");
            let dur = std::time::Duration::from_millis(wait_ms as u64);
            match tokio::time::timeout(dur, rx.recv()).await {
                Ok(Ok(Ok(data))) if data.len() as u64 == expected_len => return Some(data),
                _ => return None,
            }
        }

        let mount_cfg = match self.mounts.get(&key.mount).cloned() {
            Some(m) => m,
            None => {
                if let Some((_, tx)) = self.inflight.remove(&key) {
                    self.stats.singleflight_inflight.dec();
                    let _ = tx.send(Err("unknown mount".into()));
                }
                return None;
            }
        };

        let guard = LeaderGuard {
            inflight: self.inflight.clone(),
            stats: self.stats.clone(),
            key: key.clone(),
            armed: true,
        };
        let result = self
            .fetch_blob_direct(&mount_cfg, &key.blob, &key, expected_len)
            .await;
        if let Some(tx) = guard.disarm() {
            let msg = match &result {
                Ok(b) => Ok(b.clone()),
                Err(e) => Err(e.to_string()),
            };
            let _ = tx.send(msg);
        }
        result.ok()
    }

    // Blob-only fetch path used by the stampede-leader: no peer fan-out
    // (to avoid recursive RDMA back to the requester), no candidate
    // ranking. Caches the result and bumps the bloom on success.
    async fn fetch_blob_direct(
        &self,
        mount: &MountConfig,
        blob_path: &str,
        key: &ChunkKey,
        expected_len: u64,
    ) -> Result<Bytes> {
        let real_path = if mount.prefix.is_empty() {
            blob_path.to_string()
        } else {
            format!("{}/{}", mount.prefix.trim_end_matches('/'), blob_path)
        };
        let data = self
            .pool
            .get_blob_range(
                &mount.name,
                &mount.account,
                &mount.container,
                &real_path,
                key.offset,
                self.chunk_size,
            )
            .await?;
        if data.len() as u64 != expected_len {
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
        self.spawn_insert(key.clone(), data.clone());
        self.note_fetch_origin(&key.mount, &key.blob, true);
        Ok(data)
    }

    fn spawn_insert(&self, key: ChunkKey, data: Bytes) {
        self.inflight_writes.insert(key.clone(), data.clone());
        self.stats.inflight_writes.inc();
        let cache = self.cache.clone();
        let stats = self.stats.clone();
        let inflight = self.inflight_writes.clone();
        let inflight_writes_drained = self.inflight_writes_drained.clone();
        let insert_failures = self.insert_failures.clone();
        let peer_index = self.peer_index.clone();
        let k = key.clone();
        let digest = key_digest(&k);
        tokio::spawn(async move {
            let t_ins = std::time::Instant::now();
            let insert_res = tokio::task::spawn_blocking(move || cache.insert(k, &data)).await;
            stats
                .chunk_cache_insert_seconds
                .observe(t_ins.elapsed().as_secs_f64());
            match insert_res {
                Ok(Ok(())) => {
                    peer_index.note_local_insert(&digest);
                }
                Ok(Err(e)) => {
                    tracing::warn!(error = %e, "cache insert failed; skipping bloom advertise");
                    record_insert_failure(&insert_failures, key.clone(), e.to_string());
                    stats.cache_insert_failures.inc();
                }
                Err(e) => {
                    tracing::warn!(error = %e, "cache insert task panicked; skipping bloom advertise");
                    record_insert_failure(&insert_failures, key.clone(), format!("panic: {e}"));
                    stats.cache_insert_failures.inc();
                }
            }
            inflight.remove(&key);
            stats.inflight_writes.dec();
            if inflight.is_empty() {
                inflight_writes_drained.notify_waiters();
            }
        });
    }

    /// Hydrate-broadcast helper: fetch a specific chunk from a specific peer
    /// and insert it into the local cache. Bypasses bloom/HRW lookup, peer
    /// fan-out, and Azure fallback. If the chunk is already cached locally
    /// at the expected length, returns it without contacting the peer (Phase A
    /// shard owners already have it). Caller (broadcast worker) controls
    /// retries; failures are surfaced to be reported per-shard.
    #[allow(clippy::too_many_arguments)]
    pub async fn pull_chunk_from_peer(
        self: &Arc<Self>,
        mount: &MountConfig,
        blob_path: &str,
        offset: u64,
        expected_len: u64,
        peer_id: &str,
        transport_url: &str,
        ucx_worker_addr: Option<&[u8]>,
        rid: Option<&crate::request_id::RequestId>,
    ) -> Result<Bytes> {
        self.pull_chunk_from_peer_wait(
            mount,
            blob_path,
            offset,
            expected_len,
            peer_id,
            transport_url,
            ucx_worker_addr,
            0,
            rid,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn pull_chunk_from_peer_wait(
        self: &Arc<Self>,
        mount: &MountConfig,
        blob_path: &str,
        offset: u64,
        expected_len: u64,
        peer_id: &str,
        transport_url: &str,
        ucx_worker_addr: Option<&[u8]>,
        wait_ms: u32,
        rid: Option<&crate::request_id::RequestId>,
    ) -> Result<Bytes> {
        let key = ChunkKey {
            mount: mount.name.clone(),
            blob: blob_path.to_string(),
            offset,
        };
        if let Some(b) = self.cache.try_get(&key) {
            if b.len() as u64 == expected_len {
                return Ok(b);
            }
        }
        let t_peer = std::time::Instant::now();
        let peer_res = self
            .peers
            .fetch_chunk(
                peer_id,
                transport_url,
                ucx_worker_addr,
                &key,
                expected_len as u32,
                wait_ms,
                rid,
            )
            .await;
        self.stats
            .chunk_peer_fetch_seconds
            .observe(t_peer.elapsed().as_secs_f64());
        let data = match peer_res {
            Ok(d) => d,
            Err(BcError::NotFound(_)) => {
                self.stats.broadcast_peer_not_found.inc();
                tracing::warn!(
                    peer_id,
                    blob = blob_path,
                    offset,
                    "broadcast peer returned NotFound; falling back to blob origin"
                );
                match self
                    .fetch_chunk_origin_only(mount, blob_path, offset, expected_len, rid)
                    .await
                {
                    Ok(d) => {
                        self.stats.broadcast_blob_fallback_ok.inc();
                        return Ok(d);
                    }
                    Err(e) => {
                        self.stats.broadcast_blob_fallback_err.inc();
                        return Err(BcError::Other(format!(
                            "broadcast peer {peer_id} NotFound and blob fallback failed for {blob_path}@{offset}: {e}"
                        )));
                    }
                }
            }
            Err(e) => return Err(e),
        };
        if data.len() as u64 != expected_len {
            self.stats.peer_fetches_err.inc();
            return Err(BcError::Other(format!(
                "broadcast peer {peer_id} returned wrong length: got {} want {} for {blob_path}@{offset}",
                data.len(),
                expected_len,
            )));
        }
        self.stats.peer_fetches_ok.inc();
        self.stats.peer_fetch_bytes.inc_by(data.len() as u64);
        self.spawn_insert(key, data.clone());
        Ok(data)
    }

    /// Poll until every chunk previously handed to spawn_insert has been
    /// durably persisted to NVMe (tmp+fsync+rename completed) and removed
    /// from inflight_writes. Used by hydrate to make wall-time measurements
    /// reflect on-disk completion, not just GET completion.
    pub async fn await_inserts_drained(&self) {
        loop {
            if self.inflight_writes.is_empty() {
                return;
            }
            // CRITICAL: create notified() future BEFORE the empty check, so we
            // cannot miss a notify_waiters() that fires between check and await.
            let notified = self.inflight_writes_drained.notified();
            if self.inflight_writes.is_empty() {
                return;
            }
            notified.await;
        }
    }

    pub fn take_insert_failures(&self) -> Vec<(ChunkKey, String)> {
        let mut g = self.insert_failures.lock();
        std::mem::take(&mut *g)
    }

    /// Drain any in-flight insert tasks, drop the on-disk cache, and reset
    /// the in-memory fetcher state (singleflight inflight map, prefetch
    /// sequential-read tracker) so subsequent reads start cold. Bumps the
    /// local bloom (rebuilt from the now-empty cache) so peers stop directing
    /// requests here for cleared keys. Used by /clear-cache.
    pub async fn clear_local_state(&self) -> Result<(u64, u64)> {
        self.await_inserts_drained().await;
        let (files, bytes) = self.cache.clear_all()?;
        self.inflight.clear();
        self.stats.singleflight_inflight.set(0);
        self.inflight_writes.clear();
        self.stats.inflight_writes.set(0);
        self.seq_state.clear();
        if let Some(lru) = &self.peer_lru {
            lru.lock().clear();
        }
        self.peer_index.rebuild_local_from_cache(&self.cache);
        Ok((files, bytes))
    }

    pub async fn fetch_range(
        &self,
        mount: &MountConfig,
        blob_path: &str,
        offset: u64,
        length: u64,
        file_size: u64,
        rid: Option<&crate::request_id::RequestId>,
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

        self.maybe_trigger_prefetch(mount, blob_path, offset, end, file_size);

        let mut tasks = Vec::new();
        let mut o = first_chunk;
        while o <= last_chunk {
            let chunk_len = cs.min(file_size - o);
            let take_start = offset.max(o) - o;
            let take_end = end.min(o + chunk_len) - o;
            let sub_len = take_end - take_start;
            let mc = mount.clone();
            let bp = blob_path.to_string();
            let me = self.clone_handles();
            let rid_owned = rid.cloned();
            tasks.push(tokio::spawn(async move {
                let _permit = me.acquire_chunk_permit().await?;
                me.fetch_chunk_range(
                    &mc,
                    &bp,
                    o,
                    take_start,
                    sub_len,
                    chunk_len,
                    rid_owned.as_ref(),
                )
                .await
                .map(|b| (o, b))
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

        if chunks.len() == 1 {
            return Ok(chunks.into_iter().next().unwrap().1);
        }
        let mut out = Vec::with_capacity((end - offset) as usize);
        for (_co, data) in chunks {
            out.extend_from_slice(&data);
        }
        Ok(Bytes::from(out))
    }

    fn clone_handles(&self) -> Self {
        Self {
            cache: self.cache.clone(),
            pool: self.pool.clone(),
            peers: self.peers.clone(),
            membership: self.membership.clone(),
            stats: self.stats.clone(),
            chunk_size: self.chunk_size,
            block_size: self.block_size,
            peer_index: self.peer_index.clone(),
            peer_max_candidates: self.peer_max_candidates,
            peer_max_yes_attempts: self.peer_max_yes_attempts,
            peer_max_maybe_attempts: self.peer_max_maybe_attempts,
            stampede_wait_ms: self.stampede_wait_ms,
            mounts: self.mounts.clone(),
            inflight: self.inflight.clone(),
            chunk_sem: self.chunk_sem.clone(),
            chunk_concurrency: self.chunk_concurrency,
            inflight_writes: self.inflight_writes.clone(),
            inflight_writes_drained: self.inflight_writes_drained.clone(),
            insert_failures: self.insert_failures.clone(),
            seq_state: self.seq_state.clone(),
            prefetch_sem: self.prefetch_sem.clone(),
            prefetch_depth: self.prefetch_depth,
            prefetch_threshold: self.prefetch_threshold,
            prefetch_origin_only: self.prefetch_origin_only,
            cache_on_peer_fetch: self.cache_on_peer_fetch,
            peer_lru: self.peer_lru.clone(),
            peer_sems: self.peer_sems.clone(),
            peer_concurrency: self.peer_concurrency,
        }
    }

    // Update the per-stream sequential tracker and, once the caller has shown
    // `prefetch_threshold` consecutive forward reads, spawn background fetches
    // for the next `prefetch_depth` chunks past the current one. Skips chunks
    // already cached or in flight so re-reads don't fan out duplicate work.
    fn maybe_trigger_prefetch(
        &self,
        mount: &MountConfig,
        blob_path: &str,
        req_offset: u64,
        req_end: u64,
        file_size: u64,
    ) {
        if self.prefetch_depth == 0 {
            return;
        }
        let cs = self.chunk_size;
        let key = format!("{}\0{}", mount.name, blob_path);
        let (consecutive, blob_streak) = {
            let mut e = self.seq_state.entry(key).or_insert(SeqState {
                last_end: 0,
                consecutive: 0,
                blob_streak: 0,
            });
            // Sequential = read starts exactly where the previous one ended,
            // or within one chunk ahead (covers FUSE sub-read reordering and
            // small skipped slack from prefetch-warmed reads).
            let forward = req_offset >= e.last_end && req_offset - e.last_end <= cs;
            if forward {
                e.consecutive = e.consecutive.saturating_add(1);
            } else {
                e.consecutive = 1;
            }
            e.last_end = req_end;
            (e.consecutive, e.blob_streak)
        };
        if consecutive < self.prefetch_threshold {
            return;
        }
        if self.prefetch_origin_only && blob_streak == 0 {
            self.stats.prefetch_skipped_not_origin.inc();
            return;
        }

        let cur_chunk = if req_end == 0 {
            0
        } else {
            ((req_end - 1) / cs) * cs
        };
        for i in 1..=self.prefetch_depth as u64 {
            let off = cur_chunk + i * cs;
            if off >= file_size {
                break;
            }
            let chunk_len = cs.min(file_size - off);
            let ck = ChunkKey {
                mount: mount.name.clone(),
                blob: blob_path.to_string(),
                offset: off,
            };
            if self.cache.entry_size(&ck).is_some() {
                self.stats.prefetch_skipped_cached.inc();
                continue;
            }
            if self.inflight_writes.contains_key(&ck) {
                self.stats.prefetch_skipped_inflight.inc();
                continue;
            }
            if self.inflight.contains_key(&ck) {
                self.stats.prefetch_skipped_inflight.inc();
                continue;
            }
            let me = self.clone_handles();
            let mc = mount.clone();
            let bp = blob_path.to_string();
            let sem = self.prefetch_sem.clone();
            let origin_only = self.prefetch_origin_only;
            self.stats.prefetch_spawned.inc();
            tokio::spawn(async move {
                let Ok(_permit) = sem.acquire_owned().await else {
                    return;
                };
                let res = if origin_only {
                    me.fetch_chunk_origin_only(&mc, &bp, off, chunk_len, None)
                        .await
                } else {
                    me.fetch_chunk(&mc, &bp, off, chunk_len, None).await
                };
                match res {
                    Ok(_) => me.stats.prefetch_completed_ok.inc(),
                    Err(_) => me.stats.prefetch_completed_err.inc(),
                }
            });
        }
    }
}

const MAX_RECORDED_INSERT_FAILURES: usize = 256;

fn record_insert_failure(sink: &Mutex<Vec<(ChunkKey, String)>>, key: ChunkKey, err: String) {
    let mut g = sink.lock();
    if g.len() < MAX_RECORDED_INSERT_FAILURES {
        g.push((key, err));
    }
}

#[cfg(test)]
mod peer_lru_tests {
    use super::PeerLru;
    use crate::cache::ChunkKey;
    use bytes::Bytes;

    fn k(off: u64) -> ChunkKey {
        ChunkKey {
            mount: "m".into(),
            blob: "b".into(),
            offset: off,
        }
    }

    #[test]
    fn put_then_get_returns_value() {
        let mut lru = PeerLru::new(4096);
        lru.put(k(0), Bytes::from(vec![0u8; 1024]));
        let got = lru.get(&k(0)).expect("hit");
        assert_eq!(got.len(), 1024);
    }

    #[test]
    fn miss_returns_none() {
        let mut lru = PeerLru::new(4096);
        assert!(lru.get(&k(0)).is_none());
    }

    #[test]
    fn put_evicts_lru_when_over_cap() {
        let mut lru = PeerLru::new(3000);
        lru.put(k(0), Bytes::from(vec![0u8; 1000]));
        lru.put(k(1), Bytes::from(vec![1u8; 1000]));
        lru.put(k(2), Bytes::from(vec![2u8; 1000]));
        assert_eq!(lru.bytes, 3000);
        lru.put(k(3), Bytes::from(vec![3u8; 1000]));
        assert_eq!(lru.bytes, 3000, "byte accounting after eviction");
        assert!(lru.get(&k(0)).is_none(), "oldest evicted");
        assert!(lru.get(&k(3)).is_some(), "newest survives");
    }

    #[test]
    fn put_rejects_oversized_entry_silently() {
        let mut lru = PeerLru::new(1000);
        lru.put(k(0), Bytes::from(vec![1u8; 100]));
        lru.put(k(99), Bytes::from(vec![9u8; 5000]));
        assert!(
            lru.get(&k(99)).is_none(),
            "oversized entry must not be inserted"
        );
        assert!(
            lru.get(&k(0)).is_some(),
            "pre-existing entry must not be evicted by oversized put"
        );
        assert_eq!(lru.bytes, 100);
    }

    #[test]
    fn get_promotes_to_mru_position() {
        let mut lru = PeerLru::new(3000);
        lru.put(k(0), Bytes::from(vec![0u8; 1000]));
        lru.put(k(1), Bytes::from(vec![1u8; 1000]));
        lru.put(k(2), Bytes::from(vec![2u8; 1000]));
        let _ = lru.get(&k(0));
        lru.put(k(3), Bytes::from(vec![3u8; 1000]));
        assert!(lru.get(&k(0)).is_some(), "k(0) was promoted, survives");
        assert!(lru.get(&k(1)).is_none(), "k(1) was now-oldest, evicted");
    }

    #[test]
    fn put_overwriting_same_key_does_not_double_count() {
        let mut lru = PeerLru::new(10_000);
        lru.put(k(0), Bytes::from(vec![1u8; 1000]));
        lru.put(k(0), Bytes::from(vec![2u8; 2000]));
        assert_eq!(
            lru.bytes, 2000,
            "must subtract previous size before adding new"
        );
        let got = lru.get(&k(0)).unwrap();
        assert_eq!(got.len(), 2000);
    }

    #[test]
    fn clear_resets_bytes_and_entries() {
        let mut lru = PeerLru::new(10_000);
        lru.put(k(0), Bytes::from(vec![0u8; 1000]));
        lru.put(k(1), Bytes::from(vec![1u8; 1000]));
        lru.clear();
        assert_eq!(lru.bytes, 0);
        assert!(lru.get(&k(0)).is_none());
        assert!(lru.get(&k(1)).is_none());
    }

    #[test]
    fn cap_zero_rejects_all_puts() {
        let mut lru = PeerLru::new(0);
        lru.put(k(0), Bytes::from(vec![0u8; 1]));
        assert!(
            lru.get(&k(0)).is_none(),
            "cap 0 means any non-empty value is oversized"
        );
        assert_eq!(lru.bytes, 0);
    }
}
