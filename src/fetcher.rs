use bytes::Bytes;
use rand::seq::SliceRandom;
use std::sync::Arc;

use crate::azure::BlobClient;
use crate::cache::{ChunkKey, DiskCache};
use crate::cluster::Membership;
use crate::config::MountConfig;
use crate::error::{BcError, Result};
use crate::stats::Stats;
use crate::transport::PeerClient;

pub struct Fetcher {
    pub cache: Arc<DiskCache>,
    pub blob: Arc<BlobClient>,
    pub peers: Arc<PeerClient>,
    pub membership: Membership,
    pub stats: Arc<Stats>,
    pub chunk_size: u64,
}

impl Fetcher {
    pub fn new(
        cache: Arc<DiskCache>,
        blob: Arc<BlobClient>,
        membership: Membership,
        stats: Arc<Stats>,
        chunk_size: u64,
    ) -> Self {
        Self { cache, blob, peers: Arc::new(PeerClient::new()), membership, stats, chunk_size }
    }

    pub async fn fetch_chunk(&self, mount: &MountConfig, blob_path: &str, offset: u64, length: u64) -> Result<Bytes> {
        let key = ChunkKey { mount: mount.name.clone(), blob: blob_path.to_string(), offset };
        if let Some(b) = self.cache.try_get(&key) {
            let want = length.min(b.len() as u64) as usize;
            return Ok(b.slice(0..want));
        }

        let alive = self.membership.members_alive();
        let mut shuffled = alive.clone();
        shuffled.shuffle(&mut rand::thread_rng());
        for peer in shuffled.iter().take(3) {
            match self.peers.fetch_chunk(&peer.transport_url, &key).await {
                Ok(data) => {
                    self.stats.peer_fetches_ok.inc();
                    self.stats.peer_fetch_bytes.inc_by(data.len() as u64);
                    let _ = self.cache.insert(key.clone(), &data);
                    let want = length.min(data.len() as u64) as usize;
                    return Ok(data.slice(0..want));
                }
                Err(BcError::NotFound(_)) => { self.stats.peer_fetches_miss.inc(); continue; }
                Err(e) => { tracing::debug!(peer=%peer.transport_url, error=%e, "peer fetch err"); self.stats.peer_fetches_err.inc(); continue; }
            }
        }

        let real_path = if mount.prefix.is_empty() { blob_path.to_string() }
                        else { format!("{}/{}", mount.prefix.trim_end_matches('/'), blob_path) };
        let data = self.blob.get_blob_range(&mount.account, &mount.container, &real_path, offset, self.chunk_size).await?;
        self.stats.blob_fetches.inc();
        self.stats.blob_fetch_bytes.inc_by(data.len() as u64);
        let _ = self.cache.insert(key, &data);
        let want = length.min(data.len() as u64) as usize;
        Ok(data.slice(0..want))
    }

    pub async fn fetch_range(&self, mount: &MountConfig, blob_path: &str, offset: u64, length: u64, file_size: u64) -> Result<Bytes> {
        if length == 0 { return Ok(Bytes::new()); }
        let cs = self.chunk_size;
        let end = (offset + length).min(file_size);
        if end <= offset { return Ok(Bytes::new()); }
        let first_chunk = (offset / cs) * cs;
        let last_chunk = ((end - 1) / cs) * cs;

        let mut chunks = Vec::new();
        let mut tasks = Vec::new();
        let mut o = first_chunk;
        while o <= last_chunk {
            let chunk_len = cs.min(file_size - o);
            let mc = mount.clone();
            let bp = blob_path.to_string();
            let me = self.clone_handles();
            tasks.push(tokio::spawn(async move {
                me.fetch_chunk(&mc, &bp, o, chunk_len).await.map(|b| (o, b))
            }));
            o += cs;
        }
        for t in tasks {
            let (co, data) = t.await.map_err(|e| BcError::Other(format!("join: {e}")))??;
            chunks.push((co, data));
        }
        chunks.sort_by_key(|(o, _)| *o);

        let mut out = Vec::with_capacity((end - offset) as usize);
        for (co, data) in chunks {
            let chunk_end = co + data.len() as u64;
            let take_start = offset.max(co) - co;
            let take_end = end.min(chunk_end) - co;
            if take_end > take_start {
                out.extend_from_slice(&data[take_start as usize .. take_end as usize]);
            }
        }
        Ok(Bytes::from(out))
    }

    fn clone_handles(&self) -> Self {
        Self {
            cache: self.cache.clone(),
            blob: self.blob.clone(),
            peers: self.peers.clone(),
            membership: self.membership.clone(),
            stats: self.stats.clone(),
            chunk_size: self.chunk_size,
        }
    }
}
