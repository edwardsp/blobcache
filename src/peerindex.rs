use crate::cache::{ChunkKey, DiskCache};
use crate::cluster::NodeInfo;
use dashmap::DashMap;
use parking_lot::RwLock;
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

const BLOOM_K: usize = 4;
pub const BLOOM_BITS_DEFAULT: usize = 1 << 23;

pub struct Bloom {
    bits: Vec<u64>,
    m_bits: usize,
}

impl Bloom {
    pub fn new(m_bits: usize) -> Self {
        let m_bits = m_bits.max(64);
        let words = (m_bits + 63) / 64;
        Self {
            bits: vec![0u64; words],
            m_bits,
        }
    }

    pub fn from_bytes(b: &[u8]) -> Option<Self> {
        if b.len() < 8 {
            return None;
        }
        let m_bits = u64::from_le_bytes(b[..8].try_into().ok()?) as usize;
        let words = (m_bits + 63) / 64;
        let payload = &b[8..];
        if payload.len() != words * 8 {
            return None;
        }
        let mut bits = Vec::with_capacity(words);
        for chunk in payload.chunks_exact(8) {
            bits.push(u64::from_le_bytes(chunk.try_into().ok()?));
        }
        Some(Self { bits, m_bits })
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(8 + self.bits.len() * 8);
        out.extend_from_slice(&(self.m_bits as u64).to_le_bytes());
        for w in &self.bits {
            out.extend_from_slice(&w.to_le_bytes());
        }
        out
    }

    pub fn insert(&mut self, digest: &[u8; 32]) {
        for i in 0..BLOOM_K {
            let pos = bit_pos(digest, i, self.m_bits);
            self.bits[pos / 64] |= 1u64 << (pos % 64);
        }
    }

    pub fn contains(&self, digest: &[u8; 32]) -> bool {
        for i in 0..BLOOM_K {
            let pos = bit_pos(digest, i, self.m_bits);
            if self.bits[pos / 64] & (1u64 << (pos % 64)) == 0 {
                return false;
            }
        }
        true
    }

    pub fn byte_len(&self) -> usize {
        8 + self.bits.len() * 8
    }
}

fn bit_pos(d: &[u8; 32], i: usize, m: usize) -> usize {
    let h1 = u64::from_le_bytes(d[0..8].try_into().unwrap());
    let h2 = u64::from_le_bytes(d[8..16].try_into().unwrap());
    let h = h1.wrapping_add((i as u64).wrapping_mul(h2));
    (h as usize) % m
}

pub fn key_digest(key: &ChunkKey) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(key.mount.as_bytes());
    h.update(b"\0");
    h.update(key.blob.as_bytes());
    h.update(b"\0");
    h.update(key.offset.to_le_bytes());
    h.finalize().into()
}

pub struct RemoteBloom {
    pub version: u64,
    pub bloom: Bloom,
}

pub struct PeerIndex {
    pub me_id: String,
    pub bloom_bits: usize,
    local: Arc<RwLock<Bloom>>,
    local_version: Arc<AtomicU64>,
    remote: Arc<DashMap<String, RemoteBloom>>,
}

impl PeerIndex {
    pub fn new(me_id: String, bloom_bits: usize) -> Arc<Self> {
        Arc::new(Self {
            me_id,
            bloom_bits,
            local: Arc::new(RwLock::new(Bloom::new(bloom_bits))),
            local_version: Arc::new(AtomicU64::new(1)),
            remote: Arc::new(DashMap::new()),
        })
    }

    pub fn note_local_insert(&self, digest: &[u8; 32]) {
        self.local.write().insert(digest);
    }

    pub fn rebuild_local_from_cache(&self, cache: &DiskCache) {
        let keys = cache.live_keys();
        let mut new = Bloom::new(self.bloom_bits);
        for k in keys.iter() {
            new.insert(&key_digest(k));
        }
        *self.local.write() = new;
        self.local_version.fetch_add(1, Ordering::Relaxed);
    }

    pub fn local_version(&self) -> u64 {
        self.local_version.load(Ordering::Relaxed)
    }

    pub fn local_serialised(&self) -> Vec<u8> {
        self.local.read().to_bytes()
    }

    pub fn ingest_remote(&self, peer_id: &str, version: u64, bytes: &[u8]) -> bool {
        match Bloom::from_bytes(bytes) {
            Some(b) => {
                self.remote
                    .insert(peer_id.to_string(), RemoteBloom { version, bloom: b });
                true
            }
            None => false,
        }
    }

    pub fn remote_version(&self, peer_id: &str) -> Option<u64> {
        self.remote.get(peer_id).map(|r| r.version)
    }

    pub fn drop_remote(&self, peer_id: &str) {
        self.remote.remove(peer_id);
    }

    pub fn rank_candidates(
        &self,
        key: &ChunkKey,
        alive: &[NodeInfo],
        max_yes: usize,
        max_maybe: usize,
    ) -> CandidateSet {
        let digest = key_digest(key);
        let mut scored: Vec<(u64, &NodeInfo)> = alive
            .iter()
            .map(|n| (hrw_score(&n.id, &digest), n))
            .collect();
        scored.sort_by(|a, b| b.0.cmp(&a.0));
        let mut yes = Vec::new();
        let mut maybe = Vec::new();
        for (_, n) in scored.iter() {
            match self.remote.get(&n.id) {
                Some(r) => {
                    if r.bloom.contains(&digest) {
                        if yes.len() < max_yes {
                            yes.push((*n).clone());
                        }
                    }
                }
                None => {
                    if maybe.len() < max_maybe {
                        maybe.push((*n).clone());
                    }
                }
            }
        }
        CandidateSet { yes, maybe }
    }
}

pub struct CandidateSet {
    pub yes: Vec<NodeInfo>,
    pub maybe: Vec<NodeInfo>,
}

impl CandidateSet {
    pub fn merged(self, max_total: usize) -> Vec<NodeInfo> {
        let mut out = self.yes;
        for n in self.maybe {
            if out.len() >= max_total {
                break;
            }
            out.push(n);
        }
        out.into_iter().take(max_total).collect()
    }
}

fn hrw_score(peer_id: &str, digest: &[u8; 32]) -> u64 {
    let mut h = Sha256::new();
    h.update(peer_id.as_bytes());
    h.update(digest);
    let d = h.finalize();
    u64::from_le_bytes(d[0..8].try_into().unwrap())
}
