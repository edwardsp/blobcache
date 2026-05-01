use crate::cache::{ChunkKey, DiskCache};
use crate::cluster::NodeInfo;
use dashmap::DashMap;
use parking_lot::{Mutex, RwLock};
use sha2::{Digest, Sha256};
use std::sync::Arc;

const BLOOM_K: usize = 4;
#[allow(dead_code)]
pub const BLOOM_BITS_DEFAULT: usize = 1 << 23;

#[derive(Clone)]
pub struct Bloom {
    bits: Vec<u64>,
    m_bits: usize,
}

impl Bloom {
    pub fn new(m_bits: usize) -> Self {
        let m_bits = m_bits.max(64);
        let words = m_bits.div_ceil(64);
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
        if m_bits < 64 {
            return None;
        }
        let words = m_bits.div_ceil(64);
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

    #[allow(dead_code)]
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

// Local view bundles bloom and version under a single RwLock so that
// /cluster/bloom can serve a coherent (version, bytes) pair. v2.5.0 had a
// publish race where local_version() and local_serialised() were two separate
// lock acquisitions: a rebuild between them could publish stale bits with the
// new version, freezing remote peers' caches with stale-but-trusted data for
// up to bloom_pull_secs.
struct Local {
    version: u64,
    bloom: Bloom,
}

pub struct PeerIndex {
    pub me_id: String,
    pub bloom_bits: usize,
    local: RwLock<Local>,
    // Inserts during rebuild (between live_keys() snapshot and the swap of the
    // freshly-built bloom) would be lost without an overlay: the snapshot can
    // miss them, and the in-place insert into the soon-to-be-overwritten bloom
    // is wiped by the swap. note_local_insert pushes here; rebuild drains
    // these into the new bloom inside the same write-lock as the swap.
    pending: Mutex<Vec<[u8; 32]>>,
    remote: DashMap<String, RemoteBloom>,
    // Optional callback fired whenever local_version advances. main.rs wires
    // this to membership.set_bloom_version so peers see a fresh version
    // immediately after a local insert (rather than waiting up to
    // bloom_rebuild_secs for the periodic rebuild to bump it).
    #[allow(clippy::type_complexity)]
    on_version_change: RwLock<Option<Arc<dyn Fn(u64) + Send + Sync>>>,
}

impl PeerIndex {
    pub fn new(me_id: String, bloom_bits: usize) -> Arc<Self> {
        Arc::new(Self {
            me_id,
            bloom_bits,
            local: RwLock::new(Local {
                version: 1,
                bloom: Bloom::new(bloom_bits),
            }),
            pending: Mutex::new(Vec::new()),
            remote: DashMap::new(),
            on_version_change: RwLock::new(None),
        })
    }

    pub fn set_on_version_change<F>(&self, hook: F)
    where
        F: Fn(u64) + Send + Sync + 'static,
    {
        *self.on_version_change.write() = Some(Arc::new(hook));
    }

    pub fn note_local_insert(&self, digest: &[u8; 32]) {
        // Push into pending FIRST so a concurrent rebuild that snapshotted
        // live_keys() before our cache.insert() landed will still pick this
        // digest up via its drain-under-lock step.
        self.pending.lock().push(*digest);
        let new_version = {
            let mut g = self.local.write();
            g.bloom.insert(digest);
            g.version = g.version.saturating_add(1);
            g.version
        };
        if let Some(hook) = self.on_version_change.read().as_ref() {
            hook(new_version);
        }
    }

    pub fn rebuild_local_from_cache(&self, cache: &DiskCache) {
        // Build the new bloom OUTSIDE the lock from a snapshot of live keys.
        // For a 100 GiB cache at 4 MiB chunks that's ~25k SHA-256s; doing it
        // under the write lock would block /cluster/bloom GETs and inserts
        // for tens of milliseconds.
        let keys = cache.live_keys();
        let mut new = Bloom::new(self.bloom_bits);
        for k in &keys {
            new.insert(&key_digest(k));
        }
        // Drain pending under the SAME write lock that swaps the bloom in,
        // so concurrent inserts cannot land between the drain and the swap.
        let new_version = {
            let mut g = self.local.write();
            let drained: Vec<[u8; 32]> = self.pending.lock().drain(..).collect();
            for d in &drained {
                new.insert(d);
            }
            g.bloom = new;
            g.version = g.version.saturating_add(1);
            g.version
        };
        if let Some(hook) = self.on_version_change.read().as_ref() {
            hook(new_version);
        }
    }

    pub fn local_version(&self) -> u64 {
        self.local.read().version
    }

    // Atomic (version, bytes) snapshot; replaces the v2.5.0 two-call pattern
    // local_serialised() + local_version() that could publish a (new_version,
    // stale_bytes) tuple if rebuild ran between the two calls.
    pub fn local_snapshot(&self) -> (u64, Vec<u8>) {
        let g = self.local.read();
        (g.version, g.bloom.to_bytes())
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

    // Returns the HRW-top peer id among `alive` plus self, used by the
    // stampede-leader path so a follower knows whether to act as leader (fetch
    // from blob) or as follower (ask the leader with wait_ms and piggyback on
    // its singleflight). Includes self in the ranking so the choice is
    // cluster-wide deterministic.
    pub fn hrw_top_id(&self, key: &ChunkKey, alive: &[NodeInfo]) -> String {
        let digest = key_digest(key);
        let mut best_id = self.me_id.clone();
        let mut best_score = hrw_score(&self.me_id, &digest);
        for n in alive {
            let s = hrw_score(&n.id, &digest);
            if s > best_score {
                best_score = s;
                best_id = n.id.clone();
            }
        }
        best_id
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
        scored.sort_by_key(|b| std::cmp::Reverse(b.0));
        let mut yes = Vec::new();
        let mut maybe = Vec::new();
        for (_, n) in scored.iter() {
            match self.remote.get(&n.id) {
                Some(r) => {
                    if r.bloom.contains(&digest) && yes.len() < max_yes {
                        yes.push((*n).clone());
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

// CandidateSet now exposes yes/maybe separately so the fetcher can apply
// independent attempt budgets (max_yes_attempts vs max_maybe_attempts). v2.5.0
// merged them under one cap which meant four bloom-positive false-positives
// would walk straight to blob with no maybe-budget left.
pub struct CandidateSet {
    pub yes: Vec<NodeInfo>,
    pub maybe: Vec<NodeInfo>,
}

fn hrw_score(peer_id: &str, digest: &[u8; 32]) -> u64 {
    let mut h = Sha256::new();
    h.update(peer_id.as_bytes());
    h.update(digest);
    let d = h.finalize();
    u64::from_le_bytes(d[0..8].try_into().unwrap())
}
