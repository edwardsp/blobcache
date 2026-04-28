use bytes::Bytes;
use parking_lot::Mutex;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::error::Result;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ChunkKey {
    pub mount: String,
    pub blob: String,
    pub offset: u64,
}

impl ChunkKey {
    pub fn cache_filename(&self) -> String {
        let mut h = Sha256::new();
        h.update(self.mount.as_bytes());
        h.update(b"\0");
        h.update(self.blob.as_bytes());
        h.update(b"\0");
        h.update(self.offset.to_le_bytes());
        let d = h.finalize();
        hex(&d)
    }
}

fn hex(b: &[u8]) -> String {
    const C: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(b.len() * 2);
    for x in b {
        s.push(C[(x >> 4) as usize] as char);
        s.push(C[(x & 0xf) as usize] as char);
    }
    s
}

#[derive(Default)]
pub struct CacheStats {
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    pub evictions: AtomicU64,
    pub bytes_in_use: AtomicU64,
    pub inserts: AtomicU64,
}

struct Entry {
    size: u64,
    last_access_seq: u64,
}

pub struct DiskCache {
    root: PathBuf,
    max_bytes: u64,
    inner: Mutex<Inner>,
    pub stats: Arc<CacheStats>,
}

struct Inner {
    entries: HashMap<ChunkKey, Entry>,
    lru: BTreeMap<u64, ChunkKey>,
    seq: u64,
    bytes: u64,
}

impl DiskCache {
    pub fn open(root: PathBuf, max_bytes: u64) -> Result<Arc<Self>> {
        std::fs::create_dir_all(&root)?;
        let stats = Arc::new(CacheStats::default());
        let inner = Inner {
            entries: HashMap::new(),
            lru: BTreeMap::new(),
            seq: 0,
            bytes: 0,
        };
        let cache = Arc::new(Self {
            root,
            max_bytes,
            inner: Mutex::new(inner),
            stats,
        });
        cache.scan_existing()?;
        Ok(cache)
    }

    fn scan_existing(self: &Arc<Self>) -> Result<()> {
        // Chunk filenames are sha256(mount, blob, offset) - there is no reverse
        // mapping back to a ChunkKey, so any file already on disk at startup is
        // unreachable through normal lookup. Delete them rather than carry them
        // as orphans that count against the byte budget but can never be hit.
        let mut purged = 0u64;
        let mut purged_bytes = 0u64;
        for e in std::fs::read_dir(&self.root)?.flatten() {
            let m = match e.metadata() {
                Ok(m) => m,
                Err(_) => continue,
            };
            if !m.is_file() {
                continue;
            }
            let name = match e.file_name().into_string() {
                Ok(s) => s,
                Err(_) => continue,
            };
            if name.starts_with(".tmp.") {
                let _ = std::fs::remove_file(e.path());
                continue;
            }
            let _ = std::fs::remove_file(e.path());
            purged += 1;
            purged_bytes += m.len();
        }
        if purged > 0 {
            tracing::info!(
                files = purged,
                bytes = purged_bytes,
                "purged orphaned cache files at startup"
            );
        }
        let mut g = self.inner.lock();
        g.bytes = 0;
        self.stats.bytes_in_use.store(0, Ordering::Relaxed);
        Ok(())
    }

    fn path_for(&self, key: &ChunkKey) -> PathBuf {
        self.root.join(key.cache_filename())
    }

    pub fn try_get(self: &Arc<Self>, key: &ChunkKey) -> Option<Bytes> {
        // Take the lock first and check tracking. A file on disk that we don't
        // track is treated as a miss - resurrecting it here would race with
        // concurrent eviction (which removes the entry then the file) and
        // could double-count bytes_in_use or revive a file scheduled for
        // deletion.
        {
            let g = self.inner.lock();
            if !g.entries.contains_key(key) {
                self.stats.misses.fetch_add(1, Ordering::Relaxed);
                return None;
            }
        }
        let path = self.path_for(key);
        let data = match std::fs::read(&path) {
            Ok(d) => d,
            Err(_) => {
                self.stats.misses.fetch_add(1, Ordering::Relaxed);
                return None;
            }
        };
        if !self.touch_lru(key) {
            return None;
        }
        self.stats.hits.fetch_add(1, Ordering::Relaxed);
        Some(Bytes::from(data))
    }

    /// Read only `[range_offset, range_offset + range_len)` from the cached
    /// chunk. Returns None on miss. Avoids reading the entire chunk into
    /// memory when the FUSE caller only needs a sub-slice (the common case
    /// when kernel splits a 4 MiB read into 32 x 128 KiB FUSE requests).
    pub fn try_get_range(
        self: &Arc<Self>,
        key: &ChunkKey,
        range_offset: u64,
        range_len: u64,
    ) -> Option<Bytes> {
        use std::os::unix::fs::FileExt;
        if range_len == 0 {
            return Some(Bytes::new());
        }
        let entry_size = {
            let g = self.inner.lock();
            match g.entries.get(key) {
                Some(e) => e.size,
                None => {
                    self.stats.misses.fetch_add(1, Ordering::Relaxed);
                    return None;
                }
            }
        };
        if range_offset.saturating_add(range_len) > entry_size {
            // Caller asked beyond the chunk. Treat as miss so the caller can
            // re-fetch a fresh full chunk; cached one is wrong size.
            self.stats.misses.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        let path = self.path_for(key);
        let f = match std::fs::File::open(&path) {
            Ok(f) => f,
            Err(_) => {
                self.stats.misses.fetch_add(1, Ordering::Relaxed);
                return None;
            }
        };
        let mut buf = vec![0u8; range_len as usize];
        if let Err(_) = f.read_exact_at(&mut buf, range_offset) {
            self.stats.misses.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        if !self.touch_lru(key) {
            return None;
        }
        self.stats.hits.fetch_add(1, Ordering::Relaxed);
        Some(Bytes::from(buf))
    }

    /// Read the entire cached chunk via a single pread into the caller's
    /// `dst` slice. Returns the number of bytes read on hit, or None on
    /// miss / size mismatch. The caller is responsible for sizing `dst`
    /// exactly to the entry size (call `entry_size(key)` first).
    /// This is the zero-extra-copy server fast-path: the server allocates
    /// one buffer holding both wire header and payload, then pread()s the
    /// payload directly into the tail of that buffer, eliminating the
    /// 4 MiB userspace memcpy that `try_get` + `extend_from_slice(payload)`
    /// would otherwise incur for every served peer chunk.
    pub fn try_get_into_slice(self: &Arc<Self>, key: &ChunkKey, dst: &mut [u8]) -> Option<usize> {
        use std::os::unix::fs::FileExt;
        let entry_size = {
            let g = self.inner.lock();
            match g.entries.get(key) {
                Some(e) => e.size as usize,
                None => {
                    self.stats.misses.fetch_add(1, Ordering::Relaxed);
                    return None;
                }
            }
        };
        if dst.len() != entry_size {
            return None;
        }
        let path = self.path_for(key);
        let f = match std::fs::File::open(&path) {
            Ok(f) => f,
            Err(_) => {
                self.stats.misses.fetch_add(1, Ordering::Relaxed);
                return None;
            }
        };
        if let Err(_) = f.read_exact_at(dst, 0) {
            self.stats.misses.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        if !self.touch_lru(key) {
            return None;
        }
        self.stats.hits.fetch_add(1, Ordering::Relaxed);
        Some(entry_size)
    }

    /// Returns the on-disk size of the cached chunk, or None on miss.
    /// Used by server fast-paths to size a single combined header+payload
    /// buffer before issuing `try_get_into`.
    pub fn entry_size(self: &Arc<Self>, key: &ChunkKey) -> Option<u64> {
        let g = self.inner.lock();
        g.entries.get(key).map(|e| e.size)
    }

    pub fn live_keys(&self) -> Vec<ChunkKey> {
        let g = self.inner.lock();
        g.entries.keys().cloned().collect()
    }

    fn touch_lru(self: &Arc<Self>, key: &ChunkKey) -> bool {
        let mut g = self.inner.lock();
        let prev_seq = match g.entries.get(key) {
            Some(e) => e.last_access_seq,
            None => {
                self.stats.misses.fetch_add(1, Ordering::Relaxed);
                return false;
            }
        };
        g.lru.remove(&prev_seq);
        g.seq += 1;
        let new_seq = g.seq;
        if let Some(entry) = g.entries.get_mut(key) {
            entry.last_access_seq = new_seq;
        }
        g.lru.insert(new_seq, key.clone());
        true
    }

    pub fn insert(self: &Arc<Self>, key: ChunkKey, data: &[u8]) -> Result<()> {
        let path = self.path_for(&key);
        let tmp = self
            .root
            .join(format!(".tmp.{}.{}", std::process::id(), rand_hex()));
        {
            use std::io::Write;
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(data)?;
            f.sync_data()?;
        }
        std::fs::rename(&tmp, &path)?;
        let size = data.len() as u64;
        let mut g = self.inner.lock();
        g.seq += 1;
        let seq = g.seq;
        if let Some(prev) = g.entries.insert(
            key.clone(),
            Entry {
                size,
                last_access_seq: seq,
            },
        ) {
            g.lru.remove(&prev.last_access_seq);
            g.bytes = g.bytes.saturating_sub(prev.size);
        }
        g.lru.insert(seq, key);
        g.bytes += size;
        self.stats.inserts.fetch_add(1, Ordering::Relaxed);
        self.stats.bytes_in_use.store(g.bytes, Ordering::Relaxed);
        self.evict_if_needed(&mut g);
        Ok(())
    }

    fn evict_if_needed(&self, g: &mut Inner) {
        while g.bytes > self.max_bytes {
            let (&seq, key) = match g.lru.iter().next() {
                Some(p) => (p.0, p.1.clone()),
                None => break,
            };
            g.lru.remove(&seq);
            if let Some(e) = g.entries.remove(&key) {
                let p = self.path_for(&key);
                let _ = std::fs::remove_file(&p);
                g.bytes = g.bytes.saturating_sub(e.size);
                self.stats.evictions.fetch_add(1, Ordering::Relaxed);
            }
        }
        self.stats.bytes_in_use.store(g.bytes, Ordering::Relaxed);
    }

    pub fn remove(self: &Arc<Self>, key: &ChunkKey) -> Result<()> {
        let path = self.path_for(key);
        let _ = std::fs::remove_file(&path);
        let mut g = self.inner.lock();
        if let Some(e) = g.entries.remove(key) {
            g.lru.remove(&e.last_access_seq);
            g.bytes = g.bytes.saturating_sub(e.size);
            self.stats.bytes_in_use.store(g.bytes, Ordering::Relaxed);
        }
        Ok(())
    }

    /// Drop every cached chunk: remove tracked files, clear the LRU/entries
    /// maps, reset bytes_in_use to zero, and sweep any stray files left in
    /// the cache root (mirrors scan_existing's behaviour). Used by the
    /// /clear-cache admin endpoint to reproducibly return a node to the
    /// post-startup empty state without restarting the daemon. Concurrent
    /// inserts may briefly race past the lock release; the next /clear-cache
    /// or scan_existing call will sweep any survivors.
    pub fn clear_all(self: &Arc<Self>) -> Result<(u64, u64)> {
        let mut g = self.inner.lock();
        let mut removed_files = 0u64;
        let mut removed_bytes = 0u64;
        let keys: Vec<ChunkKey> = g.entries.keys().cloned().collect();
        for k in keys {
            if let Some(e) = g.entries.remove(&k) {
                let p = self.path_for(&k);
                if std::fs::remove_file(&p).is_ok() {
                    removed_files += 1;
                    removed_bytes = removed_bytes.saturating_add(e.size);
                }
                g.lru.remove(&e.last_access_seq);
            }
        }
        g.bytes = 0;
        g.lru.clear();
        self.stats.bytes_in_use.store(0, Ordering::Relaxed);
        drop(g);
        // Sweep any stray files (untracked tmp files, files left by a
        // concurrent inserter, or pre-existing junk). Mirrors scan_existing.
        if let Ok(rd) = std::fs::read_dir(&self.root) {
            for e in rd.flatten() {
                if let Ok(m) = e.metadata() {
                    if m.is_file() {
                        let _ = std::fs::remove_file(e.path());
                    }
                }
            }
        }
        Ok((removed_files, removed_bytes))
    }

    pub fn local_path(&self, key: &ChunkKey) -> Option<PathBuf> {
        let p = self.path_for(key);
        if p.exists() {
            Some(p)
        } else {
            None
        }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }
}

fn rand_hex() -> String {
    let n = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    format!("{n:x}")
}
