use bytes::Bytes;
use parking_lot::Mutex;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::SystemTime;

use crate::error::{BcError, Result};

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
        let mut total = 0u64;
        let entries: Vec<_> = std::fs::read_dir(&self.root)?
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                let m = e.metadata().ok()?;
                if !m.is_file() {
                    return None;
                }
                let mtime = m
                    .modified()
                    .ok()
                    .and_then(|t| t.duration_since(SystemTime::UNIX_EPOCH).ok())
                    .map(|d| d.as_micros() as u64)
                    .unwrap_or(0);
                Some((e.file_name().into_string().ok()?, m.len(), mtime))
            })
            .collect();
        let mut g = self.inner.lock();
        for (name, size, mtime) in entries {
            let key = ChunkKey {
                mount: format!("__legacy__{name}"),
                blob: name.clone(),
                offset: 0,
            };
            g.seq = g.seq.max(mtime);
            g.entries.insert(
                key.clone(),
                Entry {
                    size,
                    last_access_seq: mtime,
                },
            );
            g.lru.insert(mtime, key);
            total += size;
        }
        g.bytes = total;
        self.stats.bytes_in_use.store(total, Ordering::Relaxed);
        Ok(())
    }

    fn path_for(&self, key: &ChunkKey) -> PathBuf {
        self.root.join(key.cache_filename())
    }

    pub fn try_get(self: &Arc<Self>, key: &ChunkKey) -> Option<Bytes> {
        let path = self.path_for(key);
        let data = match std::fs::read(&path) {
            Ok(d) => d,
            Err(_) => {
                self.stats.misses.fetch_add(1, Ordering::Relaxed);
                return None;
            }
        };
        let mut g = self.inner.lock();
        let exists = g.entries.contains_key(key);
        if exists {
            let prev_seq = g.entries.get(key).map(|e| e.last_access_seq).unwrap_or(0);
            g.lru.remove(&prev_seq);
            g.seq += 1;
            let new_seq = g.seq;
            if let Some(entry) = g.entries.get_mut(key) {
                entry.last_access_seq = new_seq;
            }
            g.lru.insert(new_seq, key.clone());
        } else {
            g.seq += 1;
            let seq = g.seq;
            let size = data.len() as u64;
            g.entries.insert(
                key.clone(),
                Entry {
                    size,
                    last_access_seq: seq,
                },
            );
            g.lru.insert(seq, key.clone());
            g.bytes += size;
            self.stats.bytes_in_use.store(g.bytes, Ordering::Relaxed);
        }
        self.stats.hits.fetch_add(1, Ordering::Relaxed);
        Some(Bytes::from(data))
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
