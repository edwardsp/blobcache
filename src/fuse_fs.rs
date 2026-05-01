use dashmap::DashMap;
use fuser::{
    FileAttr, FileType, Filesystem, KernelConfig, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry,
    Request,
};
use libc::{EIO, ENOENT, ENOTDIR};
use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::runtime::Handle;
use tokio::sync::RwLock;

use crate::azure::BlobClient;
use crate::config::MountConfig;
use crate::fetcher::Fetcher;

const TTL: Duration = Duration::from_secs(30);
const ROOT_INO: u64 = 1;
// Hard cap on tracked children per directory. A blob container with millions
// of objects at one prefix would otherwise pin O(N) heap. Above the cap we
// stop accepting new entries from listings; direct lookup() (HEAD) still works
// for callers that know the exact path.
const MAX_CHILDREN_PER_DIR: usize = 100_000;

#[derive(Clone, Debug)]
struct Node {
    ino: u64,
    parent: u64,
    name: String,
    full_path: String,
    kind: NodeKind,
}

#[derive(Clone, Debug)]
enum NodeKind {
    Dir {
        children_loaded: bool,
    },
    File {
        size: u64,
        etag: Option<String>,
        mtime: SystemTime,
    },
}

struct Inner {
    by_ino: HashMap<u64, Node>,
    by_parent_name: HashMap<(u64, String), u64>,
    children: HashMap<u64, Vec<u64>>,
    next_ino: u64,
}

pub struct BlobFs {
    mount: MountConfig,
    blob: Arc<BlobClient>,
    fetcher: Arc<Fetcher>,
    inner: Arc<RwLock<Inner>>,
    rt: Handle,
    listing_cache_ttl: Duration,
    last_listing: Arc<DashMap<u64, std::time::Instant>>,
    fuse_reads: Arc<AtomicU64>,
    fuse_read_bytes: Arc<AtomicU64>,
}

impl BlobFs {
    pub fn new(
        mount: MountConfig,
        blob: Arc<BlobClient>,
        fetcher: Arc<Fetcher>,
        rt: Handle,
    ) -> Self {
        let mut by_ino = HashMap::new();
        let by_parent_name = HashMap::new();
        let mut children = HashMap::new();
        by_ino.insert(
            ROOT_INO,
            Node {
                ino: ROOT_INO,
                parent: ROOT_INO,
                name: "".into(),
                full_path: "".into(),
                kind: NodeKind::Dir {
                    children_loaded: false,
                },
            },
        );
        children.insert(ROOT_INO, Vec::new());
        Self {
            mount,
            blob,
            fetcher,
            inner: Arc::new(RwLock::new(Inner {
                by_ino,
                by_parent_name,
                children,
                next_ino: 2,
            })),
            rt,
            listing_cache_ttl: Duration::from_secs(30),
            last_listing: Arc::new(DashMap::new()),
            fuse_reads: Arc::new(AtomicU64::new(0)),
            fuse_read_bytes: Arc::new(AtomicU64::new(0)),
        }
    }

    fn attr_for(node: &Node, uid: u32, gid: u32) -> FileAttr {
        match &node.kind {
            NodeKind::Dir { .. } => FileAttr {
                ino: node.ino,
                size: 0,
                blocks: 0,
                atime: SystemTime::UNIX_EPOCH,
                mtime: SystemTime::UNIX_EPOCH,
                ctime: SystemTime::UNIX_EPOCH,
                crtime: SystemTime::UNIX_EPOCH,
                kind: FileType::Directory,
                perm: 0o555,
                nlink: 2,
                uid,
                gid,
                rdev: 0,
                blksize: 4096,
                flags: 0,
            },
            NodeKind::File { size, mtime, .. } => FileAttr {
                ino: node.ino,
                size: *size,
                blocks: (*size).div_ceil(512),
                atime: *mtime,
                mtime: *mtime,
                ctime: *mtime,
                crtime: *mtime,
                kind: FileType::RegularFile,
                perm: 0o444,
                nlink: 1,
                uid,
                gid,
                rdev: 0,
                blksize: 4096,
                flags: 0,
            },
        }
    }

    async fn ensure_dir_listed(&self, ino: u64) -> std::result::Result<(), i32> {
        let need_load = {
            let g = self.inner.read().await;
            let n = g.by_ino.get(&ino).ok_or(ENOENT)?;
            match n.kind {
                NodeKind::Dir { children_loaded } => {
                    let recent = self
                        .last_listing
                        .get(&ino)
                        .map(|t| t.elapsed() < self.listing_cache_ttl)
                        .unwrap_or(false);
                    !children_loaded || !recent
                }
                _ => return Err(ENOTDIR),
            }
        };
        if !need_load {
            return Ok(());
        }
        let prefix = {
            let g = self.inner.read().await;
            let n = g.by_ino.get(&ino).ok_or(ENOENT)?;
            let mut p = if self.mount.prefix.is_empty() {
                String::new()
            } else {
                format!("{}/", self.mount.prefix.trim_matches('/'))
            };
            if !n.full_path.is_empty() {
                p.push_str(&n.full_path);
                p.push('/');
            }
            p
        };
        let pref_arg = if prefix.is_empty() {
            None
        } else {
            Some(prefix.as_str())
        };
        let (blobs, prefixes) = self
            .blob
            .list_blobs(&self.mount.account, &self.mount.container, pref_arg, false)
            .await
            .map_err(|e| {
                tracing::error!(error=%e, "list_blobs failed");
                EIO
            })?;

        let mut g = self.inner.write().await;
        let strip = prefix.clone();
        // Track which child names were observed in this listing, then evict
        // any previously-tracked child not present (the blob was deleted or
        // a directory marker is gone). Without this the FS keeps stale
        // entries forever and readdir lies about existing files.
        let mut seen: HashSet<String> = HashSet::new();
        let mut over_cap_logged = false;
        for p in prefixes {
            let rel = p
                .strip_prefix(&strip)
                .unwrap_or(&p)
                .trim_end_matches('/')
                .to_string();
            if rel.is_empty() {
                continue;
            }
            let cur = g.children.get(&ino).map(|v| v.len()).unwrap_or(0);
            if cur >= MAX_CHILDREN_PER_DIR && !g.by_parent_name.contains_key(&(ino, rel.clone())) {
                if !over_cap_logged {
                    tracing::warn!(
                        parent = ino,
                        cap = MAX_CHILDREN_PER_DIR,
                        "directory child cap reached; further entries dropped from listing"
                    );
                    over_cap_logged = true;
                }
                continue;
            }
            seen.insert(rel.clone());
            self.upsert_dir(&mut g, ino, &rel, &p);
        }
        for b in blobs {
            let rel = b.name.strip_prefix(&strip).unwrap_or(&b.name).to_string();
            if rel.is_empty() || rel.contains('/') {
                continue;
            }
            let cur = g.children.get(&ino).map(|v| v.len()).unwrap_or(0);
            if cur >= MAX_CHILDREN_PER_DIR && !g.by_parent_name.contains_key(&(ino, rel.clone())) {
                if !over_cap_logged {
                    tracing::warn!(
                        parent = ino,
                        cap = MAX_CHILDREN_PER_DIR,
                        "directory child cap reached; further entries dropped from listing"
                    );
                    over_cap_logged = true;
                }
                continue;
            }
            seen.insert(rel.clone());
            let mtime = SystemTime::now();
            self.upsert_file(&mut g, ino, &rel, &b.name, b.content_length, b.etag, mtime);
        }
        // Delta-delete: anything tracked under this parent that wasn't in the
        // current listing is gone from origin.
        let to_remove: Vec<(String, u64)> = g
            .children
            .get(&ino)
            .map(|kids| {
                kids.iter()
                    .filter_map(|k| g.by_ino.get(k).map(|n| (n.name.clone(), *k)))
                    .filter(|(name, _)| !seen.contains(name))
                    .collect()
            })
            .unwrap_or_default();
        for (name, child_ino) in to_remove {
            g.by_parent_name.remove(&(ino, name));
            if let Some(kids) = g.children.get_mut(&ino) {
                kids.retain(|k| *k != child_ino);
            }
            // Leave by_ino entry; in-flight handles may still reference the
            // ino. New lookups will miss via by_parent_name.
            g.children.remove(&child_ino);
        }
        if let Some(n) = g.by_ino.get_mut(&ino) {
            if let NodeKind::Dir { children_loaded } = &mut n.kind {
                *children_loaded = true;
            }
        }
        drop(g);
        self.last_listing.insert(ino, std::time::Instant::now());
        Ok(())
    }

    fn upsert_dir(&self, g: &mut Inner, parent: u64, name: &str, full_blob_path: &str) {
        if let Some(&existing) = g.by_parent_name.get(&(parent, name.to_string())) {
            let _ = existing;
            return;
        }
        let ino = g.next_ino;
        g.next_ino += 1;
        let trim = full_blob_path.trim_end_matches('/');
        let strip_prefix_len = if self.mount.prefix.is_empty() {
            0
        } else {
            self.mount.prefix.trim_matches('/').len() + 1
        };
        let full_path = trim.get(strip_prefix_len..).unwrap_or("").to_string();
        let node = Node {
            ino,
            parent,
            name: name.to_string(),
            full_path,
            kind: NodeKind::Dir {
                children_loaded: false,
            },
        };
        g.by_ino.insert(ino, node);
        g.by_parent_name.insert((parent, name.to_string()), ino);
        g.children.entry(parent).or_default().push(ino);
        g.children.entry(ino).or_default();
    }

    #[allow(clippy::too_many_arguments)]
    fn upsert_file(
        &self,
        g: &mut Inner,
        parent: u64,
        name: &str,
        full_blob_name: &str,
        size: u64,
        etag: Option<String>,
        mtime: SystemTime,
    ) {
        if let Some(&existing) = g.by_parent_name.get(&(parent, name.to_string())) {
            if let Some(n) = g.by_ino.get_mut(&existing) {
                if let NodeKind::File {
                    size: s,
                    etag: e,
                    mtime: m,
                } = &mut n.kind
                {
                    *s = size;
                    *e = etag;
                    *m = mtime;
                }
            }
            return;
        }
        let strip_prefix_len = if self.mount.prefix.is_empty() {
            0
        } else {
            self.mount.prefix.trim_matches('/').len() + 1
        };
        let full_path = full_blob_name
            .get(strip_prefix_len..)
            .unwrap_or(full_blob_name)
            .to_string();
        let ino = g.next_ino;
        g.next_ino += 1;
        let node = Node {
            ino,
            parent,
            name: name.to_string(),
            full_path,
            kind: NodeKind::File { size, etag, mtime },
        };
        g.by_ino.insert(ino, node);
        g.by_parent_name.insert((parent, name.to_string()), ino);
        g.children.entry(parent).or_default().push(ino);
    }
}

impl Filesystem for BlobFs {
    fn init(
        &mut self,
        _req: &Request<'_>,
        config: &mut KernelConfig,
    ) -> std::result::Result<(), libc::c_int> {
        let target = self.fetcher.chunk_size.max(1) as u32;
        let _ = config.set_max_readahead(target);
        let _ = config.set_max_write(target);
        Ok(())
    }

    fn lookup(&mut self, req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name = name.to_string_lossy().to_string();
        let me = self.clone_handles();
        let uid = req.uid();
        let gid = req.gid();
        self.rt.spawn(async move {
            if let Err(e) = me.ensure_dir_listed(parent).await {
                reply.error(e);
                return;
            }
            let g = me.inner.read().await;
            if let Some(&ino) = g.by_parent_name.get(&(parent, name.clone())) {
                if let Some(n) = g.by_ino.get(&ino) {
                    let attr = BlobFs::attr_for(n, uid, gid);
                    reply.entry(&TTL, &attr, 0);
                    return;
                }
            }
            if let Some(p) = g.by_ino.get(&parent) {
                let prefix = if me.mount.prefix.is_empty() {
                    String::new()
                } else {
                    format!("{}/", me.mount.prefix.trim_matches('/'))
                };
                let candidate = if p.full_path.is_empty() {
                    format!("{prefix}{name}")
                } else {
                    format!("{prefix}{}/{name}", p.full_path)
                };
                drop(g);
                match me
                    .blob
                    .get_blob_properties(&me.mount.account, &me.mount.container, &candidate)
                    .await
                {
                    Ok(info) => {
                        let mut g = me.inner.write().await;
                        me.upsert_file(
                            &mut g,
                            parent,
                            &name,
                            &candidate,
                            info.content_length,
                            info.etag,
                            SystemTime::now(),
                        );
                        let ino = g.by_parent_name[&(parent, name.clone())];
                        let n = g.by_ino[&ino].clone();
                        let attr = BlobFs::attr_for(&n, uid, gid);
                        reply.entry(&TTL, &attr, 0);
                    }
                    Err(_) => reply.error(ENOENT),
                }
            } else {
                reply.error(ENOENT);
            }
        });
    }

    fn getattr(&mut self, req: &Request, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        let me = self.clone_handles();
        let uid = req.uid();
        let gid = req.gid();
        self.rt.spawn(async move {
            let g = me.inner.read().await;
            match g.by_ino.get(&ino) {
                Some(n) => {
                    let attr = BlobFs::attr_for(n, uid, gid);
                    reply.attr(&TTL, &attr);
                }
                None => reply.error(ENOENT),
            }
        });
    }

    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock: Option<u64>,
        reply: ReplyData,
    ) {
        let me = self.clone_handles();
        let fuse_reads = self.fuse_reads.clone();
        let fuse_read_bytes = self.fuse_read_bytes.clone();
        let rid = crate::request_id::RequestId::new();
        let span = tracing::info_span!(
            "fuse_read",
            rid = %rid,
            mount = %me.mount.name,
            ino = ino,
            offset = offset,
            size = size,
        );
        self.rt.spawn(tracing::Instrument::instrument(
            async move {
                let t_read = std::time::Instant::now();
                let (full_path, file_size) = {
                    let g = me.inner.read().await;
                    match g.by_ino.get(&ino) {
                        Some(n) => match &n.kind {
                            NodeKind::File { size, .. } => (n.full_path.clone(), *size),
                            _ => {
                                reply.error(libc::EISDIR);
                                return;
                            }
                        },
                        None => {
                            reply.error(ENOENT);
                            return;
                        }
                    }
                };
                if (offset as u64) >= file_size {
                    reply.data(&[]);
                    return;
                }
                let length = (size as u64).min(file_size - offset as u64);
                fuse_reads.fetch_add(1, Ordering::Relaxed);
                match me
                    .fetcher
                    .fetch_range(
                        &me.mount,
                        &full_path,
                        offset as u64,
                        length,
                        file_size,
                        Some(&rid),
                    )
                    .await
                {
                    Ok(b) => {
                        fuse_read_bytes.fetch_add(b.len() as u64, Ordering::Relaxed);
                        me.fetcher.stats.fuse_reads.inc();
                        me.fetcher.stats.fuse_read_bytes.inc_by(b.len() as u64);
                        me.fetcher
                            .stats
                            .fuse_read_seconds
                            .observe(t_read.elapsed().as_secs_f64());
                        reply.data(&b);
                    }
                    Err(e) => {
                        tracing::error!(?e, "read failed");
                        me.fetcher
                            .stats
                            .fuse_read_seconds
                            .observe(t_read.elapsed().as_secs_f64());
                        reply.error(EIO);
                    }
                }
            },
            span,
        ));
    }

    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let me = self.clone_handles();
        self.rt.spawn(async move {
            if let Err(e) = me.ensure_dir_listed(ino).await {
                reply.error(e);
                return;
            }
            let g = me.inner.read().await;
            let mut entries: Vec<(u64, FileType, String)> = Vec::new();
            entries.push((ino, FileType::Directory, ".".into()));
            let parent_ino = g.by_ino.get(&ino).map(|n| n.parent).unwrap_or(ROOT_INO);
            entries.push((parent_ino, FileType::Directory, "..".into()));
            if let Some(kids) = g.children.get(&ino) {
                for &k in kids {
                    if let Some(n) = g.by_ino.get(&k) {
                        let kind = match n.kind {
                            NodeKind::Dir { .. } => FileType::Directory,
                            NodeKind::File { .. } => FileType::RegularFile,
                        };
                        entries.push((k, kind, n.name.clone()));
                    }
                }
            }
            for (i, (e_ino, kind, name)) in entries.into_iter().enumerate().skip(offset as usize) {
                if reply.add(e_ino, (i + 1) as i64, kind, &name) {
                    break;
                }
            }
            reply.ok();
        });
    }
}

impl BlobFs {
    fn clone_handles(&self) -> Arc<Self> {
        Arc::new(Self {
            mount: self.mount.clone(),
            blob: self.blob.clone(),
            fetcher: self.fetcher.clone(),
            inner: self.inner.clone(),
            rt: self.rt.clone(),
            listing_cache_ttl: self.listing_cache_ttl,
            last_listing: self.last_listing.clone(),
            fuse_reads: self.fuse_reads.clone(),
            fuse_read_bytes: self.fuse_read_bytes.clone(),
        })
    }
}
