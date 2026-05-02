// BlobFetcherPool: N independent tokio runtimes, each owning its own
// reqwest Client (via BlobClient) per mount. Round-robins Azure GETs
// across runtimes so a single node can scale past the ~28 Gbps ceiling
// imposed by a single tokio runtime + connection pool.
//
// Mirrors azcp's `--workers` design: each "worker" is a fully isolated
// runtime so that hot-path tokio task scheduling, reqwest pool locking,
// and rustls handshake state never become a single shared contention
// point. Concurrency sweep showed a single runtime caps near 117 Gbps
// aggregate at N=16 (≈7.3 Gbps/pod) regardless of in-flight tuning;
// scaling per-pod throughput toward azcp's 25-28 Gbps target requires
// multiple independent runtimes.
//
// The pool also exposes a "view" HashMap (workers[0].blobs) for callers
// that need a single Arc<BlobClient> reference without round-robin
// dispatch (FUSE list_blobs, stats /list, hydrate coordinator listing).
// Those paths are metadata-bound, not throughput-bound.

use anyhow::Context;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::auth::Credential;
use crate::azure::BlobClient;
use crate::config::{AzureConfig, MountConfig};
use crate::error::{BcError, Result};
use crate::stats::Stats;

struct Worker {
    rt: tokio::runtime::Runtime,
    blobs: HashMap<String, Arc<BlobClient>>,
}

pub struct BlobFetcherPool {
    workers: Vec<Worker>,
    next: AtomicUsize,
    /// First-worker blob map exposed for legacy callers (stats listing,
    /// hydrate coordinator metadata, FUSE readdir). All workers hold an
    /// equivalent client per mount, so any one of them suffices for
    /// non-throughput paths.
    view: Arc<HashMap<String, Arc<BlobClient>>>,
}

impl BlobFetcherPool {
    pub fn new(
        mounts: &[MountConfig],
        azure: &AzureConfig,
        stats: Arc<Stats>,
        n_workers: usize,
    ) -> anyhow::Result<Arc<Self>> {
        let n = n_workers.max(1);
        let mut workers = Vec::with_capacity(n);
        for i in 0..n {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .thread_name(format!("blob-w{i}"))
                .build()
                .with_context(|| format!("build blob worker runtime {i}"))?;
            let mut blobs = HashMap::new();
            for m in mounts {
                let cred =
                    Credential::resolve(&m.account, m.sas_token.as_deref()).with_context(|| {
                        format!("resolve credentials for mount {} (worker {i})", m.name)
                    })?;
                blobs.insert(
                    m.name.clone(),
                    Arc::new(BlobClient::new(cred, azure, Some(stats.clone()))?),
                );
            }
            workers.push(Worker { rt, blobs });
        }
        let view = Arc::new(workers[0].blobs.clone());
        Ok(Arc::new(Self {
            workers,
            next: AtomicUsize::new(0),
            view,
        }))
    }

    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }

    /// Single-shot view: returns one BlobClient per mount, suitable for
    /// metadata paths (list_blobs, HEAD). All workers' clients are
    /// functionally equivalent; this returns worker 0's set.
    pub fn view(&self) -> Arc<HashMap<String, Arc<BlobClient>>> {
        self.view.clone()
    }

    /// Round-robin GET. Picks the next worker, dispatches the GET on
    /// that worker's runtime (so the request is driven by that runtime's
    /// reactor + reqwest pool), and awaits the result on the caller's
    /// runtime. The handoff cost is one tokio JoinHandle per request,
    /// negligible relative to a >1ms Azure GET.
    pub async fn get_blob_range(
        &self,
        mount: &str,
        account: &str,
        container: &str,
        blob_path: &str,
        offset: u64,
        length: u64,
    ) -> Result<Bytes> {
        let i = self.next.fetch_add(1, Ordering::Relaxed) % self.workers.len();
        let w = &self.workers[i];
        let client = w
            .blobs
            .get(mount)
            .ok_or_else(|| BcError::Other(format!("no blob client for mount {mount}")))?
            .clone();
        let account = account.to_string();
        let container = container.to_string();
        let blob_path = blob_path.to_string();
        let join = w.rt.spawn(async move {
            client
                .get_blob_range(&account, &container, &blob_path, offset, length)
                .await
        });
        match join.await {
            Ok(r) => r,
            Err(e) => Err(BcError::Other(format!("blob worker join: {e}"))),
        }
    }
}
