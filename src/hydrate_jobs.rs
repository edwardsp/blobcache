use crate::hydrate::HydrateResponse;
use dashmap::DashMap;
use parking_lot::Mutex;
use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Terminal-state retention so clients can poll `GET /hydrate/{id}` after
/// the job finishes. After this window the entry is evicted on next access.
const COMPLETED_TTL: Duration = Duration::from_secs(3600);

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum JobStatus {
    Pending,
    Running,
    Completed,
    Failed,
}

#[derive(Debug)]
pub struct JobState {
    pub status: JobStatus,
    pub started_at: Instant,
    pub finished_at: Option<Instant>,
    pub result: Option<HydrateResponse>,
    pub error: Option<String>,
}

#[derive(Serialize)]
pub struct JobView {
    pub job_id: String,
    pub status: JobStatus,
    pub elapsed_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<HydrateResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Minimum interval between GC sweeps (milliseconds).
const GC_INTERVAL_MS: u64 = 5_000;

#[derive(Clone, Default)]
pub struct HydrateJobs {
    inner: Arc<DashMap<String, Arc<Mutex<JobState>>>>,
    /// Epoch-ms timestamp of the last completed GC sweep.
    /// Used by `maybe_gc` to throttle full-map scans to at most once per
    /// `GC_INTERVAL_MS`.  A compare-exchange on this field also acts as a
    /// lightweight mutex so that at most one concurrent caller performs the
    /// sweep even under high concurrency.
    last_gc_ms: Arc<AtomicU64>,
}

impl HydrateJobs {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(DashMap::new()),
            last_gc_ms: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn create(&self) -> (String, Arc<Mutex<JobState>>) {
        let id = uuid::Uuid::new_v4().to_string();
        let state = Arc::new(Mutex::new(JobState {
            status: JobStatus::Pending,
            started_at: Instant::now(),
            finished_at: None,
            result: None,
            error: None,
        }));
        self.inner.insert(id.clone(), state.clone());
        self.maybe_gc();
        (id, state)
    }

    pub fn view(&self, id: &str) -> Option<JobView> {
        self.maybe_gc();
        let entry = self.inner.get(id)?;
        let g = entry.lock();
        let now = Instant::now();
        let elapsed = g
            .finished_at
            .unwrap_or(now)
            .saturating_duration_since(g.started_at);
        Some(JobView {
            job_id: id.to_string(),
            status: g.status.clone(),
            elapsed_ms: elapsed.as_millis() as u64,
            result: g.result.clone(),
            error: g.error.clone(),
        })
    }

    /// Throttled GC entry point.  Runs at most once per `GC_INTERVAL_MS`.
    ///
    /// The CAS on `last_gc_ms` serves two purposes:
    /// 1. **Rate-limiting**: skip the sweep if fewer than 5 s have elapsed.
    /// 2. **Single-writer**: if two callers race, only the one that wins the
    ///    CAS proceeds; the loser returns immediately, avoiding redundant work.
    ///
    /// Clock skew / `duration_since` failure yields `now_ms = 0`, which
    /// always satisfies `saturating_sub(last) < GC_INTERVAL_MS` (since last
    /// is also 0 on first call, but 0.saturating_sub(0) == 0 < 5000 only on
    /// the very first call — after that `last > 0` so skew causes a skip,
    /// which is the safe choice: we never *need* to GC immediately).
    fn maybe_gc(&self) {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let last = self.last_gc_ms.load(Ordering::Relaxed);
        if now_ms.saturating_sub(last) < GC_INTERVAL_MS {
            return;
        }
        if self
            .last_gc_ms
            .compare_exchange(last, now_ms, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        self.gc();
    }

    fn gc(&self) {
        let now = Instant::now();
        self.inner.retain(|_, s| {
            let g = s.lock();
            match g.finished_at {
                Some(t) => now.saturating_duration_since(t) < COMPLETED_TTL,
                None => true,
            }
        });
    }
}
