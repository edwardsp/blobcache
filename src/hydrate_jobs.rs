use crate::hydrate::HydrateResponse;
use dashmap::DashMap;
use parking_lot::Mutex;
use serde::Serialize;
use std::sync::Arc;
use std::time::{Duration, Instant};

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

#[derive(Clone, Default)]
pub struct HydrateJobs {
    inner: Arc<DashMap<String, Arc<Mutex<JobState>>>>,
}

impl HydrateJobs {
    pub fn new() -> Self {
        Self::default()
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
        self.gc();
        (id, state)
    }

    pub fn view(&self, id: &str) -> Option<JobView> {
        self.gc();
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
