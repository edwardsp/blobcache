use crate::cluster::Membership;
use crate::error::Result;
use crate::fetcher::Fetcher;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ClearRequest {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClearShardResponse {
    pub files_removed: u64,
    pub bytes_removed: u64,
    pub elapsed_ms: u64,
    pub start_unix_ms: u64,
    pub end_unix_ms: u64,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerPeerClear {
    pub node_id: String,
    pub files_removed: u64,
    pub bytes_removed: u64,
    pub elapsed_ms: u64,
    pub start_unix_ms: u64,
    pub end_unix_ms: u64,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClearResponse {
    pub total_files_removed: u64,
    pub total_bytes_removed: u64,
    pub elapsed_ms: u64,
    pub peers: Vec<PerPeerClear>,
}

pub async fn run_shard(fetcher: Arc<Fetcher>) -> ClearShardResponse {
    let t0 = Instant::now();
    let start_unix_ms = now_unix_ms();
    match fetcher.clear_local_state().await {
        Ok((files, bytes)) => ClearShardResponse {
            files_removed: files,
            bytes_removed: bytes,
            elapsed_ms: t0.elapsed().as_millis() as u64,
            start_unix_ms,
            end_unix_ms: now_unix_ms(),
            error: None,
        },
        Err(e) => ClearShardResponse {
            files_removed: 0,
            bytes_removed: 0,
            elapsed_ms: t0.elapsed().as_millis() as u64,
            start_unix_ms,
            end_unix_ms: now_unix_ms(),
            error: Some(format!("{e}")),
        },
    }
}

pub async fn run_coordinator(
    fetcher: Arc<Fetcher>,
    membership: Membership,
    me_id: String,
    http: reqwest::Client,
) -> Result<ClearResponse> {
    let t0 = Instant::now();
    let mut targets: Vec<(String, Option<String>)> = vec![(me_id.clone(), None)];
    for n in membership.members_alive_same_cluster() {
        targets.push((n.id.clone(), Some(n.transport_url.clone())));
    }
    let n_targets = targets.len();

    // Action 20 from opus_code_eval: derive shard timeout from coordinator
    // timeout minus a fixed safety margin (see hydrate.rs for full rationale).
    const COORD_TO_SHARD_SAFETY_MARGIN: std::time::Duration = std::time::Duration::from_secs(2);
    let global_timeout = std::time::Duration::from_secs(360);
    let shard_timeout = global_timeout
        .checked_sub(COORD_TO_SHARD_SAFETY_MARGIN)
        .unwrap_or(global_timeout / 2);

    let mut handles = Vec::with_capacity(n_targets);
    for (node_id, transport_url) in targets.into_iter() {
        let Some(url) = transport_url else {
            let f = fetcher.clone();
            handles.push(tokio::spawn(async move {
                let r = run_shard(f).await;
                PerPeerClear {
                    node_id,
                    files_removed: r.files_removed,
                    bytes_removed: r.bytes_removed,
                    elapsed_ms: r.elapsed_ms,
                    start_unix_ms: r.start_unix_ms,
                    end_unix_ms: r.end_unix_ms,
                    error: r.error,
                }
            }));
            continue;
        };
        {
            let host = url
                .trim_start_matches("http://")
                .split(':')
                .next()
                .unwrap_or("")
                .to_string();
            let endpoint = format!("http://{host}:7773/clear-cache-shard");
            let http = http.clone();
            handles.push(tokio::spawn(async move {
                let t0 = Instant::now();
                let post_start = now_unix_ms();
                let resp = http
                    .post(&endpoint)
                    .json(&ClearRequest::default())
                    .timeout(shard_timeout)
                    .send()
                    .await;
                match resp {
                    Ok(r) => match r.json::<ClearShardResponse>().await {
                        Ok(s) => PerPeerClear {
                            node_id,
                            files_removed: s.files_removed,
                            bytes_removed: s.bytes_removed,
                            elapsed_ms: s.elapsed_ms,
                            start_unix_ms: s.start_unix_ms,
                            end_unix_ms: s.end_unix_ms,
                            error: s.error,
                        },
                        Err(e) => PerPeerClear {
                            node_id,
                            files_removed: 0,
                            bytes_removed: 0,
                            elapsed_ms: t0.elapsed().as_millis() as u64,
                            start_unix_ms: post_start,
                            end_unix_ms: now_unix_ms(),
                            error: Some(format!("decode: {e}")),
                        },
                    },
                    Err(e) => PerPeerClear {
                        node_id,
                        files_removed: 0,
                        bytes_removed: 0,
                        elapsed_ms: t0.elapsed().as_millis() as u64,
                        start_unix_ms: post_start,
                        end_unix_ms: now_unix_ms(),
                        error: Some(format!("post: {e}")),
                    },
                }
            }));
        }
    }

    let abort_handles: Vec<_> = handles.iter().map(|h| h.abort_handle()).collect();
    let join_all = async {
        let mut out = Vec::with_capacity(n_targets);
        for h in handles {
            match h.await {
                Ok(s) => out.push(s),
                Err(e) => out.push(PerPeerClear {
                    node_id: "unknown".into(),
                    files_removed: 0,
                    bytes_removed: 0,
                    elapsed_ms: 0,
                    start_unix_ms: 0,
                    end_unix_ms: 0,
                    error: Some(format!("join: {e}")),
                }),
            }
        }
        out
    };
    let peers = match tokio::time::timeout(global_timeout, join_all).await {
        Ok(p) => p,
        Err(_) => {
            for ah in abort_handles {
                ah.abort();
            }
            vec![PerPeerClear {
                node_id: "coordinator".into(),
                files_removed: 0,
                bytes_removed: 0,
                elapsed_ms: global_timeout.as_millis() as u64,
                start_unix_ms: 0,
                end_unix_ms: now_unix_ms(),
                error: Some(format!(
                    "coordinator timeout after {}s",
                    global_timeout.as_secs()
                )),
            }]
        }
    };
    let total_files_removed: u64 = peers.iter().map(|p| p.files_removed).sum();
    let total_bytes_removed: u64 = peers.iter().map(|p| p.bytes_removed).sum();
    Ok(ClearResponse {
        total_files_removed,
        total_bytes_removed,
        elapsed_ms: t0.elapsed().as_millis() as u64,
        peers,
    })
}
