//! Out-of-band (OOB) rendezvous service for UCC team bootstrap.
//!
//! UCC requires an OOB allgather during `ucc_context_create_post` and
//! `ucc_team_create_post` to exchange context/team addresses across the
//! cluster. We implement that exchange over HTTP, piggybacking on the
//! existing gossip server (port 7771).
//!
//! ## Coordinator election
//! The lowest sorted node-id among `members_alive() ∪ {me}` is the
//! coordinator (rank 0). All other ranks POST their per-rank contributions
//! to the coordinator and then GET the concatenated result back.
//!
//! ## Wire protocol
//!   POST /ucc/oob/{tag}/{rank}  body=raw bytes      → 200 on store
//!   GET  /ucc/oob/{tag}                             → 200 with full
//!                                                     concatenation once
//!                                                     all `world` ranks
//!                                                     have posted (long
//!                                                     poll up to 60s)
//!
//! `tag` is an opaque ASCII identifier supplied by UCC (we forward it
//! verbatim from the C callback).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::Notify;

use crate::cluster::Membership;
use crate::error::{BcError, Result};

#[derive(Default)]
struct TagState {
    /// `len() == world`; `Some(bytes)` once that rank has POSTed.
    contributions: Vec<Option<Vec<u8>>>,
    /// How many ranks have posted so far (caches `contributions.iter().filter(Some).count()`).
    filled: u32,
}

/// Coordinator-side state: shared across the gossip-server request handlers.
///
/// Wrapped in `Arc` so the gossip server (on port 7771) can hold a clone
/// alongside the `Membership` clone.
pub struct OobCoordinator {
    inner: Mutex<HashMap<String, TagState>>,
    /// Notified whenever a POST lands; GET long-pollers wake up and re-check.
    notify: Notify,
}

impl OobCoordinator {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(HashMap::new()),
            notify: Notify::new(),
        })
    }

    /// Store a rank's contribution. `world` is required so the coordinator
    /// can pre-size `contributions` on first arrival without trusting any
    /// out-of-band hint.
    pub fn store(&self, tag: &str, rank: u32, world: u32, bytes: Vec<u8>) -> Result<()> {
        if rank >= world {
            return Err(BcError::Other(format!(
                "ucc_oob: rank {rank} >= world {world}"
            )));
        }
        let mut g = self.inner.lock();
        let state = g.entry(tag.to_string()).or_insert_with(|| TagState {
            contributions: vec![None; world as usize],
            filled: 0,
        });
        if state.contributions.len() != world as usize {
            return Err(BcError::Other(format!(
                "ucc_oob: tag {tag} world mismatch: have {} got {world}",
                state.contributions.len()
            )));
        }
        let slot = &mut state.contributions[rank as usize];
        if slot.is_none() {
            *slot = Some(bytes);
            state.filled += 1;
        } else {
            // Idempotent overwrite is dangerous (different rank-local random
            // address each call), so we reject.
            return Err(BcError::Other(format!(
                "ucc_oob: tag {tag} rank {rank} already posted"
            )));
        }
        drop(g);
        self.notify.notify_waiters();
        Ok(())
    }

    /// Block (with timeout) until all `world` ranks have POSTed for `tag`,
    /// then return the concatenated payload (rank 0's bytes ‖ rank 1's ‖ …).
    /// All ranks must contribute the same payload size; we do not validate
    /// that here (UCC's OOB contract is that the caller knows `size`).
    pub async fn collect(&self, tag: &str, world: u32, timeout: Duration) -> Result<Vec<u8>> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            {
                let g = self.inner.lock();
                if let Some(state) = g.get(tag) {
                    if state.filled == world {
                        let mut out = Vec::with_capacity(
                            state.contributions.iter().map(|c| c.as_ref().map_or(0, |b| b.len())).sum(),
                        );
                        for slot in &state.contributions {
                            // Safe: filled == world means all slots are Some.
                            out.extend_from_slice(slot.as_ref().unwrap());
                        }
                        return Ok(out);
                    }
                }
            }
            let now = tokio::time::Instant::now();
            if now >= deadline {
                return Err(BcError::Other(format!(
                    "ucc_oob: timeout collecting tag {tag} (world={world})"
                )));
            }
            let _ = tokio::time::timeout(deadline - now, self.notify.notified()).await;
        }
    }

    /// Drop a tag's accumulated state — UCC calls `req_free` after a
    /// successful allgather, signalling we can release the buffers.
    pub fn release(&self, tag: &str) {
        let mut g = self.inner.lock();
        g.remove(tag);
    }
}

/// Pick the coordinator URL deterministically: lowest sorted node-id wins.
///
/// Returns `(coordinator_gossip_url, my_rank, world_size)`. The rank is the
/// 0-based index of `me_id` in the sorted member list (including ourselves).
pub fn elect_coordinator(membership: &Membership) -> (String, u32, u32) {
    let mut ids: Vec<(String, String)> = membership
        .members_alive()
        .into_iter()
        .map(|n| (n.id, n.gossip_url))
        .collect();
    let me = membership.me_snapshot();
    ids.push((me.id.clone(), me.gossip_url.clone()));
    ids.sort_by(|a, b| a.0.cmp(&b.0));
    // Dedup in case me_snapshot is also in members_alive (it shouldn't be
    // by current convention, but cheap insurance).
    ids.dedup_by(|a, b| a.0 == b.0);
    let world = ids.len() as u32;
    let my_rank = ids
        .iter()
        .position(|(id, _)| id == &me.id)
        .expect("me is in the sorted list") as u32;
    let coord_url = ids[0].1.clone();
    (coord_url, my_rank, world)
}

/// Client-side: POST our rank's contribution to the coordinator, then GET
/// the assembled result back.
///
/// Returns the concatenated allgather output (rank-ordered).
pub async fn allgather_via_coordinator(
    client: &reqwest::Client,
    coordinator_url: &str,
    tag: &str,
    rank: u32,
    world: u32,
    payload: Vec<u8>,
) -> Result<Vec<u8>> {
    let post_url = format!("{}/ucc/oob/{}/{}", coordinator_url.trim_end_matches('/'), tag, rank);
    let resp = client
        .post(&post_url)
        .query(&[("world", world.to_string())])
        .body(payload)
        .send()
        .await
        .map_err(|e| BcError::Other(format!("ucc_oob POST {post_url}: {e}")))?;
    if !resp.status().is_success() {
        return Err(BcError::Other(format!(
            "ucc_oob POST {post_url} status {}",
            resp.status()
        )));
    }
    drop(resp);

    let get_url = format!("{}/ucc/oob/{}", coordinator_url.trim_end_matches('/'), tag);
    let resp = client
        .get(&get_url)
        .timeout(Duration::from_secs(120))
        .send()
        .await
        .map_err(|e| BcError::Other(format!("ucc_oob GET {get_url}: {e}")))?;
    if !resp.status().is_success() {
        return Err(BcError::Other(format!(
            "ucc_oob GET {get_url} status {}",
            resp.status()
        )));
    }
    let body = resp
        .bytes()
        .await
        .map_err(|e| BcError::Other(format!("ucc_oob GET {get_url} body: {e}")))?;
    Ok(body.to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn store_then_collect_concatenates_in_rank_order() {
        let coord = OobCoordinator::new();
        coord.store("t1", 0, 3, b"AAA".to_vec()).unwrap();
        coord.store("t1", 2, 3, b"CCC".to_vec()).unwrap();
        coord.store("t1", 1, 3, b"BBB".to_vec()).unwrap();
        let out = coord.collect("t1", 3, Duration::from_secs(1)).await.unwrap();
        assert_eq!(out, b"AAABBBCCC");
    }

    #[tokio::test]
    async fn collect_blocks_until_all_ranks_post() {
        let coord = OobCoordinator::new();
        let coord2 = coord.clone();
        let h = tokio::spawn(async move {
            coord2.collect("t2", 2, Duration::from_secs(2)).await.unwrap()
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        coord.store("t2", 0, 2, b"X".to_vec()).unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        coord.store("t2", 1, 2, b"Y".to_vec()).unwrap();
        let out = h.await.unwrap();
        assert_eq!(out, b"XY");
    }

    #[tokio::test]
    async fn double_post_same_rank_rejected() {
        let coord = OobCoordinator::new();
        coord.store("t3", 0, 2, b"A".to_vec()).unwrap();
        let err = coord.store("t3", 0, 2, b"A".to_vec()).unwrap_err();
        assert!(format!("{err}").contains("already posted"));
    }

    #[tokio::test]
    async fn release_clears_state() {
        let coord = OobCoordinator::new();
        coord.store("t4", 0, 1, b"Z".to_vec()).unwrap();
        coord.release("t4");
        // After release, posting rank 0 again is fine (state was wiped).
        coord.store("t4", 0, 1, b"Z".to_vec()).unwrap();
    }

    #[tokio::test]
    async fn rank_out_of_bounds_rejected() {
        let coord = OobCoordinator::new();
        let err = coord.store("t5", 5, 3, b"X".to_vec()).unwrap_err();
        assert!(format!("{err}").contains("rank 5 >= world 3"));
    }
}
