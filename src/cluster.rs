//! Gossip-based cluster membership for blobcache.
//!
//! NOTE(opus-eval-15): The gossip protocol is push-pull: each round one node
//! POSTs its full member list to a random peer and receives the peer's full
//! list in response.  Payload size is O(N) per round (each `NodeInfo` entry
//! is ~1–2 KiB once UCX worker addresses are base64-encoded).  The 1 MiB
//! `MAX_GOSSIP_BODY_BYTES` cap puts a hard ceiling around N ≈ 300 nodes.
//! Beyond that, switch to a delta or SWIM-style protocol that only exchanges
//! changed entries.

use parking_lot::RwLock;
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::error::{BcError, Result};

const HEARTBEAT_TIMEOUT_SECS: u64 = 30;
const GOSSIP_INTERVAL_MS: u64 = 1500;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum NodeState {
    Alive,
    Suspect,
    Dead,
}

impl NodeState {
    fn rank(&self) -> u8 {
        match self {
            NodeState::Alive => 0,
            NodeState::Suspect => 1,
            NodeState::Dead => 2,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeInfo {
    pub id: String,
    pub transport_url: String,
    pub gossip_url: String,
    pub cluster_hash: String,
    #[serde(default)]
    pub ucx_worker_addr_b64: Option<String>,
    pub last_seen_unix: u64,
    pub state: NodeState,
    pub incarnation: u64,
    #[serde(default)]
    pub bloom_version: u64,
    /// Admin/stats endpoint URL.  Nodes published before this field existed
    /// advertise `None`; consumers fall back to port substitution from
    /// `transport_url`.  Action 4 from opus_code_eval.
    #[serde(default)]
    pub admin_url: Option<String>,
}

impl NodeInfo {
    /// Returns the admin (stats) URL for this node.
    ///
    /// If the node has published an explicit `admin_url` (Action 4), that is
    /// returned directly.  Otherwise we fall back to the pre-Action-4
    /// behaviour: substitute port 7773 into `transport_url` so that older
    /// peers (which don't publish `admin_url`) still work.
    pub fn effective_admin_url(&self) -> String {
        if let Some(url) = &self.admin_url {
            return url.clone();
        }
        port_substitute(&self.transport_url, 7773)
    }
}

/// Replace the port in `url` with `new_port`.
///
/// Handles `http://host:port/path` and bare `host:port` forms.  If the URL
/// cannot be parsed the original string is returned unchanged.
pub fn port_substitute(url: &str, new_port: u16) -> String {
    // Strip scheme if present.
    let (scheme, rest) = if let Some(s) = url.strip_prefix("http://") {
        ("http://", s)
    } else if let Some(s) = url.strip_prefix("https://") {
        ("https://", s)
    } else {
        ("", url)
    };
    // Split off any path component.
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, ""),
    };
    // Replace the port in the authority (host:port).
    let host = match authority.rfind(':') {
        Some(i) => &authority[..i],
        None => authority,
    };
    format!("{scheme}{host}:{new_port}{path}")
}

#[derive(Serialize, Deserialize)]
pub struct GossipPayload {
    pub from: NodeInfo,
    pub members: Vec<NodeInfo>,
}

#[derive(Clone)]
pub struct Membership {
    pub me_id: String,
    pub me_template: NodeInfo,
    me_incarnation: Arc<AtomicU64>,
    me_bloom_version: Arc<AtomicU64>,
    inner: Arc<RwLock<Inner>>,
    pub stats: Arc<crate::stats::ClusterStats>,
    #[cfg(feature = "ucx")]
    rdma_peer_update_hook: Option<Arc<dyn Fn(&NodeInfo) + Send + Sync>>,
    // Fired when sweep() transitions a peer to Dead. Lets the PeerIndex
    // forget that peer's bloom immediately rather than waiting up to
    // bloom_pull_secs for run_bloom_pull_loop's reconciliation pass.
    #[allow(clippy::type_complexity)]
    on_peer_dead: Option<Arc<dyn Fn(&str) + Send + Sync>>,
}

struct Inner {
    members: HashMap<String, NodeInfo>,
}

impl Membership {
    pub fn new(me: NodeInfo, stats: Arc<crate::stats::ClusterStats>) -> Self {
        let mut m = HashMap::new();
        m.insert(me.id.clone(), me.clone());
        let me_incarnation = Arc::new(AtomicU64::new(me.incarnation.max(1)));
        let me_bloom_version = Arc::new(AtomicU64::new(me.bloom_version));
        Self {
            me_id: me.id.clone(),
            me_template: me,
            me_incarnation,
            me_bloom_version,
            inner: Arc::new(RwLock::new(Inner { members: m })),
            stats,
            #[cfg(feature = "ucx")]
            rdma_peer_update_hook: None,
            on_peer_dead: None,
        }
    }

    pub fn set_on_peer_dead<F>(&mut self, hook: F)
    where
        F: Fn(&str) + Send + Sync + 'static,
    {
        self.on_peer_dead = Some(Arc::new(hook));
    }

    #[cfg(feature = "ucx")]
    pub fn set_rdma_peer_update_hook<F>(&mut self, hook: F)
    where
        F: Fn(&NodeInfo) + Send + Sync + 'static,
    {
        self.rdma_peer_update_hook = Some(Arc::new(hook));
    }

    pub fn me_snapshot(&self) -> NodeInfo {
        let mut me = self.me_template.clone();
        me.incarnation = self.me_incarnation.load(Ordering::Relaxed);
        me.bloom_version = self.me_bloom_version.load(Ordering::Relaxed);
        me.last_seen_unix = unix_now();
        me.state = NodeState::Alive;
        me
    }

    pub fn set_bloom_version(&self, v: u64) {
        self.me_bloom_version.store(v, Ordering::Relaxed);
    }

    pub fn members_alive(&self) -> Vec<NodeInfo> {
        let g = self.inner.read();
        g.members
            .values()
            .filter(|n| {
                n.state == NodeState::Alive
                    && n.id != self.me_id
                    && n.cluster_hash == self.me_template.cluster_hash
            })
            .cloned()
            .collect()
    }

    /// Like [`members_alive`] but makes the cluster-boundary filter explicit
    /// for fan-out call sites (hydrate, clear, peer fetch candidates) so that
    /// requests never cross cluster boundaries when multiple logical clusters
    /// share the same gossip topology.  Action 14 from opus_code_eval.
    ///
    /// Semantically identical to [`members_alive`] — both already filter on
    /// `cluster_hash` — but the distinct name documents intent at each call
    /// site and guards against future refactors that might relax the filter in
    /// [`members_alive`] for observability purposes.
    pub fn members_alive_same_cluster(&self) -> Vec<NodeInfo> {
        self.members_alive()
    }

    pub fn members_all(&self) -> Vec<NodeInfo> {
        self.inner.read().members.values().cloned().collect()
    }

    // Merge with SWIM-like precedence: higher (incarnation, state-rank) wins,
    // where state rank Dead > Suspect > Alive disambiguates same incarnation.
    // Mismatched cluster_hash entries are dropped (defence-in-depth against a
    // misconfigured node forwarding members from another cluster).
    pub fn merge(&self, incoming: &[NodeInfo]) {
        let now = unix_now();
        #[cfg(feature = "ucx")]
        let mut update_peers = Vec::new();
        let mut g = self.inner.write();
        for n in incoming {
            if n.id == self.me_id {
                let our_inc = self.me_incarnation.load(Ordering::Relaxed);
                let claims_we_are_down = n.state != NodeState::Alive;
                if n.incarnation > our_inc || (n.incarnation == our_inc && claims_we_are_down) {
                    let new_inc = n.incarnation.max(our_inc) + 1;
                    self.me_incarnation.store(new_inc, Ordering::Relaxed);
                    tracing::info!(
                        peer_inc = n.incarnation,
                        new_inc,
                        "refuting suspicion; bumped incarnation"
                    );
                }
                let mut me = self.me_template.clone();
                me.incarnation = self.me_incarnation.load(Ordering::Relaxed);
                me.last_seen_unix = now;
                me.state = NodeState::Alive;
                g.members.insert(self.me_id.clone(), me);
                continue;
            }
            if n.cluster_hash != self.me_template.cluster_hash {
                self.stats.config_mismatches.inc();
                continue;
            }
            match g.members.get(&n.id) {
                Some(existing) => {
                    // SWIM merge precedence:
                    //   1. higher incarnation always wins (definitive refresh)
                    //   2. same incarnation: freshest last_seen wins (recency beats stale rank)
                    //   3. same incarnation AND same timestamp: more-severe state wins (rank tiebreak)
                    // Without rule 2, stale Dead@old records persist after a peer recovers
                    // (fresh Alive can't satisfy any branch) AND fresh Alive observations
                    // get clobbered by stale Dead replies (rank-only check), partitioning
                    // membership permanently after any rolling restart.
                    let should_update = n.incarnation > existing.incarnation
                        || (n.incarnation == existing.incarnation
                            && n.last_seen_unix > existing.last_seen_unix)
                        || (n.incarnation == existing.incarnation
                            && n.last_seen_unix == existing.last_seen_unix
                            && n.state.rank() > existing.state.rank());
                    if should_update {
                        #[cfg(feature = "ucx")]
                        let should_ensure = existing.ucx_worker_addr_b64 != n.ucx_worker_addr_b64;
                        g.members.insert(n.id.clone(), n.clone());
                        #[cfg(feature = "ucx")]
                        if should_ensure {
                            update_peers.push(n.clone());
                        }
                    }
                }
                None => {
                    g.members.insert(n.id.clone(), n.clone());
                    self.stats.joins.inc();
                    tracing::info!(id=%n.id, url=%n.transport_url, "peer joined");
                    #[cfg(feature = "ucx")]
                    update_peers.push(n.clone());
                }
            }
        }
        drop(g);

        #[cfg(feature = "ucx")]
        if let Some(hook) = &self.rdma_peer_update_hook {
            for peer in update_peers {
                hook(&peer);
            }
        }
    }

    pub fn touch_peer(&self, id: &str) {
        let now = unix_now();
        let mut g = self.inner.write();
        if let Some(n) = g.members.get_mut(id) {
            n.last_seen_unix = now;
            if n.state != NodeState::Alive {
                tracing::info!(id, "peer recovered");
                n.state = NodeState::Alive;
            }
        }
    }

    pub fn sweep(&self) {
        let now = unix_now();
        let mut newly_dead: Vec<String> = Vec::new();
        {
            let mut g = self.inner.write();
            for (id, n) in g.members.iter_mut() {
                if id == &self.me_id {
                    continue;
                }
                let age = now.saturating_sub(n.last_seen_unix);
                let new_state = if age > HEARTBEAT_TIMEOUT_SECS * 2 {
                    NodeState::Dead
                } else if age > HEARTBEAT_TIMEOUT_SECS {
                    NodeState::Suspect
                } else {
                    NodeState::Alive
                };
                if new_state != n.state {
                    tracing::info!(id, ?new_state, age_secs = age, "state change");
                    if matches!(new_state, NodeState::Dead) {
                        self.stats.failures.inc();
                        newly_dead.push(id.clone());
                    }
                    n.state = new_state;
                }
            }
        }
        if let Some(hook) = &self.on_peer_dead {
            for id in &newly_dead {
                hook(id);
            }
        }
    }
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

pub struct GossipServer {
    pub membership: Membership,
    pub peer_index: Option<Arc<crate::peerindex::PeerIndex>>,
}

impl GossipServer {
    pub fn new(membership: Membership) -> Self {
        Self {
            membership,
            peer_index: None,
        }
    }

    pub fn with_peer_index(mut self, peer_index: Arc<crate::peerindex::PeerIndex>) -> Self {
        self.peer_index = Some(peer_index);
        self
    }

    // NOTE(opus-eval-18): Known refactor target. This function is a single
    // ~120-line nested closure handling every gossip route inline.  It should
    // be split into per-route handler functions to improve testability and
    // allow middleware insertion.  Deferred: zero behavioural change but high
    // churn; tracked in opus_code_eval_actions.md row 18.
    pub async fn serve(self, addr: SocketAddr) -> Result<()> {
        use http_body_util::{BodyExt, Full};
        use hyper::body::{Bytes as HBytes, Incoming};
        use hyper::server::conn::http1;
        use hyper::service::service_fn;
        use hyper::{Method, Request, Response};
        use hyper_util::rt::TokioIo;
        use std::convert::Infallible;
        use tokio::net::TcpListener;

        let listener = TcpListener::bind(addr).await?;
        tracing::info!(%addr, "cluster gossip listening");
        let me = self.membership;
        let pi = self.peer_index;
        loop {
            let (stream, _peer) = listener.accept().await?;
            let me = me.clone();
            let pi = pi.clone();
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let _ = http1::Builder::new().serve_connection(io, service_fn(move |req: Request<Incoming>| {
                    let me = me.clone();
                    let pi = pi.clone();
                    async move {
                        let path = req.uri().path().to_string();
                        let method = req.method().clone();
                        let resp = match (&method, path.as_str()) {
                            (&Method::POST, "/cluster/sync") => {
                                let body = req.collect().await.map(|b| b.to_bytes()).unwrap_or_default();
                                if body.len() > MAX_GOSSIP_BODY_BYTES {
                                    return Ok::<_, Infallible>(crate::http_util::error_response(
                                        hyper::StatusCode::PAYLOAD_TOO_LARGE,
                                        "too large",
                                    ));
                                }
                                match serde_json::from_slice::<GossipPayload>(&body) {
                                    Ok(payload) => {
                                        let from_id = payload.from.id.clone();
                                        if payload.from.cluster_hash != me.me_template.cluster_hash {
                                            me.stats.config_mismatches.inc();
                                            tracing::warn!(peer=%from_id, "cluster_hash mismatch — refusing merge");
                                            return Ok::<_, Infallible>(crate::http_util::error_response(
                                                hyper::StatusCode::CONFLICT,
                                                "{\"error\":\"cluster_hash mismatch\"}",
                                            ));
                                        }
                                        let mut all = payload.members;
                                        all.push(payload.from);
                                        me.merge(&all);
                                        me.touch_peer(&from_id);
                                        let response_payload = GossipPayload {
                                            from: me.me_snapshot(),
                                            members: me.members_all(),
                                        };
                                        let body = serde_json::to_vec(&response_payload).unwrap();
                                        crate::http_util::ok_response(body)
                                    }
                                    Err(e) => crate::http_util::error_response(
                                        hyper::StatusCode::BAD_REQUEST,
                                        &format!("bad: {e}"),
                                    ),
                                }
                            }
                            (&Method::GET, "/cluster/bloom") => match &pi {
                                Some(idx) => {
                                    let (v, body) = idx.local_snapshot();
                                    Response::builder().status(200)
                                        .header("content-type", "application/octet-stream")
                                        .header("x-blobcache-bloom-version", v.to_string())
                                        .body(Full::new(HBytes::from(body))).unwrap()
                                }
                                None => crate::http_util::empty_response(hyper::StatusCode::NOT_FOUND),
                            }
                            _ => crate::http_util::empty_response(hyper::StatusCode::NOT_FOUND),
                        };
                        Ok::<_, Infallible>(resp)
                    }
                })).await;
            });
        }
    }
}

const MAX_GOSSIP_BODY_BYTES: usize = 1024 * 1024;

pub async fn run_gossip_loop(membership: Membership, seeds: Vec<String>) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("client");
    let mut interval = tokio::time::interval(Duration::from_millis(GOSSIP_INTERVAL_MS));
    let me_id = membership.me_id.clone();

    for seed in &seeds {
        if let Err(e) = gossip_with(&client, &membership, seed).await {
            tracing::warn!(seed, error=%e, "initial seed contact failed");
        } else {
            tracing::info!(seed, "joined via seed");
        }
    }

    loop {
        interval.tick().await;
        membership.sweep();
        let alive = membership.members_alive();
        let target = if alive.is_empty() {
            seeds.choose(&mut rand::thread_rng()).cloned()
        } else {
            alive
                .choose(&mut rand::thread_rng())
                .map(|n| n.gossip_url.clone())
        };
        if let Some(url) = target {
            if let Err(e) = gossip_with(&client, &membership, &url).await {
                tracing::debug!(target=%url, me=%me_id, error=%e, "gossip round failed");
            }
        }
    }
}

async fn gossip_with(client: &reqwest::Client, m: &Membership, peer_url: &str) -> Result<()> {
    let payload = GossipPayload {
        from: m.me_snapshot(),
        members: m.members_all(),
    };
    let url = format!("{}/cluster/sync", peer_url.trim_end_matches('/'));
    let resp = client.post(&url).json(&payload).send().await?;
    let status = resp.status();
    if !status.is_success() {
        return Err(BcError::Cluster(format!("sync HTTP {status}")));
    }
    let body = resp.bytes().await?;
    let reply: GossipPayload = serde_json::from_slice(&body)
        .map_err(|e| BcError::Cluster(format!("decode reply: {e}")))?;
    if reply.from.cluster_hash != m.me_template.cluster_hash {
        m.stats.config_mismatches.inc();
        return Err(BcError::Cluster(format!(
            "reply cluster_hash mismatch from {}",
            reply.from.id
        )));
    }
    let from_id = reply.from.id.clone();
    let mut all = reply.members;
    all.push(reply.from);
    m.merge(&all);
    m.touch_peer(&from_id);
    m.stats.gossip_rounds.inc();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::{IntCounter, Registry};
    use std::sync::Arc;

    fn make_stats() -> Arc<crate::stats::ClusterStats> {
        let r = Registry::new();
        let make = |name: &str| {
            let c = IntCounter::new(name, name).unwrap();
            r.register(Box::new(c.clone())).unwrap();
            c
        };
        Arc::new(crate::stats::ClusterStats {
            gossip_rounds: make("t_gossip_rounds"),
            joins: make("t_joins"),
            failures: make("t_failures"),
            config_mismatches: make("t_config_mismatches"),
        })
    }

    fn node(id: &str, hash: &str, state: NodeState) -> NodeInfo {
        NodeInfo {
            id: id.to_string(),
            transport_url: format!("http://{id}:7772"),
            gossip_url: format!("http://{id}:7771"),
            cluster_hash: hash.to_string(),
            ucx_worker_addr_b64: None,
            last_seen_unix: unix_now(),
            state,
            incarnation: 1,
            bloom_version: 0,
            admin_url: None,
        }
    }

    #[test]
    fn members_alive_same_cluster_filters_by_hash() {
        let me = node("me", "hash-a", NodeState::Alive);
        let m = Membership::new(me, make_stats());

        let peer_same = node("peer-same", "hash-a", NodeState::Alive);
        let peer_other = node("peer-other", "hash-b", NodeState::Alive);
        m.merge(&[peer_same, peer_other]);

        assert_eq!(
            m.members_alive().len(),
            1,
            "members_alive returns same-cluster only"
        );
        assert_eq!(
            m.members_alive_same_cluster().len(),
            1,
            "members_alive_same_cluster returns same-cluster only"
        );
        assert_eq!(m.members_alive_same_cluster()[0].id, "peer-same");
    }

    #[test]
    fn effective_admin_url_falls_back_to_port_substitution() {
        let n_no_admin = NodeInfo {
            admin_url: None,
            transport_url: "http://10.0.0.5:7772".into(),
            ..node("n1", "h", NodeState::Alive)
        };
        assert_eq!(n_no_admin.effective_admin_url(), "http://10.0.0.5:7773");

        let n_with_admin = NodeInfo {
            admin_url: Some("http://admin.example:9999".into()),
            transport_url: "http://10.0.0.5:7772".into(),
            ..node("n2", "h", NodeState::Alive)
        };
        assert_eq!(
            n_with_admin.effective_admin_url(),
            "http://admin.example:9999"
        );
    }
}
