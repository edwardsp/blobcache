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
    inner: Arc<RwLock<Inner>>,
    pub stats: Arc<crate::stats::ClusterStats>,
    #[cfg(feature = "ucx")]
    rdma_peer_update_hook: Option<Arc<dyn Fn(&NodeInfo) + Send + Sync>>,
}

struct Inner {
    members: HashMap<String, NodeInfo>,
}

impl Membership {
    pub fn new(me: NodeInfo, stats: Arc<crate::stats::ClusterStats>) -> Self {
        let mut m = HashMap::new();
        m.insert(me.id.clone(), me.clone());
        let me_incarnation = Arc::new(AtomicU64::new(me.incarnation.max(1)));
        Self {
            me_id: me.id.clone(),
            me_template: me,
            me_incarnation,
            inner: Arc::new(RwLock::new(Inner { members: m })),
            stats,
            #[cfg(feature = "ucx")]
            rdma_peer_update_hook: None,
        }
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
        me.last_seen_unix = unix_now();
        me.state = NodeState::Alive;
        me
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
                    let inc_better = n.incarnation > existing.incarnation;
                    let same_inc_more_severe = n.incarnation == existing.incarnation
                        && n.state.rank() > existing.state.rank();
                    let same_state_fresher = n.incarnation == existing.incarnation
                        && n.state == existing.state
                        && n.last_seen_unix > existing.last_seen_unix;
                    if inc_better || same_inc_more_severe || same_state_fresher {
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
                }
                n.state = new_state;
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
}

impl GossipServer {
    pub fn new(membership: Membership) -> Self {
        Self { membership }
    }

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
        loop {
            let (stream, _peer) = listener.accept().await?;
            let me = me.clone();
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let _ = http1::Builder::new().serve_connection(io, service_fn(move |req: Request<Incoming>| {
                    let me = me.clone();
                    async move {
                        let resp = match (req.method(), req.uri().path()) {
                            (&Method::POST, "/cluster/sync") => {
                                let body = req.collect().await.map(|b| b.to_bytes()).unwrap_or_default();
                                if body.len() > MAX_GOSSIP_BODY_BYTES {
                                    return Ok::<_, Infallible>(Response::builder().status(413)
                                        .body(Full::new(HBytes::from_static(b"too large"))).unwrap());
                                }
                                match serde_json::from_slice::<GossipPayload>(&body) {
                                    Ok(payload) => {
                                        let from_id = payload.from.id.clone();
                                        if payload.from.cluster_hash != me.me_template.cluster_hash {
                                            me.stats.config_mismatches.inc();
                                            tracing::warn!(peer=%from_id, "cluster_hash mismatch — refusing merge");
                                            return Ok::<_, Infallible>(Response::builder().status(409)
                                                .header("content-type", "application/json")
                                                .body(Full::new(HBytes::from_static(b"{\"error\":\"cluster_hash mismatch\"}"))).unwrap());
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
                                        Response::builder().status(200)
                                            .header("content-type", "application/json")
                                            .body(Full::new(HBytes::from(body))).unwrap()
                                    }
                                    Err(e) => Response::builder().status(400)
                                        .body(Full::new(HBytes::from(format!("bad: {e}")))).unwrap(),
                                }
                            }
                            _ => Response::builder().status(404).body(Full::new(HBytes::new())).unwrap(),
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
