// UCX-over-InfiniBand peer transport (v2 backend).
//
// This module intentionally uses raw ucx1-sys FFI directly instead of the
// async-ucx wrapper. The public API and wire protocol stay identical to the
// previous implementation, except the peer data-plane now uses the UCX tag
// API rather than the stream API.

#![cfg(feature = "ucx")]

// ucx1-sys' build script links libucp but not libucs; we use ucs_status_string
// directly, so request the linker to also pull in libucs.
#[link(name = "ucs")]
extern "C" {}

use bytes::Bytes;
use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::CStr;
use std::future::Future;
use std::mem;
use std::net::SocketAddr;
use std::os::fd::{AsRawFd, RawFd};
use std::os::raw::{c_char, c_void};
use std::pin::Pin;
use std::ptr;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll, Waker};
use std::time::Duration;
use tokio::io::unix::AsyncFd;
use tokio::sync::{mpsc, oneshot, Notify};
use tokio::task::JoinHandle;
use ucx1_sys::*;

// Per-thread Notify woken whenever a UCX op is posted that may need immediate
// worker progress to complete. Set once at runtime startup; tag_send/tag_recv/
// tag_msg_recv/close_ep_* call `kick_progress()` after posting a deferred op.
thread_local! {
    static PROGRESS_KICK: RefCell<Option<Rc<Notify>>> = const { RefCell::new(None) };
}

fn install_progress_kick(kick: Rc<Notify>) {
    PROGRESS_KICK.with(|cell| {
        *cell.borrow_mut() = Some(kick);
    });
}

fn kick_progress() {
    PROGRESS_KICK.with(|cell| {
        if let Some(k) = cell.borrow().as_ref() {
            k.notify_one();
        }
    });
}

const PROGRESS_BUDGET: usize = 64;
const INBOUND_DRAIN_BUDGET: usize = 32;

use crate::cache::{ChunkKey, DiskCache};
use crate::error::{BcError, Result};

const MAGIC: u32 = 0xBC10_C001;
const STATUS_OK: u32 = 0;
const STATUS_MISS: u32 = 1;
const STATUS_ERR: u32 = 2;
const MAX_RESPONSE_BYTES: u32 = 64 * 1024 * 1024;
const TAG_CLASS_MASK: u64 = 0xFFFF_0000_0000_0000;
const TAG_ID_MASK: u64 = 0x0000_FFFF_FFFF_FFFF;
const TAG_REQ_BASE: u64 = 0xBC01_0000_0000_0000;
const TAG_RESP_BASE: u64 = 0xBC02_0000_0000_0000;
static REQ_ID: AtomicU64 = AtomicU64::new(1);

type SharedRuntimeState = Rc<RefCell<RuntimeState>>;

struct RecvSlab {
    backing: Box<[u8]>,
    slot_size: usize,
    n_slots: usize,
    memh: ucp_mem_h,
    context: ucp_context_h,
    free: RefCell<Vec<usize>>,
    permits: Arc<tokio::sync::Semaphore>,
}

unsafe impl Send for RecvSlab {}
unsafe impl Sync for RecvSlab {}

impl RecvSlab {
    fn new(context: ucp_context_h, n_slots: usize, slot_size: usize) -> Result<Rc<Self>> {
        let total = n_slots
            .checked_mul(slot_size)
            .ok_or_else(|| BcError::Peer("RecvSlab size overflow".into()))?;
        let backing: Box<[u8]> = vec![0u8; total].into_boxed_slice();

        let mut params: ucp_mem_map_params_t = unsafe { mem::zeroed() };
        params.field_mask = (ucp_mem_map_params_field::UCP_MEM_MAP_PARAM_FIELD_ADDRESS.0 as u64)
            | (ucp_mem_map_params_field::UCP_MEM_MAP_PARAM_FIELD_LENGTH.0 as u64)
            | (ucp_mem_map_params_field::UCP_MEM_MAP_PARAM_FIELD_FLAGS.0 as u64);
        params.address = backing.as_ptr() as *mut c_void;
        params.length = total as _;
        params.flags = UCP_MEM_MAP_NONBLOCK as u32;

        let mut memh: ucp_mem_h = ptr::null_mut();
        let status = unsafe { ucp_mem_map(context, &params, &mut memh) };
        check_status("ucp_mem_map", status)?;

        let mut advise: ucp_mem_advise_params_t = unsafe { mem::zeroed() };
        advise.field_mask = (ucp_mem_advise_params_field::UCP_MEM_ADVISE_PARAM_FIELD_ADDRESS.0
            as u64)
            | (ucp_mem_advise_params_field::UCP_MEM_ADVISE_PARAM_FIELD_LENGTH.0 as u64)
            | (ucp_mem_advise_params_field::UCP_MEM_ADVISE_PARAM_FIELD_ADVICE.0 as u64);
        advise.address = backing.as_ptr() as *mut c_void;
        advise.length = total as _;
        advise.advice = ucp_mem_advice::UCP_MADV_WILLNEED;
        let advise_status = unsafe { ucp_mem_advise(context, memh, &mut advise) };
        if let Err(e) = check_status("ucp_mem_advise(WILLNEED)", advise_status) {
            tracing::warn!(error = %e, "ucp_mem_advise(WILLNEED) failed; continuing");
        }

        let free = RefCell::new((0..n_slots).rev().collect());
        let permits = Arc::new(tokio::sync::Semaphore::new(n_slots));

        tracing::info!(
            n_slots,
            slot_size,
            total_bytes = total,
            "RecvSlab registered with UCX (NONBLOCK + WILLNEED)"
        );

        Ok(Rc::new(Self {
            backing,
            slot_size,
            n_slots,
            memh,
            context,
            free,
            permits,
        }))
    }

    async fn checkout(self: &Rc<Self>) -> Result<SlabSlot> {
        let permit = self
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| BcError::Peer("RecvSlab semaphore closed".into()))?;
        let idx =
            self.free.borrow_mut().pop().ok_or_else(|| {
                BcError::Peer("RecvSlab free list empty (semaphore desync)".into())
            })?;
        Ok(SlabSlot {
            slab: self.clone(),
            idx,
            _permit: permit,
        })
    }
}

impl Drop for RecvSlab {
    fn drop(&mut self) {
        unsafe {
            ucp_mem_unmap(self.context, self.memh);
        }
    }
}

struct SlabSlot {
    slab: Rc<RecvSlab>,
    idx: usize,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl SlabSlot {
    fn as_mut_slice(&mut self, len: usize) -> &mut [u8] {
        debug_assert!(len <= self.slab.slot_size);
        let start = self.idx * self.slab.slot_size;
        let backing_ptr = self.slab.backing.as_ptr() as *mut u8;
        unsafe { std::slice::from_raw_parts_mut(backing_ptr.add(start), len) }
    }
}

impl Drop for SlabSlot {
    fn drop(&mut self) {
        self.slab.free.borrow_mut().push(self.idx);
    }
}

pub struct RdmaPeerService {
    _shared: Arc<RdmaRuntimeShared>,
    // Set after Fetcher is built (chicken-and-egg: Fetcher needs PeerClient,
    // PeerClient needs RdmaPeerService running so worker_addr_blob is in
    // gossip). The runtime thread reads this via Arc clone on each inbound
    // request that carries wait_ms>0.
    chunk_provider_slot: Arc<OnceLock<crate::transport::ChunkProvider>>,
}

impl RdmaPeerService {
    pub fn set_chunk_provider(&self, provider: crate::transport::ChunkProvider) {
        let _ = self.chunk_provider_slot.set(provider);
    }
}

#[derive(Clone)]
pub struct RdmaPeerClient {
    _shared: Arc<RdmaRuntimeShared>,
    cmd_tx: mpsc::UnboundedSender<RdmaCmd>,
}

struct RdmaRuntimeShared {
    shutdown: Mutex<Option<oneshot::Sender<()>>>,
    thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for RdmaRuntimeShared {
    fn drop(&mut self) {
        if let Ok(mut shutdown) = self.shutdown.lock() {
            if let Some(tx) = shutdown.take() {
                let _ = tx.send(());
            }
        }
        if let Ok(mut thread) = self.thread.lock() {
            if let Some(handle) = thread.take() {
                let _ = handle.join();
            }
        }
    }
}

enum RdmaCmd {
    Fetch {
        peer_id: String,
        peer_addr_blob: Vec<u8>,
        key: ChunkKey,
        length: u32,
        wait_ms: u32,
        request_id: String,
        reply: oneshot::Sender<Result<Bytes>>,
    },
    Health {
        peer_id: String,
        peer_addr_blob: Vec<u8>,
        reply: oneshot::Sender<Result<()>>,
    },
    UpdatePeer {
        peer_id: String,
        peer_addr_blob: Vec<u8>,
    },
}

impl RdmaPeerService {
    pub fn start(
        cache: Arc<DiskCache>,
        addr: SocketAddr,
        stats: Arc<crate::stats::PeerStats>,
        local_peer_id: String,
        slab_slot_size: usize,
        slab_n_slots: usize,
    ) -> Result<(Self, RdmaPeerClient, Vec<u8>)> {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<RdmaCmd>();
        let (started_tx, started_rx) = std::sync::mpsc::sync_channel::<Result<Vec<u8>>>(1);
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        let chunk_provider_slot: Arc<OnceLock<crate::transport::ChunkProvider>> =
            Arc::new(OnceLock::new());
        let cps_for_thread = chunk_provider_slot.clone();

        let thread = std::thread::Builder::new()
            .name("ucx-runtime".into())
            .spawn(move || {
                let rt = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(rt) => rt,
                    Err(e) => {
                        let _ = started_tx.send(Err(BcError::Peer(format!("rt: {e}"))));
                        return;
                    }
                };
                let local = tokio::task::LocalSet::new();
                local.block_on(&rt, async move {
                    if let Err(e) = run_runtime(
                        cache,
                        stats,
                        local_peer_id,
                        cmd_rx,
                        started_tx,
                        shutdown_rx,
                        slab_slot_size,
                        slab_n_slots,
                        cps_for_thread,
                    )
                    .await
                    {
                        tracing::error!(error = %e, "ucx runtime thread exited with error");
                    }
                });
            })
            .map_err(|e| BcError::Peer(format!("ucx-runtime thread: {e}")))?;

        let worker_addr_blob = started_rx
            .recv()
            .map_err(|_| BcError::Peer("ucx runtime startup channel closed".into()))??;

        let shared = Arc::new(RdmaRuntimeShared {
            shutdown: Mutex::new(Some(shutdown_tx)),
            thread: Mutex::new(Some(thread)),
        });
        let client = RdmaPeerClient {
            _shared: shared.clone(),
            cmd_tx,
        };

        tracing::info!(
            %addr,
            worker_addr_len = worker_addr_blob.len(),
            "ucx peer transport ready via worker-address wireup"
        );

        Ok((
            Self {
                _shared: shared,
                chunk_provider_slot,
            },
            client,
            worker_addr_blob,
        ))
    }
}

impl RdmaPeerClient {
    pub fn update_peer(&self, peer_id: &str, peer_addr_blob: &[u8]) -> Result<()> {
        self.cmd_tx
            .send(RdmaCmd::UpdatePeer {
                peer_id: peer_id.to_string(),
                peer_addr_blob: peer_addr_blob.to_vec(),
            })
            .map_err(|_| BcError::Peer("ucx runtime thread gone".into()))
    }

    pub async fn fetch_chunk(
        &self,
        peer_id: &str,
        peer_addr_blob: &[u8],
        key: &ChunkKey,
        length: u32,
        wait_ms: u32,
        rid: Option<&crate::request_id::RequestId>,
    ) -> Result<Bytes> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(RdmaCmd::Fetch {
                peer_id: peer_id.to_string(),
                peer_addr_blob: peer_addr_blob.to_vec(),
                key: key.clone(),
                length,
                wait_ms,
                request_id: rid.map(|r| r.as_str().to_string()).unwrap_or_default(),
                reply: tx,
            })
            .map_err(|_| BcError::Peer("ucx runtime thread gone".into()))?;
        rx.await
            .map_err(|_| BcError::Peer("ucx fetch reply dropped".into()))?
    }

    pub async fn health(&self, peer_id: &str, peer_addr_blob: &[u8]) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(RdmaCmd::Health {
                peer_id: peer_id.to_string(),
                peer_addr_blob: peer_addr_blob.to_vec(),
                reply: tx,
            })
            .map_err(|_| BcError::Peer("ucx runtime thread gone".into()))?;
        rx.await
            .map_err(|_| BcError::Peer("ucx health reply dropped".into()))?
    }
}

async fn run_runtime(
    cache: Arc<DiskCache>,
    stats: Arc<crate::stats::PeerStats>,
    local_peer_id: String,
    mut cmd_rx: mpsc::UnboundedReceiver<RdmaCmd>,
    started_tx: std::sync::mpsc::SyncSender<Result<Vec<u8>>>,
    mut shutdown_rx: oneshot::Receiver<()>,
    slab_slot_size: usize,
    slab_n_slots: usize,
    chunk_provider_slot: Arc<OnceLock<crate::transport::ChunkProvider>>,
) -> Result<()> {
    let ucx = match UcxRuntime::new() {
        Ok(ucx) => ucx,
        Err(e) => {
            let msg = e.to_string();
            let _ = started_tx.send(Err(BcError::Peer(msg.clone())));
            return Err(e);
        }
    };

    let recv_slab = match RecvSlab::new(ucx.context, slab_n_slots, slab_slot_size) {
        Ok(s) => s,
        Err(e) => {
            let msg = e.to_string();
            let _ = started_tx.send(Err(BcError::Peer(msg.clone())));
            return Err(e);
        }
    };

    let mut worker_addr: *mut ucp_address_t = ptr::null_mut();
    let mut worker_addr_len: usize = 0;
    let worker_addr_status =
        unsafe { ucp_worker_get_address(ucx.worker, &mut worker_addr, &mut worker_addr_len) };
    if let Err(e) = check_status("ucp_worker_get_address", worker_addr_status) {
        let msg = e.to_string();
        let _ = started_tx.send(Err(BcError::Peer(msg.clone())));
        return Err(e);
    }

    let worker_addr_blob =
        unsafe { std::slice::from_raw_parts(worker_addr.cast::<u8>(), worker_addr_len).to_vec() };
    let state = Rc::new(RefCell::new(RuntimeState::new(
        ucx.worker,
        local_peer_id,
        recv_slab,
    )));

    let inbound_ready = Rc::new(Notify::new());
    let progress_kick = Rc::new(Notify::new());
    install_progress_kick(progress_kick.clone());

    let progress = spawn_progress_task(
        ucx.worker,
        ucx.async_fd.clone(),
        progress_kick.clone(),
        inbound_ready.clone(),
    );

    if started_tx.send(Ok(worker_addr_blob.clone())).is_err() {
        progress.abort();
        let _ = progress.await;
        unsafe {
            ucp_worker_release_address(ucx.worker, worker_addr);
        }
        return Err(BcError::Peer("ucx runtime startup receiver dropped".into()));
    }

    let mut safety_tick = tokio::time::interval(Duration::from_millis(100));
    safety_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let result = loop {
        tokio::select! {
            _ = &mut shutdown_rx => {
                tracing::info!("ucx runtime shutting down");
                break Ok(());
            }
            maybe_cmd = cmd_rx.recv() => {
                let Some(cmd) = maybe_cmd else {
                    break Ok(());
                };
                dispatch_cmd(state.clone(), cmd);
            }
            _ = inbound_ready.notified() => {
                drain_inbound(state.clone(), cache.clone(), stats.clone(), chunk_provider_slot.clone());
            }
            _ = safety_tick.tick() => {
                if let Err(e) = reap_broken_eps(state.clone()).await {
                    break Err(e);
                }
            }
        }
    };

    progress.abort();
    let _ = progress.await;
    let shutdown_result = close_all_endpoints(state.clone()).await;
    unsafe {
        ucp_worker_release_address(ucx.worker, worker_addr);
    }
    result.and(shutdown_result)
}

fn dispatch_cmd(state: SharedRuntimeState, cmd: RdmaCmd) {
    tokio::task::spawn_local(async move {
        match cmd {
            RdmaCmd::Fetch {
                peer_id,
                peer_addr_blob,
                key,
                length,
                wait_ms,
                request_id,
                reply,
            } => {
                let result = client_fetch(
                    state.clone(),
                    &peer_id,
                    &peer_addr_blob,
                    &key,
                    length,
                    wait_ms,
                    &request_id,
                )
                .await;
                let _ = reply.send(result);
            }
            RdmaCmd::Health {
                peer_id,
                peer_addr_blob,
                reply,
            } => {
                let result = client_health(state.clone(), &peer_id, &peer_addr_blob).await;
                let _ = reply.send(result);
            }
            RdmaCmd::UpdatePeer {
                peer_id,
                peer_addr_blob,
            } => {
                if let Err(e) = update_peer(state.clone(), &peer_id, &peer_addr_blob).await {
                    tracing::warn!(peer = %peer_id, error = %e, "failed to update UCX peer address");
                }
            }
        }
    });
}

async fn update_peer(
    state: SharedRuntimeState,
    peer_id: &str,
    peer_addr_blob: &[u8],
) -> Result<()> {
    let removed = state
        .borrow_mut()
        .update_peer_addr_blob(peer_id, peer_addr_blob);
    if let Some(removed) = removed {
        close_removed_endpoint(removed).await;
    }
    Ok(())
}

fn drain_inbound(
    state: SharedRuntimeState,
    cache: Arc<DiskCache>,
    stats: Arc<crate::stats::PeerStats>,
    chunk_provider_slot: Arc<OnceLock<crate::transport::ChunkProvider>>,
) {
    let worker = state.borrow().worker;
    let mut drained = 0usize;
    loop {
        let mut info: ucp_tag_recv_info_t = unsafe { mem::zeroed() };
        let msg = unsafe { ucp_tag_probe_nb(worker, TAG_REQ_BASE, TAG_CLASS_MASK, 1, &mut info) };
        if msg.is_null() {
            break;
        }

        stats.chunk_requests.inc();
        let sender_tag = info.sender_tag;
        let recv_len = info.length as usize;
        let state_c = state.clone();
        let cache_c = cache.clone();
        let stats_c = stats.clone();
        let cps_c = chunk_provider_slot.clone();

        tokio::task::spawn_local(async move {
            if let Err(e) =
                handle_inbound_request(state_c, cache_c, stats_c, msg, recv_len, sender_tag, cps_c)
                    .await
            {
                tracing::warn!(error = %e, "inbound UCX request handler failed");
            }
        });

        drained += 1;
        if drained >= INBOUND_DRAIN_BUDGET {
            break;
        }
    }
}

async fn handle_inbound_request(
    state: SharedRuntimeState,
    cache: Arc<DiskCache>,
    stats: Arc<crate::stats::PeerStats>,
    msg: ucp_tag_message_h,
    recv_len: usize,
    sender_tag: u64,
    chunk_provider_slot: Arc<OnceLock<crate::transport::ChunkProvider>>,
) -> Result<()> {
    let t_total = std::time::Instant::now();
    let worker = state.borrow().worker;
    let mut buf = vec![0u8; recv_len];
    let actual = tag_msg_recv(worker, &mut buf, msg).await?;
    buf.truncate(actual);

    let request = decode_request(&buf)?;
    if !request.request_id.is_empty() {
        tracing::debug!(
            rid = %request.request_id,
            peer = %request.requester_peer_id,
            mount = %request.key.mount,
            offset = request.key.offset,
            length = request.length,
            "ucx peer request received"
        );
    }
    let resp_tag = TAG_RESP_BASE | (sender_tag & TAG_ID_MASK);

    if request.length == 0 || request.length > MAX_RESPONSE_BYTES {
        tracing::warn!(
            peer = %request.requester_peer_id,
            length = request.length,
            "dropping UCX tag request with invalid length"
        );
        return Ok(());
    }

    let cache2 = cache.clone();
    let key2 = request.key.clone();
    let req_len = request.length as usize;
    let t_cg = std::time::Instant::now();
    let response = match tokio::task::spawn_blocking(move || {
        let size = cache2.entry_size(&key2)?;
        let size_us = size as usize;
        if size_us > req_len {
            return None;
        }
        let total = 8 + size_us;
        let mut buf: Vec<u8> = Vec::with_capacity(total);
        buf.extend_from_slice(&STATUS_OK.to_le_bytes());
        buf.extend_from_slice(&(size_us as u32).to_le_bytes());
        let payload_slice =
            unsafe { std::slice::from_raw_parts_mut(buf.as_mut_ptr().add(8), size_us) };
        cache2.try_get_into_slice(&key2, payload_slice)?;
        unsafe {
            buf.set_len(total);
        }
        Some((buf, size_us))
    })
    .await
    {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(error = %e, "cache try_get_into blocking task failed");
            let response = encode_response(STATUS_ERR, &[])?;
            let _ =
                server_send_response(state, &request.requester_peer_id, resp_tag, &response).await;
            return Ok(());
        }
    };
    stats
        .server_cache_get_seconds
        .observe(t_cg.elapsed().as_secs_f64());

    let response = match response {
        Some((buf, served)) => {
            stats.chunk_bytes_served.inc_by(served as u64);
            buf
        }
        None => {
            // v2.6.0 stampede-leader path: on cache miss, if the requester
            // asked us to wait (wait_ms>0) and a ChunkProvider is wired,
            // call into Fetcher::serve_peer_chunk which will subscribe to an
            // existing singleflight or become leader and fetch from blob.
            // Avoids the cold-start herd where 8 nodes hit the same cold
            // chunk and all 8 call into Azure.
            let provider_resp = if request.wait_ms > 0 && request.length > 0 {
                if let Some(provider) = chunk_provider_slot.get() {
                    let len_u64 = request.length as u64;
                    let key_for_p = request.key.clone();
                    let wait = request.wait_ms;
                    provider(key_for_p, len_u64, wait).await
                } else {
                    None
                }
            } else {
                None
            };
            match provider_resp {
                Some(b) => {
                    let served = b.len();
                    if served > req_len {
                        encode_response(STATUS_MISS, &[])?
                    } else {
                        let total = 8 + served;
                        let mut buf: Vec<u8> = Vec::with_capacity(total);
                        buf.extend_from_slice(&STATUS_OK.to_le_bytes());
                        buf.extend_from_slice(&(served as u32).to_le_bytes());
                        buf.extend_from_slice(&b);
                        stats.chunk_bytes_served.inc_by(served as u64);
                        buf
                    }
                }
                None => encode_response(STATUS_MISS, &[])?,
            }
        }
    };

    let t_send = std::time::Instant::now();
    let send_res =
        server_send_response(state, &request.requester_peer_id, resp_tag, &response).await;
    stats
        .server_send_seconds
        .observe(t_send.elapsed().as_secs_f64());
    if let Err(e) = send_res {
        tracing::warn!(
            peer = %request.requester_peer_id,
            error = %e,
            "failed to send UCX tag response"
        );
    }
    stats
        .server_handler_seconds
        .observe(t_total.elapsed().as_secs_f64());
    Ok(())
}

async fn reap_broken_eps(state: SharedRuntimeState) -> Result<()> {
    let stale = state.borrow_mut().take_broken_peer_eps();
    for stale_ep in stale {
        close_removed_endpoint(stale_ep).await;
    }
    Ok(())
}

async fn close_all_endpoints(state: SharedRuntimeState) -> Result<()> {
    let all = state.borrow_mut().take_all_peer_eps();
    for slot in all {
        close_removed_endpoint(slot).await;
    }
    Ok(())
}

async fn close_removed_endpoint(slot: RemovedEndpoint) {
    let _ = if slot.broken.load(Ordering::SeqCst) {
        close_ep_force(slot.ep).await
    } else {
        close_ep(slot.ep).await
    };
    release_callback_arg(slot.callback_arg);
}

struct ChunkRequest {
    key: ChunkKey,
    length: u32,
    requester_peer_id: String,
    // v2.6.0: stampede-leader hint. 0 means "normal fetch, just check cache";
    // >0 invites the server to subscribe to its own singleflight (or become
    // leader and fetch from blob) for up to `wait_ms` ms before replying MISS.
    wait_ms: u32,
    // Tier 2 observability: optional request-id propagated from the originating
    // FUSE read or hydrate handler. Empty means caller did not supply one
    // (older clients, prefetch, etc). Validated against the same alphanumeric+dash
    // rules as the TCP `x-blobcache-rid` header at decode time.
    request_id: String,
}

struct RuntimeState {
    worker: ucp_worker_h,
    local_peer_id: String,
    peer_eps: HashMap<String, EndpointSlot>,
    peer_addr_blobs: HashMap<String, Vec<u8>>,
    lane_verified: bool,
    recv_slab: Rc<RecvSlab>,
}

struct EndpointSlot {
    ep: ucp_ep_h,
    broken: Arc<AtomicBool>,
    callback_arg: *mut Arc<AtomicBool>,
    peer_addr_blob: Vec<u8>,
    in_flight: usize,
}

struct CheckedOutEndpoint {
    peer_id: String,
    ep: ucp_ep_h,
}

struct RemovedEndpoint {
    ep: ucp_ep_h,
    broken: Arc<AtomicBool>,
    callback_arg: *mut Arc<AtomicBool>,
}

impl RemovedEndpoint {
    fn from_slot(slot: EndpointSlot) -> Self {
        Self {
            ep: slot.ep,
            broken: slot.broken,
            callback_arg: slot.callback_arg,
        }
    }
}

impl RuntimeState {
    fn new(worker: ucp_worker_h, local_peer_id: String, recv_slab: Rc<RecvSlab>) -> Self {
        Self {
            worker,
            local_peer_id,
            peer_eps: HashMap::new(),
            peer_addr_blobs: HashMap::new(),
            lane_verified: false,
            recv_slab,
        }
    }

    fn update_peer_addr_blob(
        &mut self,
        peer_id: &str,
        peer_addr_blob: &[u8],
    ) -> Option<RemovedEndpoint> {
        self.peer_addr_blobs
            .insert(peer_id.to_string(), peer_addr_blob.to_vec());

        let should_remove = self
            .peer_eps
            .get(peer_id)
            .map(|slot| slot.peer_addr_blob != peer_addr_blob && slot.in_flight == 0)
            .unwrap_or(false);

        if should_remove {
            return self
                .peer_eps
                .remove(peer_id)
                .map(RemovedEndpoint::from_slot);
        }

        if let Some(slot) = self.peer_eps.get_mut(peer_id) {
            if slot.peer_addr_blob != peer_addr_blob {
                slot.broken.store(true, Ordering::SeqCst);
            }
        }

        None
    }

    fn checkout_endpoint(
        &mut self,
        peer_id: &str,
        peer_addr_blob: &[u8],
    ) -> Result<CheckedOutEndpoint> {
        let mut verify_ep = None;
        let ep = {
            let slot = match self.peer_eps.get_mut(peer_id) {
                Some(slot) => {
                    if slot.broken.load(Ordering::SeqCst) {
                        return Err(BcError::Peer(format!(
                            "UCX endpoint is broken for peer {peer_id}"
                        )));
                    }
                    if slot.peer_addr_blob != peer_addr_blob {
                        return Err(BcError::Peer(format!(
                            "UCX endpoint address update still in flight for peer {peer_id}"
                        )));
                    }
                    slot
                }
                None => {
                    let slot = create_peer_ep(self.worker, peer_addr_blob)?;
                    verify_ep = Some(slot.ep);
                    self.peer_eps.insert(peer_id.to_string(), slot);
                    self.peer_eps
                        .get_mut(peer_id)
                        .expect("inserted endpoint slot missing")
                }
            };

            slot.in_flight += 1;
            slot.ep
        };

        if let Some(ep) = verify_ep {
            self.verify_lane_once(ep);
        }

        Ok(CheckedOutEndpoint {
            peer_id: peer_id.to_string(),
            ep,
        })
    }

    fn release_endpoint(&mut self, checked_out: CheckedOutEndpoint) -> Option<RemovedEndpoint> {
        let slot = self.peer_eps.get_mut(&checked_out.peer_id)?;
        if slot.in_flight > 0 {
            slot.in_flight -= 1;
        }
        if slot.in_flight == 0 && slot.broken.load(Ordering::SeqCst) {
            let slot = self.peer_eps.remove(&checked_out.peer_id)?;
            return Some(RemovedEndpoint::from_slot(slot));
        }
        None
    }

    fn mark_peer_ep_broken(&mut self, peer_id: &str) {
        if let Some(slot) = self.peer_eps.get_mut(peer_id) {
            slot.broken.store(true, Ordering::SeqCst);
        }
    }

    fn take_broken_peer_eps(&mut self) -> Vec<RemovedEndpoint> {
        let broken_ids: Vec<String> = self
            .peer_eps
            .iter()
            .filter_map(|(peer_id, slot)| {
                (slot.in_flight == 0 && slot.broken.load(Ordering::SeqCst)).then(|| peer_id.clone())
            })
            .collect();

        let mut removed = Vec::with_capacity(broken_ids.len());
        for peer_id in broken_ids {
            if let Some(slot) = self.peer_eps.remove(&peer_id) {
                removed.push(RemovedEndpoint::from_slot(slot));
            }
        }
        removed
    }

    fn take_all_peer_eps(&mut self) -> Vec<RemovedEndpoint> {
        std::mem::take(&mut self.peer_eps)
            .into_values()
            .map(RemovedEndpoint::from_slot)
            .collect()
    }

    fn verify_lane_once(&mut self, _ep: ucp_ep_h) {
        if self.lane_verified {
            return;
        }
        self.lane_verified = true;
        tracing::info!("ucx lane verification relies on UCX_LOG_LEVEL=info pod logs on UCX 1.13.1");
    }
}

async fn client_fetch(
    state: SharedRuntimeState,
    peer_id: &str,
    peer_addr_blob: &[u8],
    key: &ChunkKey,
    length: u32,
    wait_ms: u32,
    request_id: &str,
) -> Result<Bytes> {
    if length == 0 || length > MAX_RESPONSE_BYTES {
        return Err(BcError::Peer(format!("bad chunk length {length}")));
    }

    let (checked_out, removed_before, worker, requester_peer_id, slab) = {
        let mut runtime = state.borrow_mut();
        let removed_before = runtime.update_peer_addr_blob(peer_id, peer_addr_blob);
        let checked_out = runtime.checkout_endpoint(peer_id, peer_addr_blob)?;
        (
            checked_out,
            removed_before,
            runtime.worker,
            runtime.local_peer_id.clone(),
            runtime.recv_slab.clone(),
        )
    };
    if let Some(removed) = removed_before {
        close_removed_endpoint(removed).await;
    }

    let result = client_fetch_inner(
        worker,
        checked_out.ep,
        key,
        length,
        wait_ms,
        &requester_peer_id,
        request_id,
        &slab,
    )
    .await;
    if should_mark_endpoint_broken(&result) {
        state.borrow_mut().mark_peer_ep_broken(peer_id);
    }
    let removed_after = state.borrow_mut().release_endpoint(checked_out);
    if let Some(removed) = removed_after {
        close_removed_endpoint(removed).await;
    }
    result
}

async fn client_fetch_inner(
    worker: ucp_worker_h,
    ep: ucp_ep_h,
    key: &ChunkKey,
    length: u32,
    wait_ms: u32,
    requester_peer_id: &str,
    request_id: &str,
    slab: &Rc<RecvSlab>,
) -> Result<Bytes> {
    let id = REQ_ID.fetch_add(1, Ordering::Relaxed);
    let req_tag = TAG_REQ_BASE | id;
    let resp_tag = TAG_RESP_BASE | id;

    // v2.3.3: receive into a pre-registered slab slot so UCX's reg-cache
    // hits immediately and the rendezvous path goes RDMA without a per-
    // request memory registration. The slot itself is uninitialized
    // (Box<[u8]> backing via MaybeUninit); only [..resp_len] is read.
    let total = 8 + length as usize;
    let mut slot = slab.checkout().await?;
    let req = encode_request(key, length, requester_peer_id, wait_ms, request_id)?;
    // Post recv FIRST (so server's reply isn't dropped if it races our send),
    // post send, then await both concurrently. Sequential .await on recv would
    // deadlock: server can't reply until we send the request.
    let resp_len = {
        let recv_slice = slot.as_mut_slice(total);
        let recv_fut = tag_recv(worker, recv_slice, resp_tag);
        let send_fut = tag_send(ep, &req, req_tag);
        let (send_res, recv_res) = tokio::join!(send_fut, recv_fut);
        send_res?;
        recv_res?
    };
    decode_response(&slot.as_mut_slice(total)[..resp_len], length)
}

async fn client_health(
    state: SharedRuntimeState,
    peer_id: &str,
    peer_addr_blob: &[u8],
) -> Result<()> {
    let (checked_out, removed_before) = {
        let mut runtime = state.borrow_mut();
        let removed_before = runtime.update_peer_addr_blob(peer_id, peer_addr_blob);
        let checked_out = runtime.checkout_endpoint(peer_id, peer_addr_blob)?;
        (checked_out, removed_before)
    };
    if let Some(removed) = removed_before {
        close_removed_endpoint(removed).await;
    }

    let removed_after = state.borrow_mut().release_endpoint(checked_out);
    if let Some(removed) = removed_after {
        close_removed_endpoint(removed).await;
    }
    Ok(())
}

async fn server_send_response(
    state: SharedRuntimeState,
    peer_id: &str,
    tag: u64,
    response: &[u8],
) -> Result<()> {
    let peer_addr_blob = match state.borrow().peer_addr_blobs.get(peer_id) {
        Some(blob) => blob.clone(),
        None => {
            tracing::warn!(peer = %peer_id, "missing UCX worker address for reply; dropping request");
            return Ok(());
        }
    };

    let (checked_out, removed_before) = {
        let mut runtime = state.borrow_mut();
        let removed_before = runtime.update_peer_addr_blob(peer_id, &peer_addr_blob);
        let checked_out = runtime.checkout_endpoint(peer_id, &peer_addr_blob)?;
        (checked_out, removed_before)
    };
    if let Some(removed) = removed_before {
        close_removed_endpoint(removed).await;
    }

    let result = tag_send(checked_out.ep, response, tag).await;
    if result.is_err() {
        state.borrow_mut().mark_peer_ep_broken(peer_id);
    }
    let removed_after = state.borrow_mut().release_endpoint(checked_out);
    if let Some(removed) = removed_after {
        close_removed_endpoint(removed).await;
    }
    result
}

fn should_mark_endpoint_broken<T>(result: &Result<T>) -> bool {
    match result {
        Ok(_) => false,
        Err(BcError::NotFound(_)) => false,
        Err(_) => true,
    }
}

fn create_peer_ep(worker: ucp_worker_h, peer_addr_blob: &[u8]) -> Result<EndpointSlot> {
    let mut ep: ucp_ep_h = ptr::null_mut();
    let broken = Arc::new(AtomicBool::new(false));
    let callback_arg = Box::into_raw(Box::new(broken.clone()));

    let mut ep_params: ucp_ep_params_t = unsafe { mem::zeroed() };
    ep_params.field_mask = (ucp_ep_params_field::UCP_EP_PARAM_FIELD_REMOTE_ADDRESS.0 as u64)
        | (ucp_ep_params_field::UCP_EP_PARAM_FIELD_ERR_HANDLER.0 as u64)
        | (ucp_ep_params_field::UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE.0 as u64);
    ep_params.address = peer_addr_blob.as_ptr().cast::<ucp_address_t>();
    ep_params.err_mode = ucp_err_handling_mode_t::UCP_ERR_HANDLING_MODE_PEER;
    ep_params.err_handler.cb = Some(endpoint_error_cb);
    ep_params.err_handler.arg = callback_arg.cast::<c_void>();

    let status = unsafe { ucp_ep_create(worker, &ep_params, &mut ep) };
    if let Err(e) = check_status("ucp_ep_create(remote_address)", status) {
        release_callback_arg(callback_arg);
        return Err(e);
    }

    Ok(EndpointSlot {
        ep,
        broken,
        callback_arg,
        peer_addr_blob: peer_addr_blob.to_vec(),
        in_flight: 0,
    })
}

fn release_callback_arg(callback_arg: *mut Arc<AtomicBool>) {
    if !callback_arg.is_null() {
        unsafe {
            drop(Box::from_raw(callback_arg));
        }
    }
}

fn encode_request(
    key: &ChunkKey,
    length: u32,
    requester_peer_id: &str,
    wait_ms: u32,
    request_id: &str,
) -> Result<Vec<u8>> {
    let mount = key.mount.as_bytes();
    let blob = key.blob.as_bytes();
    let requester = requester_peer_id.as_bytes();
    let rid = request_id.as_bytes();
    if mount.len() > u8::MAX as usize {
        return Err(BcError::Peer("mount name too long".into()));
    }
    if blob.len() > u16::MAX as usize {
        return Err(BcError::Peer("blob name too long".into()));
    }
    if requester.len() > u16::MAX as usize {
        return Err(BcError::Peer("requester peer id too long".into()));
    }
    if rid.len() > 128 {
        return Err(BcError::Peer("request id too long".into()));
    }

    let mut req = Vec::with_capacity(
        4 + 1 + mount.len() + 2 + blob.len() + 8 + 4 + 2 + requester.len() + 4 + 2 + rid.len(),
    );
    req.extend_from_slice(&MAGIC.to_be_bytes());
    req.push(mount.len() as u8);
    req.extend_from_slice(mount);
    req.extend_from_slice(&(blob.len() as u16).to_be_bytes());
    req.extend_from_slice(blob);
    req.extend_from_slice(&key.offset.to_be_bytes());
    req.extend_from_slice(&length.to_be_bytes());
    req.extend_from_slice(&(requester.len() as u16).to_be_bytes());
    req.extend_from_slice(requester);
    req.extend_from_slice(&wait_ms.to_be_bytes());
    req.extend_from_slice(&(rid.len() as u16).to_be_bytes());
    req.extend_from_slice(rid);
    Ok(req)
}

fn decode_request(data: &[u8]) -> Result<ChunkRequest> {
    let mut idx = 0;

    let magic = read_be_u32(data, &mut idx)?;
    if magic != MAGIC {
        return Err(BcError::Peer(format!("bad magic 0x{magic:08x}")));
    }

    let mount_len = read_u8(data, &mut idx)? as usize;
    let mount = read_utf8(data, &mut idx, mount_len, "mount")?;

    let blob_len = read_be_u16(data, &mut idx)? as usize;
    let blob = read_utf8(data, &mut idx, blob_len, "blob")?;

    let offset = read_be_u64(data, &mut idx)?;
    let length = read_be_u32(data, &mut idx)?;

    let requester_len = read_be_u16(data, &mut idx)? as usize;
    let requester_peer_id = read_utf8(data, &mut idx, requester_len, "requester_peer_id")?;

    let wait_ms = if idx + 4 <= data.len() {
        read_be_u32(data, &mut idx)?
    } else {
        0
    };

    let request_id = if idx + 2 <= data.len() {
        let n = read_be_u16(data, &mut idx)? as usize;
        if n > 128 {
            return Err(BcError::Peer("request id too long".into()));
        }
        let s = read_utf8(data, &mut idx, n, "request_id")?;
        if !s.chars().all(|c| c.is_ascii_alphanumeric() || c == '-') {
            return Err(BcError::Peer("request id has invalid chars".into()));
        }
        s
    } else {
        String::new()
    };

    if idx != data.len() {
        return Err(BcError::Peer("trailing bytes in request".into()));
    }

    Ok(ChunkRequest {
        key: ChunkKey {
            mount,
            blob,
            offset,
        },
        length,
        requester_peer_id,
        wait_ms,
        request_id,
    })
}

fn encode_response(status: u32, payload: &[u8]) -> Result<Vec<u8>> {
    if payload.len() > MAX_RESPONSE_BYTES as usize {
        return Err(BcError::Peer(format!(
            "response too large: {} bytes",
            payload.len()
        )));
    }

    let mut resp = Vec::with_capacity(8 + payload.len());
    resp.extend_from_slice(&status.to_le_bytes());
    resp.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    resp.extend_from_slice(payload);
    Ok(resp)
}

fn decode_response(data: &[u8], expected_len: u32) -> Result<Bytes> {
    if data.len() < 8 {
        return Err(BcError::Peer(format!(
            "short UCX response: {} bytes",
            data.len()
        )));
    }

    let status = u32::from_le_bytes(data[0..4].try_into().unwrap());
    let payload_len = u32::from_le_bytes(data[4..8].try_into().unwrap());
    let payload = &data[8..];

    if payload_len as usize != payload.len() {
        return Err(BcError::Peer(format!(
            "UCX response length mismatch: header {} body {}",
            payload_len,
            payload.len()
        )));
    }

    match status {
        STATUS_OK => {
            if payload_len == 0 || payload_len > MAX_RESPONSE_BYTES {
                return Err(BcError::Peer(format!("bad resp len {payload_len}")));
            }
            if payload_len > expected_len {
                return Err(BcError::Peer(format!(
                    "response larger than requested: got {payload_len}, expected {expected_len}"
                )));
            }
            Ok(Bytes::copy_from_slice(payload))
        }
        STATUS_MISS => Err(BcError::NotFound("peer miss".into())),
        STATUS_ERR => Err(BcError::Peer("ucx peer returned error".into())),
        _ => Err(BcError::Peer(format!("ucx peer status {status}"))),
    }
}

fn read_u8(data: &[u8], idx: &mut usize) -> Result<u8> {
    if *idx >= data.len() {
        return Err(BcError::Peer("unexpected end of request".into()));
    }
    let value = data[*idx];
    *idx += 1;
    Ok(value)
}

fn read_slice<'a>(data: &'a [u8], idx: &mut usize, len: usize) -> Result<&'a [u8]> {
    if data.len().saturating_sub(*idx) < len {
        return Err(BcError::Peer("unexpected end of request".into()));
    }
    let start = *idx;
    *idx += len;
    Ok(&data[start..start + len])
}

fn read_utf8(data: &[u8], idx: &mut usize, len: usize, field: &str) -> Result<String> {
    String::from_utf8(read_slice(data, idx, len)?.to_vec())
        .map_err(|e| BcError::Peer(format!("{field} utf8: {e}")))
}

fn read_be_u16(data: &[u8], idx: &mut usize) -> Result<u16> {
    let bytes = read_slice(data, idx, 2)?;
    Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
}

fn read_be_u32(data: &[u8], idx: &mut usize) -> Result<u32> {
    let bytes = read_slice(data, idx, 4)?;
    Ok(u32::from_be_bytes(bytes.try_into().unwrap()))
}

fn read_be_u64(data: &[u8], idx: &mut usize) -> Result<u64> {
    let bytes = read_slice(data, idx, 8)?;
    Ok(u64::from_be_bytes(bytes.try_into().unwrap()))
}

struct UcxRuntime {
    context: ucp_context_h,
    worker: ucp_worker_h,
    async_fd: Arc<AsyncFd<WorkerEventFd>>,
}

impl UcxRuntime {
    fn new() -> Result<Self> {
        let mut config: *mut ucp_config_t = ptr::null_mut();
        let status = unsafe { ucp_config_read(ptr::null(), ptr::null(), &mut config) };
        check_status("ucp_config_read", status)?;

        let mut params: ucp_params_t = unsafe { mem::zeroed() };
        params.field_mask = (ucp_params_field::UCP_PARAM_FIELD_FEATURES.0 as u64)
            | (ucp_params_field::UCP_PARAM_FIELD_REQUEST_SIZE.0 as u64)
            | (ucp_params_field::UCP_PARAM_FIELD_REQUEST_INIT.0 as u64)
            | (ucp_params_field::UCP_PARAM_FIELD_REQUEST_CLEANUP.0 as u64);
        params.features =
            (ucp_feature::UCP_FEATURE_TAG.0 as u64) | (ucp_feature::UCP_FEATURE_WAKEUP.0 as u64);
        params.request_size = mem::size_of::<RequestState>() as _;
        params.request_init = Some(request_init_cb);
        params.request_cleanup = Some(request_cleanup_cb);

        let mut context: ucp_context_h = ptr::null_mut();
        let init_status = unsafe {
            ucp_init_version(
                UCP_API_MAJOR as u32,
                UCP_API_MINOR as u32,
                &params,
                config,
                &mut context,
            )
        };
        unsafe {
            ucp_config_release(config);
        }
        check_status("ucp_init_version", init_status)?;

        let mut worker_params: ucp_worker_params_t = unsafe { mem::zeroed() };
        worker_params.field_mask =
            ucp_worker_params_field::UCP_WORKER_PARAM_FIELD_THREAD_MODE.0 as u64;
        worker_params.thread_mode = ucs_thread_mode_t::UCS_THREAD_MODE_SINGLE;

        let mut worker: ucp_worker_h = ptr::null_mut();
        if let Err(e) = check_status("ucp_worker_create", unsafe {
            ucp_worker_create(context, &worker_params, &mut worker)
        }) {
            unsafe {
                ucp_cleanup(context);
            }
            return Err(e);
        }

        let mut efd: i32 = -1;
        if let Err(e) = check_status("ucp_worker_get_efd", unsafe {
            ucp_worker_get_efd(worker, &mut efd)
        }) {
            unsafe {
                ucp_worker_destroy(worker);
                ucp_cleanup(context);
            }
            return Err(e);
        }

        let async_fd = match AsyncFd::new(WorkerEventFd(efd)) {
            Ok(fd) => Arc::new(fd),
            Err(e) => {
                unsafe {
                    ucp_worker_destroy(worker);
                    ucp_cleanup(context);
                }
                return Err(BcError::Peer(format!("AsyncFd ucx worker efd: {e}")));
            }
        };

        Ok(Self {
            context,
            worker,
            async_fd,
        })
    }
}

impl Drop for UcxRuntime {
    fn drop(&mut self) {
        unsafe {
            ucp_worker_destroy(self.worker);
            ucp_cleanup(self.context);
        }
    }
}

fn spawn_progress_task(
    worker: ucp_worker_h,
    async_fd: Arc<AsyncFd<WorkerEventFd>>,
    progress_kick: Rc<Notify>,
    inbound_ready: Rc<Notify>,
) -> JoinHandle<()> {
    tokio::task::spawn_local(async move {
        if let Err(e) = progress_worker(worker, async_fd, progress_kick, inbound_ready).await {
            tracing::debug!(error = %e, "ucx progress loop exited");
        }
    })
}

async fn progress_worker(
    worker: ucp_worker_h,
    async_fd: Arc<AsyncFd<WorkerEventFd>>,
    progress_kick: Rc<Notify>,
    inbound_ready: Rc<Notify>,
) -> Result<()> {
    loop {
        let mut budget = PROGRESS_BUDGET;
        let mut did_work = false;
        loop {
            if unsafe { ucp_worker_progress(worker) } == 0 {
                break;
            }
            did_work = true;
            budget -= 1;
            if budget == 0 {
                tokio::task::yield_now().await;
                budget = PROGRESS_BUDGET;
            }
        }
        if did_work {
            inbound_ready.notify_one();
        }
        match unsafe { ucp_worker_arm(worker) } {
            ucs_status_t::UCS_OK => {
                tokio::select! {
                    biased;
                    _ = progress_kick.notified() => {}
                    guard = async_fd.readable() => {
                        let mut g = guard
                            .map_err(|e| BcError::Peer(format!("ucx worker efd wait: {e}")))?;
                        g.clear_ready();
                    }
                }
            }
            ucs_status_t::UCS_ERR_BUSY => {
                tokio::task::yield_now().await;
                continue;
            }
            status => return Err(status_error("ucp_worker_arm", status)),
        }
    }
}

async fn tag_send(ep: ucp_ep_h, data: &[u8], tag: u64) -> Result<()> {
    let mut params: ucp_request_param_t = unsafe { mem::zeroed() };
    params.op_attr_mask = ucp_op_attr_t::UCP_OP_ATTR_FIELD_CALLBACK as u32;
    params.cb.send = Some(send_cb);

    let ptr = unsafe {
        ucp_tag_send_nbx(
            ep,
            data.as_ptr().cast::<c_void>(),
            data.len().try_into().unwrap(),
            tag,
            &params,
        )
    };
    let needs_progress = !ptr.is_null() && !UCS_PTR_IS_ERR(ptr);
    let fut = request_from_status_ptr("ucp_tag_send_nbx", ptr, 0);
    if needs_progress {
        kick_progress();
    }
    fut.await?;
    Ok(())
}

async fn tag_recv(worker: ucp_worker_h, buf: &mut [u8], tag: u64) -> Result<usize> {
    let mut recv_info: ucp_tag_recv_info_t = unsafe { mem::zeroed() };
    let mut params: ucp_request_param_t = unsafe { mem::zeroed() };
    params.op_attr_mask = (ucp_op_attr_t::UCP_OP_ATTR_FIELD_CALLBACK as u32)
        | (ucp_op_attr_t::UCP_OP_ATTR_FIELD_RECV_INFO as u32);
    params.cb.recv = Some(recv_tag_cb);
    params.recv_info.tag_info = &mut recv_info;

    let ptr = unsafe {
        ucp_tag_recv_nbx(
            worker,
            buf.as_mut_ptr().cast::<c_void>(),
            buf.len().try_into().unwrap(),
            tag,
            u64::MAX,
            &params,
        )
    };
    let inline_len = recv_info.length as usize;
    let needs_progress = !ptr.is_null() && !UCS_PTR_IS_ERR(ptr);
    let fut = request_from_status_ptr("ucp_tag_recv_nbx", ptr, inline_len);
    if needs_progress {
        kick_progress();
    }
    fut.await
}

async fn tag_msg_recv(
    worker: ucp_worker_h,
    buf: &mut [u8],
    msg: ucp_tag_message_h,
) -> Result<usize> {
    let mut recv_info: ucp_tag_recv_info_t = unsafe { mem::zeroed() };
    let mut params: ucp_request_param_t = unsafe { mem::zeroed() };
    params.op_attr_mask = (ucp_op_attr_t::UCP_OP_ATTR_FIELD_CALLBACK as u32)
        | (ucp_op_attr_t::UCP_OP_ATTR_FIELD_RECV_INFO as u32);
    params.cb.recv = Some(recv_tag_cb);
    params.recv_info.tag_info = &mut recv_info;

    let ptr = unsafe {
        ucp_tag_msg_recv_nbx(
            worker,
            buf.as_mut_ptr().cast::<c_void>(),
            buf.len().try_into().unwrap(),
            msg,
            &params,
        )
    };
    let needs_progress = !ptr.is_null() && !UCS_PTR_IS_ERR(ptr);
    let fut = request_from_status_ptr(
        "ucp_tag_msg_recv_nbx",
        ptr,
        recv_info.length.try_into().unwrap(),
    );
    if needs_progress {
        kick_progress();
    }
    fut.await
}

async fn close_ep(ep: ucp_ep_h) -> Result<()> {
    close_ep_with_flags(ep, 0, false).await
}

async fn close_ep_force(ep: ucp_ep_h) -> Result<()> {
    close_ep_with_flags(ep, ucp_ep_close_flags_t::UCP_EP_CLOSE_FLAG_FORCE.0, true).await
}

async fn close_ep_with_flags(ep: ucp_ep_h, flags: u32, include_flags: bool) -> Result<()> {
    let mut params: ucp_request_param_t = unsafe { mem::zeroed() };
    params.op_attr_mask = ucp_op_attr_t::UCP_OP_ATTR_FIELD_CALLBACK as u32;
    params.cb.send = Some(send_cb);
    if include_flags {
        params.op_attr_mask |= ucp_op_attr_t::UCP_OP_ATTR_FIELD_FLAGS as u32;
        params.flags = flags;
    }

    let ptr = unsafe { ucp_ep_close_nbx(ep, &params) };
    let needs_progress = !ptr.is_null() && !UCS_PTR_IS_ERR(ptr);
    let fut = request_from_status_ptr("ucp_ep_close_nbx", ptr, 0);
    if needs_progress {
        kick_progress();
    }
    fut.await?;
    Ok(())
}

#[repr(C)]
struct RequestState {
    done: bool,
    status: ucs_status_t,
    length: usize,
    waker: Option<Waker>,
}

struct UcxRequest {
    action: &'static str,
    request: Option<*mut c_void>,
    inline: Option<Result<usize>>,
}

impl Future for UcxRequest {
    type Output = Result<usize>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(inline) = self.inline.take() {
            return Poll::Ready(inline);
        }

        let Some(request) = self.request else {
            return Poll::Ready(Err(BcError::Peer(format!(
                "{} polled after completion",
                self.action
            ))));
        };

        let state = unsafe { request_state_mut(request) };
        if state.done {
            let status = state.status;
            let length = state.length;
            unsafe {
                ucp_request_free(request);
            }
            self.request = None;
            return Poll::Ready(if status == ucs_status_t::UCS_OK {
                Ok(length)
            } else {
                Err(status_error(self.action, status))
            });
        }

        state.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl Drop for UcxRequest {
    fn drop(&mut self) {
        if let Some(request) = self.request {
            unsafe {
                request_state_mut(request).waker = None;
            }
        }
    }
}

fn request_from_status_ptr(
    action: &'static str,
    ptr: ucs_status_ptr_t,
    inline_length: usize,
) -> UcxRequest {
    if ptr.is_null() {
        return UcxRequest {
            action,
            request: None,
            inline: Some(Ok(inline_length)),
        };
    }

    if UCS_PTR_IS_ERR(ptr) {
        return UcxRequest {
            action,
            request: None,
            inline: Some(Err(status_error(action, UCS_PTR_STATUS(ptr)))),
        };
    }

    let request = ptr.cast::<c_void>();
    // UCX recycles request handles from a pool. `request_init_cb` only fires
    // on the FIRST allocation of a slot; subsequent reuses retain the previous
    // RequestState (done=true, length=N from the prior op). We MUST reset
    // here, before any poll, otherwise UcxRequest::poll sees stale `done=true`
    // and returns immediately with a stale length while the buffer is unwritten.
    unsafe {
        let state = request_state_mut(request);
        state.done = false;
        state.status = ucs_status_t::UCS_INPROGRESS;
        state.length = 0;
        state.waker = None;
    }
    UcxRequest {
        action,
        request: Some(request),
        inline: None,
    }
}

unsafe fn request_state_mut<'a>(request: *mut c_void) -> &'a mut RequestState {
    &mut *(request.cast::<RequestState>())
}

extern "C" fn request_init_cb(request: *mut c_void) {
    unsafe {
        ptr::write(
            request.cast::<RequestState>(),
            RequestState {
                done: false,
                status: ucs_status_t::UCS_INPROGRESS,
                length: 0,
                waker: None,
            },
        );
    }
}

extern "C" fn request_cleanup_cb(request: *mut c_void) {
    unsafe {
        ptr::drop_in_place(request.cast::<RequestState>());
    }
}

extern "C" fn send_cb(request: *mut c_void, status: ucs_status_t, _user_data: *mut c_void) {
    complete_request(request, status, 0);
}

extern "C" fn recv_tag_cb(
    request: *mut c_void,
    status: ucs_status_t,
    tag_info: *const ucp_tag_recv_info_t,
    _user_data: *mut c_void,
) {
    let length = unsafe { tag_info.as_ref().map(|info| info.length).unwrap_or(0) };
    complete_request(request, status, length);
}

fn complete_request(request: *mut c_void, status: ucs_status_t, length: usize) {
    unsafe {
        let state = request_state_mut(request);
        state.done = true;
        state.status = status;
        state.length = length.try_into().unwrap();
        if let Some(waker) = state.waker.take() {
            waker.wake();
        }
    }
}

extern "C" fn endpoint_error_cb(arg: *mut c_void, _ep: ucp_ep_h, status: ucs_status_t) {
    if !arg.is_null() {
        let broken = unsafe { &*(arg.cast::<Arc<AtomicBool>>()) };
        broken.store(true, Ordering::SeqCst);
    }
    tracing::debug!(
        status = ucx_status_name(status),
        "ucx endpoint error callback"
    );
}

fn check_status(action: &str, status: ucs_status_t) -> Result<()> {
    if status == ucs_status_t::UCS_OK {
        Ok(())
    } else {
        Err(status_error(action, status))
    }
}

fn status_error(action: &str, status: ucs_status_t) -> BcError {
    BcError::Peer(format!("{action} failed: {}", ucx_status_name(status)))
}

fn ucx_status_name(status: ucs_status_t) -> String {
    let ptr = unsafe { ucs_status_string(status) };
    if ptr.is_null() {
        return format!("{status:?}");
    }
    unsafe { CStr::from_ptr(ptr.cast::<c_char>()) }
        .to_string_lossy()
        .into_owned()
}

#[derive(Clone, Copy)]
struct WorkerEventFd(RawFd);

impl AsRawFd for WorkerEventFd {
    fn as_raw_fd(&self) -> RawFd {
        self.0
    }
}
