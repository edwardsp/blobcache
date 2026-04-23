// UCX-over-InfiniBand peer transport (v2 backend).
//
// This module intentionally uses raw ucx1-sys FFI directly instead of the
// async-ucx wrapper. The public API and wire protocol stay identical to the
// previous implementation, but all UCP objects are now driven explicitly from
// a dedicated single-threaded worker thread.

#![cfg(feature = "ucx")]

// ucx1-sys' build script links libucp but not libucs; we use ucs_status_string
// directly, so request the linker to also pull in libucs.
#[link(name = "ucs")]
extern "C" {}

use bytes::Bytes;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::ffi::CStr;
use std::future::Future;
use std::mem::{self, MaybeUninit};
use std::net::SocketAddr;
use std::os::fd::{AsRawFd, RawFd};
use std::os::raw::{c_char, c_void};
use std::pin::Pin;
use std::ptr;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;
use tokio::io::unix::AsyncFd;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use ucx1_sys::*;

use crate::cache::{ChunkKey, DiskCache};
use crate::error::{BcError, Result};

const MAGIC: u32 = 0xBC10_C001;
const STATUS_OK: u8 = 0;
const STATUS_MISS: u8 = 1;
const STATUS_ERR: u8 = 2;
const MAX_RESPONSE_BYTES: u32 = 64 * 1024 * 1024;
const MAX_POLLED_EPS: usize = 16;
const POOL_SIZE_PER_PEER: usize = 4;
type SharedRuntimeState = Rc<RefCell<RuntimeState>>;

// ============================================================================
// Server + shared runtime bootstrap
// ============================================================================

pub struct RdmaPeerService {
    _shared: Arc<RdmaRuntimeShared>,
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
        reply: oneshot::Sender<Result<Bytes>>,
    },
    Health {
        peer_id: String,
        peer_addr_blob: Vec<u8>,
        reply: oneshot::Sender<Result<()>>,
    },
}

impl RdmaPeerService {
    pub fn start(
        cache: Arc<DiskCache>,
        addr: SocketAddr,
        stats: Arc<crate::stats::PeerStats>,
    ) -> Result<(Self, RdmaPeerClient, Vec<u8>)> {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<RdmaCmd>();
        let (started_tx, started_rx) = std::sync::mpsc::sync_channel::<Result<Vec<u8>>>(1);
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

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
                    if let Err(e) = run_runtime(cache, stats, cmd_rx, started_tx, shutdown_rx).await
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

        Ok((Self { _shared: shared }, client, worker_addr_blob))
    }
}

impl RdmaPeerClient {
    pub async fn fetch_chunk(
        &self,
        peer_id: &str,
        peer_addr_blob: &[u8],
        key: &ChunkKey,
        length: u32,
    ) -> Result<Bytes> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(RdmaCmd::Fetch {
                peer_id: peer_id.to_string(),
                peer_addr_blob: peer_addr_blob.to_vec(),
                key: key.clone(),
                length,
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
    mut cmd_rx: mpsc::UnboundedReceiver<RdmaCmd>,
    started_tx: std::sync::mpsc::SyncSender<Result<Vec<u8>>>,
    mut shutdown_rx: oneshot::Receiver<()>,
) -> Result<()> {
    let ucx = match UcxRuntime::new() {
        Ok(ucx) => ucx,
        Err(e) => {
            let msg = e.to_string();
            let _ = started_tx.send(Err(BcError::Peer(msg.clone())));
            return Err(e);
        }
    };

    let mut worker_addr: *mut ucp_address_t = ptr::null_mut();
    let mut worker_addr_len: u64 = 0;
    let worker_addr_status =
        unsafe { ucp_worker_get_address(ucx.worker, &mut worker_addr, &mut worker_addr_len) };
    if let Err(e) = check_status("ucp_worker_get_address", worker_addr_status) {
        let msg = e.to_string();
        let _ = started_tx.send(Err(BcError::Peer(msg.clone())));
        return Err(e);
    }

    let worker_addr_blob = unsafe {
        std::slice::from_raw_parts(worker_addr.cast::<u8>(), worker_addr_len as usize).to_vec()
    };
    let progress = spawn_progress_task(ucx.worker, ucx.async_fd.clone());
    let state = Rc::new(RefCell::new(RuntimeState::new(ucx.worker, stats.clone())));

    if started_tx.send(Ok(worker_addr_blob.clone())).is_err() {
        progress.abort();
        let _ = progress.await;
        unsafe {
            ucp_worker_release_address(ucx.worker, worker_addr);
        }
        return Err(BcError::Peer("ucx runtime startup receiver dropped".into()));
    }

    let mut poll_tick = tokio::time::interval(Duration::from_millis(5));
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
            _ = poll_tick.tick() => {
                if let Err(e) = poll_ready_endpoints(state.clone(), cache.clone(), stats.clone()) {
                    break Err(e);
                }
                if let Err(e) = reap_broken_client_eps(state.clone()).await {
                    break Err(e);
                }
            }
        }
    };

    progress.abort();
    let _ = progress.await;
    let shutdown_result = close_all_client_eps(state.clone()).await;
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
                reply,
            } => {
                let result =
                    client_fetch(state.clone(), &peer_id, &peer_addr_blob, &key, length).await;
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
        }
    });
}

fn poll_ready_endpoints(
    state: SharedRuntimeState,
    cache: Arc<DiskCache>,
    stats: Arc<crate::stats::PeerStats>,
) -> Result<()> {
    let worker = state.borrow().worker;
    let mut polled_eps: [ucp_stream_poll_ep_t; MAX_POLLED_EPS] = unsafe { mem::zeroed() };
    let ready = unsafe {
        ucp_stream_worker_poll(worker, polled_eps.as_mut_ptr(), MAX_POLLED_EPS as u64, 0)
    };
    if ready < 0 {
        return Err(BcError::Peer(format!(
            "ucp_stream_worker_poll failed: {ready}"
        )));
    }

    let mut to_serve = Vec::new();
    {
        let mut runtime = state.borrow_mut();
        for polled in polled_eps.iter().take(ready as usize) {
            if polled.ep.is_null() {
                continue;
            }
            if runtime.active_server_eps.insert(polled.ep) {
                to_serve.push(polled.ep);
            }
        }
    }

    for ep in to_serve {
        let state = state.clone();
        let cache = cache.clone();
        let stats = stats.clone();
        tokio::task::spawn_local(async move {
            let result = serve_one_inner(ep, cache, stats).await;
            if let Err(e) = &result {
                tracing::debug!(error = %e, "ucx serve_one_inner ended");
                let _ = close_ep_force(ep).await;
            }
            state.borrow_mut().active_server_eps.remove(&ep);
        });
    }

    Ok(())
}

async fn reap_broken_client_eps(state: SharedRuntimeState) -> Result<()> {
    let stale = state.borrow_mut().take_broken_client_eps();
    for stale_ep in stale {
        let _ = close_ep_force(stale_ep.ep).await;
        release_callback_arg(stale_ep.callback_arg);
    }
    Ok(())
}

async fn close_all_client_eps(state: SharedRuntimeState) -> Result<()> {
    let all = state.borrow_mut().take_all_client_eps();
    for slot in all {
        let _ = if slot.broken.load(Ordering::SeqCst) {
            close_ep_force(slot.ep).await
        } else {
            close_ep(slot.ep).await
        };
        release_callback_arg(slot.callback_arg);
    }
    Ok(())
}

// ============================================================================
// Server data-plane
// ============================================================================

async fn serve_one_inner(
    ep: ucp_ep_h,
    cache: Arc<DiskCache>,
    stats: Arc<crate::stats::PeerStats>,
) -> Result<()> {
    stats.chunk_requests.inc();

    let request = match read_request(ep).await {
        Ok(req) => req,
        Err(e) => {
            let _ = send_error_response(ep).await;
            return Err(e);
        }
    };

    let cache2 = cache.clone();
    let key2 = request.clone();
    let got = tokio::task::spawn_blocking(move || cache2.try_get(&key2))
        .await
        .map_err(|e| BcError::Peer(format!("spawn_blocking: {e}")))?;

    match got {
        Some(b) => {
            stats.chunk_bytes_served.inc_by(b.len() as u64);
            let mut resp = Vec::with_capacity(5 + b.len());
            resp.push(STATUS_OK);
            resp.extend_from_slice(&(b.len() as u32).to_be_bytes());
            resp.extend_from_slice(&b);
            stream_send(ep, &resp).await?;
        }
        None => {
            stream_send(ep, &[STATUS_MISS, 0, 0, 0, 0]).await?;
        }
    }

    Ok(())
}

async fn send_error_response(ep: ucp_ep_h) -> Result<()> {
    stream_send(ep, &[STATUS_ERR, 0, 0, 0, 0]).await
}

async fn read_request(ep: ucp_ep_h) -> Result<ChunkKey> {
    let mut hdr = [MaybeUninit::<u8>::uninit(); 4 + 1];
    fill_exact(ep, &mut hdr).await?;
    let hdr = unsafe { mem_init(&hdr) };
    let magic = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]);
    if magic != MAGIC {
        return Err(BcError::Peer(format!("bad magic 0x{magic:08x}")));
    }
    let mount_len = hdr[4] as usize;

    let mut mount = vec![MaybeUninit::<u8>::uninit(); mount_len];
    fill_exact(ep, &mut mount).await?;
    let mount = String::from_utf8(unsafe { mem_init(&mount).to_vec() })
        .map_err(|e| BcError::Peer(format!("mount utf8: {e}")))?;

    let mut blob_len_buf = [MaybeUninit::<u8>::uninit(); 2];
    fill_exact(ep, &mut blob_len_buf).await?;
    let blob_len = u16::from_be_bytes(unsafe {
        let s = mem_init(&blob_len_buf);
        [s[0], s[1]]
    }) as usize;

    let mut blob = vec![MaybeUninit::<u8>::uninit(); blob_len];
    fill_exact(ep, &mut blob).await?;
    let blob = String::from_utf8(unsafe { mem_init(&blob).to_vec() })
        .map_err(|e| BcError::Peer(format!("blob utf8: {e}")))?;

    let mut tail = [MaybeUninit::<u8>::uninit(); 8 + 4];
    fill_exact(ep, &mut tail).await?;
    let tail = unsafe { mem_init(&tail) };
    let offset = u64::from_be_bytes([
        tail[0], tail[1], tail[2], tail[3], tail[4], tail[5], tail[6], tail[7],
    ]);
    let _length = u32::from_be_bytes([tail[8], tail[9], tail[10], tail[11]]);

    Ok(ChunkKey {
        mount,
        blob,
        offset,
    })
}

// ============================================================================
// Client data-plane + pooled endpoints
// ============================================================================

struct RuntimeState {
    worker: ucp_worker_h,
    peer_stats: Arc<crate::stats::PeerStats>,
    client_pools: HashMap<String, EndpointPool>,
    active_server_eps: HashSet<ucp_ep_h>,
    lane_verified: bool,
}

struct EndpointPool {
    slots: Vec<Option<EndpointSlot>>,
    next: AtomicUsize,
}

struct EndpointSlot {
    ep: ucp_ep_h,
    broken: Arc<AtomicBool>,
    callback_arg: *mut Arc<AtomicBool>,
    busy: bool,
}

struct CheckedOutEndpoint {
    peer_id: String,
    slot_idx: usize,
    ep: ucp_ep_h,
}

struct RemovedEndpoint {
    ep: ucp_ep_h,
    broken: Arc<AtomicBool>,
    callback_arg: *mut Arc<AtomicBool>,
}

impl RuntimeState {
    fn new(worker: ucp_worker_h, peer_stats: Arc<crate::stats::PeerStats>) -> Self {
        Self {
            worker,
            peer_stats,
            client_pools: HashMap::new(),
            active_server_eps: HashSet::new(),
            lane_verified: false,
        }
    }

    fn checkout_endpoint(
        &mut self,
        peer_id: &str,
        peer_addr_blob: &[u8],
    ) -> Result<CheckedOutEndpoint> {
        let mut verify_ep = None;
        let mut selected = None;
        {
            let pool = self
                .client_pools
                .entry(peer_id.to_string())
                .or_insert_with(EndpointPool::new);
            let start = pool.next.fetch_add(1, Ordering::Relaxed) % POOL_SIZE_PER_PEER;

            for offset in 0..POOL_SIZE_PER_PEER {
                let idx = (start + offset) % POOL_SIZE_PER_PEER;
                let slot = &mut pool.slots[idx];

                let should_create = match slot {
                    Some(existing) => existing.broken.load(Ordering::SeqCst),
                    None => true,
                };
                if should_create {
                    *slot = Some(create_pooled_ep(self.worker, peer_addr_blob)?);
                    if verify_ep.is_none() {
                        verify_ep = slot.as_ref().map(|created| created.ep);
                    }
                }

                if let Some(existing) = slot.as_mut() {
                    if existing.broken.load(Ordering::SeqCst) || existing.busy {
                        continue;
                    }
                    existing.busy = true;
                    selected = Some((idx, existing.ep));
                    break;
                }
            }
        }

        if let Some(ep) = verify_ep {
            self.verify_lane_once(ep);
        }
        if let Some((slot_idx, ep)) = selected {
            return Ok(CheckedOutEndpoint {
                peer_id: peer_id.to_string(),
                slot_idx,
                ep,
            });
        }

        Err(BcError::Peer(format!(
            "all UCX pooled endpoints are busy for peer {peer_id}"
        )))
    }

    fn release_endpoint(&mut self, checked_out: CheckedOutEndpoint) -> Option<RemovedEndpoint> {
        let pool = self.client_pools.get_mut(&checked_out.peer_id)?;
        let slot = pool.slots.get_mut(checked_out.slot_idx)?;
        let existing = slot.as_mut()?;
        existing.busy = false;
        if existing.broken.load(Ordering::SeqCst) {
            let removed = slot.take()?;
            return Some(RemovedEndpoint {
                ep: removed.ep,
                broken: removed.broken,
                callback_arg: removed.callback_arg,
            });
        }
        None
    }

    fn take_broken_client_eps(&mut self) -> Vec<RemovedEndpoint> {
        let mut removed = Vec::new();
        for pool in self.client_pools.values_mut() {
            for slot in &mut pool.slots {
                let should_remove = slot
                    .as_ref()
                    .map(|entry| entry.broken.load(Ordering::SeqCst) && !entry.busy)
                    .unwrap_or(false);
                if should_remove {
                    if let Some(entry) = slot.take() {
                        removed.push(RemovedEndpoint {
                            ep: entry.ep,
                            broken: entry.broken,
                            callback_arg: entry.callback_arg,
                        });
                    }
                }
            }
        }
        removed
    }

    fn take_all_client_eps(&mut self) -> Vec<RemovedEndpoint> {
        let mut removed = Vec::new();
        for pool in self.client_pools.values_mut() {
            for slot in &mut pool.slots {
                if let Some(entry) = slot.take() {
                    removed.push(RemovedEndpoint {
                        ep: entry.ep,
                        broken: entry.broken,
                        callback_arg: entry.callback_arg,
                    });
                }
            }
        }
        removed
    }

    fn verify_lane_once(&mut self, _ep: ucp_ep_h) {
        if self.lane_verified {
            return;
        }
        self.lane_verified = true;

        // UCX 1.13.1 in Debian bookworm does not expose
        // ucp_ep_query(UCP_EP_ATTR_FIELD_TRANSPORTS), so lane verification is
        // performed via UCX_LOG_LEVEL=info pod logs instead. Grep the UCX wireup
        // lines at runtime to confirm RC/DC selection versus TCP fallback.
        // Keep blobcache_rdma_non_rdma_lane_total defined for future UCX builds
        // where programmatic endpoint-lane inspection is available.
        tracing::info!("ucx lane verification relies on UCX_LOG_LEVEL=info pod logs on UCX 1.13.1");
    }
}

impl EndpointPool {
    fn new() -> Self {
        Self {
            slots: (0..POOL_SIZE_PER_PEER).map(|_| None).collect(),
            next: AtomicUsize::new(0),
        }
    }
}

async fn client_fetch(
    state: SharedRuntimeState,
    peer_id: &str,
    peer_addr_blob: &[u8],
    key: &ChunkKey,
    length: u32,
) -> Result<Bytes> {
    if length == 0 || length > MAX_RESPONSE_BYTES {
        return Err(BcError::Peer(format!("bad chunk length {length}")));
    }

    let checked_out = state
        .borrow_mut()
        .checkout_endpoint(peer_id, peer_addr_blob)?;
    let ep = checked_out.ep;
    let result = client_fetch_inner(ep, key, length).await;
    let removed = state.borrow_mut().release_endpoint(checked_out);
    if let Some(removed) = removed {
        let _ = close_ep_force(removed.ep).await;
        release_callback_arg(removed.callback_arg);
    }
    result
}

async fn client_fetch_inner(ep: ucp_ep_h, key: &ChunkKey, length: u32) -> Result<Bytes> {
    let req = encode_request(key, length)?;
    stream_send(ep, &req).await?;

    let mut head = [MaybeUninit::<u8>::uninit(); 1 + 4];
    fill_exact(ep, &mut head).await?;
    let head = unsafe { mem_init(&head) };
    let status = head[0];
    let resp_len = u32::from_be_bytes([head[1], head[2], head[3], head[4]]);

    match status {
        STATUS_OK => {
            if resp_len == 0 || resp_len > MAX_RESPONSE_BYTES {
                return Err(BcError::Peer(format!("bad resp len {resp_len}")));
            }
            let mut buf = vec![MaybeUninit::<u8>::uninit(); resp_len as usize];
            fill_exact(ep, &mut buf).await?;
            Ok(Bytes::from(unsafe { mem_init(&buf).to_vec() }))
        }
        STATUS_MISS => Err(BcError::NotFound("peer miss".into())),
        STATUS_ERR => Err(BcError::Peer("ucx peer returned error".into())),
        _ => Err(BcError::Peer(format!("ucx peer status {status}"))),
    }
}

async fn client_health(
    state: SharedRuntimeState,
    peer_id: &str,
    peer_addr_blob: &[u8],
) -> Result<()> {
    let checked_out = state
        .borrow_mut()
        .checkout_endpoint(peer_id, peer_addr_blob)?;
    let removed = state.borrow_mut().release_endpoint(checked_out);
    if let Some(removed) = removed {
        let _ = close_ep_force(removed.ep).await;
        release_callback_arg(removed.callback_arg);
    }
    Ok(())
}

fn create_pooled_ep(worker: ucp_worker_h, peer_addr_blob: &[u8]) -> Result<EndpointSlot> {
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
        busy: false,
    })
}

fn release_callback_arg(callback_arg: *mut Arc<AtomicBool>) {
    if !callback_arg.is_null() {
        unsafe {
            drop(Box::from_raw(callback_arg));
        }
    }
}

fn encode_request(key: &ChunkKey, length: u32) -> Result<Vec<u8>> {
    let mount = key.mount.as_bytes();
    let blob = key.blob.as_bytes();
    if mount.len() > u8::MAX as usize {
        return Err(BcError::Peer("mount name too long".into()));
    }
    if blob.len() > u16::MAX as usize {
        return Err(BcError::Peer("blob name too long".into()));
    }

    let mut req = Vec::with_capacity(4 + 1 + mount.len() + 2 + blob.len() + 8 + 4);
    req.extend_from_slice(&MAGIC.to_be_bytes());
    req.push(mount.len() as u8);
    req.extend_from_slice(mount);
    req.extend_from_slice(&(blob.len() as u16).to_be_bytes());
    req.extend_from_slice(blob);
    req.extend_from_slice(&key.offset.to_be_bytes());
    req.extend_from_slice(&length.to_be_bytes());
    Ok(req)
}

// ============================================================================
// UCX helpers
// ============================================================================

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
            (ucp_feature::UCP_FEATURE_STREAM.0 as u64) | (ucp_feature::UCP_FEATURE_WAKEUP.0 as u64);
        params.request_size = mem::size_of::<RequestState>() as u64;
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
) -> JoinHandle<()> {
    tokio::task::spawn_local(async move {
        if let Err(e) = progress_worker(worker, async_fd).await {
            tracing::debug!(error = %e, "ucx progress loop exited");
        }
    })
}

async fn progress_worker(
    worker: ucp_worker_h,
    async_fd: Arc<AsyncFd<WorkerEventFd>>,
) -> Result<()> {
    loop {
        while unsafe { ucp_worker_progress(worker) } != 0 {}
        match unsafe { ucp_worker_arm(worker) } {
            ucs_status_t::UCS_OK => {
                let mut guard = async_fd
                    .readable()
                    .await
                    .map_err(|e| BcError::Peer(format!("ucx worker efd wait: {e}")))?;
                guard.clear_ready();
            }
            ucs_status_t::UCS_ERR_BUSY => continue,
            status => return Err(status_error("ucp_worker_arm", status)),
        }
    }
}

async fn stream_send(ep: ucp_ep_h, data: &[u8]) -> Result<()> {
    let mut params: ucp_request_param_t = unsafe { mem::zeroed() };
    params.op_attr_mask = ucp_op_attr_t::UCP_OP_ATTR_FIELD_CALLBACK as u32;
    params.cb.send = Some(send_cb);

    let ptr = unsafe {
        ucp_stream_send_nbx(
            ep,
            data.as_ptr().cast::<c_void>(),
            data.len() as u64,
            &params,
        )
    };
    request_from_status_ptr("ucp_stream_send_nbx", ptr, 0).await?;
    Ok(())
}

async fn stream_recv(ep: ucp_ep_h, buf: &mut [MaybeUninit<u8>]) -> Result<usize> {
    let mut received_length: u64 = 0;
    let mut params: ucp_request_param_t = unsafe { mem::zeroed() };
    params.op_attr_mask = ucp_op_attr_t::UCP_OP_ATTR_FIELD_CALLBACK as u32;
    params.cb.recv_stream = Some(recv_stream_cb);

    let ptr = unsafe {
        ucp_stream_recv_nbx(
            ep,
            buf.as_mut_ptr().cast::<c_void>(),
            buf.len() as u64,
            &mut received_length,
            &params,
        )
    };
    request_from_status_ptr("ucp_stream_recv_nbx", ptr, received_length as usize).await
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
    request_from_status_ptr("ucp_ep_close_nbx", ptr, 0).await?;
    Ok(())
}

async fn fill_exact(ep: ucp_ep_h, buf: &mut [MaybeUninit<u8>]) -> Result<()> {
    let mut filled = 0;
    while filled < buf.len() {
        let n = stream_recv(ep, &mut buf[filled..]).await?;
        if n == 0 {
            return Err(BcError::Peer("ucx recv: peer closed".into()));
        }
        filled += n;
    }
    Ok(())
}

unsafe fn mem_init(b: &[MaybeUninit<u8>]) -> &[u8] {
    std::slice::from_raw_parts(b.as_ptr() as *const u8, b.len())
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
    unsafe {
        reset_request_state(request);
    }
    UcxRequest {
        action,
        request: Some(request),
        inline: None,
    }
}

unsafe fn reset_request_state(request: *mut c_void) {
    let state = request_state_mut(request);
    state.done = false;
    state.status = ucs_status_t::UCS_INPROGRESS;
    state.length = 0;
    state.waker = None;
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

extern "C" fn recv_stream_cb(
    request: *mut c_void,
    status: ucs_status_t,
    length: u64,
    _user_data: *mut c_void,
) {
    complete_request(request, status, length as usize);
}

fn complete_request(request: *mut c_void, status: ucs_status_t, length: usize) {
    unsafe {
        let state = request_state_mut(request);
        state.done = true;
        state.status = status;
        state.length = length;
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
