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
use std::ffi::CStr;
use std::future::Future;
use std::mem::{self, MaybeUninit};
use std::net::SocketAddr;
use std::os::fd::{AsRawFd, RawFd};
use std::os::raw::{c_char, c_void};
use std::pin::Pin;
use std::ptr;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
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

// ============================================================================
// Server
// ============================================================================

pub struct RdmaPeerService {
    shutdown: Option<oneshot::Sender<()>>,
}

impl RdmaPeerService {
    pub fn start(
        cache: Arc<DiskCache>,
        addr: SocketAddr,
        stats: Arc<crate::stats::PeerStats>,
    ) -> Result<Self> {
        let (started_tx, started_rx) = std::sync::mpsc::sync_channel::<Result<()>>(1);
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        std::thread::Builder::new()
            .name("ucx-server".into())
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
                    if let Err(e) = run_server(cache, addr, stats, started_tx, shutdown_rx).await {
                        tracing::error!(error=%e, "ucx server thread exited with error");
                    }
                });
            })
            .map_err(|e| BcError::Peer(format!("ucx-server thread: {e}")))?;

        started_rx
            .recv()
            .map_err(|_| BcError::Peer("ucx-server startup channel closed".into()))??;
        tracing::info!(%addr, "ucx peer transport listening");
        Ok(Self {
            shutdown: Some(shutdown_tx),
        })
    }
}

impl Drop for RdmaPeerService {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}

async fn run_server(
    cache: Arc<DiskCache>,
    addr: SocketAddr,
    stats: Arc<crate::stats::PeerStats>,
    started_tx: std::sync::mpsc::SyncSender<Result<()>>,
    shutdown_rx: oneshot::Receiver<()>,
) -> Result<()> {
    let ucx = match UcxRuntime::new() {
        Ok(ucx) => ucx,
        Err(e) => {
            let msg = e.to_string();
            let _ = started_tx.send(Err(BcError::Peer(msg.clone())));
            return Err(e);
        }
    };

    let progress = spawn_progress_task(ucx.worker, ucx.async_fd.clone());

    let (conn_tx, mut conn_rx) = mpsc::unbounded_channel::<ucp_conn_request_h>();
    let conn_tx_ptr = Box::into_raw(Box::new(conn_tx));

    let sockaddr = SockAddr::from(addr);
    let mut listener: ucp_listener_h = ptr::null_mut();
    let mut listener_params: ucp_listener_params_t = unsafe { mem::zeroed() };
    listener_params.field_mask =
        (ucp_listener_params_field::UCP_LISTENER_PARAM_FIELD_SOCK_ADDR.0 as u64)
            | (ucp_listener_params_field::UCP_LISTENER_PARAM_FIELD_CONN_HANDLER.0 as u64);
    listener_params.sockaddr = sockaddr.as_ucs_sock_addr();
    listener_params.conn_handler.cb = Some(server_conn_handler_cb);
    listener_params.conn_handler.arg = conn_tx_ptr.cast::<c_void>();
    let status = unsafe { ucp_listener_create(ucx.worker, &listener_params, &mut listener) };
    if let Err(e) = check_status("ucp_listener_create", status) {
        progress.abort();
        unsafe {
            drop(Box::from_raw(conn_tx_ptr));
        }
        let msg = e.to_string();
        let _ = started_tx.send(Err(BcError::Peer(msg)));
        return Err(e);
    }

    let _ = started_tx.send(Ok(()));

    let mut shutdown_rx = shutdown_rx;
    let result = loop {
        tokio::select! {
            _ = &mut shutdown_rx => {
                tracing::info!("ucx server shutting down");
                break Ok(());
            }
            maybe_conn = conn_rx.recv() => {
                let Some(conn_request) = maybe_conn else {
                    break Err(BcError::Peer("ucx conn request channel closed".into()));
                };
                let cache = cache.clone();
                let stats = stats.clone();
                let worker = ucx.worker;
                tokio::task::spawn_local(async move {
                    match accept_connection(worker, conn_request) {
                        Ok(ep) => {
                            if let Err(e) = serve_one(ep, cache, stats).await {
                                tracing::debug!(error=%e, "ucx serve_one ended");
                            }
                        }
                        Err(e) => tracing::warn!(error=%e, "ucx accept failed"),
                    }
                });
            }
        }
    };

    progress.abort();
    let _ = progress.await;
    unsafe {
        ucp_listener_destroy(listener);
        drop(Box::from_raw(conn_tx_ptr));
    }
    result
}

fn accept_connection(worker: ucp_worker_h, conn_request: ucp_conn_request_h) -> Result<ucp_ep_h> {
    let mut ep: ucp_ep_h = ptr::null_mut();
    let mut ep_params: ucp_ep_params_t = unsafe { mem::zeroed() };
    ep_params.field_mask = (ucp_ep_params_field::UCP_EP_PARAM_FIELD_CONN_REQUEST.0 as u64)
        | (ucp_ep_params_field::UCP_EP_PARAM_FIELD_ERR_HANDLER.0 as u64)
        | (ucp_ep_params_field::UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE.0 as u64);
    ep_params.conn_request = conn_request;
    ep_params.err_mode = ucp_err_handling_mode_t::UCP_ERR_HANDLING_MODE_PEER;
    ep_params.err_handler.cb = Some(endpoint_error_cb);
    ep_params.err_handler.arg = ptr::null_mut();
    let status = unsafe { ucp_ep_create(worker, &ep_params, &mut ep) };
    check_status("ucp_ep_create(server)", status)?;
    Ok(ep)
}

async fn serve_one(
    ep: ucp_ep_h,
    cache: Arc<DiskCache>,
    stats: Arc<crate::stats::PeerStats>,
) -> Result<()> {
    let result = serve_one_inner(ep, cache, stats).await;
    let _ = close_ep(ep).await;
    result
}

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
// Client
// ============================================================================

#[derive(Clone)]
pub struct RdmaPeerClient {
    cmd_tx: mpsc::UnboundedSender<RdmaCmd>,
}

enum RdmaCmd {
    Fetch {
        peer_addr: SocketAddr,
        key: ChunkKey,
        length: u32,
        reply: oneshot::Sender<Result<Bytes>>,
    },
    Health {
        peer_addr: SocketAddr,
        reply: oneshot::Sender<Result<()>>,
    },
}

impl RdmaPeerClient {
    pub fn new() -> Result<Self> {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<RdmaCmd>();
        let (started_tx, started_rx) = std::sync::mpsc::sync_channel::<Result<()>>(1);

        std::thread::Builder::new()
            .name("ucx-client".into())
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
                    if let Err(e) = run_client(cmd_rx, started_tx).await {
                        tracing::error!(error=%e, "ucx client thread exited");
                    }
                });
            })
            .map_err(|e| BcError::Peer(format!("ucx-client thread: {e}")))?;

        started_rx
            .recv()
            .map_err(|_| BcError::Peer("ucx-client startup channel closed".into()))??;
        tracing::info!("ucx peer client ready");
        Ok(Self { cmd_tx })
    }

    pub async fn fetch_chunk(&self, peer_url: &str, key: &ChunkKey, length: u32) -> Result<Bytes> {
        let peer_addr = parse_peer_addr(peer_url)?;
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(RdmaCmd::Fetch {
                peer_addr,
                key: key.clone(),
                length,
                reply: tx,
            })
            .map_err(|_| BcError::Peer("ucx client thread gone".into()))?;
        rx.await
            .map_err(|_| BcError::Peer("ucx fetch reply dropped".into()))?
    }

    pub async fn health(&self, peer_url: &str) -> Result<()> {
        let peer_addr = parse_peer_addr(peer_url)?;
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(RdmaCmd::Health {
                peer_addr,
                reply: tx,
            })
            .map_err(|_| BcError::Peer("ucx client thread gone".into()))?;
        rx.await
            .map_err(|_| BcError::Peer("ucx health reply dropped".into()))?
    }
}

fn parse_peer_addr(peer_url: &str) -> Result<SocketAddr> {
    let s = peer_url
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .trim_start_matches("rdma://")
        .trim_end_matches('/');
    s.parse::<SocketAddr>()
        .map_err(|e| BcError::Peer(format!("peer addr {peer_url:?}: {e}")))
}

async fn run_client(
    mut rx: mpsc::UnboundedReceiver<RdmaCmd>,
    started_tx: std::sync::mpsc::SyncSender<Result<()>>,
) -> Result<()> {
    let ucx = match UcxRuntime::new() {
        Ok(ucx) => ucx,
        Err(e) => {
            let msg = e.to_string();
            let _ = started_tx.send(Err(BcError::Peer(msg.clone())));
            return Err(e);
        }
    };

    let progress = spawn_progress_task(ucx.worker, ucx.async_fd.clone());
    let _ = started_tx.send(Ok(()));

    while let Some(cmd) = rx.recv().await {
        let worker = ucx.worker;
        tokio::task::spawn_local(async move {
            match cmd {
                RdmaCmd::Fetch {
                    peer_addr,
                    key,
                    length,
                    reply,
                } => {
                    let r = client_fetch(worker, peer_addr, &key, length).await;
                    let _ = reply.send(r);
                }
                RdmaCmd::Health { peer_addr, reply } => {
                    let r = client_health(worker, peer_addr).await;
                    let _ = reply.send(r);
                }
            }
        });
    }

    progress.abort();
    let _ = progress.await;
    Ok(())
}

async fn client_fetch(
    worker: ucp_worker_h,
    peer_addr: SocketAddr,
    key: &ChunkKey,
    length: u32,
) -> Result<Bytes> {
    if length == 0 || length > MAX_RESPONSE_BYTES {
        return Err(BcError::Peer(format!("bad chunk length {length}")));
    }

    let ep = connect_socket(worker, peer_addr)?;
    let result = client_fetch_inner(ep, key, length).await;
    let _ = close_ep(ep).await;
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

async fn client_health(worker: ucp_worker_h, peer_addr: SocketAddr) -> Result<()> {
    let ep = connect_socket(worker, peer_addr)?;
    let result = close_ep(ep).await;
    match result {
        Ok(()) => Ok(()),
        Err(e) => Err(e),
    }
}

fn connect_socket(worker: ucp_worker_h, peer_addr: SocketAddr) -> Result<ucp_ep_h> {
    let sockaddr = SockAddr::from(peer_addr);
    let mut ep: ucp_ep_h = ptr::null_mut();
    let mut ep_params: ucp_ep_params_t = unsafe { mem::zeroed() };
    ep_params.field_mask = (ucp_ep_params_field::UCP_EP_PARAM_FIELD_FLAGS.0 as u64)
        | (ucp_ep_params_field::UCP_EP_PARAM_FIELD_SOCK_ADDR.0 as u64)
        | (ucp_ep_params_field::UCP_EP_PARAM_FIELD_ERR_HANDLER.0 as u64)
        | (ucp_ep_params_field::UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE.0 as u64);
    ep_params.flags = ucp_ep_params_flags_field::UCP_EP_PARAMS_FLAGS_CLIENT_SERVER.0 as u32;
    ep_params.sockaddr = sockaddr.as_ucs_sock_addr();
    ep_params.err_mode = ucp_err_handling_mode_t::UCP_ERR_HANDLING_MODE_PEER;
    ep_params.err_handler.cb = Some(endpoint_error_cb);
    ep_params.err_handler.arg = ptr::null_mut();
    let status = unsafe { ucp_ep_create(worker, &ep_params, &mut ep) };
    check_status(&format!("ucp_ep_create(client {peer_addr})"), status)?;
    Ok(ep)
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
            tracing::debug!(error=%e, "ucx progress loop exited");
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
        ucp_stream_send_nbx(ep, data.as_ptr().cast::<c_void>(), data.len() as u64, &params)
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
    let mut params: ucp_request_param_t = unsafe { mem::zeroed() };
    params.op_attr_mask = ucp_op_attr_t::UCP_OP_ATTR_FIELD_CALLBACK as u32;
    params.cb.send = Some(send_cb);

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

extern "C" fn server_conn_handler_cb(conn_request: ucp_conn_request_h, arg: *mut c_void) {
    if arg.is_null() {
        return;
    }
    let sender = unsafe { &*(arg.cast::<mpsc::UnboundedSender<ucp_conn_request_h>>()) };
    if sender.send(conn_request).is_err() {
        tracing::debug!("ucx conn request receiver dropped");
    }
}

extern "C" fn endpoint_error_cb(_arg: *mut c_void, _ep: ucp_ep_h, status: ucs_status_t) {
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

struct SockAddr {
    storage: libc::sockaddr_storage,
    len: libc::socklen_t,
}

impl From<SocketAddr> for SockAddr {
    fn from(addr: SocketAddr) -> Self {
        match addr {
            SocketAddr::V4(v4) => {
                let mut storage: libc::sockaddr_storage = unsafe { mem::zeroed() };
                let sockaddr = libc::sockaddr_in {
                    sin_family: libc::AF_INET as libc::sa_family_t,
                    sin_port: v4.port().to_be(),
                    sin_addr: libc::in_addr {
                        s_addr: u32::from_ne_bytes(v4.ip().octets()),
                    },
                    sin_zero: [0; 8],
                };
                unsafe {
                    ptr::write(
                        (&mut storage as *mut libc::sockaddr_storage).cast::<libc::sockaddr_in>(),
                        sockaddr,
                    );
                }
                Self {
                    storage,
                    len: mem::size_of::<libc::sockaddr_in>() as libc::socklen_t,
                }
            }
            SocketAddr::V6(v6) => {
                let mut storage: libc::sockaddr_storage = unsafe { mem::zeroed() };
                let sockaddr = libc::sockaddr_in6 {
                    sin6_family: libc::AF_INET6 as libc::sa_family_t,
                    sin6_port: v6.port().to_be(),
                    sin6_flowinfo: v6.flowinfo(),
                    sin6_addr: libc::in6_addr {
                        s6_addr: v6.ip().octets(),
                    },
                    sin6_scope_id: v6.scope_id(),
                };
                unsafe {
                    ptr::write(
                        (&mut storage as *mut libc::sockaddr_storage).cast::<libc::sockaddr_in6>(),
                        sockaddr,
                    );
                }
                Self {
                    storage,
                    len: mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t,
                }
            }
        }
    }
}

impl SockAddr {
    fn as_ucs_sock_addr(&self) -> ucs_sock_addr_t {
        ucs_sock_addr_t {
            addr: (&self.storage as *const libc::sockaddr_storage).cast::<libc::sockaddr>()
                as *const _,
            addrlen: self.len as _,
        }
    }
}
