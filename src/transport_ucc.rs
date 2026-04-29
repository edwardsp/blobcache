#![cfg(feature = "ucc")]

use std::ffi::c_void;
use std::mem;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::cluster::Membership;
use crate::error::{BcError, Result};
use crate::ucc_oob::{allgather_via_coordinator, elect_coordinator, OobCoordinator};

#[allow(non_camel_case_types, non_snake_case, non_upper_case_globals, dead_code)]
pub mod sys {
    pub use ucc_sys::*;
}

use sys::*;

const COLLECTIVE_TIMEOUT: Duration = Duration::from_secs(300);
const CREATE_TIMEOUT: Duration = Duration::from_secs(120);
const UCC_CONTEXT_PARAM_FIELD_OOB_MASK: u64 = 1 << 2;
const UCC_TEAM_PARAM_FIELD_OOB_MASK: u64 = 1 << 11;
const UCC_COLL_ARGS_FIELD_FLAGS_MASK: u64 = 1 << 2;
const UCC_COLL_ARGS_FLAG_CONTIG_DST_BUFFER_MASK: u64 = 1 << 2;
const UCC_DT_UINT8_VALUE: ucc_datatype_t = (5u64 << 3) as ucc_datatype_t;

struct SendPtr<T>(*mut T);

unsafe impl<T> Send for SendPtr<T> {}

struct OobBridge {
    rank: u32,
    world: u32,
    coord_url: String,
    http_client: reqwest::Client,
    tokio_handle: tokio::runtime::Handle,
    tag_counter: AtomicU64,
    coord_arc: Arc<OobCoordinator>,
}

struct OobRequest {
    done: AtomicBool,
    ok: AtomicBool,
    tag: String,
    coord: Arc<OobCoordinator>,
}

pub struct UccCollectives {
    rank: u32,
    world: u32,
    lib: ucc_lib_h,
    context: ucc_context_h,
    team: ucc_team_h,
    bridge: Box<OobBridge>,
}

unsafe impl Send for UccCollectives {}
unsafe impl Sync for UccCollectives {}

impl UccCollectives {
    pub fn new(
        rank: u32,
        world: u32,
        membership: Arc<Membership>,
        tokio_handle: tokio::runtime::Handle,
        coordinator: Arc<OobCoordinator>,
    ) -> Result<Arc<Self>> {
        let (coord_url, elected_rank, elected_world) = elect_coordinator(&membership);
        if elected_rank != rank || elected_world != world {
            return Err(BcError::Other(format!(
                "ucc rank mismatch: caller rank/world {rank}/{world}, election {elected_rank}/{elected_world}"
            )));
        }

        let mut lib_config: ucc_lib_config_h = ptr::null_mut();
        check_status(
            "ucc_lib_config_read",
            unsafe { ucc_lib_config_read(ptr::null(), ptr::null(), &mut lib_config) },
        )?;

        let mut lib_params: ucc_lib_params_t = unsafe { mem::zeroed() };
        let mut lib: ucc_lib_h = ptr::null_mut();
        let init_status = unsafe { ucc_init_ffi(&mut lib_params, lib_config, &mut lib) };
        unsafe { ucc_lib_config_release(lib_config) };
        check_status("ucc_init", init_status)?;

        let result = Self::create_after_lib(
            lib,
            rank,
            world,
            coord_url,
            tokio_handle,
            coordinator,
        );
        if result.is_err() {
            unsafe { ucc_finalize(lib) };
        }
        result
    }

    fn create_after_lib(
        lib: ucc_lib_h,
        rank: u32,
        world: u32,
        coord_url: String,
        tokio_handle: tokio::runtime::Handle,
        coordinator: Arc<OobCoordinator>,
    ) -> Result<Arc<Self>> {
        let mut bridge = Box::new(OobBridge {
            rank,
            world,
            coord_url,
            http_client: reqwest::Client::new(),
            tokio_handle,
            tag_counter: AtomicU64::new(1),
            coord_arc: coordinator,
        });

        let bridge_ptr = bridge.as_mut() as *mut OobBridge as *mut c_void;
        let mut ctx_config: ucc_context_config_h = ptr::null_mut();
        check_status(
            "ucc_context_config_read",
            unsafe { ucc_context_config_read(lib, ptr::null(), &mut ctx_config) },
        )?;

        let mut ctx_params: ucc_context_params_t = unsafe { mem::zeroed() };
        ctx_params.mask = UCC_CONTEXT_PARAM_FIELD_OOB_MASK;
        ctx_params.oob = oob_coll(bridge_ptr, rank, world);

        let mut context: ucc_context_h = ptr::null_mut();
        let ctx_status = unsafe {
            ucc_context_create(lib, &mut ctx_params, ctx_config, &mut context)
        };
        unsafe { ucc_context_config_release(ctx_config) };
        check_status("ucc_context_create", ctx_status)?;

        let mut team_params: ucc_team_params_t = unsafe { mem::zeroed() };
        team_params.mask = UCC_TEAM_PARAM_FIELD_OOB_MASK;
        team_params.oob = oob_coll(bridge_ptr, rank, world);

        let mut ctx_array = [context];
        let mut team: ucc_team_h = ptr::null_mut();
        let team_status = unsafe {
            ucc_team_create_post(ctx_array.as_mut_ptr(), 1, &mut team_params, &mut team)
        };
        if let Err(e) = check_status("ucc_team_create_post", team_status) {
            unsafe { ucc_context_destroy(context) };
            return Err(e);
        }
        if let Err(e) = wait_team_create(team, context) {
            unsafe {
                ucc_team_destroy(team);
                ucc_context_destroy(context);
            }
            return Err(e);
        }

        Ok(Arc::new(Self {
            rank,
            world,
            lib,
            context,
            team,
            bridge,
        }))
    }

    pub fn rank(&self) -> u32 {
        self.rank
    }

    pub fn world(&self) -> u32 {
        self.world
    }

    pub fn progress(&self) {
        unsafe {
            ucc_context_progress(self.context);
        }
    }

    pub fn allgatherv(
        &self,
        send: &[u8],
        recv: &mut [u8],
        counts: &[usize],
        displs: &[usize],
    ) -> Result<()> {
        if counts.len() != self.world as usize || displs.len() != self.world as usize {
            return Err(BcError::Other(format!(
                "ucc allgatherv counts/displs len mismatch: {}/{} for world {}",
                counts.len(),
                displs.len(),
                self.world
            )));
        }
        let total = counts
            .iter()
            .try_fold(0usize, |acc, n| acc.checked_add(*n))
            .ok_or_else(|| BcError::Other("ucc allgatherv byte count overflow".into()))?;
        if total > recv.len() {
            return Err(BcError::Other(format!(
                "ucc allgatherv recv too small: total {total} > recv {}",
                recv.len()
            )));
        }
        if counts[self.rank as usize] != send.len() {
            return Err(BcError::Other(format!(
                "ucc allgatherv local send len {} != count {}",
                send.len(),
                counts[self.rank as usize]
            )));
        }

        let counts64 = to_u64_vec("counts", counts)?;
        let displs64 = to_u64_vec("displacements", displs)?;
        let mut coll: ucc_coll_args_t = unsafe { mem::zeroed() };
        coll.mask = UCC_COLL_ARGS_FIELD_FLAGS_MASK;
        coll.flags = UCC_COLL_ARGS_FLAG_CONTIG_DST_BUFFER_MASK;
        coll.coll_type = ucc_coll_type_t_UCC_COLL_TYPE_ALLGATHERV;
        unsafe {
            coll.src.info.buffer = send.as_ptr() as *mut c_void;
            coll.src.info.count = send.len() as u64;
            coll.src.info.datatype = UCC_DT_UINT8_VALUE;
            coll.src.info.mem_type = ucc_memory_type_UCC_MEMORY_TYPE_HOST;
            coll.dst.info_v.buffer = recv.as_mut_ptr() as *mut c_void;
            coll.dst.info_v.counts = counts64.as_ptr() as *mut u64;
            coll.dst.info_v.displacements = displs64.as_ptr() as *mut u64;
            coll.dst.info_v.datatype = UCC_DT_UINT8_VALUE;
            coll.dst.info_v.mem_type = ucc_memory_type_UCC_MEMORY_TYPE_HOST;
        }

        let mut req: ucc_coll_req_h = ptr::null_mut();
        check_status(
            "ucc_collective_init",
            unsafe { ucc_collective_init(&mut coll, &mut req, self.team) },
        )?;

        let post_status = unsafe { ucc_collective_post(req) };
        if !status_is_ok_or_progress(post_status) {
            unsafe { ucc_collective_finalize(req) };
            return Err(status_error("ucc_collective_post", post_status));
        }

        let start = Instant::now();
        loop {
            match unsafe { ucc_collective_test_ffi(req) } {
                s if status_is_ok(s) => {
                    unsafe { ucc_collective_finalize(req) };
                    return Ok(());
                }
                s if status_is_progress(s) => {
                    self.progress();
                    if start.elapsed() > COLLECTIVE_TIMEOUT {
                        unsafe { ucc_collective_finalize(req) };
                        return Err(BcError::Other("ucc allgatherv timed out".into()));
                    }
                    std::thread::sleep(Duration::from_micros(10));
                }
                s => {
                    unsafe { ucc_collective_finalize(req) };
                    return Err(status_error("ucc_collective_test", s));
                }
            }
        }
    }
}

impl Drop for UccCollectives {
    fn drop(&mut self) {
        unsafe {
            if !self.team.is_null() {
                ucc_team_destroy(self.team);
            }
            if !self.context.is_null() {
                ucc_context_destroy(self.context);
            }
            if !self.lib.is_null() {
                ucc_finalize(self.lib);
            }
        }
    }
}

fn oob_coll(coll_info: *mut c_void, rank: u32, world: u32) -> ucc_oob_coll_t {
    ucc_oob_coll_t {
        allgather: Some(oob_allgather),
        req_test: Some(oob_test),
        req_free: Some(oob_free),
        coll_info,
        n_oob_eps: world,
        oob_ep: rank,
    }
}

extern "C" fn oob_allgather(
    src: *mut c_void,
    recv: *mut c_void,
    size: usize,
    coll_info: *mut c_void,
    request: *mut *mut c_void,
) -> ucc_status_t {
    if src.is_null() || recv.is_null() || coll_info.is_null() || request.is_null() {
        return status_no_message();
    }
    let bridge = unsafe { &*(coll_info as *const OobBridge) };
    let tag = bridge.tag_counter.fetch_add(1, Ordering::Relaxed).to_string();
    let payload = unsafe { std::slice::from_raw_parts(src as *const u8, size) }.to_vec();
    let req = Box::new(OobRequest {
        done: AtomicBool::new(false),
        ok: AtomicBool::new(false),
        tag: tag.clone(),
        coord: bridge.coord_arc.clone(),
    });
    let req_ptr = Box::into_raw(req);
    unsafe { *request = req_ptr as *mut c_void };

    let done = SendPtr(unsafe { &raw mut (*req_ptr).done });
    let ok = SendPtr(unsafe { &raw mut (*req_ptr).ok });
    let recv_ptr = SendPtr(recv as *mut u8);
    let done_addr = done.0 as usize;
    let ok_addr = ok.0 as usize;
    let recv_addr = recv_ptr.0 as usize;
    let client = bridge.http_client.clone();
    let coord_url = bridge.coord_url.clone();
    let rank = bridge.rank;
    let world = bridge.world;
    bridge.tokio_handle.spawn(async move {
        let result = allgather_via_coordinator(&client, &coord_url, &tag, rank, world, payload).await;
        match result {
            Ok(bytes) if bytes.len() == size.saturating_mul(world as usize) => {
                unsafe {
                    ptr::copy_nonoverlapping(bytes.as_ptr(), recv_addr as *mut u8, bytes.len());
                }
                unsafe { (*(ok_addr as *mut AtomicBool)).store(true, Ordering::Release) };
            }
            Ok(bytes) => {
                tracing::warn!(got = bytes.len(), want = size * world as usize, "ucc oob allgather size mismatch");
            }
            Err(e) => {
                tracing::warn!(error = %e, "ucc oob allgather failed");
            }
        }
        unsafe { (*(done_addr as *mut AtomicBool)).store(true, Ordering::Release) };
    });
    status_ok()
}

extern "C" fn oob_test(request: *mut c_void) -> ucc_status_t {
    if request.is_null() {
        return status_no_message();
    }
    let req = unsafe { &*(request as *const OobRequest) };
    if !req.done.load(Ordering::Acquire) {
        return status_progress();
    }
    if req.ok.load(Ordering::Acquire) {
        status_ok()
    } else {
        status_no_message()
    }
}

extern "C" fn oob_free(request: *mut c_void) -> ucc_status_t {
    if request.is_null() {
        return status_ok();
    }
    let req = unsafe { Box::from_raw(request as *mut OobRequest) };
    req.coord.release(&req.tag);
    status_ok()
}

fn wait_team_create(team: ucc_team_h, context: ucc_context_h) -> Result<()> {
    let start = Instant::now();
    loop {
        match unsafe { ucc_team_create_test(team) } {
            s if status_is_ok(s) => return Ok(()),
            s if status_is_progress(s) => {
                unsafe { ucc_context_progress(context) };
                if start.elapsed() > CREATE_TIMEOUT {
                    return Err(BcError::Other("ucc team create timed out".into()));
                }
                std::thread::sleep(Duration::from_millis(1));
            }
            s => return Err(status_error("ucc_team_create_test", s)),
        }
    }
}

fn to_u64_vec(name: &str, values: &[usize]) -> Result<Vec<u64>> {
    values
        .iter()
        .map(|v| {
            u64::try_from(*v).map_err(|_| BcError::Other(format!("ucc allgatherv {name} value too large: {v}")))
        })
        .collect()
}

fn check_status(op: &str, status: ucc_status_t) -> Result<()> {
    if status_is_ok(status) {
        Ok(())
    } else {
        Err(status_error(op, status))
    }
}

fn status_error(op: &str, status: ucc_status_t) -> BcError {
    BcError::Other(format!("{op}: UCC status {}", status_to_i32(status)))
}

fn status_is_ok_or_progress(status: ucc_status_t) -> bool {
    status_is_ok(status) || status_is_progress(status)
}

fn status_to_i32(status: ucc_status_t) -> i32 {
    status as i32
}

fn status_is_ok(status: ucc_status_t) -> bool {
    status == status_ok()
}

fn status_is_progress(status: ucc_status_t) -> bool {
    status == status_progress()
}

fn status_ok() -> ucc_status_t {
    ucc_status_t_UCC_OK
}

fn status_progress() -> ucc_status_t {
    ucc_status_t_UCC_INPROGRESS
}

fn status_no_message() -> ucc_status_t {
    ucc_status_t_UCC_ERR_NO_MESSAGE
}

unsafe fn ucc_collective_test_ffi(request: ucc_coll_req_h) -> ucc_status_t {
    unsafe extern "C" {
        fn ucc_collective_test(request: ucc_coll_req_h) -> ucc_status_t;
    }
    unsafe { ucc_collective_test(request) }
}

unsafe fn ucc_init_ffi(
    params: *const ucc_lib_params_t,
    config: ucc_lib_config_h,
    lib: *mut ucc_lib_h,
) -> ucc_status_t {
    unsafe extern "C" {
        fn ucc_init_version(
            api_major_version: ::std::os::raw::c_uint,
            api_minor_version: ::std::os::raw::c_uint,
            params: *const ucc_lib_params_t,
            config: ucc_lib_config_h,
            lib_p: *mut ucc_lib_h,
        ) -> ucc_status_t;
    }
    unsafe { ucc_init_version(UCC_API_MAJOR, UCC_API_MINOR, params, config, lib) }
}
