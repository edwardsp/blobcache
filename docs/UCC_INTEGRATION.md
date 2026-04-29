# UCC FFI Integration Guide for blobcache

## 1. OOB Exchange ABI (Out-of-Band Collective)

### Callback Signatures

**Source**: `/tmp/ucc/src/ucc/api/ucc.h`

```c
typedef struct ucc_oob_coll {
    // Allgather callback: synchronous or async (returns request handle)
    ucc_status_t (*allgather)(void *src_buf, void *recv_buf, size_t size,
                              void *allgather_info, void **request);
    
    // Test callback: poll for completion (non-blocking)
    ucc_status_t (*req_test)(void *request);
    
    // Free callback: release request resources
    ucc_status_t (*req_free)(void *request);
    
    // Opaque context passed to callbacks (e.g., MPI_Comm, your gossip handle)
    void *coll_info;
    
    // Total number of endpoints in the collective
    uint32_t n_oob_eps;
    
    // This rank's position in [0, n_oob_eps)
    uint32_t oob_ep;
} ucc_oob_coll_t;

typedef ucc_oob_coll_t ucc_context_oob_coll_t;
typedef ucc_oob_coll_t ucc_team_oob_coll_t;
```

### Key Contract

- **`allgather(src_buf, recv_buf, size, coll_info, &request)`**:
  - Initiates an allgather: each rank sends `size` bytes from `src_buf`
  - Rank `i` receives at offset `i * size` in `recv_buf`
  - Returns `UCC_OK` if request is posted (async), or `UCC_INPROGRESS` if deferred
  - `*request` is an opaque handle for polling; can be NULL if synchronous
  - **Synchronization**: UCC expects **non-blocking** semantics; you poll via `req_test()`

- **`req_test(request)`**:
  - Returns `UCC_OK` when allgather is complete
  - Returns `UCC_INPROGRESS` if still in flight
  - Returns error code on failure (e.g., `UCC_ERR_NO_MESSAGE` if peer unreachable)
  - Called repeatedly by UCC until completion

- **`req_free(request)`**:
  - Called after `req_test()` returns `UCC_OK`
  - Frees the request handle; no further calls to `req_test()` on this handle

### No Barrier Callback Required

UCC does **not** require a separate barrier callback. The allgather itself is the synchronization point.

---

## 2. UCC + Existing UCX Context Reuse

### OpenMPI Pattern (Reference)

**Source**: `/tmp/ompi/ompi/mca/coll/ucc/coll_ucc_module.c:303-309`

```c
// OpenMPI creates a fresh UCC context with OOB callbacks
ctx_params.mask             = UCC_CONTEXT_PARAM_FIELD_OOB;
ctx_params.oob.allgather    = oob_allgather;
ctx_params.oob.req_test     = oob_allgather_test;
ctx_params.oob.req_free     = oob_allgather_free;
ctx_params.oob.coll_info    = (void*)comm;  // MPI_Comm
ctx_params.oob.n_oob_eps    = ompi_comm_size(comm);
ctx_params.oob.oob_ep       = ompi_comm_rank(comm);

// Then create context
ucc_context_create(cm->ucc_lib, &ctx_params, ctx_config, &cm->ucc_context);
```

### For blobcache: Reuse Existing UCX Worker

**Key Finding**: UCC does **not** expose a direct API to register an existing `ucp_context_h` / `ucp_worker_h`. Instead:

1. **Option A (Recommended)**: Let UCC create its own UCX context internally
   - Set `UCC_CONTEXT_PARAM_FIELD_OOB` only
   - UCC will auto-initialize UCX via its TL (transport layer)
   - Your existing `transport_ucx.rs` UCX worker remains separate for peer-to-peer RDMA

2. **Option B (Advanced)**: Use UCC's UCX transport layer config
   - UCC v1.7.0 supports `UCC_CONTEXT_PARAM_FIELD_TYPE` to specify context type
   - But there's no public API to inject an existing `ucp_context_h`
   - **Verdict**: Not recommended; adds complexity without benefit

### Concrete Rust Binding Pattern

```rust
// In ucc-sys/src/lib.rs (after bindgen)
pub struct UccContextParams {
    pub mask: u64,
    pub oob: ucc_oob_coll_t,
    // ... other fields
}

// In your code:
let mut ctx_params: ucc_context_params_t = unsafe { mem::zeroed() };
ctx_params.mask = UCC_CONTEXT_PARAM_FIELD_OOB;
ctx_params.oob = ucc_oob_coll_t {
    allgather: Some(my_allgather_cb),
    req_test: Some(my_req_test_cb),
    req_free: Some(my_req_free_cb),
    coll_info: my_gossip_handle as *mut c_void,
    n_oob_eps: cluster_size as u32,
    oob_ep: my_rank as u32,
};

unsafe {
    ucc_context_create(
        ucc_lib,
        &ctx_params,
        ctx_config,
        &mut ucc_context,
    )
}
```

---

## 3. Allgatherv vs Broadcast

### Why Allgatherv for Hydrate

**Source**: `/tmp/ucc/test/gtest/coll/test_allgatherv.cc:21-62`

Your use case: **variable per-rank chunk counts** (HRW-hashed, unequal distribution).

```c
// Each rank has a different send count
size_t my_count = (nprocs - rank) * count;  // Rank 0 sends most, rank N-1 sends least

// Receive buffer has variable displacements per rank
int counts[nprocs];      // How many elements each rank sends
int displacements[nprocs]; // Where each rank's data goes in recv_buf

for (int i = 0; i < nprocs; i++) {
    counts[i] = (nprocs - i) * count;
    displacements[i] = cumulative_offset;
    cumulative_offset += counts[i];
}
```

**Allgatherv** is the right choice because:
- `UCC_COLL_TYPE_ALLGATHER` requires all ranks to send the same count
- `UCC_COLL_TYPE_ALLGATHERV` allows variable counts per rank ✓

### Allgatherv Setup

**Source**: `/tmp/ompi/ompi/mca/coll/ucc/coll_ucc_allgatherv.c:44-61`

```c
ucc_coll_args_t coll = {
    .mask      = UCC_COLL_ARGS_FIELD_FLAGS,
    .flags     = UCC_COLL_ARGS_FLAG_CONTIG_DST_BUFFER,  // or 0 for non-contig
    .coll_type = UCC_COLL_TYPE_ALLGATHERV,
    
    // Send buffer (same for all ranks)
    .src.info = {
        .buffer   = my_send_buf,
        .count    = my_send_count,
        .datatype = UCC_DT_UINT8,  // or your datatype
        .mem_type = UCC_MEMORY_TYPE_HOST,
    },
    
    // Receive buffer (variable per rank)
    .dst.info_v = {
        .buffer        = recv_buf,
        .counts        = counts_array,        // [count_rank0, count_rank1, ...]
        .displacements = displacements_array, // [disp_rank0, disp_rank1, ...]
        .datatype      = UCC_DT_UINT8,
        .mem_type      = UCC_MEMORY_TYPE_HOST,
    }
};

ucc_collective_init(&coll, &req, team);
ucc_collective_post(req);
```

### Flags

- `UCC_COLL_ARGS_FLAG_CONTIG_DST_BUFFER`: Set if displacements are contiguous (no gaps)
- `UCC_COLL_ARGS_FLAG_IN_PLACE`: If send buffer is at `recv_buf[my_rank_disp]`
- `UCC_COLL_ARGS_FLAG_COUNT_64BIT`: If counts/displacements are `uint64_t` (not `uint32_t`)

---

## 4. Chunked Pipelining

### Problem

413 GB dataset, 16 pods → 26 GB per pod receive buffer. Allgatherv in one shot is feasible but:
- Ties up memory for the duration
- Single collective latency dominates if any rank is slow

### Solution: Batch Allgatherv per Chunk Batch

**Pattern** (from UCC design):

```c
// Pseudocode: pipeline 64 chunks at a time
for (batch_idx = 0; batch_idx < total_chunks; batch_idx += BATCH_SIZE) {
    // Prepare counts/displacements for this batch
    for (int i = 0; i < nprocs; i++) {
        counts[i] = chunks_per_rank_in_batch[i];
        displacements[i] = cumulative;
        cumulative += counts[i];
    }
    
    // Post allgatherv for this batch
    ucc_coll_args_t coll = {
        .coll_type = UCC_COLL_TYPE_ALLGATHERV,
        .src.info.buffer = my_batch_send_buf,
        .src.info.count = my_batch_count,
        .dst.info_v.buffer = batch_recv_buf,
        .dst.info_v.counts = counts,
        .dst.info_v.displacements = displacements,
    };
    
    ucc_collective_init(&coll, &req, team);
    ucc_collective_post(req);
    
    // Poll until complete
    while (ucc_collective_test(req) == UCC_INPROGRESS) {
        ucc_context_progress(ctx);  // Drive progress
    }
    
    ucc_collective_finalize(req);
}
```

### No Built-in Pipelining Flag

UCC does **not** have a `pipelined_size` flag for automatic chunking. You must:
1. Manually split the dataset into batches
2. Post separate allgatherv collectives per batch
3. Poll each to completion before posting the next

**Recommended batch size**: 64 chunks = 256 MiB (if 4 MiB chunks)
- Balances memory footprint vs. collective overhead
- Allows progress on slow ranks without blocking others

---

## 5. Failure Modes & Error Mapping

### UCC Status Codes

**Source**: `/tmp/ucc/src/ucc/api/ucc_status.h`

```c
typedef enum {
    UCC_OK                    =    0,  // Success
    UCC_INPROGRESS            =    1,  // Operation in flight
    UCC_ERR_NOT_SUPPORTED     =   -1,  // Collective not supported
    UCC_ERR_NOT_IMPLEMENTED   =   -2,  // Feature not implemented
    UCC_ERR_INVALID_PARAM     =   -3,  // Invalid argument
    UCC_ERR_NO_MEMORY         =   -4,  // Allocation failed
    UCC_ERR_NO_RESOURCE       =   -5,  // Resource exhausted
    UCC_ERR_NO_MESSAGE        =   -6,  // Generic error (OOB callback failure)
    UCC_ERR_NOT_FOUND         =   -7,  // Peer not found
    UCC_ERR_TIMED_OUT         =   -8,  // Timeout (if UCC_COLL_ARGS_FLAG_TIMEOUT set)
    UCC_ERR_IO_ERROR          =   -9,  // I/O error
    UCC_ERR_LAST              = -100,
} ucc_status_t;
```

### Specific Failure Scenarios

#### (a) One Peer Unreachable

**Symptom**: OOB allgather callback returns `UCC_ERR_NO_MESSAGE`

```c
// In your oob_allgather_test():
if (peer_unreachable) {
    return UCC_ERR_NO_MESSAGE;  // UCC will propagate this
}
```

**Handling**:
```rust
match ucc_collective_test(req) {
    UCC_OK => { /* done */ },
    UCC_INPROGRESS => { /* retry */ },
    UCC_ERR_NO_MESSAGE => {
        // Peer unreachable; fallback to TCP or mark peer dead
        return Err(BcError::Peer("OOB allgather failed: peer unreachable".into()));
    },
    other => return Err(BcError::Peer(format!("UCC error: {:?}", other))),
}
```

#### (b) Team OOB Callback Returns Error

**Symptom**: `ucc_team_create_post()` or `ucc_collective_post()` fails

```c
// If your allgather callback returns UCC_ERR_NO_MESSAGE:
ucc_status_t status = ucc_collective_post(req);
if (status != UCC_OK && status != UCC_INPROGRESS) {
    // OOB callback failed during team creation
    return status;
}
```

**Mapping**:
```rust
pub enum UccError {
    OobCallbackFailed(String),
    PeerUnreachable(String),
    InvalidParam(String),
    NoMemory,
    Timeout,
    Other(i32),
}

impl From<ucc_status_t> for UccError {
    fn from(status: ucc_status_t) -> Self {
        match status {
            UCC_OK => panic!("not an error"),
            UCC_INPROGRESS => panic!("not an error"),
            UCC_ERR_NO_MESSAGE => UccError::OobCallbackFailed("...".into()),
            UCC_ERR_NOT_FOUND => UccError::PeerUnreachable("...".into()),
            UCC_ERR_INVALID_PARAM => UccError::InvalidParam("...".into()),
            UCC_ERR_NO_MEMORY => UccError::NoMemory,
            UCC_ERR_TIMED_OUT => UccError::Timeout,
            other => UccError::Other(other as i32),
        }
    }
}
```

#### (c) `ucc_collective_post()` Called Before Previous `ucc_collective_test()` Returned UCC_OK

**Symptom**: Undefined behavior; UCC does not support concurrent collectives on the same team

**Prevention**:
```rust
// WRONG: Don't do this
ucc_collective_post(req1);
ucc_collective_post(req2);  // ❌ Undefined behavior

// RIGHT: Poll to completion first
ucc_collective_post(req1);
while ucc_collective_test(req1) == UCC_INPROGRESS {
    ucc_context_progress(ctx);
}
ucc_collective_finalize(req1);

// Now safe to post next
ucc_collective_post(req2);
```

**Error Handling**:
```rust
pub fn post_allgatherv_batch(
    team: ucc_team_h,
    ctx: ucc_context_h,
    args: &ucc_coll_args_t,
) -> Result<ucc_coll_req_h> {
    let mut req: ucc_coll_req_h = ptr::null_mut();
    
    // Initialize
    let status = unsafe { ucc_collective_init(args, &mut req, team) };
    if status != UCC_OK {
        return Err(UccError::from(status).into());
    }
    
    // Post
    let status = unsafe { ucc_collective_post(req) };
    if status != UCC_OK && status != UCC_INPROGRESS {
        unsafe { ucc_collective_finalize(req) };
        return Err(UccError::from(status).into());
    }
    
    Ok(req)
}

pub fn wait_allgatherv(req: ucc_coll_req_h, ctx: ucc_context_h) -> Result<()> {
    loop {
        let status = unsafe { ucc_collective_test(req) };
        match status {
            UCC_OK => {
                unsafe { ucc_collective_finalize(req) };
                return Ok(());
            },
            UCC_INPROGRESS => {
                unsafe { ucc_context_progress(ctx) };
            },
            other => {
                unsafe { ucc_collective_finalize(req) };
                return Err(UccError::from(other).into());
            }
        }
    }
}
```

---

## 6. Concrete Rust Skeleton

### OOB Callbacks

```rust
use std::os::raw::c_void;
use ucc_sys::*;

// Your gossip/cluster handle
struct GossipHandle {
    // ... cluster membership, peer addresses, etc.
}

// OOB allgather: initiate async allgather via gossip
extern "C" fn oob_allgather(
    src_buf: *mut c_void,
    recv_buf: *mut c_void,
    size: usize,
    coll_info: *mut c_void,
    request: *mut *mut c_void,
) -> ucc_status_t {
    let gossip = unsafe { &*(coll_info as *const GossipHandle) };
    
    // Initiate allgather via your gossip protocol
    let req = match gossip.initiate_allgather(src_buf, recv_buf, size) {
        Ok(r) => r,
        Err(_) => return UCC_ERR_NO_MESSAGE,
    };
    
    unsafe { *request = Box::into_raw(Box::new(req)) as *mut c_void };
    UCC_OK
}

// OOB test: poll for completion
extern "C" fn oob_req_test(request: *mut c_void) -> ucc_status_t {
    let req = unsafe { &*(request as *const GossipRequest) };
    
    match req.poll() {
        Ok(true) => UCC_OK,
        Ok(false) => UCC_INPROGRESS,
        Err(_) => UCC_ERR_NO_MESSAGE,
    }
}

// OOB free: release request
extern "C" fn oob_req_free(request: *mut c_void) -> ucc_status_t {
    unsafe { drop(Box::from_raw(request as *mut GossipRequest)) };
    UCC_OK
}
```

### Context Creation

```rust
pub fn create_ucc_context(
    ucc_lib: ucc_lib_h,
    gossip: &GossipHandle,
    cluster_size: usize,
    my_rank: usize,
) -> Result<ucc_context_h> {
    let mut ctx_params: ucc_context_params_t = unsafe { mem::zeroed() };
    ctx_params.mask = UCC_CONTEXT_PARAM_FIELD_OOB;
    ctx_params.oob = ucc_oob_coll_t {
        allgather: Some(oob_allgather),
        req_test: Some(oob_req_test),
        req_free: Some(oob_req_free),
        coll_info: gossip as *const _ as *mut c_void,
        n_oob_eps: cluster_size as u32,
        oob_ep: my_rank as u32,
    };
    
    let mut ctx_config: ucc_context_config_h = ptr::null_mut();
    let status = unsafe { ucc_context_config_read(ucc_lib, ptr::null(), &mut ctx_config) };
    if status != UCC_OK {
        return Err(UccError::from(status).into());
    }
    
    let mut ctx: ucc_context_h = ptr::null_mut();
    let status = unsafe {
        ucc_context_create(ucc_lib, &ctx_params, ctx_config, &mut ctx)
    };
    unsafe { ucc_context_config_release(ctx_config) };
    
    if status != UCC_OK {
        return Err(UccError::from(status).into());
    }
    
    Ok(ctx)
}
```

### Allgatherv Collective

```rust
pub fn run_allgatherv_batch(
    team: ucc_team_h,
    ctx: ucc_context_h,
    my_send_buf: *const u8,
    my_send_count: usize,
    recv_buf: *mut u8,
    counts: &[u32],
    displacements: &[u32],
) -> Result<()> {
    let mut coll: ucc_coll_args_t = unsafe { mem::zeroed() };
    coll.mask = UCC_COLL_ARGS_FIELD_FLAGS;
    coll.flags = 0;  // or UCC_COLL_ARGS_FLAG_CONTIG_DST_BUFFER if contiguous
    coll.coll_type = UCC_COLL_TYPE_ALLGATHERV;
    
    coll.src.info.buffer = my_send_buf as *mut c_void;
    coll.src.info.count = my_send_count as u64;
    coll.src.info.datatype = UCC_DT_UINT8;
    coll.src.info.mem_type = UCC_MEMORY_TYPE_HOST;
    
    coll.dst.info_v.buffer = recv_buf as *mut c_void;
    coll.dst.info_v.counts = counts.as_ptr() as *mut u32;
    coll.dst.info_v.displacements = displacements.as_ptr() as *mut u32;
    coll.dst.info_v.datatype = UCC_DT_UINT8;
    coll.dst.info_v.mem_type = UCC_MEMORY_TYPE_HOST;
    
    let mut req: ucc_coll_req_h = ptr::null_mut();
    let status = unsafe { ucc_collective_init(&coll, &mut req, team) };
    if status != UCC_OK {
        return Err(UccError::from(status).into());
    }
    
    let status = unsafe { ucc_collective_post(req) };
    if status != UCC_OK && status != UCC_INPROGRESS {
        unsafe { ucc_collective_finalize(req) };
        return Err(UccError::from(status).into());
    }
    
    // Poll to completion
    loop {
        let status = unsafe { ucc_collective_test(req) };
        match status {
            UCC_OK => {
                unsafe { ucc_collective_finalize(req) };
                return Ok(());
            },
            UCC_INPROGRESS => {
                unsafe { ucc_context_progress(ctx) };
            },
            other => {
                unsafe { ucc_collective_finalize(req) };
                return Err(UccError::from(other).into());
            }
        }
    }
}
```

---

## 7. Key Takeaways

1. **OOB is non-blocking**: Your callbacks must return immediately; UCC polls via `req_test()`
2. **No existing UCX reuse**: Let UCC create its own UCX context; your peer transport stays separate
3. **Allgatherv for variable counts**: Use `UCC_COLL_TYPE_ALLGATHERV` with `counts[]` and `displacements[]`
4. **Pipeline in batches**: No built-in pipelining; manually split into 64-chunk batches
5. **Error handling**: Map `UCC_ERR_NO_MESSAGE` to peer unreachable; poll until `UCC_OK` before posting next collective
6. **Single-threaded progress**: Call `ucc_context_progress()` in your UCX progress loop; UCC is not thread-safe

---

## References

- UCC API: `/tmp/ucc/src/ucc/api/ucc.h`
- OpenMPI Integration: `/tmp/ompi/ompi/mca/coll/ucc/coll_ucc_module.c`
- Allgatherv Test: `/tmp/ucc/test/gtest/coll/test_allgatherv.cc`
- Status Codes: `/tmp/ucc/src/ucc/api/ucc_status.h`
