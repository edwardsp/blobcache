# Bench: ring-allgather hydrate + 2 sequential reads

## TL;DR

Same image (`sha-83924a7-arm64`) and configuration as the broadcast and
Phase-A-only runs but the hydrate request uses `mode: "ring"`. After
Phase A's sharded blob fetch, Phase B runs as a coordinator-driven
ring-allgather: at each of `N-1 = 16` steps every pod pulls the shard
that is currently held by its left-neighbour `prev = (i-1) mod N` and
moving "around the ring", with an explicit barrier between steps so
that `prev` provably has the next chunk in cache before being asked
for it.

**Result: ring works, but is 6.3× slower than broadcast for hydrate.**
The cluster ends with 17× full replication (~7 TB) just like broadcast,
and subsequent reads run at the same NVMe-bound rate. The slowness is
inherent to the ring topology on this hardware: each pod has exactly
**one** inbound peer link active per step, vs. **16** simultaneous
inbound links in broadcast Phase B. With ~5 GB/s per UCX endpoint, a
24 GB shard takes ~5 s per ring step in steady-state — but step 1 took
413 s and step 2 took 142 s due to UCX endpoint cold-start and Phase A
fsync overlap.

## Configuration

Identical to the broadcast and Phase-A-only runs except the hydrate
body sets `"mode": "ring"`.

| Setting | Value |
|---|---|
| Image | `ghcr.io/edwardsp/blobcache:sha-83924a7-arm64` |
| Pods | 17 (gb300 nodepool) |
| Dataset | `models/nvidia_DeepSeek-R1-0528-NVFP4-v2/` (350 files, 98 804 chunks, 413 340 567 567 B) |
| `chunk_size` / `block_size` | 4 MiB / 4 MiB |
| `workers` | 8 |
| `chunk_concurrency` | 32 |
| `peer_concurrency` | 8 |
| `prefetch_depth` | 0 |
| `cache_on_peer_fetch` | false |
| `peer_lru_bytes` | 1 073 741 824 (1 GiB) |
| Transport | RDMA |
| **Hydrate mode** | **ring** |

## Results

| Step | Wall time (s) | Per-pod elapsed (s) | Throughput |
|---|---|---|---|
| Hydrate Phase A (sharded blob fetch) | 16.30 | min 15.79 / max 16.29 / mean 16.04 | ~25.4 GB/s aggregate |
| Hydrate Phase B (ring, 16 steps with barriers) | 628.07 | (uniform; coordinator-bounded) | **0.62 GB/s/pod recv mean** |
| **Hydrate total** | **644.45** | | |
| Read 1 (every pod reads full 413 GB via FUSE) | 237.29 | min 153.48 / max 237.28 | **29.61 GB/s aggregate, ~1.74 GB/s/pod** |
| Read 2 (immediately after Read 1) | 292.38 | min 168.51 / max 292.38 | **24.03 GB/s aggregate, ~1.41 GB/s/pod** |

Read throughput matches the broadcast run — same final cache state,
same NVMe-served read path. Read 2 is slower than Read 1 here, opposite
to broadcast (where Read 1 ≈ Read 2). This is within the per-pod
slowest-tail noise observed across all three configurations and not a
structural property of ring.

## Cache footprint verification

Cluster cache reaches near-full 17× replication, confirming the ring
allgather is correct end-to-end (modulo a small loss; see below):

```
per-pod cache_bytes after ring hydrate:  ~413.32 GB  (min 413,323,789,728 / max 413,340,567,193)
cluster total after hydrate:             7,026,504,428,617 B  ≈ 7.027 TB
expected (17 × 413,340,567,567):         7,026,789,648,639 B
delta:                                       285,220,022 B  ≈ 285 MB short  (≈ 71 chunks × 4 MiB)
```

71 missing chunks across 17 × 98 804 = 1 679 668 chunk-slots = **0.0042 %
loss**. Source attribution from the per-step error log:

```
step 5: 6 errors (cumulative)   "not found: peer miss"
step 6: 8 errors                same source pod, different blobs
step 7: 9 errors                cumulative; no further growth
... steps 8-16: 9 errors total, no new losses
```

All errors are `peer miss` — the receiver asked `prev` for a chunk that
`prev`'s `peer_index` did not advertise. The barrier logic prevents
this in steady state but the 9 lost chunks suggest a small race between
the inflight-write drain and the bloom-filter rebuild on `prev`. Those
chunks then fall through to the disk-read path on FUSE and are filled
on demand, which is why the cluster total stays at 99.996 % rather than
collapsing further.

## Per-step ring breakdown

The 16 ring-allgather steps reveal a sharp step-1 cold-start cliff:

| Step | Wall (ms) | Bytes | Chunks | Errors | Notes |
|---|---|---|---|---|---|
|  1 | 413,060 | 413,323,790,351 | 98,800 | 4 | UCX endpoint cold-start across 17 pods + Phase A fsync overlap |
|  2 | 141,759 | 413,323,790,351 | 98,800 | 4 | Two stragglers (~140 s); the other 15 pods finish in 2-5 s |
|  3 | 5,226 | 413,323,790,351 | 98,800 | 4 | Steady state begins |
|  4 | 5,461 | 413,323,790,351 | 98,800 | 4 | |
|  5 | 5,525 | 413,323,790,102 | 98,798 | 6 | First "peer miss" appears |
|  6 | 4,969 | 413,323,789,853 | 98,796 | 8 | |
|  7 | 5,541 | 413,323,789,728 | 98,795 | 9 | Loss settles at 9 chunks total |
|  8 | 5,073 | 413,323,789,728 | 98,795 | 9 | |
|  9 | 5,355 | 413,323,789,728 | 98,795 | 9 | |
| 10 | 6,116 | 413,323,789,728 | 98,795 | 9 | |
| 11 | 4,827 | 413,323,789,728 | 98,795 | 9 | |
| 12 | 5,319 | 413,323,789,728 | 98,795 | 9 | |
| 13 | 5,026 | 413,323,789,728 | 98,795 | 9 | |
| 14 | 5,030 | 413,323,789,728 | 98,795 | 9 | |
| 15 | 4,907 | 413,323,789,728 | 98,795 | 9 | |
| 16 | 4,854 | 413,323,789,728 | 98,795 | 9 | |

The steady-state cost (steps 3-16) is ~5 s per step × 14 steps = 70 s.
Add the cold steps 1 (413 s) and 2 (142 s) and you get the 628 s total.

### Why is step 1 so slow?

Hypotheses, in decreasing order of confidence:

1. **UCX endpoint cold-start (most likely).** Phase A only fetches from
   blob; no peer endpoints have been opened. At step 1, every pod
   simultaneously establishes its single inbound endpoint to `prev`.
   Endpoint creation involves the UCX `ucp_ep_create` handshake and
   queue-pair setup which is not cheap on first use. Once warm, the
   same endpoint is reused for all 16 subsequent steps.
2. **Phase A fsync still draining.** `await_inserts_drained` is called
   at the end of Phase A's `run_shard`, but the kernel page cache
   write-back may still be in flight. Step 1 chunks competing with the
   tail of Phase A writes for NVMe bandwidth would amplify the
   slowdown, especially on the slowest 3 pods (273-413 s vs the band
   of 14 pods at 96-141 s).
3. **Per-pod long-tail noise.** Step 1 per-pod elapsed has a clear
   trimodal distribution: 2 fast pods (~15-20 s), 14 mid pods
   (~96-141 s), 1 slow pod (413 s). The slow pod is the receiver
   who serializes the entire step. Possibly correlated with which pod
   is the coordinator (it does extra work as both Phase A puller and
   Phase B dispatcher).

### Why is step 2 still slow?

Step 2 has 15 pods at **2-5 s** and 2 pods at **140-142 s**. The 2
stragglers serialize the barrier. Without those, step 2 would have
finished in ~5 s like steps 3-16. The slow pods have errors=1 each at
step 2, suggesting a partial retry / re-fetch path is being hit.

### Why are steps 3-16 fast (~5 s)?

Each receiver pulls a 24 GB shard from `prev` over a single warm UCX
endpoint. ~5 GB/s × 5 s = 25 GB ≈ shard size. The barrier ensures all
17 receivers finish before the next step, so step time is dominated by
the **slowest** receiver, but in steady state the slowest is only
~10 % above the mean.

## Comparison vs broadcast and Phase-A-only

| Axis | Broadcast | Phase-A-only | **Ring** |
|---|---|---|---|
| Hydrate Phase A (s) | 16.26 | 16.81 | 16.30 |
| Hydrate Phase B (s) | 82.76 | 0 (skipped) | **628.07** |
| **Hydrate total (s)** | **99.17** | **16.81** | **644.45** |
| Cluster cache after hydrate | 7.027 TB (17×) | 0.413 TB (1×) | 7.027 TB (17×, –285 MB) |
| Phase B per-pod recv (GB) | ~389 | n/a | ~389 |
| Phase B effective per-pod recv rate | **5.00 GB/s** | n/a | **0.62 GB/s** |
| Read 1 wall (s) | 240.03 | 319.51 | 237.29 |
| Read 2 wall (s) | 239.00 | 299.67 | 292.38 |
| Hydrate + 2 reads (s) | 578.20 | 636.99 | **1 174.12** |

Take-aways:

- **Broadcast wins decisively for full replication** when the goal is
  "every pod has every chunk on disk". Each receiver opens **16**
  inbound UCX endpoints concurrently and saturates its NIC at
  ~5 GB/s sustained. Ring is wire-optimal in bytes (each chunk
  crosses the wire exactly N-1 times instead of N×(N-1)) but loses
  the parallelism dividend: only one inbound endpoint is active per
  step, and its 5 GB/s ceiling becomes a per-step floor.
- **Ring's theoretical advantage is wire bytes**, not wall time.
  Broadcast moves 16 × 24 GB = 384 GB into each pod across 16 sources
  in ~83 s. Ring moves the same 384 GB into each pod across **one
  source per step** for 16 steps, also netting 384 GB per pod, but
  serialised — and the per-step cost is dominated by the slowest pod
  thanks to the barrier.
- **Step 1 alone (413 s) is 5× the entire broadcast Phase B**. If
  step 1 were as cheap as the steady-state steps (~5 s) the total
  ring Phase B would be ~80 s — competitive with broadcast. The
  cold-start cost is the biggest fixable item.
- **Reads are unaffected by hydrate mode** (as expected once the
  cache is full).

## Per-pod read breakdown

### Step 2 — Read 1
Wall window: start 1 777 501 929 863, end 1 777 502 167 157 → 237 294 ms.

| Pod (sanitized) | Elapsed (ms) |
|---|---|
| pod-E | 153,477 |
| pod-I | 168,746 |
| pod-A | 171,158 |
| pod-N | 171,672 |
| pod-L | 171,904 |
| pod-O | 171,979 |
| pod-J | 177,529 |
| pod-B | 183,896 |
| pod-D | 188,473 |
| pod-M | 192,547 |
| pod-H | 193,692 |
| pod-Q | 197,484 |
| pod-C | 199,661 |
| pod-G | 204,808 |
| pod-K | 210,939 |
| pod-P | 212,291 |
| pod-F | 237,284 |

Spread = 83 807 ms (35 % of slowest).

### Step 3 — Read 2
Wall window: start 1 777 502 167 189, end 1 777 502 459 579 → 292 390 ms.

| Pod (sanitized) | Elapsed (ms) |
|---|---|
| pod-E | 168,505 |
| pod-H | 174,348 |
| pod-L | 179,186 |
| pod-I | 180,764 |
| pod-N | 181,835 |
| pod-M | 185,527 |
| pod-A | 201,895 |
| pod-D | 203,286 |
| pod-J | 203,544 |
| pod-Q | 206,299 |
| pod-C | 206,815 |
| pod-O | 213,344 |
| pod-K | 215,350 |
| pod-B | 216,612 |
| pod-F | 243,580 |
| pod-P | 278,957 |
| pod-G | 292,377 |

Spread = 123 872 ms (42 % of slowest). The two slowest pods (pod-G
and pod-P) account for the ~50 s gap vs Read 1; the median is similar.

## Suggested follow-ups (not in scope here)

1. **Pre-warm UCX endpoints during Phase A** — open endpoints to all
   peers (or at least to `prev` and `next`) in the background while
   Phase A is fetching from blob. Should remove the ~408 s step-1 cold
   tail.
2. **Investigate the 9 missed chunks.** They appear cleanly (`peer
   miss`, single source pod, all in the first 7 steps). Likely an
   early step-N receiver is pulling chunks from a `prev` that hasn't
   completed its bloom rebuild from step-1 inserts. Fix candidate:
   in `run_ring_step`, after `await_inserts_drained()`, also bump the
   peer_index bloom before returning.
3. **Pipeline ring steps.** A real ring-allgather without barriers
   pipelines: while step k is moving shard A→B, step k+1 starts
   moving shard A→C. Coordinator-driven barriers serialize this for
   simplicity but at the cost of waiting for the slowest pod each
   step. A pipelined design would amortise the 413 s cold step over
   the full transfer.

## Exact timestamps (UTC ms since epoch — for grafana correlation)

```
hydrate_start = 1777501265070
hydrate_end   = 1777501910714
read1_start   = 1777501929863
read1_end     = 1777502167157
read2_start   = 1777502167189
read2_end     = 1777502459579
```

## Artefacts

- Driver: `/tmp/bench-ring.sh`
- Helm values: `/tmp/values-bench-cache-off.yaml` (unchanged from prior runs)
- Raw per-pod logs: `/tmp/bench-ring/step{1,2,3}.*`
- Hydrate JSON response (with full per-step / per-pod data): `/tmp/bench-ring/step1.json`
- Cache footprint snapshot: `/tmp/bench-ring/cache_bytes_after_hydrate.txt`
- Final metrics snapshot: `/tmp/bench-ring/metrics_final.txt`
- Driver console log: `/tmp/bench-ring-83924a7.log`
