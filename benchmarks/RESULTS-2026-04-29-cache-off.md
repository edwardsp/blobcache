# Bench: hydrate + 2 sequential reads with `cache_on_peer_fetch=false` + peer LRU

## TL;DR

The first iteration of `cache_on_peer_fetch` (commit `bdb6883`) shipped two
bugs that made the prior version of this document misleading:

1. **Phase B was gated by the flag.** The hydrate broadcast pulled chunks
   off the wire on every receiver and dropped them, so the "11× Phase B
   speedup" was Phase B doing no work. Fixed by forcing
   `pull_chunk_from_peer*` to always cache (it is hydrate, not FUSE).
2. **FUSE re-fetched every chunk ~32×.** FUSE issues 128 KiB sub-reads
   against a 4 MiB chunk; with no disk write and no in-memory fallback
   each sub-read became an independent peer fetch. Fixed by adding a
   parameterized in-memory peer LRU (`peer_lru_bytes`, default 1 GiB)
   consulted before disk in `fetch_chunk_inner` and
   `fetch_chunk_range_inner`, populated from `try_peer_fetch` when
   `cache_on_peer_fetch=false`.

This document is the re-bench on `sha-83924a7-arm64` after both fixes.

## Configuration

| Setting | Value |
|---|---|
| Image | `ghcr.io/edwardsp/blobcache:sha-83924a7-arm64` |
| Pods | 17 (gb300 nodepool) |
| Dataset | `models/nvidia_DeepSeek-R1-0528-NVFP4-v2/` (350 files, 413,340,567,567 B) |
| `chunk_size` / `block_size` | 4 MiB / 4 MiB |
| `workers` | 8 |
| `chunk_concurrency` | 32 |
| `peer_concurrency` | 8 |
| `prefetch_depth` | 0 |
| `prefetch_origin_only` | false |
| **`cache_on_peer_fetch`** | **false** |
| **`peer_lru_bytes`** | **1 073 741 824 (1 GiB)** |
| Transport | RDMA |
| Hydrate mode | broadcast |

## Results

| Step | Wall time (s) | Per-pod elapsed (s) | Throughput |
|---|---|---|---|
| Hydrate Phase A (sharded blob fetch) | 16.26 | — | ~25.4 GB/s aggregate |
| Hydrate Phase B (peer broadcast) | 82.76 | min 54.40 / max 82.68 / mean 77.50 | ~5.00 GB/s/pod recv |
| **Hydrate total** | **99.17** | | |
| Read 1 (every pod reads full 413 GB via FUSE) | 240.03 | min 181.72 / max 240.02 | **29.27 GB/s aggregate, ~1.72 GB/s/pod** |
| Read 2 (immediately after Read 1) | 239.00 | min 165.03 / max 239.00 | **29.40 GB/s aggregate, ~1.73 GB/s/pod** |

Read 2 / Read 1 ratio = 0.996 → essentially identical, as expected: the
broadcast hydrate already populated each pod's full disk cache, so reads
are NVMe-served on hits and only the residual ~3 % miss rate goes over
the wire.

## Cache footprint verification

After hydrate the cache is fully populated cluster-wide (Phase B fix):

```
sample pod cache_bytes:    413 336 373 263  ≈ 413.34 GB  (≈ dataset)
cache_hits_total:        3 247 160
cache_misses_total:         98 808
peer_fetches_ok_total:      92 992
peer_lru_hits_total:            30   (rare; disk hit dominates)
blob_fetches_total:          5 812
```

The peer LRU rarely fires here because the disk is fully primed by Phase B.
The LRU's value shows up in **Phase-A-only / no-broadcast** workloads
where the cluster cache is intentionally disjoint and FUSE sub-reads of a
just-fetched peer chunk would otherwise re-fetch.

## Comparison vs prior runs

| Run | Image | flag | Hydrate total | Read 1 wall | Read 2 wall |
|---|---|---|---|---|---|
| Prior `bdb6883` (broken) | `sha-bdb6883-arm64` | `cache_on_peer_fetch=false` (bugs) | 105.65 s | 1 279.38 s | 1 323.30 s |
| **This run** | `sha-83924a7-arm64` | `cache_on_peer_fetch=false` + 1 GiB LRU | **99.17 s** | **240.03 s** | **239.00 s** |

Read path: **5.3× faster** wall-time (1 279 → 240 s), confirming the 32×
re-fetch amplification is gone. Hydrate Phase B time is comparable
across runs because in the broken run Phase B was skipping the disk
write but still doing the wire transfer; in the fixed run it does both
but at the same network-bound rate.

## Per-pod breakdown

### Step 1 — Hydrate (broadcast, full dataset)
- Phase A elapsed: 16 257 ms
- Phase B elapsed: 82 755 ms
- Per-pod broadcast peer elapsed: min 54 398 ms, max 82 675 ms, mean 77 502 ms
- Total response: 99 170 ms
- Wall window (driver clock): start 1 777 495 872 882, end 1 777 495 973 125 → 100 243 ms

### Step 2 — Read 1
- Wall window: start 1 777 495 973 154, end 1 777 496 213 184 → 240 030 ms
- Per-pod elapsed (sorted ascending):

| Pod (sanitized) | Elapsed (ms) |
|---|---|
| pod-A | 181 719 |
| pod-B | 186 006 |
| pod-C | 190 022 |
| pod-D | 201 003 |
| pod-E | 202 621 |
| pod-F | 203 029 |
| pod-G | 203 062 |
| pod-H | 204 302 |
| pod-I | 204 575 |
| pod-J | 207 251 |
| pod-K | 209 505 |
| pod-L | 209 944 |
| pod-M | 215 163 |
| pod-N | 215 933 |
| pod-O | 217 415 |
| pod-P | 220 055 |
| pod-Q | 240 024 |

Spread = max−min = 58 305 ms.

### Step 3 — Read 2
- Wall window: start 1 777 496 213 209, end 1 777 496 452 213 → 239 004 ms
- Per-pod elapsed (sorted ascending):

| Pod (sanitized) | Elapsed (ms) |
|---|---|
| pod-A | 165 025 |
| pod-R | 171 039 |
| pod-H | 179 538 |
| pod-I | 188 672 |
| pod-J | 197 643 |
| pod-B | 201 588 |
| pod-C | 203 847 |
| pod-L | 205 812 |
| pod-K | 208 538 |
| pod-D | 212 264 |
| pod-F | 212 596 |
| pod-E | 213 705 |
| pod-N | 214 299 |
| pod-O | 218 655 |
| pod-M | 222 022 |
| pod-P | 230 250 |
| pod-Q | 238 995 |

Spread = max−min = 73 970 ms.

## Read-path observations

- Aggregate **29.3 GB/s** sustained across 17 pods (~1.72 GB/s per pod
  via FUSE, dominated by NVMe pread on cache hits).
- Read 1 ≈ Read 2 because the dataset already fully fits the per-pod
  disk cache after broadcast hydrate. There is no warm-up effect to
  observe between passes.
- The peer LRU is wired and counted; with broadcast hydrate it sees only
  ~30 hits/pod over a ~7 TB read (chunks served from peers due to a
  transient miss). Its value emerges in Phase-A-only configurations,
  not measured here.

## Exact timestamps (UTC ms since epoch — for grafana correlation)

```
hydrate_start = 1777495872882
hydrate_end   = 1777495973125
read1_start   = 1777495973154
read1_end     = 1777496213184
read2_start   = 1777496213209
read2_end     = 1777496452213
```

## Artefacts

- Driver: `/tmp/bench-cache-off.sh`
- Helm values: `/tmp/values-bench-cache-off.yaml`
- Raw per-pod logs: `/tmp/bench-cache-off/step{1,2,3}.*`
- Hydrate JSON response: `/tmp/bench-cache-off/step1.json`
- Driver console log: `/tmp/bench-cache-off-83924a7.log`
