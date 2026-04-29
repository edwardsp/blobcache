# Bench: hydrate + 2 sequential reads with `cache_on_peer_fetch=false`

## Configuration

| Setting | Value |
|---|---|
| Image | `ghcr.io/edwardsp/blobcache:sha-bdb6883-arm64` |
| Pods | 17 (gb300 nodepool) |
| Dataset | `models/nvidia_DeepSeek-R1-0528-NVFP4-v2/` (350 files, 413,340,567,567 B) |
| `chunk_size` / `block_size` | 4 MiB / 4 MiB |
| `workers` | 8 |
| `chunk_concurrency` | 32 |
| `peer_concurrency` | 8 |
| `prefetch_depth` | 0 |
| `prefetch_origin_only` | false |
| **`cache_on_peer_fetch`** | **false** (new flag) |
| Transport | RDMA |
| Hydrate mode | broadcast |

## Results

| Step | Wall time (s) | Per-pod elapsed (s) | Throughput |
|---|---|---|---|
| Hydrate Phase A (sharded blob fetch) | 17.81 | — | ~23.2 GB/s aggregate |
| Hydrate Phase B (peer broadcast) | 87.70 | min 54.56 / max 87.62 / mean 81.80 | ~4.71 GB/s/pod recv |
| **Hydrate total** | **105.65** | | |
| Read 1 (every pod reads full 413 GB via FUSE) | 1279.38 | min 1007.34 / max 1279.37 | 5.49 GB/s aggregate, ~325 MB/s/pod |
| Read 2 (immediately after Read 1) | 1323.30 | min 1006.03 / max 1323.30 | 5.31 GB/s aggregate, ~318 MB/s/pod |

Read 2 / Read 1 ratio = 1.034 → essentially identical (no caching benefit, as expected with the flag off).

## Cache footprint verification

After Hydrate + Read 1 + Read 2, every pod holds **exactly its sharded slice** (~24.3 GB), confirming `cache_on_peer_fetch=false` works as designed: cache-on-blob-fetch only.

```
per-pod cache_bytes range: 24.30 – 24.34 GB
cluster total: 413.38 GB == hydrate Phase A footprint
```

## Per-pod breakdown

### Step 1 — Hydrate (broadcast, full dataset)
- Phase A elapsed: 17 808 ms
- Phase B elapsed: 87 702 ms
- Per-pod broadcast peer elapsed: min 54 558 ms, max 87 623 ms, mean 81 795 ms
- Total response: 105 653 ms
- Wall window (driver clock): start 1 777 488 335 531, end 1 777 488 442 298 → 106 767 ms (matches)

### Step 2 — Read 1
- Wall window: start 1 777 488 442 328, end 1 777 489 721 711 → 1 279 383 ms
- Per-pod elapsed (sorted ascending):

| Pod (sanitized) | Elapsed (ms) |
|---|---|
| pod-A | 1 007 337 |
| pod-B | 1 031 493 |
| pod-C | 1 034 192 |
| pod-D | 1 120 680 |
| pod-E | 1 232 840 |
| pod-F | 1 246 990 |
| pod-G | 1 248 062 |
| pod-H | 1 251 923 |
| pod-I | 1 251 932 |
| pod-J | 1 255 631 |
| pod-K | 1 258 935 |
| pod-L | 1 260 649 |
| pod-M | 1 265 627 |
| pod-N | 1 268 135 |
| pod-O | 1 268 743 |
| pod-P | 1 269 917 |
| pod-Q | 1 279 374 |

Spread = max−min = 272 037 ms (slowest pod waited 0, fastest pod waited ~272 s at the post-step barrier).

### Step 3 — Read 2
- Wall window: start 1 777 489 721 743, end 1 777 491 045 049 → 1 323 306 ms
- Per-pod elapsed (sorted ascending):

| Pod (sanitized) | Elapsed (ms) |
|---|---|
| pod-A | 1 006 032 |
| pod-B | 1 012 986 |
| pod-C | 1 043 723 |
| pod-D | 1 101 657 |
| pod-E | 1 217 473 |
| pod-F | 1 227 960 |
| pod-G | 1 227 964 |
| pod-H | 1 234 963 |
| pod-I | 1 236 555 |
| pod-J | 1 242 617 |
| pod-K | 1 245 028 |
| pod-L | 1 248 487 |
| pod-M | 1 257 468 |
| pod-N | 1 269 788 |
| pod-O | 1 279 389 |
| pod-P | 1 283 261 |
| pod-Q | 1 323 296 |

Spread = max−min = 317 264 ms.

## Comparison vs prior baseline

| Run | chunk | prefetch | cache-on-peer | Hydrate total |
|---|---|---|---|---|
| `sha-1a659a8` baseline | 64 MiB | 4 | true | ~1 001 s (Phase A 16.5 s + Phase B 984 s) |
| **this run** | **4 MiB** | **0** | **false** | **105.7 s** (Phase A 17.8 s + Phase B 87.7 s) |

Hydrate is **~9.5× faster** end-to-end. Phase A is unchanged (blob throughput identical). Phase B is ~11× faster, almost entirely due to skipping the cache write on the receive path.

## Read path observations

- Reads are NOT bottlenecked by NVMe write (cache off on peer fetch). Per-pod read throughput is ~325 MB/s steady state, dominated by peer-fetch round-trips and `fetcher`/RDMA pipeline cost.
- Same throughput Read 1 vs Read 2 confirms the cache flag is honoured (no warm-cache speedup on rerun).
- Per-pod spread of ~270–320 s on a 21-min step suggests heterogeneous peer-serving load — ~25 % variance.

## Exact timestamps (UTC ms since epoch — for grafana correlation)

```
hydrate_start = 1777488335531
hydrate_end   = 1777488442298
read1_start   = 1777488442328
read1_end     = 1777489721711
read2_start   = 1777489721743
read2_end     = 1777491045049
```

## Artefacts

- Driver: `/tmp/bench-cache-off.sh`
- Helm values: `/tmp/values-bench-cache-off.yaml`
- Raw per-pod logs: `/tmp/bench-cache-off/step{1,2,3}.*`
- Hydrate JSON response: `/tmp/bench-cache-off/step1.json`
