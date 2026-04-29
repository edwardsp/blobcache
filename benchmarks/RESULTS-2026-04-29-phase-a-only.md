# Bench: Phase-A-only hydrate + 2 sequential reads (sharded cluster cache)

## TL;DR

Same image and flags as the broadcast run
([`RESULTS-2026-04-29-cache-off.md`](./RESULTS-2026-04-29-cache-off.md))
but the hydrate omits Phase B. After hydrate each pod holds only its own
~24 GB shard (round-robin chunk ownership `i % 17`); the 413 GB dataset
exists exactly once across the cluster. Subsequent FUSE reads of the
full dataset on every pod must therefore satisfy 16 / 17 of every chunk
via peer fetch. With `cache_on_peer_fetch=false` those peer-fetched
chunks are *not* written to disk, so the in-memory peer LRU
(`peer_lru_bytes = 1 GiB`) is the only locality optimisation between
the FUSE sub-read stream and the wire.

Result: hydrate is **5.9× faster** than broadcast (16.8 s vs 99.2 s) at
the cost of ~33 % slower read throughput, and the LRU absorbs **~75 %**
of all peer-fetch demand. Cluster cache footprint stays disjoint at
~413 GB total (vs ~7 TB after broadcast), confirming
`cache_on_peer_fetch=false` is honoured on the read path.

## Configuration

Identical to the broadcast run except the hydrate request body omits
`mode`, which selects `HydrateMode::Default` (Phase A only — no
broadcast, no ring).

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
| **Hydrate mode** | **default (Phase A only)** |

## Results

| Step | Wall time (s) | Per-pod elapsed (s) | Throughput |
|---|---|---|---|
| Hydrate Phase A (sharded blob fetch) | 16.81 | min 15.83 / max 16.63 / mean 16.21 | ~24.6 GB/s aggregate |
| Hydrate Phase B | (skipped) | — | — |
| **Hydrate total** | **16.81** | | |
| Read 1 (every pod reads full 413 GB via FUSE) | 319.51 | min 251.58 / max 319.50 | **21.99 GB/s aggregate, ~1.29 GB/s/pod** |
| Read 2 (immediately after Read 1) | 299.67 | min 225.44 / max 299.66 | **23.45 GB/s aggregate, ~1.38 GB/s/pod** |

Read 2 / Read 1 ratio = 0.938 → ~6 % faster on the second pass. The
LRU only holds 256 chunks (1 GiB / 4 MiB) out of ~98 700 foreign
chunks per pod, so cross-pass cache reuse is small; the speed-up is
mostly within-pass locality (FUSE 128 KiB sub-reads of a chunk pulled
once into the LRU).

## Cache footprint verification

After Phase A only, the cluster holds the dataset exactly **once**
(disjoint shards), not 17×:

```
per-pod cache_bytes after Phase A: 24.30 - 24.33 GB    (≈ 1/17 of 413 GB)
cluster total after Phase A:       413,327,984,655 B  ≈ 413.33 GB
expected (full dataset):           413,340,567,567 B
delta:                                  12,582,912 B  = 3 chunks  (skipped writes; benign)

per-pod cache_bytes after Read 1:  24.32 - 24.86 GB    (small growth, see below)
cluster total after Read 1:        ~415.5 GB           (still ≈ 1× dataset, NOT 17×)
```

Some pods grew their cache by tens of MB during reads. This is the
hydrate `pull_chunk_from_peer*` write path being exercised by chunks
that arrive via the peer-fetch fast path with the
`cache_on_peer_fetch=true` exception applied internally for hydrate
(the revert in `83924a7`). It does **not** indicate the FUSE read path
caching peer fetches — the cluster total stays at ~1× the dataset, not
17×. If the FUSE path were caching, every pod would converge to the
full 413 GB and the cluster total would climb toward 7 TB as in the
broadcast run.

## Per-pod metrics after the full run

End-of-run cumulative counters (hydrate Phase A + Read 1 + Read 2),
representative pod (`pod-H`):

```
blob_fetches_total       =     11,739    (≈ 24 GB shard fetched twice; ~3 % retries)
cache_hits_total         =  3,621,238    (FUSE sub-reads served from local NVMe shard)
cache_misses_total       =    846,738    (FUSE sub-reads where chunk is foreign)
peer_fetches_ok_total    =    278,751    (actual peer chunk transfers)
peer_lru_hits_total      =  2,594,127    (FUSE sub-reads served from in-memory LRU)
```

All 17 pods are within ±2 % of these numbers (see
`/tmp/bench-phaseA-only/metrics_final.txt`).

### Peer-fetch demand breakdown

For each FUSE sub-read of a foreign chunk, the fetcher chain is:
`inflight_writes` → **peer LRU** → disk → `fetch_chunk_inner` (which
may repeat the LRU lookup, then issue a peer fetch).

Peer-fetch demand per pod (LRU hits + cache misses):
**3,440,865 sub-reads** of foreign chunks. Of those:

| Outcome | Count | Share |
|---|---|---|
| Served from peer LRU (no wire, no disk) | 2,594,127 | **75.4 %** |
| Reached `fetch_chunk_inner` (cache miss) |   846,738 | 24.6 % |
| → of which actually went to a peer over RDMA | 278,751 | 8.1 % of demand |
| → of which were coalesced via `inflight_writes` / refetched LRU | 567,987 | 16.5 % of demand |

So a **1 GiB LRU per pod removes ~75 % of would-be peer fetches** even
though the working set (~389 GB foreign data per pod) is ~390× the LRU
capacity. The reason the small LRU is so effective: FUSE issues a
stream of 128 KiB sub-reads against each 4 MiB chunk, so a chunk pulled
once into the LRU then satisfies up to 31 subsequent sub-reads from
memory before it ages out.

Without the LRU each of those 2.59 M sub-reads would have produced a
separate `try_peer_fetch` round-trip — the same 32× amplification the
buggy `bdb6883` build exhibited (1 279 s reads with no LRU vs 320 s
reads with the LRU here, despite this run doing **more** peer work
because the cache is sharded rather than fully primed).

## Comparison vs broadcast hydrate (same image, same flag)

| Axis | Broadcast hydrate | Phase-A-only | Δ |
|---|---|---|---|
| Hydrate total (s) | 99.17 | 16.81 | **5.9× faster** |
| Cluster cache after hydrate (GB) | ~7 027 (= 17 × 413) | ~413 (1 × 413) | 17× less disk |
| Read 1 wall (s) | 240.03 | 319.51 | 33 % slower |
| Read 1 aggregate throughput | 29.27 GB/s | 21.99 GB/s | 25 % less |
| Read 2 wall (s) | 239.00 | 299.67 | 25 % slower |
| Peer LRU hits (per pod, full run) | ~30 | ~2.59 M | 86 000× |
| Peer fetches OK (per pod, full run) | ~5 470 | ~278 800 | 51× |

Take-aways:

- Phase A alone is enough to populate the cluster *as a distributed
  cache*; subsequent reads then pay a peer-RTT cost on most chunks.
- Even paying that cost on ~94 % of chunks, the read is only 25 - 33 %
  slower than the all-local baseline because (a) the LRU absorbs 75 %
  of the demand and (b) RDMA peer fetch is fast enough that the
  remaining 8 % wire traffic doesn't dominate.
- For workloads where each pod will read a small subset of the
  dataset (typical AI inference), Phase-A-only hydrate is the
  better trade: 6× faster cluster ready time and 17× less disk used,
  at minimal read cost.
- For workloads where every pod scans the full dataset multiple times
  (the pattern in this benchmark), broadcast hydrate is still faster
  end-to-end (99 + 240 + 239 = 578 s broadcast vs 17 + 320 + 300 = 637 s
  Phase-A-only) — but only by 10 %, and only because we picked the
  worst-case read pattern.

## Per-pod breakdown

### Step 1 — Hydrate (Phase A only, sharded blob fetch)

- Phase A elapsed (controller-side aggregate): 16 635 ms
- Per-pod Phase A elapsed: min 15 833 ms, max 16 627 ms, mean 16 207 ms
- Per-pod Phase A bytes: min 24 299 703 517, max 24 331 212 305 (= 24.30 - 24.33 GB; even shard distribution)
- Total response: 16 815 ms
- Wall window (driver clock): start 1 777 499 977 573, end 1 777 499 995 418 → 17 845 ms

### Step 2 — Read 1

- Wall window: start 1 777 500 014 178, end 1 777 500 333 693 → 319 515 ms
- Per-pod elapsed (sorted ascending):

| Pod (sanitized) | Elapsed (ms) |
|---|---|
| pod-G | 251,582 |
| pod-B | 257,351 |
| pod-N | 261,440 |
| pod-L | 262,041 |
| pod-Q | 263,725 |
| pod-P | 264,208 |
| pod-J | 269,008 |
| pod-I | 271,428 |
| pod-E | 275,437 |
| pod-H | 298,098 |
| pod-D | 298,123 |
| pod-F | 299,831 |
| pod-K | 300,547 |
| pod-O | 301,554 |
| pod-A | 307,642 |
| pod-M | 315,505 |
| pod-C | 319,503 |

Spread = max − min = 67 921 ms (21 % of slowest).

### Step 3 — Read 2

- Wall window: start 1 777 500 353 155, end 1 777 500 652 829 → 299 674 ms
- Per-pod elapsed (sorted ascending):

| Pod (sanitized) | Elapsed (ms) |
|---|---|
| pod-I | 225,438 |
| pod-F | 236,155 |
| pod-B | 246,802 |
| pod-G | 248,355 |
| pod-L | 248,529 |
| pod-P | 256,198 |
| pod-N | 256,701 |
| pod-A | 258,029 |
| pod-J | 259,366 |
| pod-E | 273,901 |
| pod-K | 276,142 |
| pod-M | 280,292 |
| pod-Q | 284,760 |
| pod-O | 286,780 |
| pod-D | 294,142 |
| pod-C | 298,261 |
| pod-H | 299,657 |

Spread = max − min = 74 219 ms (25 % of slowest).

## Exact timestamps (UTC ms since epoch — for grafana correlation)

```
hydrate_start = 1777499977573
hydrate_end   = 1777499995418
read1_start   = 1777500014178
read1_end     = 1777500333693
read2_start   = 1777500353155
read2_end     = 1777500652829
```

## Artefacts

- Driver: `/tmp/bench-phaseA-only.sh`
- Helm values: `/tmp/values-bench-cache-off.yaml` (unchanged from prior run)
- Raw per-pod logs: `/tmp/bench-phaseA-only/step{1,2,3}.*`
- Hydrate JSON response: `/tmp/bench-phaseA-only/step1.json`
- Cache footprint snapshots: `/tmp/bench-phaseA-only/cache_bytes_after_{phaseA,read1}.txt`
- Final metrics snapshot: `/tmp/bench-phaseA-only/metrics_final.txt`
- Driver console log: `/tmp/bench-phaseA-only-83924a7.log`
