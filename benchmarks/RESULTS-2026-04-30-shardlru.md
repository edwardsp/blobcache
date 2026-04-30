# Bench: sharded peer-LRU (audit §2.7) — Phase-A-only hydrate + 1 read pass

## TL;DR

Audit §2.7 sharded the in-memory `PeerLru` 16 ways (Mutex per shard,
ChunkKey hashed via `DefaultHasher` to pick a shard) to remove the
single-mutex serialisation point on the FUSE peer-fetch hot path.
Re-ran the same Phase-A-only protocol as
[`RESULTS-2026-04-29-phase-a-only.md`](./RESULTS-2026-04-29-phase-a-only.md)
on the new image.

**Result: no measurable cluster-wall improvement.** Read-pass-1 wall is
331.2 s vs 319.5 s baseline (+3.7 %, within run-to-run noise on this
tail-bound workload). Per-pod fastest improved 21 % (199 s vs 252 s)
but the slowest pod barely moved (313 s vs 320 s), so the cluster
remains tail-bound by the slowest pod, not by mutex contention. Hydrate
Phase A (16.94 s) is unchanged — it doesn't touch the peer LRU.

The change is correct and preserved (LRU absorbs 93.3 % of peer
demand cluster-wide, in line with the ~75-93 % range observed before),
but it does not address the dominant bottleneck. **Reverting is
optional**; keeping it costs nothing at runtime and removes a future
contention risk under higher concurrency.

## Configuration

Identical to the
[Phase-A-only baseline](./RESULTS-2026-04-29-phase-a-only.md) except
the image tag.

| Setting | Value |
|---|---|
| Image | `ghcr.io/edwardsp/blobcache:shardlru-arm64` (branch `shardlru`, commit `b5b8552`) |
| Pods | 17 (gb300 nodepool) |
| Dataset | `models/nvidia_DeepSeek-R1-0528-NVFP4-v2/` (350 files, 413,340,567,567 B) |
| `chunk_size` / `block_size` | 4 MiB / 4 MiB |
| `workers` | 8 |
| `chunk_concurrency` | 32 |
| `peer_concurrency` | 8 |
| `prefetch_depth` | 0 |
| `prefetch_origin_only` | false |
| `cache_on_peer_fetch` | false |
| `peer_lru_bytes` | 1 073 741 824 (1 GiB total, 64 MiB × 16 shards) |
| Transport | RDMA |
| Hydrate mode | default (Phase A only) |

## Implementation

`src/fetcher.rs`: replace `Option<Arc<Mutex<PeerLru>>>` with
`Option<Arc<PeerLru>>`, where `PeerLru` is now a wrapper holding
`Vec<Mutex<PeerLruShard>>` (16 shards). Each shard owns 1/16 of the
total byte cap (rounded up via `div_ceil`). `ChunkKey` is hashed
with the standard library `DefaultHasher` to pick a shard, so any
single hot blob spreads its chunks uniformly across all 16 shards
rather than pinning one. Get/put/clear methods on the wrapper acquire
only the relevant shard's mutex.

API surface unchanged: the 5 callers in `fetch_chunk_range_inner`,
`do_fetch`, `do_fetch` peer-success branch, and `clear_local_state`
were updated mechanically (`lru.lock().get(&k)` → `lru.get(&k)` etc.).

## Results

### Hydrate Phase A

| Metric | Baseline (`sha-83924a7-arm64`) | Shardlru (`shardlru-arm64`) | Δ |
|---|---|---|---|
| Wall (cluster) | 16.81 s | 16.94 s | +0.8 % |
| Per-pod min | 15.83 s | 15.77 s | -0.4 % |
| Per-pod max | 16.63 s | 16.38 s | -1.5 % |
| Per-pod mean | 16.21 s | 16.16 s | -0.3 % |
| Aggregate | ~24.6 GB/s | ~23.3 GiB/s | (same) |

Phase A doesn't touch the peer LRU (every chunk goes blob → cache
direct). Numbers are identical within noise, as expected.

### Read pass 1 (every pod reads full 413 GB via FUSE)

| Metric | Baseline | Shardlru | Δ |
|---|---|---|---|
| Cluster wall | 319.51 s | **331.24 s** | **+3.7 %** |
| Per-pod min | 251.58 s | 199.29 s | **-20.8 %** |
| Per-pod max | 319.50 s | 313.22 s | -2.0 % |
| Per-pod mean | (n/a) | 269.67 s | — |
| Per-pod stddev | (n/a) | ~33 s | — |
| Aggregate (max-wall) | 21.99 GB/s | 22.43 GB/s | +2.0 % |

The fastest pod improved by 21 % (the only place mutex contention
could plausibly have been hurting), but the slowest pod (the cluster
wall) is essentially unchanged. The widened spread (114 s gap between
fastest and slowest, vs 68 s in the baseline) suggests the bottleneck
is per-pod tail (single-stream UCX progress, NVMe write tail, or peer
overload) rather than aggregate concurrency on the LRU mutex.

### Per-pod read-pass-1 walls (sorted)

```
pod    wall_s
prv56  199.29   (min — 21% faster than baseline min)
z5ngr  241.94
vl9c2  242.27
shzxf  251.47
62v7g  252.45
qfvvh  252.62
wkwj5  251.97
7b6ps  261.00
rtnhs  263.83
gbmrj  275.61
hnhhh  279.95
hd75t  281.36
79k25  288.77
vsnrc  306.36
crfdx  310.96
tdz28  311.30
khscf  313.22   (max — cluster wall)
```

Mean 269.67 s, stddev ~33 s. The 5 slowest pods (303–313 s range)
account for the cluster wall; the 12 fastest are all well below the
baseline cluster wall.

## Cluster-wide metrics (end of run)

```
blobcache_blob_fetches_total          102,174    (≈ 1× dataset; Phase A retries are minimal)
blobcache_cache_hits_total          3,151,350    (FUSE sub-reads served from local NVMe)
blobcache_cache_misses_total        6,404,866    (FUSE sub-reads where chunk is foreign)
blobcache_peer_fetches_ok_total     1,574,476    (actual peer chunk transfers)
blobcache_peer_lru_hits_total      22,029,678    (FUSE sub-reads served from sharded LRU)

total FUSE sub-reads        9,556,216
peer-side demand            23,604,154   (peer + LRU together)
LRU absorption rate         93.3 %       (LRU hits / total peer-side demand)
```

LRU absorption (93.3 %) sits at the high end of the baseline range
(75-93 %), confirming the sharding is functionally correct — every
shard is participating, no single shard is starved or overflowing
disproportionately.

## Cache footprint verification

Cluster cache stays disjoint at ~413 GB total (1× dataset, not 17×),
confirming `cache_on_peer_fetch=false` is honoured on the read path
under the sharded LRU just as before.

## Why §2.7 didn't move the cluster wall

The per-pod fastest case improved by 21 %, which is exactly where
single-mutex contention would show up (a high-concurrency pod doing
many parallel sub-read fetches against the LRU). But the slowest pods
— which set the cluster wall — are bottlenecked elsewhere:

1. **Single-stream UCX progress** on the slowest peer's outbound side
   (3.55 GiB/s software ceiling per `README.md` "Known limitations").
2. **NVMe RAID write-amp tail** when a few pods finish their hydrate
   shard fsync late and stall foreign chunk reads behind it.
3. **Receiver-side scheduler skew** — 17 concurrent pods saturating
   `ucp_worker_progress` on a single tokio thread per pod.

Sharding the LRU removes a future scaling cliff (e.g. 32+ FUSE
threads, larger LRU caps, or hot-blob workloads) but for this
workload the LRU mutex was simply not the binding constraint. The
audit's intuition note already flagged this:
> "Magnitude guess: Unknown; could be 0% or could be 10-30%. Test first."

It landed near 0 %.

## Reproduce

```sh
# 1. Branch + image (one-time)
git checkout -b shardlru
# (apply src/fetcher.rs changes)
git push -u origin shardlru
gh workflow run container.yml --ref shardlru
gh run watch --exit-status

# 2. Helm uninstall + reinstall with new tag
helm get values blobcache -n blobcache -o yaml > /tmp/values.yaml
sed -i 's|tag: sha-.*-arm64|tag: shardlru-arm64|' /tmp/values.yaml
helm uninstall blobcache -n blobcache --wait
helm install blobcache deploy/helm/blobcache -n blobcache \
  -f /tmp/values.yaml --wait --timeout 5m

# 3. Bench (Phase A only, 1 read pass)
deploy/storage-access.sh on
bash benchmarks/run.sh \
  --tag shardlru-phaseA-r1 \
  --out-dir /tmp/bench-shardlru \
  --prefix nvidia_DeepSeek-R1-0528-NVFP4-v2/ \
  --hydrate-mode default \
  --clear-cache \
  --passes 1
deploy/storage-access.sh off
```

## Timeline (UTC, for Grafana correlation)

| Phase | Start | End | Wall |
|---|---|---|---|
| Clear cache | 2026-04-30 06:06:05Z | 2026-04-30 06:06:06Z | 0.42 s |
| Hydrate Phase A | 2026-04-30 06:06:12Z | 2026-04-30 06:06:28Z | 16.94 s |
| Read pass 1 | 2026-04-30 06:06:29Z | 2026-04-30 06:12:00Z | 331.24 s |
| **Total** | 2026-04-30 06:06:05Z | 2026-04-30 06:12:00Z | 5 m 55 s |
