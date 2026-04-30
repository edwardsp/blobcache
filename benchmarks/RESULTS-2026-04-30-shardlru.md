# Bench: sharded peer-LRU (audit §2.7) — Phase-A-only hydrate + 1 read pass

## TL;DR

Audit §2.7 sharded the in-memory `PeerLru` 16 ways (Mutex per shard,
ChunkKey hashed via `DefaultHasher` to pick a shard) to remove the
single-mutex serialisation point on the FUSE peer-fetch hot path.
Re-ran the same Phase-A-only protocol as
[`RESULTS-2026-04-29-phase-a-only.md`](./RESULTS-2026-04-29-phase-a-only.md)
on the new image, then tested two follow-up variants and captured a
`perf record` profile against the baseline image to confirm root cause.

**Result: §2.7 was solving a non-problem and should be reverted.**

Five experiments, five-pod-cluster wall (PASS1):

| Run | Image | LRU | peer_concurrency | Hydrate | PASS1 wall |
|---|---|---|---|---|---|
| Baseline (29-Apr) | sha-83924a7-arm64 | 1 GiB | 8 | 16.81 s | **319.5 s** |
| Shardlru | shardlru-arm64 | 1 GiB | 8 | 16.94 s | 331.2 s |
| Shardlru + 8 GiB | shardlru-arm64 | 8 GiB | 8 | 21.23 s | 301.3 s |
| Shardlru + 8 GiB + pc=32 | shardlru-arm64 | 8 GiB | 32 | 21.70 s | 331.2 s |
| **Baseline rerun (30-Apr)** | **sha-83924a7-arm64** | **1 GiB** | **8** | **23.52 s** | **301.2 s** |

The baseline-image rerun lands at exactly the same 301 s as the best
sharded result. Cluster-wall variance run-to-run is ±10 % from
unrelated noise (other tenants on the cluster, NVMe scheduling).
Neither the sharded LRU nor the 8 GiB cap moves the needle.

`perf record` against the baseline (single-mutex) image during PASS1
captured 10,390 samples on `blobcached`. **`parking_lot::Mutex::lock`
does not appear above 0.5 %.** The actual hot symbols:

| % | Symbol | Path |
|---|---|---|
| 21.4 % | `__pi_memcpy_generic` (kernel) | FUSE writeback (`fuse_copy_page` → `writev` from fuser reply) |
| 15.6 % | `__aarch64_swp8_acq_rel` | tokio scheduler atomics (work-stealing) |
| 5.7 % | `__arch_copy_to_user` (kernel) | ext4 `pread64` (chunk read from NVMe) |
| 4.7 % | libc in `ucx-runtime` | UCX progress poll |
| ~12 % | other `__aarch64_*` atomics + `tokio::worker::run` | more tokio scheduler |
| <3 % | all futex/lock/mutex symbols combined | (parking_lot lock_slow effectively absent) |

The audit's hypothesis was wrong. The bottleneck is **byte movement**
(~27 % CPU spent in memcpy across FUSE + ext4) and **tokio scheduler
overhead on aarch64** (~18 % atomics for work-stealing). §2.7 sharded
a lock that wasn't contended.

**Recommendation: revert §2.7.** Real wins live elsewhere — see
"Where the wins actually are" section below.

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

---

# Follow-up experiments (30-Apr afternoon)

After the initial 1 GiB shardlru result above came in flat, three
additional runs were made to (a) test whether a larger LRU absorbed
more peer demand, (b) test whether outbound peer fan-out was the
bottleneck, and (c) capture a `perf` profile against the original
single-mutex baseline image to ground-truth the audit's contention
hypothesis.

## Run 2 — Shardlru + 8 GiB LRU

Same shardlru image, but `peer_lru_bytes=8589934592` (8 GiB total =
512 MiB × 16 shards) instead of 1 GiB. Hypothesis: at 1 GiB cluster-
wide cap (~17 GiB total across pods), the 384 GiB-per-pod working set
fits the LRU only ~5 % of the time, so a larger cap should absorb more
demand.

| Metric | Shardlru 1 GiB | **Shardlru 8 GiB** | Δ |
|---|---|---|---|
| Hydrate Phase A | 16.94 s | 21.23 s | +25 % (suspicious; see below) |
| PASS1 cluster wall | 331.24 s | **301.30 s** | **-9.0 %** |
| PASS1 max pod wall | 313.22 s | 296.13 s | -5.5 % |
| PASS1 mean pod wall | 269.67 s | 237.42 s | -12.0 % |
| Aggregate (max-wall) | 22.43 GB/s | 23.73 GB/s | +5.8 % |

The 9 % cluster-wall improvement initially looked real, but **see Run
4** — the same effect appears in the baseline-image rerun, so it's
run-to-run variance, not a real signal from the larger LRU.

Hydrate +25 % is also noise — Phase A doesn't touch the peer LRU at
all. Re-running on a quiet cluster would be needed to establish a
true variance band; for now, treat ±15 % cluster wall as noise on
this workload.

Timeline UTC: clear 06:21:32→06:21:32 (0.45 s); hydrate 06:21:38→
06:21:59 (21.23 s); PASS1 06:21:59→06:27:01 (301.30 s).

## Run 3 — Shardlru + 8 GiB + peer_concurrency=32

Hypothesis: with 17 pods and 16/17 chunks per pod foreign during
PASS1, a per-pod outbound peer fan-out cap of 8 might be limiting
cluster throughput. Bumped to 32 to match `chunk_concurrency`.

| Metric | Shardlru 8 GiB pc=8 | **Shardlru 8 GiB pc=32** | Δ |
|---|---|---|---|
| Hydrate Phase A | 21.23 s | 21.70 s | +0.5 s (noise) |
| PASS1 cluster wall | 301.30 s | **331.19 s** | **+10 % (worse)** |
| PASS1 max pod wall | 296.13 s | 309.61 s | +4.5 % |
| PASS1 mean pod wall | 237.42 s | 257.40 s | +8.4 % |
| Aggregate | 23.73 GB/s | 22.69 GB/s | -4.4 % |

**Bumping peer_concurrency made things slightly worse**, confirming
the outbound peer fan-out is not the bottleneck — adding more in-flight
peer requests just queues them at the receiving side's
`chunk_concurrency=32` cap and at the single-threaded UCX progress
loop, adding latency without adding bandwidth.

Timeline UTC: clear 06:35:25→06:35:25; hydrate 06:35:31→06:35:53
(21.70 s); PASS1 06:35:25→06:40:56 (331.19 s).

## Run 4 — Baseline image rerun (single-mutex, 1 GiB, pc=8)

Reverted to the original baseline image to (a) capture a `perf`
profile of the contended-LRU codepath and (b) sanity-check the 29-Apr
baseline number against a same-day reading.

| Metric | 29-Apr baseline | **30-Apr baseline rerun** | Δ |
|---|---|---|---|
| Hydrate Phase A | 16.81 s | 23.52 s | +40 % |
| PASS1 cluster wall | 319.51 s | **301.20 s** | **-5.7 %** |
| PASS1 max pod wall | 319.50 s | 290.72 s | -9.0 % |
| PASS1 mean pod wall | (n/a) | 242.51 s | — |
| Aggregate | 21.99 GB/s | 24.17 GB/s | +9.9 % |

**Same-day baseline matches the best shardlru result within noise
(301.20 s vs 301.30 s).** The 9 % "improvement" attributed to either
sharding or the 8 GiB LRU is actually run-to-run variance.

Hydrate spread (16.81 → 23.52 s) on the same image with no code change
is itself a 40 % swing, so the variance band is wide on this cluster
today.

### Per-pod read walls (sorted)

```
pod    wall_s
5pcrr  184.79   (min — better than any shardlru run's min)
flqzf  193.14
ncwqx  214.42
h9425  219.96
zrdkb  224.64
mqq4k  239.58
zlmgr  241.47
cxn5d  245.07
zps4r  246.74
hzk4d  247.40
d5vgw  250.33
9tpk9  250.62
pwj22  257.87
w4mz8  262.14
9qrxd  269.08
hdbc8  284.64
bxn88  290.72   (max — cluster wall)
```

Mean 242.51 s, max-min spread 106 s. The single-mutex baseline has a
*tighter* tail than the sharded variant (290.7 vs 313.2 s max),
further evidence that mutex contention isn't what's setting the wall.

Timeline UTC: clear 06:57:51→06:57:52; hydrate 06:57:58→06:58:17
(23.52 s); PASS1 06:58:17→07:03:19 (301.20 s).

## Run 5 — `perf record` against baseline image

Captured 60 s of stack samples on a single pod (`5pcrr`) during PASS1
on the baseline (single-mutex) image. 99 Hz, frequency-sampled, with
DWARF callgraphs, `linux-tools-6.8.0-110-generic/perf` from
ubuntu-noble.

```sh
PERF=/usr/lib/linux-tools/6.8.0-110-generic/perf
$PERF record -F 99 -g -p $(pidof blobcached) \
  -o /tmp/perf-baseline.data -- sleep 60
$PERF report -i /tmp/perf-baseline.data --stdio --no-children -g none
# 10,390 samples / 60 s = ~173 samples/sec on the blobcached PID
```

### Top symbols (flat, no callgraph) — `≥0.3 %`

| % | Command | Where | Symbol | Interpretation |
|---|---|---|---|---|
| 21.4 | blobcached | kernel | `__pi_memcpy_generic` | FUSE writeback memcpy from blobcached to FUSE kernel buffer (call chain: `fuse_copy_page` ← `fuse_dev_write` ← fuser `reply::send`) |
| 15.6 | blobcached | blobcached | `__aarch64_swp8_acq_rel` | Atomic 8-byte swap (work-stealing queue ops in tokio scheduler `worker::run`) |
| 5.7 | tokio-rt-worker | kernel | `__arch_copy_to_user` | ext4 `pread64` from NVMe page cache to user buffer (chunk read on cache hit) |
| 4.7 | ucx-runtime | libc | (anon) | UCX progress polling (likely libc memcpy/atomic in `ucp_worker_progress`) |
| 2.5 | blobcached | blobcached | `tokio::worker::run` | tokio scheduler hot loop |
| 2.2 | blobcached | blobcached | `__aarch64_ldadd8_relax` | More atomic ops |
| 1.9 | blobcached | libc | (anon) | libc, likely memcpy |
| 1.8 | ucx-runtime | libc | (anon) | UCX progress |
| 1.6 | blobcached | blobcached | `__aarch64_cas1_acq` | Compare-and-swap |
| 1.6 | blobcached | kernel | `el0_svc` | syscall entry overhead |
| 1.6 | blobcached | blobcached | `__aarch64_ldadd8_rel` | Atomic add |
| 1.5 | blobcached | blobcached | `__aarch64_cas1_rel` | CAS release |
| 1.3 | blobcached | kernel | `fuse_copy_finish` | FUSE buffer release |
| 1.0 | blobcached | kernel | `futex_wake` | tokio task wakeup |
| 0.9 | blobcached | kernel | `fuse_dev_do_write` | FUSE reply write |
| 0.5 | blobcached | blobcached | `BlobFs::read::closure` | FUSE handler (rust code) |
| 0.3 | blobcached | blobcached | `Fetcher::fetch_chunk_inner::closure` | Cache lookup path |

### Lock/mutex symbols (full scan)

```
0.99 %  futex_wake               (kernel; tokio task wakeups)
0.69 %  futex_q_lock             (kernel)
0.30 %  futex_wake_mark          (kernel)
0.25 %  parking_lot::Condvar::wait_until_internal   (cv-wait, NOT lock contention)
0.18 %  lock_request.part.0      (kernel, FUSE)
0.15 %  futex_unqueue            (kernel)
0.08 %  parking_lot_core::lock_bucket_pair          (parking_lot internal)
0.05 %  __futex_wait             (kernel)
0.04 %  __lll_lock_wait_private  (libc)
0.03 %  mutex_lock               (kernel)
```

**`parking_lot::Mutex::lock_slow` does not appear.** All lock-related
symbols combined (kernel futex + parking_lot + libc lll) sum to <3 %
of CPU. Of those, the largest (`futex_wake` at 1 %) is tokio's task-
wakeup path, not an LRU-mutex hold.

### What this means

1. **Compute cost is dominated by byte movement** (~27 % CPU): FUSE
   writeback memcpy (21 %) + ext4 `pread64` `copy_to_user` (6 %).
   This is fundamental to the architecture (cache → user → FUSE
   kernel → user) and would only shrink with `splice()`/`SPLICE_F_MOVE`,
   `io_uring` for the FUSE reply, or zero-copy paths like
   `vmsplice`/`io_uring_register_buffers`.

2. **Tokio scheduler overhead is shockingly high** (~18 % across
   `swp8_acq_rel`, `cas1_*`, `ldadd8_*`, `tokio::worker::run`).
   This is the work-stealing scheduler's atomic operations across 8
   worker threads on aarch64. Worth investigating: pinning workers
   to cores, reducing worker count, or using `current_thread`
   runtime per FUSE reply would all be plausible mitigations.

3. **UCX progress is only ~5 % of CPU**, contradicting the README's
   "single-thread `ucp_worker_progress` is the ceiling" framing for
   *this* workload. The single UCX worker is busy enough to deliver
   bytes, but it's not the dominant CPU consumer. The pc=32
   regression is consistent with this: extra in-flight requests add
   coordination cost in UCX without adding completion bandwidth.

4. **Mutex contention on PeerLru is unmeasurable.** §2.7's premise was
   wrong. The audit said *"Magnitude guess: Unknown; could be 0 % or
   could be 10-30 %. Test first."* — it landed at 0 %.

# Where the wins actually are

Based on the perf evidence, ranked by likely impact:

1. **Reduce FUSE memcpy overhead** (~21 % CPU). Options:
   - Use `splice()` from cache file → FUSE reply where possible
     (avoids one memcpy by passing pages directly via the pipe).
   - Switch to `io_uring`-backed FUSE (kernel 6.5+; the host runs
     6.14 so this is available). The `fuse-passthrough` pattern lets
     reads on cached files bypass the FUSE daemon entirely.
   - Larger FUSE max_read (currently 4 MiB = chunk_size): reduces
     per-byte syscall overhead but doesn't reduce the memcpy itself.
2. **Reduce tokio scheduler overhead** (~18 % CPU). Options:
   - `current_thread` runtime + manual core pinning per FUSE reply
     thread (eliminates work-stealing atomics).
   - Reduce `workers` from 8 (try 4 or 2) — at this load there might
     be too many idle workers churning the work-stealing path.
3. **Remove §2.7** — restores ~30 lines, removes future maintenance,
   makes hot-path single-mutex acquire (which is uncontended).

Items 1 and 2 are real engineering work; item 3 is a 5-line revert.
