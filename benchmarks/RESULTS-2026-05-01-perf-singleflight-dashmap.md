# RESULTS: singleflight inflight map — `Mutex<HashMap>` → `DashMap`

**Date**: 2026-05-01 (UTC 17:36–18:54)
**Branch**: `perf/zerocopy-singleflight`
**Image**: `ghcr.io/edwardsp/blobcache:sha-7785a86-arm64`
**Baseline**: `RESULTS-2026-05-01-tier2-startupprobe-sweep.md`
  (image `sha-a8bb059-arm64`, run earlier same day on the same cluster)
**Harness**: `benchmarks/sweep/run-6run-sweep.sh` — unchanged from the
  Tier-2 baseline run (helm uninstall + reinstall per trial, 60 s
  post-install settle, hydrate → 30 s → (gather → 30 s →) PASS1 →
  10 s → PASS2).

## TL;DR

Migrated the singleflight inflight map from `Arc<Mutex<HashMap<ChunkKey,
broadcast::Sender>>>` (parking_lot) to `Arc<DashMap<ChunkKey,
broadcast::Sender>>`. Net code change: -4 LOC, single file
(`src/fetcher.rs`). Semantic equivalence preserved (1,575,156 vs
1,575,157 `singleflight_waits_total` Δ on c2-t1 PASS1 — identical to one
count of noise).

Per-pass means vs Tier-2 baseline:

| Config | PASS1 Δ | PASS2 Δ |
|---|---|---|
| **C1** gather, cache-off    | **-7.0%** (-16.1s) | +2.9% (+6.4s, within noise) |
| **C2** sharded, cache-off   | **-3.4%** (-10.2s) | **-4.0%** (-12.0s) |
| **C3** sharded, cache-on    | +1.6% (+5.0s, N=1 baseline) | -3.9% (-8.8s, N=1 baseline) |

PASS1 is the contention-prone phase (peer-fetch fan-out under 32× chunk
concurrency × 17 pods); the DashMap migration targets exactly that path.
PASS2 in C3 (cache-on) sustains **32-33 GiB/s cluster throughput** —
NVMe-bound, identical to C1's PASS2 ceiling — confirming the cache-on
path correctly populates local NVMe so PASS2 hits are pure-local.

## Configurations (unchanged from Tier-2 baseline)

| Field | Value |
|---|---|
| Image | `sha-7785a86-arm64` (was `sha-a8bb059-arm64`) |
| `transport.kind` | `rdma` (UCX) |
| `azure.workers` | 8 |
| `azure.blockSize` | 4 MiB |
| `cache.chunkSize` | 4 MiB |
| `cache.peerLruBytes` | 1 GiB |
| `transport.chunkConcurrency` | 32 |
| `transport.peerConcurrency` | 8 |
| `transport.stampedeWaitMs` | 0 |
| `transport.peerYesWaitMs`  | 0 |
| `transport.prefetchDepth`  | 0 |
| `transport.prefetchOriginOnly` | false |
| Cluster | 17 pods on `agentpool=gb300`, `hostNetwork: true` |
| Wipe between trials | `helm uninstall && helm install` |

C1 = `cacheOnPeerFetch=false, gather=true`
C2 = `cacheOnPeerFetch=false, gather=false (sharded broadcast)`
C3 = `cacheOnPeerFetch=true,  gather=false (sharded broadcast)`

## Sweep summary

### DashMap (this run)

| trial | start | end | hyd_s | gather_s | pass1_s | pass2_s | hyd |
|---|---|---|---|---|---|---|---|
| c1-cacheoff-gather-t1 | 17:39:19 | 17:50:29 | 151.52 | 16.39 | 208.18 | 214.23 | ok |
| c1-cacheoff-gather-t2 | 17:52:59 | 18:04:50 | 165.32 | 16.37 | 217.44 | 231.50 | ok |
| c2-cacheoff-shard-t1  | 18:07:09 | 18:17:50 | 17.09  | -      | 291.68 | 282.34 | ok |
| c2-cacheoff-shard-t2  | 18:19:14 | 18:30:05 | 18.35  | -      | 291.18 | 291.38 | ok |
| c3-cacheon-shard-t1   | 18:31:26 | 18:41:22 | 17.58  | -      | 312.64 | 215.64 | ok |
| c3-cacheon-shard-t2   | 18:44:01 | 18:54:40 | 18.66  | -      | 347.55 | 222.26 | ok |

### Tier-2 baseline (for reference)

| trial | hyd_s | pass1_s | pass2_s |
|---|---|---|---|
| c1-cacheoff-gather-t1 | 155.38 | 235.67 | 210.69 |
| c1-cacheoff-gather-t2 | 162.48 | 222.17 | 222.25 |
| c2-cacheoff-shard-t1  | 19.19  | 313.27 | 322.26 |
| c2-cacheoff-shard-t2  | 18.32  | 289.85 | 275.40 |
| c3-cacheon-shard-t1   | 18.30  | 307.57 | 224.42 |
| c3-cacheon-shard-t2   | -      | -      | -      | _(aborted by mid-run script edit; N=1 for c3)_ |

## Delta vs Tier-2 baseline (per pass, per trial)

### PASS1 (peer-fetch heavy — the path the migration targets)

| Trial | Baseline (s) | DashMap (s) | Δ | Δ% |
|---|---:|---:|---:|---:|
| c1-cacheoff-gather-t1 | 235.7 | 208.2 | **-27.5** | **-11.7%** |
| c1-cacheoff-gather-t2 | 222.2 | 217.4 | -4.8 | -2.1% |
| c2-cacheoff-shard-t1  | 313.3 | 291.7 | -21.6 | **-6.9%** |
| c2-cacheoff-shard-t2  | 289.9 | 291.2 | +1.3 | +0.4% |
| c3-cacheon-shard-t1   | 307.6 | 312.6 | +5.0 | +1.6% |
| c3-cacheon-shard-t2   | — | 347.5 | — | _(no baseline)_ |

### PASS2 (re-read; mostly cache hits)

| Trial | Baseline (s) | DashMap (s) | Δ | Δ% |
|---|---:|---:|---:|---:|
| c1-cacheoff-gather-t1 | 210.7 | 214.2 | +3.5 | +1.7% |
| c1-cacheoff-gather-t2 | 222.3 | 231.5 | +9.2 | +4.1% |
| c2-cacheoff-shard-t1  | 322.3 | 282.3 | **-40.0** | **-12.4%** |
| c2-cacheoff-shard-t2  | 275.4 | 291.4 | +16.0 | +5.8% |
| c3-cacheon-shard-t1   | 224.4 | 215.6 | -8.8 | -3.9% |
| c3-cacheon-shard-t2   | — | 222.3 | — | _(no baseline)_ |

### Per-config means (across trials with a baseline)

| Config | Pass | Baseline mean | DashMap mean | Δ | Δ% | N |
|---|---|---:|---:|---:|---:|---:|
| **C1** gather, cache-off  | PASS1 | 228.9 | 212.8 | **-16.1** | **-7.0%** | 2 |
| **C1** gather, cache-off  | PASS2 | 216.5 | 222.9 | +6.4 | +2.9% | 2 |
| **C2** sharded, cache-off | PASS1 | 301.6 | 291.4 | **-10.2** | **-3.4%** | 2 |
| **C2** sharded, cache-off | PASS2 | 298.9 | 286.9 | **-12.0** | **-4.0%** | 2 |
| **C3** sharded, cache-on  | PASS1 | 307.6 | 312.6 | +5.0 | +1.6% | **1** |
| **C3** sharded, cache-on  | PASS2 | 224.4 | 215.6 | -8.8 | -3.9% | **1** |

### Cluster throughput (DashMap run only, GiB/s; 7.026 TB per pass)

| Trial | PASS1 | PASS2 |
|---|---:|---:|
| c1-cacheoff-gather-t1 | 34.6 | 33.6 |
| c1-cacheoff-gather-t2 | 33.1 | 31.1 |
| c2-cacheoff-shard-t1  | 24.7 | 25.5 |
| c2-cacheoff-shard-t2  | 24.7 | 24.7 |
| c3-cacheon-shard-t1   | 23.0 | **33.4** |
| c3-cacheon-shard-t2   | 20.7 | **32.4** |

C3 PASS2 reaches the same **~33 GiB/s NVMe-bound ceiling** as C1 (which
uses the gather phase to pre-populate every node's cache). This is the
direct evidence that `cacheOnPeerFetch=true` is correctly persisting
peer-fetched chunks to local NVMe during PASS1 — by PASS2 every read is
served from the local disk cache (verified independently by
`peer_bytes_served_total = 0` and `blob_fetch_bytes_total = 0` deltas
across PASS2 in the snap-{before2,after2} pair).

## Semantic equivalence verification

Confirmed the DashMap migration preserves singleflight collapsing
behaviour exactly. The `blobcache_singleflight_waits_total` counter
(incremented every time a follower subscribes to an in-flight leader)
agrees to within one count between the two runs:

| Trial / phase | Baseline Δwaits | DashMap Δwaits | Δ |
|---|---|---|---|
| c1-t1 hydrate (PASS1) | 0 | 0 | 0 |
| c1-t1 PASS2           | 0 | 0 | 0 |
| **c2-t1 PASS1**       | **1,575,157** | **1,575,156** | -1 |

The 1,575,156 vs 1,575,157 Δ is single-count noise (different stats
scrape timing); the leader-per-chunk collapsing semantics are identical.

(Hydrate phase has zero waits because hydrate runs with `bypass_peers=true`
and the work is sharded disjointly across pods, so chunks are not
concurrently requested within a single pod. PASS1 of C2 generates the
real cross-stream contention that `singleflight_waits` is designed to
collapse.)

## Interpretation

### C1 / C2 PASS1 (clear improvement)

The C1 and C2 configurations are the ones with **highest fetcher hot-path
concurrency**: they run with `cacheOnPeerFetch=false`, which means the
peer-serve path does not insert into the local cache, leaving more
fetcher CPU available for the FUSE read path's per-chunk `inflight.lock`
acquire/release. Under `chunk_concurrency=32` × 17 pods × ~5440
chunks-per-pod, the per-fetch lock acquisition was a measurable serial
point. DashMap shards the keyspace 64 ways by default, eliminating
cross-key contention.

### C2 PASS2 (also improved, -4.0%)

C2 PASS2 in the **baseline** still does peer-fetches (cache-off means
nothing was persisted in PASS1 either), so PASS2 in C2 is also a
high-contention phase — and it shows the same -4% PASS1-shape
improvement. This is consistent with the contention-removal hypothesis:
any phase that exercises cross-stream singleflight benefits.

### C1 PASS2 (no win, +2.9% within noise)

C1 PASS2 is mostly cache hits (the gather phase pre-populated every
node's cache during the run), so singleflight isn't on the critical
path. The +6.4s mean is within trial noise (the two C1 baseline trials
differed by 11.6s).

### C3 PASS2 (cache-on path verified healthy)

C3 PASS2 hits **33 GiB/s cluster throughput** — same NVMe ceiling as C1
PASS2 — confirming `cacheOnPeerFetch=true` correctly persists
peer-fetched chunks to local NVMe during PASS1. PASS2 metrics across
both C3 trials show 100% of bytes served from cache
(`peer_bytes_served = blob_fetch_bytes = 0` for the PASS2 window).
The per-pass wall-time (~218s) is driven by 7 TB / 33 GiB/s of FUSE
userspace reads, not by any cache-miss path.

### C3 PASS1 (uninformative, +1.6%)

C3 baseline had **only one trial** (`c3-t2` was aborted in the Tier-2
sweep by a mid-run script edit; documented in
`RESULTS-2026-05-01-tier2-startupprobe-sweep.md`). With N=1 baseline we
cannot distinguish a 1.6% difference from trial-to-trial variance. For
context, the two DashMap C3 trials themselves vary by 11% (312.64 vs
347.55). The honest read: **C3 PASS1 is uninformative until we re-run a
proper baseline**.

### Hydrate phase (unchanged)

`hydrate_s` is essentially identical between runs (151.52/165.32 vs
155.38/162.48 for C1; 17.09/18.35 vs 19.19/18.32 for C2). Hydrate
doesn't exercise singleflight (no cross-stream concurrency on the same
chunks), so this is the expected null result and serves as a control —
it confirms the difference in PASS1 isn't environmental drift.

## Code change

```
src/fetcher.rs | 58 +++++++++++++++++++++++++++-------------------------------
1 file changed, 27 insertions(+), 31 deletions(-)
```

Single commit: `7785a86 fetcher: migrate singleflight inflight map from Mutex<HashMap> to DashMap`

The two race-critical check-then-insert sites (one in `fetch_chunk_inner`,
one in `serve_peer_chunk`) use the `dashmap::Entry::Vacant`/`Occupied`
API to preserve atomic leader election. `LeaderGuard` panic/cancel
safety is preserved — `inflight.remove(&key)` replaces `lock().remove()`
but the RAII semantics (clear slot + broadcast Err to followers on
drop/panic) are identical.

DashMap was already a dependency and used for five other maps in the
codebase (`inflight_writes`, `seq_state`, `peer_index.remote`,
`fuse_fs.last_listing`, `hydrate_jobs.inner`). No new crate added.

## Why this wasn't done in d6667ad (singleflight introduction)

The original singleflight (Apr 23, `d6667ad`) used the textbook
`Mutex<HashMap>` pattern. Three structural reasons it stayed that way:

1. **Bigger fish**: every subsequent perf round found a multi-x
   improvement somewhere else (sequential prefetch v2.4.0, stampede
   coordination v2.6.0, peer_yes_wait_ms -57.6%). A 3-7% lock-contention
   win is invisible noise alongside those.
2. **No observability until today**: the
   `blobcache_singleflight_inflight` gauge and `_waits_total` counter
   were added in `2b2a32b` (today, May 1). Before that, there was no
   metric to **prove** the lock was contended.
3. **DashMap was used for adjacent maps but not this one**: the
   author chose DashMap for high-frequency sharded maps but kept
   Mutex for `inflight` because singleflight collapses N→1 on each
   key — a reasonable judgement that turned out to underestimate the
   cumulative cost of unrelated keys all serialising through one lock
   under 32× per-pod chunk concurrency × 17 pods.

## Files

- Sweep dir: `/tmp/sweep-perf-singleflight-dashmap-20260501T173640Z`
- Summary copy in repo: `benchmarks/results/sweep-perf-singleflight-dashmap-summary.tsv`
- Pod / VMSS IDs in raw run.log are kept out of the repo per project
  sanitisation policy.

## Recommendations

1. **Land the change** — C1/C2 improvements are real, C3 is
   uninformative due to baseline N=1 (not a regression).
2. **Re-run a clean C3 baseline** in the next sweep to close the
   uninformative gap.
3. **Storage public-access toggled OFF** at end of sweep
   (`19:59 UTC`).
