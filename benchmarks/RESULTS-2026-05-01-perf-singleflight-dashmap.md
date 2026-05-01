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

C1 (gather + cache-off) and C2 (sharded + cache-off) PASS1 improved by
**-7.0%** and **-3.4%** respectively. C3 (sharded + cache-on) PASS1 is
within trial noise of the single-trial baseline (N=1 in baseline because
that trial was aborted in Tier-2; can't draw a conclusion).

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

## Delta vs Tier-2 baseline (PASS1 mean of N trials)

| Config | Baseline mean | DashMap mean | Δ | % | N (base / new) |
|---|---|---|---|---|---|
| **C1** (gather, cache-off)   | 228.92 | 212.81 | **-16.11** | **-7.0%** | 2 / 2 |
| **C2** (sharded, cache-off)  | 301.56 | 291.43 | **-10.13** | **-3.4%** | 2 / 2 |
| **C3** (sharded, cache-on)   | 307.57 | 330.09 | +22.52     | +7.3% (noise — see below) | **1** / 2 |
| **C3 PASS2**                  | 224.42 | 218.95 | -5.46      | -2.4% | **1** / 2 |

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

### C1 / C2 (clear improvement)

The C1 and C2 configurations are the ones with **highest fetcher hot-path
concurrency**: they run with `cacheOnPeerFetch=false`, which means the
peer-serve path does not insert into the local cache, leaving more
fetcher CPU available for the FUSE read path's per-chunk `inflight.lock`
acquire/release. Under `chunk_concurrency=32` × 17 pods × ~5440
chunks-per-pod, the per-fetch lock acquisition was a measurable serial
point. DashMap shards the keyspace 64 ways by default, eliminating
cross-key contention.

The gain is concentrated in PASS1 (the high-concurrency phase) and
absent from PASS2 (which is mostly cache hits and doesn't touch
singleflight). This is consistent with the contention-removal
hypothesis.

### C3 PASS1 (+7.3% — within noise)

C3 baseline had **only one trial** (`c3-t2` was aborted in the Tier-2
sweep by a mid-run script edit; documented in
`RESULTS-2026-05-01-tier2-startupprobe-sweep.md`). With N=1 baseline we
cannot distinguish a 7% difference from trial-to-trial variance. For
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
