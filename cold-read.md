# Cold-read benchmarks: stampede vs. no-stampede

What happens when 17 pods read the same 413 GB dataset in parallel from a
fully-cold cache, with the stampede/HRW coordination mechanism toggled on
or off, and what that tells us about where time goes in the cold path.

## The dataset and harness

- **Dataset**: `nvidia_DeepSeek-R1-0528-NVFP4-v2/`, 163 `*.safetensors`
  files, 413.34 GB per pod (each pod independently reads every file)
- **Cluster**: 17 × gb300 ARM64 nodes, RDMA peer transport (UCX over
  IB), `chunk_size = 64 MiB`
- **Harness**: `benchmarks/run.sh --clear-cache --passes 2 --tag <tag>`
  - `--clear-cache` cluster-wide via `/clear-cache` endpoint
  - PASS1 = cold (post-clear), PASS2 = warm (immediately after PASS1)
  - 17 parallel `kubectl exec ... cat *.safetensors > /dev/null` per pod

## Headline results

| Run | Stampede | Prefetch | PASS1 (cold) | PASS2 (warm) | Notes |
|---|---|---|---|---|---|
| #5 baseline | ON (5000ms) | depth=4, all pods | **307s** | 237s | reference |
| #7 origin-only | ON (5000ms) | depth=4, origin only | 2221s (7.2×) | 241s | regression: serialised cold pipeline |
| **#8 no-stampede** | **OFF (0ms)** | depth=4, all pods | **3301s (10.8×)** | 241s | regression: 17× independent Azure pulls |

PASS2 is invariant — once warm, all configs serve from local NVMe at the
same rate (~1.74 GiB/s per pod over the cat read).

The cold path is where the architecture earns or loses its keep, and
both deviations from the baseline (origin-only, no-stampede) regress
hard. They regress for *opposite* reasons.

## Run #8 deep dive: why no-stampede was worse than origin-only

### Per-pod metrics at end of run

Sample of 5 pods (out of 17), all read the same 413 GB:

| Pod | Azure GETs | Peer fetches OK | Bloom NO holder | Bloom YES |
|---|---|---|---|---|
| 52gqr | 5624 | 654 | 5624 | 654 |
| 865bp | 5791 | 487 | 5791 | 487 |
| 9g78m | 5400 | 878 | 5400 | 878 |
| ndxx5 | 5038 | 1240 | 5038 | 1240 |
| xvhqb | 5879 | 399 | 5879 | 399 |

Two exact equalities tell the whole story:

- **`peer_bloom_no_holder == blob_fetches`** — every Azure GET happened
  because, at the moment the fetcher checked, no peer's bloom claimed
  to have the chunk. This is true on every pod, every time.
- **`peer_bloom_yes == peer_fetches_ok`** — every "yes" succeeded; zero
  false positives in this run (`peer_bloom_false_positive_total = 0`).
  The bloom is *accurate*, just *late*.

### What the numbers mean

Cluster-wide cold ingest ≈ 17 pods × 5500 chunks × 64 MiB ≈ **6 TB**
(versus 413 GB if perfectly deduplicated). Peers absorbed ~12% of
chunks by the time PASS1 ended, but the first 30+ minutes of the run
were almost entirely Azure pulls because:

1. PASS1 starts with every pod's cache empty AND every pod's bloom
   filter empty.
2. Bloom updates propagate via gossip pull on a **1.5 s cycle** plus a
   bloom-rebuild interval. So node A inserts chunk X into its cache at
   t=0, but node B doesn't *see* A's bloom claim until t ≈ 1.5–3 s.
3. With stampede off, when node B misses chunk X at t=1.0 s, the
   bloom shows "no holder" for X, and B goes straight to Azure
   independently of A.
4. All 17 pods read sequentially through the same files, so they all
   miss the same chunks in roughly the same order. The bloom-pull
   delay window swallows most of the cold pass.

### Was it Azure throttling?

**No.** Coordinator pod metrics at end of PASS1:

```
blobcache_blob_request_status_total{status="200"} 328
blobcache_blob_request_status_total{status="206"} 5624
blobcache_blob_request_giveups_total 0
blobcache_blob_retry_sleep_seconds_total 0
```

Zero 429s, zero retry sleep, zero giveups. The storage account is not
the bottleneck. Per-pod Azure throughput was ~109 MB/s; cluster
aggregate ~1.85 GB/s = ~15 Gbps — well below the storage account's
typical headroom (~25–50 Gbps) and well below the daemon's documented
~28 Gbps single-runtime ceiling.

### So what *is* the bottleneck?

In this configuration, the bottleneck is the **bloom propagation
window** combined with the **synchronised access pattern of 17 pods
reading the same files**. The system is software-bound somewhere
between the chunk-concurrency semaphore (32 permits per pod) and the
single-tokio-runtime Azure client (`azure.workers = 1`); evidence:

- `blob_fetches` 5624 over 3301 s = 1.7 GETs/s with 32 in-flight
  permitted → either each GET takes ~19 s (unlikely; 64 MiB at
  reasonable bandwidth is sub-second) or the in-flight count never
  approaches 32 because the workload provides no parallelism *for
  Azure* at any one time (most chunk fetches are local-cache hits as
  soon as a peer or this pod's earlier read landed it).
- Cache-insert NVMe write averages 19.4 ms per chunk
  (`chunk_cache_insert_seconds 121.6 / 6278`). With 32 concurrent
  inserts capped by tokio's blocking pool, that's a ceiling on how
  fast a pod can absorb new chunks — **disk write is plausibly a
  meaningful contributor** under the broadcast scenario we're about
  to test.
- Single-stream warm-peer is documented at 3.55 GiB/s (BENCHMARKS.md
  v2.3); the cold path never gets the chance to use peer transport
  efficiently because bloom never says yes during the bootstrap window.

## Run #7 vs Run #8: the two failure modes are opposite

Both regressed PASS1 hard. They regressed for opposite reasons.

- **#7 origin-only** (`prefetch_origin_only=true`): only the node that
  pulled a chunk from Azure prefetches forward. With 17 pods reading
  the same file, each chunk has exactly one origin pod; the other 16
  pods serve reads strictly on demand without the read-ahead pipeline
  that hides per-chunk latency. Cold pass collapses to a serialised
  per-file pipeline behind the origin.
- **#8 no-stampede** (`stampede_wait_ms=0`): every pod independently
  decides to go to Azure when bloom says "no holder", because the HRW
  leader-election that would have deduplicated origin pulls is
  bypassed. Cold pass becomes 17× duplicate Azure work because bloom
  hasn't propagated yet during the critical bootstrap window.

The baseline (#5) wins by combining:
- Stampede HRW elects a per-chunk leader → only one pod hits Azure
  for chunk X, the other 16 piggyback on its singleflight via the
  HRW-top peer with `wait_ms = 5000`.
- Prefetch on all pods → as soon as a pod's read pointer catches up to
  cached data, its sequential detector kicks off background pulls of
  the next 4 chunks.

These two mechanisms are complementary: stampede deduplicates origin
work across pods, prefetch hides per-chunk latency within a pod.

## What this changes about my mental model

- **Bloom is for the warm path, not the cold bootstrap.** During cold
  reads, bloom is universally negative for ~1.5–3 s after each insert.
  HRW + stampede is what carries the system through that window. If
  you ever turn stampede off "to compare", expect 10×.
- **The 4% bloom FP rate observed in earlier production-style runs
  was insert-race staleness**, not hash collisions. This run shows
  zero FPs because the access pattern (whole-cluster simultaneous cold
  read) leaves no time for asymmetric staleness — every pod is in the
  same propagation gap simultaneously.
- **`prefetch_origin_only` is the wrong knob for "avoid duplicate
  prefetch"** in the all-pods-read-same-tree workload. It's the right
  knob for disjoint-shard workloads (each pod owns a subset of files).
  We don't have that workload yet.
- **`azure.block_size` and `transport.peer_concurrency` are dead
  config**. `block_size` is parsed but the actual GETs use
  `cache.chunk_size`. `peer_concurrency` has no readers in the code.
  These should be removed or wired up.

## Followups for review (decisions deferred to user)

Listed roughly in expected leverage. Pick the ones you want to run.

1. **Hydrate-broadcast (built and tested; first run uncovered a stall —
   fix landed but not yet re-run)**. Two-phase: each node shard-pulls
   from Azure (disjoint), then UCX all-to-all distributes to all 17
   nodes. Optional via `mode=broadcast` parameter, default off.
   Hypothesis: cluster-wide cold ingest drops from 17× to 1× dataset,
   and the RDMA distribution phase is fast (~minutes at 11 GiB/s
   aggregate); disk write becomes the bottleneck and is the honest
   lower bound for "everyone has everything cached".

   **Run #9 (broadcast, first attempt)** at 2026-04-29T08:33:57Z stalled
   in Phase B. Phase A finished cleanly in ~4 min (each pod downloaded
   its ~358-chunk shard ≈ 24.6 GB from Azure). Phase B then ran for
   >20 min and only completed ~32 outbound peer fetches per pod (out of
   ~5722 expected). Histograms showed `chunk_peer_fetch_seconds_count=32`
   with `_sum ≈ 13948 s` — i.e. ≈ 11 fetches finished promptly (≤100 ms),
   then 21 fetches each took several hundred seconds. Inbound serves
   were skewed: coordinator served 1019 chunks (54 GB), one peer served
   64, two sampled peers served 0. **Cause**: `run_broadcast_shard`
   spawned chunks in source-major order. With `chunk_concurrency = 32`
   and 16 sources, the first 32 spawns to acquire permits were all the
   coordinator's chunks → every receiver hammered the same source,
   serialising 16 outbound RDMA endpoints through the single UCX
   runtime task. 15 sources sat idle. Fix in commit
   `<round-robin commit>`: round-robin chunks across sources at spawn
   time, plus a per-source semaphore sized at `chunk_concurrency /
   n_sources` so no single endpoint can monopolise the puller. Not
   re-run; left for the next session to validate.

2. **Bump `azure.workers` from 1 → 8**. Single-runtime ceiling is
   ~28 Gbps documented. We're nowhere near it per-pod, but this is a
   prerequisite for any future scenario where one pod needs more than
   ~3.5 GB/s sustained from Azure (e.g., the broadcast phase A above).
   Trivial change, no code work, just `--set config.azure.workers=8`
   on helm upgrade.

   **Update 2026-04-29**: deployed `azure.workers=8` was already in
   force when run #9 executed (confirmed via `helm get values blobcache`
   — chart default is `workers: 8` in values.yaml; user-supplied values
   never overrode it). Despite that, run #9 Phase A clocked
   ~127 MB/s/pod cold (24.6 GB / ~4 min). A targeted code-path audit
   (explore agent, 2026-04-29) confirmed the knob is wired correctly:
   `BlobFetcherPool::new` constructs N independent multi-thread tokio
   runtimes, each owning its own `BlobClient` (which builds its own
   `reqwest::Client` with independent HTTP/1.1 connection pool); the
   hot path round-robins via `AtomicUsize` and dispatches every GET
   onto the chosen worker's runtime via `worker.rt.spawn(...)`.
   Hydrate's `run_shard` reaches this pool through
   `Fetcher::fetch_chunk_origin_only → fetch_chunk_inner → do_fetch →
   pool.get_blob_range`. So workers=8 is exercised, not silently capped.

   That makes the remaining suspects: (a) `transport.chunk_concurrency`
   (default 32) gating in-flight GETs; (b) Azure-side throttling on a
   single storage account fanning out to 17 pods × ≤32 concurrent GETs
   = 544 in-flight requests (visible in
   `blob_request_retries_total` / `blob_retry_sleep_seconds_total`
   if it's the cause); (c) per-pod NIC / VM SKU bandwidth ceiling.

   No per-worker Prometheus label exists today — adding one
   (`worker_id` on `blob_fetches_total` etc.) would prove the
   round-robin is even across all 8 runtimes. Tracked as next step
   below.

3. **Bump prefetch `depth=8, concurrency=8`** (your suggestion).
   Current is `depth=4, concurrency=4`. Doubles read-ahead window per
   pod. Worth testing under baseline (stampede on) to see if it
   improves the existing 307 s cold time. Small risk: more prefetch
   in flight means more cache_insert pressure and more peer fan-out,
   could regress if NVMe writes are already saturated.

4. **Reduce bloom-pull interval**. Currently 1.5 s. Cutting it to
   500 ms would shrink the bootstrap window where every miss looks
   like "no holder". Cost: 3× gossip CPU/network, ~1 MiB bloom per
   pull * 17 pods * higher rate. Probably worth it on a 17-node
   cluster. Code change: `transport.bloom_pull_secs` knob already
   exists in config.

5. **Add chunk-size comparison run**. Current cluster is 64 MiB
   chunks. BENCHMARKS.md historically used 4 MiB. Smaller chunks =
   smaller propagation grain, finer-grained sharing, but more
   per-chunk overhead. Worth one apples-to-apples comparison at the
   current `azure.workers=1, prefetch_depth=4` baseline. Not sure
   which direction it moves the cold-pass number.

6. **Remove or wire up dead config**. `azure.block_size` and
   `transport.peer_concurrency` should either be removed (cleaner
   config) or actually plumbed through (less surprising). Trivial
   refactor, no perf impact in itself.

7. **Add per-source latency histograms**. We have
   `chunk_fetch_total_seconds` (everything mixed) and
   `chunk_peer_fetch_seconds`, but no `blob_fetch_seconds` histogram.
   Adding it would let us see whether individual Azure GETs are slow
   (network-bound) or fast-but-rare (concurrency-bound). 5-line
   change in `azure.rs` + `stats.rs`.

8. **Add chunk_sem occupancy gauge**. Right now we infer concurrency
   from singleflight_waits and prefetch_skipped_inflight. A direct
   gauge of `chunk_sem.available_permits()` sampled every second
   would tell us definitively whether we're ever pinned at 0 permits
   (the "we're capped by chunk_concurrency" signal). 3-line change.

9. **Re-run baseline with stampede on + prefetch=8/8** to provide a
   clean reference point for the broadcast hydrate comparison. If the
   broadcast-hydrate cold time beats the best stampede+prefetch cold
   time, the broadcast mode earns its keep.

## Configuration discrepancies noted

- `transport.peer_concurrency` (default 8): defined in config, no
  code reads it. Dead.
- `azure.block_size` (default 0 = use chunk_size): defined and
  computed by Fetcher, but the actual `pool.get_blob_range(...)`
  calls always pass `self.chunk_size`, never `self.block_size`. Dead
  unless you wanted block aggregation in a later iteration.
- Helm template had a Go-truthiness bug: `{{- with X }}` skipped the
  line when `X = 0`, so `--set stampedeWaitMs=0` silently no-op'd.
  Fixed in commit `bc26600`. The same bug pattern exists for
  `prefetchDepth`, `prefetchThreshold`, `prefetchConcurrency`,
  `bloomBits`, `bloomRebuildSecs`, `bloomPullSecs`,
  `peerMaxCandidates`, `peerMaxYesAttempts`, `peerMaxMaybeAttempts` —
  same template, lines 39, 44, 47, 53, 56, 59, 62, 65, 68. Fix when
  someone next needs to override one of those to 0.

## Reproduction

```sh
# Reproduce run #8 (no stampede)
helm -n blobcache upgrade blobcache ./deploy/helm/blobcache --reuse-values \
  --set config.transport.stampedeWaitMs=0
kubectl -n blobcache rollout status ds/blobcache-blobcached
./benchmarks/run.sh \
  --prefix nvidia_DeepSeek-R1-0528-NVFP4-v2/ \
  --tag <your-tag> --clear-cache --passes 2 --read-timeout 1800
```

Outputs land in `benchmarks/out/<tag>-{run.log,pass1.tsv,pass2.tsv}`.
TSVs already contain only sanitized identifiers (pod name, vmss
instance ID, byte counts, wall time).
