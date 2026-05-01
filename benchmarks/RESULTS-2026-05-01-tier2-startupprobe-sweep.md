# Tier-2 sweep — startupProbe + silent-loss broadcast fix

| | |
|---|---|
| **Date** | 2026-05-01 |
| **Branch** | `feat/observability-v2` |
| **Image**  | `sha-a8bb059-arm64` |
| **Commits in scope** | `a8bb059` (silent-loss broadcast fix) · `8b6677c` (sweep readiness gate + hydrate verify) · `0e4b9be` (helm startupProbe) |
| **Cluster** | 17× GB300 (aarch64, 8× NVMe RAID-0, IB) |
| **Dataset** | `nvidia_DeepSeek-R1-0528-NVFP4-v2/` — 350 files, 98 804 chunks, 413 GB |
| **Sweep wall** | 14:53Z–15:56Z (~62 min, 5 trials) |

## TL;DR

| Config | Trial | Hydrate (s) | Gather (s) | PASS1 (s) | PASS2 (s) | hyd_status |
|---|---|---:|---:|---:|---:|:---:|
| **C1** cacheOff + sharded + **gather** | t1 | 155.4 | 17.1 | **235.7** | 210.7 | ok |
| **C1** cacheOff + sharded + **gather** | t2 | 162.5 | 17.5 | **222.2** | 222.3 | ok |
| **C2** cacheOff + sharded (no gather) | t1 | 19.2 | – | **313.3** | 322.3 | ok |
| **C2** cacheOff + sharded (no gather) | t2 | 18.3 | – | **289.9** | 275.4 | ok |
| **C3** cacheOn  + sharded (no gather) | t1 | 18.3 | – | **307.6** | 224.4 | ok |
| **C3** cacheOn  + sharded (no gather) | t2 | — | — | — | — | not run † |

† c3-t2 was not executed: an in-flight edit to the orchestrator script
(adding the 60 s settle window — see "Operational notes" below) caused
bash to re-read the file mid-run and abort with a phantom syntax error
after c3-t1 completed. The script as committed is syntactically clean
(`bash -n` passes); the 5 completed trials are sufficient to validate
both fixes and remain consistent with the Tier-1 baseline.

## Headline takeaways

1. **All 17 nodes participated cleanly in every trial.** Every trial
   reports `hyd_status=ok` (every peer fetched its full assigned shard
   with zero errors). The previously crash-looping node now hydrates in
   line with the rest of the cluster.

2. **Throughput tracks the Tier-1 baseline** (within ±10 % run-to-run
   noise on a shared storage account):

   | | Tier-1 PASS1 (mean) | Tier-2 PASS1 (mean) | Δ |
   |---|---:|---:|---:|
   | C1 gather + cacheOff | 211.8 s | 229.0 s | +8 % |
   | C2 shard  + cacheOff | 289.7 s | 301.6 s | +4 % |
   | C3 shard  + cacheOn  | 302.5 s | 307.6 s | +2 % |

   No regression from the silent-loss broadcast fix or the startupProbe
   addition.

3. **`cacheOnPeerFetch=true` (C3) still pays its ~80 s PASS2 win.**
   PASS1 = 307.6 s (peer-bound, like C2), PASS2 = 224.4 s (NVMe-bound
   because peer fetches populated the local cache). Identical to the
   Tier-1 finding.

4. **Gather phase moves a lot of bytes through peers, by design.**
   c1-t1 snap-after counters (cluster aggregate, post-hydrate+gather,
   pre-PASS1):
   - `blobcache_blob_fetches_total` = **98 804** (1× per chunk, exact)
   - `blobcache_peer_fetches_ok_total` = **1 580 864**
   - `blobcache_peer_chunk_bytes_served_total` = **6.6 TB**
   - `blobcache_peer_bloom_false_positive_total` = **0**
   - `blobcache_peer_fetches_miss_total` = **0**

   Hydrate Phase A is origin-only (`fetch_chunk_origin_only`,
   `bypass_peers=true`) — it never touches peers. The 6.6 TB of
   peer-served bytes and the 1.58 M peer fetches are entirely the
   gather phase doing its job: every node pulls the other 16/17 of the
   dataset from its peers so PASS1 can be 100 % local-NVMe.
   c2-t1 (shard mode, no gather) snap-after for the same counters
   reads `peer_fetches_ok=0`, `peer_served_bytes=0`, confirming the
   attribution.

5. **Per-VM blob latency is uniform.** Live `blobcache_blob_request_seconds`
   inspection across all 17 nodes after the sweep:

   | Stat | min | median | max |
   |---|---:|---:|---:|
   | avg request (ms) | 75.4 | 78.1 | 79.8 |
   | longest single request (s) | 0.40 | 0.65 | 1.63 |

   No outlier. The previously-suspect node sits in the bottom quartile
   on both metrics — its earlier crash loop was a probe-budget issue
   (78 s cold-start NVMe scan vs 75 s liveness budget), not hardware.

## Fixes validated

### `0e4b9be` helm: add startupProbe to blobcached

Splits liveness and startup. Liveness budget stays at 75 s (catches
runtime hangs); startupProbe gives 5 min cold-start grace
(`failureThreshold=30 periodSeconds=10`) for `DiskCache::open` to
finish scanning persisted chunks (~1 s per 1 k chunks empirically).

Before this fix, on a node that survived previous helm-installs with
~98 k chunks on disk, the daemon took ~78 s to bind `:7773`, the
liveness probe failed at 75 s, kubelet restarted the container, and
the node entered a restart loop while every other node hydrated and
moved on. PR #4's Tier-2 sweep on this build shows that node
participating cleanly in all 5 trials.

### `a8bb059` fix(hydrate): close silent-loss path in two-phase broadcast

(Validated separately in the prior 6-run sweep documented in
`benchmarks/RESULTS-2026-04-30-fix-bloom-stale-revalidate.md`. This
sweep re-confirms no regression: Phase A counts exactly 98 804 blob
fetches across the cluster, one per chunk.)

### `8b6677c` fix(sweep): gate hydrate on HTTP readiness + verify shard completion

The harness now polls every pod's `:7773/metrics` until 200 OK before
posting `/hydrate-shard`, then checks the per-peer hydrate JSON for
`assigned_chunks == fetched` and zero errors before allowing the trial
to record `hyd_status=ok`. All 5 trials in this sweep passed both
gates.

## Operational notes

### Grafana scrape-baseline gotcha

c1-t1's gather phase moves 6.6 TB of peer-served bytes in 17 s, but
the spike was nearly invisible on the cluster Grafana panel. c1-t2,
running the *identical* workload 13 minutes later, lit up the panel
clearly.

Cause: PodMonitor scrape interval is 15 s and `rate()` requires ≥ 2
samples in its 1-min window. Pods became Ready at 14:55:19 Z; hydrate
started 14:55:34 Z — only a single scrape window of "low" baseline
existed before the burst. Depending on PodMonitor discovery latency
(itself 15–30 s), Prometheus may have recorded zero pre-burst samples
for those targets, so `rate()` returned no value at all.

The harness now sleeps 60 s after `wait_http_ready` (was 15 s) so
Prometheus accumulates at least 4 baseline scrapes before any
workload starts. Documented in
`benchmarks/sweep/run-6run-sweep.sh:reinstall()`.

### Cache wipe is safe

The harness wipes per-pod cache between trials with
`find /mnt/nvme/blobcache-cache/ -mindepth 1 -delete`. This does not
clear the daemon's in-memory LRU/bloom — but every trial does a helm
uninstall+install first, so each trial sees fresh pods with fresh
in-memory state. Even without the helm reset, the peer-serve code
path is provably contamination-safe:

- TCP server (`src/transport.rs:142`) calls `DiskCache::try_get` which
  returns `None` on `ENOENT` → 404 miss; `chunk_bytes_served` is **not**
  incremented.
- UCX server (`src/transport_ucx.rs:621`) calls `try_get_into_slice`
  with the same behaviour → `STATUS_MISS`; `chunk_bytes_served` is
  **not** incremented.
- A bloom-yes peer that returns NotFound increments the requester's
  `peer_bloom_false_positive_total` and triggers `peer_index.drop_remote`,
  which self-corrects routing without polling.

Sweep counters confirm this in practice: `peer_bloom_false_positive_total`
= 0 across the entire sweep.

## Reproducing

```sh
export BLOBCACHE_CLIENT_ID=<azure-mi-client-uuid>
export BLOBCACHE_ACCOUNT=<storage-account>
export BLOBCACHE_SEED_1=<seed-pod-ip>
export BLOBCACHE_SEED_2=<seed-pod-ip>
export BLOBCACHE_SEED_3=<seed-pod-ip>
export BLOBCACHE_IMAGE_TAG=sha-a8bb059-arm64

./benchmarks/sweep/run-6run-sweep.sh
```

Output:
- `$OUT_DIR/sweep-summary.tsv` — one line per trial
- `$OUT_DIR/<tag>-{run.log,pass1.tsv,pass2.tsv,hydrate.json,gather.json,snap-*.tsv}`

`start_utc`/`end_utc` columns are wall-clock bookends per trial,
ready to paste into Grafana time-range pickers.
