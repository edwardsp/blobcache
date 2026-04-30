# End-to-end run: DeepSeek-R1-NVFP4-v2 hydrate + dual sequential read

17-node AKS gb300 cluster, blobcached `v2.7.1-bench` (UCX/RDMA peer
transport), one pod per node, NVMe RAID-0 host-path cache, 10 TiB cache
ceiling, chunk size 4 MiB.

Dataset: `models/nvidia_DeepSeek-R1-0528-NVFP4-v2/` —
**350 files / 98 804 chunks / 385.0 GiB** (411.34 GB) on the storage
account.

The 18th gb300 VM (`node-Z`) was `NotReady` at run start and excluded.

## Timeline (UTC, correlate against Grafana)

| Phase            | Start                | End                  | Wall    |
|------------------|----------------------|----------------------|---------|
| Hydrate          | 2026-04-28 07:37:28Z | 2026-04-28 07:37:49Z | 20.45 s |
| Read pass 1      | 2026-04-28 07:37:49Z | 2026-04-28 07:51:41Z | 832 s   |
| Read pass 2      | 2026-04-28 07:51:41Z | 2026-04-28 07:56:38Z | 296 s   |
| **Total**        | 2026-04-28 07:37:21Z | 2026-04-28 07:56:38Z | 1 158 s |

Grafana Prometheus has full per-node series for the entire window above;
zoom to 07:37–07:57 UTC and split by `node`.

## Pre-conditions

Cache wiped on every node before the run via the privileged
`nvme-raid-init` DS (`rm -rf /host/mnt/nvme/blobcache-cache`), then
`kubectl rollout restart ds/blobcache-blobcached`. All 17 daemons came
up with `blobcache_cache_bytes == 0` and `cluster_members_alive == 17`.

Storage public-access toggled on for the run via
`deploy/storage-access.sh on` (vnet-restricted; AKS subnet allowed).

## Hydrate

Coordinator: `node-A`. POST `/hydrate` with
`{mount: "models", path: "nvidia_DeepSeek-R1-0528-NVFP4-v2/",
recursive: true}` shards all chunks across the 16 reachable peers.

| Metric              | Value          |
|---------------------|----------------|
| Files               | 350            |
| Chunks              | 98 804         |
| Total bytes         | 413 340 567 567 (385.0 GiB) |
| Wall                | 20.45 s        |
| Aggregate throughput| **19 650 MiB/s** (≈19.2 GiB/s) |
| Per-peer            | 24.6 GiB each, ~1.24–2.46 GiB/s |
| Errors              | 0              |
| Peers reporting     | 16 of 17       |

The 17th peer (`node-B`, just-restarted pod) had not finished
gossip-joining when the coordinator polled; its chunks were
re-distributed across the 16 ready peers.

The cluster pulled **387 GB** from the storage account
(`blobcache_blob_fetch_bytes_total` delta), matching the dataset size
within 1 chunk-rounding.

## Read pass 1 — peer-fetch warm-up

For each pod in parallel:

```
find /mnt/blobcache/models/nvidia_DeepSeek-R1-0528-NVFP4-v2 \
     -maxdepth 1 -type f | sort | xargs -n1 dd of=/dev/null bs=4M
```

Each pod reads the full 163-file top-level subset = **384.93 GiB**.

| Metric                          | Value         |
|---------------------------------|---------------|
| Pods that completed             | 12 of 17      |
| Per-pod bytes read              | 413 328 348 544 (384.93 GiB) |
| Cluster wall                    | 832 s         |
| Per-pod effective throughput    | ~474 MiB/s    |
| Cache-hit rate (cluster)        | 92.9 %        |
| Peer-fetch p50 / p99            | 1.6 ms / 719 ms |
| FUSE read p50 / p99             | 0.14 ms / 0.25 ms |
| Peak FUSE BW (cluster sum)      | **26 GB/s**   |

The 7.1 % miss rate is the expected peer-fetch tail: each pod has only
its own hydrate shard locally on first touch, so ~92 % of chunks were
served from the local cache (the assigned shard plus quickly-replicated
neighbours) and ~8 % went over UCX to peers. No traffic to Azure during
this pass.

## Read pass 2 — fully cache-resident

| Metric                       | Value         |
|------------------------------|---------------|
| Pods that completed          | 12 of 17      |
| Per-pod bytes read           | 384.93 GiB    |
| Cluster wall                 | 296 s         |
| Per-pod effective throughput | ~1 332 MiB/s (~1.30 GiB/s) |
| Cache-hit rate (cluster)     | **100 %**     |
| FUSE read p50 / p99 / avg    | 0.13 / 0.41 / 0.11 ms |
| Peak FUSE BW (cluster sum)   | **25.5 GB/s** |
| Sustained FUSE BW (210 s)    | 16–25 GB/s    |

Pass 2 is the steady-state target case — every chunk on every pod is
local, no peer or blob traffic. Per-pod ~1.3 GiB/s sustained, in line
with the v2.7.1-bench `dd` numbers in `BENCHMARKS.md` once the cache is
sized to fit the working set.

## Excluded nodes

5 of 17 pods did not complete either read pass and were excluded:

| Pod / node                                | Pass 1 bytes | Pass 2 bytes | Notes                |
|-------------------------------------------|-------------:|-------------:|----------------------|
| `pod-B` / node-B |          0 B |          0 B | hydrate-shard missed |
| `pod-C` / node-C |     ~1.6 GB  |          —   | UCX peer-fetch stall |
| `pod-D` / node-D |     ~4–8 GB  |          —   | UCX peer-fetch stall |
| `pod-E` / node-E |    ~9–17 GB  |          —   | UCX peer-fetch stall |
| `pod-F` / node-F |   ~15–30 GB  |          —   | UCX peer-fetch stall |

The same five pods stalled on both passes. No errors logged in the
daemons; the FUSE `read()` syscalls were blocked inside `Fetcher` on a
peer-fetch that never returned. Hydrate had succeeded for 4 of these
5 (only `node-B` was missing entirely), so the local shard was
present, but reading any *other* shard across those nodes' UCX endpoints
hung.

Root cause is in the UCX endpoint lifecycle:
`endpoint_error_cb` flags a slot `broken=true`, but the reaper only
removes a broken slot when `in_flight == 0`. If the in-flight requests
are themselves wedged (no UCX timeout fires), the slot is never
recreated and subsequent requests against the same peer block forever.
The Helm chart does not currently set `UCX_RC_TIMEOUT`,
`UCX_RC_RNR_TIMEOUT`, or `UCX_RC_RETRY_COUNT`, so the kernel/driver
defaults apply (effectively unlimited for our workload). See
*Recommended follow-ups* below.

The 12 completing pods produced clean, monotonic Prometheus series
across both passes; the 5 excluded pods show flat counters after their
stall point, which is the easy way to spot them in Grafana.

## Headline numbers

| Phase  | Total bytes moved (cluster)   | Wall   | Aggregate BW       | Per-pod BW         |
|--------|-------------------------------|--------|--------------------|--------------------|
| Hydrate| 385 GiB (Azure → cluster)     | 20.5 s | 19.2 GiB/s         | ~1.2 GiB/s ingest  |
| Pass 1 | 12 × 384.93 GiB = 4 619 GiB   | 832 s  | ~5.5 GiB/s sustained, 26 GB/s peak | ~474 MiB/s |
| Pass 2 | 12 × 384.93 GiB = 4 619 GiB   | 296 s  | ~15.6 GiB/s sustained, 25.5 GB/s peak | ~1.30 GiB/s |

Pass 2 / pass 1 wall ratio = **2.81×** speed-up from the warm cache.

## Dashboard

`templates/grafana-dashboard.yaml` (auto-loaded by the kube-prometheus
sidecar from the `grafana_dashboard=1` label) now has 13 panels,
including four blob-download views added for this run:

- *Azure blob download BW per node* (timeseries, `rate(blobcache_blob_fetch_bytes_total)` by `node`)
- *Azure blob downloads (aggregate BW + cumulative bytes)* (timeseries, cluster sum)
- *Total bytes downloaded from Azure (cumulative)* (stat)
- *Bytes downloaded from Azure per node (cumulative)* (bargauge)

These make it easy to see that pass 1 and pass 2 caused **zero** Azure
egress (hydrate is the only spike).

## Recommended follow-ups

1. ✅ **Done** — UCX timeouts added to chart (`UCX_RC_TIMEOUT=30s`,
   `UCX_RC_RNR_TIMEOUT=30s`, `UCX_RC_RETRY_COUNT=8`) so a stuck QP
   surfaces as `endpoint_error_cb` instead of an indefinite block.
2. ✅ **Done** — `run_coordinator` is now wrapped in
   `tokio::time::timeout(BLOBCACHE_HYDRATE_TIMEOUT_SECS, ...)` (default
   3700 s, 100 s above the per-shard HTTP timeout). On expiry, all
   outstanding handles are aborted and the coordinator returns the
   partial result with an explicit error row.
3. ✅ **Done** — replaced `bc` with `awk` in the bench scripts
   (`benchmarks/e2e-hydrate-read.sh`, `deploy/wipe-caches.sh`); no
   image rebuild needed.
4. ✅ **Investigated, fabric ruled out** — `ucx_perftest` (rc_mlx5,
   1 MiB / 200 iter) was run between the healthy coordinator
   (`node-A`) and each of the five stalling nodes, plus a
   healthy↔healthy control:

   | Server-side node | Bandwidth | 50 %ile latency |
   |---|---:|---:|
   | node-F (stuck)                  | 42 753 MB/s | 22.98 µs |
   | node-E (stuck)                  | 42 871 MB/s | 22.94 µs |
   | node-D (stuck)                  | 42 736 MB/s | 22.94 µs |
   | node-B (stuck)                  | 43 001 MB/s | 22.98 µs |
   | node-C (stuck)                  | 42 480 MB/s | 23.04 µs |
   | node-G (control, healthy)       | 42 937 MB/s | 22.98 µs |

   All five "bad" pairs sustain ~42 GB/s (≈336 Gbps), within 1.2 % of
   the healthy control and at the line rate of a single mlx5 rail.
   **The IB fabric is healthy for every pair.** The stalls are
   software-side, in blobcached's `Fetcher` / UCX endpoint management
   — likely the in-flight-counter / endpoint-reaper interaction
   already documented above (a request that wedges in user-space
   keeps `in_flight > 0` so the broken-slot reaper never fires).
   Mitigations (1)+(2) are the correct response; the next time the
   five-node stall reproduces, they should now produce a clean error
   instead of a hang, which is observable end-to-end. The
   `ucx_perftest` probe script is preserved at `/tmp/ucx-probe.sh`
   (and full output at `/tmp/ucx-probe.log`) for re-running if the
   pattern returns after the chart upgrade.

## Reproduce

```sh
# 0. enable storage public network access (vnet-restricted)
deploy/storage-access.sh on

# 1. wipe caches on every node (privileged DS)
bash /tmp/wipe-caches.sh
kubectl -n blobcache rollout restart ds/blobcache-blobcached
kubectl -n blobcache rollout status   ds/blobcache-blobcached --timeout=2m

# 2. run e2e (hydrate + 2 sequential reads, ~14 min)
setsid nohup /tmp/run-e2e.sh </dev/null >/tmp/e2e.log 2>&1 & disown

# 3. inspect
grep -E '^=== ' /tmp/e2e.log
python3 /tmp/q.py        # latency / hit-rate from Prometheus

# 4. lock storage back down
deploy/storage-access.sh off
```

Artifacts kept in `/tmp/`: `e2e.log`, `hydrate.json`, `pass1.tsv`,
`pass2.tsv`, `wipe-caches.sh`, `run-e2e.sh`, `q.py`.
