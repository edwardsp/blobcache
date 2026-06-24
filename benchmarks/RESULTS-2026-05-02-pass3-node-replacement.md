# RESULTS: PASS3 — node-replacement recovery on a warm 17-node cluster

**Date**: 2026-05-02 (UTC 04:33–04:47, 13.7 min wall)
**Branch**: `bench/sweep-pass3-node-replacement`
**Harness**: `benchmarks/e2e-hydrate-read.sh` (extended in this branch
  to add an optional third pass; see commit message)
**Cluster**: 17× GB300 (Grace+Blackwell, aarch64), one DaemonSet pod
  per node, host-network, NVMe-RAID-0 cache (~480 GiB usable each),
  UCX/RDMA peer transport on `rc_mlx5`, gossip + stats on TCP
**Workload**: `nvidia/DeepSeek-R1-0528-NVFP4-v2` — 163 `.safetensors`
  files, 413.33 GiB total, served fully replicated (every pod cached
  the entire prefix from a prior run; per-pod `blobcache_cache_bytes`
  ≈ 384.94 GiB on entry, aggregate ≈ 6.39 TiB)
**Storage**: account public access toggled on for the run window only;
  flipped back off immediately after PASS3 finished

## TL;DR

| | wiped pod (`node-15`) | other 16 pods (mean) | aggregate cluster |
|---|---:|---:|---:|
| **PASS1** (warm, 17/17 hot) | 234.7 s | 192.2 s | 27.0 GiB/s |
| **PASS2** (warm, 17/17 hot) | 234.7 s | 178.8 s | 27.9 GiB/s |
| **PASS3** (one pod cleared) | **304.9 s** | 180.9 s | 21.5 GiB/s |
| Δ PASS3 vs PASS2 | **+30.0 %** | +1.2 % | −22.9 % |

The pod whose cache was wiped paid a clean **+30 %** wall-time penalty
to peer-fetch its full 384.94 GiB share back from the other 16 nodes
during the read. The other 16 pods saw no measurable slowdown
(+1.2 % is within run-to-run noise on this cluster) despite each one
serving roughly 1/16 of the wiped pod's restored data over the peer
transport in parallel with their own local-cache reads. Cluster
aggregate read throughput dropped 22.9 % (27.9 → 21.5 GiB/s) and is
gated by the wiped pod's tail.

## Pass semantics (this run)

- **PASS1**: each pod reads every file in the prefix. Because the
  cluster was already fully hydrated on entry, this is "17/17 warm" —
  every chunk hits local NVMe.
- **PASS2**: identical workload, immediate repeat. Steady-state warm.
- **PASS3**: before the read, `POST /clear-cache-shard` is sent to
  exactly one pod (chosen via `WIPE_POD_INDEX`, default 1 to preserve
  the coordinator at index 0). That endpoint:
  1. drains in-flight inserts (so we don't drop bytes mid-write),
  2. removes every chunk file under `/mnt/nvme/blobcache-cache`,
  3. resets singleflight, peer-LRU, and inflight-write maps,
  4. rebuilds the pod's local bloom — peers see the gossip-bumped
     bloom version and stop routing chunks to it.
  Post-wipe `blobcache_cache_bytes` on the target pod is asserted to
  be 0 (logged in the e2e output before PASS3 starts). The same read
  workload as PASS2 then runs on all 17 pods in parallel; the wiped
  pod must peer-fetch its entire 384.94 GiB share from the other 16.

This simulates a single-node failure-and-replacement scenario: the
hostPath cache lives on `/mnt/nvme` so a pod restart alone would not
clear it, but replacing a node (or wiping its NVMe) does. The cluster
keeps serving reads from the surviving 16 while the new node refills.

## Per-pod walls

`benchmarks/results/2026-05-02-pass3-node-replacement-pass{1,2,3}.tsv`
(pod identifiers anonymised to `node-NN` for check-in; rows are sorted
by PASS1 wall ascending, so `node-00` is the fastest pod and `node-16`
is the slowest. The wiped pod is `node-15` — chosen by `WIPE_POD_INDEX=1`
on the second pod returned by the kubectl pod listing, which on this run
was the second-slowest pod in the load-ordered ranking).

```
pod         P1 s    P2 s    P3 s   GiB/s P1  GiB/s P2  GiB/s P3
node-00    133.4   122.7   135.0     2.88      3.14      2.85
node-01    136.1   136.0   164.7     2.83      2.83      2.34
node-02    140.2   127.3   147.0     2.75      3.02      2.62
node-03    164.7   140.6   126.1     2.34      2.74      3.05
node-04    181.6   198.3   196.8     2.12      1.94      1.96
node-05    183.6   148.4   123.6     2.10      2.59      3.11
node-06    189.8   140.2   176.4     2.03      2.74      2.18
node-07    196.0   201.2   208.8     1.96      1.91      1.84
node-08    199.8   229.7   199.1     1.93      1.68      1.93
node-09    203.1   215.2   203.7     1.90      1.79      1.89
node-10    205.9   176.8   188.2     1.87      2.18      2.05
node-11    218.6   185.4   214.0     1.76      2.08      1.80
node-12    223.0   214.1   177.5     1.73      1.80      2.17
node-13    223.4   221.5   218.1     1.72      1.74      1.76
node-14    233.0   186.4   194.8     1.65      2.07      1.98
node-15    234.7   234.7   304.9     1.64      1.64      1.26   <-- WIPED before PASS3
node-16    242.5   216.4   220.7     1.59      1.78      1.74
```

Top three PASS3 − PASS2 deltas: `node-15 +70.2 s` (the wiped pod),
`node-06 +36.1 s`, `node-01 +28.7 s`. The two non-wiped pods that
gained 30+ s in PASS3 are within the run-to-run variance seen on this
cluster between PASS1 and PASS2 (e.g. `node-08` swung 199.8 → 229.7
between PASS1 and PASS2 with no intervention) — there is no second
"degraded" signal beyond the wipe target.

## Wipe call cost

`POST /clear-cache-shard` returned in **40.3 s** for the wiped pod
(server-side `elapsed_ms=39273`; daemon removed 98 626 chunks ≈
413.33 GB from disk, drained inflight, reset state, rebuilt bloom).
PASS3 read started immediately after.

## Observations and caveats

- The `/clear-cache-shard` endpoint correctly rebuilds the local bloom
  before returning, so peers stopped advertising the wiped pod as a
  candidate within one gossip round. The 16 surviving pods routed
  their incoming `node-15`-shard requests via the standard rendezvous
  (HRW) fallback as if the chunks had never been there.
- The wiped pod's PASS3 throughput (1.26 GiB/s) reflects the
  *aggregate* of (a) re-fetching 384.94 GiB over 16 peer connections,
  (b) writing them through to NVMe, and (c) FUSE-serving them to the
  read syscalls, all under a single tokio runtime per pod. The
  per-peer transport itself is well below saturation here — the
  bottleneck is the daemon's main runtime, the same software ceiling
  documented in `BENCHMARKS.md` for single-stream warm-peer reads.
- Other-pod throughput is essentially flat (180.9 s vs 178.8 s,
  +1.2 %) because each surviving pod was serving roughly
  384.94 / 16 ≈ 24 GiB of outbound peer requests on top of its own
  local read. The peer-fetch path runs on a separate tokio task pool
  and the fan-out hides the additional load.
- Aggregate cluster wall-clock (max-over-pods) is gated by the
  slowest pod (always the wiped one in PASS3), which is why the
  aggregate GiB/s is the right summary statistic for "what the
  cluster delivered" — it captures the failure-recovery tail.
- This run is on an *already-warm* cluster. A clean cold-start sweep
  (wipe-all → hydrate → PASS1/2/3) would change PASS1 (origin-fetch
  warmup) but is expected to leave the PASS3 delta vs PASS2 essentially
  unchanged, because PASS3's cost is purely the peer-fetch refill of
  one pod's share, independent of how the cluster got warm in the
  first place.

## Reproduction

```sh
# storage account public access ON (vnet rules still apply)
deploy/storage-access.sh on

PATH_PREFIX="nvidia_DeepSeek-R1-0528-NVFP4-v2/" \
  READ_GLOB="*.safetensors" \
  LOG=/tmp/e2e-pass3.log \
  benchmarks/e2e-hydrate-read.sh

# storage account public access OFF (mandatory)
deploy/storage-access.sh off
```

Optional knobs:
- `SKIP_PASS3=1` — runs the original two-pass workflow (backward-compatible)
- `WIPE_POD_INDEX=N` — 0-indexed line in `/tmp/podnodes.txt` to wipe (default `1`)
- `WIPE_TIMEOUT_S=120` — `curl --max-time` for the `/clear-cache-shard` call

## Artifacts (sanitised, in this commit)

- `benchmarks/results/2026-05-02-pass3-node-replacement-pass1.tsv`
- `benchmarks/results/2026-05-02-pass3-node-replacement-pass2.tsv`
- `benchmarks/results/2026-05-02-pass3-node-replacement-pass3.tsv`
- `benchmarks/results/2026-05-02-pass3-node-replacement-e2e.log`
