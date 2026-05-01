# Results: Tier-1 baseline 6-run sweep (gather vs sharded × cacheOnPeerFetch)

**Branch**: `perf-fuse-splice` (pre-Tier-2)  **Image**: `sha-f3a1b2a-arm64`
**Date**: 2026-04-30 (run 17:48 → 18:57 UTC)  **Cluster**: 17× GB300 nodes, ns `blobcache`
**Dataset**: `nvidia_DeepSeek-R1-0528-NVFP4-v2/` — 350 files, 413.3 GiB, 98 804 chunks of 4 MiB
**Per-pod sharded footprint**: ~24 GiB (~5 812 chunks)

## TL;DR

| Config | Trial | Hydrate (s) | Gather (s) | PASS1 (s) | PASS2 (s) |
|---|---|---:|---:|---:|---:|
| **C1** cacheOff + sharded + **gather** | t1 | 17.6 | 131.0 | **203.3** | 219.6 |
| **C1** cacheOff + sharded + **gather** | t2 | 1.0 † | 154.7 | **220.3** | 221.6 |
| **C2** cacheOff + sharded (no gather) | t1 | 18.8 | – | **310.4** | 299.2 |
| **C2** cacheOff + sharded (no gather) | t2 | 20.1 | – | **269.1** | 259.5 |
| **C3** cacheOn  + sharded (no gather) | t1 | 19.9 | – | **293.3** | 236.1 |
| **C3** cacheOn  + sharded (no gather) | t2 | 24.9 | – | **311.9** | 214.2 |

† c1-t2 hydrate=1.0 s is an outlier (page-cache warm from t1); the real work
is in the gather phase that follows.

### Headline takeaways

1. **C1 (gather mode) is the clear PASS1 winner** at 203–220 s — every chunk
   is on every NVMe so PASS1 is 100 % local-cache reads. PASS2 ≈ PASS1
   because both passes are local-NVMe-bound.
2. **C2 sharded + cacheOff** is 30–50 % slower on PASS1 (269–310 s) — pod
   reads its 1/17 from local NVMe + 16/17 from peers over IB.  PASS2
   stays at 260–300 s because peer-fetched chunks are **not** inserted
   into the local cache; PASS2 re-fetches over the wire identically.
3. **C3 sharded + cacheOn**: PASS1 looks like C2 (293–312 s) but PASS2
   drops to 214–236 s — peer fetches **do** populate the local cache on
   PASS1, so PASS2 hits NVMe.  The PASS1→PASS2 delta of ~80 s is the
   value of `cacheOnPeerFetch=true`.
4. **Gather costs 131–155 s upfront** but saves ~100 s on every subsequent
   PASS.  Worth it if the dataset is read more than once.

## Configuration

### Identical knobs across all 6 runs

| Knob | Value |
|---|---|
| Pods / nodes | 17 (1 per GB300 node, DaemonSet) |
| Image | `sha-f3a1b2a-arm64` (includes `spawn_blocking` peer-cache lookup) |
| `azure.workers` (main runtime threads) | **8** |
| `transport.kind` | `rdma` (UCX over IB) |
| `transport.chunk_concurrency` | **32** |
| `transport.peer_concurrency` | 8 |
| `cache.chunkSize` | **4 MiB** (4 194 304) |
| `cache.peerLruBytes` | 1 GiB (in-memory peer LRU; used when `cacheOnPeerFetch=false`) |
| Mount | RDMA over IB; pods request `rdma/ib: 1`; hostPath `/mnt/nvme` (NVMe RAID-0, 14 TB) |
| Wipe between trials | `helm uninstall && helm install` (forces fresh bloom + clean NVMe) |

### The 3 setups (only 2 knobs vary)

| Config | `cacheOnPeerFetch` | Hydrate mode | Sequence |
|---|:---:|---|---|
| **C1** gather + cacheOff | `false` | `broadcast` then explicit gather | `hydrate → 30 s → gather → 30 s → PASS1 → 10 s → PASS2` |
| **C2** shard + cacheOff  | `false` | `default` (sharded, no gather)   | `hydrate → 30 s → PASS1 → 10 s → PASS2` |
| **C3** shard + cacheOn   | `true`  | `default` (sharded, no gather)   | `hydrate → 30 s → PASS1 → 10 s → PASS2` |

Helm values overlays (under `deploy/helm/blobcache/`):

- `values-cache-peer-off.yaml` — `cache.cacheOnPeerFetch: false` (used for C1, C2)
- `values-cache-peer-on.yaml`  — `cache.cacheOnPeerFetch: true`  (used for C3)

## Reproduction

The orchestrator was an ad-hoc script at `/tmp/diag-strag/run-all-6.sh`
(not committed; throwaway).  It calls the committed
`benchmarks/diag-run.sh` once per trial.  To reproduce, drive
`diag-run.sh` directly with these env vars (one trial per invocation,
helm-reinstall between every trial):

```bash
NS=blobcache
RELEASE=blobcache
CHART=deploy/helm/blobcache
VALUES_OFF=deploy/helm/blobcache/values-cache-peer-off.yaml
VALUES_ON=deploy/helm/blobcache/values-cache-peer-on.yaml

reinstall() {                       # $1 = values file
  helm -n "$NS" uninstall "$RELEASE" --wait || true
  helm -n "$NS" install   "$RELEASE" "$CHART" -f "$1" --wait
  # Wait for all pods Ready before running the next trial.
  kubectl -n "$NS" rollout status ds/blobcached --timeout=10m
}

# --- C1: cacheOff + gather, 2 trials ---
for t in t1 t2; do
  reinstall "$VALUES_OFF"
  HYDRATE_MODE=broadcast \
    POST_HYDRATE_SLEEP_S=30 \
    RUN_GATHER=1 POST_GATHER_SLEEP_S=30 \
    RUN_PASS2=1 POST_PASS1_SLEEP_S=10 \
    TAG="c1-cacheoff-gather-$t" \
    benchmarks/diag-run.sh
done

# --- C2: cacheOff + sharded, 2 trials ---
for t in t1 t2; do
  reinstall "$VALUES_OFF"
  POST_HYDRATE_SLEEP_S=30 \
    RUN_PASS2=1 POST_PASS1_SLEEP_S=10 \
    TAG="c2-cacheoff-shard-$t" \
    benchmarks/diag-run.sh           # default HYDRATE_MODE = sharded
done

# --- C3: cacheOn + sharded, 2 trials ---
for t in t1 t2; do
  reinstall "$VALUES_ON"
  POST_HYDRATE_SLEEP_S=30 \
    RUN_PASS2=1 POST_PASS1_SLEEP_S=10 \
    TAG="c3-cacheon-shard-$t" \
    benchmarks/diag-run.sh
done
```

Per-trial outputs land in `benchmarks/out/${TAG}-{run.log,pass1.tsv,pass2.tsv,hydrate.json,gather.json,snap-{before,after,before2,after2}.tsv}`.

### Wall time

Round-4 wall: **~70 minutes** for all 6 trials including helm reinstalls.

## Bugs uncovered during the sweep (all fixed before this clean round)

These were found in the broken Round-3 attempt at ~15:00 UTC and fixed
before this Round-4 clean re-run.  All three are now on `main`.

1. **Harness wipe broken** — `rm -f *` over the cache dir hit `ARG_MAX`
   silently (98k chunk files).  Fixed in `benchmarks/diag-run.sh` by
   using `find <dir> -mindepth 1 -delete`.
2. **Snapshot capture broken** — the per-pod stats snapshot ran in a
   backgrounded subshell whose stdout was never collected.  Fixed by
   writing to per-pod tmp files and concatenating after `wait`.
3. **Bloom filter never cleared in daemon** — wipe-then-trial scenarios
   produced catastrophic peer-miss + blob-fallback storms (~146 k
   bloom false-positives per pod; PASS1 took 1 h 45 m before kill on
   Round 3).  Worked around in this sweep by `helm uninstall && install`
   between every trial; properly fixed in commit `f2fc31c` and the
   subsequent `fix-bloom-stale` series that landed on `main`.

The fix commit for items (1) and (2) is `f2fc31c`
("bench: fix wipe (find -delete) + snapshot capture; add RUN_GATHER").

## Notes for future runs

- This table is the **Tier-1 baseline** that Tier-2 changes (PR #4,
  `feat/observability-v2`: gauges + bloom histo + async hydrate +
  request-id propagation) should be measured against.
- The 3 configs and their `[[mounts]]`/wait sequence are stable across
  runs; only the **image** and any code-level knob changes between
  Tier-2 vs Tier-1 should differ.
- For comparison runs, capture **exact UTC start and end timestamps**
  per trial so Grafana can be cross-referenced.  `diag-run.sh` already
  prints `=== POST_HYDRATE_SLEEP_START ===` markers.
