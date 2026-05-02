# Tier-2 sweep + PASS3 (single-node failure-and-replacement)

| | |
|---|---|
| **Date** | 2026-05-02 |
| **Branch** | `bench/sweep-pass3-node-replacement` |
| **Image**  | `sha-7785a86-arm64` |
| **Cluster** | 17× GB300 (aarch64, 8× NVMe RAID-0, IB), UCX/RDMA peer transport |
| **Dataset** | `nvidia_DeepSeek-R1-0528-NVFP4-v2/` — 350 files, 98 804 chunks, 413.33 GB total per pod read |
| **Sweep wall** | 06:54Z–08:56Z (~2 h 02 min, 6 trials) |
| **Harness** | `benchmarks/sweep/run-6run-sweep.sh` (extended) + `benchmarks/diag-run.sh` (extended with `RUN_PASS3`) |

## What's new vs `RESULTS-2026-05-01-tier2-startupprobe-sweep.md`

This sweep re-runs the canonical 3-config × 2-trial tier-2 protocol
(C1 = cacheOff + sharded + gather, C2 = cacheOff + sharded, C3 = cacheOn
+ sharded), and appends a **PASS3** stage to every trial. PASS3 wipes
exactly one pod's local cache via the in-tree `POST /clear-cache-shard`
admin endpoint (drains in-flight inserts, removes every chunk file,
resets singleflight + peer-LRU + inflight-write maps, rebuilds the local
bloom), then re-runs the same parallel read workload on all 17 pods.
This simulates a single node failing and being replaced with empty NVMe
storage: the wiped pod must peer-fetch its full cache share back from
the surviving 16 nodes while the cluster continues serving.

## TL;DR

| Config | Trial | Hydrate (s) | Gather (s) | PASS1 (s) | PASS2 (s) | **PASS3 (s)** | Wiped pod |
|---|---|---:|---:|---:|---:|---:|---|
| **C1** cacheOff + sharded + **gather** | t1 | 164.4 | 16.9 | 212.6 | 234.7 | **228.1** | node-00 |
| **C1** cacheOff + sharded + **gather** | t2 | 163.6 | 16.3 | 215.9 | 231.8 | **250.2** | node-17 |
| **C2** cacheOff + sharded (no gather) | t1 | 17.2  | – | 306.5 | 315.4 | **614.3** | node-44 |
| **C2** cacheOff + sharded (no gather) | t2 | 17.7  | – | 332.6 | 311.1 | **616.9** | node-56 |
| **C3** cacheOn  + sharded (no gather) | t1 | 16.8  | – | 318.6 | 218.8 | **281.2** | node-68 |
| **C3** cacheOn  + sharded (no gather) | t2 | 17.4  | – | 326.4 | 223.3 | **298.2** | node-90 |

All 6 trials report `hyd_status=ok` (every peer fetched its full
assigned shard with zero errors).

## Headline takeaways

### 1. PASS3 cost depends sharply on `cacheOnPeerFetch`

Mean PASS3 wall vs PASS2 wall (steady-state warm peers, same workload):

| Config | PASS2 (s, mean) | PASS3 (s, mean) | Δ | Notes |
|---|---:|---:|---:|---|
| **C1** cacheOff + gather | 233.3 | **239.2** | **+2.5 %** | every pod has full local copy from gather; PASS3 = effectively warm |
| **C2** cacheOff + shard  | 313.3 | **615.6** | **+96.5 %** | wiped pod must peer-fetch every chunk *and* peer cache stays cold |
| **C3** cacheOn  + shard  | 221.1 | **289.7** | **+31.0 %** | wiped pod refills via peers, but those peers cache what they fetch |

The C2 result is the key finding: **without `cacheOnPeerFetch`, a
single-node replacement nearly doubles cluster read time** because
every read on the wiped pod is a peer round-trip with no local
materialisation, and the cluster aggregate is gated entirely by the
wiped pod's tail.

### 2. C2-PASS3 reveals a serialised refill ceiling, not a peer ceiling

In C2-PASS3 the wiped pod's wall (612.7 s, t1) is essentially identical
to the next-slowest pod's wall (613.2 s) — **all 17 pods finished
within 0.9 s of each other**:

| Stat | C2-t1 PASS3 | C2-t2 PASS3 |
|---|---:|---:|
| min wall | 612.3 s | 615.4 s |
| mean | 612.9 s | 615.5 s |
| max | 613.2 s | 615.8 s |
| spread | 0.9 s | 0.4 s |

Compare PASS2 (no wipe, same config) where spread is ~115 s. PASS3 in
C2 is bandwidth-coupled across the entire cluster: the 16 surviving
pods can't make progress on their *own* reads any faster than they can
also serve the wiped pod's chunks back to it, because every peer-served
byte never lands in the requester's local cache (so subsequent reads of
the same chunk by *any* pod re-fetch from the same peer).

C3 breaks this lock-in: peer-served bytes are cached locally on receipt,
so the wiped pod's tail (mean 288.6 s) decouples from the others'
recovery bandwidth (mean 191.4 s) — a 97 s gap.

### 3. C1's PASS3 is essentially free

Because C1's gather phase replicates every chunk to every node *before*
PASS1, the wiped pod can refill from peers that all already have full
local copies (no peer-side fetch amplification). C1 PASS3 = 239.2 s vs
PASS2 = 233.3 s, a noise-level +2.5 %. This makes C1 the most
recovery-resilient configuration measured.

### 4. Aggregate throughput collapse is config-dependent

Cluster aggregate throughput (max-pod-wall) during PASS3 vs PASS2:

| Config | PASS2 agg (GiB/s) | PASS3 agg (GiB/s) | Δ |
|---|---:|---:|---:|
| C1 (gather)        | 28.2 | 27.6 | −2 % |
| C2 (cacheOff)      | 21.0 | **10.6** | **−49 %** |
| C3 (cacheOn)       | 29.8 | 22.7 | −24 % |

## Per-config per-pod breakdown

Per-pod walls in seconds; "wiped" column marks the cache-wiped pod;
"others (mean / min / max)" summarises the surviving 16. Pod IDs are
sanitised (`node-NN`); per-trial TSVs in `benchmarks/results/2026-05-02-pass3-sweep/`.

### C1 — cacheOff + sharded + gather

| Trial | Pass | Wiped | Others mean | Others min | Others max | Cluster agg |
|---|---|---:|---:|---:|---:|---:|
| t1 | PASS1 | 143.5 | 182.0 | 150.5 | 211.3 | 30.97 GiB/s |
| t1 | PASS2 | 181.6 | 183.6 | 130.9 | 233.6 | 28.02 GiB/s |
| t1 | PASS3 | 202.6 | 169.4 | 131.2 | 226.9 | 28.84 GiB/s |
| t2 | PASS1 | 150.4 | 185.8 | 155.3 | 214.7 | 30.48 GiB/s |
| t2 | PASS2 | 191.1 | 184.3 | 135.8 | 230.6 | 28.37 GiB/s |
| t2 | PASS3 | 249.0 | 175.1 | 137.6 | 198.3 | 26.28 GiB/s |

### C2 — cacheOff + sharded (no gather)

| Trial | Pass | Wiped | Others mean | Others min | Others max | Cluster agg |
|---|---|---:|---:|---:|---:|---:|
| t1 | PASS1 | 285.7 | 269.4 | 208.8 | 305.2 | 21.44 GiB/s |
| t1 | PASS2 | 277.0 | 256.7 | 199.7 | 314.2 | 20.83 GiB/s |
| t1 | PASS3 | **612.7** | **612.9** | 612.3 | 613.2 | **10.67 GiB/s** |
| t2 | PASS1 | 245.5 | 266.8 | 200.3 | 331.3 | 19.75 GiB/s |
| t2 | PASS2 | 247.2 | 263.2 | 204.3 | 309.9 | 21.12 GiB/s |
| t2 | PASS3 | **615.5** | **615.5** | 615.4 | 615.8 | **10.63 GiB/s** |

### C3 — cacheOn + sharded (no gather)

| Trial | Pass | Wiped | Others mean | Others min | Others max | Cluster agg |
|---|---|---:|---:|---:|---:|---:|
| t1 | PASS1 | 229.5 | 274.9 | 229.8 | 317.4 | 20.62 GiB/s |
| t1 | PASS2 | 203.5 | 176.1 | 131.7 | 217.6 | 30.07 GiB/s |
| t1 | PASS3 | 279.8 | 184.6 | 122.7 | 220.8 | 23.39 GiB/s |
| t2 | PASS1 | 265.7 | 276.8 | 235.7 | 325.0 | 20.13 GiB/s |
| t2 | PASS2 | 160.6 | 194.8 | 120.8 | 222.1 | 29.46 GiB/s |
| t2 | PASS3 | 297.0 | 198.2 | 172.6 | 223.0 | 22.03 GiB/s |

## Operational notes

### Wipe call cost

`POST /clear-cache-shard` returned in 30–45 s per trial (server-side
removal of ~98 k chunks ≈ 384 GiB plus drain, reset, and bloom
rebuild). Logged per trial as `WIPE_END … rc=0 wall=…s` in each
`<trial>-run.log`.

### Wipe target selection

The harness wipes the pod at `WIPE_POD_INDEX=1` (the second pod from
the running-pods listing) so the coordinator (index 0, which receives
the hydrate POST) is never wiped. Each helm reinstall produces fresh
pod names, so the wiped pod identity rotates across trials.

### Storage account public access

Toggled `Enabled` at sweep start, `Disabled` immediately after sweep
completion. Verified `publicAccess=Disabled` in ARM at end of run.

## Reproducing

```sh
# Cluster prerequisites: helm release deployed, builder pod present,
# storage account public-access toggled on for the run window.
export BLOBCACHE_CLIENT_ID=<azure-mi-client-uuid>
export BLOBCACHE_ACCOUNT=<storage-account>
export BLOBCACHE_SEED_1=<seed-pod-ip>
export BLOBCACHE_SEED_2=<seed-pod-ip>
export BLOBCACHE_SEED_3=<seed-pod-ip>
export BLOBCACHE_IMAGE_TAG=sha-<short>-arm64

# Optional: WIPE_POD_INDEX=1  (default; index 0 = coordinator, never wipe)
./benchmarks/sweep/run-6run-sweep.sh
```

Output: per-trial `pass{1,2,3}.tsv`, `hydrate.json`, `gather.json`,
`snap-{before,after,before2,after2,before3,after3}.tsv`, `wipe.json`,
`run.log` plus `sweep-summary.tsv` with one line per trial:

```
tag start_utc end_utc hydrate_s gather_s pass1_s pass2_s pass3_s wiped_pod hyd_status
```

Per-trial sanitised artefacts for this sweep are in
`benchmarks/results/2026-05-02-pass3-sweep/`.
