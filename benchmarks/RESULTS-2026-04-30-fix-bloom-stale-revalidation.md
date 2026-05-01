# Results: `fix-bloom-stale` re-validation 6-run sweep

**Branch**: `fix-bloom-stale` (off `main@f2fc31c`)  **Image**: `sha-9c718ac-arm64`
**Date**: 2026-04-30 (run 22:22 → 23:28 UTC, ~66 min)  **Cluster**: 17× GB300 nodes, ns `blobcache`
**Dataset**: `nvidia_DeepSeek-R1-0528-NVFP4-v2/` — 350 files, 413.3 GiB, 98 804 chunks of 4 MiB

## TL;DR

Re-validation of the bloom-staleness fix series on the same 3-config × 2-trial
matrix as the Tier-1 baseline (`RESULTS-2026-04-30-tier1-baseline-6run-sweep.md`).
**No regressions; 5/6 PASS2 trials improved.**  The `fix-bloom-stale` branch
(commits `c8f73bd`, `b2a5723`, `9c718ac`) was subsequently merged.

| Config | Trial | Start (UTC) | End (UTC) | Hydrate (s) | Gather (s) | PASS1 (s) | PASS2 (s) |
|---|---|---|---|---:|---:|---:|---:|
| **C1** cacheOff + sharded + **gather** | t1 | 22:22:59 | 22:33:16 | 30.27 |  88.40 | **206.78** | 211.54 |
| **C1** cacheOff + sharded + **gather** | t2 | 22:34:06 | 22:45:18 | 20.60 | 147.08 | **216.27** | 207.46 |
| **C2** cacheOff + sharded (no gather) | t1 | 22:46:08 | 22:56:36 | 26.30 | – | **282.79** | 268.72 |
| **C2** cacheOff + sharded (no gather) | t2 | 22:57:26 | 23:07:01 | 18.97 | – | **258.80** | 247.81 |
| **C3** cacheOn  + sharded (no gather) | t1 | 23:07:52 | 23:17:48 | 16.51 | – | **304.29** | 225.73 |
| **C3** cacheOn  + sharded (no gather) | t2 | 23:18:38 | 23:28:39 | 20.54 | – | **307.63** | 222.28 |

### Comparison vs Tier-1 baseline (`sha-f3a1b2a-arm64`)

PASS1 (lower is better):

| Config | Tier-1 PASS1 (s) | This PASS1 (s) | Δ |
|---|---:|---:|---:|
| C1 t1 | 203.3 | 206.78 | +3.5 (+1.7 %) |
| C1 t2 | 220.3 | 216.27 | −4.0 (−1.8 %) |
| C2 t1 | 310.4 | 282.79 | −27.6 (−8.9 %) |
| C2 t2 | 269.1 | 258.80 | −10.3 (−3.8 %) |
| C3 t1 | 293.3 | 304.29 | +11.0 (+3.7 %) |
| C3 t2 | 311.9 | 307.63 | −4.3 (−1.4 %) |

PASS2:

| Config | Tier-1 PASS2 (s) | This PASS2 (s) | Δ |
|---|---:|---:|---:|
| C1 t1 | 219.6 | 211.54 | −8.1 (−3.7 %) |
| C1 t2 | 221.6 | 207.46 | −14.1 (−6.4 %) |
| C2 t1 | 299.2 | 268.72 | −30.5 (−10.2 %) |
| C2 t2 | 259.5 | 247.81 | −11.7 (−4.5 %) |
| C3 t1 | 236.1 | 225.73 | −10.4 (−4.4 %) |
| C3 t2 | 214.2 | 222.28 |  +8.1 (+3.8 %) |

5/6 PASS2 trials improved.  PASS1 mixed within trial-to-trial noise.

## What changed vs the Tier-1 baseline

This is a **code-change re-validation**, not a knob sweep.  Every config knob
is identical to the baseline; only the image differs.

| Item | Baseline (`sha-f3a1b2a-arm64`) | This run (`sha-9c718ac-arm64`) |
|---|---|---|
| Branch | `perf-fuse-splice` (pre-Tier-2) | `fix-bloom-stale` off `main@f2fc31c` |
| Code deltas | — | 3 commits stacked (see below) |
| Knobs | — | unchanged |

### `fix-bloom-stale` commits under test

1. **`c8f73bd`** — `cache+bloom: reconcile entries with disk before bloom rebuild`
   The root-cause fix for the catastrophic bloom-stale storms uncovered in
   the Tier-1 baseline post-mortem.  On rebuild, walks the cache dir and
   drops bloom entries that no longer exist on disk before recomputing.
2. **`b2a5723`** — `fetcher: drop remote bloom on was_yes NotFound (self-correction)`
   Defense-in-depth.  When a peer says it has a chunk but a subsequent
   pull returns NotFound, drops that peer's bloom entry locally so we
   stop trying it and fall through to the next peer / blob origin.
3. **`9c718ac`** — `hydrate: surface broadcast Phase B silent loss`
   Counts and surfaces Phase-B (broadcast/gather) silent failures via
   the `blobcache_bloom_stale_drops_total` counter.

## Configuration (identical to Tier-1 baseline)

| Knob | Value |
|---|---|
| Pods / nodes | 17 |
| `azure.workers` | 8 |
| `transport.kind` | `rdma` (UCX over IB) |
| `transport.chunk_concurrency` | 32 |
| `transport.peer_concurrency` | 8 |
| `cache.chunkSize` | 4 MiB |
| `cache.peerLruBytes` | 1 GiB |
| `transport.stampedeWaitMs` | 0 |
| `transport.peerYesWaitMs`  | 0 |
| Wipe between trials | `helm uninstall && helm install` |
| Wait sequence | `hydrate → 30 s → (gather → 30 s →) PASS1 → 10 s → PASS2` |

## Reproduction

Same harness as the Tier-1 baseline; see `benchmarks/sweep/README.md` and
`benchmarks/sweep/run-6run-sweep.sh`.  Override `BLOBCACHE_IMAGE_TAG=sha-9c718ac-arm64`
to reproduce this specific run.

## Provenance

- Orchestrator log: `/tmp/diag-strag/orchestrate-20260430T222207Z.log`
- Per-trial artefacts: `/tmp/diag-strag/c{1,2,3}-cache*-t{1,2}-{run.log,pass1.tsv,pass2.tsv,hydrate.json,gather.json,snap-{before,after,before2,after2}.tsv}`
- Result message: session `ses_22d228e16ffe7gvmKt8g2Lr5Et`, msg `de22b83610015c7gjn2owHqcGM` (2026-05-01 06:13 UTC report)

## Outcome

The `fix-bloom-stale` branch shipped — see `git log --oneline | grep -E '(c8f73bd|b2a5723|9c718ac|f580ab1|1a56ce9|fe1216b)'` for the merged commits on `main`.
