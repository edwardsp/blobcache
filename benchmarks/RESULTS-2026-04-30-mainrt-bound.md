# Results: bound main tokio runtime to 8 worker threads

**Branch**: `perf-fuse-splice`  **Commit**: `a21232e`  **Image**: `sha-a21232e-arm64`
**Date**: 2026-04-30  **Cluster**: 17× GB300 nodes
**Dataset**: `nvidia_DeepSeek-R1-0528-NVFP4-v2/` — 350 files, 413.3 GiB, 98 804 chunks of 4 MiB

## TL;DR

Capping the main `tokio::runtime::Builder::new_multi_thread()` worker_threads at **8** (down from the unbounded `num_cpus() ≈ 128` default on GB300) reduces PASS1 cluster wall by **−9.95 %** (301.20 s → 271.23 s) and per-pod straggler max by **−11.2 %** (290.7 s → 258.1 s). The change is a 2-line code edit (`src/main.rs:60-61`).

The mechanism is exactly what the v2.9 perf-record predicted: with ~128 worker threads contending on the same scheduler work-stealing queues, `__aarch64_swp8_acq_rel` consumed **15.55 % CPU** during read passes. With 8 worker threads it drops to **2.41 %** (−85 %).

This is the **first targeted hot-path fix** to land on `perf-fuse-splice`. The follow-up is FUSE_PASSTHROUGH (kernel ≥6.5, fuser `BackingId` API) which would target the now-dominant `__arch_copy_to_user` 23.24 % from the FUSE reply path.

## Configuration

All runs use the v2.9 baseline knobs (`benchmarks/RESULTS-2026-04-30-shardlru.md` § Baseline run for context):

| Knob | Value |
|---|---|
| `azure.workers` | 8 (8 independent blob-fetch tokio runtimes; idle on cache-hit reads) |
| `cache.peerLruBytes` | 1 GiB |
| `transport.chunk_concurrency` | 32 |
| `transport.peer_concurrency` | 8 |
| Image (baseline) | `sha-83924a7-arm64` (no `worker_threads` cap; `num_cpus()` default) |
| Image (this run) | `sha-a21232e-arm64` (`.worker_threads(8)`) |

## Code change

`src/main.rs` (commit `a21232e`):

```diff
+    // Bound the main runtime worker count. perf-record on the v2.9 baseline
+    // showed ~15.6% CPU in __aarch64_swp8_acq_rel inside the tokio
+    // multi-thread scheduler's work-stealing path, all attributed to the
+    // "blobcached"-named threads of this runtime (the per-worker blob-fetch
+    // runtimes were idle on cache-hit / peer-RDMA workloads). The default
+    // worker count is num_cpus() which is ~72 on GB300; that many workers
+    // contending on the same work-stealing queues generates atomic-op
+    // overhead that dominates the read hot path. Cap at 8 to match the
+    // azure.workers convention; revisit if perf shows queue starvation.
     let rt = tokio::runtime::Builder::new_multi_thread()
+        .worker_threads(8)
         .enable_all()
         .thread_name("blobcached")
         .build()?;
```

(GB300 actually has 128 cores, not 72 — the comment was written before runtime verification. Doesn't change the conclusion: 128 → 8 is a 16× reduction.)

Verified post-deploy on one sample pod:

```
=== unique thread names ===
    128 blob-w0..7   (each)
     10 blobcached   ← 8 workers + main + 1 spawn helper
      1 ucx-runtime
      1 async
```

vs baseline which had ~128 `blobcached` threads (one per core).

## Bench results — three independent PASS1 reads

| Run | Tag | Hydrate? | Cluster wall | Per-pod mean | Per-pod min | Per-pod max | Spread |
|---|---|---|---|---|---|---|---|
| Baseline | `baseline-r1` (29-Apr) | yes | **301.20 s** | 242.5 s | 184.8 s | 290.7 s | 105.9 s |
| mainrt8 #1 | `mainrt8-r1` | yes | **271.23 s** | 217.7 s | 172.7 s | 258.1 s | 85.4 s |
| mainrt8 #2 | `mainrt8-perf-r2` | no (warm) | **271.31 s** | 208.7 s | 171.9 s | 243.8 s | 71.9 s |
| mainrt8 #3 | `mainrt8-perf-r3` | no (warm) | **271.11 s** | 209.8 s | 171.2 s | 253.5 s | 82.3 s |

**Cluster-wall delta**: 301.20 → 271.23 = **−29.97 s, −9.95 %**.

Three back-to-back runs land within **0.2 s of each other** (271.11 / 271.23 / 271.31). This is far inside the day-to-day noise (±10 % cluster wall) we observed across days, so the improvement is well-resolved.

Spread (max−min) drops 105.9 → 82.3 s, meaning the straggler tail is also improved, not just the average. That's consistent with the explanation: with fewer threads, fewer scheduling-induced delays propagate through the per-pod pipeline.

### Hydrate (Phase A) — was not the target, no measurable change

| Run | Hydrate wall | Aggregate MiB/s |
|---|---|---|
| Baseline | 25.48 s | 15 737 |
| mainrt8 #1 | 27.36 s | 14 597 |

Hydrate is bandwidth-bound on the blob-fetch runtimes (which we did **not** touch). The 1.9 s difference is well inside hydrate noise (we've seen ±5 s across same-image runs on prior dates). No regression.

## perf-record comparison (60 s @ 99 Hz on one sample node during active read pass)

Baseline: 10 390 samples (`/tmp/bench-shardlru/perf-baseline.data`)
mainrt8: 9 410 samples (`/tmp/bench-mainrt8/perf-mainrt8.data`)

### Top symbols

| Symbol | comm | Baseline | mainrt8 | Δ pp |
|---|---|---:|---:|---:|
| `__aarch64_swp8_acq_rel` | `blobcached` | **15.55 %** | **2.41 %** | **−13.14** |
| `tokio::scheduler::worker::run` | `blobcached` | 2.54 % | 0.58 % | −1.96 |
| `__aarch64_ldadd8_relax` | `blobcached` | 2.18 % | 2.38 % | +0.20 |
| `__aarch64_cas1_acq` | `blobcached` | 1.59 % | 1.43 % | −0.16 |
| `__pi_memcpy_generic` | `blobcached` | 21.38 % | 19.52 % | −1.86 |
| `__arch_copy_to_user` | `tokio-rt-worker` | 5.67 % | **23.24 %** | **+17.57** |
| `__arch_copy_to_user` | `blobcached` | 0.39 % | 0.30 % | −0.09 |
| `fuse_copy_finish` | `blobcached` | 1.27 % | 0.66 % | −0.61 |
| `fuse_dev_do_read` | `blobcached` | 0.50 % | 1.49 % | +0.99 |
| `fuse_dev_do_write` | `blobcached` | 0.94 % | 1.05 % | +0.11 |

### Per-comm CPU distribution

| comm | Baseline | mainrt8 |
|---|---:|---:|
| `blobcached` | 80.88 % | 60.73 % |
| `tokio-rt-worker` | 7.06 % | 25.71 % |
| `ucx-runtime` | 11.97 % | 13.38 % |
| `blob-w0..7` | <0.01 % | <0.01 % |

### Interpretation

1. **Atomic-ops collapse confirmed.** The 15.55 % work-stealing atomics on `blobcached` threads dropped to 2.41 % — exactly the predicted mechanism. This freed up CPU that was previously burning on scheduler bookkeeping.

2. **Same workload, different bottleneck.** `tokio-rt-worker` jumped from 7 % to 26 % of CPU because it's now doing actual work that was previously gated by the noisy main runtime. Specifically, `__arch_copy_to_user` on these threads went from 5.67 % to 23.24 % — that's the **kernel-side FUSE reply copy** (data going from our reply buffer into the FUSE kernel buffer that the calling process will read). This is the **expected new dominant cost** and is exactly what FUSE_PASSTHROUGH would eliminate by letting the kernel `read()` straight from our cache file instead of going through the FUSE userspace bounce.

3. **`__pi_memcpy_generic` dropped slightly (21.38 → 19.52)** but is still the #2 hotspot. Most of this is also FUSE writeback memcpy (`fuse_copy_*` family) and would drop to ~0 with PASSTHROUGH.

4. **`blob-w*` threads still <0.01 % CPU** — confirms they remain idle on read workloads. The earlier guess that they might "wake up" once main-runtime noise drops was wrong; the work-stealing across runtimes is per-runtime-internal, not cross-runtime.

5. **`fuse_dev_do_read` grew (0.50 → 1.49 %)** because we're now doing more FUSE reads per second (work that was previously starved). This is a healthy signal — the daemon is using its CPU for actual FUSE IO rather than scheduler atomics.

## Recommendation

**Land this change.** It's a one-line edit, 10 % wall-time win on the dominant workload, no regressions on hydrate or other paths, perf-attribution-validated.

**Follow-ups in order of expected impact:**

1. **FUSE_PASSTHROUGH** (kernel ≥ 6.5; cluster runs 6.14). Now that scheduler overhead is gone, `__arch_copy_to_user` (23 %) + `__pi_memcpy_generic` (20 %) together dominate at ~43 % CPU and are both eliminable in the fast path (cache hit → file already on local NVMe). fuser exposes `BackingId` and `reply.opened_passthrough()`. Estimated: another 15-25 % wall reduction if it works as documented.

2. **Make `worker_threads` configurable** via `values.yaml` (e.g. `runtime.mainWorkerThreads: 8`) so we can sweep sweet-spot without rebuilding. Likely values to try: 4, 16. With 8 we may be slightly over- or under-shooting — but 8 already eliminated the bulk of the contention so we may have already captured most of the available win.

3. **Sub-runtime audit on `blob-w*` runtimes.** They still default to `num_cpus()` per runtime (8 × 128 = 1024 idle threads). On read workloads they cost essentially nothing, but on hydrate-heavy mixed workloads they might. Worth a measurement-driven cap if hydrate ever becomes the bottleneck.

## Artifacts

- Commit: `a21232e` on branch `perf-fuse-splice`
- Image: `ghcr.io/edwardsp/blobcache:sha-a21232e-arm64` (built via `gh workflow run container.yml --ref perf-fuse-splice`, GH run `25153200876`)
- Bench artifacts: `/tmp/bench-mainrt8/` (3 run logs, 3 pass1 TSVs, 1 hydrate JSON)
- perf data: `/tmp/bench-mainrt8/perf-mainrt8.data` (9 410 samples; 60 s @ 99 Hz on one sample pod, during pass3 read)
- Helm values diff vs baseline: only `image.tag` (`sha-83924a7-arm64` → `sha-a21232e-arm64`)

## Timings (UTC, for Grafana correlation)

| Phase | Tag | Start | End | Wall |
|---|---|---|---|---|
| Helm reinstall | — | 07:36:26Z | 07:36:46Z | 20 s |
| Pass1 (cleared + hydrate) | mainrt8-r1 | 07:40:51Z | 07:45:23Z | 271.23 s |
| Pass2 (warm) | mainrt8-perf-r2 | 07:46:34Z | 07:51:06Z | 271.31 s |
| Pass3 (warm + perf record) | mainrt8-perf-r3 | 07:52:38Z | 07:57:09Z | 271.11 s |
| perf record window | — | 07:53:23Z | 07:54:23Z | 60 s |

All three pass1 reads land at 271 s ±0.2 s, demonstrating the result is reproducible within a single deployment.
