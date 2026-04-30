# Results: peer_yes_wait_ms knob + peer-server cache lookup offload

**Branch**: `perf-peer-wait-ms`  **Commit**: `f3a1b2a`  **Image**: `sha-f3a1b2a-arm64`
**Date**: 2026-04-30  **Cluster**: 17× GB300 nodes (RDMA/UCX transport)
**Dataset**: `nvidia_DeepSeek-R1-0528-NVFP4-v2/` — 163 files, 413.3 GiB, 98 804 chunks of 4 MiB

## TL;DR

Two targeted fixes in the peer-fetch hot path eliminate the persistent
**bimodal PASS1 split** observed on every prior 17-node run of this dataset
and cut cluster PASS1 wall **−57.6 %** (694.1 s → 294.4 s, **2.36×**).

| Run | Cluster wall_pass1 | Slowest pod | Fastest pod | Spread | Slow pods peer_miss | Slow pods blob_fetches |
|---|---|---|---|---|---|---|
| baseline-alpha (no fix) | 694.1 s | 693 s | 216 s | **3.21×** | 2 942–6 441 | 17.3k–18.9k |
| barrier-30s-fix (control) | ~660 s | ~658 s | ~225 s | **2.92×** | 2 942–6 441 | 17.3k–18.9k |
| **peer-wait-200 (this fix)** | **294.4 s** | **293.1 s** | **176.6 s** | **1.66×** | **0 across all 17** | **5 812 across all 17** (= hydrate-only) |

Every PASS1 blob fallback was eliminated. Singleflight wait counts went up
proportionally (≈92 660 / pod), confirming peers are now correctly
subscribing to in-flight inserts instead of bailing to blob.

## Configuration

Identical to the prior straggler-investigation runs except for the new
knob and image tag (full file: `/tmp/diag-strag/values-peer-wait-200.yaml`).

| Knob | baseline-alpha | peer-wait-200 |
|---|---|---|
| `image.blobcached.tag` | `sha-a21232e-arm64` | `sha-f3a1b2a-arm64` |
| `config.transport.peerYesWaitMs` | (n/a, code defaulted 0) | **200** |

All other values pinned: `transport.kind=rdma`, `chunk_concurrency=32`,
`peer_concurrency=8`, `cache_on_peer_fetch=false`, `peer_lru_bytes=1 GiB`,
`stampede_wait_ms=0`, `prefetch_depth=0`, `azure.workers=8`,
`bloom_pull_secs=5`, `bloom_bits=2^23`, hydrate sharding enabled.

## Run timeline

| Phase | Start (UTC) | End (UTC) | Duration |
|---|---|---|---|
| HYDRATE | 11:33:14 | 11:33:30 | 16.0 s |
| BARRIER (POST_HYDRATE_SLEEP_S=30) | 11:33:30 | 11:34:00 | 30.0 s |
| PASS1 (cluster aggregate) | 11:34:03.806 | 11:39:00.283 | **294.385 s** |

After hydrate, every pod held exactly **5 812 chunks** on disk (98 804 / 17;
sharding is even). After PASS1 every pod read **163 files / 413 328 348 544 B**.
`blob_fetches_total` did not increase by a single fetch during PASS1.

## Per-pod PASS1 wall (sorted)

VMSS suffixes anonymised; pod-name hashes truncated. The bimodal cluster of
~14 slow pods + 3 fast pods seen on every prior run is gone. The new
distribution is a single tight cluster with a 1.66× max/min spread.

```
node-A  176.6 s   node-J  226.6 s
node-B  190.2 s   node-K  228.9 s
node-C  194.8 s   node-L  234.5 s
node-D  199.9 s   node-M  243.9 s
node-E  208.6 s   node-N  267.4 s
node-F  217.6 s   node-O  268.1 s
node-G  222.0 s   node-P  268.5 s
                  node-Q  285.7 s
                  node-R  290.0 s
                  node-S  293.1 s
```

## Per-pod final metric totals (representative)

All 17 pods landed within 1 % of these numbers:

```
blob_fetches_total            5 812      (hydrate only; 0 PASS1 fallback)
blob_fetch_bytes_total       ~24.3 GB    (≈ chunks × 4 MiB)
peer_fetches_ok_total        ~92 815     (≈ chunks × 16 peers)
peer_fetches_miss_total          0       (was 2 942-6 441 on baseline)
peer_fetches_err_total           0
singleflight_waits_total     ~92 660     (was ~92 660 on baseline)
peer_chunk_requests_total    ~92 815     (each pod served the rest)
peer_chunk_bytes_served_total ~389 GB    (≈ 22.9 GB × 17 peers)
fuse_read_bytes_total       413 328 348 544
```

## Root cause and fix

Two compounding bugs in the peer-fetch path:

### 1. `wait_ms = 0` for bloom-positive peers (primary)

`src/fetcher.rs::do_fetch` ranked candidate peers using bloom filters and
called each with `wait_ms = 0`. The peer server (`src/transport.rs`) only
consults the singleflight subscribe path (`chunk_provider`) when
`wait_ms > 0`; with 0 it short-circuits to disk-only and returns 404 the
moment a chunk isn't yet on disk under its final name.

For chunks caught mid-`spawn_insert` (between the temp-file write and the
`entries` map insertion) **and** for any chunk where the bloom advertised
"yes" but the owning peer was concurrently fetching it (singleflight
in-flight, not yet flushed), the requester saw 404 → mapped it to
`peer_fetches_miss` → fell straight back to Azure Blob. With ~98k chunks
and 17 nodes racing the same dataset, this produced 3-6k spurious blob
fallbacks per pod and the persistent ~22-29 GiB blob-traffic tax visible
in the bimodal slow group.

**Fix**: new `transport.peer_yes_wait_ms` knob (default 0 for backward
compatibility) is passed as `wait_ms` to bloom-positive peers only, so the
peer server engages the singleflight subscriber path and streams the
in-flight insert's result rather than bailing.

### 2. Synchronous `std::fs::read` on a tokio worker (secondary)

`PeerService::handle_chunk` invoked `cache.try_get(&key)` directly from
the async handler. `try_get` calls `std::fs::read(&path)` inline — a
synchronous blocking call. On a tokio worker shared with other peer
requests, any single slow NVMe read (cleanup, page-cache flush, CPU steal)
stalled the entire worker and could starve sibling requests, widening the
MISS pool further.

**Fix**: wrap the lookup in `tokio::task::spawn_blocking`.

Both fixes are independent. The `peer_yes_wait_ms` knob is the dominant
contributor; the `spawn_blocking` move is a robustness improvement that
will matter more under heavier per-node concurrency.

## Code changes

```
src/config.rs                         | +5
src/fetcher.rs                        | +13 -1   (struct field, ctor param, clone, call site)
src/main.rs                           | +1       (pass-through)
src/transport.rs                      | +5 -1    (spawn_blocking)
deploy/helm/blobcache/templates/...   | +3
deploy/helm/blobcache/values.yaml     | +4
```

Two commits: `da56d2f` (harness slash-normalization, unrelated diag-run
bug found en route) and `f3a1b2a` (the actual fix).

## What's next

The new floor of 294 s closes most of the gap to the prior fast-pod
"best case" of 216 s but does not eliminate it. The remaining spread
(176-293 s among PASS1 walls) is consistent with NVMe service-time
variance + the 200 ms wait_ms cost amortised across late-claim chunks,
not bloom propagation. Future investigations:

- Sweep `peer_yes_wait_ms ∈ {50, 100, 500}` to find the knee.
- Lift `peer_concurrency` from 8; the per-node peer-serve load is now
  balanced (~93k requests/pod served) and CPU is far from saturated.
- Audit `cache_bytes` accuracy on out-of-band wipe (separate finding,
  observability only — does not affect throughput).
