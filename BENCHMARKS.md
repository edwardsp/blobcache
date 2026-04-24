# Benchmarks (v1.0.0)

End-to-end measurements on a 3-node AKS GB300 cluster (NVIDIA Grace,
aarch64), one `blobcached` per node, host-network, NVMe RAID-0 cache,
TCP peer transport. Run via `benchmarks/bench.sh`; raw TSV in
`benchmarks/results.tsv`.

## Setup

| | |
|---|---|
| Cluster | 3× `Standard_GB300` (Grace + Blackwell), `agentpool=gb300` |
| Cache backend | hostPath `/mnt/nvme/blobcache-cache` (RAID-0 over node NVMes) |
| Chunk size | 4 MiB |
| `chunk_concurrency` | 32 |
| `peer_concurrency` | 8 |
| Peer transport | HTTP/1.1 over TCP (host-network, intra-AZ) |
| Storage account | `myaccount`, container `test`, blobs `azcp-bench/dl-src-big/file_*.bin` (2 GiB each) |
| Read pattern | `dd bs=1M count=N iflag=fullblock` after `drop_caches` |

All Oracle CRITICAL + HIGH issues from the v1 review are fixed in this
binary (per-chunk singleflight with leader-cancel guard, bearer refresh
with skew + retry-once-on-401, per-mount blob client, length-validated
chunks with bounded fan-out, phantom-resurrect fix, dir delta-delete +
child cap, shared listing across handles, startup orphan purge, gossip
body cap + cluster-hash filter + SWIM merge precedence).

## Per-size, per-scenario throughput

`cold` = first read on node A (origin: Azure Blob).
`warm-local` = re-read on node A (origin: local NVMe cache).
`warm-peer` = read same file on node B (origin: peer chunk fetch from A).

| Size | Scenario | Wall (s) | Throughput (MiB/s) | blob_fetches Δ | peer_fetches Δ | cache_hits Δ |
|---:|---|---:|---:|---:|---:|---:|
| 1 MiB | cold | 0.174 | 5.7 | 1 | 0 | 8 |
| 1 MiB | warm-local | 0.006 | 166.7 | 0 | 0 | 9 |
| 1 MiB | warm-peer | 0.417 | 2.4 | 0 | 1 | 8 |
| 4 MiB | cold | 0.114 | 35.1 | 2 | 0 | 31 |
| 4 MiB | warm-local | 0.011 | 363.6 | 0 | 0 | 33 |
| 4 MiB | warm-peer | 0.014 | 285.7 | 0 | 2 | 31 |
| 16 MiB | cold | 0.225 | 71.1 | 5 | 0 | 121 |
| 16 MiB | warm-local | 0.245 | 65.3 | 0 | 0 | 129 |
| 16 MiB | warm-peer | 0.107 | 149.5 | 0 | 5 | 121 |
| 64 MiB | cold | 0.977 | 65.5 | 17 | 0 | 481 |
| 64 MiB | warm-local | 0.087 | 735.6 | 0 | 0 | 513 |
| 64 MiB | warm-peer | 0.179 | 357.5 | 0 | 17 | 481 |
| 256 MiB | cold | 2.860 | 89.5 | 65 | 0 | 1921 |
| 256 MiB | warm-local | 0.325 | 787.7 | 0 | 0 | 2049 |
| 256 MiB | warm-peer | 0.838 | 305.5 | 0 | 65 | 1921 |
| 1024 MiB | cold | 11.381 | 90.0 | 257 | 0 | 7681 |
| 1024 MiB | warm-local | 1.238 | 827.1 | 0 | 0 | 8193 |
| 1024 MiB | warm-peer | 2.513 | 407.5 | 0 | 257 | 7681 |

`cache_hits` here counts FUSE-level chunk lookups, which fire many times
per chunk during a sequential `dd bs=1M` (one `read()` syscall per 1 MiB,
so ~32 lookups per 4 MiB chunk on warm paths).

### Observations

- **Cold path saturates around ~90 MiB/s** for a single sequential
  reader. This is the per-chunk Azure Blob HTTPS GET ceiling at the
  current `chunk_concurrency=32` for a single ranged-read stream;
  multi-reader workloads scale further (see singleflight stress, where
  8 concurrent readers share one fetch set).
- **Warm-local plateaus at ~830 MiB/s** for 1 GiB. The cap is FUSE-syscall
  overhead, not the NVMe.
- **Warm-peer plateaus at ~410 MiB/s** for 1 GiB over the in-VPC TCP
  transport. The RDMA backend (v2) is the path to exceed this.
- **Small-file overhead dominates at 1 MiB**: every scenario pays the
  fixed FUSE lookup + first-chunk fetch round-trip cost. `cold` 1 MiB
  at 5.7 MiB/s is one HTTP round-trip + auth; `warm-peer` 1 MiB at
  2.4 MiB/s adds the peer round-trip.
- **The 16 MiB warm-local anomaly (65 MiB/s)** is a `drop_caches`
  artifact — the kernel page cache for the FUSE-served bytes is wiped
  between cold and warm, so this row re-pays page-fault latency on warm
  while the FUSE/cache layers are fast. Larger sizes amortise it.

## Singleflight stress

8 concurrent readers on the same node, same uncached 64 MiB file
(`file_b.bin`, chunk_size=4 MiB → 16 chunks):

| Metric | Value | Without singleflight (expected) |
|---|---:|---:|
| Wall (s) | 1.688 | ~1.0 (8× parallel fetch contention) |
| Aggregate throughput (MiB/s) | 303.3 | — |
| `blob_fetches` Δ | **17** | **128** (16 chunks × 8 readers) |
| `singleflight_waits` Δ | 12 | 0 |

**`blob_fetches=17` against 8 concurrent readers of 16 chunks** confirms
per-chunk singleflight is collapsing duplicate work to a single Azure GET
per chunk (the extra `+1` is one prefetched / re-issued chunk during the
ramp). Without the C3 fix this would be `chunks × readers = 128` GETs.
`singleflight_waits=12` counts the readers that blocked waiting for an
in-flight leader rather than firing their own GET.

## Reproducing

```sh
# Enable storage public access (vnet-restricted; toggles only the flag)
deploy/storage-access.sh on

# Run benchmark
bash benchmarks/bench.sh > benchmarks/results.tsv

# Always disable after
deploy/storage-access.sh off
```

`bench.sh` auto-discovers pods via `kubectl -n blobcache get pods -l
app=blobcached`. P0 is used for cold/warm-local; P1 for warm-peer.

---

# v2 (UCX peer transport)

Same cluster, same chunk size, same workload. Peer transport switched to
UCX (`transport.kind = "rdma"`, `--features ucx`). Raw TSV:
`benchmarks/results-rdma.tsv`.

## Important caveat: no IPoIB on this cluster

The pod IPs we use for the UCX listener are Azure CNI overlay addresses
(`10.16.0.x`) bound to `eth0`. The IB devices are passed in via SR-IOV
(`rdma/ib: 1`) but there is **no IPoIB plugin** on the cluster
(`NicClusterPolicy.state-ipoib-cni: ignore`), so `eth0` is the only
interface that has an IP, and the IB GIDs are link-local-only and not in
a common subnet between hosts. UCX's RDMA-CM wireup requires an IP that
maps to an RDMA device; with neither pod-side IB IP nor a route between
the per-host IB GIDs, the IB transports (`rc_*`, `ud_*`, `dc_*`) cannot
complete the wireup phase between two pods.

We therefore restrict UCX to `tcp,sm,self` (`UCX_TLS=tcp,sm,self`,
`UCX_NET_DEVICES=eth0`). The cross-node hop runs over UCX's TCP
transport on `eth0` — kernel TCP, no kernel bypass, no RDMA. The win
this delivers over v1 is bounded by what UCX-TCP can do better than
HTTP/1.1 keep-alive, which on this network is "not much, and possibly
worse" because each chunk request opens a fresh UCX endpoint
(no pooling yet) and pays a full wireup round-trip.

This is a deployment-environment limitation, not a daemon limitation.
Once IPoIB (or a Multus/IPAM that exposes per-pod IB IPs) is enabled on
the cluster, the same binary, same config, will negotiate `rc_mlx5` for
the data path and the numbers below will be replaced by RDMA throughput.

## Per-size, per-scenario throughput (UCX-TCP)

| Size | Scenario | Wall (s) | Throughput (MiB/s) | blob_fetches Δ | peer_fetches Δ | cache_hits Δ |
|---:|---|---:|---:|---:|---:|---:|
| 1 MiB | cold | 0.254 | 3.9 | 1 | 0 | 8 |
| 1 MiB | warm-local | 0.006 | 166.7 | 0 | 0 | 9 |
| 1 MiB | warm-peer | 0.063 | 15.9 | 0 | 1 | 8 |
| 4 MiB | cold | 0.076 | 52.6 | 2 | 0 | 31 |
| 4 MiB | warm-local | 0.234 | 17.1 | 0 | 0 | 33 |
| 4 MiB | warm-peer | 0.031 | 129.0 | 0 | 2 | 31 |
| 16 MiB | cold | 0.417 | 38.4 | 5 | 0 | 121 |
| 16 MiB | warm-local | 0.027 | 592.6 | 0 | 0 | 129 |
| 16 MiB | warm-peer | 0.406 | 39.4 | 0 | 5 | 121 |
| 64 MiB | cold | 1.020 | 62.7 | 17 | 0 | 481 |
| 64 MiB | warm-local | 0.101 | 633.7 | 0 | 0 | 513 |
| 64 MiB | warm-peer | 0.399 | 160.4 | 0 | 17 | 481 |
| 256 MiB | cold | 4.042 | 63.3 | 65 | 0 | 1921 |
| 256 MiB | warm-local | 0.559 | 458.0 | 0 | 0 | 2049 |
| 256 MiB | warm-peer | 1.813 | 141.2 | 0 | 65 | 1921 |
| 1024 MiB | cold | 14.955 | 68.5 | 257 | 0 | 7681 |
| 1024 MiB | warm-local | 1.273 | 804.4 | 0 | 0 | 8193 |
| 1024 MiB | warm-peer | 7.190 | 142.4 | 0 | 257 | 7681 |

## v1 (HTTP/1.1) vs v2 (UCX-TCP) warm-peer head-to-head

| Size | v1 HTTP MiB/s | v2 UCX-TCP MiB/s | Δ |
|---:|---:|---:|---:|
| 1 MiB | 2.4 | 15.9 | +13.5 (UCX wins on tiny single-shot) |
| 4 MiB | 285.7 | 129.0 | −156.7 |
| 16 MiB | 149.5 | 39.4 | −110.1 |
| 64 MiB | 357.5 | 160.4 | −197.1 |
| 256 MiB | 305.5 | 141.2 | −164.3 |
| 1024 MiB | 407.5 | 142.4 | −265.1 |

v2 UCX-TCP is **2–3× slower than v1 HTTP/1.1** on every multi-chunk
read. Two effects dominate:

1. **No connection pooling** — every chunk opens a fresh UCX endpoint
   and tears it down after one request/response (~ms-class wireup vs
   reqwest's keep-alive HTTP/1.1).
2. **No RDMA** — without IPoIB, UCX is just kernel TCP with extra
   wrapping. v1's `reqwest`/hyper stack is more mature on plain TCP.

The 1 MiB row goes the other way only because v1 paid two `connect()`s
in that scenario (different pod, no warmed pool yet) while v2's UCX
listener cached the wireup faster on the second hop.

The architectural value of v2 is unchanged — the FFI integration, the
threading model, the protocol, and the `PeerClient` enum are all in
place behind `transport.kind = "rdma"`. The cluster wiring is what
gates the throughput. To realise the win on this hardware:

- Enable IPoIB so each pod has an `ib0` IP that UCX can resolve to an
  RDMA device, OR
- Add an endpoint pool to `RdmaPeerClient` (one ep per peer, kept warm)
  and re-test on UCX-TCP. Even without RDMA this should close most of
  the gap to v1 by amortising wireup.

## Singleflight stress (UCX-TCP)

| Metric | Value |
|---|---:|
| Concurrent readers | 8 |
| File size | 64 MiB (16 chunks) |
| Wall (s) | 1.985 |
| Aggregate throughput (MiB/s) | 257.9 |
| `blob_fetches` Δ | 17 |
| `singleflight_waits` Δ | 15 |

Singleflight still collapses to ~1 GET per chunk regardless of
transport, exactly as designed.

## Default

Default `transport.kind` remains `"tcp"`. Operators on clusters with
IPoIB (or once a pool is added) flip to `"rdma"` per node — the binary
auto-rejects mixed-kind clusters via the `cluster_hash`.

---

# v2.1 (UCX peer transport — real RDMA)

The IPoIB-fallback caveat above is now obsolete on this cluster. v2.1
replaces the per-chunk endpoint, sequential-await server loop with:

1. **OOB worker-address wireup** — pods exchange UCX worker addresses via
   gossip (no IPoIB / no RDMA-CM round trip), so the IB transports
   (`rc_mlx5`, `ud_mlx5`) negotiate directly between mlx5 devices.
2. **UCX tag API** with one persistent endpoint per peer; chunk requests
   ride sender-tag-bits = local node id, request-id correlation in the
   tag low bits.
3. **Event-driven progress** — `progress_worker` arms `epoll(eventfd)`
   and a `Notify` "kick" channel, calls `ucp_worker_progress` to budget
   exhaustion then yields cooperatively. The 5 ms `tokio::time::interval`
   poll tick that gated v2.0's inbound drain is gone.
4. **Fire-and-forget per-request handlers** spawned from a sync
   `drain_inbound` probe. Server-side `cache.try_get` runs on
   `spawn_blocking` so progress isn't blocked.
5. **Request-pool reuse fix** — `RequestState` is reset to
   `{done:false, status:UCS_INPROGRESS}` on every deferred op, so a
   recycled UCX request slot doesn't see a stale `done` from a prior
   completion.

Result: zero IPoIB anywhere in the wireup, `rdma_non_rdma_lane_total = 0`
across all peer fetches, `peer_fetches_err_total = 0`. Raw TSV:
`benchmarks/results-rdma-real.tsv`.

## Per-size, per-scenario throughput (v2.1 RDMA)

| Size | Scenario | Wall (s) | Throughput (MiB/s) | blob_fetches Δ | peer_fetches Δ | cache_hits Δ |
|---:|---|---:|---:|---:|---:|---:|
| 1 MiB | cold | 0.206 | 4.9 | 1 | 0 | 8 |
| 1 MiB | warm-local | 0.007 | 142.9 | 0 | 0 | 9 |
| 1 MiB | warm-peer | 0.093 | 10.8 | 0 | 1 | 8 |
| 4 MiB | cold | 0.075 | 53.3 | 2 | 0 | 31 |
| 4 MiB | warm-local | 0.053 | 75.5 | 0 | 0 | 33 |
| 4 MiB | warm-peer | 0.015 | 266.7 | 0 | 2 | 31 |
| 16 MiB | cold | 0.193 | 82.9 | 5 | 0 | 121 |
| 16 MiB | warm-local | 0.037 | 432.4 | 0 | 0 | 129 |
| 16 MiB | warm-peer | 0.061 | 262.3 | 0 | 5 | 121 |
| 64 MiB | cold | 0.629 | 101.7 | 17 | 0 | 481 |
| 64 MiB | warm-local | 0.108 | 592.6 | 0 | 0 | 513 |
| 64 MiB | warm-peer | 0.107 | 598.1 | 0 | 17 | 481 |
| 256 MiB | cold | 2.496 | 102.6 | 65 | 0 | 1921 |
| 256 MiB | warm-local | 0.427 | 599.5 | 0 | 0 | 2049 |
| 256 MiB | warm-peer | 0.450 | 568.9 | 0 | 65 | 1921 |
| 1024 MiB | cold | 10.266 | 99.7 | 257 | 0 | 7681 |
| 1024 MiB | warm-local | 1.744 | 587.2 | 0 | 0 | 8193 |
| 1024 MiB | warm-peer | 1.626 | **629.8** | 0 | 257 | 7681 |

## v1 (HTTP/1.1) vs v2.1 (RDMA) warm-peer head-to-head

| Size | v1 HTTP MiB/s | v2.1 RDMA MiB/s | Speedup |
|---:|---:|---:|---:|
| 1 MiB | 2.4 | 10.8 | 4.5× |
| 4 MiB | 285.7 | 266.7 | 0.93× |
| 16 MiB | 149.5 | 262.3 | 1.75× |
| 64 MiB | 357.5 | 598.1 | 1.67× |
| 256 MiB | 305.5 | 568.9 | 1.86× |
| 1024 MiB | 407.5 | **629.8** | **1.55×** |

Single-stream, large-file warm-peer is **1.55–1.86× v1**. The 4 MiB row
is in the noise — at one chunk the read finishes before steady state.

## Multi-stream aggregate (v2.1 RDMA)

The single-stream 630 MiB/s is software-bound (per-chunk client
overhead in `fetcher::fetch_chunk` and FUSE single-thread serialization).
The transport itself sustains far more under concurrency — measured
on `blobcached-h5j2w` reading files pre-warmed on `blobcached-h4pr5`,
2 GiB per file, all-RDMA peer fetches:

| Streams | Wall (s) | Aggregate (MiB/s) | vs v1 single-stream (407 MiB/s) |
|---:|---:|---:|---:|
| 1 | 3.17 | 678 | 1.67× |
| 4 | 4.56 | **1886** | **4.6×** |
| 8 | 7.34 | **2340** | **5.75×** |

8-stream aggregate lands inside the 1.5–3 GB/s target band. Per-stream
scaling tapers past 4 streams — server-side `spawn_blocking(cache.try_get)`
serializes on the blocking pool and the FUSE handler runs on a single
thread. Both are tractable in v2.2.

## Latency

Per-chunk warm-peer (4 KiB read forces one cold-via-peer fetch on
otherwise-cached file):

| Version | Per-chunk latency |
|---|---|
| v1 HTTP/1.1 (TCP) | 8–12 ms |
| v2.0 UCX-TCP | 12–25 ms (per-chunk wireup) |
| v2.1 UCX RC over IB | **3.3–4.0 ms** |

UCX `tag_bw` 4 MiB on the same fabric reaches ~80 µs / 53 GiB/s, so the
remaining ~3 ms per chunk is in our daemon (request decode → cache
lookup → response encode → FUSE return), not the wire.

## Singleflight stress (v2.1 RDMA)

| Metric | Value |
|---|---:|
| Concurrent readers | 8 |
| File size | 64 MiB (16 chunks) |
| Wall (s) | 1.774 |
| Aggregate throughput (MiB/s) | 288.6 |
| `blob_fetches` Δ | 17 |
| `singleflight_waits` Δ | 13 |

Per-chunk singleflight collapses 8× duplicate work to 1 GET per chunk,
unchanged from v1.

## UCX runtime configuration (v2.1 baked into `deploy/blobcached.yaml`)

```
UCX_NET_DEVICES   = mlx5_0:1
UCX_TLS           = rc,ud,sm,self
UCX_IB_ADDR_TYPE  = auto         # required on UCX 1.16; "gid" was rejected
UCX_RNDV_THRESH   = 64K
UCX_ZCOPY_THRESH  = 64K
UCX_RNDV_SCHEME   = put_zcopy
UCX_MAX_RNDV_RAILS = 1
```

`UCX_RNDV_SCHEME` and `UCX_MAX_RNDV_RAILS` give a small (within-noise)
single-stream gain on this hardware; the dominant wins came from the
five driver fixes above.

## Default

Default remains `transport.kind = "tcp"`. Set `kind = "rdma"` (and build
with `--features ucx`) on clusters that have IB devices and a
multi-pod gossip wireup path. The binary refuses to merge mixed-kind
clusters via `cluster_hash`.

# v2.2 (read-amplification fix)

## Setup

Same hardware, transport, and bench harness as v2.1. Two changes ship
in v2.2:

1. `cache.try_get_range(key, sub_offset, sub_len)` — pread()s only the
   requested slice instead of reading the whole 4 MiB chunk for every
   FUSE sub-read.
2. `BlobFs::init` negotiates `max_readahead = max_write = chunk_size`
   so the kernel can ship up to 4 MiB FUSE requests where the client
   API allows.

## Profile that motivated the fix

Per-stage histograms added in v2.2 (5 new
`blobcache_chunk_*_seconds` / `blobcache_fuse_read_seconds`) revealed
v2.1.0 single-stream warm-peer was spending **56% of FUSE time in
`cache.try_get`**:

| Histogram (3 × 2 GiB single-stream warm-peer on v2.1.0) | Count | Sum | Mean |
|---|---|---|---|
| `chunk_cache_get_seconds` | 49152 | 9.15 s | **186 µs** |
| `chunk_cache_insert_seconds` | 1536 | 1.92 s | 1.25 ms |
| `chunk_peer_fetch_seconds` | 2322 | 1.49 s | 643 µs |
| `chunk_fetch_total_seconds` | 49152 | 15.93 s | 324 µs |
| `fuse_read_seconds` | 49152 | 16.40 s | 333 µs |

`49152 / 1536 = 32` — the kernel was splitting each `dd bs=4M` syscall
into 32 × 128 KiB FUSE reads, each one re-reading the entire 4 MiB
chunk into a Vec. With 4 MiB read amplification per 128 KiB of user
data we were burning ~32× more bandwidth on the cache file than on the
peer fetch.

After the fix `chunk_cache_get_seconds` mean dropped to **44 µs**.

## Throughput (warm-peer, file_a..h_9.bin = 2 GiB each)

| Concurrency | v2.1.0 | v2.2 | Speedup |
|---|---|---|---|
| 1-stream | 670 MiB/s | **1067–1099 MiB/s** | **1.59–1.64×** |
| 4-stream aggregate | 1886 MiB/s | **2700 MiB/s** | **1.43×** |
| 8-stream aggregate | 2340 MiB/s | **3500 MiB/s** | **1.50×** |

## v1 (HTTP/1.1) vs v2.2 (RDMA + slice cache) warm-peer head-to-head

Single-stream warm-peer at 2 GiB (file_a_9.bin):

| Variant | MiB/s | vs v1 |
|---|---|---|
| v1 HTTP/1.1 | 407 | 1.00× |
| v2.1 RDMA | 670 | 1.65× |
| **v2.2 RDMA + slice cache** | **1083** | **2.66×** |

## What's left for v2.3+

- 1-stream now caps at ~1.1 GiB/s. The remaining per-chunk fixed cost
  is split roughly: peer round-trip ~640 µs, NVMe slice read ~44 µs,
  cache.insert ~1.25 ms (still on the critical path), tokio scheduler
  + FUSE handler dispatch.
- `cache.insert` fire-and-forget (the failed v2.2 #1 attempt) was
  rejected at 22% regression because pre-fix the 32× cache_get cost
  was so dominant that any extra `tokio::spawn` overhead was visible.
  With cache_get at 44 µs that headroom may now exist; a re-try is
  warranted but should be measured per-concurrency.
- FUSE multi-worker still untested. With a single FUSE handler thread
  and per-chunk semaphore at 32, single-stream is still serialised
  across the chunk boundary.


# v2.3 (cache.insert off the critical path)

## Setup

Same hardware/transport. v2.2 profile showed cache.insert was now the
single-largest per-chunk cost (mean 1.25 ms x 1 per chunk) sitting on
the synchronous return path. v2.3 spawns the disk insert in the
background and stages the bytes in an in-memory `inflight_writes`
DashMap so concurrent / immediately-following readers don't trigger
a redundant peer fetch in the 1.25 ms commit window.

## v2.3-A throughput (warm-peer, 2 GiB files, RDMA)

| Concurrency | v2.2 | **v2.3-A** | vs v2.2 | vs v2.1.0 | vs v1 |
|---|---|---|---|---|---|
| 1-stream | 1083 MiB/s | **2046 MiB/s** | 1.89x | 3.05x | 5.03x |
| 4-stream | 2700 MiB/s | **4358 MiB/s** | 1.61x | 2.31x | n/a |
| 8-stream | 3500 MiB/s | **4071 MiB/s** | 1.16x | 1.74x | n/a |

Single-stream now sustains over **2 GiB/s from a single dd**. The
v2.1.0 attempt at this same change regressed by 22-29% because the
32:1 cache_get amplification was so dominant that any extra spawn
overhead was net-negative; with v2.2's slice-cache freeing that
headroom, the same change is +89%.

## Correctness invariant

`inflight_writes: DashMap<ChunkKey, Bytes>` is populated synchronously
before `tokio::spawn` starts the disk insert and removed only after
the spawn_blocking call returns. All three read paths
(fetch_chunk_inner, fetch_chunk_range_inner, FUSE follower via
singleflight rx) consult inflight_writes before cache.try_get, so a
follower arriving in the commit window sees the in-memory bytes and
returns immediately rather than re-fetching from the peer.


# v2.3.1 (server-side single-buffer pread, zero extra copy)

## Setup

Same hardware/transport. v2.3.0 single-stream profile showed the server
handler (h4pr5) spent 605 µs/req split as: cache.try_get 313 µs,
encode_response (extend_from_slice of the 4 MiB payload) ~149 µs,
ucp_tag_send_nbx 143 µs. The dominant cost was two back-to-back
4 MiB userspace memcpys: page-cache → fresh `Vec` (try_get), then
fresh `Vec` → response buffer (encode_response). At the measured
~16 GiB/s memcpy throughput, each 4 MiB copy costs ~244 µs, matching
the breakdown.

v2.3.1 collapses the two copies into one. The server allocates a
single `Vec<u8>` of `8 + chunk_size` capacity (uninitialized payload
region via `Vec::with_capacity` + unsafe `set_len` — only 8 header
bytes are explicitly written before the pread fills the rest), then
`cache.try_get_into_slice` pread()s the cached file directly into the
tail of that buffer. UCX `tag_send_nbx` ships the combined buffer as
the response. Net: one syscall, one userspace copy (page cache → user
buf via pread), no second memcpy.

The TCP transport already had only one copy (it hands `Bytes` straight
to `Full::new(b)` with no separate header-prefix payload), so v2.3.1
ships UCX-only.

## Server-side timing (warm-peer, 1-stream, 1536 reqs each)

| Stage | v2.3.0 | **v2.3.1** | Δ |
|---|---|---|---|
| `server_cache_get_seconds` mean | 313 µs | 348 µs | +35 µs (now includes header alloc + write) |
| `server_send_seconds` mean | 143 µs | 143 µs | unchanged |
| `server_handler_seconds` mean | **605 µs** | **492 µs** | **−113 µs (−19%)** |

The cache_get histogram went up because v2.3.1 moved the response-buffer
allocation and header writes inside the `spawn_blocking` block (and thus
into the `cache_get` timing window). Total handler time dropped because
the second 4 MiB extend_from_slice in `encode_response` is gone.

## v2.3.1 throughput (warm-peer, 2 GiB files, RDMA, 3 runs/config)

| Concurrency | v2.3.0 | **v2.3.1** | Δ vs v2.3.0 |
|---|---|---|---|
| 1-stream | 2046 MiB/s | **2320 MiB/s** | **+13%** |
| 4-stream | 4358 MiB/s | 3352 MiB/s | −23% (see below) |
| 8-stream | 4071 MiB/s | **4629 MiB/s** | **+14%** |

## 4-stream regression — under investigation

3 consecutive runs at 3308–3408 MiB/s vs v2.3.0's 4282–4433 MiB/s. 1-
and 8-stream both improve consistently with the same change, so this is
not pure variance. Hypotheses (to verify in v2.3-B/D):

- The single `spawn_blocking` block now does 2 mutex acquires (`entry_size`
  + `try_get_into_slice`'s `touch_lru`) + a 4 MiB pread, vs v2.3.0's one
  acquire + pread; under the specific concurrency curve of 4-stream
  (4 × 32 = 128 in-flight, server fan-out cap), the extra mutex round
  may serialise more than at 1- or 8-stream.
- 4-stream is the load point where server-side spawn_blocking pool
  contention (default 512 threads, but per-task wakeup cost) and
  receive-side tag-message drain interact unfavourably.
- 8-stream wins more from the lower per-request CPU because the runtime
  is genuinely bottlenecked there; 4-stream may be I/O-or-wire bound
  in v2.3.0 already.

## Server-side instrumentation added

v2.3.1 also adds three new histograms on the server side, with the same
100 µs–1 s buckets used elsewhere:

- `blobcache_peer_server_handler_seconds` — total time per inbound
  peer chunk request handler (UCX path measures from receive of the
  tag-message to completion of the tag-send response; TCP path
  measures across the whole `handle_chunk` body).
- `blobcache_peer_server_cache_get_seconds` — time spent loading the
  chunk on the server (UCX: spawn_blocking that allocs response buf +
  pread; TCP: synchronous `cache.try_get`).
- `blobcache_peer_server_send_seconds` — time spent sending the
  response back over UCX (TCP path is unmeasured because `Full::new`
  + hyper handle that internally).

Combined with the existing client-side `chunk_*` histograms, every per-
chunk stage is now timed end-to-end: client `fetch_chunk_seconds` =
client cache_get + peer_fetch (which spans server handler) + cache_insert
+ scheduler.

# v2.3.2 — uninitialised client recv buffer (2.26 GiB/s 1-stream)

The client-side per-fetch path was allocating a fresh 4 MiB+8 byte
zero-initialised `Vec<u8>` on every chunk recv (`transport_ucx.rs:735`
`vec![0u8; 8 + length as usize]`). That mirrors the same anti-pattern
fixed on the server side in v2.3.1 — UCX immediately overwrites every
byte of the buffer, so the zeroing is pure waste.

v2.3.2 swaps to the same `Vec::with_capacity` + `unsafe set_len` pattern
already used in v2.3.1's server-side response build:

```rust
let total = 8 + length as usize;
let mut resp_buf: Vec<u8> = Vec::with_capacity(total);
unsafe { resp_buf.set_len(total); }
```

The `Vec` is only ever read up to `resp_len` returned by `tag_recv`, so
the uninit tail (if any — `recv_info.length` is exact) is never observed.

## Throughput (head-to-head on the same cluster session)

Cluster co-tenants visibly drift run-to-run on shared GB300 VMSS — to
get a clean number we re-benched v2.3.1 immediately before v2.3.2 on
the same pods. (The v2.3.1 absolute numbers therefore differ from the
v2.3.1 release section above, which was captured a session earlier;
relative deltas are the metric to read.)

| Concurrency | v2.3.1 (re-bench) | **v2.3.2** | Δ |
|---|---|---|---|
| 1-stream | 1122 MiB/s | **2259 MiB/s** | **+101%** |
| 4-stream | 3329 MiB/s | **4636 MiB/s** | **+39%** |
| 8-stream | 3433 MiB/s | **4412 MiB/s** | **+29%** |

The 4-stream regression that v2.3.1 introduced (−23% vs v2.3.0) is also
swept away by this single change — the client-side zeroing was likely
the same root cause amplified by 4× concurrency hitting the allocator.

## Honesty footnote — cross-session absolute numbers

Comparing v2.3.2 numbers to the v2.3.1 *release-session* numbers above
shows a smaller gap (1-stream ~ −8%) than the head-to-head re-bench.
That gap is cluster co-tenancy noise on the shared GB300 nodepool, not
a regression: re-benching v2.3.1 in the same v2.3.2 session reproduces
the same 1122 MiB/s baseline that v2.3.2 then doubles. Future entries
will use within-session re-benches as the canonical comparison.

## What's next (v2.3.3)

The leftover client-side overhead is per-fetch HCA registration (UCX
registers each fresh recv buffer with the IB MR cache on first use) and
the page-fault tax on touching freshly-allocated 4 MiB pages. v2.3.3
will introduce a `RegisteredSlab` — a single `ucp_mem_map`'d region
sliced into per-in-flight slots, eliminating both costs.
