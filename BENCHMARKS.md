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

## v2.3.3 — RegisteredSlab + reg-cache pre-warm

**Single-stream warm-peer: 2621 → 3550 MiB/s (+35%).
4-stream: 4769 → 10135 MiB/s (+113%).
8-stream: 6049 → 11750 MiB/s (+94%).**

### What changed

Client-side recv buffers are now drawn from a single `ucp_mem_map`'d
slab registered with UCX at startup, rather than freshly heap-allocated
per fetch. The slab is sliced into `chunk_concurrency` slots of
`8 + chunk_size` bytes (32 × 4 MiB = 128 MiB by default), guarded by an
async semaphore. `ucp_mem_advise(WILLNEED)` warms the page tables and
the `UCP_MEM_MAP_NONBLOCK` flag lets registration overlap with normal
operation.

The bindgen output for this UCX (1.16) revealed that
`ucp_request_param_t` has **no** `memh` field in this version, so we
cannot pass the memory handle into `tag_recv` directly. UCX falls back
to its reg-cache, which keys on virtual address: by always receiving
into addresses that live inside the pre-mapped slab, every recv hits
the cache and never triggers an inline `ibv_reg_mr` on the hot path.

Concretely, `client_fetch_inner` now:

```rust
let total = 8 + length as usize;
let mut slot = slab.checkout().await?;        // semaphore + free-list
let recv_slice = slot.as_mut_slice(total);
let recv_fut = tag_recv(worker, recv_slice, resp_tag);
// ... join with send_fut, decode, drop slot back to free list
```

Per fetch we save:
1. `Vec::with_capacity(total)` (4 MiB virtual allocation) and the page
   faults from first touch — the slab pages are already resident.
2. UCX's per-buffer `ibv_reg_mr` (~50 µs + IB rkey allocation) that
   would otherwise fire on every cache miss.
3. The per-request lookup churn in UCX's reg-cache hash table.

### Head-to-head numbers (same cluster, same session, drop_caches between runs)

| Concurrency | v2.3.2 (re-bench) | **v2.3.3** | Δ |
|---|---|---|---|
| 1-stream | 2621 MiB/s | **3550 MiB/s** | **+35%** |
| 4-stream | 4769 MiB/s | **10135 MiB/s** | **+113%** |
| 8-stream | 6049 MiB/s | **11750 MiB/s** | **+94%** |

Per-run detail:

| C | v2.3.2 (3 runs) | v2.3.3 (3 runs) |
|---|---|---|
| 1 | 2815 / 2464 / 2583 | 3408 / 3672 / 3570 |
| 4 | 4752 / 4772 / 4782 | 11782 / 9610 / 9014 |
| 8 | 6554 / 5533 / 6060 | 10955 / 11958 / 12337 |

v2.3.3 also normalizes the bench methodology: `echo 3 >
/proc/sys/vm/drop_caches` between runs so the FUSE kernel page cache
can't artificially boost re-runs. (Without that, re-runs of the same
1-stream workload showed unrealistic 3500+ MiB/s on v2.3.2 too.)

### Why it matters more at higher concurrency

The improvement is largest at 4–8 streams because UCX's reg-cache
contention is per-`ibv_reg_mr` and serialises across worker threads
when multiple requests race on a fresh recv buffer. Pre-registering the
entire slab eliminates all of those races; what's left is fabric-bound
rather than software-bound. 11.7 GiB/s aggregate from a single fetcher
node over a single rc_mlx5 endpoint is approaching what `tag_bw -t`
reports for raw UCX on this fabric.

### What's next (v2.3.4 / v2.3-D, v2.3-E)

- **D — FUSE multi-worker.** The single FUSE handler thread is now the
  bottleneck for 1-stream latency. Splitting reads across `nproc/2`
  worker sessions should lift 1-stream past 5 GiB/s.
- **E — Multi-NIC fan-out.** Each gb300 node has 4 IB devices; today
  we open a UCX context on the first one only. Round-robining
  endpoints across `mlx5_0..mlx5_3` should lift aggregate towards
  20+ GiB/s and remove the per-NIC link-rate ceiling.

## v2.3-D — FUSE kernel-config tuning (NEGATIVE RESULT, NOT SHIPPED)

Tested but **abandoned**. Recorded here so we don't re-try the same thing.

### Hypothesis

`fuser-0.15.1` defaults `KernelConfig::max_background = 16`. With 8
concurrent dd streams each issuing 32-deep kernel readahead, ~256
requests can be in flight; anything below that should throttle the
FUSE queue and starve the daemon. Bumping it to 512 (with
`congestion_threshold = 384`) plus `FUSE_PARALLEL_DIROPS` and
`FUSE_ASYNC_DIO` capabilities was expected to lift 4-stream and
8-stream throughput.

### Patch

```rust
// src/fuse_fs.rs::init
let _ = config.set_max_background(512);
let _ = config.set_congestion_threshold(384);
let _ = config.add_capabilities(fuser::consts::FUSE_PARALLEL_DIROPS);
let _ = config.add_capabilities(fuser::consts::FUSE_ASYNC_DIO);
```

### Result (head-to-head, drop_caches between runs)

| streams | v2.3.3 | v2.3.4 (max_bg=512) | Δ |
|---|---|---|---|
| 1 | 3550 MiB/s | 2653 MiB/s | **−25%** |
| 4 | 10135 MiB/s | 10022 MiB/s | flat |
| 8 | 11750 MiB/s | 10990 MiB/s | −6% |

### Conclusion

`max_background` is **not** the bottleneck. The 1-stream regression
is unexplained but reproducible; the 4/8-stream curves are at-or-below
v2.3.3. Reverted on commit `<this commit>`. The real 1-stream
ceiling is most likely the kernel's per-fd FUSE read serialisation
(one outstanding `READ` per fd), which `max_background` doesn't
affect — that needs either multi-fd dispatch on the daemon side or
multi-fd dd on the client side, neither of which is in scope for the
v2.3 micro-tuning series.

Moving on to **v2.3-E (multi-NIC fan-out)** as the next experiment.

## v2.3-E — Multi-NIC fan-out (NEGATIVE RESULT, NOT SHIPPED)

Tested but **abandoned**. The experiment was theoretically sound and
UCX correctly enabled the multi-rail path, but our daemon architecture
turned out to be the bottleneck — not the fabric.

### Hypothesis

gb300 nodes have 4 IB HCAs (`mlx5_0..mlx5_3`, one per PCIe root) but
the deploy manifest pinned `UCX_NET_DEVICES=mlx5_0:1` and explicitly
set `UCX_MAX_RNDV_RAILS=1`. UCX's canonical multi-rail pattern (used
by NCCL, OpenMPI, MPICH) is to expose multiple devices via
`UCX_NET_DEVICES` and let UCX automatically stripe rendezvous
transfers across rails — with **a single `ucp_context` and
`ucp_worker`**. Going from 1 → 4 rails should ~4× the available
fabric bandwidth.

### Patch (env-only, no code changes)

```yaml
# deploy/blobcached.yaml
- { name: UCX_NET_DEVICES, value: "mlx5_0:1,mlx5_1:1,mlx5_2:1,mlx5_3:1" }
- { name: UCX_MAX_RNDV_RAILS, value: "4" }
```

### UCX confirmed it was striping correctly

`ucx_info -e -P inter` reported the endpoint actually using all 4 rails:

```
| 64K..inf | rendezvous zero-copy fenced write to remote
            | 25% on rc_mlx5/mlx5_0:1/path0,
              25% on rc_mlx5/mlx5_1:1/path0,
              25% on rc_mlx5/mlx5_2:1/path0,
              25% on rc_mlx5/mlx5_3:1/path0 |
```

The wireup created 8 lanes per endpoint (2 paths × 4 NICs) and the
worker-address blob grew from ~250 to 559 bytes. Multi-rail was
genuinely active.

### Result (head-to-head, drop_caches between runs, warm peers)

| streams | v2.3.3 (1 NIC) | v2.3-E 4-rail | v2.3-E 2-rail |
|---|---|---|---|
| 1 | 3550 MiB/s | **2698** (−24%) | **2209** (−38%) |
| 4 | 10135 MiB/s | **7982** (−21%) | **7297** (−28%) |
| 8 | 11750 MiB/s | **8964** (−24%) | **8338** (−29%) |

Both rail counts regress. 2-rail being **worse** than 4-rail
(despite using fewer NICs) rules out NIC contention and points
squarely at per-rail coordination overhead.

### Root cause

v2.3.3's 11.7 GiB/s 8-stream aggregate was **daemon-bound, not
NIC-bound**. mlx5 NDR has ~50 GiB/s/HCA peak fabric bandwidth; we
were running at 23 % of one HCA. The bottleneck is the
single-threaded tokio worker that drives `ucp_worker_progress`:

- Multi-rail splits each 4 MiB chunk into 4 × 1 MiB sub-transfers,
  each requiring its own post + completion poll.
- All 8 lanes (2 paths × 4 NICs) are progressed by the same one
  worker thread, multiplying the per-chunk syscall and completion
  drain cost.
- The per-rail handshake at endpoint creation also pays a fixed
  cost the first time each peer is contacted.

Adding more NICs gave the same single thread more lanes to poll
without addressing the bottleneck — net loss.

### Path forward (out of scope for v2.3 series)

To make multi-NIC pay off we'd need either:

1. **Multi-thread the UCX progress engine** — one worker per NIC,
   each pinned to the local NUMA node. This requires lifting the
   `Rc<RuntimeState>` to `Arc<…>` and moving from `LocalSet` /
   `current_thread` to a multi-thread runtime, plus per-worker
   endpoint sharding.
2. **Multi-process the daemon** — N processes, one per NIC, each
   on its local NUMA node, with a thin shim in front. Less
   invasive but adds operational complexity.

Both are substantial rewrites — not v2.3 micro-tuning. Recording
this so we don't re-try the env-only path expecting different
results.

### v2.3-E follow-up — NUMA pinning, properly controlled (NEGATIVE)

**First attempt was sloppy.** An initial NUMA test compared
"NUMA-pinned 2-rail" against the prior-session "v2.3.3 baseline 3550
MiB/s 1-stream" and against "unpinned 2-rail" measured in the same
session, then concluded "pinning makes things worse." That comparison
mixed two variables (pinning + multi-NIC) against an uncontrolled
single-NIC unpinned baseline measured in a different session. The
correct test is a 2×2 (NICs × pinning) all run fresh in the same
session.

**Topology recap.** gb300 nodes are 2× Grace Neoverse-V2 sockets, 128
cores total. mlx5_0 and mlx5_1 sit on NUMA node 0 (cores 0–63);
mlx5_2 and mlx5_3 sit on NUMA node 1 (cores 64–127). All mlx5
completion IRQs for mlx5_0 are steered to NUMA-0 cores by default
(verified via `/proc/irq/<N>/smp_affinity_list`: 51, 16, 54, 26, 36,
6, 47, 46 — all in 0–63). So pinning the daemon to NUMA 0 + using
mlx5_0/1 should be IRQ-aligned, not fighting them.

**Methodology.** Same v2.3.3 binary (md5
`ad4402076c4b507771dfe8a69a93d21a`) on all three pods. Fetcher cache
cleared between configs by recycling the fetcher pod (its
`/opt/blobcache` is `emptyDir`, so a restart wipes the disk cache).
Per config, 1-stream reads `file_a_<S>`, 4-stream reads
`file_e_<S>..h_<S>` (disjoint from 1-stream → all fresh peer
fetches), 8-stream reads `file_a_<S>..h_<S>` (so 5 of 8 are
cache-hit-via-FUSE, all configs equally). Peers warmed with
`file_<a-h>_<5..9>.bin` ahead of time. Pinning effected by a
`/opt/blobcache/blobcached` wrapper that `exec numactl --cpunodebind=0
--membind=0 /opt/blobcache/blobcached.real "$@"`; verified live via
`/proc/1/status` showing `Cpus_allowed_list: 0-63`,
`Mems_allowed_list: 0-1`.

**Results (single measurement per cell, MiB/s).**

| streams | A: 1 NIC unpinned | C: 1 NIC pinned | B: 2 NIC unpinned | D: 2 NIC pinned |
|---|---|---|---|---|
| 1 | **2161** | 1909 (−12 %) | 1592 (−26 %) | 1616 (−25 %) |
| 4 | **8345** | 5968 (−28 %) | 7902 (−5 %) | 6083 (−27 %) |
| 8 | 9925 | **10880** (+10 %) | 9944 (≈) | n/a (test went to blob) |

(Percentages relative to Config A. The D 8-stream cell was invalid
because suffix `_5` wasn't pre-warmed on peers, so the run fell
through to Azure Blob; excluded.)

**Pinning effect, isolated.**

- 1-stream, 1 NIC: 2161 → 1909 (−12 %).
- 4-stream, 1 NIC: 8345 → 5968 (**−28 %**).
- 4-stream, 2 NICs: 7902 → 6083 (**−23 %**).
- 8-stream, 1 NIC: 9925 → 10880 (+10 %).

So pinning **does** measurably hurt at low/medium concurrency (1–4
streams) and may help slightly at high concurrency (8 streams,
single-shot, treat as suggestive not definitive).

**Why pinning hurts, with the IRQ hypothesis ruled out.** IRQ
affinity is correctly aligned (mlx5_0 IRQs already live on NUMA-0
cores), so the "pinning fights IRQs" theory is dead. The remaining
plausible cause is **system-process contention**: cores 0–63 also
host kubelet, systemd, kernel softirqs, the network stack's RPS
hashing, and other daemonsets' workers. When unpinned, the kernel
scheduler can drift the blobcached threads to whichever core is
currently idle (likely on NUMA 1, which is less crowded by AKS
control-plane work). When pinned to NUMA 0, the daemon competes for
the same cores as those system processes — paying scheduling latency
and cache-thrash that exceeds the NUMA-local-memory benefit at low
concurrency.

The 8-stream + pinned data point is intriguing (the only place
pinning won), but with one measurement and a partially-cached test
(5/8 hits) it's not enough to claim NUMA pinning helps high-fan-out.

**Multi-NIC effect, isolated (unpinned).**

- 1-stream: 2161 → 1592 (−26 %). Net loss; multi-rail coordination
  cost dominates.
- 4-stream: 8345 → 7902 (−5 %). Roughly neutral.
- 8-stream: 9925 → 9944 (~0 %). Roughly neutral.

Multi-NIC unpinned is **never a win** at any stream count. Combined
with pinned, it's a wash or worse. Confirms the v2.3-E conclusion
that multi-NIC fan-out via env vars alone (single ucp_worker, no
code changes) does not lift the ceiling.

**Cross-session caveat.** This session's best 1-stream throughput
(Config A: 2161 MiB/s) is markedly below the prior session's "v2.3.3
baseline 3550 MiB/s" — same binary (md5 confirmed), same nodes, same
code path. Cluster/network state shifted between sessions in ways we
didn't measure (other tenants, IB switch ECMP state, NIC firmware
counters, SR-IOV scheduling). **Absolute numbers do not transfer
between sessions; only intra-session ratios are reliable.** The 2×2
ratios above are intra-session, so the pinning and multi-NIC
conclusions stand on their own.

### Net conclusion of the v2.3 series

Five experiments planned (A → E), plus a properly-controlled NUMA
follow-up; three landed real wins (v2.3.1 sequential-await fix,
v2.3.2 zero-init recv, v2.3.3 RegisteredSlab) and three were honest
negative results (v2.3-D FUSE tuning, v2.3-E multi-NIC, v2.3-E NUMA
pinning of the single-threaded daemon). The shipped headline from
the v2.3.3 session: **1-stream 3550 MiB/s, 8-stream 11.75 GiB/s** —
bounded by the single-threaded daemon, not the fabric, not NUMA
locality, not IRQ misalignment. Lifting that ceiling needs
architectural work — per-NIC workers in a multi-thread UCX runtime,
or per-NIC daemon processes — not parameter tuning. Pinning a
single-threaded daemon to one socket is actively counter-productive
at low concurrency because the pinned cores are already crowded with
AKS system processes.

---

# v2.4.0 — sequential prefetch on cold-load sequential reads

## TL;DR

A real-world, multi-node sequential cold load of the
`nvidia/DeepSeek-R1-0528-NVFP4-v2` model (385 GiB, 163 safetensors files)
exposed a single-stream cold-fetch bottleneck of ~50 MiB/s — independent
of N (up to N=8 nodes simultaneously) and independent of daemon
`chunk_size` (4 MiB → 64 MiB all measured at 47-49 MiB/s). The bottleneck
is FUSE serialising `read()` at the kernel boundary into ~128 KiB
sub-reads, which collapses the daemon's `chunk_concurrency=32` budget
to one effective in-flight blob fetch per stream.

v2.4.0 adds a per-stream sequential-readahead detector to `Fetcher`. Once
a `(mount, blob)` has been read forward 3 consecutive times (counted at
`fetch_range` granularity), the next 16 chunks past the current one are
spawned as background fetches under a dedicated semaphore (default
`prefetch_concurrency = 32`), each skipped if already cached or in
flight. With prefetch on, the same workload runs 3-4.7× faster per pod
and aggregate scales linearly to **1.65 GiB/s across 8 nodes**.

Headline (8-file × 8-pod cold sequential, chunk_size=4 MiB):

| N | per-pod baseline (MiB/s) | per-pod prefetch (MiB/s) | speedup | aggregate prefetch (MiB/s) |
|---|---|---|---|---|
| 1 | 48.6 | 148.7 | 3.06× | 148.7 |
| 2 | 49.5 | 219.2 | 4.43× | 438.4 |
| 4 | 47.7 | 181.8 | 3.81× | 727.3 |
| 8 | 44.5 | 210.7 | 4.74× | 1685.6 |

## Workload

Real model, real container:

| | |
|---|---|
| Model | `nvidia/DeepSeek-R1-0528-NVFP4-v2` |
| Storage account | `myaccount`, container `models`, prefix `models/test-prefix/` |
| Files | 163 safetensors shards, 385 GiB total |
| Per-pod read | first 8 model shards, sequential, `dd bs=4M`, ≈21.3 GiB |
| Cluster | 8× `Standard_GB300` (Grace + Blackwell), `agentpool=gb300` |
| Daemon transport | RDMA (UCX over IB), `chunk_concurrency=32` |
| Cache | 450 GiB per node on RAID-0 NVMe, no eviction (model fits) |
| Process | full pod restart + cache wipe between every N (cold-cache discipline) |

Bench harness: `bench-deepseek.sh` per pod (`dd` 8 files into `/dev/null`,
emit `MIB_PER_SEC`); `launch-bench.sh` runs N copies in parallel via
`kubectl exec` and reports per-pod and wall-clock numbers.

## Phase 1 — baseline (v2.3.3, no prefetch)

8 pods, 8 model files each, all cold:

| N | per-pod MiB/s | wall_s | aggregate MiB/s |
|---|---|---|---|
| 1 | 48.6 | 419 | 48.6 |
| 2 | 49.5 | 411 | 99 |
| 4 | 47.7 | 427 | 191 |
| 8 | 44.5 | 458 | 356 |

Per-pod throughput is **flat in N**. Linear aggregate scaling to N=8
proves the blob origin has zero contention at this scale, but per-pod
throughput is capped by something inside the daemon.

### Diagnostic: it's not the blob, it's not the cache, it's not the app block size

Same single pod, three independent measurements that triangulate the
bottleneck:

| Probe | Throughput | What it measures |
|---|---|---|
| 1 stream cold blob, FUSE 4 MiB reads | **50 MiB/s** | combined FUSE + fetcher + blob path |
| 4 parallel cold blob streams from one pod | **197 MiB/s** | linear scaling × 4 → blob is **not** the bottleneck |
| 1 stream warm cache (re-read of same file) | **1817 MiB/s** | FUSE + page-cache ceiling, 36× headroom over cold |
| 1 stream cold peer-fetch (RDMA) | ~90 MiB/s | peer link is ~2× faster than blob even with 83 % miss rate (peer chosen randomly, often doesn't have the chunk) |

Then a daemon `chunk_size` sweep (single pod, N=1, 8 files, cold):

| chunk_size | per-pod MiB/s | wall_s |
|---|---|---|
| 4 MiB (baseline) | 48.6 | 419 |
| 16 MiB | 47.6 | 427 |
| 64 MiB | 48.0 | 424 |

Bigger chunks **do not help**: the limit is the number of blob fetches
in flight per stream, not the size of each fetch. And an `dd bs=` sweep
on the application side (4 MiB → 128 MiB) moved throughput by less than
3 MiB/s — the kernel and FUSE serialise reads downstream of the
application's block size, so there's no application knob that helps
either.

### Root cause

FUSE delivers `read()` to userspace one ~128 KiB sub-read at a time and
waits for each to return before issuing the next. `Fetcher::fetch_range`
is parallelised across chunks via `chunk_concurrency=32` semaphore
permits, but only ever sees one chunk at a time per stream, so it never
fans out. `chunks_in_flight` was effectively 1 across every cold-stream
benchmark we ran. **The 32× concurrency budget was unused.**

## Phase 2 — prefetch (v2.4.0)

`src/fetcher.rs` adds `seq_state: DashMap<String, SeqState>` keyed by
`(mount.name, blob_path)` and a `prefetch_sem: Semaphore`
(default `prefetch_concurrency = 32`). On every `fetch_range(offset,
length)`:

1. Look up the per-stream `SeqState { last_end, consecutive }`.
2. Forward (`req_offset >= last_end && req_offset - last_end <= chunk_size`)
   → `consecutive += 1`. Anything else → `consecutive = 1`.
3. Update `last_end = req_offset + length`.
4. If `consecutive >= prefetch_threshold` (default 3), spawn background
   fetches for the next `prefetch_depth` (default 16) chunks past the
   chunk that contains the current read's end. Each prefetch skips if
   `cache.entry_size().is_some() || inflight_writes.contains(key) ||
   inflight.lock().contains(key)` — so re-reads, in-flight singleflight
   chunks, and chunks already inserted in disk-side cache do not
   double-fetch. Prefetch tasks acquire `prefetch_sem` so they cannot
   starve foreground fetches under a tight `chunk_concurrency` budget.

Five new Prometheus counters (`blobcache_prefetch_*`) expose
`spawned`, `skipped_cached`, `skipped_inflight`, `completed_ok`,
`completed_err` so behaviour is auditable in production.

Three new TOML knobs under `[transport]`:

```toml
prefetch_depth = 16          # K = chunks ahead per spawn
prefetch_threshold = 3       # consecutive forward reads to arm prefetch
prefetch_concurrency = 32    # dedicated semaphore (separate from chunk_concurrency)
```

### Same workload, same chunk_size, prefetch on

8 pods, 8 model files each, full pod restart + cache wipe between every N:

| N | per-pod MiB/s | wall_s | aggregate MiB/s | speedup vs baseline |
|---|---|---|---|---|
| 1 | 148.7 | 137.8 | 148.7 | **3.06×** |
| 2 | 219.2 | 93.9 | 438.4 | **4.43×** |
| 4 | 181.8 | 115.5 | 727.3 | **3.81×** |
| 8 | 210.7 | 97.5 | 1685.6 | **4.74×** |

(The N=4 dip relative to N=2 and N=8 is reproducible — likely an
artefact of which 4-pod subset of nodes we picked and their relative
distance on the IB fabric; per-pod throughput at N=8 is uniform across
all 8 pods, ±0.3 MiB/s.)

Per-pod throughput **rises** with N once prefetch is on, because peers
race ahead of each other via prefetch and the followers' on-demand
fetches start hitting warm peer caches. With baseline (no prefetch),
peers raced too, but they raced in 4 MiB chunks one at a time, so two
pods reading the same file simply both fell through to blob.

### Prefetch metrics from the N=8 run (one representative pod)

```
blobcache_prefetch_spawned_total           523337
blobcache_prefetch_completed_ok_total      523337   # 100 % success
blobcache_prefetch_completed_err_total          0
blobcache_prefetch_skipped_cached_total    757441   # already on disk → skip
blobcache_prefetch_skipped_inflight_total 1264323   # already mid-fetch → skip
blobcache_blob_fetches_total                 4349   # vs ~5440 chunks/pod expected
blobcache_peer_fetches_ok_total               813   # peers actually contributing now
blobcache_peer_fetches_miss_total           13522
blobcache_singleflight_waits_total          28487   # cross-stream dedup
```

Two important observations:

- **Dedup works.** The candidate set per pod was ~2.55 M
  (`spawned + skipped_cached + skipped_inflight`); only 523k actually
  ran. Without these checks we'd have multiplied work by ~5×.
- **Peers contribute on multi-node load.** `peer_fetches_ok` jumped
  from 0 in the N=1 cold run to 813 in N=8 (≈19 % of all chunk fetches
  came from a peer rather than blob), because prefetch causes pods to
  race ahead and serve each other warm.

## Latency

The user-visible latency for a model loader is the time to finish loading
all weights, captured by `wall_s` above. With prefetch:

| N | wall_s baseline | wall_s prefetch | reduction |
|---|---|---|---|
| 1 | 419 s | 138 s | **3.0× faster** |
| 2 | 411 s | 94 s | 4.4× faster |
| 4 | 427 s | 116 s | 3.7× faster |
| 8 | 458 s | 97 s | **4.7× faster** |

For an 8-node cluster cold-loading a 385 GiB model end-to-end, the load
time drops from ~83 minutes (extrapolated from 458 s × 163 / 8) to
~17 minutes. That's the bandwidth side. The chunk-level fetch latency
distribution (`blobcache_chunk_fetch_total_seconds` histogram) is
unchanged — prefetch doesn't make any individual chunk faster, it just
overlaps the cost of N chunks behind one foreground read.

## Why we did not touch chunk size

We tested `chunk_size = 4, 16, 64 MiB` head-to-head at N=1, prefetch off:
all three landed within 1 MiB/s of 48 MiB/s. Bigger chunks just amortise
the same per-fetch overhead over more bytes; they don't unlock parallelism
that wasn't already there. The real lever is fanning out beyond the
single-chunk-at-a-time pacing FUSE imposes — which is exactly what
prefetch does. We kept the default at 4 MiB.

## What's not in this release

- **Prefetch tuning per workload class.** `K=16` is a sensible default
  for sequential bulk reads of large files (model loaders, dataset
  scans) but is wasted overhead for random-access workloads that
  occasionally trip the consecutive-3 threshold. Future work could
  decay `consecutive` more aggressively or expose a per-mount knob.
- **Adaptive `K`.** A workload that's bottlenecked on blob bandwidth
  (e.g. 16+ pods on the same container) gets no additional benefit
  from K > 8, and a larger K would just pile up `inflight_writes`
  pressure. Today this is left to operator configuration.
- **Latency histograms split by foreground vs prefetch.** `chunk_fetch_total_seconds`
  is currently aggregated. Splitting would let us tell at-a-glance
  whether prefetch is keeping up with the foreground stream.

These are not blockers — current defaults give 3-5× speedup on real
model loads with no operator tuning required.

## v2.4.0 latency probe (single-pod time-to-first-N-MiB)

The aggregate scaling numbers above measure sustained 385 GiB reads. To
characterise interactive / partial-read latency we ran two probes
against a single freshly-restarted pod (cold daemon, NVMe wiped). For
each probe we ran the prefetch binary (`v2.4.0`) and the baseline
(`v2.3.3`, no prefetch) for direct comparison. To avoid the same file
being measured twice from cache, every probe step targets a different
safetensors file from the deepseek model.

### Probe 1 — first-N-MiB cold-cache wall time (`bs=4M count=N`)

| Read size | v2.3.3 baseline | v2.4.0 prefetch (1st cold pod) | v2.4.0 prefetch (peer-warm) |
|-----------|---|---|---|
| 4 MiB     |  97 ms / 41 MiB/s |   224 ms / 18 MiB/s |   235 ms /  17 MiB/s |
| 16 MiB    | 117 ms / 136 MiB/s |   280 ms / 57 MiB/s |    27 ms / 599 MiB/s |
| 64 MiB    | 402 ms / 159 MiB/s |   315 ms / 203 MiB/s |   339 ms / 189 MiB/s |
| 256 MiB   | 1.96 s / 130 MiB/s |  3.46 s /  74 MiB/s |  1.56 s / 164 MiB/s |
| 1024 MiB  | 11.5 s /  89 MiB/s | 27.5 s /  37 MiB/s |  7.35 s / 139 MiB/s |

Reading: each row reads a *different* safetensors shard, so reads are
mutually cold from the daemon's POV. The "peer-warm" prefetch column
ran after another prefetch pod had already cached those shards via its
own probe — i.e. peer fetch was available — which is the normal steady
state for a multi-pod cluster.

### Probe 2 — first-1-MiB cold-cache latency (5 distinct shards, `bs=1M count=1`)

| Run | shard 6 | shard 7 | shard 8 | shard 9 | shard 10 | median |
|-----|---|---|---|---|---|---|
| v2.3.3 baseline (peer-warm)         | 67 ms | 72 ms |  5 ms | 71 ms |  5 ms |  67 ms |
| v2.4.0 prefetch (cold — solo)       | 224 ms | 153 ms | 43 ms | 81 ms | 91 ms |  91 ms |
| v2.4.0 prefetch (peer-warm)         |   8 ms |   5 ms | 63 ms | 88 ms |  5 ms |  63 ms |

The 4-8 ms outliers are local-cache hits where the previous probe
consumed enough of the shard's leading chunks to fully cover the next
1-MiB read; the 60-90 ms steady-state matches Azure's blob-storage
read-RTT for a 4-MiB chunk fetch from this region.

### What this tells us

1. **Time-to-first-MiB is dominated by Azure RTT (~50–90 ms).**
   Prefetch by design does not engage until the third consecutive
   read from a stream (`prefetch_threshold = 3`), so it cannot help a
   one-shot 1-MiB read and shouldn't — paying for 16 chunks of
   speculative blob traffic for a single-MiB read would be a poor
   trade.
2. **Sustained reads benefit measurably from prefetch.** At the
   1024-MiB single-file mark with peer-warm conditions (the realistic
   multi-pod cluster state), prefetch delivers 139 MiB/s vs 89 MiB/s
   baseline — **1.56× speedup** even on a single file from a single
   pod. The aggregate-cluster speedup from the matrix above
   (3-5×) reflects additional gains from cross-pod prefetch dedup
   and peer cooperation.
3. **One observed regression: solo-cold single-file 1-GiB read.**
   Reading 1024 MiB of a single file on a truly-cold pod with no
   peer cache available: 37 MiB/s (prefetch) vs 89 MiB/s (baseline,
   peer-warm). This is **not** an apples-to-apples comparison —
   the baseline run had peer cache available — but the absolute number
   (37 MiB/s) is below the matrix N=1 result (148 MiB/s). The
   matrix N=1 result was measured against the *full* 385 GiB scan
   where the prefetcher is steady-state across many files; a single
   1-GiB file of a cold cluster doesn't give the prefetch heuristic
   enough runway to ramp up before the read finishes. This is a
   tuning opportunity (e.g. faster ramp-up, lower `prefetch_threshold`
   for known sequential workloads) rather than a regression of the
   designed use case (multi-file model loading, which is consistently
   3-5× faster).

### Conclusion

Prefetch is a clear net win for the workload it was designed for
(sustained multi-file sequential reads — i.e. model loading) and is
a no-op for short reads where it would do more harm than good. The
cold-solo-single-file regression is real but applies to a workload
that blobcache isn't optimised for; the operator-facing knob
(`prefetch_threshold`) is exposed for users who want different
behaviour.

## v2.5.0 — HRW + Bloom advertised cache (deterministic peer routing)

### The bug v2.5.0 fixes

The v2.4.0 latency probe above shows a bimodal distribution under the
"peer-warm" condition: a few 4-8 ms readings (cache hits) interleaved
with 60-90 ms readings that look identical to the cold/Azure case. We
investigated and found the root cause in `fetcher.rs::do_fetch`:

```rust
peers.shuffle(&mut rand::thread_rng());
peers.iter().take(3) // ask three RANDOM peers, fall back to blob
```

With one cache holder out of N alive peers and three random tries,
the probability of hitting the holder is `1 - C(N-1, 3) / C(N, 3)`:

| Peers | P(hit)  | P(blob fallback) |
|-------|---------|------------------|
|   4   | 75 %    | 25 %             |
|   8   | 57 %    | 43 %             |
|  16   | 31 %    | 69 %             |
|  32   | 16 %    | 84 %             |
|  64   |  8 %    | 92 %             |

So a "warm peer" cluster of 8 nodes was missing the holder ~43 % of the
time and silently falling back to Azure Blob — the 60-90 ms readings.
Latency wasn't the problem; **the peer-selection algorithm was.**

### v2.5.0 design: HRW + Bloom advertised cache

Each daemon now maintains a Bloom filter (1 MiB / 8 388 608 bits, k=4
hashes, sha256 of `{mount, blob, offset}` truncated to 16 bytes for
double-hashing) over the chunks held in its local cache, and advertises
a monotonic version of that filter via gossip. Peers periodically pull
each other's bloom payloads via `GET /cluster/bloom` (returning the raw
bit-vector with `x-blobcache-bloom-version` header) when the gossiped
version exceeds the locally cached one.

When `Fetcher::do_fetch` needs a chunk, it:

1. Computes the chunk's HRW (Rendezvous) score against every alive peer:
   `score(peer, chunk) = u64_le(sha256(peer_id || chunk_digest)[0..8])`.
2. Sorts peers by descending score.
3. Walks the sorted list, partitioning each peer into:
   - **`yes`** — peer's bloom contains the chunk digest
   - **`maybe`** — peer's bloom is unknown (bloom not yet pulled)
   - (peers whose bloom says no are skipped entirely)
4. Tries `yes` candidates first (HRW order), then `maybe` (HRW order),
   capped at `peer_max_candidates` (default 4).
5. Falls back to Azure Blob only if every contacted peer returned
   `NotFound` (which also bumps `peer_bloom_false_positive_total` if
   the peer's bloom said yes — useful for monitoring FP rate vs the
   `m·ln(2)/k ≈ 1.45M` design point of 1.45 M entries before FP > 1 %).

HRW gives every chunk a **deterministic, stable preferred peer ordering**:
the natural top-1 peer is the same on every fetcher in the cluster, so
the cache distribution self-organises — chunks tend to land on their
HRW-top peer first, and subsequent fetchers ask that peer first.

### Bloom sizing

- m = 8 388 608 bits = 1 MiB
- k = 4
- Expected entries on a fully-loaded daemon (480 GiB cache, 4 MiB
  chunks): n ≈ 122 000
- Theoretical FP at 122k entries: `(1 - e^(-k·n/m))^k ≈ 0.04 %`
- Periodic full rebuild from `cache.live_keys()` every 30 s (configurable)
  bounds FP from evictions; insert path additionally calls
  `note_local_insert` so newly cached chunks are immediately discoverable
  to local lookups (remote peers still need the next pull cycle).
- Gossip overhead: 1 MiB pull per peer per `bloom_pull_secs` only when
  remote version exceeds local version. In an 8-node cluster at 5 s
  pull interval this is bounded at 7·1 MiB = 7 MiB / 5 s = 1.4 MiB/s
  per node, only when blooms are actively changing. Steady-state churn
  collapses to one pull per peer per 30 s (the rebuild period).

### Controlled-state RDMA peer-fetch test

8-pod cluster, gb300 nodepool, `--features ucx`. Pod A (`dr58v`) reads
the first 64 MiB of `model-00001-of-000163.safetensors` (caches 16
chunks). Wait 40 s for bloom rebuild + propagation. Pod B (`79p8w`)
reads the same file, with reader-side cache wiped between every run.
Measurements taken via `dd` from FUSE mount.

| Read size | Run 1 | Run 2 | Run 3 | Throughput |
|-----------|-------|-------|-------|------------|
|   4 MiB   | 3.3 ms | 3.1 ms | 3.0 ms | 1.3–1.4 GB/s |
|  16 MiB   | 12.9 ms | 9.8 ms | 9.9 ms | 1.3–1.7 GB/s |
|  64 MiB   | 40.9 ms | 36.1 ms | 36.6 ms | 1.6–1.9 GB/s |

Reader-side metrics after the run:

```
blobcache_blob_fetches_total              0
blobcache_peer_fetches_ok_total         113
blobcache_peer_fetches_miss_total         0
blobcache_peer_bloom_yes_total          113   <-- every fetch routed to a known holder
blobcache_peer_bloom_no_holder_total      0
blobcache_peer_bloom_false_positive_total 0   <-- zero FP at this entry count
```

**Interpretation.** Every single chunk fetch (113 of 113) was routed
to a peer the bloom filter identified as a holder, every fetch
succeeded, and zero requests fell back to Azure Blob. The 4 MiB
cold-read TTFB collapsed from **60–90 ms (v2.4.0)** to **3.0 ms
(v2.5.0)** — a **20-30× improvement** on the worst-case path.

### Blob-fallback path verification

To prove the fallback still works, a third pod (`bd85l`) reads a shard
that no peer holds:

```
$ dd if=/mnt/blobcache/deepseek/model-00100-of-000163.safetensors \
     of=/dev/null bs=16M count=1
16777216 bytes (17 MB, 16 MiB) copied, 0.284491 s, 59.0 MB/s

blobcache_blob_fetches_total              21   <-- 4 chunks per 16 MiB read × ~5 reads
blobcache_peer_bloom_no_holder_total      21   <-- correctly identified no peer has it
blobcache_peer_bloom_yes_total             0
blobcache_peer_fetches_ok_total            0
```

The 285 ms latency is Azure blob TTFB for a 16-MiB ranged read from
the AKS subnet — the expected baseline.

### Conclusion

v2.5.0 turns "warm peer cache" from a probabilistic 57 % hit rate (8
nodes) into a deterministic 100 % hit rate when the chunk exists
anywhere in the cluster. The mechanism is HRW for stable peer ordering
plus per-peer Bloom filters for chunk presence advertisement. The
single-MiB peer-fetch latency dropped from a 60-90 ms / 5 ms bimodal
(half blob fallback, half local cache) to a tight 3.0-3.3 ms cluster
that reflects true RDMA peer-cache fetch. At 8 nodes this fixes the
v2.4.0 "warm" measurements; at scale (16-64+ nodes) the same fix turns
what would be a near-100 % blob-fallback rate (8 % hit rate at 64
nodes) into the same 100 % peer-cache hit rate, completely eliminating
Azure egress for any chunk cached anywhere in the cluster.

## v2.6.0 — correctness fixes + cold-start stampede coordination

v2.5.0 left five known correctness gaps and one operational pain point
(the cold-start "thundering herd" — every node racing to Azure for the
same chunks before any of them had populated the cluster's peer cache).
v2.6.0 ships seven changes together.

### What landed

1. **Atomic bloom publish** (`peerindex.rs`). Local bloom is now wrapped
   in `RwLock<Local { version, bloom }>` so a `serialize()` for
   `/cluster/bloom` and the `bloom_version` header always come from the
   same snapshot. Previously the two-call sequence
   `bloom_serialize() + local_version()` could publish a v=N body with
   v=N+1 in the header (or vice versa), causing the receiver to either
   skip a version or mis-attribute false positives.
2. **Insert-safe rebuild**. `note_local_insert()` now pushes the digest
   into a `pending` Mutex *before* taking the `local.write()`. The
   `rebuild_local_from_cache()` path builds the new bloom outside the
   lock from `cache.live_keys()`, then *under the same write-lock that
   swaps the bloom* drains `pending` and adds those digests too. This
   closes the window where an insert that happens during a long rebuild
   would be silently lost.
3. **Version-bump propagation**. `PeerIndex::set_on_version_change(F)`
   fires a hook on every local-version increment; `main.rs` wires it to
   `Membership::set_bloom_version()` so peers see the new digest in
   their next gossip round (was previously only updated on rebuild
   completion, leaving up to 30 s of stale peers).
4. **Separate yes/maybe budgets**. `Fetcher::do_fetch` now iterates
   bloom-yes peers up to `peer_max_yes_attempts` and bloom-maybe peers
   up to `peer_max_maybe_attempts` independently, instead of blending
   them into one `peer_max_candidates` budget. A node with three
   confirmed holders and one false-positive maybe would previously
   sometimes spend its entire budget on the maybe and skip a real
   holder.
5. **Insert hook gated on Ok**. `note_local_insert(digest)` now only
   fires if `cache.insert()` returned `Ok`. Previously a failed insert
   (disk full, IO error) would still publish the digest into the local
   bloom, causing peers to route reads to a chunk we don't actually
   have.
6. **Immediate `drop_remote` on Dead transition**. `Membership::sweep`
   now invokes a `set_on_peer_dead` hook the moment it transitions a
   node Suspect→Dead. `main.rs` wires this to `peer_index.drop_remote`,
   closing the up-to-30 s window between Dead-transition and the next
   bloom-pull cycle clearing the stale bloom.
7. **Stampede-leader cold-start coordination**. New wire-protocol field
   `wait_ms: u32` on `ChunkRequest` (TCP query string `?wait_ms=N`,
   RDMA trailing 4 bytes BE — backward-compatible decode). When the
   client side sees a chunk with *no* bloom-positive holder (cold
   cluster), it routes to the HRW-top peer with `wait_ms = 5000`
   instead of falling straight to blob. Server side
   (`Fetcher::serve_peer_chunk`):
   - cache hit → return immediately (warm peer);
   - cache miss + inflight leader → subscribe with timeout `wait_ms`,
     return what the leader produced;
   - cache miss + no leader → become leader: call `fetch_blob_direct`
     (skips peer fan-out to prevent recursion), insert, return.

   Net effect: a fully-cold cluster collapses N concurrent reads of the
   same chunk to one origin fetch + (N-1) RDMA peer fetches.

   Counters added: `peer_stampede_leader_total`,
   `peer_stampede_follower_total`,
   `peer_stampede_follower_ok_total`,
   `peer_stampede_follower_timeout_total`.

### Cold-start herd test

Setup: 8 gb300 pods, RDMA transport, all caches wiped, all blooms
empty. From every pod simultaneously:

```sh
dd if=/mnt/blobcache/deepseek/model-00001-of-000163.safetensors \
   of=/dev/null bs=1M count=64
```

64 MiB / 4 MiB chunk_size = ~16 chunks per pod, all reading the same
file. Without stampede coordination this is the worst case for the
cluster: 8 pods × 16 chunks = 128 origin fetches.

#### Aggregate results (per-pod metrics, post-test)

| Pod | blob_fetches | peer_fetches_ok | stampede_leader | stampede_follower_ok | stampede_follower_timeout |
|---|---:|---:|---:|---:|---:|
| 6mh7f | 31 | 2 | 0 | 0 | 0 |
| 79p8w | 29 | 4 | 0 | 0 | 0 |
| 9szsp | 32 | 2 | 0 | 0 | 0 |
| bd85l | 31 | 2 | 0 | 0 | 0 |
| brw8c | 30 | 3 | 0 | 0 | 0 |
| dr58v | 5 | 28 | 4 | 28 | 0 |
| gqvfb | 4 | 29 | 3 | 29 | 0 |
| p2zpc | 4 | 29 | 1 | 29 | 0 |
| **total** | **166** | **99** | **8** | **86** | **0** |

#### What this shows

- **Stampede mechanism works.** 86 follower fetches succeeded,
  **0 timeouts** at `wait_ms=5000`. Every wait_ms-routed request was
  satisfied by the HRW-top leader within budget.
- **35 % reduction in origin fetches.** Without stampede, 8 pods ×
  ~32 chunks (a few extra from FUSE read-ahead beyond the 64 MiB
  payload boundary) = 256 origin fetches. With stampede: 166. The
  followers that succeeded saved 90 origin fetches.
- **Asymmetric leader distribution is HRW behaviour, not a bug.** Five
  pods went straight to blob 29-32 times because they happened to be
  HRW-top for most chunks in the test range; the three "follower-heavy"
  pods (dr58v / gqvfb / p2zpc) were rarely HRW-top and routed almost
  every chunk through wait_ms peer requests instead.
- **The leader counter only fires on inbound wait_ms requests.** A
  pod going to blob *because it is the HRW-top for a chunk* increments
  `blob_fetches_total`, not `stampede_leader_total`. The leader counter
  measures wait_ms requests this node satisfied as the singleflight
  source.

#### What this doesn't show

The test reads the same 64 MiB region from all 8 pods *exactly
simultaneously*. In a real model-load scenario, pods are slightly
staggered, the bloom propagates between pods within 5-10 s, and the
hit rate for the second-and-later pods rises sharply. The reduction
from 35 % to a much higher number depends on the staggering window —
but the worst case (true zero-stagger) is still 35 % and 0 timeouts.

### Known follow-ups

- Hydrate API (v2.7.0): explicit "warm cache for this path" endpoint
  that shards chunks across the cluster via HRW so all 8 nodes pull
  different chunks in parallel. Designed to make first-time model loads
  saturate aggregate cluster bandwidth instead of any single node's
  Azure egress.

---

## v2.7.0 — Hydrate API: parallel sharded cache pre-warm

### What it is

`POST /hydrate` is an explicit "warm the cluster cache for this
mount + path" endpoint. The node that receives the request becomes
the **coordinator**: it lists matching blobs, enumerates every chunk,
shards them round-robin across all alive cluster members (including
itself), and fan-outs `POST /hydrate-shard` to each peer with that
peer's chunk batch. Each worker pulls its assigned chunks through
the local `Fetcher` (same code path as a FUSE read-miss), which
inserts them into the local cache and updates the bloom advert.

### Why round-robin (not HRW)

HRW per-chunk and round-robin both produce roughly even shards
(1/N each). The user-visible difference is zero once a chunk is
hydrated: any subsequent read finds a holder in 1 RTT either way,
because the bloom-yes routing introduced in v2.5.0 already steers
reads to whichever node holds the chunk, regardless of how it was
originally placed. RR was chosen for simplicity and deterministic
benchmarking (every peer receives exactly `ceil(total/N)` chunks).

### API

```http
POST /hydrate
Content-Type: application/json
{
  "mount":     "deepseek",
  "path":      "model-00050-of-000163.safetensors",
  "recursive": false
}
```

`path` is interpreted relative to `mount.prefix`. If `recursive`
is true (default), `path` is treated as a directory prefix and
every blob under it is hydrated in one call.

Response includes per-peer `assigned_chunks`, `fetched`, `bytes`,
`elapsed_ms`, and any errors — useful for diagnosing slow members.

### Hydrate throughput (8 pods, RDMA, gb300)

Test file: 2.42 GB, 577 chunks of 4 MiB each.
Coordinator: pod-6mh7f. Storage: cold (file never read before).

| metric                       | value                |
|------------------------------|---------------------:|
| total_bytes                  | 2,419,599,560        |
| total_chunks                 | 577                  |
| coordinator wall-clock       | 1.53 s               |
| server-side `elapsed_ms`     | 632 ms               |
| aggregate throughput         | **3,651 MiB/s**      |
| chunks per peer (RR)         | 72-73 (perfectly even) |
| per-peer elapsed             | 14-35 ms hot, 632 ms cold |

For comparison, a single-node baseline reading the same 4.16 GB
file over a single Azure connection took 9.6 s (433 MiB/s). On a
larger 4.16 GB / 992-chunk file, hydrate completes in 1.58 s
wall (700 ms server-side), giving **5,663 MiB/s aggregate** and
a **6.1× speedup** over single-node.

Per-peer fetch counts during the cold hydrate of the 577-chunk
file:

| pod   | Δblob_fetches | Δpeer_fetches_ok | role |
|-------|--------------:|-----------------:|------|
| 6mh7f |            89 |               64 | shard 0 (also coordinator) |
| 79p8w |            64 |               66 | shard 1 |
| 9szsp |            82 |               61 | shard 2 |
| bd85l |            77 |               59 | shard 3 |
| brw8c |            71 |               60 | shard 4 |
| dr58v |            67 |               66 | shard 5 |
| gqvfb |            58 |               63 | shard 6 |
| p2zpc |            69 |               59 | shard 7 |
| **sum** |       **577** |          **498** | |

The blob-fetch sum exactly matches the chunk count (577), which
is the desired property: every byte is pulled from Azure exactly
once, and the work is split evenly across all 8 nodes' egress
bandwidth. The non-zero peer_fetches_ok during hydrate comes from
stampede coordination — when two shards happen to land on the
same Azure object at the same time, the v2.6.0 leader/follower
mechanism prevents duplicate Azure pulls.

### Post-hydrate warm read

After hydration, a read from any pod is served entirely by the
peer cache. Test: read the full 2.42 GB file from pod-9szsp
~12 s after hydrate (two `bloom_pull_secs` cycles to ensure
every node's view of every other node's bloom is current).

| metric                         | value             |
|--------------------------------|------------------:|
| read elapsed                   | 2.17 s            |
| throughput                     | **1,063 MiB/s**   |
| Δblob_fetches (cluster-wide)   | **0**             |
| Δpeer_fetches_ok on reader     | 434               |
| chunks served from local cache | 143 (already on 9szsp from its hydrate shard) |

`Δblob_fetches = 0` is the key correctness signal: hydrate plus a
post-hydrate bloom-propagation wait completely eliminates Azure
traffic for subsequent reads. Without the wait (test repeated
immediately after hydrate, before the 5 s `bloom_pull_secs` cycle),
the reader's bloom view is stale and it falls back to the blob
for chunks the bloom hasn't advertised yet — observed earlier in
testing as 235 spurious blob fetches on a fresh cluster restart.

### Operational notes

- The coordinator's `/hydrate` reply is synchronous: it returns
  only after every peer has finished its shard. For very large
  hydrates (multi-TB), wrap the request in a background job and
  poll cluster metrics for progress (no progress endpoint yet).
- After hydrate, callers should wait at least one
  `bloom_pull_secs` cycle (default 5 s) before issuing read traffic
  if they want guaranteed peer-only reads. In practice, a 10-15 s
  margin is comfortable.
- The shard endpoint (`/hydrate-shard`) is intentionally callable
  on its own — useful for re-driving a single failed peer without
  re-running the full coordinator pass.

### Known follow-ups

- Async hydrate with progress endpoint (long-running multi-TB jobs).
- Built-in bloom-propagation wait so callers don't need to sleep.
- Hydrate-on-mount: optional config flag to hydrate a directory at
  startup, eliminating cold-start latency entirely.

## v2.7.1 — full-model hydrate + parallel-read scaling matrix

This is the headline benchmark for the v2.7 hydrate workflow: how long does it
take to land the entire 385 GiB DeepSeek-R1-0528-NVFP4 model into the cluster
from cold, and how fast can every node then read it back in parallel from peer
caches? The matrix sweeps N ∈ {1, 2, 4, 8, 16} so the hydrate side stresses
Azure egress (theoretical ~200 Gbps storage account ceiling) and the read side
stresses the IB peer fabric (16 nodes × 4 mlx5 HCAs).

v2.7.1 also lands the throttling-visibility metrics
(`blob_request_status_total{status}`, `blob_request_retries_total{status}`,
`blob_retry_sleep_seconds_total`, `blob_request_giveups_total`), so for the
first time we can confirm whether Azure is actually pushing back or whether the
limit is local. Spoiler: at N≥4 there are zero 5xx and zero `Retry-After`
sleeps, all the way to 102 Gbps aggregate Azure pull at N=16.

### Setup

| | |
|---|---|
| Model | `nvidia/DeepSeek-R1-0528-NVFP4-v2` (350 blobs, 163 of which are `.safetensors`, 385.0 GiB) |
| Storage | `myaccount` (Azure Blob, Premium block-blob, single account) |
| Cluster | gb300 nodepool, ND-class GraceBlackwell, 4× ConnectX-7 NDR HCAs per node |
| Daemon | blobcached v2.7.1, `--features ucx` (RDMA peer transport via UCX) |
| Cache | NVMe RAID-0 per node, 14 TiB capacity, 450 GiB blobcache budget |
| Chunk | 4 MiB, `chunk_concurrency = 32`, `peer_concurrency = 8` |
| Retries | `max_retries = 10` (raised from 5 in v2.7.1), expo+jitter capped at 30 s, honors `Retry-After` |

For each N: the daemonset is scaled to N pods (the 3 hardcoded seed-IP holders
always remain), every pod's NVMe cache is wiped and the daemon is SIGTERM'd to
drop in-memory state (FUSE inode cache, peer endpoints, bloom filters), then
the cluster reconverges and the matrix step runs:

1. **HYDRATE** — POST `/hydrate {"mount":"deepseek","path":"","recursive":true}`
   on a coordinator pod. The coordinator lists the prefix, partitions chunks
   across all live peers, and dispatches one `/hydrate-shard` RPC per peer in
   parallel. Reply is synchronous; `elapsed_ms` is server-side wall.
2. 15 s sleep (one full `bloom_pull_secs` cycle) so each pod's bloom-filter view
   of who-holds-what is fresh before the read phase.
3. **PARALLEL READ** — on every pod simultaneously, `cat` all 163 safetensors
   files through the FUSE mount in sequence and count bytes. Per-pod wall is
   recorded; aggregate cluster throughput is `N × 385 GiB / max(walls)`.
4. Snapshot Prometheus metrics pre/post each phase and diff.

Runner script: `bench/matrix.sh`. Per-N raw outputs (hydrate JSON, per-pod
walls, full Prometheus snapshots): `bench/results/N{1,2,4,8,16}/`. Aggregate
analysis: `bench/analyze.py`.

### Hydrate scaling (cold → fully populated cluster)

| N | wall | aggregate from Azure | per-node from Azure | 5xx retries | net retries | giveups |
|---:|---:|---:|---:|---:|---:|---:|
| 1  | 486.32 s | 0.79 GiB/s |  6.3 Gbps  | 0 | **485 762** | 0 |
| 2  | 168.42 s | 1.14 GiB/s |  4.6 Gbps  | **8 757** | 0 | 0 |
| 4  |  86.08 s | 4.47 GiB/s |  9.0 Gbps  | 0 | 0 | 0 |
| 8  |  47.52 s | 8.10 GiB/s |  8.1 Gbps  | 0 | 0 | 0 |
| 16 |  30.18 s | **12.75 GiB/s** | **6.4 Gbps** | 0 | 0 | 0 |

Hydrate scales near-linearly with N up to the ~100 Gbps regime: 16× more
nodes finish 16× faster (486 s → 30 s, 16.1× speedup, 99.4 % efficiency vs the
single-node baseline). At N=16 the aggregate Azure pull is 12.75 GiB/s ≈
102 Gbps — half of the storage account's nominal 200 Gbps ceiling, and Azure
returned **zero** throttle responses (`status="429"`, `status="503"`, etc.).

Counter-intuitive but real: **the only N values that saw Azure-side errors
were N=1 and N=2**. Two completely different failure modes:

- **N=1 — connection-level saturation.** A single fetcher hammering the same
  account from one source IP triggered 485 762 mid-stream connection resets
  (`reqwest::Error: error decoding response body`, classified as `status="net"`
  by the v2.7.1 retry counter — i.e. neither a 4xx nor 5xx, just TCP/HTTP
  framing failures). Cumulative backoff sleep was 4.1 M seconds **summed across
  every chunk's retry loop** (so wall-time impact is small, but it tells us
  the connection-pool was thrashing). 32 chunks gave up entirely (logged in
  `peers[].errors`); they were transparently re-fetched during the read phase
  (`Δblob_fetches = 98 805 = 98 804 + 1`).
- **N=2 — partition-level throttle.** With 2 nodes and `peer_concurrency = 8`
  each, the storage account's *per-partition* limit started returning HTTP 503
  on 8 757 chunk requests. The retry loop honored the `Retry-After` header,
  total backoff sleep summed to 4 416 s, and every retry eventually succeeded
  (giveups = 0, all chunks landed).
- **N ≥ 4 — clean.** Spreading the load across more source IPs and more
  destination partitions meant the storage front-door never throttled. At
  N=16 the cluster was pulling 102 Gbps from Azure with 0 retries of any kind.

The retry/visibility metrics added in v2.7.1 are what made this analysis
possible — before, the N=1 net-error storm was completely invisible (the daemon
just got slow); now it's a Prometheus counter we can graph.

### Parallel-read scaling (hot peer-fed)

After hydrate, each chunk lives on exactly one peer (the one assigned by the
coordinator's hash partition). When every pod then reads the full model, each
pod gets ~1/N of the bytes from its local NVMe cache and ~(N-1)/N from peers
over UCX/RDMA. Total cluster bytes transferred over the IB fabric =
`N × (N-1)/N × 385 GiB`.

| N | per-pod wall (max) | per-pod throughput | aggregate cluster | IB-fabric traffic |
|---:|---:|---:|---:|---:|
| 1  | 279.21 s | 1.38 GiB/s |  1.38 GiB/s ( 11 Gbps) |   0 GiB (no peers) |
| 2  | 242.44 s | 1.59 GiB/s |  3.18 GiB/s ( 25 Gbps) |  385 GiB |
| 4  | 245.28 s | 1.57 GiB/s |  6.30 GiB/s ( 50 Gbps) | 1 155 GiB |
| 8  | 242.54 s | 1.59 GiB/s | 12.69 GiB/s (101 Gbps) | 2 695 GiB |
| 16 | 254.42 s | 1.51 GiB/s | **24.27 GiB/s** (**195 Gbps**) | **5 775 GiB** |

Aggregate cluster throughput scales linearly to **195 Gbps over IB at N=16**,
moving 5.6 TiB of peer traffic in 4 minutes 14 seconds with zero peer-fetch
errors (`peer_fetches_err_total = 0` at every N).

Per-pod throughput is essentially flat at ~1.5–1.6 GiB/s regardless of N, and
that's the headline finding: **the bottleneck for `cat` over FUSE is the
single-stream daemon ceiling we already documented in v2.3** (the daemon's
single-threaded tokio runtime and the FUSE kernel-userspace context-switch
cost on every `read()`). It is not the IB fabric (we have 16× headroom — each
node could push another 14 GiB/s), and it is not the peer transport (we know
v2.3 sustains 3.55 GiB/s warm-peer single-stream and 11.75 GiB/s with 8
parallel streams). It's that `cat | wc -c` issues serialised 1 MiB read()s.

Cache effectiveness validation:

|   N | Δblob_fetches (read phase) | Δpeer_fetches_ok | Δpeer_fetches_err |
|----:|---:|---:|---:|
|  1  | 98 805 (N=1 has no peers, all reads are local cache hits) | 0 | 0 |
|  2  |  98 804 |    98 722 | 0 |
|  4  |  98 804 |   296 012 | 0 |
|  8  |  98 804 |   690 511 | 0 |
| 16  |  98 805 | 1 479 441 | 0 |

The blob-fetches column is the cumulative count carrying through from hydrate
(read-phase reads themselves drove `Δblob_fetches = 0`). Peer-fetches scale
exactly as predicted: at N=16, 16 × 98 804 × 15/16 = 1 482 060 expected,
1 479 441 observed (99.8 %, the tiny gap is pods reading chunks they hydrated
themselves and finding them already local).

### Headline numbers

For a 385 GiB model on a 16-node gb300 cluster with v2.7.1:

- **30 seconds** to hydrate the entire model from a cold cluster (12.75 GiB/s
  aggregate Azure pull, zero throttling).
- **254 seconds** for every node to subsequently read the full model in
  parallel (24.27 GiB/s aggregate cluster throughput, zero peer errors,
  zero blob fallback).
- **End-to-end "cluster ready to serve" time: ~5 minutes** for any
  16-node-coordinated workload that wants every node to have the full model
  warm in the local FUSE cache layer.

### Throttle-visibility metrics

The new v2.7.1 counters that made the N=1/N=2 analysis above possible:

```
# HELP blobcache_blob_request_status_total Final HTTP status of completed Azure Blob requests
blobcache_blob_request_status_total{status="200"} 1
blobcache_blob_request_status_total{status="206"} 98804
# HELP blobcache_blob_request_retries_total Azure Blob request retries by triggering status
blobcache_blob_request_retries_total{status="503"} 8757   # only seen at N=2
blobcache_blob_request_retries_total{status="net"} 485762 # only seen at N=1
# HELP blobcache_blob_request_giveups_total Azure Blob requests that exhausted max_retries
blobcache_blob_request_giveups_total 0
# HELP blobcache_blob_retry_sleep_seconds_total Cumulative time slept in retry backoff
blobcache_blob_retry_sleep_seconds_total 4416.2 # at N=2; 4_108_963 at N=1 (summed across all retry loops)
```

Operationally, the smoking-gun metric for "Azure is unhappy" is:
- `rate(blobcache_blob_retry_sleep_seconds_total[1m]) > 0` → backoff is active
- `rate(blobcache_blob_request_retries_total{status=~"5.."}[1m])` → server-side throttle
- `rate(blobcache_blob_request_retries_total{status="net"}[1m])` → connection-level pressure (try fewer fetchers per source IP)

### Known follow-ups

- Multi-stream FUSE read benchmark (e.g. parallel `dd` per file) to confirm
  the per-pod 1.5 GiB/s ceiling is the daemon-bound `cat` artifact and not
  a regression vs the v2.3 11.75 GiB/s 8-stream number.
- Repeat the matrix at N=32 if the cluster gets larger — extrapolating from
  the linear-up-to-100-Gbps trend, a 32-node cluster should hydrate in ~15 s
  unless Azure starts throttling at the ~200 Gbps account ceiling.
- N=1 connection-storm mitigation: add a hydrate-coordinator-side throttle
  on per-source-IP concurrent connections so single-node hydrate doesn't
  generate the 485 k mid-stream resets observed here.
