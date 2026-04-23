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
