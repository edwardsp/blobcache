# blobcache — `blobcached.toml` configuration reference

Complete reference for every option the daemon reads, derived from `src/config.rs`
(the authoritative TOML schema) plus the validation rules and `cluster_hash`
membership. Defaults shown are the daemon's built-in defaults; anything marked
**required** has no default and must be present.

A minimal config has `[cache]`, `[cluster]`, `[transport]`, `[stats]`, and at
least one `[[mounts]]` block. `[azure]` is optional (it has sane defaults).

---

## Top level

| Key | Type | Default | Description |
|---|---|---|---|
| `node_id` | string | hostname | Stable identity for this node in the cluster. Optional; defaults to the machine hostname, which is already unique across a Slurm cluster. |

---

## `[cache]`  (required)

On-disk NVMe chunk cache.

| Key | Type | Default | Description |
|---|---|---|---|
| `dir` | path | **required** | Directory for the on-disk chunk cache (point at the NVMe RAID, e.g. `/mnt/nvme/blobcache-cache`). |
| `max_bytes` | uint | **required** | Max cache size in bytes (LRU-evicted). Size to ~80–90% of NVMe capacity. |
| `chunk_size` | uint | `4194304` (4 MiB) | On-disk chunk size and the peer/cache key boundary. **Must be a non-zero multiple of 4096.** **Part of `cluster_hash` — must match cluster-wide.** |
| `cache_on_peer_fetch` | bool | `true` | When `true`, chunks fetched from a peer are written to local disk so later local reads hit instantly. When `false`, peer-fetched chunks are served but never inserted (disk cache only grows from blob fetches) — maximises effective cluster capacity (no replication) and isolates peer-fetch throughput in benchmarks. Blob fetches always cache regardless. |
| `peer_lru_bytes` | uint | `1073741824` (1 GiB) | Bounded in-memory LRU of peer-fetched chunks, so within-chunk locality (FUSE issues ~32 sub-reads per 4 MiB chunk) doesn't re-fetch a chunk per sub-read. `0` disables. Relevant mainly when `cache_on_peer_fetch = false`. |

---

## `[azure]`  (optional)

Blob-origin fetch tuning. Local-only knobs (not in `cluster_hash`).

| Key | Type | Default | Description |
|---|---|---|---|
| `pool_max_idle_per_host` | uint | `512` | reqwest idle-socket pool size per storage-account host. Must be ≥ 1. |
| `block_size` | uint | `0` | Bytes per Azure GET, decoupled from `chunk_size`: the daemon issues block-sized GETs then slices each into `block_size / chunk_size` cache chunks. `0` = use `chunk_size`. When non-zero, **must be ≥ `chunk_size` and a multiple of it**. Larger blocks cut per-request overhead and Azure throttling (small chunks at low concurrency hit the request-rate limit before the bandwidth limit). |
| `workers` | uint | `1` | Number of independent tokio runtimes (each its own reqwest pool) dispatching Azure GETs. A single runtime tops out near ~28 Gbps regardless of concurrency; raise to 4–8 to scale a node past that. Must be ≥ 1. |
| `main_worker_threads` | uint | `8` | Worker-thread cap for the main runtime (FUSE handlers, gossip, peer server, stats). Caps scheduler overhead on high-core nodes. Must be ≥ 1. Independent of `workers`. |

---

## `[cluster]`  (required)

Gossip membership (port 7771 by convention).

| Key | Type | Default | Description |
|---|---|---|---|
| `bind` | string | **required** | Address:port for the gossip server, e.g. `0.0.0.0:7771`. |
| `seeds` | array<string> | `[]` | Gossip seed URLs, e.g. `["http://node-0002:7771"]`. Any one alive node is enough; push-pull discovers the rest. |
| `advertise` | string\|null | `null` | URL other nodes should use to reach this node's gossip server. `null` = derive automatically. Set explicitly when the bind address isn't reachable by peers (e.g. pod vs node IP). |

---

## `[transport]`  (required)

Peer chunk transport (port 7772), peer selection, prefetch, and bloom advertisement.

### Binding & kind

| Key | Type | Default | Description |
|---|---|---|---|
| `bind` | string | **required** | Address:port for the peer transport server, e.g. `0.0.0.0:7772`. |
| `advertise` | array<string> | `[]` | URL(s) peers use to reach this node's transport. Empty = derive automatically. |
| `kind` | string | `"tcp"` | Peer transport: `"tcp"` (HTTP/1.1) or `"rdma"` (UCX over InfiniBand; requires the `ucx` build feature + runtime libs). **Part of `cluster_hash` — must match cluster-wide.** |

### Concurrency

| Key | Type | Default | Description |
|---|---|---|---|
| `chunk_concurrency` | uint | `32` | Max concurrent chunk fetches fanned out per ranged read (also gates hydrate Phase-A burst load). |
| `peer_concurrency` | uint | `8` | Max concurrent outbound peer requests. |

### Prefetch (sequential readahead)

| Key | Type | Default | Description |
|---|---|---|---|
| `prefetch_depth` | uint | `16` | Chunks to fetch ahead of the read head once a stream is detected sequential. `0` disables prefetch. |
| `prefetch_threshold` | uint | `3` | Consecutive forward reads on a stream before prefetch triggers. |
| `prefetch_concurrency` | uint | `32` | Max in-flight background prefetch fetches. |
| `prefetch_origin_only` | bool | `false` | When `true`: (1) prefetch only triggers on streams whose recent fetches came from Azure (a peer-fetch success resets the streak), and (2) prefetched chunks use the origin-only path, bypassing peer fan-out and stampede-leader hops. |

### Peer selection (HRW-ranked, bloom-aware)

For each missing chunk, alive peers are ranked by rendezvous (HRW) hashing and
split into a **yes-set** (their advertised bloom contains the chunk) and a
**maybe-set** (peers with no bloom yet, e.g. just joined). The yes-set is tried
first, then the maybe-set; each has its own attempt budget so false positives in
one can't starve the other.

| Key | Type | Default | Description |
|---|---|---|---|
| `peer_max_candidates` | uint | `4` | Overall cap on ranked peer candidates considered per chunk. |
| `peer_max_yes_attempts` | uint | `2` | Max peers tried from the yes-set (bloom says "have it"). |
| `peer_max_maybe_attempts` | uint | `2` | Max peers tried from the maybe-set (unknown bloom). |
| `stampede_wait_ms` | uint | `5000` | Cold-start stampede control: if no peer claims a chunk, route to the HRW-top owner and wait up to this long so the first node to reach blob becomes the singleflight "leader" and the rest piggyback — avoids N nodes each issuing an independent blob GET for the same file. `0` disables. |

### Bloom advertisement

Each node advertises which chunks it holds via a bloom filter, gossiped to peers.

| Key | Type | Default | Description |
|---|---|---|---|
| `bloom_bits` | uint | `8388608` (1<<23, ~1 MiB) | Size of this node's peer-advertisement bloom **in bits**. Bigger → lower false-positive rate (fewer wasted peer probes) at the cost of memory + gossip bytes. Self-describing on the wire (peers read each other's size), per-node, and **not** in `cluster_hash`, so it can differ between nodes and change without breaking merges. Clamped to ≥ 64 bits. |
| `bloom_rebuild_secs` | uint | `30` | How often a node rebuilds its own bloom from its current cache contents. |
| `bloom_pull_secs` | uint | `5` | How often a node pulls peers' blooms. |

---

## `[stats]`  (required)

| Key | Type | Default | Description |
|---|---|---|---|
| `bind` | string | **required** | Address:port for the stats/admin HTTP server, e.g. `0.0.0.0:7773`. Serves `/metrics` (Prometheus), `/stats`, `/peers`, and the `/clear-cache` + `/hydrate` admin API. |

---

## `[[mounts]]`  (at least one required, repeatable)

One block per blob path exposed under `/blobcache/`.

| Key | Type | Default | Description |
|---|---|---|---|
| `name` | string | **required** | Mount name (non-empty). **In `cluster_hash`.** |
| `mountpoint` | path | **required** | FUSE mountpoint, e.g. `/blobcache/pexels`. |
| `account` | string | **required** | Storage account **short name** (not the FQDN). Non-empty. **In `cluster_hash`.** |
| `container` | string | **required** | Blob container. Non-empty. **In `cluster_hash`.** |
| `prefix` | string | `""` | Path prefix within the container to scope the mount to one directory. **In `cluster_hash`.** |
| `sas_token` | string\|null | `null` | Optional SAS token for this mount. Omit to use Managed Identity (workload-identity → IMDS → anonymous). |

---

## Validation rules (daemon refuses to start otherwise)

- `cache.chunk_size` must be non-zero and a multiple of `4096`.
- `azure.pool_max_idle_per_host`, `azure.workers`, `azure.main_worker_threads` must each be ≥ 1.
- `azure.block_size`, when non-zero, must be ≥ `cache.chunk_size` and a multiple of it.
- At least one `[[mounts]]` block, each with non-empty `name`, `account`, `container`.
- `transport.kind` must be `"tcp"` or `"rdma"`.

## `cluster_hash` — what must match across the cluster

Peers refuse to merge if their `cluster_hash` differs (counter
`blobcache_cluster_config_mismatches_total`). It is a SHA-256 over **only**:

- `cache.chunk_size`
- `transport.kind`
- every mount's `name`, `account`, `container`, `prefix` (order-independent)

Everything else (cache size, azure tuning, concurrency, prefetch, bloom sizing,
peer-attempt budgets, bind/advertise addresses, `node_id`) is **node-local** and
may differ between nodes.

> Note: `blobcached` does not unmount its FUSE mounts on stop, so after editing
> the config use the clean stop → `umount` → `systemctl reset-failed` → start
> sequence (see `docs/slurm.md`) rather than a bare `systemctl restart`, which
> crash-loops on `File exists (os error 17)`.

---

## Auth (environment, not the TOML)

When a mount has no `sas_token`, `Credential::resolve()` tries, in order:

1. Inline `sas_token` (if set on the mount)
2. **Workload Identity** — env `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_FEDERATED_TOKEN_FILE`
3. **IMDS** (`169.254.169.254`) — uses `AZURE_CLIENT_ID` to pick the right user-assigned MSI
4. Anonymous

Set `AZURE_CLIENT_ID` (e.g. in `/etc/default/blobcached`) to the client-id of the
user-assigned identity that holds **Storage Blob Data Reader** (or higher) on the
target account.

---

## Annotated example

```toml
node_id = "node-a"                     # optional; defaults to hostname

[cache]
dir = "/mnt/nvme/blobcache-cache"
max_bytes = 1099511627776              # 1 TiB
chunk_size = 4194304                   # 4 MiB (cluster_hash)
cache_on_peer_fetch = true
peer_lru_bytes = 1073741824            # 1 GiB

[azure]
pool_max_idle_per_host = 512
workers = 4                            # 4 blob-fetch runtimes
main_worker_threads = 8
block_size = 0                         # 0 => use chunk_size

[cluster]
bind = "0.0.0.0:7771"
seeds = ["http://node-0002:7771"]
# advertise = "http://10.0.0.6:7771"

[transport]
bind = "0.0.0.0:7772"
kind = "tcp"                           # or "rdma" (cluster_hash)
chunk_concurrency = 32
peer_concurrency = 8
prefetch_depth = 16
prefetch_threshold = 3
prefetch_concurrency = 32
prefetch_origin_only = false
bloom_bits = 8388608                   # 1 MiB bloom
bloom_rebuild_secs = 30
bloom_pull_secs = 5
peer_max_candidates = 4
peer_max_yes_attempts = 2
peer_max_maybe_attempts = 2
stampede_wait_ms = 5000

[stats]
bind = "0.0.0.0:7773"

[[mounts]]
name = "models"
mountpoint = "/mnt/blobcache/models"
account = "myaccount"
container = "models"
prefix = ""
# sas_token = "..."                    # omit to use Managed Identity
```
