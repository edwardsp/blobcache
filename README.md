# blobcache

Distributed FUSE-mounted Azure Blob cache for high-throughput AI/HPC clusters.

Each node mounts one or more blob containers as read-only filesystems. Reads
miss into a local NVMe-backed chunk cache; on miss the daemon first asks
peer nodes (discovered via gossip) for the chunk, falling back to Azure Blob
only if no peer has it. Designed for GB300-class nodes with InfiniBand
HCAs (currently transports over TCP; the IB devices are passed into the pod
so an RDMA/UCX backend can be wired in without changing the deployment).

## Architecture

```
┌────────── pod (gb300 node) ───────────┐
│                                       │
│  user → /mnt/blobcache/<mount>/...    │
│         │                             │
│         ▼ FUSE (fuser)                │
│  ┌──────────────┐                     │
│  │ BlobFs       │ readdir/getattr/read│
│  └──────┬───────┘                     │
│         ▼                             │
│  ┌──────────────┐  cache hit          │
│  │ Fetcher      │────► DiskCache (NVMe RAID-0)
│  └─┬──────┬─────┘                     │
│    │      │ peer miss                 │
│    │      ▼                           │
│    │  ┌──────────────┐  HTTP/1.1      │
│    │  │ PeerClient   │──:7772──► other node's PeerService
│    │  └──────────────┘                │
│    │ blob fallback                    │
│    ▼                                  │
│  ┌──────────────┐                     │
│  │ BlobClient   │── HTTPS ── Azure Blob (MSI bearer)
│  └──────────────┘                     │
│                                       │
│  GossipServer :7771 ◄── push/pull ──► other nodes
│  PeerService  :7772 ◄── chunk GET ──► other nodes
│  Stats        :7773  /metrics /stats /peers
└───────────────────────────────────────┘
```

| Port | Purpose |
|------|---------|
| 7771 | Gossip (HTTP push-pull membership + cluster-hash check) |
| 7772 | Peer chunk transport (HTTP GET `/v1/chunk/{mount}?blob=…&offset=…`) |
| 7773 | Stats: `/metrics` (Prometheus), `/stats` (JSON), `/peers` (JSON) |

All three speak HTTP/1.1 today. The peer transport is behind a `Transport`
trait shape so an RDMA/UCX backend can be added later without touching FUSE
or fetcher code.

## Build

Native (Linux x86_64 or aarch64):

```sh
sudo apt-get install -y libfuse3-dev pkg-config
cargo build --release
```

Cross-build for cluster: see `Deploy → cross-compile note` below.

## Configure

`/etc/blobcached.toml`:

```toml
node_id = "node-a"

[cache]
dir = "/mnt/nvme/blobcache-cache"
max_bytes = 107374182400      # 100 GiB
chunk_size = 4194304          # 4 MiB; MUST match across cluster

[cluster]
bind = "0.0.0.0:7771"
seeds = ["http://10.0.0.5:7771"]
advertise = "http://10.0.0.6:7771"

[transport]
bind = "0.0.0.0:7772"
advertise = ["http://10.0.0.6:7772"]
chunk_concurrency = 32
peer_concurrency = 8

[stats]
bind = "0.0.0.0:7773"

[[mounts]]
name = "models"
mountpoint = "/mnt/blobcache/models"
account = "myaccount"
container = "models"
prefix = ""
# sas_token = "..."           # optional; omit to use managed identity
```

`cluster.advertise` and `transport.advertise` must be reachable from other
nodes. With `hostNetwork: true` use the node IP; otherwise use the pod IP.

A SHA-256 over `chunk_size` and the sorted mount list is exchanged with
every gossip round; peers with mismatched hashes are rejected at merge time
(`blobcache_cluster_config_mismatches_total`).

## Run

```sh
RUST_LOG=info,blobcache=debug \
  ./blobcached --config /etc/blobcached.toml
```

The daemon mounts each `[[mounts]]` entry on startup (FUSE), starts the
gossip / peer / stats servers, joins via `cluster.seeds`, and stays in
the foreground until `SIGINT`/`SIGTERM`.

### Auth resolution

`Credential::resolve()` tries in order:

1. Inline `sas_token` in the mount entry
2. Workload Identity (env: `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`,
   `AZURE_FEDERATED_TOKEN_FILE`)
3. IMDS (`http://169.254.169.254`) — uses `AZURE_CLIENT_ID` to
   disambiguate when the VMSS has multiple user-assigned identities
4. Anonymous

Set `AZURE_CLIENT_ID` to the client-id of the user-assigned MSI that
holds **Storage Blob Data Reader** (or higher) on the target account.

## Deploy on AKS (gb300 nodepool)

The `deploy/blobcached.yaml` manifest provisions:

1. **`nvme-raid-init` DaemonSet** — privileged, runs once per gb300 node,
   uses `nsenter` to assemble all spare NVMe disks into `/dev/md/blobcache`
   (mdadm RAID-0), formats ext4, and mounts on the host at `/mnt/nvme`.
   Idempotent.
2. **`blobcached` DaemonSet** — Ubuntu 24.04 base, `hostNetwork: true`,
   privileged with `SYS_ADMIN` + `IPC_LOCK`, requests `rdma/ib: 1` per
   pod (IB device passthrough via the SR-IOV device plugin), pinned to
   `agentpool=gb300` and labelled test nodes (`blobcache.test/enabled=true`).

### Apply

```sh
kubectl label node <node1> <node2> <node3> blobcache.test/enabled=true
kubectl apply -f deploy/blobcached.yaml
```

### Cross-compile note

GB300 nodes are aarch64 (NVIDIA Grace). Build the binary there to avoid
juggling sysroots locally:

```sh
# spin a one-shot rust:1.86-bookworm builder pod on a gb300 node
kubectl -n blobcache run blobcache-builder \
  --image=rust:1.86-bookworm --restart=Never \
  --overrides='{"spec":{"nodeSelector":{"kubernetes.azure.com/agentpool":"gb300"}}}' \
  -- sleep infinity
kubectl -n blobcache cp src.tar.gz blobcache-builder:/tmp/
kubectl -n blobcache exec blobcache-builder -- bash -c '
  apt-get update -qq && apt-get install -y -qq libfuse3-dev pkg-config
  cd /work && tar xzf /tmp/src.tar.gz && cargo build --release
'
kubectl -n blobcache cp blobcache-builder:/work/target/release/blobcached ./blobcached.aarch64
```

### Push binary + config

The DS pod waits on a presence loop; copy the binary and config in:

```sh
kubectl -n blobcache cp blobcached.aarch64 <pod>:/opt/blobcache/blobcached
kubectl -n blobcache cp blobcached.toml    <pod>:/opt/blobcache/blobcached.toml
```

## Storage account public-access toggle

`myaccount` uses `defaultAction: Deny` with the AKS subnet allowed.
The `deploy/storage-access.sh` helper toggles only `publicNetworkAccess`:

```sh
deploy/storage-access.sh on      # enable (still vnet-restricted)
deploy/storage-access.sh status
deploy/storage-access.sh off     # MUST run after every test session
```

## Verifying a deployment

```sh
# cluster membership and config-hash agreement
kubectl -n blobcache exec <pod> -- curl -sS http://127.0.0.1:7773/peers

# read a file (forces miss → blob → cache → peer-on-second-node)
kubectl -n blobcache exec <pod> -- \
  dd if=/mnt/nvme/blobcache-mnt/<mount>/<path> of=/dev/null bs=1M count=64

# expect on a 2nd node: blob_fetches=0, peer_fetches_ok>0
kubectl -n blobcache exec <other-pod> -- \
  curl -sS http://127.0.0.1:7773/metrics | grep '^blobcache_'
```

## Key Prometheus metrics

| Metric | Meaning |
|---|---|
| `blobcache_cache_hits_total` / `..._misses_total` | local cache lookup outcomes |
| `blobcache_cache_inserts_total` / `..._evictions_total` | LRU activity |
| `blobcache_cache_bytes` | bytes currently on disk |
| `blobcache_blob_fetches_total` / `..._bytes_total` | Azure Blob origin fetches |
| `blobcache_peer_fetches_ok_total` / `..._miss_total` / `..._err_total` | outbound peer fetch outcomes |
| `blobcache_peer_chunk_requests_total` / `..._bytes_served_total` | inbound peer requests served |
| `blobcache_cluster_members_alive` / `..._dead` | gossip view |
| `blobcache_cluster_gossip_rounds_total` / `..._joins_total` / `..._failures_total` | membership churn |
| `blobcache_cluster_config_mismatches_total` | rejected merges due to differing cluster_hash |
| `blobcache_fuse_reads_total` / `..._read_bytes_total` | FUSE-layer reads |

## Project layout

```
src/
  main.rs           # wires everything; mounts FUSE per [[mounts]]
  config.rs         # TOML schema + cluster_hash()
  error.rs
  auth/             # MSI (workload + IMDS), SharedKey, SAS, Anonymous
  azure.rs          # BlobClient: HEAD, ranged GET, list (with retry)
  cache.rs          # DiskCache: sha256-named flat dir, BTreeMap LRU
  cluster.rs        # Membership + GossipServer + push-pull loop
  transport.rs      # PeerService + PeerClient (HTTP/1.1)
  fetcher.rs        # cache → 3 random peers → blob; parallel chunk fan-out
  fuse_fs.rs        # BlobFs: lookup/getattr/readdir/read; on-demand listing
  nic.rs            # multi-NIC enumeration (IB heuristic)
  stats.rs          # Prometheus + JSON HTTP server
deploy/
  blobcached.yaml   # NVMe RAID DS + blobcached DS
  storage-access.sh # ARM toggle for myaccount public access
examples/
  blobcached.toml   # sample config
```

## Known limitations

- **Read-only**. Writes are not implemented; the FUSE handler returns
  `EROFS`-style errors on write-open paths.
- **TCP-only transport** v1. The pod spec already requests `rdma/ib: 1`
  and `/dev/infiniband/uverbs*` is exposed inside the container; an RDMA
  backend can be slotted behind the existing `PeerClient` shape without
  touching FUSE / fetcher / cache code.
- **No Multus / IPoIB** on this cluster (NicClusterPolicy reports
  `state-ipoib-cni: ignore`); a future RDMA backend would use the raw
  verbs / RDMA-CM devices, not an IB IP.
- **Failure detection is coarse** (30 s heartbeat timeout to Suspect; no
  separate Suspect→Dead transition). Adequate for a ~20-node cluster.
