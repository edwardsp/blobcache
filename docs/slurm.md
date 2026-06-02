# Deploying blobcache on a Slurm cluster (azcluster)

This guide walks through deploying `blobcached` on an Azure Slurm cluster
provisioned with [azcluster](https://github.com/edwardsp/azcluster), so
that one or more Azure Blob paths appear as read-only FUSE mounts under
`/blobcache/<name>/` on every compute node, backed by the local NVMe
RAID and shared between nodes over the cluster interconnect.

It was developed and validated on:

| | |
|---|---|
| Cluster | `paul-h200` (16 × `Standard_ND96isr_H200_v5`, mexicocentral) |
| OS | Ubuntu 24.04 |
| Interconnect | NDR InfiniBand, ConnectX-7, `mlx5_ib0` |
| Local storage | 8 × NVMe assembled into RAID-0 at `/mnt/nvme` |
| Storage | Per-cluster account behind a Private Endpoint, UAI auth |
| Monitoring | Per-node Prometheus → Azure Monitor Workspace → Azure Managed Grafana |
| Model | DeepSeek-R1-0528, 642 GiB across 350 blobs |

Nothing in the design is hardware-specific past the build step — the
same pattern works on any azcluster pool.

The artifacts referenced below live in
[`deploy/slurm/`](../deploy/slurm/),
[`dist/systemd/`](../dist/systemd/), and
[`examples/blobcached-azcluster.toml`](../examples/blobcached-azcluster.toml).

---

## 1. Architecture on Slurm vs. Kubernetes

The Helm chart in [`deploy/helm/`](../deploy/helm/) deploys `blobcached`
as a privileged DaemonSet that mounts `hostPath` volumes for the NVMe
cache and FUSE mountpoint and runs with `hostNetwork: true` so gossip
and the peer transport reach other nodes directly.

On a Slurm cluster the equivalent of "a pod on every node" is "a
systemd service on every node". There is no DaemonSet, no admission
controller, no per-job sidecar. Instead:

```
              ┌──────────────────────── compute node ─────────────────────┐
   sbatch ──► │  job process                                              │
              │     │ read()                                              │
              │     ▼                                                     │
              │  /blobcache/<mount>/...   (FUSE, allow_other, RO)         │
              │     │                                                     │
              │     ▼                                                     │
              │  blobcached.service (systemd)                             │
              │     │                                                     │
              │     ▼                                                     │
              │  /mnt/nvme/blobcache-cache/    (NVMe RAID-0, ~1.5 TiB)    │
              │                                                           │
              │  :7771 gossip ─┐  :7772 peer ─┐  :7773 stats              │
              └────────────────┼──────────────┼───────────────────────────┘
                               ▼              ▼
                    other compute nodes (push-pull)
```

Daemon lifecycle is decoupled from jobs: it starts at boot, stays up
across job boundaries, and serves whatever FUSE mounts the operator
configured. Jobs see plain POSIX files.

## 2. One-time build

On any node with `libfuse3-dev` and a recent Rust toolchain:

```sh
sudo apt-get install -y libfuse3-dev pkg-config
cargo build --release                       # tcp transport
# or, for RDMA between nodes:
sudo apt-get install -y libucx-dev libibverbs-dev librdmacm-dev
cargo build --release --features ucx
```

The output is a single binary, `target/release/blobcached`. Runtime
deps on the target nodes are `libfuse3-3`, plus `libucx0 libibverbs1
librdmacm1t64` if you built with `--features ucx`. All of those are
present in the azcluster cloud-init image.

The build node and the compute nodes must agree on CPU architecture
(x86_64 here). If you're cross-targeting Grace (aarch64), build on a
Grace node — see the `kubectl run` recipe in the main
[README](../README.md#cross-compile-note), but use `srun` instead of
`kubectl`.

## 3. Staging the artifacts

Upload the binary, config, unit, and installer to the per-cluster
blob staging path. On azcluster, `${AZCLUSTER_USER_BLOB_URL}` resolves
to the per-user prefix on the per-cluster account; the daemon reads
from the model prefix via the cluster's user-assigned MSI.

```sh
source /etc/profile.d/azcluster-storage.sh
STAGE="${AZCLUSTER_USER_BLOB_URL}/blobcache-deploy"

azcp copy target/release/blobcached                $STAGE/blobcached
azcp copy examples/blobcached-azcluster.toml       $STAGE/blobcached.toml
azcp copy dist/systemd/blobcached.service          $STAGE/blobcached.service
azcp copy deploy/slurm/install-blobcached.sh       $STAGE/install-blobcached.sh
```

Edit `blobcached.toml` first to point `[[mounts]]` at your blob path(s)
and `[cluster].seeds` at the first compute node (any reachable node is
fine).

## 4. Fanout install

Submit the Slurm wrapper with `--nodes=` equal to your pool size:

```sh
export AZURE_CLIENT_ID=<UAI client id with Storage Blob Data Reader>
sbatch --nodes=16 deploy/slurm/install-blobcached.sbatch
```

Each task downloads the staged binary + config, installs to
`/opt/blobcache/`, writes `/etc/default/blobcached` with
`AZURE_CLIENT_ID`, drops the systemd unit, ensures `user_allow_other`
in `/etc/fuse.conf`, and `systemctl enable --now blobcached`.

Verify cluster-wide:

```sh
srun -N16 --ntasks-per-node=1 bash -c \
  'curl -sf http://127.0.0.1:7773/peers | jq ".alive | length"'
# expect: 16 on every line

srun -N16 --ntasks-per-node=1 bash -c \
  'curl -sf http://127.0.0.1:7773/stats | jq -r .cluster_hash' | sort -u
# expect: exactly one hash
```

If `cluster_hash` disagrees between nodes, every node's `[cache].chunk_size`
and `[[mounts]]` set must be identical — that's what `cluster_hash`
covers — and `[transport].kind` must match across the cluster.

## 5. Using the mount from a job

```sh
#!/bin/bash -l
#SBATCH --nodes=8
#SBATCH --gpus-per-node=8

# Files appear like any other POSIX path; the daemon resolves cache miss
# → peer (3 random candidates) → Azure Blob in the background.
torchrun --nproc-per-node=8 train.py \
    --model /blobcache/deepseek/ \
    --config /blobcache/deepseek/config.json
```

The first read on any chunk takes a peer or blob round-trip; subsequent
reads on the same node hit the NVMe cache (and then the OS page cache).
Cluster-wide, **only one** node fetches each chunk from Azure — the rest
get it via the peer transport.

### Optional: pre-warm with `/hydrate`

To eagerly populate every node's cache with a path tree (e.g. before a
synchronised job starts), use the cluster-wide hydrate coordinator:

```sh
curl -sf -X POST http://<any-node>:7773/hydrate \
    -H 'content-type: application/json' \
    -d '{"mount":"deepseek","prefix":""}' | jq
```

This shards the blob list across all alive peers (Phase A) and runs
on-demand backfill for any misses (Phase B). The per-node `cache_bytes`
metric will rise in lockstep.

## 6. Observability

azcluster pre-deploys per-node Prometheus that remote-writes via AAD-MI
to the cluster's Azure Monitor Workspace, plus an Azure Managed Grafana
instance. To surface blobcache metrics there:

1. Append the contents of
   [`deploy/slurm/prometheus-blobcache.yml`](../deploy/slurm/prometheus-blobcache.yml)
   to each node's `/opt/prometheus/prometheus.yml` (under
   `scrape_configs`).
2. Add a `node: <hostname>` line under `global.external_labels` in the
   same file. The shipped Grafana dashboard uses `$node` as a template
   variable. azcluster sets `nodename` but not `node`; the dashboard's
   PromQL relies on the short form.
3. `sudo kill -HUP $(pgrep -x prometheus)` to reload (Prometheus on
   this distro doesn't honour `systemctl reload`).
4. In Grafana, **Dashboards → Import** and upload
   [`deploy/slurm/grafana-dashboard.json`](../deploy/slurm/grafana-dashboard.json).

A 13-panel `blobcache` dashboard will appear with cluster aggregate
read throughput, per-node hit / miss / peer / blob breakdown, gossip
membership, hydrate progress, and FUSE op rates.

## 7. Validation

A 3-tier benchmark script lives at
[`deploy/slurm/bench.sh`](../deploy/slurm/bench.sh). It picks a unique
shard per node so tier 1 is a true cold-blob fetch on each:

```sh
srun -N16 --ntasks-per-node=1 bash deploy/slurm/bench.sh
```

Expected order of magnitude on H200 + NDR IB + the v2.9 fetcher (the
TCP transport, single-stream `dd bs=4M`):

| Tier | What it measures | Median |
|---|---|---|
| 1 cold-blob | NVMe miss → peer miss → Azure Blob | ~290 MB/s |
| 2 cold-page | NVMe hit, OS page cache cold | ~530 MB/s |
| 3 warm-page | OS page cache hit | ~1.2 GB/s |

For higher single-stream numbers, build with `--features ucx` so the
peer transport runs over InfiniBand instead of TCP/IB — see
[BENCHMARKS.md](../BENCHMARKS.md) v2.3 onwards. Multi-stream aggregates
scale to ~11.75 GiB/s per fetcher node.

## 8. Teardown

```sh
srun -N16 --ntasks-per-node=1 bash -c '
  sudo systemctl disable --now blobcached
  sudo fusermount3 -u /blobcache/<mount> || true
  sudo rm -rf /mnt/nvme/blobcache-cache/*
'
```

## 9. Troubleshooting

| Symptom | Likely cause |
|---|---|
| `/blobcache/<m>` is empty but daemon is active | First-time mount; FUSE listing is lazy. Touch any path to trigger. |
| `cluster_members_alive` < N on some nodes | `[cluster].seeds` unreachable from that node, or `[transport].kind` mismatch. Check `/peers`. |
| `cluster_config_mismatches_total` increasing | `cache.chunk_size` or `[[mounts]]` differ between nodes. |
| Reads return EIO | Check the daemon's journal: `journalctl -u blobcached -e -n 200`. Common: blob 403 (UAI lacks Storage Blob Data Reader on the account). |
| `fusermount: option allow_other only allowed if 'user_allow_other' is set` | `/etc/fuse.conf` missing the directive; the installer adds it but a custom image may have removed it. |
| Dashboard has no data, AMW does | `node` label missing from `external_labels` (see step 6.2). |

## 10. Not in scope

- A SPANK plugin — the daemon is cluster-long-lived, not per-job.
- Pyxis/Enroot container packaging — the daemon runs on the host;
  containerised jobs read from the host bind-mount of `/blobcache/`.
- Cluster autoscaling of cache size — `[cache].max_bytes` is static
  per node; size it for the largest expected working set.
