# Slurm / azcluster deploy artifacts

Node-local systemd + Slurm-fanout install for `blobcached`. Designed for
[azcluster](https://github.com/edwardsp/azcluster) Slurm clusters (Ubuntu
24.04 compute, NVMe RAID at `/mnt/nvme`, per-cluster blob account with
Managed Identity), but nothing here is azcluster-specific past the
`AZCLUSTER_USER_BLOB_URL` shell variable in the sbatch wrapper.

The full deploy walkthrough lives in [`docs/slurm.md`](../../docs/slurm.md).

## Files

| File | What it is |
|---|---|
| `install-blobcached.sh` | Node-local installer. Downloads binary + config from a blob URL, drops the systemd unit, enables `user_allow_other`, starts the service, verifies metrics endpoint. Idempotent. |
| `install-blobcached.sbatch` | Slurm wrapper that runs `install-blobcached.sh` once per compute node (`--ntasks-per-node=1 --exclusive`). |
| `prometheus-blobcache.yml` | Scrape snippet to append to each node's `/opt/prometheus/prometheus.yml` (azcluster pre-deploys Prometheus + AMW remote_write). |
| `grafana-dashboard.json` | 13-panel dashboard, drag-and-drop import into Azure Managed Grafana. Uses a `node` template variable; ensure the scrape config in `prometheus-blobcache.yml` is wired so that label exists. |
| `bench.sh` | 3-tier `dd` benchmark (cold-blob / cold-page / warm-page) that picks a unique fresh shard per node to avoid cross-contaminating the NVMe cache. |

## Quick start

```sh
# 1. Build (once, on a node with libfuse3-dev + cargo)
cargo build --release    # tcp transport; add --features ucx for RDMA

# 2. Upload binary + config + scripts to the per-cluster blob staging URL
azcp copy target/release/blobcached         "${AZCLUSTER_USER_BLOB_URL}/blobcache-deploy/blobcached"
azcp copy /etc/blobcached.toml              "${AZCLUSTER_USER_BLOB_URL}/blobcache-deploy/blobcached.toml"
azcp copy dist/systemd/blobcached.service   "${AZCLUSTER_USER_BLOB_URL}/blobcache-deploy/blobcached.service"
azcp copy deploy/slurm/install-blobcached.sh "${AZCLUSTER_USER_BLOB_URL}/blobcache-deploy/install-blobcached.sh"

# 3. Fan out to every compute node
sbatch deploy/slurm/install-blobcached.sbatch
```

Each node ends up with:

- `/opt/blobcache/blobcached` — binary
- `/etc/blobcached.toml` — config
- `/etc/systemd/system/blobcached.service` — unit (enabled)
- `/etc/default/blobcached` — env file (`AZURE_CLIENT_ID=...`)
- `/mnt/nvme/blobcache-cache/` — chunk cache
- `/blobcache/<mount>/` — FUSE mount (read-only)
