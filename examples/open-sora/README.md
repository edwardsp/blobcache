# Open-Sora v2 × blobcache — benchmark artifacts

Runnable scripts behind the case study in
[`docs/examples/open-sora-pexels.md`](../../docs/examples/open-sora-pexels.md):
real Open-Sora v2 (11B) training and a DataLoader read-benchmark on 64 NDv5 H200
nodes (512 GPUs), streaming the Pexels video dataset and the v2 checkpoints from
Azure Blob through blobcache. The experiment varies exactly one thing — the
*data* caching strategy (cold / sharded / replicated) — while always broadcasting
the weights.

These are the actual scripts that produced the numbers, with site-specific
identifiers replaced by `<placeholders>`. They assume blobcache is already
installed and running on the cluster (see [`docs/slurm.md`](../../docs/slurm.md)).

## Layout

| File | Role |
|---|---|
| `blobcached.toml` | Daemon config used for the run — `pexels` + `osv2` mounts under `/blobcache/` |
| `run_all.sh` | Unattended driver: 3 training + 3 read-bench runs (clear → hydrate → run) |
| `os-train.sbatch` | One training run; per-node `/metrics` sampler + phase markers, then `train.py` under enroot |
| `harness.sbatch` | One read-bench run; drives `harness.py` (DataLoader only, no model) |
| `harness.py` | The DataLoader stress harness (decode + collate, per-rank throughput) |
| `hydrate.sbatch` | dd-based whole-mount warm (every byte on every node), the brute-force alternative to the admin-API hydrate |
| `hydrate-osv2.json` | Admin-API payload: broadcast the weights to every node |
| `hydrate-pexels.json` | Admin-API payload: broadcast the dataset (warm-replicated) |
| `hydrate-pexels-shard.json` | Admin-API payload: round-robin shard the dataset (warm-sharded) |
| `snap_metrics.sh` | Snapshot `:7773/metrics` from every node into one CSV (brackets a read-bench) |
| `blobcache_stage1.py` | Open-Sora training config — reads weights/data from `/blobcache/...` |
| `plot_reads.py` `plot_summary.py` `plot_throughput.py` `plot_readbench.py` | Plotters for the metrics CSVs / logs |
| `results/64node/` | The summarized metric JSONs the plots in the case study were rendered from |

## Prerequisites

- blobcache running cluster-wide with the two mounts from `blobcached.toml`
  populated (`/blobcache/pexels`, `/blobcache/osv2`). Fill the `<placeholders>`
  with your storage account, container, and blob prefixes.
- A staging directory on shared NFS (the scripts assume `/shared/blobcache-deploy`)
  holding: this directory's scripts, the `Open-Sora` repo, the enroot `.sqsh`
  container images referenced by `os-train.sbatch` / `harness.sbatch`, and the
  dataset CSVs (`pexels.csv`, `pexels_meta.csv`).
- `blobcache_stage1.py` copied into the Open-Sora checkout at
  `configs/diffusion/train/blobcache_stage1.py`.

## Run

The full step-by-step playbook (clear-cache → hydrate → submit → collect → plot)
lives in the [case study](../../docs/examples/open-sora-pexels.md#6-playbook--run-it-yourself).
For the unattended path, set the two required env vars and launch the driver from
the login node:

```bash
ADMIN=<a-compute-node> NODELIST='<your-nodelist>' ./run_all.sh
```

Each `*.sbatch` also runs standalone; the single scale knob is `--nodes=N`.
