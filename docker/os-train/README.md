# `os-train` — Open-Sora v2 training image (Kubernetes / AKS)

Self-contained training image for the [Open-Sora v2 × blobcache benchmark](../../docs/examples/open-sora-pexels.md).
The Slurm benchmark ([`examples/open-sora/os-train.sbatch`](../../examples/open-sora))
runs an enroot squashfs (`os-train.sqsh`) and mounts the Open-Sora repo + config from
shared NFS. Kubernetes has no shared NFS, so this image bakes everything *except* the
weights and dataset, which are streamed at runtime through the blobcache FUSE mount.

## What's in it

- CUDA 12.4 + Python 3.10 + **torch 2.4.0 / torchvision 0.19.0**
- **Open-Sora v2** (`hpcaitech/Open-Sora`) source + `pip install -v .` → ColossalAI,
  mmengine, liger-kernel, av, accelerate, ftfy, omegaconf
- **xformers 0.0.27.post2** + **flash-attention** (FA2 default, FA3/Hopper optional)
- the benchmark train config `blobcache_stage1.py` (baked into
  `configs/diffusion/train/`)
- `entrypoint.sh` — the Kubernetes torchrun launcher (per-node rank from the
  StatefulSet pod ordinal)

**Not** baked (streamed via `/blobcache`): the 11B checkpoints (`/blobcache/osv2/...`)
and the Pexels dataset (`/blobcache/pexels/...`).

## Build

```bash
# default: FA2 (reliable, fast, sufficient — the blobcache I/O path is attention-impl independent)
docker build -f docker/os-train/Dockerfile -t ghcr.io/edwardsp/blobcache/os-train:dev .

# perf-faithful FA3 (Hopper) to match the H200 reference step-time (longer source build)
docker build --build-arg FLASH_ATTN=3 -f docker/os-train/Dockerfile -t ...:fa3 .

# pin Open-Sora for reproducibility
docker build --build-arg OPENSORA_REF=<sha> ...
```

CI: [`.github/workflows/os-train-image.yml`](../../.github/workflows/os-train-image.yml)
builds on changes under `docker/os-train/**` and pushes to
`ghcr.io/edwardsp/blobcache/os-train`. `workflow_dispatch` accepts `flash_attn` (2|3)
and `opensora_ref` inputs.

## `pyshim` (action required)

`os-train.sbatch` runs with `PYTHONPATH=/shared/blobcache-deploy/pyshim` — the
benchmark author's shim, which is **not** in this repo. Drop its module(s) under
[`pyshim/`](pyshim/) and they are baked to `/opt/pyshim` and prepended to
`PYTHONPATH` at runtime (the entrypoint skips it if the dir is empty). If the run
depends on the shim (e.g. a dataloader/IO patch), the training will not match the
reference until the real `pyshim` is added here.

## Runtime contract (set by the Kubernetes manifest)

| Env | Meaning |
|---|---|
| `NNODES` | number of training pods (e.g. 64) |
| `MASTER_ADDR` | rank-0 pod DNS (e.g. `os-train-0.os-train`) |
| `NODE_RANK` | defaults to the pod ordinal (`${HOSTNAME##*-}`) |
| `CONFIG` | train config (default `configs/diffusion/train/blobcache_stage1.py`) |
| `PEXELS_META_CSV` | dataset metadata CSV (default `/blobcache/pexels/pexels_meta.csv`) |

Mounts the manifest must provide: `/blobcache` (blobcached FUSE, `HostToContainer`),
`/etc/topology/ndv5-topo.xml` (NCCL topology), `/dev/shm`, 8 GPUs + `rdma/ib: 8`.
Each pod runs `torchrun --nnodes=$NNODES --nproc_per_node=8 --node_rank=$NODE_RANK
--master_addr=$MASTER_ADDR scripts/diffusion/train.py $CONFIG --dataset.data-path $PEXELS_META_CSV`.
