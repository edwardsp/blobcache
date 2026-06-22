# `os-train` — Open-Sora v2 training image (Kubernetes / AKS)

Self-contained training image for the [Open-Sora v2 × blobcache benchmark](../../docs/examples/open-sora-pexels.md).
The Slurm benchmark ([`examples/open-sora/os-train.sbatch`](../../examples/open-sora))
runs an enroot squashfs (`os-train.sqsh`) and mounts the Open-Sora repo + config from
shared NFS. Kubernetes has no shared NFS, so this image bakes everything *except* the
weights and dataset, which are streamed at runtime through the blobcache FUSE mount.

It mirrors the proven Slurm build recipe (`os-train-build.sbatch`) step for step, so
the Kubernetes image matches the reference run's dependency stack.

## What's in it

- base **`pytorch/pytorch:2.4.0-cuda12.1-cudnn9-devel`** — python 3.10, **torch
  2.4.0 / torchvision (cu121)**, cuDNN 9, nvcc (so flash-attn compiles in-image and
  torch is never rebuilt)
- **Open-Sora v2** (`hpcaitech/Open-Sora`, default ref
  `7ad6a96a135feb81f755c84fb391818718f6beb2`) source + `pip install -v .` →
  ColossalAI, mmengine, liger-kernel, av, accelerate, ftfy, omegaconf
- **xformers 0.0.27.post2** (cu121 index) + **flash-attn 2** (compiled from source,
  `FLASH_ATTENTION_FORCE_BUILD=TRUE`, `MAX_JOBS=32`)
- `opencv-python-headless` (swapped in for `opencv-python` — the GUI build segfaults
  on import in a headless pod)
- the benchmark train config `blobcache_stage1.py` (baked into
  `configs/diffusion/train/`)
- the `tensornvme` import shim (`pyshim/`, see below)
- `entrypoint.sh` — the Kubernetes torchrun launcher (per-node rank from the
  StatefulSet pod ordinal)

A `python -c "import colossalai, flash_attn, xformers; from
opensora.datasets.read_video import read_video"` smoke-import runs at build time, so a
broken image fails the build rather than the 64-node job.

**Not** baked (streamed via `/blobcache`): the 11B checkpoints (`/blobcache/osv2/...`)
and the Pexels dataset (`/blobcache/pexels/...`).

## Build

```bash
docker build -f docker/os-train/Dockerfile -t ghcr.io/edwardsp/blobcache/os-train:dev .

# pin Open-Sora to a different commit
docker build --build-arg OPENSORA_REF=<sha> -f docker/os-train/Dockerfile -t ...:<tag> .
```

CI: [`.github/workflows/os-train-image.yml`](../../.github/workflows/os-train-image.yml)
builds on changes under `docker/os-train/**` and pushes to
`ghcr.io/edwardsp/blobcache/os-train`. `workflow_dispatch` accepts an `opensora_ref`
input.

## `pyshim`

`opensora/utils/ckpt.py` eagerly runs `from tensornvme.async_file_io import
AsyncFileWriter` at import time, but `tensornvme` (async NVMe checkpoint IO) is not
installed in the image. The benchmark runs with `epochs=1` and `ckpt_every=100000`,
so the async writer is never instantiated — [`pyshim/`](pyshim/) is a stub
`tensornvme` package that satisfies the import (and raises if a save is ever actually
attempted). It is baked to `/opt/pyshim` and prepended to `PYTHONPATH` by the
entrypoint. This reproduces the reference run, which used the same shim via
`PYTHONPATH=/shared/blobcache-deploy/pyshim`.

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
