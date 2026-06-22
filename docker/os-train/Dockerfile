# Open-Sora v2 training image for the blobcache x Open-Sora benchmark (AKS / Kubernetes).
#
# The Slurm benchmark (examples/open-sora/os-train.sbatch) runs an enroot squashfs
# (`os-train.sqsh`) and mounts the Open-Sora repo + config from shared NFS. On
# Kubernetes there is no shared NFS, so this image is self-contained: it bakes the
# Open-Sora v2 code, its dependency stack (ColossalAI + xformers + flash-attn), the
# benchmark train config, and the `tensornvme` import shim. The 11B checkpoints and
# the Pexels dataset are NOT baked — they are streamed at runtime through the
# blobcache FUSE mount at /blobcache/{osv2,pexels} (see
# examples/open-sora/blobcached.toml).
#
# This recipe mirrors the proven Slurm build (os-train-build.sbatch): the
# pytorch/pytorch DEVEL base already ships python 3.10 + torch 2.4.0 (cu121) + nvcc,
# so torch is never rebuilt and flash-attn compiles against the in-image toolkit.
#
# Build:
#   docker build -f docker/os-train/Dockerfile -t ghcr.io/edwardsp/blobcache/os-train:dev .
# Pin Open-Sora to a specific commit (default is the reference-run commit):
#   docker build --build-arg OPENSORA_REF=<sha> ... .

# DEVEL base: python 3.10, torch 2.4.0 + torchvision (cu121), cuDNN 9, nvcc. This is
# the exact base the Slurm reference image (os-train.sqsh) was built from.
ARG BASE_IMAGE=pytorch/pytorch:2.4.0-cuda12.1-cudnn9-devel
FROM ${BASE_IMAGE}

ENV DEBIAN_FRONTEND=noninteractive

# System libs: git/ninja for the source builds, and the GL/X stubs OpenCV +
# decord/av pull in. opencv-python is swapped for opencv-python-headless so the
# image needs no X server (the GUI build segfaults on import in a headless pod).
# rdma-core (libibverbs/librdmacm/ibverbs-providers) is REQUIRED so NCCL can drive
# the InfiniBand HCAs for multi-node training — without it NCCL logs "Failed to
# open libibverbs.so" and silently falls back to NET/Socket (TCP over eth0), which
# collapses cross-node all-reduce bandwidth. The pytorch base image ships none of it.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       ca-certificates curl git ninja-build \
       libglib2.0-0 libgl1 libxcb1 libsm6 libxext6 libxrender1 \
       libibverbs1 librdmacm1 ibverbs-providers ibverbs-utils \
    && rm -rf /var/lib/apt/lists/* \
    && pip uninstall -y opencv-python opencv-contrib-python 2>/dev/null || true \
    && pip install --no-cache-dir packaging ninja opencv-python-headless

# xformers pinned to the version Open-Sora v2 requires, from the cu121 wheel index
# so it matches the in-base torch 2.4.0 CUDA build (avoids a torch reinstall).
RUN pip install --no-cache-dir xformers==0.0.27.post2 \
      --index-url https://download.pytorch.org/whl/cu121

# Open-Sora v2 source + python package (pulls ColossalAI, mmengine, liger-kernel,
# av, accelerate, ftfy, omegaconf, ...). Default ref is the reference-run commit.
ARG OPENSORA_REF=7ad6a96a135feb81f755c84fb391818718f6beb2
RUN git clone --filter=blob:none https://github.com/hpcaitech/Open-Sora /opt/Open-Sora \
    && cd /opt/Open-Sora && git checkout "${OPENSORA_REF}" \
    && pip install --no-cache-dir -v .

# flash-attention 2: prefer the prebuilt wheel matching torch 2.4 / cu121 / cp310.
# The Slurm reference compiled from source (FORCE_BUILD) only because it ran on a
# 1.8 TB-RAM H200 node; a source compile with high MAX_JOBS OOM-kills a 16 GB CI
# runner. The blobcache I/O path is attention-impl independent, so the prebuilt
# wheel is equivalent here. MAX_JOBS=2 caps memory if pip ever falls back to source.
RUN MAX_JOBS=2 pip install --no-cache-dir flash-attn --no-build-isolation

# Smoke-import the whole stack at build time so a broken image fails the build, not
# the 64-node job (mirrors the reference build's TRAIN_IMPORT_OK check).
RUN python -c "import colossalai, flash_attn, xformers; from opensora.datasets.read_video import read_video; print('TRAIN_IMPORT_OK')"

# Bake the benchmark train config (extends Open-Sora's configs/.../stage1.py).
COPY examples/open-sora/blobcache_stage1.py /opt/Open-Sora/configs/diffusion/train/blobcache_stage1.py

# `tensornvme` import shim. opensora/utils/ckpt.py eagerly does
# `from tensornvme.async_file_io import AsyncFileWriter`, but tensornvme is not
# installed; with epochs=1 / ckpt_every=100000 the async writer is never used. The
# stub satisfies the import and is prepended to PYTHONPATH by the entrypoint.
COPY docker/os-train/pyshim/ /opt/pyshim/

COPY docker/os-train/entrypoint.sh /usr/local/bin/os-train-entrypoint
RUN chmod +x /usr/local/bin/os-train-entrypoint

ENV HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1 WANDB_MODE=disabled WANDB_DISABLED=true \
    PYTHONUNBUFFERED=1
WORKDIR /opt/Open-Sora
ENTRYPOINT ["/usr/local/bin/os-train-entrypoint"]
