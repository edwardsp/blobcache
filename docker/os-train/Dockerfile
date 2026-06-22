# Open-Sora v2 training image for the blobcache x Open-Sora benchmark (AKS / Kubernetes).
#
# The Slurm benchmark (examples/open-sora/os-train.sbatch) runs an enroot squashfs
# (`os-train.sqsh`) and mounts the Open-Sora repo + config from shared NFS. On
# Kubernetes there is no shared NFS, so this image is self-contained: it bakes the
# Open-Sora v2 code, its dependency stack (ColossalAI + xformers + flash-attn), and
# the benchmark's train config. The 11B checkpoints and the Pexels dataset are NOT
# baked — they are streamed at runtime through the blobcache FUSE mount at
# /blobcache/{osv2,pexels} (see examples/open-sora/blobcached.toml).
#
# Build (default, reliable, ~README-faithful FA2):
#   docker build -f docker/os-train/Dockerfile -t ghcr.io/edwardsp/blobcache/os-train:dev .
# Perf-faithful FA3 (Hopper; longer, source build):
#   docker build --build-arg FLASH_ATTN=3 ... .
# Pin Open-Sora to a specific commit for reproducibility:
#   docker build --build-arg OPENSORA_REF=<sha> ... .

ARG CUDA_IMAGE=nvidia/cuda:12.4.1-cudnn-devel-ubuntu22.04
FROM ${CUDA_IMAGE}

ENV DEBIAN_FRONTEND=noninteractive
# Open-Sora v2 targets python 3.10 + torch 2.4.0 (see hpcaitech/Open-Sora README).
RUN apt-get update && apt-get install -y --no-install-recommends \
      software-properties-common ca-certificates curl git ninja-build \
      ffmpeg libgl1 libglib2.0-0 \
    && add-apt-repository -y ppa:deadsnakes/ppa \
    && apt-get update && apt-get install -y --no-install-recommends \
      python3.10 python3.10-dev python3.10-venv python3.10-distutils \
    && rm -rf /var/lib/apt/lists/* \
    && curl -fsSL https://bootstrap.pypa.io/get-pip.py | python3.10 - \
    && update-alternatives --install /usr/bin/python python /usr/bin/python3.10 1 \
    && update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.10 1

# torch/torchvision pinned to the versions Open-Sora v2 requires (CUDA 12.4 wheels;
# H200 is sm_90). Installed before `pip install -v .` so the requirements.txt
# torch==2.4.0 pin is already satisfied and torch is not rebuilt.
RUN pip install --no-cache-dir torch==2.4.0 torchvision==0.19.0 \
      --index-url https://download.pytorch.org/whl/cu124

# Open-Sora v2 source + python package (pulls ColossalAI, mmengine, liger-kernel,
# av, accelerate, ftfy, omegaconf, ...). Pin with --build-arg OPENSORA_REF=<sha>.
ARG OPENSORA_REF=main
RUN git clone https://github.com/hpcaitech/Open-Sora /opt/Open-Sora \
    && cd /opt/Open-Sora && git checkout "${OPENSORA_REF}" \
    && pip install --no-cache-dir -v . \
    && pip install --no-cache-dir xformers==0.0.27.post2

# flash-attention. FA2 (default) is a reliable prebuilt wheel and is sufficient to
# reproduce the blobcache data-strategy results (the I/O path is attention-impl
# independent). FA3 (Hopper) matches the H200 reference step-time more closely but
# is a long source build — opt in with --build-arg FLASH_ATTN=3.
ARG FLASH_ATTN=2
ARG FLASH_ATTN_REF=4f0640d5
RUN set -eux; \
    if [ "${FLASH_ATTN}" = "3" ]; then \
      git clone https://github.com/Dao-AILab/flash-attention /opt/flash-attention; \
      cd /opt/flash-attention && git checkout "${FLASH_ATTN_REF}"; \
      cd hopper && MAX_JOBS=$(nproc) python setup.py install; \
    else \
      pip install --no-cache-dir flash-attn --no-build-isolation; \
    fi

# Bake the benchmark train config (extends Open-Sora's configs/.../stage1.py).
COPY examples/open-sora/blobcache_stage1.py /opt/Open-Sora/configs/diffusion/train/blobcache_stage1.py

# Optional: the benchmark author's PYTHONPATH shim (referenced by os-train.sbatch
# as /shared/blobcache-deploy/pyshim). Drop the real module under
# docker/os-train/pyshim/ to bake it; the entrypoint prepends it only if present.
COPY docker/os-train/pyshim/ /opt/pyshim/

COPY docker/os-train/entrypoint.sh /usr/local/bin/os-train-entrypoint
RUN chmod +x /usr/local/bin/os-train-entrypoint

ENV HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1 WANDB_MODE=disabled WANDB_DISABLED=true \
    PYTHONUNBUFFERED=1
WORKDIR /opt/Open-Sora
ENTRYPOINT ["/usr/local/bin/os-train-entrypoint"]
