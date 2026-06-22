#!/bin/bash
# Kubernetes launcher for the Open-Sora v2 x blobcache benchmark, derived from the
# srun/torchrun block in examples/open-sora/os-train.sbatch. Per-node rank comes
# from the StatefulSet pod ordinal; the headless service gives a stable master.
set -uo pipefail

: "${NNODES:?set NNODES (number of training pods)}"
: "${MASTER_ADDR:?set MASTER_ADDR (e.g. os-train-0.os-train)}"
MASTER_PORT="${MASTER_PORT:-29502}"
CONFIG="${CONFIG:-configs/diffusion/train/blobcache_stage1.py}"
PEXELS_META_CSV="${PEXELS_META_CSV:-/blobcache/pexels/pexels_meta.csv}"

# Pod ordinal = node rank (StatefulSet hostname is <name>-<ordinal>).
NODE_RANK="${NODE_RANK:-${HOSTNAME##*-}}"
case "$NODE_RANK" in (*[!0-9]*|"") echo "FATAL: NODE_RANK='$NODE_RANK' not numeric (set NODE_RANK explicitly)"; exit 1;; esac

# NDv5 IB topology (mounted by the manifest at /etc/topology) so cross-node NCCL
# all-reduce builds the right GPU<->NIC routing; harmless if absent.
[ -f /etc/topology/ndv5-topo.xml ] && export NCCL_TOPO_FILE=/etc/topology/ndv5-topo.xml

# Author's PYTHONPATH shim, baked only if docker/os-train/pyshim/ was non-empty.
[ -n "$(ls -A /opt/pyshim 2>/dev/null)" ] && export PYTHONPATH="/opt/pyshim:${PYTHONPATH:-}"

export WANDB_MODE=disabled WANDB_DISABLED=true HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1

echo "[os-train] rank=${NODE_RANK}/${NNODES} master=${MASTER_ADDR}:${MASTER_PORT} host=$(hostname)"
echo "[os-train] config=${CONFIG} dataset=${PEXELS_META_CSV} topo=${NCCL_TOPO_FILE:-none} pyshim=${PYTHONPATH:-none}"

# Single-process grammar warmup before torchrun fans out to 8 ranks: mmengine
# imports yapf, which races a pickled lib2to3 grammar cache write across ranks
# ("EOFError: Ran out of input"). Regenerate it single-threaded first.
find /opt /usr -path "*_ylib2to3*" -name "*.pickle" -delete 2>/dev/null || true
python -c "import opensora.datasets.aspect" >/dev/null 2>&1 || true

exec torchrun \
  --nnodes="${NNODES}" --nproc_per_node=8 \
  --node_rank="${NODE_RANK}" \
  --master_addr="${MASTER_ADDR}" --master_port="${MASTER_PORT}" \
  scripts/diffusion/train.py "${CONFIG}" --dataset.data-path "${PEXELS_META_CSV}"
