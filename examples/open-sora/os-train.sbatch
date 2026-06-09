#!/bin/bash
#SBATCH --job-name=os-train
#SBATCH --partition=gpu
#SBATCH --nodes=64
#SBATCH --ntasks-per-node=8
#SBATCH --gpus-per-node=8
#SBATCH --exclusive
#SBATCH --output=/shared/blobcache-deploy/os-train-%j.out
#
# Open-Sora v2 training, streaming BOTH
#   - the Pexels mp4 dataset  via /blobcache/pexels   (data path)
#   - the 11B v2 checkpoints   via /blobcache/osv2     (weights: DiT + Hunyuan VAE + T5 + CLIP)
# through blobcache, with a per-node background read-metrics sampler so we can
# plot blob-vs-peer read rates over the full run window (incl. the model-load
# phase) and compare COLD vs WARM cache.
#
# Single scale knob: #SBATCH --nodes=N  (or `sbatch --nodes=N`).
#
# Required env:
#   RUN_TAG   cold | warm   (labels the metrics CSVs + markers)
# Optional:
#   CONFIG       training config (default blobcache_stage1.py)
#   SAMPLE_SECS  sampler interval (default 5)
#   SQSH_SHA     sha256 of os-train.sqsh; if set, NFS staging is verified + retried

set -uo pipefail

SQSH_BLOB=https://<storage-account>.blob.core.windows.net/<container>/<path/to>/os-train.sqsh
SQSH_LOCAL=/mnt/nvme/os-train.sqsh
# Stage the training container from NFS with an integrity check + retry rather
# than N concurrent `azcp` pulls from blob, which can silently corrupt the sqsh
# under load (same size, bad bytes -> pyxis lzo uncompress error -> task_init rc=-1).
SQSH_NFS=/shared/blobcache-deploy/os-train.sqsh
SQSH_SHA=${SQSH_SHA:-}
REPO=/shared/blobcache-deploy/Open-Sora
CONFIG=${CONFIG:-configs/diffusion/train/blobcache_stage1.py}
RUN_TAG=${RUN_TAG:-run}
SAMPLE_SECS=${SAMPLE_SECS:-5}

METRICS_DIR=/shared/blobcache-deploy/metrics
RUN_ID="${RUN_TAG}-${SLURM_JOB_ID}"
STOP_FILE="${METRICS_DIR}/${RUN_ID}.stop"
MARKERS="${METRICS_DIR}/${RUN_ID}.markers"
mkdir -p "$METRICS_DIR"
rm -f "$STOP_FILE"

export MASTER_ADDR=$(scontrol show hostnames "$SLURM_JOB_NODELIST" | head -n1)
export MASTER_PORT=29502

mark() { echo "$(date -u +%s%3N) $1" >> "$MARKERS"; }

echo "[$(date +%H:%M:%S)] RUN_ID=$RUN_ID nodes=$SLURM_NNODES gpus=$((SLURM_NNODES*8)) tag=$RUN_TAG"
echo "RUN_ID=$RUN_ID" > "$MARKERS"
echo "NODES=$SLURM_NNODES" >> "$MARKERS"
echo "TAG=$RUN_TAG" >> "$MARKERS"
mark JOB_START

# Per-node background read-metrics sampler (host daemon :7773). Runs on EVERY
# node, overlapping training, until STOP_FILE appears. One CSV per node: epoch_ms
# + monotonic blobcache counters, diffed in the plotter to get per-interval read
# rates and total filesystem demand:
#   bandwidth  = d(fuse_read_bytes)/dt
#   IOPS       = d(fuse_reads)/dt
#   source split: local = fuse_read_bytes - peer_fetch_bytes - blob_fetch_bytes,
#                 peer = peer_fetch_bytes, blob = blob_fetch_bytes.
echo "[$(date +%H:%M:%S)] starting per-node metrics samplers -> $METRICS_DIR/${RUN_ID}-<host>.csv"
srun --nodes="$SLURM_NNODES" --ntasks="$SLURM_NNODES" --ntasks-per-node=1 --overlap --label bash -c '
  H=$(hostname)
  OUT='"$METRICS_DIR"'/'"$RUN_ID"'-$H.csv
  echo "epoch_ms,host,blob_fetches,blob_bytes,peer_fetches,peer_bytes,cache_hits,cache_misses,cache_bytes,fuse_reads,fuse_read_bytes" > "$OUT"
  g(){ awk -v k="$1" "\$1==k{print \$2; found=1} END{if(!found)print 0}" /tmp/'"$RUN_ID"'.m 2>/dev/null; }
  while [ ! -f '"$STOP_FILE"' ]; do
    curl -s http://localhost:7773/metrics > /tmp/'"$RUN_ID"'.m 2>/dev/null
    now=$(date +%s%3N)
    echo "$now,$H,$(g blobcache_blob_fetches_total),$(g blobcache_blob_fetch_bytes_total),$(g blobcache_peer_fetches_ok_total),$(g blobcache_peer_fetch_bytes_total),$(g blobcache_cache_hits_total),$(g blobcache_cache_misses_total),$(g blobcache_cache_bytes),$(g blobcache_fuse_reads_total),$(g blobcache_fuse_read_bytes_total)" >> "$OUT"
    sleep '"$SAMPLE_SECS"'
  done
' &
SAMPLER_PID=$!

echo "[$(date +%H:%M:%S)] stage train container to each node (NFS copy + optional sha256 verify + retry)"
mark STAGE_START
srun --nodes="$SLURM_NNODES" --ntasks="$SLURM_NNODES" --ntasks-per-node=1 --overlap --label bash -c '
  set -e
  SRC='"$SQSH_NFS"'; DST='"$SQSH_LOCAL"'; WANT='"$SQSH_SHA"'
  ok() { [ -f "$DST" ] && { [ -z "$WANT" ] || [ "$(sha256sum "$DST" | cut -d" " -f1)" = "$WANT" ]; }; }
  if ok; then echo "$(hostname): sqsh already valid"; else
    for try in 1 2 3; do
      echo "$(hostname): staging sqsh from NFS (attempt $try)"
      cp -f "$SRC" "$DST"
      if ok; then echo "$(hostname): sqsh staged"; break; fi
      echo "$(hostname): sha mismatch on attempt $try"; rm -f "$DST"
    done
  fi
  ok || { echo "$(hostname): FATAL sqsh staging failed after retries"; exit 1; }
  ls -lh "$DST"
'
mark STAGE_DONE

echo "[$(date +%H:%M:%S)] launch training: nodes=$SLURM_NNODES gpus=$((SLURM_NNODES*8)) config=$CONFIG"
mark TRAIN_START
srun --nodes="$SLURM_NNODES" --ntasks="$SLURM_NNODES" --ntasks-per-node=1 --overlap --label \
  --container-image="$SQSH_LOCAL" \
  --container-mounts=/blobcache:/blobcache,/shared:/shared \
  --container-workdir="$REPO" \
  bash -c '
    export WANDB_MODE=disabled WANDB_DISABLED=true HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1
    export PYTHONPATH=/shared/blobcache-deploy/pyshim:${PYTHONPATH:-}
    # Single-process grammar warmup (runs ONCE per node, before torchrun fans out
    # to 8 ranks). mmengine imports yapf, which lazily regenerates a pickled
    # lib2to3 grammar cache on first import; 8 ranks racing that write truncate the
    # pickle ("EOFError: Ran out of input"). Regenerate it single-threaded first.
    find /opt/conda -path "*_ylib2to3*" -name "*.pickle" -delete 2>/dev/null || true
    python -c "import opensora.datasets.aspect" >/dev/null 2>&1 || true
    torchrun \
      --nnodes=$SLURM_NNODES --nproc_per_node=8 \
      --node_rank=$SLURM_NODEID \
      --master_addr=$MASTER_ADDR --master_port=$MASTER_PORT \
      scripts/diffusion/train.py '"$CONFIG"'
  '
mark TRAIN_DONE

echo "[$(date +%H:%M:%S)] stopping samplers"
touch "$STOP_FILE"
wait "$SAMPLER_PID" 2>/dev/null
mark JOB_END
echo "[$(date +%H:%M:%S)] TRAIN_DONE RUN_ID=$RUN_ID markers=$MARKERS"
