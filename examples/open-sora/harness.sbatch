#!/bin/bash
#SBATCH --job-name=os-harness
#SBATCH --partition=gpu
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=8
#SBATCH --gpus-per-node=8
#SBATCH --exclusive
#SBATCH --output=/shared/blobcache-deploy/os-harness-%j.out
#
# Read-benchmark: drives the real Open-Sora DataLoader (decode + collate, no
# model) against /blobcache/pexels to isolate the I/O path from GPU compute.
# Single scale knob: #SBATCH --nodes=N (or `sbatch --nodes=N`).

set -uo pipefail

SQSH_BLOB=https://<storage-account>.blob.core.windows.net/<container>/<path/to>/os-harness.sqsh
SQSH_LOCAL=/mnt/nvme/os-harness.sqsh
CSV=/shared/blobcache-deploy/pexels.csv

STEPS=${STEPS:-200}
WORKERS=${WORKERS:-8}
BATCH=${BATCH:-4}
NUM_FRAMES=${NUM_FRAMES:-33}
HEIGHT=${HEIGHT:-256}
WIDTH=${WIDTH:-256}

export MASTER_ADDR=$(scontrol show hostnames "$SLURM_JOB_NODELIST" | head -n1)
export MASTER_PORT=29501

echo "[$(date +%H:%M:%S)] staging container to each node"
srun --ntasks-per-node=1 --label bash -c '
  set -e
  export AZURE_CLIENT_ID=<UAI client id with Storage Blob Data Reader>
  if [ ! -f '"$SQSH_LOCAL"' ]; then azcp copy '"$SQSH_BLOB"' '"$SQSH_LOCAL"'; fi
  ls -lh '"$SQSH_LOCAL"'
'

echo "[$(date +%H:%M:%S)] launch harness: nodes=$SLURM_NNODES ranks=$SLURM_NTASKS steps=$STEPS workers=$WORKERS batch=$BATCH"
srun --ntasks-per-node=8 --label \
  --container-image="$SQSH_LOCAL" \
  --container-mounts=/blobcache:/blobcache,/shared:/shared \
  --container-workdir=/shared/blobcache-deploy \
  bash -c '
    export RANK=$SLURM_PROCID WORLD_SIZE=$SLURM_NTASKS LOCAL_RANK=$SLURM_LOCALID
    python /shared/blobcache-deploy/harness.py \
      --csv '"$CSV"' --steps '"$STEPS"' --workers '"$WORKERS"' --batch '"$BATCH"' \
      --num-frames '"$NUM_FRAMES"' --height '"$HEIGHT"' --width '"$WIDTH"'
  '

echo "[$(date +%H:%M:%S)] HARNESS_DONE"
