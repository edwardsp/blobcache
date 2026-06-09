#!/bin/bash
#SBATCH --job-name=hydrate
#SBATCH --partition=gpu
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=1
#SBATCH --exclusive
#SBATCH --time=01:30:00
#SBATCH --output=/shared/blobcache-deploy/hydrate-%j.out
#
# Brute-force warm-cache prep: read every file in /blobcache/pexels +
# /blobcache/osv2 -> /dev/null at P=64 on each node, so each node's local cache
# is fully warm. This is the dd-based alternative to the coordinated admin-API
# hydrate (POST /hydrate); use the admin API when you want sharded/broadcast
# placement, use this when you just want every byte on every node.
# Single scale knob: `sbatch --nodes=N` (srun fans out to $SLURM_NNODES).

srun --nodes="$SLURM_NNODES" --ntasks="$SLURM_NNODES" --ntasks-per-node=1 bash -c '
  H=$(hostname)
  echo "[$H] === hydration start $(date -u +%H:%M:%S) ==="
  GTOT=0
  for M in /blobcache/pexels /blobcache/osv2; do
    BYTES=$(find "$M" -type f -printf "%s\n" 2>/dev/null | awk "{s+=\$1} END{print s+0}")
    NF=$(find "$M" -type f 2>/dev/null | wc -l)
    T0=$(date +%s.%N)
    find "$M" -type f -print0 2>/dev/null | xargs -0 -P 64 -I{} dd if={} of=/dev/null bs=4M 2>/dev/null
    T1=$(date +%s.%N)
    awk -v h="$H" -v m="$M" -v b="$BYTES" -v nf="$NF" -v t0="$T0" -v t1="$T1" \
      "BEGIN{dt=t1-t0; gib=b/1073741824; printf \"[%s] %-18s %8.2f GiB  %6d files  %8.1f s  %6.2f GiB/s (%5.2f Gbps)\n\", h, m, gib, nf, dt, gib/dt, gib*8.589934592/dt}"
    GTOT=$(awk -v g="$GTOT" -v b="$BYTES" "BEGIN{print g+b}")
  done
  awk -v h="$H" -v g="$GTOT" "BEGIN{printf \"[%s] TOTAL hydrated %.2f GiB\n\", h, g/1073741824}"
  echo "[$H] === hydration done $(date -u +%H:%M:%S) ==="
'
