#!/bin/bash
# Three-tier benchmark for blobcache
# Per-node picks a unique fresh shard (not yet in NVMe cache) so tier 1 is a true cold blob fetch
set -u
H=$(hostname)
# Stable per-node shard index: extract NN from paul-h200-gpu-00NN, shift into 50..65 range
N=$(echo "$H" | sed -E 's/.*gpu-([0-9]+)$/\1/')
IDX=$((50 + (10#$N - 2)))   # 0002→50, 0003→51, ...
SHARD=$(printf "/blobcache/deepseek/model-%05d-of-000163.safetensors" "$IDX")
BS=4M
COUNT=512   # 2 GiB per test
SIZE_MB=$((4 * COUNT))

run() {
  local label=$1
  local out
  out=$(dd if="$SHARD" of=/dev/null bs=$BS count=$COUNT iflag=fullblock 2>&1 | tail -1)
  # extract "X.Y GB/s" or "X MB/s"
  local rate=$(echo "$out" | grep -oE '[0-9.]+ [GM]B/s' | tail -1)
  echo "  [$H] $label  $rate"
}

# Make sure the file exists / preflight
if ! [ -f "$SHARD" ]; then echo "[$H] MISSING $SHARD"; exit 0; fi

# TIER 1: cold blob (no NVMe cache yet, no page cache)
sudo /usr/bin/sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'
run "tier1 cold-blob   "

# TIER 2: warm NVMe / cold page cache
sudo /usr/bin/sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'
run "tier2 cold-page   "

# TIER 3: warm page cache
run "tier3 warm-page   "
