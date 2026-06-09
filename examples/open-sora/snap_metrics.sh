#!/bin/bash
# snap_metrics.sh OUT.csv
# Snapshot blobcache counters from every benchmark node (direct :7773/metrics),
# one row per node. Used to bracket a read-benchmark run (before/after) so we can
# diff blob/peer/local bytes + IOPS + bandwidth without the in-job sampler.
#
# Set NODES to your benchmark nodelist in Slurm hostlist syntax, e.g.
#   NODES='node-[0002-0006,0008-0066]' ./snap_metrics.sh before.csv
set -uo pipefail
OUT=$1
NODES=${NODES:?set NODES to your benchmark nodelist, e.g. node-[0002-0066]}
echo "host,blob_fetches,blob_bytes,peer_fetches,peer_bytes,cache_hits,cache_misses,cache_bytes,fuse_reads,fuse_read_bytes,ts_ms" > "$OUT"
for n in $(scontrol show hostnames "$NODES"); do
  m=$(curl -s --max-time 8 "http://$n:7773/metrics" 2>/dev/null)
  g(){ printf '%s\n' "$m" | awk -v k="$1" '$1==k{print $2; f=1} END{if(!f)print 0}'; }
  echo "$n,$(g blobcache_blob_fetches_total),$(g blobcache_blob_fetch_bytes_total),$(g blobcache_peer_fetches_ok_total),$(g blobcache_peer_fetch_bytes_total),$(g blobcache_cache_hits_total),$(g blobcache_cache_misses_total),$(g blobcache_cache_bytes),$(g blobcache_fuse_reads_total),$(g blobcache_fuse_read_bytes_total),$(date +%s%3N)" >> "$OUT"
done
echo "snapshot -> $OUT ($(($(wc -l < "$OUT")-1)) nodes)"
