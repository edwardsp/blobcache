#!/usr/bin/env bash
# diag-straggler.sh - per-pod metric snapshot tool for straggler diagnosis.
#
# Snapshots a fixed set of Prometheus counters from every blobcached pod
# in the namespace into TSV files. Run twice (BEFORE and AFTER a
# benchmark pass) and feed both files into diff-snapshots.awk to
# produce per-pod deltas + rate analysis.
#
# Usage:
#   ./diag-straggler.sh snapshot  <out.tsv>
#   ./diag-straggler.sh diff      <before.tsv> <after.tsv> <wall_seconds>
#
# Required env (or defaults):
#   NS  - namespace (default: blobcache)
set -uo pipefail

NS=${NS:-blobcache}

# Counters to snapshot. Keep this list stable across runs so diff lines
# up cleanly. We grab every metric we care about for convoy / hardware
# variance / hot-spot diagnosis.
METRIC_NAMES=(
  blobcache_blob_fetches_total
  blobcache_blob_fetch_bytes_total
  blobcache_cache_hits_total
  blobcache_cache_misses_total
  blobcache_peer_fetches_ok_total
  blobcache_peer_fetches_miss_total
  blobcache_peer_fetches_err_total
  blobcache_peer_fetch_bytes_total
  blobcache_peer_chunk_requests_total
  blobcache_peer_chunk_bytes_served_total
  blobcache_fuse_reads_total
  blobcache_fuse_read_bytes_total
  blobcache_singleflight_waits_total
  blobcache_peer_bloom_stale_drops_total
  blobcache_peer_bloom_false_positive_total
  blobcache_cache_insert_failures_total
  blobcache_broadcast_peer_not_found_total
  blobcache_broadcast_blob_fallback_ok_total
  blobcache_broadcast_blob_fallback_err_total
)

snapshot() {
  local out=$1
  : >"$out"
  local pods
  pods=$(kubectl --request-timeout=10s -n "$NS" get pod \
    -l app.kubernetes.io/component=blobcached \
    --field-selector=status.phase=Running \
    -o jsonpath='{range .items[*]}{.metadata.name} {.spec.nodeName}{"\n"}{end}')
  local pat
  pat=$(printf '^%s ' "${METRIC_NAMES[@]}" | sed 's/ $//' | tr ' ' '|')

  echo "# snapshot ts=$(date -u +%FT%T.%NZ)" >>"$out"
  local tmpdir
  tmpdir=$(mktemp -d)
  while read -r pod node; do
    [ -z "$pod" ] && continue
    (
      local body
      body=$(kubectl --request-timeout=15s -n "$NS" exec "$pod" -- \
        curl -sS --max-time 10 http://127.0.0.1:7773/metrics 2>/dev/null \
        | grep -E "$pat" || true)
      while IFS= read -r line; do
        [ -z "$line" ] && continue
        printf '%s\t%s\t%s\n' "$pod" "$node" "$line"
      done <<<"$body" >"$tmpdir/$pod"
    ) &
  done <<<"$pods"
  wait
  cat "$tmpdir"/* 2>/dev/null >>"$out"
  rm -rf "$tmpdir"
}

diff_snapshot() {
  local before=$1 after=$2 wall=$3
  awk -v wall="$wall" '
    /^#/ { next }
    NR==FNR {
      key=$1 "\t" $3
      before[key]=$NF
      node[$1]=$2
      next
    }
    {
      key=$1 "\t" $3
      pod=$1
      metric=$3
      delta=$NF - (key in before ? before[key] : 0)
      rate=delta/wall
      d[pod, metric]=delta
      r[pod, metric]=rate
      pods[pod]=$2
      metrics[metric]=1
    }
    END {
      n=0
      for (m in metrics) ms[++n]=m
      asort(ms)
      np=0
      for (p in pods) ps[++np]=p
      asort(ps)
      printf "%-32s %-32s", "pod", "node"
      for (i=1;i<=n;i++) printf " %30s", ms[i]
      print ""
      for (j=1;j<=np;j++) {
        p=ps[j]
        printf "%-32s %-32s", p, pods[p]
        for (i=1;i<=n;i++) {
          v=d[p,ms[i]]+0
          printf " %30.0f", v
        }
        print ""
      }
    }
  ' "$before" "$after"
}

cmd=${1:?usage: $0 snapshot|diff ...}
shift
case "$cmd" in
  snapshot) snapshot "$@" ;;
  diff)     diff_snapshot "$@" ;;
  *) echo "unknown cmd: $cmd" >&2; exit 2 ;;
esac
