#!/usr/bin/env bash
# diag-summary.sh - render a per-pod table from a diag-run.sh tag.
#
# Usage:
#   ./diag-summary.sh <out_dir> <run_tag> [<run_tag2> ...]
#
# Emits one block per tag with columns:
#   pod  node  wall_s  blob_fetches  peer_fetches_ok  peer_chunk_requests
#   peer_chunk_bytes_served  cache_hits  cache_misses
# Sorted by wall_s descending (slowest first).
set -uo pipefail

OUT_DIR=${1:?usage: $0 <out_dir> <tag> [tag...]}
shift
[ $# -ge 1 ] || { echo "need at least one run_tag" >&2; exit 2; }

for tag in "$@"; do
  PASS1="$OUT_DIR/${tag}-pass1.tsv"
  BEFORE="$OUT_DIR/${tag}-snap-before.tsv"
  AFTER="$OUT_DIR/${tag}-snap-after.tsv"
  for f in "$PASS1" "$BEFORE" "$AFTER"; do
    [ -s "$f" ] || { echo "missing or empty: $f" >&2; continue 2; }
  done
  echo "=== tag=$tag ==="
  awk -F'\t' '
    function metric(b, a, p, m) {
      key=p"\t"m
      return (a[key]+0) - (b[key]+0)
    }
    BEGIN { kind="pass1" }
    FNR==1 && FILENAME==before_f { kind="before" }
    FNR==1 && FILENAME==after_f  { kind="after" }
    FNR==1 && FILENAME==pass1_f  { kind="pass1" }
    /^#/ { next }
    kind=="before" {
      key=$1"\t"$3
      bv[key]=$NF
      pn[$1]=$2
      next
    }
    kind=="after" {
      key=$1"\t"$3
      av[key]=$NF
      pn[$1]=$2
      next
    }
    kind=="pass1" {
      pod=$1; node=$2; rest=$3
      n=split(rest, a, " ")
      for (i=1;i<=n;i++) {
        if (a[i] ~ /^wall_s=/) { split(a[i], b, "="); wall[pod]=b[2] }
      }
      pn[pod]=node
    }
    END {
      printf "%-32s %-32s %8s %10s %12s %14s %16s %12s %12s\n", \
        "pod","node","wall_s","blob_fetch","peer_ok","peer_serve_n","peer_serve_GiB","cache_hits","cache_miss"
      for (p in pn) ps[++np]=p
      asort(ps)
      n=np
      for (i=1;i<=n;i++) order[i]=ps[i]
      for (i=1;i<=n-1;i++) for (j=i+1;j<=n;j++) {
        if ((wall[order[i]]+0) < (wall[order[j]]+0)) {
          t=order[i]; order[i]=order[j]; order[j]=t
        }
      }
      for (i=1;i<=n;i++) {
        p=order[i]
        bf=metric(bv,av,p,"blobcache_blob_fetches_total")
        po=metric(bv,av,p,"blobcache_peer_fetches_ok_total")
        psn=metric(bv,av,p,"blobcache_peer_chunk_requests_total")
        psb=metric(bv,av,p,"blobcache_peer_chunk_bytes_served_total")
        ch=metric(bv,av,p,"blobcache_cache_hits_total")
        cm=metric(bv,av,p,"blobcache_cache_misses_total")
        printf "%-32s %-32s %8.1f %10d %12d %14d %16.2f %12d %12d\n", \
          p, pn[p], wall[p]+0, bf, po, psn, psb/1073741824.0, ch, cm
      }
    }
  ' before_f="$BEFORE" after_f="$AFTER" pass1_f="$PASS1" "$BEFORE" "$AFTER" "$PASS1"
  echo
done
