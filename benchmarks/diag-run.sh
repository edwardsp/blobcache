#!/usr/bin/env bash
# diag-run.sh - one-shot diagnostic benchmark with metric snapshots.
#
# Wraps benchmarks/e2e-hydrate-read.sh-style read pass with:
#   - cache wipe before run
#   - metric snapshot before PASS1
#   - metric snapshot after PASS1
#   - per-pod wall-clock from cat-style read
#   - optional shuffled read order (READ_ORDER=shuffled|alpha)
#
# Outputs to OUT_DIR (default /tmp/diag) named by RUN_TAG:
#   <tag>-snap-before.tsv
#   <tag>-snap-after.tsv
#   <tag>-pass1.tsv
#   <tag>-run.log
#
# Required env (or argv):
#   PATH_PREFIX  - blob path prefix, e.g. nvidia_DeepSeek-R1-0528-NVFP4-v2/
# Optional:
#   NS=blobcache MOUNT=models READ_GLOB='*.safetensors'
#   READ_ORDER=alpha|shuffled  (default: alpha)
#   SHUFFLE_SEED=<int>         (default: pod-name-derived, deterministic per pod)
#   RUN_TAG=<name>             (default: timestamp)
#   OUT_DIR=/tmp/diag
#   READ_TIMEOUT_S=3600
#   SKIP_HYDRATE=0|1           (1 = assume cluster already hydrated; cache wipe still happens)
#   SKIP_WIPE=0|1              (1 = no cache wipe; expect cache hot already)
set -uo pipefail

NS=${NS:-blobcache}
MOUNT=${MOUNT:-models}
PATH_PREFIX=${PATH_PREFIX:?PATH_PREFIX required}
READ_GLOB=${READ_GLOB:-*.safetensors}
READ_ORDER=${READ_ORDER:-alpha}
SHUFFLE_SEED=${SHUFFLE_SEED:-}
RUN_TAG=${RUN_TAG:-$(date -u +%Y%m%dT%H%M%S)}
OUT_DIR=${OUT_DIR:-/tmp/diag}
READ_TIMEOUT_S=${READ_TIMEOUT_S:-3600}
SKIP_HYDRATE=${SKIP_HYDRATE:-0}
SKIP_WIPE=${SKIP_WIPE:-0}
HYDRATE_TIMEOUT_S=${HYDRATE_TIMEOUT_S:-3700}
HYDRATE_MODE=${HYDRATE_MODE:-default}

mkdir -p "$OUT_DIR"
LOG="$OUT_DIR/${RUN_TAG}-run.log"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

exec >"$LOG" 2>&1

fdiff() { awk -v a="$1" -v b="$2" 'BEGIN{printf "%.6f\n", a-b}'; }

echo "=========================================="
echo "DIAG_RUN_START=$(date -u +%FT%T.%NZ) tag=$RUN_TAG order=$READ_ORDER"
echo "=========================================="

PODS_FILE="$OUT_DIR/${RUN_TAG}-podnodes.txt"
kubectl --request-timeout=10s -n "$NS" get pods \
  -l app.kubernetes.io/component=blobcached \
  --field-selector=status.phase=Running \
  -o jsonpath='{range .items[*]}{.metadata.name} {.spec.nodeName}{"\n"}{end}' >"$PODS_FILE"
echo "POD/NODE MAP:"
cat "$PODS_FILE"

if [ "$SKIP_WIPE" != "1" ]; then
  echo
  echo "=== CACHE_WIPE_START=$(date -u +%FT%TZ) ==="
  WIPE_T0=$(date +%s.%N)
  while read -r pod node; do
    [ -z "$pod" ] && continue
    (
      kubectl --request-timeout=120s -n "$NS" exec "$pod" -- \
        sh -c 'rm -f /mnt/nvme/blobcache-cache/* 2>&1 | head -3; echo "wiped"' >/dev/null 2>&1
    ) &
  done <"$PODS_FILE"
  wait
  WIPE_T1=$(date +%s.%N)
  echo "=== CACHE_WIPE_END=$(date -u +%FT%TZ) wall=$(fdiff "$WIPE_T1" "$WIPE_T0")s ==="
fi

if [ "$SKIP_HYDRATE" != "1" ]; then
  COORD=$(head -1 "$PODS_FILE" | awk '{print $1}')
  echo
  echo "=== HYDRATE_START=$(date -u +%FT%TZ) coordinator=$COORD ==="
  HYD_T0=$(date +%s.%N)
  kubectl --request-timeout="${HYDRATE_TIMEOUT_S}s" -n "$NS" exec "$COORD" -- \
    curl -sS --max-time "$HYDRATE_TIMEOUT_S" -X POST "http://127.0.0.1:7773/hydrate" \
      -H 'content-type: application/json' \
      -d "{\"mount\":\"${MOUNT}\",\"path\":\"${PATH_PREFIX}\",\"recursive\":true,\"mode\":\"${HYDRATE_MODE}\"}" \
    >"$OUT_DIR/${RUN_TAG}-hydrate.json" 2>"$OUT_DIR/${RUN_TAG}-hydrate.err"
  HYD_RC=$?
  HYD_T1=$(date +%s.%N)
  echo "=== HYDRATE_END=$(date -u +%FT%TZ) rc=$HYD_RC wall=$(fdiff "$HYD_T1" "$HYD_T0")s bytes=$(wc -c <"$OUT_DIR/${RUN_TAG}-hydrate.json") ==="
  head -c 600 "$OUT_DIR/${RUN_TAG}-hydrate.json"; echo
fi

SNAP_BEFORE="$OUT_DIR/${RUN_TAG}-snap-before.tsv"
SNAP_AFTER="$OUT_DIR/${RUN_TAG}-snap-after.tsv"
echo
echo "=== SNAP_BEFORE=$(date -u +%FT%T.%NZ) ==="
"$SCRIPT_DIR/diag-straggler.sh" snapshot "$SNAP_BEFORE"
echo "snap-before lines=$(wc -l <"$SNAP_BEFORE")"

PASS1_TSV="$OUT_DIR/${RUN_TAG}-pass1.tsv"
echo
echo "=== READ_PASS1_START=$(date -u +%FT%T.%NZ) order=$READ_ORDER ==="
P1_T0=$(date +%s.%N)
: >"$PASS1_TSV"

if [ "$READ_ORDER" = "shuffled" ]; then
  ORDER_SCRIPT='
SEED=${SHUFFLE_SEED:-$(printf "%s" "$HOSTNAME" | cksum | awk "{print \$1}")}
mapfile -t FILES < <(ls /mnt/nvme/blobcache-mnt/'"${MOUNT}"'/'"${PATH_PREFIX}"''"${READ_GLOB}"' 2>/dev/null | shuf --random-source=<(awk "BEGIN{srand($SEED); for(i=0;i<1000000;i++) printf \"%c\", int(rand()*256)}"))
'
else
  ORDER_SCRIPT='
mapfile -t FILES < <(ls /mnt/nvme/blobcache-mnt/'"${MOUNT}"'/'"${PATH_PREFIX}"''"${READ_GLOB}"' 2>/dev/null | sort)
'
fi

while read -r pod node; do
  [ -z "$pod" ] && continue
  (
    OUT=$(kubectl --request-timeout="${READ_TIMEOUT_S}s" -n "$NS" exec "$pod" -- bash -c "
$ORDER_SCRIPT
START=\$(date +%s.%N)
TOTAL=0
COUNT=0
for f in \"\${FILES[@]}\"; do
  [ -e \"\$f\" ] || continue
  S=\$(stat -c %s \"\$f\")
  cat \"\$f\" >/dev/null 2>&1 || true
  TOTAL=\$((TOTAL+S))
  COUNT=\$((COUNT+1))
done
END=\$(date +%s.%N)
WALL=\$(awk -v a=\"\$END\" -v b=\"\$START\" 'BEGIN{printf \"%.3f\", a-b}')
echo \"files=\$COUNT bytes=\$TOTAL wall_s=\$WALL\"
" 2>&1)
    printf '%s\t%s\t%s\n' "$pod" "$node" "$OUT" >>"$PASS1_TSV"
  ) &
done <"$PODS_FILE"
wait
P1_T1=$(date +%s.%N)
P1_WALL=$(fdiff "$P1_T1" "$P1_T0")
echo "=== READ_PASS1_END=$(date -u +%FT%T.%NZ) wall=${P1_WALL}s ==="
sort -t $'\t' -k1 "$PASS1_TSV" | head
echo

echo "=== SNAP_AFTER=$(date -u +%FT%T.%NZ) ==="
"$SCRIPT_DIR/diag-straggler.sh" snapshot "$SNAP_AFTER"
echo "snap-after lines=$(wc -l <"$SNAP_AFTER")"

echo
echo "=========================================="
echo "DIAG_RUN_END=$(date -u +%FT%T.%NZ) wall_pass1=${P1_WALL}s"
echo "=========================================="
echo "tag=$RUN_TAG"
echo "out_dir=$OUT_DIR"
echo "pass1_tsv=$PASS1_TSV"
echo "snap_before=$SNAP_BEFORE"
echo "snap_after=$SNAP_AFTER"
touch "$OUT_DIR/${RUN_TAG}.done"
