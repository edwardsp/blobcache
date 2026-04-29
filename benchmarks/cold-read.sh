#!/usr/bin/env bash
set -uo pipefail

# Cold-cache parallel read benchmark across every blobcached pod.
#
# Caller is responsible for ensuring caches are empty (see
# deploy/wipe-caches.sh + `kubectl rollout restart ds/<release>-blobcached`).
# No hydrate is performed - every chunk is fetched on demand from
# Azure Blob (or peer-served via the stampede leader once one pod
# starts caching). Useful for isolating blob-fetch performance,
# prefetch tuning, and head-of-line behaviour under concurrent miss.
#
# Required env (or argv):
#   PATH_PREFIX        - blob path prefix, e.g. nvidia_DeepSeek-R1-0528-NVFP4-v2/
# Optional:
#   NS                 - namespace             (default: blobcache)
#   MOUNT              - mount name            (default: models)
#   READ_GLOB          - per-pod read pattern  (default: *.safetensors)
#   LOG                - log file              (default: /tmp/cold-read.log)
#   TSV                - per-pod TSV           (default: /tmp/cold-read.tsv)
#   READ_TIMEOUT_S     - kubectl exec timeout  (default: 3600)

NS=${NS:-blobcache}
MOUNT=${MOUNT:-models}
PATH_PREFIX=${PATH_PREFIX:?PATH_PREFIX required, e.g. nvidia_DeepSeek-R1-0528-NVFP4-v2/}
READ_GLOB=${READ_GLOB:-*.safetensors}
LOG=${LOG:-/tmp/cold-read.log}
TSV=${TSV:-/tmp/cold-read.tsv}
READ_TIMEOUT_S=${READ_TIMEOUT_S:-3600}

exec >"$LOG" 2>&1

fdiff() { awk -v a="$1" -v b="$2" 'BEGIN{printf "%.6f\n", a-b}'; }

echo "=========================================="
echo "COLD_READ_START=$(date -u +%FT%TZ) ns=$NS mount=$MOUNT prefix=$PATH_PREFIX glob=$READ_GLOB"
echo "=========================================="

kubectl --request-timeout=10s -n "$NS" get pods \
  -l app.kubernetes.io/component=blobcached \
  --field-selector=status.phase=Running \
  -o jsonpath='{range .items[*]}{.metadata.name} {.spec.nodeName}{"\n"}{end}' >/tmp/podnodes.txt
echo "=== POD/NODE MAP ==="
cat /tmp/podnodes.txt

echo
echo "=== READ_START=$(date -u +%FT%TZ) ==="
T0=$(date +%s.%N)
: >"$TSV"
while read -r pod node; do
  [ -z "$pod" ] && continue
  (
    OUT=$(kubectl --request-timeout="${READ_TIMEOUT_S}s" -n "$NS" exec "$pod" -- bash -c "
START=\$(date +%s.%N)
TOTAL=0
COUNT=0
for f in /mnt/nvme/blobcache-mnt/${MOUNT}/${PATH_PREFIX}${READ_GLOB}; do
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
    printf '%s\t%s\t%s\n' "$pod" "$node" "$OUT" >>"$TSV"
  ) &
done </tmp/podnodes.txt
wait
T1=$(date +%s.%N)
WALL=$(fdiff "$T1" "$T0")
echo "=== READ_END=$(date -u +%FT%TZ) wall=${WALL}s ==="
echo "--- per-pod results ---"
sort "$TSV"

echo
echo "=========================================="
echo "COLD_READ_END=$(date -u +%FT%TZ)"
echo "=========================================="
touch "${LOG%.log}.done"
