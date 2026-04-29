#!/usr/bin/env bash
set -uo pipefail

# blobcache end-to-end hydrate + dual-read benchmark.
#
# Wipes nothing (caller's responsibility - see deploy/wipe-caches.sh).
# Drives the coordinator's /hydrate endpoint then runs two parallel
# read passes across all blobcached pods. Pass 1 is peer-fetch warm-up
# (each pod has only its hydrate-shard locally); pass 2 should be
# fully cache-resident and is the steady-state target case.
#
# Wall-clock arithmetic uses awk (no `bc` dependency in the daemon
# image). All output is appended to $LOG (default /tmp/e2e.log).
#
# Required env (or argv):
#   NS                 - namespace            (default: blobcache)
#   MOUNT              - mount name           (default: models)
#   PATH_PREFIX        - blob path prefix     (REQUIRED, e.g. nvidia_DeepSeek-R1-0528-NVFP4-v2/)
#   READ_GLOB          - per-pod read pattern (default: *.safetensors)
# Optional:
#   LOG                - log file             (default: /tmp/e2e.log)
#   PF_LOCAL_PORT      - kubectl port-forward (default: 17773)
#   HYDRATE_TIMEOUT_S  - curl --max-time      (default: 3700)
#   READ_TIMEOUT_S     - kubectl exec timeout (default: 3600)

NS=${NS:-blobcache}
MOUNT=${MOUNT:-models}
PATH_PREFIX=${PATH_PREFIX:?PATH_PREFIX required, e.g. nvidia_DeepSeek-R1-0528-NVFP4-v2/}
READ_GLOB=${READ_GLOB:-*.safetensors}
LOG=${LOG:-/tmp/e2e.log}
PF_LOCAL_PORT=${PF_LOCAL_PORT:-17773}
HYDRATE_TIMEOUT_S=${HYDRATE_TIMEOUT_S:-3700}
READ_TIMEOUT_S=${READ_TIMEOUT_S:-3600}
HYDRATE_MODE=${HYDRATE_MODE:-default}

exec >"$LOG" 2>&1

# Float subtraction without bc.
fdiff() { awk -v a="$1" -v b="$2" 'BEGIN{printf "%.6f\n", a-b}'; }

echo "=========================================="
echo "E2E_RUN_START=$(date -u +%FT%TZ) ns=$NS mount=$MOUNT prefix=$PATH_PREFIX"
echo "=========================================="

pkill -9 -f "kubectl.*port-forward.*${PF_LOCAL_PORT}" 2>/dev/null
sleep 2
COORD=$(kubectl --request-timeout=10s -n "$NS" get pod \
  -l app.kubernetes.io/component=blobcached \
  --field-selector=status.phase=Running \
  -o jsonpath='{.items[0].metadata.name}')
echo "COORDINATOR_POD=$COORD"
kubectl -n "$NS" port-forward "pod/$COORD" "${PF_LOCAL_PORT}:7773" >/tmp/e2e-pf.log 2>&1 &
PF_PID=$!
sleep 5

echo
echo "=== HYDRATE_START=$(date -u +%FT%TZ) ==="
HYDRATE_START_S=$(date +%s.%N)
curl -sS --max-time "$HYDRATE_TIMEOUT_S" -X POST "http://127.0.0.1:${PF_LOCAL_PORT}/hydrate" \
  -H 'content-type: application/json' \
  -d "{\"mount\":\"${MOUNT}\",\"path\":\"${PATH_PREFIX}\",\"recursive\":true,\"mode\":\"${HYDRATE_MODE}\"}" \
  >/tmp/hydrate.json 2>/tmp/hydrate.err
HYDRATE_RC=$?
HYDRATE_END_S=$(date +%s.%N)
HYDRATE_WALL=$(fdiff "$HYDRATE_END_S" "$HYDRATE_START_S")
echo "=== HYDRATE_END=$(date -u +%FT%TZ) rc=$HYDRATE_RC wall=${HYDRATE_WALL}s bytes=$(wc -c </tmp/hydrate.json) ==="
echo "--- hydrate response (head) ---"
head -c 600 /tmp/hydrate.json; echo
echo "--- hydrate err ---"
cat /tmp/hydrate.err

kill $PF_PID 2>/dev/null
wait $PF_PID 2>/dev/null

kubectl --request-timeout=10s -n "$NS" get pods \
  -l app.kubernetes.io/component=blobcached \
  --field-selector=status.phase=Running \
  -o jsonpath='{range .items[*]}{.metadata.name} {.spec.nodeName}{"\n"}{end}' >/tmp/podnodes.txt
echo
echo "=== POD/NODE MAP ==="
cat /tmp/podnodes.txt

run_pass() {
  local label=$1 tsv=$2
  echo
  echo "=== READ_${label}_START=$(date -u +%FT%TZ) ==="
  local t0 t1 wall
  t0=$(date +%s.%N)
  : >"$tsv"
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
      printf '%s\t%s\t%s\n' "$pod" "$node" "$OUT" >>"$tsv"
    ) &
  done </tmp/podnodes.txt
  wait
  t1=$(date +%s.%N)
  wall=$(fdiff "$t1" "$t0")
  echo "=== READ_${label}_END=$(date -u +%FT%TZ) wall=${wall}s ==="
  echo "--- ${label,,} results ---"
  cat "$tsv"
}

run_pass PASS1 /tmp/pass1.tsv
run_pass PASS2 /tmp/pass2.tsv

echo
echo "=========================================="
echo "E2E_RUN_END=$(date -u +%FT%TZ)"
echo "=========================================="
touch /tmp/e2e.done
