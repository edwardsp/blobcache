#!/usr/bin/env bash
set -uo pipefail

# blobcache end-to-end hydrate + triple-read benchmark.
#
# Wipes nothing on entry (caller's responsibility - see deploy/wipe-caches.sh).
# Drives the coordinator's /hydrate endpoint then runs three parallel
# read passes across all blobcached pods.
#
#   PASS1  - cold-after-hydrate: each pod has only its hydrate-shard
#            locally, so reads of the full prefix peer-fetch the rest.
#   PASS2  - warm: every pod is fully cache-resident; this is the
#            steady-state target case.
#   PASS3  - degraded recovery: BEFORE the read, exactly one pod's local
#            cache is wiped via POST /clear-cache-shard. That endpoint
#            drains in-flight inserts, removes every chunk file, resets
#            singleflight + peer-LRU + inflight-write maps, and rebuilds
#            the local bloom so peers stop routing to it. This simulates
#            a node failing and being replaced with empty NVMe (matches
#            real-world StatefulSet/DaemonSet recovery on this cluster
#            because cache lives on hostPath /mnt/nvme — pod restart
#            alone does NOT clear it). The wiped pod must then peer-fetch
#            its entire share from the other N-1 nodes.
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
#   LOG                - log file                          (default: /tmp/e2e.log)
#   HYDRATE_TIMEOUT_S  - curl --max-time for hydrate       (default: 3700)
#   READ_TIMEOUT_S     - kubectl exec timeout for reads    (default: 3600)
#   SKIP_PASS3         - set to 1 to skip the wipe+pass3 stage (default: unset)
#   WIPE_POD_INDEX     - 0-indexed line in podnodes.txt of the pod to
#                        wipe before PASS3. Default 1 (i.e. the SECOND
#                        pod) so we never wipe the coordinator (line 0).
#   WIPE_TIMEOUT_S     - curl --max-time for /clear-cache-shard (default: 120)

NS=${NS:-blobcache}
MOUNT=${MOUNT:-models}
PATH_PREFIX=${PATH_PREFIX:?PATH_PREFIX required, e.g. nvidia_DeepSeek-R1-0528-NVFP4-v2/}
READ_GLOB=${READ_GLOB:-*.safetensors}
LOG=${LOG:-/tmp/e2e.log}
HYDRATE_TIMEOUT_S=${HYDRATE_TIMEOUT_S:-3700}
READ_TIMEOUT_S=${READ_TIMEOUT_S:-3600}
HYDRATE_MODE=${HYDRATE_MODE:-default}
SKIP_PASS3=${SKIP_PASS3:-}
WIPE_POD_INDEX=${WIPE_POD_INDEX:-1}
WIPE_TIMEOUT_S=${WIPE_TIMEOUT_S:-120}

exec >"$LOG" 2>&1

fdiff() { awk -v a="$1" -v b="$2" 'BEGIN{printf "%.6f\n", a-b}'; }

echo "=========================================="
echo "E2E_RUN_START=$(date -u +%FT%TZ) ns=$NS mount=$MOUNT prefix=$PATH_PREFIX"
echo "=========================================="

COORD=$(kubectl --request-timeout=10s -n "$NS" get pod \
  -l app.kubernetes.io/component=blobcached \
  --field-selector=status.phase=Running \
  -o jsonpath='{.items[0].metadata.name}')
echo "COORDINATOR_POD=$COORD"

echo
echo "=== HYDRATE_START=$(date -u +%FT%TZ) ==="
HYDRATE_START_S=$(date +%s.%N)
kubectl --request-timeout="${HYDRATE_TIMEOUT_S}s" -n "$NS" exec "$COORD" -- \
  curl -sS --max-time "$HYDRATE_TIMEOUT_S" -X POST "http://127.0.0.1:7773/hydrate" \
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

if [ -z "$SKIP_PASS3" ]; then
  WIPE_POD=$(awk -v i="$WIPE_POD_INDEX" 'NR==i+1{print $1}' /tmp/podnodes.txt)
  WIPE_NODE=$(awk -v i="$WIPE_POD_INDEX" 'NR==i+1{print $2}' /tmp/podnodes.txt)
  if [ -z "$WIPE_POD" ]; then
    echo "=== PASS3_SKIPPED reason=no_pod_at_index_${WIPE_POD_INDEX} ==="
  else
    echo
    echo "=== WIPE_START=$(date -u +%FT%TZ) pod=$WIPE_POD node=$WIPE_NODE ==="
    WIPE_T0=$(date +%s.%N)
    kubectl --request-timeout="${WIPE_TIMEOUT_S}s" -n "$NS" exec "$WIPE_POD" -- \
      curl -sS --max-time "$WIPE_TIMEOUT_S" -X POST "http://127.0.0.1:7773/clear-cache-shard" \
        -H 'content-type: application/json' -d '{}' \
      >/tmp/wipe.json 2>/tmp/wipe.err
    WIPE_RC=$?
    WIPE_T1=$(date +%s.%N)
    WIPE_WALL=$(fdiff "$WIPE_T1" "$WIPE_T0")
    echo "=== WIPE_END=$(date -u +%FT%TZ) rc=$WIPE_RC wall=${WIPE_WALL}s ==="
    echo "--- wipe response ---"
    cat /tmp/wipe.json; echo
    echo "--- wipe err ---"
    cat /tmp/wipe.err

    POST_BYTES=$(kubectl --request-timeout=10s -n "$NS" exec "$WIPE_POD" -- \
      curl -sS --max-time 5 http://127.0.0.1:7773/metrics 2>/dev/null \
      | awk '$1=="blobcache_cache_bytes"{print $2; exit}')
    echo "--- post-wipe blobcache_cache_bytes on $WIPE_POD = ${POST_BYTES:-unknown} ---"

    run_pass PASS3 /tmp/pass3.tsv
  fi
else
  echo "=== PASS3_SKIPPED reason=SKIP_PASS3_set ==="
fi

echo
echo "=========================================="
echo "E2E_RUN_END=$(date -u +%FT%TZ)"
echo "=========================================="
touch /tmp/e2e.done
