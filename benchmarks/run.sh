#!/usr/bin/env bash
set -uo pipefail

# Consolidated blobcache benchmark harness.
#
# Replaces cold-read.sh + e2e-hydrate-read.sh. Single entrypoint with flags
# for cache clearing, hydration, and N read passes. Designed for repeatable
# experiments: --clear-cache uses the daemon's /clear-cache endpoint
# (cluster-wide, no pod restart, no nvme-raid-init wipe DS race).
#
# Output: appends to ${OUT_DIR}/${TAG}-run.log; per-pass TSV at
# ${OUT_DIR}/${TAG}-passN.tsv; hydrate JSON at ${OUT_DIR}/${TAG}-hydrate.json
# (when --hydrate). All output is sanitized (pod/vmss IDs only, no personal
# identifiers). Wall-clock arithmetic via awk - no bc dependency.
#
# Usage:
#   benchmarks/run.sh [flags]
#
# Flags (or matching env var, env wins if both set on caller):
#   --ns NS              namespace                      (default: blobcache)
#   --mount NAME         mount name                     (default: models)
#   --prefix PREFIX      blob path prefix               (REQUIRED, e.g. models/foo/)
#   --read-glob GLOB     per-pod cat glob               (default: *.safetensors)
#   --tag TAG            run tag for output filenames   (default: yyyymmdd-hhmm)
#   --out-dir DIR        output directory               (default: benchmarks/out)
#   --passes N           number of read passes          (default: 1)
#   --hydrate            run /hydrate before passes     (default: off)
#   --hydrate-mode MODE  default | broadcast            (default: default)
#   --clear-cache        POST /clear-cache before run   (default: off)
#   --pf-port N          local port-forward port        (default: 17773)
#   --hydrate-timeout N  curl --max-time for hydrate    (default: 3700)
#   --read-timeout N     kubectl exec timeout per pod   (default: 3600)

NS=${NS:-blobcache}
MOUNT=${MOUNT:-models}
PATH_PREFIX=${PATH_PREFIX:-}
READ_GLOB=${READ_GLOB:-*.safetensors}
TAG=${TAG:-$(date -u +%Y%m%d-%H%M)}
OUT_DIR=${OUT_DIR:-benchmarks/out}
PASSES=${PASSES:-1}
DO_HYDRATE=${DO_HYDRATE:-0}
HYDRATE_MODE=${HYDRATE_MODE:-default}
DO_CLEAR=${DO_CLEAR:-0}
PF_LOCAL_PORT=${PF_LOCAL_PORT:-17773}
HYDRATE_TIMEOUT_S=${HYDRATE_TIMEOUT_S:-3700}
READ_TIMEOUT_S=${READ_TIMEOUT_S:-3600}
CLEAR_TIMEOUT_S=${CLEAR_TIMEOUT_S:-360}

while [ $# -gt 0 ]; do
  case "$1" in
    --ns) NS=$2; shift 2;;
    --mount) MOUNT=$2; shift 2;;
    --prefix) PATH_PREFIX=$2; shift 2;;
    --read-glob) READ_GLOB=$2; shift 2;;
    --tag) TAG=$2; shift 2;;
    --out-dir) OUT_DIR=$2; shift 2;;
    --passes) PASSES=$2; shift 2;;
    --hydrate) DO_HYDRATE=1; shift;;
    --hydrate-mode) HYDRATE_MODE=$2; DO_HYDRATE=1; shift 2;;
    --clear-cache) DO_CLEAR=1; shift;;
    --pf-port) PF_LOCAL_PORT=$2; shift 2;;
    --hydrate-timeout) HYDRATE_TIMEOUT_S=$2; shift 2;;
    --read-timeout) READ_TIMEOUT_S=$2; shift 2;;
    --clear-timeout) CLEAR_TIMEOUT_S=$2; shift 2;;
    -h|--help) sed -n '1,40p' "$0"; exit 0;;
    *) echo "unknown flag: $1" >&2; exit 2;;
  esac
done

[ -z "$PATH_PREFIX" ] && { echo "--prefix is required" >&2; exit 2; }
mkdir -p "$OUT_DIR"
LOG="$OUT_DIR/${TAG}-run.log"
exec >"$LOG" 2>&1

fdiff() { awk -v a="$1" -v b="$2" 'BEGIN{printf "%.6f\n", a-b}'; }

start_pf() {
  # Reuse existing port-forward if alive on the same port; otherwise reap and start.
  local pids
  pids=$(ps -eo pid,args --no-headers | grep -E "kubectl.*port-forward.*${PF_LOCAL_PORT}:7773" | grep -v grep | awk '{print $1}')
  [ -n "$pids" ] && echo "$pids" | xargs -r kill -9 2>/dev/null || true
  COORD=$(kubectl --request-timeout=10s -n "$NS" get pod \
    -l app.kubernetes.io/component=blobcached \
    --field-selector=status.phase=Running \
    -o jsonpath='{.items[0].metadata.name}')
  echo "COORDINATOR_POD=$COORD"
  kubectl -n "$NS" port-forward "pod/$COORD" "${PF_LOCAL_PORT}:7773" >"$OUT_DIR/${TAG}-pf.log" 2>&1 &
  PF_PID=$!
  sleep 5
}

stop_pf() {
  [ -n "${PF_PID:-}" ] && kill "$PF_PID" 2>/dev/null
  wait "$PF_PID" 2>/dev/null || true
  PF_PID=""
}

echo "=========================================="
echo "RUN_START=$(date -u +%FT%TZ) tag=$TAG ns=$NS mount=$MOUNT prefix=$PATH_PREFIX glob=$READ_GLOB passes=$PASSES hydrate=$DO_HYDRATE hydrate_mode=$HYDRATE_MODE clear=$DO_CLEAR"
echo "=========================================="

if [ "$DO_CLEAR" = "1" ]; then
  start_pf
  echo
  echo "=== CLEAR_CACHE_START=$(date -u +%FT%TZ) ==="
  C0=$(date +%s.%N)
  curl -sS --max-time "$CLEAR_TIMEOUT_S" -X POST "http://127.0.0.1:${PF_LOCAL_PORT}/clear-cache" \
    -H 'content-type: application/json' -d '{}' \
    >"$OUT_DIR/${TAG}-clear.json" 2>"$OUT_DIR/${TAG}-clear.err"
  CRC=$?
  C1=$(date +%s.%N)
  CW=$(fdiff "$C1" "$C0")
  echo "=== CLEAR_CACHE_END=$(date -u +%FT%TZ) rc=$CRC wall=${CW}s ==="
  echo "--- clear response (head) ---"
  head -c 1200 "$OUT_DIR/${TAG}-clear.json"; echo
  stop_pf
fi

if [ "$DO_HYDRATE" = "1" ]; then
  start_pf
  echo
  echo "=== HYDRATE_START=$(date -u +%FT%TZ) ==="
  H0=$(date +%s.%N)
  curl -sS --max-time "$HYDRATE_TIMEOUT_S" -X POST "http://127.0.0.1:${PF_LOCAL_PORT}/hydrate" \
    -H 'content-type: application/json' \
    -d "{\"mount\":\"${MOUNT}\",\"path\":\"${PATH_PREFIX}\",\"recursive\":true,\"mode\":\"${HYDRATE_MODE}\"}" \
    >"$OUT_DIR/${TAG}-hydrate.json" 2>"$OUT_DIR/${TAG}-hydrate.err"
  HRC=$?
  H1=$(date +%s.%N)
  HW=$(fdiff "$H1" "$H0")
  echo "=== HYDRATE_END=$(date -u +%FT%TZ) rc=$HRC wall=${HW}s bytes=$(wc -c <"$OUT_DIR/${TAG}-hydrate.json") ==="
  echo "--- hydrate response (head) ---"
  head -c 600 "$OUT_DIR/${TAG}-hydrate.json"; echo
  stop_pf
fi

kubectl --request-timeout=10s -n "$NS" get pods \
  -l app.kubernetes.io/component=blobcached \
  --field-selector=status.phase=Running \
  -o jsonpath='{range .items[*]}{.metadata.name} {.spec.nodeName}{"\n"}{end}' >"$OUT_DIR/${TAG}-podnodes.txt"
echo
echo "=== POD/NODE MAP ==="
cat "$OUT_DIR/${TAG}-podnodes.txt"

run_pass() {
  local n=$1
  local tsv="$OUT_DIR/${TAG}-pass${n}.tsv"
  echo
  echo "=== READ_PASS${n}_START=$(date -u +%FT%TZ) ==="
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
# Background heartbeat keeps the kubectl exec websocket alive; without it
# the apiserver SPDY/websocket idle-timeout (3 min) tears down long reads
# even though the cat is still progressing on the pod.
( while :; do sleep 30; echo HB \$(date +%s); done ) &
HB_PID=\$!
trap 'kill \$HB_PID 2>/dev/null' EXIT
for f in /mnt/nvme/blobcache-mnt/${MOUNT}/${PATH_PREFIX}${READ_GLOB}; do
  [ -e \"\$f\" ] || continue
  S=\$(stat -c %s \"\$f\")
  cat \"\$f\" >/dev/null 2>&1 || true
  TOTAL=\$((TOTAL+S))
  COUNT=\$((COUNT+1))
done
END=\$(date +%s.%N)
WALL=\$(awk -v a=\"\$END\" -v b=\"\$START\" 'BEGIN{printf \"%.3f\", a-b}')
kill \$HB_PID 2>/dev/null
echo \"RESULT files=\$COUNT bytes=\$TOTAL wall_s=\$WALL\"
" 2>&1 | grep -E '^RESULT')
      printf '%s\t%s\t%s\n' "$pod" "$node" "$OUT" >>"$tsv"
    ) &
  done <"$OUT_DIR/${TAG}-podnodes.txt"
  wait
  t1=$(date +%s.%N)
  wall=$(fdiff "$t1" "$t0")
  echo "=== READ_PASS${n}_END=$(date -u +%FT%TZ) wall=${wall}s ==="
  echo "--- pass${n} results ---"
  sort "$tsv"
}

i=1
while [ "$i" -le "$PASSES" ]; do
  run_pass "$i"
  i=$((i+1))
done

echo
echo "=========================================="
echo "RUN_END=$(date -u +%FT%TZ) tag=$TAG"
echo "=========================================="
touch "$OUT_DIR/${TAG}.done"
