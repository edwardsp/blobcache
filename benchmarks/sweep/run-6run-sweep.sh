#!/usr/bin/env bash
# run-6run-sweep.sh - canonical 3-config × 2-trial 6-run sweep.
#
# Reproduces benchmarks/RESULTS-2026-04-30-tier1-baseline-6run-sweep.md.
# Each trial does helm uninstall + reinstall to guarantee a clean bloom +
# clean NVMe (matches the Tier-1 baseline protocol so results are directly
# comparable run-over-run).
#
# Required env (also see render-overlay.sh):
#   BLOBCACHE_CLIENT_ID       Azure MI client-id
#   BLOBCACHE_ACCOUNT         storage account
#   BLOBCACHE_SEED_1/2/3      gossip seed pod IPs
#   BLOBCACHE_IMAGE_TAG       e.g. sha-16ec6a6-arm64
# Optional:
#   OUT_DIR=/tmp/sweep-<utc>
#   PATH_PREFIX=nvidia_DeepSeek-R1-0528-NVFP4-v2/
#   NS=blobcache
#   RELEASE=blobcache
#   CHART=deploy/helm/blobcache
#
# Output: per-trial pass1.tsv, pass2.tsv, hydrate.json, gather.json,
# snap-{before,after,before2,after2}.tsv, run.log under OUT_DIR.
# Plus OUT_DIR/sweep-summary.tsv with one line per trial:
#   tag start_utc end_utc hydrate_s gather_s pass1_s pass2_s
set -uo pipefail

NS="${NS:-blobcache}"
RELEASE="${RELEASE:-blobcache}"
CHART="${CHART:-deploy/helm/blobcache}"
PATH_PREFIX="${PATH_PREFIX:-nvidia_DeepSeek-R1-0528-NVFP4-v2/}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

UTC_TS="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_DIR="${OUT_DIR:-/tmp/sweep-$UTC_TS}"
mkdir -p "$OUT_DIR"

# Render overlays from templates -> OUT_DIR (keeps secrets out of cwd).
"$SCRIPT_DIR/render-overlay.sh" "$SCRIPT_DIR/values-cache-peer-off.yaml.tmpl" > "$OUT_DIR/values-off.yaml"
"$SCRIPT_DIR/render-overlay.sh" "$SCRIPT_DIR/values-cache-peer-on.yaml.tmpl"  > "$OUT_DIR/values-on.yaml"

SUMMARY="$OUT_DIR/sweep-summary.tsv"
printf 'tag\tstart_utc\tend_utc\thydrate_s\tgather_s\tpass1_s\tpass2_s\thyd_status\n' > "$SUMMARY"

reinstall() {
  local values="$1"
  echo "==[ helm uninstall + install with $(basename "$values") ]=="
  helm -n "$NS" uninstall "$RELEASE" --wait --timeout 5m >/dev/null 2>&1 || true
  helm -n "$NS" install   "$RELEASE" "$REPO_ROOT/$CHART" -f "$values" --wait --timeout 10m
  kubectl --request-timeout=15s -n "$NS" rollout status ds/blobcache-blobcached --timeout=10m
  wait_http_ready
  # Settle window: 60s covers two needs.
  #   1. Gossip + bloom warmup.
  #   2. Prometheus scrape baseline. PodMonitor scrape interval is 15s and
  #      rate() needs >=2 samples in its 1-min window. With only 15s of
  #      pre-workload runtime the first trial of a fresh-pod sweep produces
  #      flat Grafana panels even when GBs are moving (observed: t1 invisible,
  #      t2 of identical workload clearly visible because pods had been
  #      scraped ~50 times by then). 60s gives Prometheus 4+ baseline scrapes.
  sleep 60
}

# wait_http_ready: poll every blobcached pod's stats endpoint (:7773/metrics)
# until ALL respond 200, with a hard timeout. k8s `rollout status` only
# guarantees the container is running; the embedded HTTP server may not yet
# be accepting connections. Hydrate POSTs to /hydrate-shard fail silently
# otherwise (one peer never fetches its 5,812-chunk shard, evident only as
# blob-fetch surge during PASS1 - sweep run 2026-05-01T12:56Z reproduced this).
wait_http_ready() {
  local timeout_s=120 poll_s=2 deadline expected ready ips
  deadline=$(( $(date +%s) + timeout_s ))
  expected="$(kubectl -n "$NS" get ds blobcache-blobcached -o jsonpath='{.status.desiredNumberScheduled}' 2>/dev/null || echo 0)"
  echo "==[ wait_http_ready: polling :7773/metrics on $expected pods, timeout ${timeout_s}s ]=="
  while [ "$(date +%s)" -lt "$deadline" ]; do
    ips="$(kubectl -n "$NS" get pods -l app.kubernetes.io/component=blobcached \
             -o jsonpath='{range .items[?(@.status.phase=="Running")]}{.status.podIP}{"\n"}{end}' 2>/dev/null \
           | grep -v '^$' | tr '\n' ' ')"
    if [ -n "$ips" ]; then
      ready="$(kubectl -n "$NS" exec blobcache-builder -- bash -c "
        ok=0
        for ip in $ips; do
          curl -sf -m 2 \"http://\$ip:7773/metrics\" -o /dev/null 2>/dev/null && ok=\$((ok+1))
        done
        echo \$ok
      " 2>/dev/null || echo 0)"
      if [ "$ready" -ge "$expected" ] && [ "$expected" -gt 0 ]; then
        echo "==[ wait_http_ready: $ready/$expected pods healthy at $(date -u +%FT%TZ) ]=="
        return 0
      fi
    fi
    sleep "$poll_s"
  done
  echo "==[ wait_http_ready: TIMEOUT - $ready/$expected pods responded after ${timeout_s}s ]==" >&2
  return 1
}

# verify_hydrate: parse hydrate.json and abort the trial if any peer reported
# errors or under-fetched its assigned shard. Catches the silent-loss class of
# bug where coordinator records errs>0 but rc=0, leaving chunks missing from
# the cluster bloom. Returns 0 if healthy, 1 otherwise (caller decides whether
# to skip the trial vs continue with PASS1 polluted by blob fallback).
verify_hydrate() {
  local hydrate_json="$1" tag="$2"
  if [ ! -s "$hydrate_json" ]; then
    echo "==[ verify_hydrate $tag: missing $hydrate_json ]==" >&2
    return 1
  fi
  python3 - "$hydrate_json" "$tag" <<'PY'
import json, sys
hyd_path, tag = sys.argv[1], sys.argv[2]
d = json.load(open(hyd_path))
peers = d.get('peers', [])
bad = []
for p in peers:
    a = p.get('assigned_chunks', 0)
    f = p.get('fetched', 0)
    e = p.get('errors', [])
    if e or (a > 0 and f != a):
        bad.append((p.get('node_id', '?'), a, f, len(e), (e[:1] or [''])[0]))
print(f"verify_hydrate {tag}: {len(peers)} peers, {len(bad)} unhealthy")
for n, a, f, ec, msg in bad:
    print(f"  UNHEALTHY {n}: assigned={a} fetched={f} errs={ec} first={msg!r}")
sys.exit(1 if bad else 0)
PY
}

# Args: <tag> <values-file> <run_gather:0|1>
trial() {
  local tag="$1" values="$2" run_gather="$3"
  reinstall "$values"

  local start_utc end_utc
  start_utc="$(date -u +%FT%TZ)"
  echo "==[ TRIAL $tag start=$start_utc ]=="

  local hydrate_mode="default"
  [ "$run_gather" = "1" ] && hydrate_mode="broadcast"

  ( cd "$REPO_ROOT" && \
    OUT_DIR="$OUT_DIR" \
    PATH_PREFIX="$PATH_PREFIX" \
    HYDRATE_MODE="$hydrate_mode" \
    POST_HYDRATE_SLEEP_S=30 \
    RUN_GATHER="$run_gather" POST_GATHER_SLEEP_S=30 \
    RUN_PASS2=1 POST_PASS1_SLEEP_S=10 \
    RUN_TAG="$tag" \
    benchmarks/diag-run.sh ) || echo "==[ TRIAL $tag: diag-run.sh exited non-zero ]=="

  end_utc="$(date -u +%FT%TZ)"

  local hydrate_json="$OUT_DIR/${tag}-hydrate.json"
  local hyd_status="ok"
  if ! verify_hydrate "$hydrate_json" "$tag"; then
    hyd_status="FAIL"
    echo "==[ TRIAL $tag: hydrate verification FAILED - results polluted, see hydrate.json ]==" >&2
  fi

  # Extract durations from the run log.
  local log="$OUT_DIR/${tag}-run.log"
  local hyd gat p1 p2
  hyd="$(grep -oE 'HYDRATE_END=[^ ]+ rc=0 wall=[0-9.]+s' "$log" 2>/dev/null | grep -oE 'wall=[0-9.]+' | head -1 | cut -d= -f2 || echo '')"
  gat="$(grep -oE 'GATHER_END=[^ ]+ rc=0 wall=[0-9.]+s'  "$log" 2>/dev/null | grep -oE 'wall=[0-9.]+' | head -1 | cut -d= -f2 || echo '')"
  p1="$(grep -oE 'READ_PASS1_END=[^ ]+ wall=[0-9.]+s'    "$log" 2>/dev/null | grep -oE 'wall=[0-9.]+' | head -1 | cut -d= -f2 || echo '')"
  p2="$(grep -oE 'READ_PASS2_END=[^ ]+ wall=[0-9.]+s'    "$log" 2>/dev/null | grep -oE 'wall=[0-9.]+' | head -1 | cut -d= -f2 || echo '')"

  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$tag" "$start_utc" "$end_utc" "${hyd:--}" "${gat:--}" "${p1:--}" "${p2:--}" "$hyd_status" \
    | tee -a "$SUMMARY"
  echo "==[ TRIAL $tag end=$end_utc ]=="
}

echo "==[ SWEEP_START=$(date -u +%FT%TZ) image=${BLOBCACHE_IMAGE_TAG} out=$OUT_DIR ]=="

# C1: cacheOff + sharded + gather, 2 trials.
trial c1-cacheoff-gather-t1 "$OUT_DIR/values-off.yaml" 1
trial c1-cacheoff-gather-t2 "$OUT_DIR/values-off.yaml" 1

# C2: cacheOff + sharded (no gather), 2 trials.
trial c2-cacheoff-shard-t1  "$OUT_DIR/values-off.yaml" 0
trial c2-cacheoff-shard-t2  "$OUT_DIR/values-off.yaml" 0

# C3: cacheOn + sharded (no gather), 2 trials.
trial c3-cacheon-shard-t1   "$OUT_DIR/values-on.yaml"  0
trial c3-cacheon-shard-t2   "$OUT_DIR/values-on.yaml"  0

echo "==[ SWEEP_END=$(date -u +%FT%TZ) summary=$SUMMARY ]=="
echo
column -t -s $'\t' "$SUMMARY"
