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
printf 'tag\tstart_utc\tend_utc\thydrate_s\tgather_s\tpass1_s\tpass2_s\n' > "$SUMMARY"

reinstall() {
  local values="$1"
  echo "==[ helm uninstall + install with $(basename "$values") ]=="
  helm -n "$NS" uninstall "$RELEASE" --wait --timeout 5m >/dev/null 2>&1 || true
  helm -n "$NS" install   "$RELEASE" "$REPO_ROOT/$CHART" -f "$values" --wait --timeout 10m
  kubectl --request-timeout=15s -n "$NS" rollout status ds/blobcache-blobcached --timeout=10m
  # Brief settle for gossip + bloom warmup.
  sleep 15
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

  # Extract durations from the run log.
  local log="$OUT_DIR/${tag}-run.log"
  local hyd gat p1 p2
  hyd="$(grep -oE 'HYDRATE_END=[^ ]+ rc=0 wall=[0-9.]+s' "$log" 2>/dev/null | grep -oE 'wall=[0-9.]+' | head -1 | cut -d= -f2 || echo '')"
  gat="$(grep -oE 'GATHER_END=[^ ]+ rc=0 wall=[0-9.]+s'  "$log" 2>/dev/null | grep -oE 'wall=[0-9.]+' | head -1 | cut -d= -f2 || echo '')"
  p1="$(grep -oE 'READ_PASS1_END=[^ ]+ wall=[0-9.]+s'    "$log" 2>/dev/null | grep -oE 'wall=[0-9.]+' | head -1 | cut -d= -f2 || echo '')"
  p2="$(grep -oE 'READ_PASS2_END=[^ ]+ wall=[0-9.]+s'    "$log" 2>/dev/null | grep -oE 'wall=[0-9.]+' | head -1 | cut -d= -f2 || echo '')"

  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$tag" "$start_utc" "$end_utc" "${hyd:--}" "${gat:--}" "${p1:--}" "${p2:--}" \
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
