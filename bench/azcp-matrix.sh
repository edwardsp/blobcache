#!/usr/bin/env bash
# azcp baseline: how fast does the same data flow into the same NVMe when we
# bypass blobcache entirely and use a tuned per-node azcp invocation?
#
# Per-N protocol (N in {1,2,4,8,16}):
#   1. Pick N pods (always including seed-IP holders, same as bench/matrix.sh).
#   2. On each picked pod i: rm -rf /mnt/nvme/azcp-test/* to clear NVMe.
#   3. Launch all N pods simultaneously, each running:
#        azcp copy <src> /mnt/nvme/azcp-test/ \
#          --recursive --shard i/N \
#          --workers 8 --concurrency 32 --block-size 16777216 --no-progress
#   4. Wait for all N to finish, record per-pod wall + total bytes.
#   5. Aggregate throughput = sum(bytes) / max(walls).
#
# Same hardware, same dataset, same hostNetwork pods as bench/matrix.sh -
# the only thing that changes is the fetcher.
set -euo pipefail

NS="blobcache"
RESULTS_DIR="$(cd "$(dirname "$0")" && pwd)/results-azcp"
DEST="/mnt/nvme/azcp-test"
SRC="https://myaccount.blob.core.windows.net/models/models/test-prefix/"
AZCP="/opt/blobcache/azcp"
CLIENT_ID="00000000-0000-0000-0000-000000000000"

# Same seed-IP nodes as bench/matrix.sh - keep cluster integrity intact even
# while we're not actually using blobcached for these tests, so we can A/B
# back to the hydrate matrix without re-labeling.
SEEDS_LABEL_KEEP=(
  "aks-cluster-00000000-vmss00001h"
  "aks-cluster-00000000-vmss000010"
  "aks-cluster-00000000-vmss00001g"
)
ALL_NODES=(
  aks-cluster-00000000-vmss000010 aks-cluster-00000000-vmss000011
  aks-cluster-00000000-vmss000012 aks-cluster-00000000-vmss000013
  aks-cluster-00000000-vmss000014 aks-cluster-00000000-vmss000015
  aks-cluster-00000000-vmss000016 aks-cluster-00000000-vmss000018
  aks-cluster-00000000-vmss000019 aks-cluster-00000000-vmss00001b
  aks-cluster-00000000-vmss00001c aks-cluster-00000000-vmss00001d
  aks-cluster-00000000-vmss00001e aks-cluster-00000000-vmss00001f
  aks-cluster-00000000-vmss00001g aks-cluster-00000000-vmss00001h
)

log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*" >&2; }

pick_nodes() {
  local n=$1
  local picked=("${SEEDS_LABEL_KEEP[@]}")
  if [[ $n -lt 3 ]]; then
    picked=("${SEEDS_LABEL_KEEP[@]:0:$n}")
  else
    for node in "${ALL_NODES[@]}"; do
      [[ ${#picked[@]} -ge $n ]] && break
      local already=0
      for p in "${picked[@]}"; do [[ "$p" == "$node" ]] && already=1 && break; done
      [[ $already -eq 0 ]] && picked+=("$node")
    done
  fi
  printf '%s\n' "${picked[@]}"
}

scale_to() {
  local n=$1
  log "scaling to N=$n nodes"
  mapfile -t keep < <(pick_nodes "$n")
  declare -A keep_set
  for k in "${keep[@]}"; do keep_set[$k]=1; done
  for node in "${ALL_NODES[@]}"; do
    if [[ -n "${keep_set[$node]:-}" ]]; then
      kubectl label --overwrite node "$node" blobcache.test/enabled=true >/dev/null
    else
      kubectl label node "$node" blobcache.test/enabled- >/dev/null 2>&1 || true
    fi
  done
  for _ in {1..60}; do
    local got
    got=$(kubectl -n "$NS" get pods -l app=blobcached --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l)
    [[ "$got" -eq "$n" ]] && break
    sleep 2
  done
  log "pod count = $(kubectl -n "$NS" get pods -l app=blobcached --field-selector=status.phase=Running --no-headers | wc -l) / $n"
}

current_pods() {
  kubectl -n "$NS" get pods -l app=blobcached --field-selector=status.phase=Running \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}'
}

ensure_azcp() {
  log "verifying azcp present on all current pods"
  while read -r pod; do
    [[ -z "$pod" ]] && continue
    if ! kubectl -n "$NS" exec "$pod" -- test -x "$AZCP" 2>/dev/null; then
      log "  pushing azcp to $pod"
      kubectl -n "$NS" cp /tmp/azcp.aarch64 "$pod:/opt/blobcache/azcp.new" 2>/dev/null
      kubectl -n "$NS" exec "$pod" -- bash -c 'mv -f /opt/blobcache/azcp.new /opt/blobcache/azcp && chmod +x /opt/blobcache/azcp' 2>/dev/null
    fi
  done < <(current_pods)
}

clean_dest() {
  log "wiping $DEST on all pods"
  while read -r pod; do
    [[ -z "$pod" ]] && continue
    kubectl -n "$NS" exec "$pod" -- bash -c "rm -rf $DEST && mkdir -p $DEST" >/dev/null 2>&1 &
  done < <(current_pods)
  wait || true
}

run_shard() {
  local n=$1
  log "launching N=$n shards (workers=1 conc=32 block=16M, --shard i/N)"
  mkdir -p "$RESULTS_DIR/N$n"
  local i=0
  local pids=()
  while read -r pod; do
    [[ -z "$pod" ]] && continue
    local idx=$i
    (
      set +e
      local t0=$EPOCHREALTIME
      kubectl -n "$NS" exec "$pod" -- bash -c "
        AZURE_CLIENT_ID=$CLIENT_ID $AZCP copy '$SRC' $DEST/ \
          --recursive --shard $idx/$n \
          --workers 1 --concurrency 32 --block-size 16777216 \
          --no-progress 2>&1
      " > "$RESULTS_DIR/N$n/${pod}.log" 2>&1
      local rc=$?
      local t1=$EPOCHREALTIME
      awk -v t0="$t0" -v t1="$t1" -v rc="$rc" \
        'BEGIN{printf "%.3f %d\n", t1-t0, rc}' > "$RESULTS_DIR/N$n/${pod}.wall_rc"
      local b
      b=$(kubectl -n "$NS" exec "$pod" -- bash -c "du -sb $DEST 2>/dev/null | awk '{print \$1}'" 2>/dev/null)
      echo "${b:-0}" > "$RESULTS_DIR/N$n/${pod}.bytes"
    ) &
    pids+=($!)
    i=$((i+1))
  done < <(current_pods)
  for p in "${pids[@]}"; do wait "$p" 2>/dev/null || true; done
  log "all shards done; per-pod walls in $RESULTS_DIR/N$n/"
}

main() {
  mkdir -p "$RESULTS_DIR"
  local Ns
  if [[ $# -gt 0 ]]; then
    Ns=("$@")
  else
    Ns=(1 2 4 8 16)
  fi
  for N in "${Ns[@]}"; do
    log "================ azcp N=$N START ================"
    rm -rf "$RESULTS_DIR/N$N"; mkdir -p "$RESULTS_DIR/N$N"
    scale_to "$N"
    sleep 5
    ensure_azcp
    clean_dest
    run_shard "$N"
    current_pods > "$RESULTS_DIR/N$N/pods.txt"
    log "================ azcp N=$N DONE  =================="
  done
  clean_dest
}

main "$@"
