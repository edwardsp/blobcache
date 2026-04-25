#!/usr/bin/env bash
# Hydrate + parallel-read matrix benchmark for N in {1,2,4,8,16}.
#
# For each N:
#   1. Label N nodes (subset that includes the 3 seed-IP holders), unlabel rest.
#   2. Wait for daemonset to converge to N Running pods + gossip members_alive==N.
#   3. On every pod: wipe NVMe cache + SIGTERM PID 1 to restart container with
#      empty in-memory state (emptyDir survives container restart, binary intact).
#   4. Wait for daemons up + cluster reconverged.
#   5. Snapshot pre-test metrics on every pod.
#   6. HYDRATE: POST /hydrate on a coord pod for entire mount.prefix.
#   7. PARALLEL READ: every pod simultaneously cats all 163 .safetensors files
#      to /dev/null. Per-pod wall time recorded.
#   8. Snapshot post-test metrics, save to bench/results/N${N}/.
#
# Designed to be re-runnable. Caches are fully wiped between Ns.
set -euo pipefail

NS="blobcache"
RESULTS_DIR="$(cd "$(dirname "$0")" && pwd)/results"
# Cluster bootstrap invariant: the 3 hardcoded seed IPs (10.0.0.5/6/7)
# live on these specific nodes. They MUST remain labeled at every N>=1
# or no pod can join gossip. Order matches the seed list in pod configs.
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
    -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.status.podIP}{"\n"}{end}'
}

wipe_and_restart() {
  log "wiping caches + SIGTERM PID 1 on all pods"
  while IFS=$'\t' read -r pod ip; do
    kubectl -n "$NS" exec "$pod" -- bash -c \
      'rm -rf /mnt/nvme/blobcache-cache/* 2>/dev/null; kill -TERM 1' >/dev/null 2>&1 &
  done < <(current_pods)
  wait || true
  sleep 8
}

wait_cluster_ready() {
  local n=$1
  log "waiting for daemons + cluster gossip convergence (target N=$n)"
  for _ in {1..120}; do
    sleep 3
    local ready
    ready=$(kubectl -n "$NS" get pods -l app=blobcached \
      -o jsonpath='{range .items[*]}{.status.containerStatuses[0].ready}{"\n"}{end}' 2>/dev/null \
      | grep -c '^true$' || true)
    [[ "$ready" -ne "$n" ]] && continue
    local converged=1
    while IFS=$'\t' read -r pod _; do
      local m
      m=$(kubectl -n "$NS" exec "$pod" -- curl -sS --max-time 3 localhost:7773/metrics 2>/dev/null \
        | awk '$1=="blobcache_cluster_members_alive"{print $2}')
      if [[ "$m" != "$n" ]]; then converged=0; break; fi
    done < <(current_pods)
    [[ $converged -eq 1 ]] && { log "cluster ready: $n alive on every pod"; return 0; }
  done
  log "WARN: cluster did not converge to N=$n"
  return 1
}

snapshot_metrics() {
  local out=$1 label=$2
  mkdir -p "$out"
  while IFS=$'\t' read -r pod ip; do
    kubectl -n "$NS" exec "$pod" -- curl -sS --max-time 5 localhost:7773/metrics 2>/dev/null \
      > "$out/${label}_${pod}.prom" &
  done < <(current_pods)
  wait || true
}

run_hydrate() {
  local out=$1
  local coord
  coord=$(current_pods | head -1 | awk '{print $1}')
  log "HYDRATE coord=$coord (full mount.prefix, recursive)"
  local t0=$EPOCHREALTIME
  kubectl -n "$NS" exec "$coord" -- curl -sS --max-time 7200 -X POST localhost:7773/hydrate \
    -H 'content-type: application/json' \
    -d '{"mount":"deepseek","path":"","recursive":true}' \
    > "$out/hydrate.json" 2>&1 || true
  local t1=$EPOCHREALTIME
  awk -v t0="$t0" -v t1="$t1" 'BEGIN{printf "%.3f\n", t1-t0}' > "$out/hydrate.wall_s"
  log "hydrate wall = $(cat "$out/hydrate.wall_s") s"
}

run_parallel_read() {
  local out=$1
  log "PARALLEL READ: every pod cats all 163 safetensors files"
  mkdir -p "$out/read"
  local pids=()
  while IFS=$'\t' read -r pod ip; do
    (
      local t0=$EPOCHREALTIME
      kubectl -n "$NS" exec "$pod" -- bash -c '
        set -e
        bytes=0
        for f in /mnt/blobcache/deepseek/*.safetensors; do
          # use cat to stream-read; no kernel readahead games
          b=$(cat "$f" | wc -c)
          bytes=$((bytes + b))
        done
        echo "$bytes"
      ' > "$out/read/${pod}.bytes" 2> "$out/read/${pod}.err" || echo FAILED > "$out/read/${pod}.bytes"
      local t1=$EPOCHREALTIME
      awk -v t0="$t0" -v t1="$t1" 'BEGIN{printf "%.3f\n", t1-t0}' > "$out/read/${pod}.wall_s"
    ) &
    pids+=($!)
  done < <(current_pods)
  for p in "${pids[@]}"; do wait "$p" 2>/dev/null || true; done
  log "parallel read complete; per-pod walls in $out/read/"
}

main() {
  local Ns=("${@:-16 8 4 2 1}")
  for N in "${Ns[@]}"; do
    local out="$RESULTS_DIR/N${N}"
    rm -rf "$out"; mkdir -p "$out"
    log "================ N=$N START ================"

    scale_to "$N"
    wipe_and_restart
    wait_cluster_ready "$N" || { log "skip N=$N (no convergence)"; continue; }

    snapshot_metrics "$out" pre
    run_hydrate "$out"
    snapshot_metrics "$out" post_hydrate

    sleep 15
    run_parallel_read "$out"
    snapshot_metrics "$out" post_read

    current_pods > "$out/pods.txt"
    log "================ N=$N DONE  =================="
  done
}

main "$@"
