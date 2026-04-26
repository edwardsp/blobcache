#!/usr/bin/env bash
# v2.9.0 N=1 workers sweep: 1 pod, vary azure.workers to validate that
# the multi-runtime pool actually breaks the single-tokio-runtime ceiling.
# At N=1 the storage account is not the bottleneck; per-node throughput
# directly reflects per-process tokio/reqwest scaling.

HERE="$(cd "$(dirname "$0")" && pwd)"
NS="blobcache"
SWEEP="$HERE/sweep-workers-n1"
BIN=/tmp/blobcached.aarch64
SEEDS_LABEL_KEEP=(vmss00001h vmss000010 vmss00001g)
mkdir -p "$SWEEP"

scale_to_one() {
  local keep="${SEEDS_LABEL_KEEP[0]}"
  for n in $(kubectl get nodes -l agentpool=gb300 -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}'); do
    if [[ "$n" == *"$keep" ]]; then
      kubectl label --overwrite node "$n" blobcache.test/enabled=true >/dev/null
    else
      kubectl label node "$n" blobcache.test/enabled- >/dev/null 2>&1 || true
    fi
  done
  for _ in {1..60}; do
    local got
    got=$(kubectl -n "$NS" get pods -l app=blobcached --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l)
    [[ "$got" -eq "1" ]] && break
    sleep 2
  done
  echo "[$(date +%H:%M:%S)] pod count = $(kubectl -n "$NS" get pods -l app=blobcached --field-selector=status.phase=Running --no-headers | wc -l) / 1"
}

current_pods() {
  kubectl -n "$NS" get pods -l app=blobcached --field-selector=status.phase=Running \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.status.podIP}{"\n"}{end}'
}

render_cfg() {
  local pod=$1 ip=$2 workers=$3 out=$4
  cat > "$out" <<EOF
node_id = "node-${pod#blobcached-}"

[cache]
dir = "/mnt/nvme/blobcache-cache"
max_bytes = 483183820800
chunk_size = 4194304

[azure]
pool_max_idle_per_host = 512
workers = $workers

[cluster]
bind = "0.0.0.0:7771"
seeds = ["http://$ip:7771"]
advertise = "http://$ip:7771"

[transport]
kind = "rdma"
bind = "0.0.0.0:7772"
advertise = ["http://$ip:7772"]
chunk_concurrency = 32
peer_concurrency = 8
bloom_bits = 8388608

[stats]
bind = "0.0.0.0:7773"

[[mounts]]
name = "deepseek"
mountpoint = "/mnt/blobcache/deepseek"
account = "myaccount"
container = "models"
prefix = "models/test-prefix/"
EOF
}

push_cfg() {
  local workers=$1
  local tmp; tmp=$(mktemp -d)
  while IFS=$'\t' read -r pod ip; do
    [[ -z "$pod" ]] && continue
    render_cfg "$pod" "$ip" "$workers" "$tmp/${pod}.toml"
    kubectl -n "$NS" cp "$BIN" "$pod:/opt/blobcache/blobcached.new" >/dev/null 2>&1
    kubectl -n "$NS" cp "$tmp/${pod}.toml" "$pod:/opt/blobcache/blobcached.toml.new" >/dev/null 2>&1
    kubectl -n "$NS" exec "$pod" -- bash -c \
      'mv -f /opt/blobcache/blobcached.new /opt/blobcache/blobcached && chmod +x /opt/blobcache/blobcached && mv -f /opt/blobcache/blobcached.toml.new /opt/blobcache/blobcached.toml' \
      >/dev/null 2>&1
  done < <(current_pods)
  rm -rf "$tmp"
}

wipe_restart() {
  while IFS=$'\t' read -r pod ip; do
    [[ -z "$pod" ]] && continue
    kubectl -n "$NS" exec "$pod" -- bash -c \
      'rm -rf /mnt/nvme/blobcache-cache/* 2>/dev/null; kill -TERM 1' >/dev/null 2>&1
  done < <(current_pods)
  sleep 12
}

wait_ready() {
  for i in $(seq 1 120); do
    sleep 3
    local ready
    ready=$(kubectl -n "$NS" get pods -l app=blobcached \
      -o jsonpath='{range .items[*]}{.status.containerStatuses[0].ready}{"\n"}{end}' 2>/dev/null \
      | grep -c '^true$')
    [[ "$ready" -eq "1" ]] && break
  done
  # Then actively probe the daemon's stats port; the readinessProbe alone
  # fires before apt-install completes inside the entrypoint, so we must
  # confirm the binary is actually serving HTTP.
  local pod
  pod=$(current_pods | head -1 | awk '{print $1}')
  [[ -z "$pod" ]] && { echo "[$(date +%H:%M:%S)] WARN: no pod"; return 1; }
  for i in $(seq 1 120); do
    if kubectl -n "$NS" exec "$pod" -- bash -c \
         'command -v curl >/dev/null && curl -sS --max-time 2 -o /dev/null -w "%{http_code}\n" localhost:7773/metrics 2>/dev/null' \
         2>/dev/null | grep -q '^200$'; then
      echo "[$(date +%H:%M:%S)] pod ready (stats port serving)"
      return 0
    fi
    sleep 3
  done
  echo "[$(date +%H:%M:%S)] WARN: stats port never came up"
  return 1
}

snap() {
  local out=$1 label=$2
  mkdir -p "$out"
  while IFS=$'\t' read -r pod ip; do
    [[ -z "$pod" ]] && continue
    kubectl -n "$NS" exec "$pod" -- curl -sS --max-time 5 localhost:7773/metrics 2>/dev/null \
      > "$out/${label}_${pod}.prom"
  done < <(current_pods)
}

hydrate() {
  local out=$1
  local coord
  coord=$(current_pods | head -1 | awk '{print $1}')
  echo "[$(date +%H:%M:%S)] HYDRATE coord=$coord"
  local t0=$EPOCHREALTIME
  kubectl -n "$NS" exec "$coord" -- curl -sS --max-time 7200 -X POST localhost:7773/hydrate \
    -H 'content-type: application/json' \
    -d '{"mount":"deepseek","path":"","recursive":true}' \
    > "$out/hydrate.json" 2>&1
  local t1=$EPOCHREALTIME
  awk -v t0="$t0" -v t1="$t1" 'BEGIN{printf "%.3f\n", t1-t0}' > "$out/hydrate.wall_s"
  echo "[$(date +%H:%M:%S)] hydrate wall = $(cat "$out/hydrate.wall_s") s"
}

run_one() {
  local label=$1 workers=$2
  local out="$SWEEP/$label"
  rm -rf "$out"; mkdir -p "$out"
  echo "[$(date +%H:%M:%S)] === $label : workers=$workers ==="
  push_cfg "$workers"
  echo "[$(date +%H:%M:%S)] pushed; wiping + restarting"
  wipe_restart
  wait_ready || { echo "  no readiness; skipping"; return 1; }
  snap "$out" pre
  hydrate "$out"
  snap "$out" post
  current_pods > "$out/pods.txt"
  echo "[$(date +%H:%M:%S)] === $label DONE ==="
}

scale_to_one
sleep 20
run_one "w1" 1
run_one "w2" 2
run_one "w4" 4
run_one "w8" 8
run_one "w16" 16
echo "[$(date +%H:%M:%S)] ALL DONE"
