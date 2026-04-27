#!/usr/bin/env bash
# v2.9.0 workers sweep: fixed chunk_size=4M, conc=32, vary azure.workers.
# Storage public access must be enabled before invocation; this script
# does not toggle it.

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$HERE/.." && pwd)"
[[ -f "$REPO_ROOT/.env" ]] && set -a && . "$REPO_ROOT/.env" && set +a

: "${STORAGE_ACCOUNT:?set STORAGE_ACCOUNT in .env}"
: "${CONTAINER:?set CONTAINER in .env}"
: "${MODEL_PREFIX:?set MODEL_PREFIX in .env}"
: "${GOSSIP_SEEDS:?set GOSSIP_SEEDS (comma-separated http://IP:7771 list) in .env}"

NS="${NAMESPACE:-blobcache}"
SWEEP="$HERE/sweep-workers"
BIN=/tmp/blobcached.aarch64
mkdir -p "$SWEEP"

# GOSSIP_SEEDS is "url1,url2,url3"; render as a TOML string array.
seeds_toml() { python3 -c 'import os,sys;print(",".join("\""+s.strip()+"\"" for s in os.environ["GOSSIP_SEEDS"].split(",") if s.strip()))'; }
SEEDS_TOML="$(seeds_toml)"

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
seeds = [$SEEDS_TOML]
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
account = "$STORAGE_ACCOUNT"
container = "$CONTAINER"
prefix = "$MODEL_PREFIX"
EOF
}

push_cfg() {
  local workers=$1
  local tmp; tmp=$(mktemp -d)
  while IFS=$'\t' read -r pod ip; do
    [[ -z "$pod" ]] && continue
    render_cfg "$pod" "$ip" "$workers" "$tmp/${pod}.toml"
    (
      kubectl -n "$NS" cp "$BIN" "$pod:/opt/blobcache/blobcached.new" >/dev/null 2>&1
      kubectl -n "$NS" cp "$tmp/${pod}.toml" "$pod:/opt/blobcache/blobcached.toml.new" >/dev/null 2>&1
      kubectl -n "$NS" exec "$pod" -- bash -c \
        'mv -f /opt/blobcache/blobcached.new /opt/blobcache/blobcached && chmod +x /opt/blobcache/blobcached && mv -f /opt/blobcache/blobcached.toml.new /opt/blobcache/blobcached.toml' \
        >/dev/null 2>&1
    ) &
  done < <(current_pods)
  wait
  rm -rf "$tmp"
}

wipe_restart() {
  while IFS=$'\t' read -r pod ip; do
    [[ -z "$pod" ]] && continue
    kubectl -n "$NS" exec "$pod" -- bash -c \
      'rm -rf /mnt/nvme/blobcache-cache/* 2>/dev/null; kill -TERM 1' >/dev/null 2>&1 &
  done < <(current_pods)
  wait
  sleep 12
}

wait_ready() {
  local n=$1
  for i in $(seq 1 60); do
    sleep 3
    local ready
    ready=$(kubectl -n "$NS" get pods -l app=blobcached \
      -o jsonpath='{range .items[*]}{.status.containerStatuses[0].ready}{"\n"}{end}' 2>/dev/null \
      | grep -c '^true$')
    [[ "$ready" -ne "$n" ]] && continue
    local converged=1
    while IFS=$'\t' read -r pod _; do
      [[ -z "$pod" ]] && continue
      local m
      m=$(kubectl -n "$NS" exec "$pod" -- curl -sS --max-time 3 localhost:7773/metrics 2>/dev/null \
        | awk '$1=="blobcache_cluster_members_alive"{print $2}')
      if [[ "$m" != "$n" ]]; then converged=0; break; fi
    done < <(current_pods)
    [[ "$converged" -eq 1 ]] && { echo "[$(date +%H:%M:%S)] cluster ready ($n alive)"; return 0; }
  done
  echo "[$(date +%H:%M:%S)] WARN: not converged"
  return 1
}

snap() {
  local out=$1 label=$2
  mkdir -p "$out"
  while IFS=$'\t' read -r pod ip; do
    [[ -z "$pod" ]] && continue
    kubectl -n "$NS" exec "$pod" -- curl -sS --max-time 5 localhost:7773/metrics 2>/dev/null \
      > "$out/${label}_${pod}.prom" &
  done < <(current_pods)
  wait
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
  local n; n=$(current_pods | wc -l)
  wait_ready "$n" || { echo "  no convergence; skipping"; return 1; }
  snap "$out" pre
  hydrate "$out"
  snap "$out" post
  current_pods > "$out/pods.txt"
  echo "[$(date +%H:%M:%S)] === $label DONE ==="
}

run_one "w1" 1
run_one "w2" 2
run_one "w4" 4
run_one "w8" 8
echo "[$(date +%H:%M:%S)] ALL DONE"
