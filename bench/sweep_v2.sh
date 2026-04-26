#!/usr/bin/env bash
HERE="$(cd "$(dirname "$0")" && pwd)"
NS="blobcache"
SWEEP="$HERE/sweep"
BIN=/tmp/blobcached.aarch64
mkdir -p "$SWEEP"

current_pods() {
  kubectl -n "$NS" get pods -l app=blobcached --field-selector=status.phase=Running \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.status.podIP}{"\n"}{end}'
}

render_cfg() {
  local pod=$1 ip=$2 chunk=$3 conc=$4 out=$5
  cat > "$out" <<EOF
node_id = "node-${pod#blobcached-}"

[cache]
dir = "/mnt/nvme/blobcache-cache"
max_bytes = 483183820800
chunk_size = $chunk

[azure]
pool_max_idle_per_host = 512

[cluster]
bind = "0.0.0.0:7771"
seeds = ["http://10.0.0.5:7771","http://10.0.0.6:7771","http://10.0.0.7:7771"]
advertise = "http://$ip:7771"

[transport]
kind = "rdma"
bind = "0.0.0.0:7772"
advertise = ["http://$ip:7772"]
chunk_concurrency = $conc
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
  local chunk=$1 conc=$2
  local tmp; tmp=$(mktemp -d)
  while IFS=$'\t' read -r pod ip; do
    [[ -z "$pod" ]] && continue
    render_cfg "$pod" "$ip" "$chunk" "$conc" "$tmp/${pod}.toml"
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
  local label=$1 chunk=$2 conc=$3
  local out="$SWEEP/$label"
  rm -rf "$out"; mkdir -p "$out"
  echo "[$(date +%H:%M:%S)] === $label : chunk=$chunk conc=$conc ==="
  push_cfg "$chunk" "$conc"
  echo "[$(date +%H:%M:%S)] pushed; wiping + restarting"
  wipe_restart
  wait_ready 16 || { echo "  no convergence; skipping"; return 1; }
  snap "$out" pre
  hydrate "$out"
  snap "$out" post
  current_pods > "$out/pods.txt"
  echo "[$(date +%H:%M:%S)] === $label DONE ==="
}

run_one "16M-conc8"  16777216 8
run_one "16M-conc16" 16777216 16
run_one "4M-conc64"  4194304  64
echo "[$(date +%H:%M:%S)] ALL DONE"
