#!/usr/bin/env bash
set -euo pipefail

# blobcache end-to-end benchmark.
#
# Measures wall-clock time to read varying byte-counts from FUSE-mounted blob
# files in three scenarios:
#   cold       - first read on a node with no cache (origin: Azure Blob)
#   warm-local - re-read same file on same node    (origin: local NVMe cache)
#   warm-peer  - read same file on a different node (origin: peer chunk fetch)
#
# Also runs a singleflight stress test: N concurrent readers of the same
# uncached chunk on one node; expect blob_fetches to grow by ~1 chunk-set,
# not N x chunk-set.
#
# Output: machine-readable TSV on stdout. Wrap with bench_to_md.sh for markdown.
#
# Requires: kubectl context = mycluster, namespace blobcache, 3 blobcached pods,
# storage-access ON, mount name "test", blob path azcp-bench/dl-src-big/*.

NS=${NS:-blobcache}
MOUNT_DIR=${MOUNT_DIR:-/mnt/nvme/blobcache-mnt/test}
BLOB_DIR=${BLOB_DIR:-azcp-bench/dl-src-big}
SIZES_MIB=(1 4 16 64 256 1024)

PODS=($(kubectl -n "$NS" get pods -l app=blobcached -o jsonpath='{.items[*].metadata.name}'))
if [ "${#PODS[@]}" -lt 2 ]; then
  echo "need >=2 blobcached pods, found ${#PODS[@]}" >&2; exit 1
fi
P0=${PODS[0]}
P1=${PODS[1]}
echo "# pods: P0=$P0  P1=$P1  others=${PODS[*]:2}" >&2

kx() { kubectl -n "$NS" exec "$1" -- "${@:2}"; }

read_bytes() {
  local pod=$1 file=$2 mib=$3
  kx "$pod" bash -c "
    sync; echo 1 > /proc/sys/vm/drop_caches 2>/dev/null || true
    t0=\$(date +%s.%N)
    dd if='$MOUNT_DIR/$file' of=/dev/null bs=1M count=$mib status=none iflag=fullblock
    t1=\$(date +%s.%N)
    awk -v a=\$t0 -v b=\$t1 'BEGIN{printf \"%.3f\", b-a}'
  "
}

get_metric() {
  local pod=$1 name=$2
  kx "$pod" curl -sS http://127.0.0.1:7773/metrics \
    | awk -v m="$name" '$1==m{print $2; exit}'
}

printf 'scenario\tsize_mib\tpod\tfile\twall_s\tmib_per_s\tblob_fetches_d\tpeer_fetches_ok_d\tcache_hits_d\tsingleflight_waits_d\n'

file_idx=0
files=(file_a.bin file_a_2.bin file_a_3.bin file_a_4.bin file_a_5.bin file_a_6.bin
       file_b.bin file_b_2.bin file_b_3.bin file_b_4.bin file_b_5.bin file_b_6.bin
       file_c.bin file_c_2.bin file_c_3.bin file_c_4.bin file_c_5.bin file_c_6.bin
       file_d.bin file_d_2.bin file_d_3.bin file_d_4.bin file_d_5.bin file_d_6.bin)

bench_one() {
  local scenario=$1 pod=$2 file=$3 mib=$4
  local b0 p0 c0 s0 b1 p1 c1 s1 wall mibs
  b0=$(get_metric "$pod" blobcache_blob_fetches_total)
  p0=$(get_metric "$pod" blobcache_peer_fetches_ok_total)
  c0=$(get_metric "$pod" blobcache_cache_hits_total)
  s0=$(get_metric "$pod" blobcache_singleflight_waits_total)
  wall=$(read_bytes "$pod" "$file" "$mib")
  b1=$(get_metric "$pod" blobcache_blob_fetches_total)
  p1=$(get_metric "$pod" blobcache_peer_fetches_ok_total)
  c1=$(get_metric "$pod" blobcache_cache_hits_total)
  s1=$(get_metric "$pod" blobcache_singleflight_waits_total)
  mibs=$(awk -v m="$mib" -v w="$wall" 'BEGIN{printf "%.1f", (w>0)?m/w:0}')
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$scenario" "$mib" "$pod" "$file" "$wall" "$mibs" \
    "$((b1-b0))" "$((p1-p0))" "$((c1-c0))" "$((s1-s0))"
}

# === cold + warm-local + warm-peer cycle, varying sizes ===
for mib in "${SIZES_MIB[@]}"; do
  f="${files[$file_idx]}"; file_idx=$((file_idx+1))
  bench_one cold       "$P0" "$BLOB_DIR/$f" "$mib"
  bench_one warm-local "$P0" "$BLOB_DIR/$f" "$mib"
  bench_one warm-peer  "$P1" "$BLOB_DIR/$f" "$mib"
done

# === singleflight stress: N concurrent readers, same uncached file ===
SF_FILE="$BLOB_DIR/${files[$file_idx]}"; file_idx=$((file_idx+1))
SF_MIB=64
SF_N=8
b0=$(get_metric "$P0" blobcache_blob_fetches_total)
s0=$(get_metric "$P0" blobcache_singleflight_waits_total)
t0=$(date +%s.%N)
pids=()
for i in $(seq 1 $SF_N); do
  ( kx "$P0" bash -c "dd if='$MOUNT_DIR/$SF_FILE' of=/dev/null bs=1M count=$SF_MIB status=none iflag=fullblock" ) &
  pids+=($!)
done
for pid in "${pids[@]}"; do wait $pid; done
t1=$(date +%s.%N)
wall=$(awk -v a=$t0 -v b=$t1 'BEGIN{printf "%.3f", b-a}')
b1=$(get_metric "$P0" blobcache_blob_fetches_total)
s1=$(get_metric "$P0" blobcache_singleflight_waits_total)
mibs=$(awk -v m="$SF_MIB" -v w="$wall" -v n=$SF_N 'BEGIN{printf "%.1f", (w>0)?(m*n)/w:0}')
printf 'singleflight\t%s\t%s\t%s\t%s\t%s\t%s\t-\t-\t%s\n' \
  "$SF_MIB" "$P0" "$SF_FILE" "$wall" "$mibs" "$((b1-b0))" "$((s1-s0))"
