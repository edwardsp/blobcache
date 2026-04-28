#!/usr/bin/env bash
set -uo pipefail

# Wipe blobcached host-path NVMe cache on every node.
#
# Runs an in-place `rm -rf` inside the privileged nvme-raid-init DS pods
# (which have the host filesystem at /host). Use BEFORE every cold-start
# benchmark, then `kubectl rollout restart ds/<release>-blobcached` so
# daemons re-scan an empty cache directory.

NS=${NS:-blobcache}
DS_LABEL=${DS_LABEL:-app.kubernetes.io/component=nvme-raid}
HOST_CACHE=${HOST_CACHE:-/host/mnt/nvme/blobcache-cache}
LOG=${LOG:-/tmp/wipe.log}

exec >"$LOG" 2>&1

echo "WIPE_START=$(date -u +%FT%TZ) ns=$NS path=$HOST_CACHE"
mapfile -t PODS < <(kubectl --request-timeout=10s -n "$NS" get pods \
  -l "$DS_LABEL" --field-selector=status.phase=Running \
  -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}')

if [ "${#PODS[@]}" -eq 0 ]; then
  echo "no pods matched label '$DS_LABEL' in ns '$NS'" >&2
  exit 1
fi

for pod in "${PODS[@]}"; do
  (
    kubectl --request-timeout=30s -n "$NS" exec "$pod" -- sh -c "
      du -sh ${HOST_CACHE} 2>/dev/null
      rm -rf ${HOST_CACHE}
      mkdir -p ${HOST_CACHE}
      echo OK_$pod
    " 2>&1
  ) &
done
wait
echo "WIPE_END=$(date -u +%FT%TZ)"
