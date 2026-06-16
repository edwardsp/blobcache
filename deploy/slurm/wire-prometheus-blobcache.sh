#!/usr/bin/env bash
# Wire blobcache (:7773) into this node's Prometheus and tag series with node=<hostname>.
# Idempotent. Run as root (sudo bash wire-prometheus-blobcache.sh).
set -euo pipefail
PROM=/opt/prometheus/prometheus.yml
H=$(hostname)

if grep -q 'job_name: blobcache' "$PROM"; then
  echo "[$H] blobcache scrape job already present"
else
  cat >> "$PROM" <<EOF
  - job_name: blobcache
    static_configs:
      - targets: ["127.0.0.1:7773"]
        labels:
          node: $H
EOF
  echo "[$H] appended blobcache scrape job (node=$H)"
fi

# Reload Prometheus (SIGHUP); fall back to restart if not running.
if pgrep -x prometheus >/dev/null; then
  kill -HUP "$(pgrep -x prometheus)"
  echo "[$H] sent SIGHUP to prometheus"
else
  systemctl restart prometheus 2>/dev/null || true
  echo "[$H] prometheus was not running; attempted restart"
fi

sleep 5
echo "[$H] active targets:"
curl -s http://127.0.0.1:9090/api/v1/targets \
  | python3 -c 'import sys,json
d=json.load(sys.stdin)
for t in d["data"]["activeTargets"]:
    print("   ", t["labels"].get("job"), t["health"], t["scrapeUrl"], t.get("lastError",""))'
