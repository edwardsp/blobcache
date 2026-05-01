#!/usr/bin/env bash
# render-overlay.sh - substitute placeholders in a values-*.tmpl from env vars.
#
# Usage:
#   BLOBCACHE_CLIENT_ID=<uuid> \
#   BLOBCACHE_ACCOUNT=<account> \
#   BLOBCACHE_SEED_1=<ip> BLOBCACHE_SEED_2=<ip> BLOBCACHE_SEED_3=<ip> \
#   BLOBCACHE_IMAGE_TAG=sha-<short>-arm64 \
#     ./render-overlay.sh values-cache-peer-off.yaml.tmpl > /tmp/sweep/values-off.yaml
#
# Required env vars are validated; missing ones cause a hard exit so we don't
# emit YAML with literal __PLACEHOLDER__ tokens.
set -euo pipefail

TMPL="${1:?usage: render-overlay.sh <template>}"
[ -f "$TMPL" ] || { echo "render-overlay: template not found: $TMPL" >&2; exit 2; }

REQUIRED=(BLOBCACHE_CLIENT_ID BLOBCACHE_ACCOUNT BLOBCACHE_SEED_1 BLOBCACHE_SEED_2 BLOBCACHE_SEED_3 BLOBCACHE_IMAGE_TAG)
MISSING=()
for v in "${REQUIRED[@]}"; do
  if [ -z "${!v:-}" ]; then MISSING+=("$v"); fi
done
if [ "${#MISSING[@]}" -gt 0 ]; then
  echo "render-overlay: missing env vars: ${MISSING[*]}" >&2
  exit 3
fi

sed \
  -e "s|__CLIENT_ID__|${BLOBCACHE_CLIENT_ID}|g" \
  -e "s|__ACCOUNT__|${BLOBCACHE_ACCOUNT}|g" \
  -e "s|__SEED_1__|${BLOBCACHE_SEED_1}|g" \
  -e "s|__SEED_2__|${BLOBCACHE_SEED_2}|g" \
  -e "s|__SEED_3__|${BLOBCACHE_SEED_3}|g" \
  -e "s|__IMAGE_TAG__|${BLOBCACHE_IMAGE_TAG}|g" \
  "$TMPL"
