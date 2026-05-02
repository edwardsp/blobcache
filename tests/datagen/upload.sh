#!/usr/bin/env bash
# Upload generated test blobs to Azure Blob Storage.
#
# Reads STORAGE_ACCOUNT and RESOURCE_GROUP from .env (matches
# deploy/storage-access.sh convention). Container name defaults to
# "blobcache-test" but can be overridden via $CONTAINER.
#
# Usage:
#   OUT_DIR=/tmp/blobcache-data ./tests/datagen/upload.sh
#   CONTAINER=blobcache-test-2 OUT_DIR=/tmp/blobcache-data ./tests/datagen/upload.sh
#
# Pre-reqs:
#   1. Public network access enabled on the storage account
#      (deploy/storage-access.sh on)
#   2. The account is allowed by the current network's vnet rules,
#      OR the running identity has Storage Blob Data Contributor

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$HERE/../.." && pwd)"
[[ -f "$REPO_ROOT/.env" ]] && set -a && . "$REPO_ROOT/.env" && set +a

ACCOUNT="${STORAGE_ACCOUNT:?set STORAGE_ACCOUNT (in .env or env)}"
CONTAINER="${CONTAINER:-blobcache-test}"
OUT_DIR="${OUT_DIR:?set OUT_DIR to the directory produced by gen.sh}"

if [[ ! -d "$OUT_DIR" ]]; then
  echo "[upload] $OUT_DIR does not exist; run gen.sh first" >&2
  exit 1
fi

echo "[upload] account=$ACCOUNT container=$CONTAINER source=$OUT_DIR"

az storage container create \
  --account-name "$ACCOUNT" --auth-mode login \
  --name "$CONTAINER" --only-show-errors >/dev/null

az storage blob upload-batch \
  --account-name "$ACCOUNT" --auth-mode login \
  --destination "$CONTAINER" --source "$OUT_DIR" \
  --overwrite --only-show-errors

echo "[upload] done. Verify:"
echo "  az storage blob list --account-name $ACCOUNT --auth-mode login \\"
echo "    --container-name $CONTAINER --query '[].{name:name,size:properties.contentLength}' -o table"
