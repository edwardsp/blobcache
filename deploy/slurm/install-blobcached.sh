#!/bin/bash
# Node-local installer for blobcached on an azcluster Slurm cluster.
#
# Required env:
#   AZCLUSTER_USER_BLOB_URL   per-user blob staging URL (set by
#                             /etc/profile.d/azcluster-storage.sh on
#                             azcluster nodes)
#   AZURE_CLIENT_ID           client_id of the UAI with Storage Blob
#                             Data Reader on the model account
#
# Expects these blobs to already exist under
# ${AZCLUSTER_USER_BLOB_URL}/blobcache-deploy/ :
#   blobcached                 release binary
#   blobcached.toml            cluster config
#   blobcached.service         systemd unit
#
# Idempotent: re-running upgrades the binary and restarts the daemon.
set -euxo pipefail

: "${AZCLUSTER_USER_BLOB_URL:?source /etc/profile.d/azcluster-storage.sh first}"
: "${AZURE_CLIENT_ID:?export AZURE_CLIENT_ID=<UAI client id>}"

SRC="${AZCLUSTER_USER_BLOB_URL}/blobcache-deploy"
WORK="/mnt/nvme/blobcache-stage"
mkdir -p "$WORK"

azcp copy "${SRC}/blobcached"          "${WORK}/blobcached"
azcp copy "${SRC}/blobcached.toml"     "${WORK}/blobcached.toml"
azcp copy "${SRC}/blobcached.service"  "${WORK}/blobcached.service"
chmod 755 "${WORK}/blobcached"

sudo mkdir -p /opt/blobcache /mnt/nvme/blobcache-cache
sudo install -m 0755 "${WORK}/blobcached"         /opt/blobcache/blobcached
sudo install -m 0644 "${WORK}/blobcached.toml"    /etc/blobcached.toml
sudo install -m 0644 "${WORK}/blobcached.service" /etc/systemd/system/blobcached.service

sudo install -d -m 0755 /etc/default
echo "AZURE_CLIENT_ID=${AZURE_CLIENT_ID}" | sudo tee /etc/default/blobcached >/dev/null

if ! grep -qx 'user_allow_other' /etc/fuse.conf 2>/dev/null; then
  echo 'user_allow_other' | sudo tee -a /etc/fuse.conf >/dev/null
fi

sudo systemctl daemon-reload
sudo systemctl enable --now blobcached
sleep 4
sudo systemctl is-active blobcached
curl -sf http://127.0.0.1:7773/metrics | grep -c '^blobcache_'
ls /blobcache/ || true
