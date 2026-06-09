#!/bin/bash -l
#SBATCH --job-name=bc-install
#SBATCH --output=/mnt/nvme/users/%u/bc-install-%j-%N.out
#SBATCH --partition=gpu
#SBATCH --nodes=16
#SBATCH --ntasks-per-node=1
#SBATCH --time=00:10:00
#SBATCH --exclusive
set -euxo pipefail
source /etc/profile.d/azcluster-storage.sh
# AZURE_CLIENT_ID must be exported in the user's shell or set here.
# On azcluster the per-cluster UAI client id is usually in
# /etc/profile.d/azcluster-*.sh; double-check and override if needed.
: "${AZURE_CLIENT_ID:?set AZURE_CLIENT_ID before sbatch (UAI client id)}"
export AZURE_CLIENT_ID

mkdir -p "/mnt/nvme/users/${USER}"
azcp copy "${AZCLUSTER_USER_BLOB_URL}/blobcache-deploy/install-blobcached.sh" \
          "/mnt/nvme/users/${USER}/install-blobcached.sh"
chmod +x "/mnt/nvme/users/${USER}/install-blobcached.sh"
bash    "/mnt/nvme/users/${USER}/install-blobcached.sh"
