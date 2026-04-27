# blobcache Helm chart

Helm chart for [blobcache](https://github.com/anomalyco/blobcache), a distributed
FUSE-mounted Azure Blob cache for high-throughput AI/HPC clusters with optional
RDMA (UCX) peer transport.

The chart deploys two DaemonSets:

1. **`<release>-nvme-raid-init`** — privileged, assembles spare NVMe disks on
   each target node into a RAID-0 (`/dev/md/blobcache`), formats ext4, and
   mounts it on the host at `/mnt/nvme`. Idempotent.
2. **`<release>-blobcached`** — runs the `blobcached` daemon directly from
   the `ghcr.io/edwardsp/blobcache` container image (binary baked in,
   UCX/FUSE runtime libs included). Pinned to nodes that have both the
   agent-pool label and an opt-in test label set. Reads its config from a
   `ConfigMap` mounted at `/etc/blobcached.toml`.

## Prerequisites

- Helm 3.x
- A Kubernetes cluster with:
  - A nodepool of RDMA-capable hosts (e.g. AKS GB300 / Grace ARM64).
  - The SR-IOV InfiniBand device plugin advertising `rdma/ib` (or your
    cluster's equivalent) as an extended resource (only required if
    `rdma.enabled=true`).
  - For Azure auth: a user-assigned managed identity attached to the VMSS
    holding **Storage Blob Data Reader** (or higher) on the target storage
    account(s).

## Install

```sh
cp deploy/helm/blobcache/values.example.yaml myvalues.yaml
$EDITOR myvalues.yaml   # set nodeSelector.agentPoolValue and azure.clientId

helm install blobcache ./deploy/helm/blobcache -f myvalues.yaml
```

You can also override individual values without a file:

```sh
helm install blobcache ./deploy/helm/blobcache \
  --set nodeSelector.agentPoolValue=myagentpool \
  --set azure.clientId=<msi-client-id>
```

## Minimal `myvalues.yaml`

```yaml
nodeSelector:
  agentPoolValue: "myagentpool"

azure:
  clientId: "<msi-client-id>"
```

Everything else falls back to the defaults in `values.yaml`. See
[`values.example.yaml`](values.example.yaml) for a more complete example.

## Post-install: opt nodes in

The blobcached DaemonSet pins to nodes carrying
`blobcache.test/enabled=true` (configurable via
`nodeSelector.testLabelKey` / `nodeSelector.testLabelValue`). Label the
nodes you want to run on:

```sh
kubectl label node <node1> <node2> <node3> blobcache.test/enabled=true
```

Pods start immediately using the image's baked-in binary and the
ConfigMap-mounted `/etc/blobcached.toml` — no `kubectl cp` step.

To roll out a config change, edit your values file (e.g. add a `[[mounts]]`
entry under `config.mounts`) and re-run `helm upgrade`; pods restart
automatically because the DaemonSet template carries a `checksum/config`
annotation derived from the rendered ConfigMap.

### Pinning to a specific image

The default tag is `:main` (rolling, multi-arch). For reproducible
deployments override `image.blobcached.tag` with a digest-pinned SHA tag
emitted by the container CI workflow (e.g. `sha-da10404`).

## Uninstall

```sh
helm uninstall blobcache
```

If `namespace.create=true` (default), the namespace is owned by the
release and will be removed too. If you set `namespace.create=false` and
installed into an existing namespace, that namespace is left in place.

## Configuration reference

See [`values.yaml`](values.yaml) for the full set of configurable
parameters and inline documentation. Key knobs:

| Key | Description |
|---|---|
| `namespace.create` / `namespace.name` | Whether to render a Namespace object and what to call it. |
| `nodeSelector.agentPoolValue` | **REQUIRED.** Name of the RDMA-capable nodepool. |
| `nodeSelector.testLabelKey` / `testLabelValue` | Opt-in label that gates the blobcached DS. |
| `azure.clientId` | User-assigned MSI client-id for Azure auth. Empty = system-assigned / IMDS default. |
| `rdma.enabled` / `rdma.resourceName` / `rdma.count` | RDMA device request on the blobcached pod. |
| `ucx.*` | UCX_* env vars for the blobcached container. |
| `image.blobcached.*` / `image.nvmeRaid.*` | Container images and pull policies. |
| `imagePullSecrets` | Pull secrets for private GHCR packages. |
| `config.mounts` | **REQUIRED.** List of `[[mounts]]` entries (one per blob container). |
| `config.*` | Other contents of `/etc/blobcached.toml` (cache, azure, cluster, transport, stats). |
| `hostPaths.nvme` | Where the NVMe RAID array is mounted on the host. |

## Local validation

To lint the chart without a cluster:

```sh
helm lint deploy/helm/blobcache
helm template blobcache deploy/helm/blobcache -f myvalues.yaml | less
```
