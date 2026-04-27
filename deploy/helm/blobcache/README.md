# blobcache Helm chart

Helm chart for [blobcache](https://github.com/anomalyco/blobcache), a distributed
FUSE-mounted Azure Blob cache for high-throughput AI/HPC clusters with optional
RDMA (UCX) peer transport.

The chart deploys two DaemonSets:

1. **`<release>-nvme-raid-init`** — privileged, assembles spare NVMe disks on
   each target node into a RAID-0 (`/dev/md/blobcache`), formats ext4, and
   mounts it on the host at `/mnt/nvme`. Idempotent.
2. **`<release>-blobcached`** — runs the `blobcached` daemon. Pinned to nodes
   that have both the agent-pool label and an opt-in test label set. Pod waits
   for the binary at `/opt/blobcache/blobcached` and launches it.

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

## Post-install: opt nodes in and push the binary

The blobcached DaemonSet pins to nodes carrying
`blobcache.test/enabled=true` (configurable via
`nodeSelector.testLabelKey` / `nodeSelector.testLabelValue`). Label the
nodes you want to run on:

```sh
kubectl label node <node1> <node2> <node3> blobcache.test/enabled=true
```

The blobcached pods start an Ubuntu base image, apt-install the runtime
dependencies, then poll for the binary at `/opt/blobcache/blobcached`. Push
the cross-built binary and the config in:

```sh
kubectl -n blobcache cp blobcached.aarch64 <pod>:/opt/blobcache/blobcached
kubectl -n blobcache cp blobcached.toml    <pod>:/opt/blobcache/blobcached.toml
```

Once the binary is present and executable, the pod's main loop execs it.
Repeat for each pod (or scriptify with `kubectl get pods -o name`).

This binary-push workflow is intentionally not modelled as a ConfigMap or
init-container image build — it lets you iterate on the daemon without
rebuilding container images. A future revision of this chart may add a
ConfigMap-backed option for pinned releases.

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
| `hostPaths.nvme` | Where the NVMe RAID array is mounted on the host. |

## Local validation

To lint the chart without a cluster:

```sh
helm lint deploy/helm/blobcache
helm template blobcache deploy/helm/blobcache -f myvalues.yaml | less
```
