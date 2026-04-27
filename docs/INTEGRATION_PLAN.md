# blobcache distribution & workflow integration plan

**Status:** draft / planning
**Scope:** AKS first (gb300 / Grace ARM64). Slurm sketched at the end.
**Audience:** ML platform / infra engineers who want to drop blobcache in front
of an existing inference or training workload.

---

## 1. Goals

1. **Zero-friction install** on a new AKS cluster: one `helm install`, no
   manual binary cross-builds, no `kubectl cp` dance, no per-pod babysitting.
2. **Coexistence with NCCL/UCX-based training and inference** on the same
   nodes — blobcache must not silently steal the IB fabric the workload
   needs for collectives.
3. **Drop-in mount** for the consumer pod: the training/inference job sees
   model files at a stable filesystem path and reads them with normal
   `open()/read()` (or `mmap()`).
4. **Reproducible image builds** via GitHub Actions on tag, pushed to a
   container registry (GHCR or ACR), so a `helm upgrade --set image.tag=…`
   is the only step needed to roll a new version.
5. **Forward path to Slurm** without a rewrite — the same binary + config
   model should work under `srun`/`enroot`/`pyxis`.

Non-goals for v1 of this plan: writes to blob, multi-tenant ACLs,
auto-discovery of model paths, dynamic re-sharding.

---

## 2. The big technical question first: can blobcache and the training
   workload **share** all 4 InfiniBand cards?

The hard constraint from the user: **the training/inference job needs all
4 HCAs**. We cannot ask it to give up `mlx5_0`. So the question is not
"which HCA does blobcache get" — it's "how do both processes use the
same physical HCAs at the same time without the device plugin treating
them as exclusive resources."

The short answer is **yes, this is fully supported** at multiple levels
of the stack. The mechanics are below; the choice between them depends
on the cluster's device plugin and on whether we run blobcache as a
DaemonSet or as a sidecar.

### 2.1 What we do today (and why it needs to change)

- gb300 nodes have **4 ConnectX-7 / Bluefield IB HCAs** (`mlx5_0..3`).
- The blobcached DaemonSet requests `rdma/ib: 1` per pod (one HCA).
- Under the **SR-IOV device plugin in exclusive-PF mode**, that
  *consumes* one HCA from the node's allocatable pool, leaving 3 for
  the training pod. **This is the model we must abandon.**
- Under either of the two configurations below, that same `rdma/ib: 1`
  request can map to a *shared* unit and the training pod can still
  get 4 HCAs.

### 2.2 How HCA sharing actually works (three independent layers)

Sharing happens at three levels of the stack. We need at least one of
them on for blobcache + training to coexist on the same HCAs; **all
three combine cleanly**.

**Layer 1 — Device plugin: shared mode.**
Two device plugins natively expose HCAs as a shared resource:

- **`k8s-rdma-shared-dev-plugin`** (Mellanox/NVIDIA, the canonical
  choice). ConfigMap snippet:
  ```yaml
  configList:
    - resourceName: hca_shared_all
      rdmaHcaMax: 64        # how many pods may share the pool
      selectors:
        ifNames: ["ib0","ib1","ib2","ib3"]   # all 4 HCAs in one pool
  ```
  Every pod that requests `rdma/hca_shared_all: 1` gets `/dev/infiniband/*`
  for all 4 HCAs. Both blobcached and the training pod request 1 unit
  each, both see all 4 HCAs.

- **`sriov-network-device-plugin` in VF-allocation mode.** Each HCA
  exposes N Virtual Functions (typical: 8 VFs × 4 HCAs = 32 units).
  blobcache requests 4 VFs (one per HCA), training requests 4 VFs (one
  per HCA on different VFs). Each side gets its own GIDs and QPs but
  the **same physical HCAs** push the bytes.

**Layer 2 — Linux IB stack: HCAs are inherently shareable.**
Even without the device plugin's help, the IB verbs API allows multiple
processes to open the same `/dev/infiniband/uverbs0` device. They each
allocate their own Protection Domain, Queue Pairs, and Memory Regions;
the HCA hardware multiplexes between them. NCCL and UCX in the same
host already do this implicitly when both run inside the same node's
namespace. The device plugin just controls *who is allowed to open the
file*; once it's open, the kernel and HCA do the right thing.

**Layer 3 — Pod sharing: containers in one pod share /dev/infiniband.**
This is the **sidecar** path (see §2.4). Containers within a single
pod share the pod's network namespace and the pod's device-plugin
allocations. If the pod requests `rdma/ib: 4` (or whichever shared
resource), **both** the blobcached sidecar **and** the training
container see all 4 HCAs without any further plumbing. No second
device-plugin allocation, no extra config. This is the simplest and
most ergonomic path on a per-job basis.

### 2.3 NCCL + UCX on the same HCAs: does it work?

Yes, and it is a well-trodden path:

- NCCL (training) creates its own QPs, MRs, and CQs.
- UCX (blobcache) creates its own QPs, MRs, and CQs.
- The HCA scheduler arbitrates at the wire. Both can saturate; under
  contention each gets ~half. In practice they don't overlap much:
  AllReduce traffic is bursty and synchronous with compute steps;
  blobcache traffic is bursty and synchronous with model load /
  prefetch events.
- Memory registration: each process registers its own pinned region.
  We need `IPC_LOCK` and a high `memlock` ulimit on **both** pods
  (training images already do this; blobcache does too). The HCA's
  `max_mr` is in the millions; we won't run out.
- `NCCL_IB_HCA` does not need to exclude anything. NCCL just enumerates
  `mlx5_0..3` and uses them all. Blobcache is invisible to NCCL.

### 2.4 Sidecar vs DaemonSet: which one?

**Both have a role. Pick per workload type.**

#### DaemonSet (the current model, kept)

```
┌── node ────────────────────────────────────────────────────────┐
│  [DS pod: blobcached]          [Pod: training/inference]      │
│  rdma/ib: 1  (shared pool)     rdma/ib: 4  (shared pool)      │
│  sees mlx5_0..3                sees mlx5_0..3                 │
│         │                              │                      │
│         └── shared HCAs, shared NVMe RAID via hostPath ───────┘
└────────────────────────────────────────────────────────────────┘
```

**Pros:**
- One blobcache identity per node → coherent gossip cluster, single
  bloom filter per node, no duplicate caching.
- Survives across consumer pod lifetimes → cache stays warm between
  training jobs, between inference rolling-restarts.
- Operator-installed once; consumer pods don't think about it.

**Cons:**
- Requires the cluster admin to set up shared-mode device plugin
  before consumers can also request RDMA.
- Extra always-on pod even when no consumer is running.

**Use this for:** inference (long-lived), recurring training jobs on
the same model, anywhere multiple consumer pods share a node.

#### Sidecar (new option)

```
┌── pod ─────────────────────────────────────────────────────────┐
│  rdma/ib: 4  (whole pod's allocation)                         │
│                                                                │
│  [container: trainer]          [container: blobcached]        │
│  /dev/infiniband/uverbs0..3    /dev/infiniband/uverbs0..3     │
│  reads /models                 mounts FUSE @ /models          │
│         │                              │                      │
│         └── emptyDir or hostPath /mnt/nvme cache ─────────────┘
└────────────────────────────────────────────────────────────────┘
```

**Pros:**
- **Zero device-plugin config required.** Both containers automatically
  share the pod's IB allocation by virtue of being in the same pod.
  Works on any cluster that already gives the training pod its 4 HCAs.
- Lifecycle bound to the job: starts when the pod starts, dies when it
  dies. No orphaned daemon.
- Easy to drop into an existing job spec — single YAML change, no Helm
  install on the cluster.

**Cons:**
- **No cross-pod cache sharing on the same node** unless the cache
  directory is `hostPath` AND a singleton blobcache instance is in
  charge of writes per node (avoidable with file locks but messy).
- **No cross-node peer fetch** unless multiple sidecars discover each
  other via gossip — workable, but each pod becomes its own gossip
  cluster member, and starting/stopping pods causes membership churn.
- Slow first-pod-on-node start: cold cache, must hit blob.

**Use this for:** one-shot training Jobs, ephemeral hydrate jobs,
anywhere the user wants a single self-contained pod spec.

#### Hybrid (recommended for most production setups)

Run **both**: a DaemonSet provides the always-on per-node gossip
member and the warm cache; the consumer pod runs a **thin sidecar**
that just FUSE-mounts the per-job mount path and talks to the local
DS via unix socket (or just shares the hostPath cache directly via
`hostPath` mount). The sidecar adds the per-job read-only FUSE namespace
without touching the IB stack at all (the heavy lifting is in the DS).

We will only build the hybrid if the simpler models leave gaps. Start
with DS-only and sidecar-only as the two supported patterns.

### 2.5 Recommended operating modes

The chart will support three modes; **all three assume HCA sharing**
(no more "blobcache steals an HCA" model):

| Mode | Topology | RDMA | When to use |
|---|---|---|---|
| **`shared-ds`** (default) | DaemonSet | Shared-mode device plugin; both DS and consumer request from the shared pool, both see all 4 HCAs. | Multi-tenant nodes, persistent inference, recurring training. |
| **`sidecar`** | Sidecar in consumer pod | Pod requests `rdma/ib: 4`; both containers see all 4 HCAs automatically. | One-shot Jobs, no admin access to install device plugins, single-pod simplicity. |
| **`hydrate-job`** | Pre-job `Job` | Hydrate Job requests RDMA, fills NVMe RAID, exits. Training starts with cache warm; reads are local NVMe. | Bandwidth-paranoid teams who want zero RDMA contention during training. |

We deprecate the previous `coresident` (steal-1-HCA) and
`coresident-tcp` modes from the earlier draft of this doc; they were
based on the wrong assumption that we couldn't share HCAs.

### 2.6 What the *consumer* has to do

Almost nothing on the IB side:

- **`shared-ds` mode:** consumer requests `rdma/<shared-resource-name>: 1`
  (or 4, or whatever its workload demands) from the shared pool. NCCL
  config is unchanged from a vanilla 4-HCA training image. Consumer
  also adds a `hostPath` mount of `/mnt/nvme/blobcache-mnt/<mount>`.
- **`sidecar` mode:** consumer adds a second container (the blobcached
  sidecar) and a shared `emptyDir` or `hostPath` for the cache. Pod's
  total `rdma/ib` request stays at 4. NCCL config unchanged.
- **`hydrate-job` mode:** consumer is a normal pod; nothing to change
  except mounting the hydrated NVMe path read-only.

There is no "register with blobcache", no "drain before training", no
NCCL HCA exclusion required.

---

## 3. Distribution model

### 3.1 Container images

Build **two images**, both multi-arch (amd64 for dev, arm64 for gb300):

1. **`ghcr.io/<owner>/blobcached:<version>`** — runtime image.
   - Base: `ubuntu:24.04` (matches what we apt-install today; staying on
     glibc keeps UCX simple).
   - Contents: `/opt/blobcache/blobcached` (statically-linked Rust binary
     compiled with `--features ucx`), runtime deps (`fuse3`, `libucx0`,
     `libibverbs1`, `librdmacm1t64`, `ucx-utils`, `ibverbs-utils`),
     CA certs, `tini` as PID 1.
   - Entrypoint: `/usr/local/bin/blobcached-entrypoint` — sources
     environment, validates config, execs the daemon. Replaces the
     shell-loop-waiting-for-binary hack in the current chart.

2. **`ghcr.io/<owner>/blobcache-nvme-init:<version>`** — RAID assembler.
   - Base: `ubuntu:24.04`.
   - Contents: `mdadm`, `e2fsprogs`, `nsenter` wrapper script.
   - Same image we already need; just version-tagged and published.

### 3.2 GitHub Actions pipeline

```
.github/workflows/release.yml
  on: push tags v*
  jobs:
    build-binary:
      strategy: matrix [amd64, arm64]
      runs-on: matrix arch (or qemu cross-build)
      steps:
        - install rust + libfuse3-dev + libucx-dev
        - cargo build --release --features ucx
        - upload artifact: blobcached.<arch>
    build-image:
      needs: build-binary
      steps:
        - docker buildx (multi-arch manifest)
        - tag :{{ github.ref_name }} and :latest
        - push to ghcr.io/<owner>/blobcached
        - push to ghcr.io/<owner>/blobcache-nvme-init
    publish-chart:
      needs: build-image
      steps:
        - helm package deploy/helm/blobcache
        - helm push to ghcr.io/<owner>/charts/blobcache (OCI registry)
        - update Chart.yaml appVersion to {{ github.ref_name }}
```

For arm64 builds we have two reasonable options:

- **QEMU cross-build on amd64 GH runner** (simple, ~10x slower on Rust).
- **Native arm64 runner** via GitHub's `ubuntu-24.04-arm` (preferred once
  it's GA in our org, ~native speed).

Decision: start with QEMU; switch to native arm64 runner once the binary
build time becomes the critical path.

### 3.3 Helm chart changes

Three concrete changes to the existing chart eliminate the manual
binary push:

1. **Drop the wait-for-binary loop** in `blobcached-daemonset.yaml`. The
   image now ships the binary and the apt-installed deps. Container
   command becomes simply
   `["/usr/local/bin/blobcached-entrypoint", "--config", "/etc/blobcached/blobcached.toml"]`.
2. **Add a `ConfigMap`** rendered from `values.yaml` for the daemon
   config. No more `kubectl cp blobcached.toml`. Mounted at
   `/etc/blobcached/`.
3. **Add a "blobcache-info" `ConfigMap`** (small) the chart publishes in
   the consumer namespace, containing:
   - mount host path (`/mnt/nvme/blobcache-mnt/<mount>`)
   - HCA blobcache holds (so consumers can exclude it from NCCL)
   - chunk size (consumer io-pattern tuning)
   - blobcached image tag (so consumers can correlate)

   Consumers read it via a `configMapKeyRef` env var or mount it as a
   file. This is the integration handshake.

### 3.4 What stays the same

- DaemonSet topology (one blobcached per node).
- `hostNetwork: true`, privileged, `IPC_LOCK`.
- NVMe RAID init DaemonSet (separate concern, separate image).
- All `transport.*`, `cache.*`, `cluster.*` config knobs.
- Storage account auth via MSI / `AZURE_CLIENT_ID`.

---

## 4. End-to-end workflow integration

### 4.1 The integration contract (one paragraph)

**A consumer pod gets the blobcache mount via either (a) a `hostPath`
of `/mnt/nvme/blobcache-mnt/<mount>` when the cluster runs the
DaemonSet, or (b) a sidecar container that mounts the FUSE filesystem
into a shared `emptyDir` when the consumer is self-contained. The pod's
existing `rdma/ib: 4` request is unchanged; both blobcache and the
training/inference workload see all 4 HCAs.** That is the entire
contract.

### 4.2 Pattern A: shared-DS (recommended for inference and recurring
training)

```
┌── node (every gb300 node in the pool) ───────────────────────────┐
│                                                                  │
│  [DS pod: blobcached]          [Pod: training/inference]         │
│  rdma/<shared>: 1              rdma/<shared>: 4                  │
│  sees mlx5_0..3                sees mlx5_0..3                    │
│         │                              │                         │
│         └─── share HCAs at the wire; share NVMe via hostPath ────┘
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

The DaemonSet is installed once by the cluster admin. Consumer pods
mount the host path read-only. Cache survives consumer pod lifetimes;
peer fan-out works across the node pool.

Sample consumer fragment:

```yaml
spec:
  nodeSelector:
    kubernetes.azure.com/agentpool: gb300
  containers:
    - name: trainer
      image: <your-training-image>
      env:
        - name: MODEL_DIR
          value: /models
      resources:
        limits:
          rdma/hca_shared_all: 1   # or whatever the cluster's pool name is
          nvidia.com/gpu: 4
      volumeMounts:
        - name: blobcache-mnt
          mountPath: /models
          readOnly: true
  volumes:
    - name: blobcache-mnt
      hostPath:
        path: /mnt/nvme/blobcache-mnt/<mount-name>
        type: Directory
```

Note: **no `NCCL_IB_HCA` exclusion**, **no special init**, **no awareness
of blobcache** in the training script. The pod's container reads
`/models/...` like any other filesystem.

### 4.3 Pattern B: sidecar (recommended for one-shot Jobs and self-contained
deployments)

```
┌── pod ─────────────────────────────────────────────────────────────┐
│  rdma/ib: 4   (whole pod's allocation, shared between containers) │
│                                                                    │
│  [container: blobcached]       [container: trainer]               │
│  - mounts FUSE @ /shared/mnt   - reads /shared/mnt/<path>         │
│  - cache @ /shared/cache       - GPU compute, NCCL on all 4 HCAs  │
│         │                              │                          │
│         └── shared emptyDir (cache+mnt) OR hostPath (cache only) ─┘
│                                                                    │
└────────────────────────────────────────────────────────────────────┘
```

Single pod, single YAML file, no Helm install required on the cluster.
The blobcached sidecar comes up first (init-container or just earlier
in the spec), mounts FUSE into the shared volume, and signals readiness.
The training container starts after the FUSE mount appears (use a small
`postStart` wait or a readinessProbe-gated dependent container).

Sample sidecar pod fragment:

```yaml
spec:
  nodeSelector:
    kubernetes.azure.com/agentpool: gb300
  shareProcessNamespace: false
  containers:
    - name: blobcached
      image: ghcr.io/<owner>/blobcached:<version>
      securityContext:
        privileged: true
        capabilities: { add: ["SYS_ADMIN", "IPC_LOCK"] }
      env:
        - name: BLOBCACHE_MODE
          value: sidecar
        - name: BLOBCACHE_MOUNT_PATH
          value: /shared/mnt
        - name: AZURE_CLIENT_ID
          value: <msi-client-id>
      volumeMounts:
        - name: shared
          mountPath: /shared
          mountPropagation: Bidirectional   # so trainer sees the FUSE mount
        - name: fuse
          mountPath: /dev/fuse
    - name: trainer
      image: <your-training-image>
      env:
        - name: MODEL_DIR
          value: /shared/mnt/<mount-name>
      resources:
        limits:
          rdma/ib: 4               # whole pod gets all 4 HCAs
          nvidia.com/gpu: 4
      volumeMounts:
        - name: shared
          mountPath: /shared
          mountPropagation: HostToContainer
  volumes:
    - name: shared
      emptyDir: {}                  # OR hostPath /mnt/nvme for cross-job persistence
    - name: fuse
      hostPath: { path: /dev/fuse }
```

Two important details:

1. **`mountPropagation`** is required on both volumeMounts for the
   trainer to see the FUSE filesystem the sidecar mounts inside its
   own container. Bidirectional on the sidecar (it does the mount),
   HostToContainer on the trainer (it observes the mount).
2. **Cache persistence:** `emptyDir` makes the cache pod-lifetime;
   `hostPath /mnt/nvme` makes it node-lifetime (and shareable with a
   neighbouring DS or a future job). For one-shot training where the
   model is read once, `emptyDir` is fine and avoids any state on the
   host.

### 4.4 Pattern C: hydrate-job (recommended for bandwidth-paranoid teams)

```
┌── Time ──────────────────────────────────────────────────────────►
│
│ [Job: blobcache-hydrate]
│   - DaemonSet-style fan-out across all nodes (one Job per node)
│   - rdma/ib: 4 (use everything available; no contention, no consumer)
│   - reads model files via FUSE → triggers blob fetch + peer fan-out
│   - writes to hostPath /mnt/nvme; exits when done (sentinel file)
│
│                       [Job: training-job]
│                         - hostPath /mnt/nvme  (RO)
│                         - rdma/ib: 4
│                         - reads model from local NVMe (no IB needed)
│                         - blobcached pod is gone; cache is just files on disk
│
└────────────────────────────────────────────────────────────────────►
```

The hydrate Job uses RDMA, all 4 HCAs, full bandwidth, no contention.
After it exits, the training Job sees a fully populated NVMe and reads
locally. We provide a tiny client shim (Python + Go) that resolves
`<mount>/<path>` to the underlying SHA-256-keyed cache file so training
code works whether or not the FUSE mount is currently active.

### 4.5 What the consumer **does not** have to do (in any pattern)

- No client library to import (the shim is optional, only for
  hydrate-job mode).
- No special `open()` flag, no awareness of caching.
- No coordination protocol with blobcached.
- No `NCCL_IB_HCA` exclusion.
- No init container to "warm up" blobcache.
- No teardown step.

---

## 5. Suggested directory & repo layout (post-this-plan)

```
blobcache/
├── src/                          # daemon (unchanged)
├── docker/
│   ├── blobcached.Dockerfile     # new: multi-stage, multi-arch
│   ├── nvme-init.Dockerfile      # new: ubuntu + mdadm
│   └── entrypoint.sh             # new: validate config, exec daemon
├── deploy/
│   ├── helm/blobcache/           # existing chart, simplified per §3.3
│   └── examples/                 # new: consumer manifest examples
│       ├── training-job.yaml
│       ├── inference-deployment.yaml
│       └── hydrate-job.yaml
├── client/                       # new: tiny shims for direct cache access
│   ├── python/blobcache_fs.py    # `from blobcache_fs import open as bopen`
│   └── go/blobcache-fs/          # static binary equivalent
├── docs/
│   ├── INTEGRATION_PLAN.md       # this file
│   ├── RDMA_COEXISTENCE.md       # deeper on §2 once we measure
│   └── CONSUMER_RECIPES.md       # cookbook of consumer pod patterns
├── .github/workflows/
│   ├── ci.yml                    # cargo test, clippy, helm lint
│   └── release.yml               # build images, push, package chart
└── README.md
```

---

## 6. Phased plan

### Phase 0 — verify the load-bearing assumptions (1–2 days)

These are the questions that will sink the rest of the plan if wrong.
Answer them first, write findings into `docs/RDMA_COEXISTENCE.md`.

1. **Which device plugin is on the gb300 nodepool?** `kubectl get ds -n
   kube-system | grep -E 'sriov|rdma'`. Inspect its ConfigMap to find
   the resource name and the per-node capacity. If it's
   `k8s-rdma-shared-dev-plugin`, sharing is free. If it's
   `sriov-network-device-plugin`, check whether multiple VFs per HCA
   are exposed (>4 allocatable units per node = sharing is free).
2. **If the cluster is in exclusive-PF mode**, write up the operator
   migration recipe to switch to shared mode (a ConfigMap change +
   device-plugin restart). This becomes a chart prerequisite.
3. **Confirm UCX + NCCL coexistence on the same HCAs** with a 2-node
   benchmark: run `nccl-tests` AllReduce while blobcache is doing a
   peer fetch over the same HCAs. Record any throughput regression on
   either side. Expectation: minimal interference because traffic is
   bursty in both processes.
4. **Confirm the sidecar pattern works end-to-end** with a hand-written
   pod spec: blobcached + a busybox `dd` that reads through the FUSE
   mount. Validate `mountPropagation` semantics on the cluster's CRI
   (containerd vs cri-o behave the same, but worth checking once).

### Phase 1 — image + chart simplification (1 week)

1. Write `docker/blobcached.Dockerfile` (multi-stage: rust:1.86 builder
   stage → ubuntu:24.04 runtime).
2. Write `docker/entrypoint.sh` (validate `BLOBCACHE_*` env vars, render
   final `blobcached.toml` from a template + env, exec daemon).
3. Replace the wait-for-binary loop in the chart with a direct `command`
   pointing at the entrypoint.
4. Add the `blobcached.toml` ConfigMap template; remove the
   `kubectl cp` step from chart docs.
5. Add the `blobcache-info` ConfigMap template (mount path, HCA name,
   chunk size, version).
6. `helm lint` clean; `helm template` renders sane manifests.

### Phase 2 — GitHub Actions release pipeline (3 days)

1. CI workflow: `cargo test`, `cargo clippy --features ucx`,
   `helm lint`, `helm template` smoke.
2. Release workflow on tag: build amd64 + arm64 images, push to
   GHCR, package + push Helm chart as OCI artifact.
3. README badges, install snippet pointing at the published image.

### Phase 3 — consumer ergonomics (1 week)

1. Write three example manifests (`deploy/examples/`):
   `training-job.yaml`, `inference-deployment.yaml`, `hydrate-job.yaml`.
2. Write the Python `blobcache_fs` shim (resolves `<mount>/<path>` to
   the underlying SHA-256-keyed cache file via the same
   `chunk_offset → sha256(mount, blob, offset)` scheme the daemon uses).
   This lets training code read the cache directly even after the
   daemon exits in hydrate-only mode.
3. Cookbook doc: `docs/CONSUMER_RECIPES.md` with end-to-end recipes for
   each operating mode.

### Phase 4 — operating-mode toggles in the chart (3 days)

1. Add `mode: shared-ds | sidecar | hydrate-job` to `values.yaml`.
2. `shared-ds` → DaemonSet, requests from a configurable shared resource
   pool name (default `rdma/hca_shared_all`, override per-cluster).
3. `sidecar` → render a Helm template *snippet* (not a full pod) that
   users can include in their own pod spec via `helm template`. Also
   ship a standalone YAML example in `deploy/examples/sidecar-pod.yaml`.
4. `hydrate-job` → render a `Job` with `parallelism = node count`,
   `--hydrate-paths=<glob>` arg, and a sentinel-file completion signal.

### Phase 5 — Slurm pathway (sketch only for now, do not implement)

The container model collapses under Slurm (no Kubernetes scheduler),
but the binary + config model survives intact. Sketch:

- Use **enroot/pyxis** to run the same `blobcached` image as a Slurm
  prolog/epilog or a sidecar in a `srun --container-image=...` job.
- Replace gossip seeds with the Slurm node list (`scontrol show
  hostnames $SLURM_NODELIST`).
- Hostpath equivalents become bind mounts via `--container-mounts`.
- NVMe RAID init becomes a one-shot prolog script.
- Auth: workload identity isn't a thing on Slurm; use a SAS token from
  Slurm's secret store, or a shared key from a credential file mode-600
  on the head node.

We document this but ship Phases 0–4 first.

---

## 7. Open decisions for the user

These are the choices that need a call before Phase 1 starts. Each has
a recommended default in **bold**.

1. **Container registry**: GHCR (free, public-by-default, ties to repo
   visibility) vs ACR (private, ties to subscription, faster pulls
   inside Azure). Recommend **GHCR for OSS, with optional ACR mirror
   via an Azure Pipelines step for production pulls**.
2. **Multi-arch in CI**: QEMU on amd64 runner vs native arm64 runner.
   Recommend **start with QEMU; revisit if Rust build > 15 min**.
3. **Default operating mode** in the chart. Recommend **`shared-ds`** as
   the default for cluster-wide installs, with **`sidecar`** documented
   as the per-job alternative for users who don't have admin access to
   change the device plugin config.
4. **Sidecar lifecycle ordering**: rely on Kubernetes 1.28+ native
   sidecar containers (`restartPolicy: Always` in `initContainers`) so
   the trainer waits for blobcached to be ready, or use a
   readinessProbe + a busy-wait in the trainer? Recommend **native
   sidecar containers** if the AKS cluster is on 1.28+ (verify in
   Phase 0); otherwise the readiness-gated approach with a small wait
   loop in the trainer entrypoint.
5. **Where the consumer-facing ConfigMap lives** (chart's namespace vs
   consumer's namespace). Recommend **chart's namespace + a documented
   `--namespace` clone command** for now; add a controller / operator
   only if real users ask for it.
6. **Whether to ship a Python client shim** in Phase 3 or wait. Recommend
   **ship it**; it is ~50 lines and makes hydrate-job mode actually
   usable from training scripts.

---

## 8. Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Cluster's device plugin is in exclusive-PF mode and can't be changed | Medium | High | Document the migration to shared mode; if blocked, fall back to **sidecar** mode (no extra device-plugin allocation needed). |
| HCA bandwidth contention regresses NCCL during overlapping blobcache traffic | Low | Medium | Phase 0 benchmark; if real, recommend `hydrate-job` mode for the affected workload. |
| Sidecar `mountPropagation` semantics not honored on this AKS / containerd version | Low | High | Phase 0 hand-test; document minimum k8s/containerd version. |
| Native sidecar containers (k8s 1.28+) not available on the cluster | Low | Medium | Use the readiness-gated wait-loop fallback; document both. |
| FUSE mount disappears between `helm upgrade` rollouts and a consumer pod has the file open | Medium | Medium | Use `terminationGracePeriodSeconds: 60` and `preStop` hook that waits for FUSE-open file count to drop. |
| Consumer pods on un-labelled nodes get nothing at the hostPath | Medium | Low | Document the requirement; add a Helm-rendered admission policy if it bites. |
| GHCR rate limits anonymous pulls | Low | Low | Document `imagePullSecrets`; mirror to ACR in Azure for production. |

---

## 9. What this plan deliberately does not solve (yet)

- **Writes**. blobcache is read-only; we do not propose to change that.
- **Multi-tenant ACLs**. All consumers on a node see all cached files.
  Acceptable for single-tenant training/inference clusters; not for
  shared-customer clusters.
- **Cross-region replication**. Cache is per-cluster.
- **Automatic eviction tuning**. `cache.max_bytes` is operator-set.
- **GUI / dashboard**. Stats endpoint exposes Prometheus metrics; users
  bring their own Grafana.

---

## 10. Single-paragraph TL;DR

The training/inference job keeps all 4 IB cards. blobcache shares the
**same** HCAs at the wire — either by running as a DaemonSet that
requests from a shared device-plugin pool (`k8s-rdma-shared-dev-plugin`
or SR-IOV with multi-VF), or by running as a sidecar in the consumer's
own pod (which trivially shares the pod's IB allocation). NCCL and UCX
coexist on the same HCAs without configuration changes; the consumer
pod does not need to exclude any HCA from NCCL. We ship two container
images built multi-arch in GitHub Actions on tag, simplify the existing
Helm chart to drop the manual binary push, and offer three operating
modes (`shared-ds`, `sidecar`, `hydrate-job`) so users pick the
topology that fits their workload. The consumer's only integration step
is a `hostPath` mount (DS mode) or a sidecar container (sidecar mode).
Phase 0 is a one-day verification on the live gb300 cluster: confirm
which device plugin is installed, that shared-mode allocation works,
and that NCCL + UCX run cleanly on the same HCAs.
