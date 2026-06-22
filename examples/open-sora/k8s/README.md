# Open-Sora v2 × blobcache on AKS (Kubernetes port)

Kubernetes port of the Slurm benchmark in [`../`](..) — see
[`docs/examples/open-sora-pexels.md`](../../../docs/examples/open-sora-pexels.md) for the
methodology and reference numbers. Provision the cluster with
[azcluster](https://github.com/edwardsp/azcluster) (`deploy --target aks`, ND H200 pool,
ACStor + NVIDIA network/GPU operators).

> **Status: iterated on a live cluster.** The `os-train` image (with `pyshim`) is
> built by `docker/os-train` CI. The manifest + steps below are the design; the live
> hydrate/rendezvous ordering and ACStor sizing are validated on a real run.

## Topology

One StatefulSet `os-train` **is** the job (your "per-job, ACStor, not a DaemonSet"):
each pod runs, on one GPU node —

- a privileged **blobcached** sidecar: cache on an **ACStor `local-csi`** ephemeral
  volume (`/mnt/nvme`), peer-fetch over **InfiniBand** (UCX/RDMA), FUSE mounts the
  pexels + osv2 blobs at `/blobcache/{pexels,osv2}` (`mountPropagation: Bidirectional`);
- the **os-train** container (8 GPUs + `rdma/ib: 8`) sharing `/blobcache`
  (`HostToContainer`), which waits for the gossip cluster, and on **rank-0** runs the
  cold-run hydrate (clear-cache → broadcast **weights**; the dataset is *not* hydrated),
  then `torchrun … scripts/diffusion/train.py blobcache_stage1.py`;
- a **metrics** sidecar sampling `:7773/metrics` every 5 s into the per-node CSV the
  `plot_*.py` tools consume.

The blobcached gossip cluster spans all 64 pods, so the hydrate fans out and the dataset
peer-fetches over IB exactly as in the Slurm run.

## Runbook (cold strategy)

1. **Stage** the dataset + weights into the cluster's Blob account (once), and fill the
   `STORAGE_ACCOUNT`/`*_CONTAINER`/`*_PREFIX` placeholders:
   - dataset: HF `zengxianyu/open-sora-pexels-full` (258.5 GiB) + its `pexels_meta.csv`
   - weights: HF `hpcai-tech/Open-Sora-v2` → `Open_Sora_v2.safetensors`, `hunyuan_vae.safetensors`, `google/t5-v1_1-xxl`, `openai/clip-vit-large-patch14`
2. **NCCL-validate** the pool first (the azcluster `examples/aks/nccl-allreduce-mpijob.yaml`
   scaled to the node count); `kubectl cordon` any bad node so ≥64 stay schedulable.
3. **ndv5-topo** ConfigMap from `examples/aks/ndv5-topo.xml`.
4. **Run**: `export REPLICAS=64 IMAGE=… MI_CLIENT_ID=… STORAGE_ACCOUNT=… …; envsubst < os-train-aks.yaml | kubectl apply -f -`
5. **Capture** blobcache perf while running: poll each pod's `:7773/metrics` (or collect
   the `metrics` sidecar CSVs with `kubectl cp` before teardown), then render with the
   existing `../plot_reads.py` / `../plot_summary.py` / `../plot_throughput.py`.
6. **Verify** cold-data perf matches the reference (~37 s/step, ~59 GiB/s aggregate, ~250 GiB
   blob + ~246 GiB peer) and **tear down**.

## Open items before a faithful run

- `pexels_meta.csv` location on AKS (default assumes it ships in the pexels blob at
  `/blobcache/pexels/pexels_meta.csv`; override `PEXELS_META_CSV` otherwise).
- ACStor `cache` PVC sized at 2 TiB/pod — tune to the node's NVMe RAID capacity.

## Validated results (ND H200 v5, mexicocentral)

Live-validated at **64 and 128 nodes**. The 64-node cold run matches the Slurm reference
within 1% (cold blob egress 248.2 GiB vs 249.7; peer-over-IB 248.4 GiB vs 245.9). Blobcache
cold-read throughput scales near-linearly 64→128 nodes (peak **111.7 → 198.0 GiB/s**, IOPS
**1.70M → 3.00M**) while **cold blob egress stays flat** (~248 → ~226 GiB) — the extra demand
and the 2× weights broadcast are absorbed peer-to-peer over InfiniBand.

## Operational notes (learned on live runs)

- **NCCL env is required** (baked into the manifest): IB needs `rdma-core` in the image
  *and* `NCCL_NVLS_ENABLE=0` (NVLS multicast hangs on H200 in a non-privileged container) +
  `NCCL_IB_HCA=^mlx5_8` (exclude the RoCE device). Without these NCCL either falls back to
  TCP or the first collective hangs.
- **memlock**: ColossalAI CPU-Adam offload pins ~88 GB host memory; the container default
  (64 KB) stalls it, so the os-train container takes the `SYS_RESOURCE` cap and runs
  `ulimit -l unlimited`.
- **Stragglers at large scale**: a single node with collapsed cold-read throughput starves
  its dataloader and deadlocks the synchronous job. At 128 nodes one node read at ~1 MiB/s;
  `kubectl cordon` it and raise blobcached concurrency (`[azure] workers`,
  `[transport] *_concurrency`) for 128+ node runs. See
  [blobcache#14](https://github.com/edwardsp/blobcache/issues/14).
