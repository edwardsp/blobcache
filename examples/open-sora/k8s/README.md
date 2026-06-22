# Open-Sora v2 × blobcache on AKS (Kubernetes port)

Kubernetes port of the Slurm benchmark in [`../`](..) — see
[`docs/examples/open-sora-pexels.md`](../../../docs/examples/open-sora-pexels.md) for the
methodology and reference numbers. Provision the cluster with
[azcluster](https://github.com/edwardsp/azcluster) (`deploy --target aks`, ND H200 pool,
ACStor + NVIDIA network/GPU operators).

> **Status: untested first draft.** Depends on the `os-train` image (PR #12) building
> and on `pyshim` being baked into it. Treat the manifest + steps below as the design,
> to be iterated on a live cluster.

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

- `pyshim` (the author's `/shared/blobcache-deploy/pyshim`) must be baked into the image.
- `pexels_meta.csv` location on AKS (default assumes it ships in the pexels blob at
  `/blobcache/pexels/pexels_meta.csv`; override `PEXELS_META_CSV` otherwise).
- ACStor `cache` PVC sized at 2 TiB/pod — tune to the node's NVMe RAID capacity.
- Hydrate barrier + rank-0 rendezvous ordering need validation on a live cluster.
