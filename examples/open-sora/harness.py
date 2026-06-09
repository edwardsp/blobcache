#!/usr/bin/env python3
"""
Open-Sora x blobcache dataloader-throughput harness (Phase A).

Exercises Open-Sora's REAL video read path (opensora.datasets.read_video +
temporal_random_crop) over a torch DataLoader with N workers, across
torchrun/srun ranks. Each sample reads a whole mp4 through the /blobcache FUSE
mount -> stresses blobcache (Azure egress vs peer-fetch).

Measures per-rank and aggregate samples/s and MB/s. Geometry only affects the
post-decode crop, not bytes fetched, so a fixed target geometry is fine.

Run (inside the os-harness container), one proc per GPU via srun/torchrun:
  PYTHONPATH=<opensora> python harness.py \
    --csv /blobcache/pexels/pexels.csv --steps 200 --workers 8 \
    --batch 4 --num-frames 33 --height 256 --width 256
"""
import argparse, os, time, sys
import pandas as pd
import torch
import torch.distributed as dist
from torch.utils.data import Dataset, DataLoader, DistributedSampler

from opensora.datasets.read_video import read_video
from opensora.datasets.utils import temporal_random_crop


def env_int(name, default):
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


class VideoStreamDataset(Dataset):
    """Faithful to Open-Sora's get_video() byte path; fixed geometry for batching."""

    def __init__(self, csv_path, num_frames, height, width, sampling_interval=1):
        df = pd.read_csv(csv_path)
        assert "path" in df.columns, "CSV must have a 'path' column"
        self.paths = df["path"].tolist()
        self.num_frames = num_frames
        self.height = height
        self.width = width
        self.sampling_interval = sampling_interval

    def __len__(self):
        return len(self.paths)

    def __getitem__(self, index):
        path = self.paths[index]
        try:
            nbytes = os.path.getsize(path)  # whole-file read = bytes streamed through blobcache
            vframes, _ = read_video(path, backend="av")
            video = temporal_random_crop(vframes, self.num_frames, self.sampling_interval)
            video = video.clone().float()
            video = torch.nn.functional.interpolate(
                video, size=(self.height, self.width), mode="bilinear", align_corners=False
            )
            del vframes
            return video, nbytes, 1
        except Exception as e:  # skip unreadable sample, keep throughput honest
            sys.stderr.write(f"[skip] {path}: {e}\n")
            return torch.zeros(self.num_frames, 3, self.height, self.width), 0, 0


def collate(batch):
    vids = torch.stack([b[0] for b in batch])
    nbytes = sum(b[1] for b in batch)
    ok = sum(b[2] for b in batch)
    return vids, nbytes, ok


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True)
    ap.add_argument("--steps", type=int, default=200)
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--batch", type=int, default=4)
    ap.add_argument("--num-frames", type=int, default=33)
    ap.add_argument("--height", type=int, default=256)
    ap.add_argument("--width", type=int, default=256)
    ap.add_argument("--warmup", type=int, default=5)
    args = ap.parse_args()

    rank = env_int("RANK", env_int("SLURM_PROCID", 0))
    world = env_int("WORLD_SIZE", env_int("SLURM_NTASKS", 1))
    local_rank = env_int("LOCAL_RANK", env_int("SLURM_LOCALID", 0))

    distributed = world > 1
    if distributed:
        dist.init_process_group(backend="gloo")  # cpu metadata only; data path is FUSE

    ds = VideoStreamDataset(args.csv, args.num_frames, args.height, args.width)
    sampler = DistributedSampler(ds, num_replicas=world, rank=rank, shuffle=True) if distributed else None
    dl = DataLoader(
        ds, batch_size=args.batch, sampler=sampler, shuffle=(sampler is None),
        num_workers=args.workers, collate_fn=collate, pin_memory=False,
        persistent_workers=(args.workers > 0), prefetch_factor=(4 if args.workers > 0 else None),
        drop_last=True,
    )

    if rank == 0:
        print(f"[harness] world={world} dataset={len(ds)} steps={args.steps} "
              f"batch={args.batch} workers={args.workers} "
              f"geom={args.num_frames}x{args.height}x{args.width}", flush=True)

    it = iter(dl)
    samples = 0
    total_bytes = 0
    ok_total = 0
    t_start = None
    for step in range(args.steps):
        try:
            vids, nbytes, ok = next(it)
        except StopIteration:
            it = iter(dl)
            vids, nbytes, ok = next(it)
        if step == args.warmup:
            t_start = time.perf_counter()
            samples = 0; total_bytes = 0; ok_total = 0
        if t_start is not None:
            samples += vids.shape[0]
            total_bytes += nbytes
            ok_total += ok

    elapsed = max(time.perf_counter() - t_start, 1e-6) if t_start else 1e-6
    sps = samples / elapsed
    mbps = (total_bytes / 1e6) / elapsed

    if distributed:
        t = torch.tensor([samples, total_bytes, ok_total, elapsed], dtype=torch.float64)
        gathered = [torch.zeros_like(t) for _ in range(world)]
        dist.all_gather(gathered, t)
    else:
        gathered = [torch.tensor([samples, total_bytes, ok_total, elapsed], dtype=torch.float64)]

    print(f"[rank {rank:03d}] samples={samples} ok={ok_total} "
          f"MB={total_bytes/1e6:.1f} elapsed={elapsed:.1f}s "
          f"-> {sps:.2f} samp/s {mbps:.1f} MB/s", flush=True)

    if rank == 0:
        agg_samples = sum(g[0].item() for g in gathered)
        agg_bytes = sum(g[1].item() for g in gathered)
        max_elapsed = max(g[3].item() for g in gathered)
        print("=" * 70, flush=True)
        print(f"[AGGREGATE] ranks={world} samples={int(agg_samples)} "
              f"GB={agg_bytes/1e9:.2f} wall={max_elapsed:.1f}s", flush=True)
        print(f"[AGGREGATE] {agg_samples/max_elapsed:.1f} samp/s  "
              f"{(agg_bytes/1e6)/max_elapsed:.1f} MB/s  "
              f"({(agg_bytes*8/1e9)/max_elapsed:.2f} Gbps)", flush=True)
        print("=" * 70, flush=True)

    if distributed:
        dist.barrier()
        dist.destroy_process_group()


if __name__ == "__main__":
    main()
