#!/usr/bin/env python3
"""Aggregate azcp matrix results: per-pod wall+bytes -> aggregate throughput.

Mirrors the schema of `bench/results-azcp/N{1,2,4,8,16}/` written by
`bench/azcp-matrix.sh`:
    {pod}.wall_rc   -> "<wall_seconds> <exit_code>"
    {pod}.bytes     -> total bytes written to /mnt/nvme/azcp-test on that pod
    pods.txt        -> pod names that ran in this N

Aggregate throughput convention is the same as bench/analyze.py for the
hydrate matrix: total bytes across all shards divided by max wall, so it
reflects the wall-clock the slowest shard imposes on the fleet.
"""
from __future__ import annotations
import sys, glob, os
from pathlib import Path

GIB = 1024**3
GBPS = 1e9 / 8  # bytes/sec for 1 Gbps

def load_n(root: Path, n: int):
    d = root / f"N{n}"
    if not d.is_dir():
        return None
    walls = {}
    rcs = {}
    bytez = {}
    for f in sorted(glob.glob(str(d / "*.wall_rc"))):
        pod = Path(f).stem
        parts = open(f).read().split()
        walls[pod] = float(parts[0])
        rcs[pod] = int(parts[1])
        bf = d / f"{pod}.bytes"
        bytez[pod] = int(open(bf).read().strip()) if bf.exists() else 0
    return walls, rcs, bytez

def fmt_gbps(b_per_s):
    return f"{b_per_s/GBPS:.2f}"

def main():
    root = Path(sys.argv[1] if len(sys.argv) > 1 else "bench/results-azcp")
    rows = []
    for n in (1, 2, 4, 8, 16):
        r = load_n(root, n)
        if r is None:
            continue
        walls, rcs, bytez = r
        total_bytes = sum(bytez.values())
        max_wall = max(walls.values()) if walls else 0
        n_pods = len(walls)
        n_failed = sum(1 for rc in rcs.values() if rc != 0)
        agg_bps = total_bytes / max_wall if max_wall > 0 else 0
        per_node_bps = agg_bps / n_pods if n_pods else 0
        rows.append((n, n_pods, n_failed, total_bytes, max_wall, agg_bps, per_node_bps))

    print(f"{'N':>3} {'pods':>4} {'fail':>4} {'total_GiB':>10} {'wall_s':>8} {'agg_Gbps':>9} {'agg_GiB/s':>10} {'per-node_Gbps':>14}")
    for n, p, f, b, w, agg, pn in rows:
        print(f"{n:>3} {p:>4} {f:>4} {b/GIB:>10.1f} {w:>8.2f} {fmt_gbps(agg):>9} {agg/GIB:>10.2f} {fmt_gbps(pn):>14}")

if __name__ == "__main__":
    main()
