#!/usr/bin/env python3
"""
plot_summary.py - blobcache COLD vs WARM summary bar charts.

Aggregates per-node CSV deltas (last - first sample) across all nodes in a run
and renders a 4-panel comparison that shows *where the reads came from*:
  1. Azure blob bytes read   (origin egress)
  2. Peer (P2P/IB) bytes read (cross-node serving)
  3. Local cache-hit reads    (served from local NVMe, no network)
  4. Training wall-time       (from markers)

Usage:
  plot_summary.py --metrics-dir DIR --run cold=cold-61 --run warm=warm-62 \
      --out-prefix DIR/summary
"""
import argparse, csv, glob, json, os, sys
from collections import defaultdict

GIB = 1073741824.0


def load_markers(path):
    m = {}
    if not os.path.exists(path):
        return m
    with open(path) as f:
        for line in f:
            parts = line.split()
            if len(parts) == 2 and parts[0].isdigit():
                m[parts[1]] = int(parts[0])
            elif "=" in line:
                k, v = line.strip().split("=", 1)
                m[k] = v
    return m


def agg_run(metrics_dir, run_id):
    files = sorted(glob.glob(os.path.join(metrics_dir, f"{run_id}-*.csv")))
    if not files:
        sys.exit(f"no CSVs for {run_id} in {metrics_dir}")
    tot = defaultdict(float)
    keys = ("blob_bytes", "peer_bytes", "cache_hits", "cache_misses",
            "fuse_reads", "fuse_read_bytes")
    for fp in files:
        rows = []
        with open(fp) as f:
            for row in csv.DictReader(f):
                try:
                    rows.append({k: int(float(row.get(k, 0) or 0)) for k in keys})
                except (ValueError, KeyError):
                    continue
        if len(rows) < 2:
            continue
        for k in rows[0]:
            tot[k] += max(0, rows[-1][k] - rows[0][k])
    mk = load_markers(os.path.join(metrics_dir, f"{run_id}.markers"))
    train_s = None
    if "TRAIN_START" in mk and "TRAIN_DONE" in mk:
        train_s = (mk["TRAIN_DONE"] - mk["TRAIN_START"]) / 1000.0
    # local cache-served bytes = everything the app read minus what crossed the network
    local_bytes = max(0.0, tot["fuse_read_bytes"] - tot["peer_bytes"] - tot["blob_bytes"])
    return dict(run_id=run_id, nodes=mk.get("NODES"), n=len(files),
                blob_GiB=tot["blob_bytes"] / GIB, peer_GiB=tot["peer_bytes"] / GIB,
                local_GiB=local_bytes / GIB,
                fuse_read_GiB=tot["fuse_read_bytes"] / GIB,
                remote_GiB=(tot["blob_bytes"] + tot["peer_bytes"]) / GIB,
                cache_hits_M=tot["cache_hits"] / 1e6, misses_M=tot["cache_misses"] / 1e6,
                fuse_reads_M=tot["fuse_reads"] / 1e6, train_s=train_s)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--metrics-dir", required=True)
    ap.add_argument("--run", action="append", required=True, help="label=RUN_ID")
    ap.add_argument("--out-prefix", required=True)
    args = ap.parse_args()

    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    runs = {}
    order = []
    for spec in args.run:
        label, run_id = spec.split("=", 1)
        runs[label] = agg_run(args.metrics_dir, run_id)
        order.append(label)

    colors = {"cold": "#d62728", "warm": "#1f77b4", "shard": "#2ca02c"}
    cs = [colors.get(l, "#888888") for l in order]
    labels = [f"{l}\n({runs[l]['run_id']})" for l in order]

    panels = [
        ("Local read (GiB)\nNVMe cache hit, no network", "local_GiB", "{:.1f}"),
        ("Peer P2P read over IB (GiB)\ncross-node serving", "peer_GiB", "{:.1f}"),
        ("Azure blob read (GiB)\norigin egress - lower = better", "blob_GiB", "{:.1f}"),
        ("Training wall-time (s)\nlower = faster", "train_s", "{:.0f}"),
    ]
    fig, axes = plt.subplots(1, 4, figsize=(16, 5))
    for ax, (title, key, fmt) in zip(axes, panels):
        vals = [runs[l][key] or 0 for l in order]
        bars = ax.bar(labels, vals, color=cs, width=0.6)
        ax.set_title(title, fontsize=10)
        ax.grid(axis="y", alpha=0.3)
        top = max(vals) if max(vals) > 0 else 1
        for b, v in zip(bars, vals):
            ax.text(b.get_x() + b.get_width() / 2, v + top * 0.02,
                    fmt.format(v), ha="center", va="bottom", fontsize=10, fontweight="bold")
        ax.set_ylim(0, top * 1.18)
    n = runs[order[0]].get("nodes", "?")
    fig.suptitle(f"blobcache read-source by data strategy - Open-Sora v2, {n} nodes x 8 GPU = "
                 f"{int(n) * 8 if str(n).isdigit() else '?'} GPUs "
                 f"(weights always broadcast)",
                 fontsize=13, fontweight="bold")
    fig.tight_layout(rect=[0, 0, 1, 0.95])
    png = f"{args.out_prefix}.png"
    fig.savefig(png, dpi=130)
    print(f"wrote {png}")

    js = f"{args.out_prefix}.json"
    out = {l: {k: (round(v, 3) if isinstance(v, float) else v)
               for k, v in runs[l].items()} for l in order}
    # derived comparison: cold (data from blob) vs warm-replicated (data fully local)
    if "cold" in runs and "wreplicated" in runs:
        c, w = runs["cold"], runs["wreplicated"]
        out["comparison"] = dict(
            blob_egress_avoided_GiB=round(c["blob_GiB"] - w["blob_GiB"], 1),
            cold_blob_GiB=round(c["blob_GiB"], 1),
            replicated_local_pct=round(100 * w["local_GiB"] / w["fuse_read_GiB"], 1)
            if w["fuse_read_GiB"] > 0 else 0.0,
            train_speedup_pct=round(100 * (c["train_s"] - w["train_s"]) / c["train_s"], 1)
            if c["train_s"] and w["train_s"] else None,
        )
    with open(js, "w") as f:
        json.dump(out, f, indent=2)
    print(f"wrote {js}")
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
