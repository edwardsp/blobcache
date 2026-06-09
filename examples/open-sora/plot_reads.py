#!/usr/bin/env python3
"""
plot_reads.py - blobcache read-source split + filesystem demand over a run.

Inputs (per run): the per-node sampler CSVs written by os-train.sbatch,
    {metrics_dir}/{RUN_ID}-<host>.csv
    cols: epoch_ms,host,blob_fetches,blob_bytes,peer_fetches,peer_bytes,
          cache_hits,cache_misses,cache_bytes,fuse_reads,fuse_read_bytes
plus markers {metrics_dir}/{RUN_ID}.markers (TRAIN_START / TRAIN_DONE epoch_ms).

Read-source identity (per interval, summed cluster-wide):
    total bytes the app pulled via FUSE = d(fuse_read_bytes)
    peer-served                         = d(peer_bytes)
    blob-served (Azure origin egress)   = d(blob_bytes)
    local (NVMe cache hit, no network)  = total - peer - blob
Filesystem demand the workload imposes:
    bandwidth (GiB/s) = d(fuse_read_bytes)/dt
    IOPS              = d(fuse_reads)/dt

Emits:
    {out}.png          read-source stacked area, one column per run
    {out}-fsdemand.png cluster bandwidth + IOPS, one line per run
    {out}.timings.json totals + peak/mean FS demand per run

Usage:
    plot_reads.py --metrics-dir DIR --run cold=cold-101 --run wsharded=... \
        --run wreplicated=... --out-prefix DIR/reads
"""
import argparse, csv, glob, json, os, sys
from collections import defaultdict

GIB = 1073741824.0
GBPS = 8.0 / 1e9  # bytes/s -> gigabits/s
COLORS = {"cold": "#d62728", "wsharded": "#2ca02c", "wreplicated": "#1f77b4",
          "warm": "#1f77b4", "shard": "#2ca02c"}
SRC_COLORS = {"local": "#1f77b4", "peer": "#2ca02c", "blob": "#d62728"}


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


def load_run(metrics_dir, run_id):
    files = sorted(glob.glob(os.path.join(metrics_dir, f"{run_id}-*.csv")))
    if not files:
        sys.exit(f"no CSVs for run_id={run_id} in {metrics_dir}")
    per_node = defaultdict(list)
    for fp in files:
        with open(fp) as f:
            for row in csv.DictReader(f):
                try:
                    per_node[row["host"]].append((
                        int(row["epoch_ms"]),
                        int(float(row["blob_bytes"])),
                        int(float(row["peer_bytes"])),
                        int(float(row.get("fuse_read_bytes", 0) or 0)),
                        int(float(row.get("fuse_reads", 0) or 0)),
                    ))
                except (ValueError, KeyError):
                    continue
    for h in per_node:
        per_node[h].sort()
    return per_node, files


def rate_series(per_node, t0_ms, bucket_s):
    """Bin per-node cumulative-counter deltas into cluster-wide per-bucket rates.

    Returns dict of time-aligned lists plus integrated totals. local = clamp(
    fuse_read - peer - blob) so genuine local cache hits are separated from
    network-served bytes.
    """
    blob_b = defaultdict(float)
    peer_b = defaultdict(float)
    local_b = defaultdict(float)
    total_b = defaultdict(float)
    iops = defaultdict(float)
    tb = tp = tt = tr = 0
    for samples in per_node.values():
        for (e0, b0, p0, f0, r0), (e1, b1, p1, f1, r1) in zip(samples, samples[1:]):
            dt = (e1 - e0) / 1000.0
            if dt <= 0:
                continue
            db = max(0, b1 - b0)
            dp = max(0, p1 - p0)
            df = max(0, f1 - f0)
            dr = max(0, r1 - r0)
            dl = max(0, df - dp - db)
            k = int(((e0 + e1) / 2.0 - t0_ms) / 1000.0 // bucket_s)
            blob_b[k] += db
            peer_b[k] += dp
            local_b[k] += dl
            total_b[k] += df
            iops[k] += dr
            tb += db; tp += dp; tt += df; tr += dr
    ks = sorted(set(blob_b) | set(peer_b) | set(local_b) | set(total_b))
    times = [k * bucket_s + bucket_s / 2.0 for k in ks]
    return dict(
        times=times,
        blob_Bps=[blob_b[k] / bucket_s for k in ks],
        peer_Bps=[peer_b[k] / bucket_s for k in ks],
        local_Bps=[local_b[k] / bucket_s for k in ks],
        total_Bps=[total_b[k] / bucket_s for k in ks],
        iops=[iops[k] / bucket_s for k in ks],
        tot_blob=tb, tot_peer=tp, tot_local=max(0, tt - tp - tb), tot_fuse=tt, tot_reads=tr,
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--metrics-dir", required=True)
    ap.add_argument("--run", action="append", required=True, help="label=RUN_ID")
    ap.add_argument("--bucket-s", type=float, default=10.0)
    ap.add_argument("--out-prefix", required=True)
    args = ap.parse_args()

    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    runs, order = {}, []
    for spec in args.run:
        label, run_id = spec.split("=", 1)
        per_node, files = load_run(args.metrics_dir, run_id)
        mk = load_markers(os.path.join(args.metrics_dir, f"{run_id}.markers"))
        first = min(s[0][0] for s in per_node.values())
        t0 = mk.get("JOB_START") or first
        s = rate_series(per_node, t0, args.bucket_s)
        s["run_id"] = run_id
        s["nodes"] = mk.get("NODES")
        s["n_csv"] = len(files)
        s["train_start_s"] = (mk["TRAIN_START"] - t0) / 1000.0 if "TRAIN_START" in mk else None
        s["train_done_s"] = (mk["TRAIN_DONE"] - t0) / 1000.0 if "TRAIN_DONE" in mk else None
        runs[label] = s
        order.append(label)

    fig, axes = plt.subplots(1, len(order), figsize=(6.2 * len(order), 5.2),
                             sharey=True, squeeze=False)
    for ax, label in zip(axes[0], order):
        r = runs[label]
        t = r["times"]
        ax.stackplot(
            t,
            [v * GBPS for v in r["local_Bps"]],
            [v * GBPS for v in r["peer_Bps"]],
            [v * GBPS for v in r["blob_Bps"]],
            labels=["local (NVMe hit)", "peer (P2P/IB)", "blob (Azure egress)"],
            colors=[SRC_COLORS["local"], SRC_COLORS["peer"], SRC_COLORS["blob"]],
            alpha=0.85,
        )
        if r["train_start_s"] is not None:
            ax.axvline(r["train_start_s"], color="#333", ls="--", lw=1)
            ax.text(r["train_start_s"], ax.get_ylim()[1] * 0.97, " train start",
                    fontsize=7, va="top", color="#333")
        b = r["tot_blob"] / GIB; p = r["tot_peer"] / GIB; lo = r["tot_local"] / GIB
        ax.set_title(f"{label}  ({r['run_id']})\n"
                     f"local {lo:.0f} / peer {p:.0f} / blob {b:.0f} GiB",
                     fontsize=10)
        ax.set_xlabel("seconds since JOB_START")
        ax.grid(alpha=0.3)
    axes[0][0].set_ylabel("FUSE read served (Gbps)")
    axes[0][0].legend(loc="upper right", fontsize=8)
    n = runs[order[0]].get("nodes", "?")
    fig.suptitle(f"blobcache read-source split - where every byte came from "
                 f"(weights always broadcast; only DATA strategy varies) - {n} nodes",
                 fontsize=12, fontweight="bold")
    fig.tight_layout(rect=[0, 0, 1, 0.93])
    png = f"{args.out_prefix}.png"
    fig.savefig(png, dpi=130)
    print(f"wrote {png}")

    fig2, (axb, axi) = plt.subplots(2, 1, figsize=(13, 8), sharex=True)
    for label in order:
        r = runs[label]
        c = COLORS.get(label, "#888")
        axb.plot(r["times"], [v / GIB for v in r["total_Bps"]], color=c, lw=1.8, label=label)
        axi.plot(r["times"], r["iops"], color=c, lw=1.8, label=label)
        if r["train_start_s"] is not None:
            for ax in (axb, axi):
                ax.axvline(r["train_start_s"], color=c, ls=":", lw=0.8, alpha=0.5)
    axb.set_ylabel("cluster FUSE read bandwidth (GiB/s)")
    axb.set_title("Total filesystem demand the workload imposes (read bandwidth)\n"
                  "peak during 11B checkpoint load; sustained during data loading",
                  fontsize=11)
    axb.legend(fontsize=9); axb.grid(alpha=0.3)
    axi.set_ylabel("cluster FUSE read IOPS")
    axi.set_xlabel("seconds since JOB_START")
    axi.set_title("Total filesystem demand (read operations/s)", fontsize=11)
    axi.legend(fontsize=9); axi.grid(alpha=0.3)
    fig2.tight_layout()
    png2 = f"{args.out_prefix}-fsdemand.png"
    fig2.savefig(png2, dpi=130)
    print(f"wrote {png2}")

    summary = {}
    for label in order:
        r = runs[label]
        tot = r["tot_fuse"] or 1
        summary[label] = dict(
            run_id=r["run_id"], nodes=r["nodes"], node_csvs=r["n_csv"],
            train_start_s=round(r["train_start_s"], 1) if r["train_start_s"] else None,
            train_done_s=round(r["train_done_s"], 1) if r["train_done_s"] else None,
            total_fuse_read_GiB=round(r["tot_fuse"] / GIB, 1),
            local_GiB=round(r["tot_local"] / GIB, 1),
            peer_GiB=round(r["tot_peer"] / GIB, 1),
            blob_GiB=round(r["tot_blob"] / GIB, 1),
            local_pct=round(100 * r["tot_local"] / tot, 1),
            peer_pct=round(100 * r["tot_peer"] / tot, 1),
            blob_pct=round(100 * r["tot_blob"] / tot, 1),
            peak_bandwidth_GiBps=round(max(r["total_Bps"], default=0) / GIB, 2),
            peak_iops=round(max(r["iops"], default=0), 0),
            total_read_ops_M=round(r["tot_reads"] / 1e6, 2),
        )
    js = f"{args.out_prefix}.timings.json"
    with open(js, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"wrote {js}")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
