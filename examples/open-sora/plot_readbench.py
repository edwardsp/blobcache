#!/usr/bin/env python3
"""
plot_readbench.py - read-source split + filesystem demand for the READ BENCHMARK.

Unlike the training runs (which carry an in-job time-series sampler), the read
benchmark is bracketed by two cluster-wide counter snapshots taken with
snap_metrics.sh: rb-<tag>-before.csv and rb-<tag>-after.csv. Each snapshot has
one row per node of cumulative blobcache counters plus a ts_ms wall clock. We
diff after-before per node and sum cluster-wide.

Snapshot columns (snap_metrics.sh):
    host,blob_fetches,blob_bytes,peer_fetches,peer_bytes,cache_hits,
    cache_misses,cache_bytes,fuse_reads,fuse_read_bytes,ts_ms

Read-source identity (same as the training plotter):
    total bytes pulled via FUSE = d(fuse_read_bytes)
    peer-served                 = d(peer_bytes)
    blob-served (Azure egress)  = d(blob_bytes)
    local (NVMe cache hit)      = total - peer - blob
Filesystem demand over the bracket window (dt = mean per-node d(ts_ms)):
    bandwidth (GiB/s) = d(fuse_read_bytes)/dt
    IOPS              = d(fuse_reads)/dt

Emits:
    {out}.png          grouped read-source bars (GiB), one group per scenario
    {out}-fsdemand.png bandwidth + IOPS bars, one group per scenario
    {out}.json         per-scenario totals + pct + bandwidth + IOPS

Usage:
    plot_readbench.py --bench-dir DIR --run cold=rb-cold \
        --run wsharded=rb-wsharded --run wreplicated=rb-wreplicated \
        --out-prefix DIR/readbench
"""
import argparse, csv, json, os, sys

GIB = 1073741824.0
COLORS = {"cold": "#d62728", "wsharded": "#2ca02c", "wreplicated": "#1f77b4"}
SRC_COLORS = {"local": "#1f77b4", "peer": "#2ca02c", "blob": "#d62728"}


def load_snapshot(path):
    rows = {}
    with open(path) as f:
        for row in csv.DictReader(f):
            try:
                rows[row["host"]] = {
                    "blob_bytes": int(float(row["blob_bytes"])),
                    "peer_bytes": int(float(row["peer_bytes"])),
                    "fuse_read_bytes": int(float(row.get("fuse_read_bytes", 0) or 0)),
                    "fuse_reads": int(float(row.get("fuse_reads", 0) or 0)),
                    "ts_ms": int(float(row["ts_ms"])),
                }
            except (ValueError, KeyError):
                continue
    return rows


def diff_run(bench_dir, prefix):
    before = load_snapshot(os.path.join(bench_dir, f"{prefix}-before.csv"))
    after = load_snapshot(os.path.join(bench_dir, f"{prefix}-after.csv"))
    hosts = sorted(set(before) & set(after))
    if not hosts:
        sys.exit(f"no common hosts between {prefix}-before/after in {bench_dir}")
    tot_blob = tot_peer = tot_local = tot_fuse = tot_reads = 0
    dt_sum = 0.0
    for h in hosts:
        b, a = before[h], after[h]
        dblob = max(0, a["blob_bytes"] - b["blob_bytes"])
        dpeer = max(0, a["peer_bytes"] - b["peer_bytes"])
        dfuse = max(0, a["fuse_read_bytes"] - b["fuse_read_bytes"])
        dread = max(0, a["fuse_reads"] - b["fuse_reads"])
        dlocal = max(0, dfuse - dpeer - dblob)
        tot_blob += dblob
        tot_peer += dpeer
        tot_local += dlocal
        tot_fuse += dfuse
        tot_reads += dread
        dt_sum += max(0, a["ts_ms"] - b["ts_ms"]) / 1000.0
    dt = dt_sum / len(hosts) if hosts else 1.0  # mean per-node bracket seconds
    dt = dt or 1.0
    return dict(
        prefix=prefix, n_hosts=len(hosts), dt_s=dt,
        tot_blob=tot_blob, tot_peer=tot_peer, tot_local=tot_local,
        tot_fuse=tot_fuse, tot_reads=tot_reads,
        bandwidth_GiBps=(tot_fuse / GIB) / dt,
        iops=tot_reads / dt,
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bench-dir", required=True)
    ap.add_argument("--run", action="append", required=True, help="label=PREFIX")
    ap.add_argument("--out-prefix", required=True)
    args = ap.parse_args()

    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np

    runs, order = {}, []
    for spec in args.run:
        label, prefix = spec.split("=", 1)
        runs[label] = diff_run(args.bench_dir, prefix)
        order.append(label)

    fig, ax = plt.subplots(figsize=(1.9 * len(order) + 3, 5.4))
    x = np.arange(len(order))
    local = [runs[l]["tot_local"] / GIB for l in order]
    peer = [runs[l]["tot_peer"] / GIB for l in order]
    blob = [runs[l]["tot_blob"] / GIB for l in order]
    ax.bar(x, local, color=SRC_COLORS["local"], label="local (NVMe hit)")
    ax.bar(x, peer, bottom=local, color=SRC_COLORS["peer"], label="peer (P2P/IB)")
    ax.bar(x, blob, bottom=[l + p for l, p in zip(local, peer)],
           color=SRC_COLORS["blob"], label="blob (Azure egress)")
    for i, l in enumerate(order):
        tot = runs[l]["tot_fuse"] / GIB
        ax.text(i, tot, f"{tot:.0f}\nGiB", ha="center", va="bottom", fontsize=9)
    ax.set_xticks(x)
    ax.set_xticklabels([f"{l}\n({runs[l]['prefix']})" for l in order])
    ax.set_ylabel("FUSE bytes read during benchmark (GiB)")
    ax.set_title("Read-benchmark read-source split - where every byte came from\n"
                 "(weights always broadcast; only DATA strategy varies)",
                 fontsize=12, fontweight="bold")
    ax.legend(loc="upper right", fontsize=9)
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    png = f"{args.out_prefix}.png"
    fig.savefig(png, dpi=130)
    print(f"wrote {png}")

    fig2, (axb, axi) = plt.subplots(1, 2, figsize=(4.2 * 2 + 2, 5.0))
    cols = [COLORS.get(l, "#888") for l in order]
    axb.bar(x, [runs[l]["bandwidth_GiBps"] for l in order], color=cols)
    for i, l in enumerate(order):
        axb.text(i, runs[l]["bandwidth_GiBps"], f"{runs[l]['bandwidth_GiBps']:.2f}",
                 ha="center", va="bottom", fontsize=9)
    axb.set_xticks(x); axb.set_xticklabels(order)
    axb.set_ylabel("cluster FUSE read bandwidth (GiB/s)")
    axb.set_title("Filesystem demand - read bandwidth", fontsize=11)
    axb.grid(axis="y", alpha=0.3)
    axi.bar(x, [runs[l]["iops"] for l in order], color=cols)
    for i, l in enumerate(order):
        axi.text(i, runs[l]["iops"], f"{runs[l]['iops']:.0f}",
                 ha="center", va="bottom", fontsize=9)
    axi.set_xticks(x); axi.set_xticklabels(order)
    axi.set_ylabel("cluster FUSE read IOPS")
    axi.set_title("Filesystem demand - read operations/s", fontsize=11)
    axi.grid(axis="y", alpha=0.3)
    fig2.suptitle("Read-benchmark total filesystem requirement (per data strategy)",
                  fontsize=12, fontweight="bold")
    fig2.tight_layout(rect=[0, 0, 1, 0.95])
    png2 = f"{args.out_prefix}-fsdemand.png"
    fig2.savefig(png2, dpi=130)
    print(f"wrote {png2}")

    summary = {}
    for l in order:
        r = runs[l]
        tot = r["tot_fuse"] or 1
        summary[l] = dict(
            prefix=r["prefix"], n_hosts=r["n_hosts"], bracket_s=round(r["dt_s"], 1),
            total_fuse_read_GiB=round(r["tot_fuse"] / GIB, 1),
            local_GiB=round(r["tot_local"] / GIB, 1),
            peer_GiB=round(r["tot_peer"] / GIB, 1),
            blob_GiB=round(r["tot_blob"] / GIB, 1),
            local_pct=round(100 * r["tot_local"] / tot, 1),
            peer_pct=round(100 * r["tot_peer"] / tot, 1),
            blob_pct=round(100 * r["tot_blob"] / tot, 1),
            bandwidth_GiBps=round(r["bandwidth_GiBps"], 2),
            iops=round(r["iops"], 0),
            total_read_ops_M=round(r["tot_reads"] / 1e6, 2),
        )
    js = f"{args.out_prefix}.json"
    with open(js, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"wrote {js}")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
