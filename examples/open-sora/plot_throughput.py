#!/usr/bin/env python3
"""
plot_throughput.py - Open-Sora v2 per-step training throughput + phase split.

Parses rank-0 stdout from each run's training log and produces:
  1. Per-step throughput curve (s/it, the seconds-per-training-step that
     Open-Sora's tqdm bar reports) vs global_step, one line per run. On a
     COLD cache the first steps stall on I/O and s/it falls as the cache
     fills (peer/local hits replace Azure round-trips); WARM/SHARD start flat.
  2. A separate "checkpoint-load vs training-steps" phase bar, because the
     11B weights are read in full through the osv2 blobcache mount BEFORE
     step 0 and never appear in s/it. This is the split the s/it curve hides.

Open-Sora is a diffusion *video* model, so there is no "tokens/sec"; the
honest per-step throughput metric is s/it (and its inverse, steps/min).

Log anchors (rank 0, ISO timestamps in '[YYYY-MM-DD HH:MM:SS]'):
  'Loading checkpoint from .../Open_Sora_v2.safetensors'  -> ckpt load start
  'Beginning epoch 0...'                                  -> first train step
tqdm step tokens: 'N/11 [MM:SS<REM, S.SSs/it, ... global_step=K ...]'

Usage:
  plot_throughput.py --run cold=logs/os-train-61.out \
                     --run warm=logs/os-train-62.out \
                     --run shard=logs/os-train-64.out \
                     --out-prefix results/64node/throughput-64node
"""
import argparse, json, re, sys
from datetime import datetime

ANSI = re.compile(r"\x1b\[[0-9;]*m")
TS = re.compile(r"\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]")
# tqdm token grammar: 'N/T [MM:SS<REM, S.SSs/it, loss=L, ..., global_step=K, ...]'
# groups: 1=bar_pos 2=bar_total 3=elapsed 4=s_it 5=loss 6=grad_norm 7=global_step
STEP = re.compile(
    r"(\d+)/(\d+)\s*\[(\d+:\d+(?::\d+)?)<[^,]*,\s*([\d.]+)s/it"
    r"(?:,\s*loss=([\d.eE+-]+))?"
    r"(?:[^]]*?global_grad_norm=([\d.eE+-]+))?"
    r"(?:[^]]*?global_step=(\d+))?"
)
COLORS = {"cold": "#d62728", "wsharded": "#2ca02c", "wreplicated": "#1f77b4",
          "warm": "#1f77b4", "shard": "#2ca02c",
          "broadcast": "#1f77b4", "hybrid": "#2ca02c"}


def elapsed_to_s(mmss):
    parts = [int(x) for x in mmss.split(":")]
    if len(parts) == 2:
        return parts[0] * 60 + parts[1]
    return parts[0] * 3600 + parts[1] * 60 + parts[2]


def parse_log(path):
    with open(path, errors="replace") as f:
        raw = f.read()
    text = ANSI.sub("", raw)
    r0 = [ln for ln in text.split("\n") if re.match(r"\s*0:", ln)]
    r0t = "\n".join(r0)

    def ts_of(substr):
        for ln in r0:
            if substr in ln:
                m = TS.search(ln)
                if m:
                    return datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
        return None

    ckpt_start = ts_of("Loading checkpoint from")
    vae = ts_of("hunyuan_vae")
    steps_start = ts_of("Beginning epoch")
    ckpt_load_s = ((steps_start - ckpt_start).total_seconds()
                   if ckpt_start and steps_start else None)

    per = {}
    for m in STEP.finditer(text):
        gs = m.group(7)
        if gs is None:
            continue
        gs = int(gs)
        el = elapsed_to_s(m.group(3))
        sit = float(m.group(4))
        loss = float(m.group(5)) if m.group(5) else None
        gnorm = float(m.group(6)) if m.group(6) else None
        if gs not in per or el >= per[gs][0]:
            per[gs] = (el, sit, loss, gnorm)
    steps = sorted(per)
    series = []
    prev = 0.0
    for gs in steps:
        el, sit, loss, gnorm = per[gs]
        dur = max(0.0, el - prev)
        prev = el
        series.append(dict(step=gs, elapsed_s=el, s_it_reported=sit,
                           step_dur_s=dur, loss=loss, grad_norm=gnorm))
    return dict(ckpt_load_s=ckpt_load_s,
                ckpt_start=str(ckpt_start) if ckpt_start else None,
                vae_start=str(vae) if vae else None,
                steps_start=str(steps_start) if steps_start else None,
                n_steps=len(series), series=series)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", action="append", required=True, help="label=logpath")
    ap.add_argument("--out-prefix", required=True)
    args = ap.parse_args()

    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    runs, order = {}, []
    for spec in args.run:
        label, path = spec.split("=", 1)
        try:
            runs[label] = parse_log(path)
            order.append(label)
        except FileNotFoundError:
            print(f"skip {label}: {path} not found", file=sys.stderr)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 5.5),
                                   gridspec_kw={"width_ratios": [2.3, 1]})

    # PITFALL: plot instantaneous step_dur_s, NOT tqdm's reported s/it. The
    # latter is a cumulative average dominated by step 0, so it "ramps down"
    # for every run regardless of cache state (an averaging artifact, not cache
    # warming). Step 0 itself is one-time framework warmup (~200-230s), so the
    # steady-state band is steps >= 1.
    warm0 = []
    for label in order:
        s = runs[label]["series"]
        if not s:
            continue
        steady = [d for d in s if d["step"] >= 1]
        xs = [d["step"] for d in steady]
        ys = [d["step_dur_s"] for d in steady]
        c = COLORS.get(label, "#888")
        ax1.plot(xs, ys, "-o", color=c, label=f"{label}", lw=2, ms=5)
        s0 = next((d["step_dur_s"] for d in s if d["step"] == 0), None)
        if s0 is not None:
            warm0.append(f"{label} {s0:.0f}s")
    ax1.set_xlabel("training step (global_step)")
    ax1.set_ylabel("seconds per step (instantaneous)  -  lower = faster")
    ax1.set_title("Steady-state per-step time is flat (~37s) and data-source-independent\n"
                  "cold = sharded = replicated: data fetch (blob/peer/local) hides under GPU compute")
    ax1.grid(alpha=0.3)
    ax1.legend(title="step duration")
    ax1.set_ylim(bottom=0)
    if warm0:
        ax1.text(0.5, 0.04,
                 "step 0 = one-time framework warmup (data-independent): "
                 + ", ".join(warm0),
                 transform=ax1.transAxes, ha="center", va="bottom", fontsize=8,
                 style="italic", color="#555",
                 bbox=dict(boxstyle="round", fc="#f5f5f5", ec="#ccc"))

    labels = [l for l in order if runs[l]["ckpt_load_s"] is not None]
    vals = [runs[l]["ckpt_load_s"] for l in labels]
    cs = [COLORS.get(l, "#888") for l in labels]
    bars = ax2.bar(labels, vals, color=cs, width=0.6)
    ax2.set_ylabel("checkpoint-load wall-time (s)")
    ax2.set_title("11B checkpoint load (osv2 mount)\n"
                  "constant: weights ALWAYS broadcast - every node needs them to start")
    ax2.grid(axis="y", alpha=0.3)
    top = max(vals) if vals else 1
    for b, v in zip(bars, vals):
        ax2.text(b.get_x() + b.get_width() / 2, v + top * 0.02, f"{v:.0f}s",
                 ha="center", va="bottom", fontsize=11, fontweight="bold")
    if vals:
        mean_v = sum(vals) / len(vals)
        ax2.axhline(mean_v, color="#555", ls="--", lw=1)
        ax2.annotate(f"~constant ({mean_v:.0f}s avg)\nweights pre-broadcast in hydrate,\n"
                     f"read from local NVMe at load",
                     xy=(0, mean_v), xytext=(0.5, top * 0.45),
                     textcoords=("axes fraction", "data"), ha="center", fontsize=8.5,
                     color="#333")
    ax2.set_ylim(0, top * 1.18)

    fig.suptitle("Open-Sora v2 training - throughput over time + checkpoint-load split "
                 "(64 nodes x 8 = 512 GPUs)", fontsize=13, fontweight="bold")
    fig.tight_layout(rect=[0, 0, 1, 0.94])
    png = f"{args.out_prefix}.png"
    fig.savefig(png, dpi=130)
    print(f"wrote {png}")

    js = f"{args.out_prefix}.json"
    with open(js, "w") as f:
        json.dump({l: runs[l] for l in order}, f, indent=2)
    print(f"wrote {js}")
    for l in order:
        r = runs[l]
        print(f"{l}: ckpt_load={r['ckpt_load_s']}s n_steps={r['n_steps']} "
              f"first_s_it={r['series'][0]['s_it_reported'] if r['series'] else '?'} "
              f"last_s_it={r['series'][-1]['s_it_reported'] if r['series'] else '?'}")


if __name__ == "__main__":
    main()
