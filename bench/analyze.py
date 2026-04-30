import json, sys, glob, os, re

GIB = 1024**3
GBPS_BYTES_PER_SEC = 1e9 / 8

def parse_prom(path):
    out = {}
    with open(path) as f:
        for line in f:
            if line.startswith('#') or not line.strip(): continue
            parts = line.rstrip().split(' ')
            if len(parts) < 2: continue
            key, val = parts[0], parts[-1]
            try: val = float(val)
            except: continue
            out[key] = val
    return out

def diff(post, pre, key):
    return post.get(key, 0) - pre.get(key, 0)

def diff_label(post, pre, prefix):
    keys = set(k for k in list(post)+list(pre) if k.startswith(prefix))
    return {k: diff(post, pre, k) for k in keys if diff(post, pre, k) != 0}

def hist_delta(post, pre, metric):
    bucket_re = re.compile(rf'^{re.escape(metric)}_bucket\{{le="([^"]+)"\}}$')
    buckets = []
    for key in set(list(post) + list(pre)):
        m = bucket_re.match(key)
        if not m:
            continue
        le = m.group(1)
        bound = float('inf') if le == '+Inf' else float(le)
        buckets.append((bound, diff(post, pre, key)))
    buckets.sort(key=lambda x: x[0])
    total_count = diff(post, pre, f'{metric}_count')
    total_sum = diff(post, pre, f'{metric}_sum')
    return buckets, total_count, total_sum

def hist_quantile(buckets, q):
    if not buckets:
        return None
    total = buckets[-1][1]
    if total <= 0:
        return None
    target = total * q
    for bound, count in buckets:
        if count >= target:
            return bound
    return buckets[-1][0]

def fmt_latency(v):
    if v is None:
        return 'n/a'
    if v == float('inf'):
        return '+Inf'
    if v < 0.001:
        return f'{v * 1_000_000:.0f}us'
    if v < 1.0:
        return f'{v * 1000:.1f}ms'
    return f'{v:.2f}s'

def aggregate_hist(glob_pat_pre, glob_pat_post, metric):
    agg = {}
    for prep in glob.glob(glob_pat_pre):
        pod = os.path.basename(prep)
        postp = glob_pat_post(pod)
        if not os.path.exists(postp):
            continue
        pre, post = parse_prom(prep), parse_prom(postp)
        buckets, total_count, total_sum = hist_delta(post, pre, metric)
        for bound, count in buckets:
            agg[bound] = agg.get(bound, 0) + count
        agg['_count'] = agg.get('_count', 0) + total_count
        agg['_sum'] = agg.get('_sum', 0.0) + total_sum
    buckets = sorted((k, v) for k, v in agg.items() if k not in ('_count', '_sum'))
    total_count = agg.get('_count', 0)
    total_sum = agg.get('_sum', 0.0)
    if total_count <= 0:
        return None
    mean = total_sum / total_count if total_count else None
    return {
        'count': total_count,
        'mean': mean,
        'p50': hist_quantile(buckets, 0.50),
        'p95': hist_quantile(buckets, 0.95),
        'p99': hist_quantile(buckets, 0.99),
    }

def print_latency(label, stats):
    if not stats:
        print(f'  {label}: n/a')
        return
    print(
        f'  {label}: count={int(stats["count"])} '
        f'mean={fmt_latency(stats["mean"])} '
        f'p50<={fmt_latency(stats["p50"])} '
        f'p95<={fmt_latency(stats["p95"])} '
        f'p99<={fmt_latency(stats["p99"])}'
    )

def main(N):
    base = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'results', f'N{N}')
    print(f'\n=== N={N} ===')
    if not os.path.exists(base):
        print('(no results)')
        return
    h = json.load(open(f'{base}/hydrate.json'))
    hydrate_gib_s = h["aggregate_mibs"] / 1024
    hydrate_gbps = (hydrate_gib_s * GIB) / GBPS_BYTES_PER_SEC
    print(f'Hydrate: {h["total_files"]} files, {h["total_bytes"]/GIB:.1f} GiB, {h["total_chunks"]} chunks')
    print(f'  elapsed: {h["elapsed_ms"]/1000:.2f} s   aggregate: {hydrate_gib_s:.2f} GiB/s ({hydrate_gbps:.1f} Gbps)')
    print(f'  per-peer errors: {sum(len(p["errors"]) for p in h["peers"])}')
    wall = open(f'{base}/hydrate.wall_s').read().strip()
    print(f'  wall (incl. RPC): {wall} s')
    print('  hydrate latency:')
    print_latency(
        'chunk fetch',
        aggregate_hist(
            f'{base}/pre_*.prom',
            lambda pod: f'{base}/post_hydrate_{pod.replace("pre_", "").replace(".prom", "")}.prom',
            'blobcache_chunk_fetch_total_seconds',
        ),
    )
    print_latency(
        'cache insert',
        aggregate_hist(
            f'{base}/pre_*.prom',
            lambda pod: f'{base}/post_hydrate_{pod.replace("pre_", "").replace(".prom", "")}.prom',
            'blobcache_chunk_cache_insert_seconds',
        ),
    )

    print('\nParallel read (per pod wall, file bytes):')
    walls = []
    for wf in sorted(glob.glob(f'{base}/read/*.wall_s')):
        pod = os.path.basename(wf).replace('.wall_s','')
        w = float(open(wf).read().strip())
        bf = wf.replace('.wall_s','.bytes')
        b = open(bf).read().strip()
        try: bn = int(b); gib = bn/1024**3
        except: gib = 0
        mibs = gib*1024/w if w > 0 else 0
        walls.append(w)
        print(f'  {pod:25s} {w:7.2f}s  {gib:7.2f} GiB  {mibs/1024:5.2f} GiB/s')
    if walls:
        total_cluster_gib = sum(
            int(open(wf.replace('.wall_s', '.bytes')).read().strip()) / GIB
            for wf in sorted(glob.glob(f'{base}/read/*.wall_s'))
            if open(wf.replace('.wall_s', '.bytes')).read().strip().isdigit()
        )
        max_wall = max(walls)
        agg_gib_s = total_cluster_gib / max_wall if max_wall > 0 else 0
        agg_gbps = (agg_gib_s * GIB) / GBPS_BYTES_PER_SEC
        print(
            f'  -> wall p50={sorted(walls)[len(walls)//2]:.2f}s  max={max_wall:.2f}s  '
            f'cluster aggregate={agg_gib_s:.2f} GiB/s ({agg_gbps:.1f} Gbps)  '
            f'total bytes read across cluster: {total_cluster_gib:.1f} GiB'
        )
    print('  read latency:')
    print_latency(
        'peer fetch',
        aggregate_hist(
            f'{base}/post_hydrate_*.prom',
            lambda pod: f'{base}/post_read_{pod.replace("post_hydrate_", "").replace(".prom", "")}.prom',
            'blobcache_chunk_peer_fetch_seconds',
        ),
    )
    print_latency(
        'fuse read',
        aggregate_hist(
            f'{base}/post_hydrate_*.prom',
            lambda pod: f'{base}/post_read_{pod.replace("post_hydrate_", "").replace(".prom", "")}.prom',
            'blobcache_fuse_read_seconds',
        ),
    )

    print('\nThrottling (delta pre -> post_hydrate, summed across pods):')
    totals = {}
    for prep in glob.glob(f'{base}/pre_*.prom'):
        pod = os.path.basename(prep).replace('pre_','').replace('.prom','')
        postp = f'{base}/post_hydrate_{pod}.prom'
        if not os.path.exists(postp): continue
        pre, post = parse_prom(prep), parse_prom(postp)
        for prefix in ['blobcache_blob_request_status_total',
                       'blobcache_blob_request_retries_total',
                       'blobcache_blob_request_giveups_total',
                       'blobcache_blob_retry_sleep_seconds_total']:
            d = diff_label(post, pre, prefix)
            for k, v in d.items():
                totals[k] = totals.get(k, 0) + v
    for k in sorted(totals):
        print(f'  {k:80s} +{totals[k]:.1f}')
    if not totals:
        print('  (no throttle counters changed - all 200 OK)')

    print('\nFetch counts (delta pre -> post_read, summed):')
    bf_total = pf_total = pe_total = 0
    for prep in glob.glob(f'{base}/pre_*.prom'):
        pod = os.path.basename(prep).replace('pre_','').replace('.prom','')
        postp = f'{base}/post_read_{pod}.prom'
        if not os.path.exists(postp): continue
        pre, post = parse_prom(prep), parse_prom(postp)
        bf_total += diff(post, pre, 'blobcache_blob_fetches_total')
        pf_total += diff(post, pre, 'blobcache_peer_fetches_ok_total')
        pe_total += diff(post, pre, 'blobcache_peer_fetches_err_total')
    print(f'  blob_fetches:    {bf_total:>10.0f}')
    print(f'  peer_fetches_ok: {pf_total:>10.0f}')
    print(f'  peer_fetches_err: {pe_total:>10.0f}')

if __name__ == '__main__':
    for N in sys.argv[1:]:
        main(int(N))
