import json, sys, glob, os, re

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

def main(N):
    base = f'/home/paul/Microsoft/blobcache/bench/results/N{N}'
    print(f'\n=== N={N} ===')
    if not os.path.exists(base):
        print('(no results)')
        return
    h = json.load(open(f'{base}/hydrate.json'))
    print(f'Hydrate: {h["total_files"]} files, {h["total_bytes"]/1024**3:.1f} GiB, {h["total_chunks"]} chunks')
    print(f'  elapsed: {h["elapsed_ms"]/1000:.2f} s   aggregate: {h["aggregate_mibs"]/1024:.2f} GiB/s')
    print(f'  per-peer errors: {sum(len(p["errors"]) for p in h["peers"])}')
    wall = open(f'{base}/hydrate.wall_s').read().strip()
    print(f'  wall (incl. RPC): {wall} s')

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
        print(f'  -> wall p50={sorted(walls)[len(walls)//2]:.2f}s  max={max(walls):.2f}s  total bytes read across cluster: {gib*len(walls):.1f} GiB')

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
