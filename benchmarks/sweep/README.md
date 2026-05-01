# benchmarks/sweep — canonical 6-run sweep harness

The Tier-1 baseline (`benchmarks/RESULTS-2026-04-30-tier1-baseline-6run-sweep.md`)
established a 3-config × 2-trial protocol for measuring end-to-end perf:

| Config | `cacheOnPeerFetch` | hydrate | sequence |
|---|:---:|---|---|
| **C1** | `false` | `broadcast` + explicit gather | `hydrate → 30s → gather → 30s → PASS1 → 10s → PASS2` |
| **C2** | `false` | `default` (sharded)            | `hydrate → 30s → PASS1 → 10s → PASS2` |
| **C3** | `true`  | `default` (sharded)            | `hydrate → 30s → PASS1 → 10s → PASS2` |

This directory makes that sweep re-runnable.

## Files

| File | Purpose |
|---|---|
| `values-cache-peer-off.yaml.tmpl` | Helm overlay for C1, C2 (`cacheOnPeerFetch=false`) |
| `values-cache-peer-on.yaml.tmpl`  | Helm overlay for C3 (`cacheOnPeerFetch=true`) |
| `render-overlay.sh`               | Substitutes placeholders from env vars |
| `run-6run-sweep.sh`               | Orchestrator: helm reinstall + 6 trials, writes `sweep-summary.tsv` |

Templates use `__PLACEHOLDER__` tokens for `clientId`, storage account,
seed IPs, and the image tag — keeps cluster/secret material out of git.

## Run

```bash
export BLOBCACHE_CLIENT_ID=<azure-mi-client-uuid>
export BLOBCACHE_ACCOUNT=<storage-account>
export BLOBCACHE_SEED_1=<seed-pod-ip>
export BLOBCACHE_SEED_2=<seed-pod-ip>
export BLOBCACHE_SEED_3=<seed-pod-ip>
export BLOBCACHE_IMAGE_TAG=sha-<short>-arm64

# Optional (defaults shown):
#   OUT_DIR=/tmp/sweep-<utc>
#   PATH_PREFIX=nvidia_DeepSeek-R1-0528-NVFP4-v2/
#   NS=blobcache  RELEASE=blobcache  CHART=deploy/helm/blobcache

./benchmarks/sweep/run-6run-sweep.sh
```

The sweep takes ~70 minutes wall (6 trials × ~7 min compute + helm
reinstall overhead). Output:

- `$OUT_DIR/sweep-summary.tsv` — one line per trial: `tag start_utc end_utc hydrate_s gather_s pass1_s pass2_s hyd_status` (`hyd_status=ok` means every peer fetched its full assigned shard with zero errors; `FAIL` means at least one peer underfetched and PASS1 will be polluted by blob fallback)
- `$OUT_DIR/<tag>-{run.log,pass1.tsv,pass2.tsv,hydrate.json,gather.json,snap-*.tsv}`
- `$OUT_DIR/values-{off,on}.yaml` — rendered overlays (do not commit)

The `start_utc` / `end_utc` columns are wall-clock bookends per trial,
ready to paste into Grafana time-range pickers.

## Documenting a sweep result

After the sweep finishes:

1. Copy `sweep-summary.tsv` into a new
   `benchmarks/RESULTS-<date>-<slug>.md`.
2. Compare PASS1 / PASS2 columns against the baseline table.
3. Note the image tag, branch, and commit SHA in the doc header.
4. Sanitize: ensure no pod names, vmss IDs, or seed IPs leak into the
   committed markdown.
5. Commit + push.
