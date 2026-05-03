# `opus_code_eval.md` — Action Tracker

Tracks one-by-one resolution of every finding in [`opus_code_eval.md`](opus_code_eval.md).

Branch: `fix/opus-eval-actions`.

Status legend:
- ✅ Code change landed
- 📝 Comment / doc update only (intentional acknowledgement)
- 🚧 Planned, not yet landed
- 🚫 Decided not to action (with reason)

## High-severity (1–8)

| # | Title | Resolution | Status |
|---|---|---|---|
| 1 | `azure.block_size` dead | **Acknowledged**: block→chunk slicing in `Fetcher::fetch_blob_chunks` is non-trivial (interacts with singleflight `inflight` map, prefetch worker, and chunk-level `expected_len` validation in the hot read path) and the README already documents `block_size` as "reserved for future block aggregation". The config docstring previously lied about behaviour ("the daemon issues block-sized GETs ... then slices") that didn't exist; corrected to honestly mark the field as a reserved hook with the intended semantics, so operators don't silently mis-configure it. Validation kept so future configs remain valid as soon as the implementation lands. | 📝 |
| 2 | `transport.peer_concurrency` dead | **Implement**: per-peer in-flight semaphore in `Fetcher` keyed by peer URL, capped at `peer_concurrency`. Falls back to no-op when 0/unset. | ✅ |
| 3 | `peer_max_candidates` not enforced | **Implement**: enforce combined cap across `yes`+`maybe` iteration in `do_fetch`. | ✅ |
| 4 | Hard-coded port `7773` | **Implemented**: added optional `admin_url` field on `NodeInfo` (serde-default for backward compat); `effective_admin_url()` falls back to port-substitution when peer hasn't published one; replaced 4 hard-coded `:7773` sites in clear/hydrate fan-out. | ✅ |
| 5 | Admin endpoints no auth | **Implement**: optional `admin.token` in config; when set, all destructive POSTs (`/clear-cache*`, `/hydrate*`) require `Authorization: Bearer <token>`; inter-node calls auto-include it. | ✅ |
| 6 | `shared_key.rs` `expect` panics | **Fix**: return `BcError::Auth` on bad base64 / HMAC init. | ✅ |
| 7 | UCX wire decoder back-compat ambiguity | The decoder already enforces `idx == data.len()` at end of decode, so the only true ambiguity is a v2.6 frame truncated *exactly* after `wait_ms`. Added a `Once`-gated `tracing::warn!` that fires the first time such a frame is decoded so real-world hits are visible, plus a `TODO(opus-eval-7)` marking exact-length tightening as a follow-up (requires threading variable-length field sizes through the decoder). | 📝 |
| 8 | RecvSlab semaphore desync uses `expect` | **Fix**: `try_borrow_mut` + structural error path (no panic on borrow contention); add `debug_assert_eq!` linking semaphore permits to free-list length on every checkout/return. | ✅ |

## Medium-severity (9–20)

| # | Title | Resolution | Status |
|---|---|---|---|
| 9 | `clone_handles` enumerates 25 fields | **Acknowledged**: investigated and rejected. `Fetcher` does not implement `Clone` and `clone_handles` no longer exists in the current code; `Fetcher` is held as `Arc<Fetcher>` at every call site (main.rs:264, fuse_fs.rs:59, hydrate.rs:218/307/741/911/1096) and only the outer `Arc` is cloned. The per-clone cost is one 8-byte refcount bump regardless of the inner field count, so the alleged "Arc explosion" cost does not exist. Refactoring to `Arc<FetcherInner>` would require touching every field access across ~1400 LoC of `fetcher.rs` for zero functional or performance change, with non-trivial regression risk in the hot read path. | 📝 |
| 10 | `await_inserts_drained` 5 ms busy-poll | **Fix**: replace with `tokio::sync::Notify` bumped on transition to 0. | ✅ |
| 11 | `by_ino` unbounded growth | **Implement**: gauge `blobcache_fuse_by_ino_entries` + warn-log when crossing soft cap (default 1M). | ✅ |
| 12 | `fetch_range` per-chunk allocation | **Fix**: pre-size single `BytesMut` and write chunks into the right offset; remove per-chunk `Vec::with_capacity`. | ✅ |
| 13 | Bloom version publish race | **Fix**: invoke `on_version_change` *inside* the write lock (advertised version can no longer trail published bytes). | ✅ |
| 14 | `members_all()` no `cluster_hash` filter | **Fix**: add `members_alive_same_cluster()` (a documenting alias for `members_alive`, which already filters); route `clear` and `hydrate` fan-out (which were using `members_all()` + manual Alive filter, missing the cluster check) and the fetcher peer-candidate site through it. | ✅ |
| 15 | Gossip O(N²) | 📝 Document hard ceiling (~300 nodes) in `cluster.rs` module doc. | 📝 |
| 16 | `HydrateJobs::gc()` per-call | **Fix**: gate behind 5 s throttle (`AtomicU64` last-gc timestamp). | ✅ |
| 17 | `BlobClient` double retry loop | **Fix**: collapse body-drain retry into the existing `send` retry path (single budget). | ✅ |
| 18 | Giant `serve()` lambdas | 📝 Document as known refactor target (split tracked in this file). Not landed: high churn risk for behavioural change-free split, ~600 LOC across 2 files; better as a follow-up PR. | 📝 |
| 19 | Prometheus `reset()+inc_by` anti-pattern | **Fix**: switch to a `Collector` impl reading the underlying `AtomicU64`s directly so monotonicity is structurally guaranteed. | ✅ |
| 20 | Hydrate/clear timeout misalignment | **Fix**: derive per-shard HTTP timeout from coordinator timeout minus a 2 s safety margin so a shard cannot legally outlive the coordinator's collect window.  Ring per-step phase budget unchanged (still bounded by `global_timeout` via the elapsed check). | ✅ |

## Low-severity / hygiene (21–29)

| # | Title | Resolution | Status |
|---|---|---|---|
| 21 | ~25 Hyper builder `unwrap()`s | **Fix**: helper `crate::http_util::ok_response` / `error_response` returning `Response<...>` with `.unwrap_or_else` fallback. | ✅ |
| 22 | Inconsistent `tracing::Instrument` | **Fix**: span all FUSE entrypoints + all hydrate handlers with rid. | ✅ |
| 23 | `BloomVersion` saturating add | 📝 Document that `u64` overflow is multi-century at realistic insert rates; saturating semantics are intentional to never produce a 0 version (which is sentinel for "unknown"). | 📝 |
| 24 | ~280 pedantic clippy warnings | 📝 Add `clippy.toml` raising `cast_*` thresholds; document policy in `AGENTS.md`. | ✅ |
| 25 | `nic::enumerate` / `is_likely_infiniband` dead | **Wire**: emit detected IB devices via `info!` log on startup so operators see what was found; keeps the heuristic alive without auto-binding (which would change deploy behaviour). | ✅ |
| 26 | `insert_received_chunk` / `blob_for` dead | **Remove**: delete unused entry points; if a future consumer needs them, recover from git history. | ✅ |
| 27 | Long files (`hydrate.rs`, `transport_ucx.rs`) | 📝 Document split plan in this file. Pure-mechanical split deferred (zero behavioural change, large diff, blocks unrelated PRs). | 📝 |
| 28 | Missing `rustfmt.toml` / `clippy.toml` / CI | **Add**: `rustfmt.toml`, `clippy.toml`, `.github/workflows/ci.yml` (fmt + clippy `-D warnings` + test). | ✅ |
| 29 | Inconsistent log levels | 📝 Add a `LOGGING.md` style guide and call it out in `AGENTS.md`. | ✅ |

## Build/test verification

Run on every commit in this branch:

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
```
