# TESTING

How blobcache is tested, what's covered, and how to run it.

## Quick run

```sh
cargo test                         # all tests, TCP feature set (matches CI)
cargo test --features ucx          # adds the UCX wire-protocol unit tests (local only; needs UCX libs)
cargo clippy --all-targets -- -D warnings
cargo fmt --check
```

CI runs `cargo test --release --locked` on x86_64 + aarch64 Linux, plus `cargo fmt --check` and `cargo clippy -D warnings`, on every push and PR.

## Test inventory

| File                          | Kind            | Tests | Covers                                                                                      |
| ----------------------------- | --------------- | ----- | ------------------------------------------------------------------------------------------- |
| `tests/bloom.rs`              | unit            |   10  | `Bloom::new` clamping, `insert`/`contains`, `to_bytes`/`from_bytes` roundtrip, header guards |
| `tests/chunk_key.rs`          | unit            |    8  | `ChunkKey` cache-filename SHA derivation, separator-collision resistance                     |
| `tests/config.rs`             | unit            |   22  | `Config::validate` for every option, `cluster_hash` membership, local-tuning exclusions      |
| `tests/disk_cache.rs`         | unit            |   21  | `DiskCache::open`/`insert`/`try_get`/`evict`, LRU semantics, on-startup purge, ranged reads  |
| `tests/peer_index.rs`         | unit            |   14  | HRW determinism, `rank_candidates` yes/maybe ordering, bloom snapshot roundtrip              |
| `tests/auth.rs`               | unit            |   10  | `Credential::resolve` precedence (inline SAS / env / IMDS / anon), `append_sas_token`        |
| `tests/shared_key.rs`         | unit            |    7  | Azure Shared Key signing determinism + canonicalisation invariants                            |
| `tests/transport_tcp.rs`      | unit (async)    |    9  | `PeerService` HTTP surface (200/404/400/health), URL encoding, `ChunkProvider` wait semantics |
| `tests/two_daemon.rs`         | integration     |    4  | Two real `PeerService` daemons on loopback exchanging chunks; concurrent fan-out             |
| `src/fetcher.rs` (in-source)  | unit            |    8  | `PeerLru` byte-budget eviction + key-set invariants                                          |
| `src/hydrate.rs` (in-source)  | unit            |    8  | `HYDRATE_MODE` env precedence (Default/Broadcast/Ring) + JSON serde lock                     |
| `src/transport_ucx.rs` (in-source, `--features ucx`) | unit | 19 | UCX wire-protocol: request/response encode/decode, magic, status codes, length checks, oversize rejection, back-compat for pre-rid frames |

**140 tests total** (105 integration / 35 in-source).

## Feature → coverage matrix

Every config option and feature in the codebase, mapped to where it's tested.

### Cache (`[cache]`)

| Option / behaviour            | Tested by                                                  |
| ----------------------------- | ---------------------------------------------------------- |
| `dir`                         | `disk_cache.rs::open_creates_root_when_missing`            |
| `max_bytes`                   | `disk_cache.rs::eviction_*`, `config.rs::validate_*`       |
| `chunk_size` (cluster-wide)   | `config.rs::cluster_hash_includes_chunk_size`              |
| `cache_on_peer_fetch`         | `config.rs::cluster_hash_ignores_local_tuning_fields`      |
| `peer_lru_bytes`              | `fetcher.rs::peer_lru_tests::*` (8 tests)                  |
| Startup purge of stale `.tmp` | `disk_cache.rs::open_purges_pre_existing_files`            |
| Ranged read (`try_get_range`) | `disk_cache.rs::try_get_range_returns_slice`               |
| LRU promotion on hit          | `disk_cache.rs::hit_promotes_in_lru`                       |

### Azure (`[azure]`)

| Option / behaviour                | Tested by                                                          |
| --------------------------------- | ------------------------------------------------------------------ |
| `pool_max_idle_per_host`          | `config.rs::cluster_hash_ignores_local_tuning_fields`              |
| `workers`                         | `config.rs::validate_workers_must_be_ge_1` + cluster-hash exclusion |
| `main_worker_threads`             | `config.rs::validate_main_worker_threads` + cluster-hash exclusion  |
| `block_size`                      | `config.rs::block_size_zero_is_allowed_means_use_chunk_size`, `block_size_must_be_ge_chunk_size`, `block_size_must_be_multiple_of_chunk_size`, `cluster_hash_ignores_local_tuning_fields` |

### Transport (`[transport]`)

| Option / behaviour            | Tested by                                                                  |
| ----------------------------- | -------------------------------------------------------------------------- |
| `kind` (cluster-hash)         | `config.rs::cluster_hash_includes_transport_kind`                          |
| `chunk_concurrency`           | `config.rs::cluster_hash_ignores_local_tuning_fields`                      |
| `peer_concurrency`            | `config.rs::cluster_hash_ignores_local_tuning_fields`                      |
| `bloom_bits`, `bloom_rebuild_secs` | same                                                                  |
| TCP: `/health` endpoint       | `transport_tcp.rs::health_returns_200_with_protocol_version`               |
| TCP: chunk GET 200 path       | `transport_tcp.rs::fetch_chunk_returns_cached_payload`                     |
| TCP: chunk GET miss → 404 → `BcError::NotFound` | `transport_tcp.rs::fetch_chunk_miss_returns_not_found_error` |
| TCP: bad path → 400           | `transport_tcp.rs::malformed_chunk_path_returns_400`                       |
| TCP: special chars in mount/blob | `transport_tcp.rs::fetch_chunk_url_encodes_special_chars_in_mount_and_blob` |
| TCP: `ChunkProvider` invocation | `transport_tcp.rs::fetch_chunk_uses_chunk_provider_on_miss_when_wait_ms_set` |
| TCP: provider skipped when `wait_ms=0` | `transport_tcp.rs::fetch_chunk_skips_provider_when_wait_ms_zero`     |
| TCP: x-blobcache-rid header   | `transport_tcp.rs::rid_header_is_accepted`                                  |
| **End-to-end: two real daemons** | `two_daemon.rs::two_daemons_exchange_chunks_over_loopback`               |
| **End-to-end: concurrent fan-out** | `two_daemon.rs::many_concurrent_distinct_chunks_round_trip_correctly`  |
| **End-to-end: cluster_hash agreement** | `two_daemon.rs::both_daemons_report_health_independently`          |
| UCX: request roundtrip        | `transport_ucx.rs::wire_protocol_tests::request_roundtrip_preserves_all_fields` |
| UCX: bad magic rejected       | `transport_ucx.rs::wire_protocol_tests::request_decode_rejects_bad_magic`   |
| UCX: truncation               | `transport_ucx.rs::wire_protocol_tests::request_decode_rejects_truncation` |
| UCX: oversized fields         | `request_encode_rejects_oversized_mount_name`, `..._oversized_request_id`  |
| UCX: rid character whitelist  | `request_decode_rejects_invalid_rid_chars`                                  |
| UCX: pre-rid back-compat      | `request_backcompat_no_wait_ms_no_rid_decodes`                              |
| UCX: response status codes    | `response_ok_roundtrip`, `response_miss_decodes_to_not_found`, `response_err_decodes_to_peer_error`, `response_unknown_status_decodes_to_peer_error` |
| UCX: response length checks   | `response_decode_rejects_short_header`, `..._header_body_length_mismatch`, `..._payload_larger_than_requested` |
| UCX: oversized response       | `response_encode_rejects_oversized_payload` (`MAX_RESPONSE_BYTES = 64 MiB`) |
| UCX: wire constants pinned    | `status_codes_are_stable`                                                   |

### Auth (`src/auth/`)

| Behaviour                                  | Tested by                                                  |
| ------------------------------------------ | ---------------------------------------------------------- |
| Inline `sas_token` highest precedence      | covered by `Credential::resolve` ordering in `auth.rs`     |
| Env: `AZURE_STORAGE_KEY` → SharedKey       | `auth.rs::from_env_returns_shared_key_when_account_and_key_set` |
| Env: `AZURE_STORAGE_SAS_TOKEN` → SAS       | `auth.rs::from_env_returns_sas_when_only_sas_set`          |
| No env → None                              | `auth.rs::from_env_returns_none_when_nothing_set`          |
| `append_sas_token` strips leading `?`, merges with existing query | `auth.rs::append_sas_token_*` (5 cases) |
| Shared Key signing determinism             | `shared_key.rs::sign_request_is_deterministic`             |
| Header canonicalisation (lowercase + sort) | `shared_key.rs::canonical_headers_*`                       |
| Content-length: `None == Some(0)`          | `shared_key.rs::content_length_none_equals_zero`           |
| Query-param canonicalisation               | `shared_key.rs::canonical_resource_includes_query_params`  |

### Bloom filter (`src/bloom.rs`)

| Behaviour                              | Tested by                                            |
| -------------------------------------- | ---------------------------------------------------- |
| `new` clamps `m_bits >= 64`            | `bloom.rs::new_clamps_min_m_bits`                    |
| `insert` / `contains` correctness      | `bloom.rs::insert_then_contains_returns_true`        |
| Empty bloom never matches              | `bloom.rs::empty_bloom_never_contains`               |
| `to_bytes` / `from_bytes` roundtrip    | `bloom.rs::roundtrip_through_bytes`                  |
| `from_bytes` rejects short header (<8B) | `bloom.rs::from_bytes_rejects_short_header`         |
| `from_bytes` rejects `m_bits<64`       | `bloom.rs::from_bytes_rejects_too_small_m_bits`      |
| `from_bytes` rejects size mismatch     | `bloom.rs::from_bytes_rejects_payload_size_mismatch` |

### ChunkKey (`src/cache.rs`)

| Behaviour                                      | Tested by                                              |
| ---------------------------------------------- | ------------------------------------------------------ |
| Filename = SHA256(mount\0blob\0offset_le_bytes) | `chunk_key.rs::cache_filename_is_64_hex_chars`        |
| Different offsets → different filenames        | `chunk_key.rs::different_offsets_yield_different_names` |
| `(mount,blob)=(ab,cd)` ≠ `(a,bcd)` collision   | `chunk_key.rs::no_separator_collision_between_mount_and_blob` |
| Equality / Hash / Clone                        | `chunk_key.rs::equality_*`, `hash_*`                   |

### PeerIndex / HRW (`src/peer_index.rs`)

| Behaviour                              | Tested by                                                  |
| -------------------------------------- | ---------------------------------------------------------- |
| HRW determinism                        | `peer_index.rs::hrw_is_deterministic`                      |
| Same key → same ranking                | `peer_index.rs::hrw_same_key_same_order`                   |
| Different keys → distinct distributions | `peer_index.rs::hrw_distributes_keys`                     |
| `rank_candidates` returns yes-set first | `peer_index.rs::rank_candidates_yes_before_maybe`         |
| Bloom snapshot roundtrip               | `peer_index.rs::bloom_snapshot_roundtrip`                  |

### PeerLru (`src/fetcher.rs`)

| Behaviour                                | Tested by (in-source `mod peer_lru_tests`)        |
| ---------------------------------------- | ------------------------------------------------- |
| Byte-budget eviction                     | `evicts_oldest_when_budget_exceeded`              |
| Hit promotes entry                       | `hit_promotes_to_front`                           |
| Insert returning evictions               | `insert_returns_evicted_keys`                     |
| Drop entry by key                        | `drop_removes_entry`                              |
| Empty/zero-budget edge cases             | `zero_budget_evicts_immediately`                  |
| Single-entry larger than budget          | `oversize_entry_evicts_self`                      |

### Hydrate (`src/hydrate.rs`)

| Behaviour                                | Tested by (in-source `mod hydrate_mode_tests`)    |
| ---------------------------------------- | ------------------------------------------------- |
| `HYDRATE_MODE` env wins over per-request mode | `env_broadcast_overrides_request`, `env_ring_overrides_request` |
| Case-insensitive env parsing             | `env_is_case_insensitive`                         |
| Unknown env value falls back             | `env_unknown_value_falls_back_to_request_mode`    |
| Empty env value falls back               | `env_empty_string_falls_back`                     |
| Default when nothing set                 | `default_when_no_env_and_no_request`              |
| Per-request mode used when no env        | `request_mode_used_when_no_env`                   |
| JSON serde format locked                 | `hydrate_mode_serde_lowercase_roundtrip`          |

> Multi-node broadcast/ring orchestration (`run_broadcast_phase`, `run_ring_phase`) is integration-level and requires N daemons + a blob origin (real account or mock). Currently exercised manually via `deploy/storage-access.sh` + the cluster harness; future work is a full mock-blob-backed test (see "Gaps" below).

### cluster_hash invariants (`src/config.rs`)

| Field                          | In hash | Test                                                 |
| ------------------------------ | ------- | ---------------------------------------------------- |
| `cache.chunk_size`             | yes     | `cluster_hash_includes_chunk_size`                   |
| `transport.kind`               | yes     | `cluster_hash_includes_transport_kind`               |
| `mounts[].name` (sorted)       | yes     | `cluster_hash_sorts_mounts_by_name`                  |
| 31 local tuning fields         | no      | `cluster_hash_ignores_local_tuning_fields` (parametrised over every excluded field) |

## Two-daemon localhost integration (`tests/two_daemon.rs`)

Stands up two real `PeerService` instances on independent loopback ports, each with its own on-disk cache (`tempfile::TempDir`). One node holds chunk data, the other acts as a client. Validates:

1. **Byte-exact round-trip** of a 4 MiB chunk over loopback TCP, including server-side `chunk_requests` + `chunk_bytes_served` counter increments.
2. **404 propagation** as `BcError::NotFound`, with `chunk_requests` incremented but `chunk_bytes_served` still zero.
3. **Concurrent fan-out**: 64 distinct chunks fetched concurrently against the same daemon, payload pattern verified per chunk to detect cross-talk.
4. **Cluster identity**: both daemons return the same `cluster_hash` hex via `/health`.

This test is the closest thing to a real two-node deployment that runs in CI without external dependencies.

## Datagen for cluster-scale tests

`tests/datagen/gen.sh` produces deterministic synthetic blobs locally (5 size tiers, total ~1.6 GiB if all tiers selected). `tests/datagen/upload.sh` pushes them to Azure Blob Storage using the `STORAGE_ACCOUNT` from `.env`.

```sh
OUT_DIR=/tmp/blobcache-data ./tests/datagen/gen.sh
deploy/storage-access.sh on
OUT_DIR=/tmp/blobcache-data ./tests/datagen/upload.sh
deploy/storage-access.sh off            # MUST run after every test session
```

The data is byte-deterministic across re-runs (seeded AES-CTR over `/dev/zero`) so test assertions can pin specific byte ranges without storing reference data in the repo.

## Gaps and deferred work

These are the known holes in coverage. Each is a candidate for a future PR.

1. **Singleflight semantics** (`Fetcher::fetch_chunk` leader/follower path): requires wiring a `Fetcher` with a stub `BlobFetcherPool`. Today the `LeaderGuard` RAII path is tested only structurally; concurrent leader cancellation under panic is not covered. **Mitigation**: `tests/two_daemon.rs::many_concurrent_distinct_chunks_round_trip_correctly` exercises the transport-level concurrent-request path, which is where most singleflight regressions would surface as cross-talk.
2. **Hydrate broadcast/ring orchestration**: `run_broadcast_phase` and `run_ring_phase` need N daemons + a blob origin. Today only the mode-selection logic (`hydrate_mode`) is unit-tested. End-to-end is exercised manually against the gb300 cluster.
3. **FUSE read path** (`src/fuse_fs.rs::BlobFs::read`): requires a real FUSE mount, root, and a kernel module. Out of scope for unit tests; covered by manual smoke tests on the gb300 cluster.
4. **Gossip cluster join** (`src/cluster.rs`): partial — `Membership` state transitions are exercised via the existing peer-index tests, but the full HTTP push-pull loop with cluster-hash mismatch rejection is not yet automated.
5. **UCX live transport** (`src/transport_ucx.rs::RdmaPeerService` runtime path): requires HCA + libucx + root. The wire-protocol unit tests cover encode/decode invariants, but the actual UCP endpoint lifecycle is only exercised on real hardware.

## Test-writing conventions

- **Helpers live in `tests/common/mod.rs`**. Use `minimal_config(cache_dir)` and `node(id)` rather than re-rolling them.
- **Env-mutating tests** (anything that touches `std::env`) MUST take a `static ENV_LOCK: Mutex<()>` because `cargo test` runs tests in parallel. See `tests/auth.rs` and `src/hydrate.rs::hydrate_mode_tests` for the pattern.
- **Tempfiles**: always `tempfile::TempDir`; never hard-code `/tmp/foo` paths.
- **Async tests**: `#[tokio::test]` (not `#[tokio::test(flavor = "multi_thread")]` unless concurrency is part of the assertion).
- **Private types**: test via `#[cfg(test)] mod ...` in the same source file rather than making them `pub`. See `src/fetcher.rs::peer_lru_tests` and `src/hydrate.rs::hydrate_mode_tests`.

## Running specific subsets

```sh
cargo test --test transport_tcp                # one integration file
cargo test --test two_daemon                   # two-daemon harness only
cargo test peer_lru_tests                      # in-source PeerLru tests
cargo test hydrate_mode_tests                  # in-source hydrate-mode tests
cargo test --features ucx wire_protocol_tests  # UCX wire-protocol tests
cargo test config::                            # all config validation tests
```

## CI

`.github/workflows/build.yml` builds and runs `cargo test --release --locked` on every push/PR for both `x86_64-unknown-linux-gnu` and `aarch64-unknown-linux-gnu` targets, plus `cargo fmt --check` and `cargo clippy -D warnings`. All must pass before a PR can merge.

The `ucx` feature is **not** built or tested in this workflow — `ucx1-sys` vendors UCX 1.18.1 from source and fails on the Ubuntu-runner GCC's `-Werror` flags. UCX builds (and the `wire_protocol_tests` module) are exercised by the container image workflow (`deploy/Dockerfile`) on a base image with the matching MOFED/UCX stack. Run them locally with `cargo test --features ucx wire_protocol_tests` if you have UCX installed.
