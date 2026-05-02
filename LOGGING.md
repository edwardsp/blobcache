# Logging conventions

`blobcache` uses the `tracing` crate. Pick the level by audience and intent:

| Level | When to use | Example |
|---|---|---|
| `error!` | Terminal failure: daemon will exit, or operator must intervene to restore service. | "failed to bind gossip listener: address in use" |
| `warn!` | Recoverable failure that we retried, skipped, or worked around. May indicate a real bug if it spikes. | "peer returned non-200 for /v1/chunk; falling back" |
| `info!` | Steady-state state transitions visible to operators. | "joined cluster, members=12", "mount ready: /mnt/blobcache/models" |
| `debug!` | Per-request detail useful when investigating a specific incident. | "fetch_chunk decision: peer=node-a yes_set=3 maybe=5" |
| `trace!` | Chunk/byte-level. Off by default in production. | "wire encode: 4112 bytes magic=0x..." |

## Request IDs

Per-request `rid` (request_id) MUST be carried as a span field, not interpolated into the message body. This lets the log backend index by rid without parsing.

```rust
let span = tracing::info_span!("fetch_chunk", rid = %rid);
let _g = span.enter();
tracing::debug!("starting fetch");  // rid is on the span, not the message
```

## Anti-patterns

- `warn!` for both "peer was slow" (benign) AND "we corrupted state" (bug). Pick one bucket per cause.
- Logging the same event at two levels for "audit" purposes. Use one level; let log filters do the rest.
- Including PII (peer hostnames are fine; cluster customer identifiers are not).
