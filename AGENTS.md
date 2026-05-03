# AGENTS.md — contributor guide for blobcache

## Lint policy

- `cargo fmt --check` MUST pass before commit.
- `cargo clippy --all-targets --all-features -- -D warnings` MUST pass before commit.
- Pedantic lints are advisory; thresholds for cognitive complexity, function arity, and type complexity are tuned in `clippy.toml`.
- See LOGGING.md for tracing level conventions.
