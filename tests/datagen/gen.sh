#!/usr/bin/env bash
# Generate deterministic synthetic test blobs locally.
#
# Output: $OUT_DIR populated with files whose name encodes their size and
# whose contents are pseudo-random but seeded by file index so tests can
# verify specific byte ranges without storing the data.
#
# Layout matches what blobcache integration tests expect:
#   tiny/       -- 4 KiB files (cache-internal smaller-than-chunk paths)
#   small/      -- 1 MiB files (single-chunk reads)
#   medium/     -- 16 MiB files (multi-chunk + bloom interactions)
#   large/      -- 256 MiB files (peer-fetch + parallel-chunk paths)
#   weights/    -- 1 GiB single file (hydrate timing baseline)
#
# Usage:
#   OUT_DIR=/tmp/blobcache-data ./tests/datagen/gen.sh
#   OUT_DIR=/tmp/blobcache-data SIZE=large ./tests/datagen/gen.sh   # one tier only

set -euo pipefail

OUT_DIR="${OUT_DIR:-/tmp/blobcache-data}"
SIZE_FILTER="${SIZE:-all}"

mkdir -p "$OUT_DIR"

# Deterministic generator: fills $1 bytes from /dev/urandom seeded via $2.
# We use openssl rand because /dev/urandom isn't seedable; instead we
# derive a per-file stream by encrypting a counter with a fixed key. The
# only goal is "different files have different content, same args produce
# the same output if the test re-runs", which openssl-aes-ctr satisfies
# without needing real cryptographic strength.
gen_file() {
  local path="$1" size_bytes="$2" seed="$3"
  local key
  key=$(printf 'blobcache-test-seed-%08x' "$seed" | sha256sum | cut -c1-32)
  local iv="00000000000000000000000000000000"
  head -c "$size_bytes" /dev/zero \
    | openssl enc -aes-128-ctr -K "$key" -iv "$iv" -nopad \
    > "$path"
}

want() {
  local tier="$1"
  [[ "$SIZE_FILTER" == "all" || "$SIZE_FILTER" == "$tier" ]]
}

if want tiny; then
  mkdir -p "$OUT_DIR/tiny"
  for i in $(seq 0 7); do
    gen_file "$OUT_DIR/tiny/tiny-$(printf '%02d' "$i").bin" $((4 * 1024)) "$i"
  done
  echo "[gen] 8 x 4 KiB -> $OUT_DIR/tiny/"
fi

if want small; then
  mkdir -p "$OUT_DIR/small"
  for i in $(seq 0 7); do
    gen_file "$OUT_DIR/small/small-$(printf '%02d' "$i").bin" $((1024 * 1024)) "$((100 + i))"
  done
  echo "[gen] 8 x 1 MiB -> $OUT_DIR/small/"
fi

if want medium; then
  mkdir -p "$OUT_DIR/medium"
  for i in $(seq 0 3); do
    gen_file "$OUT_DIR/medium/medium-$(printf '%02d' "$i").bin" $((16 * 1024 * 1024)) "$((200 + i))"
  done
  echo "[gen] 4 x 16 MiB -> $OUT_DIR/medium/"
fi

if want large; then
  mkdir -p "$OUT_DIR/large"
  for i in $(seq 0 1); do
    gen_file "$OUT_DIR/large/large-$(printf '%02d' "$i").bin" $((256 * 1024 * 1024)) "$((300 + i))"
  done
  echo "[gen] 2 x 256 MiB -> $OUT_DIR/large/"
fi

if want weights; then
  mkdir -p "$OUT_DIR/weights"
  gen_file "$OUT_DIR/weights/model.bin" $((1024 * 1024 * 1024)) 9999
  echo "[gen] 1 x 1 GiB -> $OUT_DIR/weights/"
fi

echo "[gen] manifest:"
( cd "$OUT_DIR" && find . -type f | sort | xargs -I{} sh -c 'printf "  %s  %s\n" "$(stat -c%s "{}")" "{}"' )
