#!/usr/bin/env bash
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
SWEEP_DIR="$HERE/sweep"
mkdir -p "$SWEEP_DIR"

run_one() {
  local label=$1; local chunk=$2; local conc=$3
  echo "[$(date +%H:%M:%S)] === START $label (chunk=$chunk conc=$conc) ==="
  CACHE_CHUNK_SIZE=$chunk CHUNK_CONCURRENCY=$conc \
    "$HERE/matrix.sh" 16 2>&1 | tee "$SWEEP_DIR/${label}.log"
  rm -rf "$SWEEP_DIR/$label"
  cp -r "$HERE/results/N16" "$SWEEP_DIR/$label"
  echo "[$(date +%H:%M:%S)] === DONE  $label ==="
}

run_one "16M-conc8"  16777216 8
run_one "16M-conc16" 16777216 16
run_one "4M-conc64"  4194304  64

echo "ALL DONE"
