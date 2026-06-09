#!/bin/bash
# run_all.sh - unattended 6-run blobcache / Open-Sora v2 benchmark driver.
#
# Runs entirely on the login node (admin API reached directly at <node>:7773).
# Weights are ALWAYS broadcast (warm-replicated): every node needs the full 11B
# checkpoint to start, so the checkpoint is never a variable - only the DATA
# strategy varies across the three scenarios.
#
#   3 TRAINING runs : clear-cache -> bcast weights -> [data strategy] -> 60s gap -> train
#   3 READ-BENCH    : clear-cache -> [data strategy] -> snapshot -> harness -> snapshot
#   data strategy   : cold (none) | sharded (hydrate default) | replicated (hydrate broadcast)
#
# Cache is cleared (bloom-aware coordinator) before EVERY run. All artefacts land
# in $BENCH; STATUS holds the live phase, DONE appears at the end.
#
# Required env:
#   ADMIN     any compute node (coordinates clear-cache + hydrate fan-out)
#   NODELIST  Slurm nodelist for the runs, e.g. node-[0002-0006,0008-0066]
set -uo pipefail

ADMIN=${ADMIN:?set ADMIN to a compute node, e.g. node-0002}
NODELIST=${NODELIST:?set NODELIST to your run nodelist, e.g. node-[0002-0066]}
DEPLOY=/shared/blobcache-deploy
BENCH=$DEPLOY/bench
mkdir -p "$BENCH"
: > "$BENCH/orch.log"
rm -f "$BENCH/DONE"
echo "runid,kind,jobid,tag" > "$BENCH/runids.csv"

log(){ echo "[$(date -u +%H:%M:%S)] $*" | tee -a "$BENCH/orch.log"; echo "$*" > "$BENCH/STATUS"; }

clearcache(){ # $1=tag
  log "clear-cache (bloom-aware) before $1"
  curl -s --max-time 300 -XPOST "http://$ADMIN:7773/clear-cache" -o "$BENCH/$1-clear.json"
  local rm bytes ms
  rm=$(sed -E 's/.*"total_files_removed":([0-9]+).*/\1/' "$BENCH/$1-clear.json")
  bytes=$(sed -E 's/.*"total_bytes_removed":([0-9]+).*/\1/' "$BENCH/$1-clear.json")
  ms=$(sed -E 's/.*"elapsed_ms":([0-9]+).*/\1/' "$BENCH/$1-clear.json")
  log "  cleared files=$rm bytes=$bytes elapsed_ms=$ms"
}

hydrate(){ # $1=payload-file $2=tag-label
  log "hydrate $2 (payload=$1)"
  local jid st
  jid=$(curl -s --max-time 60 -XPOST "http://$ADMIN:7773/hydrate?async=1" --data-binary @"$DEPLOY/$1" \
        | sed -E 's/.*"job_id" *: *"?([^",}]+)"?.*/\1/')
  log "  hydrate job_id=$jid"
  while :; do
    st=$(curl -s --max-time 30 "http://$ADMIN:7773/hydrate/$jid")
    printf '%s' "$st" > "$BENCH/$2-hydrate.json"
    case "$st" in
      *'"status":"completed"'*) log "  hydrate $2 COMPLETED"; break ;;
      *'"status":"failed"'*)    log "  hydrate $2 FAILED: $st"; break ;;
    esac
    sleep 3
  done
  local ms a b mibs
  ms=$(sed -E 's/.*"elapsed_ms":([0-9]+).*/\1/' "$BENCH/$2-hydrate.json")
  a=$(sed -E 's/.*"phase_a_elapsed_ms":([0-9]+).*/\1/' "$BENCH/$2-hydrate.json")
  b=$(sed -E 's/.*"phase_b_elapsed_ms":([0-9]+).*/\1/' "$BENCH/$2-hydrate.json")
  mibs=$(sed -E 's/.*"aggregate_mibs":([0-9.]+).*/\1/' "$BENCH/$2-hydrate.json")
  log "  hydrate $2 elapsed_ms=$ms phase_a=$a phase_b=$b aggregate_mibs=$mibs"
}

wait_job(){ # $1=jobid
  while squeue -h -j "$1" 2>/dev/null | grep -q .; do sleep 20; done
}

train_run(){ # $1=tag $2=data-strategy(none|sharded|replicated)
  local tag=$1 data=$2
  log "=== TRAINING run tag=$tag data=$data ==="
  clearcache "$tag"
  hydrate hydrate-osv2.json "$tag-weights"
  case "$data" in
    sharded)    hydrate hydrate-pexels-shard.json "$tag-data" ;;
    replicated) hydrate hydrate-pexels.json "$tag-data" ;;
    none)       log "  cold: no data hydrate (dataset streams from blob on demand)" ;;
  esac
  log "  60s idle gap (separate hydrate from training on metrics)"
  sleep 60
  local jid
  jid=$(sbatch --parsable --nodelist="$NODELIST" \
        --export=ALL,RUN_TAG="$tag" "$DEPLOY/os-train.sbatch")
  log "  submitted training job $jid RUN_ID=$tag-$jid"
  echo "$tag-$jid,train,$jid,$tag" >> "$BENCH/runids.csv"
  wait_job "$jid"
  log "  training job $jid DONE"
}

bench_run(){ # $1=tag $2=data-strategy
  local tag=$1 data=$2
  log "=== READ-BENCH run tag=$tag data=$data ==="
  clearcache "rb-$tag"
  case "$data" in
    sharded)    hydrate hydrate-pexels-shard.json "rb-$tag-data" ;;
    replicated) hydrate hydrate-pexels.json "rb-$tag-data" ;;
    none)       log "  cold: no data hydrate" ;;
  esac
  log "  30s settle"; sleep 30
  bash "$BENCH/snap_metrics.sh" "$BENCH/rb-$tag-before.csv" 2>&1 | tee -a "$BENCH/orch.log"
  local jid
  jid=$(sbatch --parsable --nodelist="$NODELIST" \
        --export=ALL,STEPS=200,WORKERS=8,BATCH=4 "$DEPLOY/harness.sbatch")
  log "  submitted read-bench job $jid (rb-$tag)"
  echo "rb-$tag-$jid,bench,$jid,rb-$tag" >> "$BENCH/runids.csv"
  wait_job "$jid"
  bash "$BENCH/snap_metrics.sh" "$BENCH/rb-$tag-after.csv" 2>&1 | tee -a "$BENCH/orch.log"
  log "  read-bench job $jid DONE"
}

log "ORCHESTRATION START"
train_run cold        none
train_run wsharded    sharded
train_run wreplicated replicated
bench_run cold        none
bench_run wsharded    sharded
bench_run wreplicated replicated
log "ORCHESTRATION COMPLETE"
touch "$BENCH/DONE"
