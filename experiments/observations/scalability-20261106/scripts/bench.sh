#!/usr/bin/env bash
# usage: bench.sh <label> <workers> <first-nn> <fraction> [<fraction> ...]
# One Docker run per fraction, in order, run-ids docker-20261106-<nn>, <nn+1>, ...
# Logs state before every run to $SP/state-<label>.txt and elapsed spark-submit time to $SP/elapsed.tsv.
set -u
REPO=/Users/radeshgovind/dev/nova-fct/parallel-computation/project/shard
SP=/private/tmp/claude-501/-Users-radeshgovind-dev-nova-fct-parallel-computation-project-shard/6b4e915e-f1b2-48bc-b14a-46be06a27b1c/scratchpad
SHA=3cc3a25
label=$1; workers=$2; nn=$3; shift 3
cd "$REPO"
for frac in "$@"; do
  id=$(printf "docker-20261106-%02d" "$nn")
  alive=$(curl -s localhost:8080/json/ | python3 -c "import json,sys;d=json.load(sys.stdin);print(d['aliveworkers'],d['cores'])")
  {
    echo "== $id label=$label workers=$workers frac=$frac $(date -u +%FT%TZ)"
    echo "alive workers/cores: $alive"
    pmset -g | grep -i lowpowermode
    pmset -g batt | tail -1
    uptime
  } >>"$SP/state-$label.txt"
  if [ "${alive%% *}" != "$workers" ]; then echo "ABORT: $alive workers alive, expected $workers" | tee -a "$SP/state-$label.txt"; exit 3; fi
  t0=$(python3 -c 'import time;print(time.time())')
  docker compose -f docker/docker-compose.yml exec -T spark-master /opt/spark/bin/spark-submit \
    --class com.radeshgovind.MainKt --master spark://spark-master:7077 /jars/shard-all.jar \
    --input /data --output /output --years 2025 --fraction "$frac" \
    --run-id "$id" --git-sha "$SHA" --runs-dir /runs >"$SP/console-$id.txt" 2>&1
  rc=$?
  t1=$(python3 -c 'import time;print(time.time())')
  el=$(python3 -c "print(f'{$t1-$t0:.2f}')")
  wall=$(python3 -c "import json;r=json.load(open('experiments/runs/$id/run.json'));print(round(sum(x['wall_time_s'] for x in r['runs']),2))" 2>/dev/null || echo NA)
  printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\n" "$id" "$label" "$workers" "$frac" "$rc" "$el" "$wall" | tee -a "$SP/elapsed.tsv"
  nn=$((nn + 1))
done
