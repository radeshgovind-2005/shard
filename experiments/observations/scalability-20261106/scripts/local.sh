#!/usr/bin/env bash
# usage: local.sh <first-nn> <master> [<master> ...]   e.g. local.sh 27 'local[*]' 'local[2]'
# make run with -Dspark.master=<master> (SparkSessions uses spark.master when it is set); fraction 1.0.
set -u
REPO=/Users/radeshgovind/dev/nova-fct/parallel-computation/project/shard
SP=/private/tmp/claude-501/-Users-radeshgovind-dev-nova-fct-parallel-computation-project-shard/6b4e915e-f1b2-48bc-b14a-46be06a27b1c/scratchpad
SHA=3cc3a25
nn=$1; shift
cd "$REPO"
for m in "$@"; do
  id=$(printf "local-20261106-%02d" "$nn")
  { echo "== $id master=$m $(date -u +%FT%TZ)"; pmset -g | grep -i lowpowermode; pmset -g batt | tail -1; uptime; } >>"$SP/state-local.txt"
  t0=$(python3 -c 'import time;print(time.time())')
  JAVA_TOOL_OPTIONS="-Dspark.master=$m" make run ARGS="--input data --output output --years 2025 --run-id $id --git-sha $SHA" >"$SP/console-$id.txt" 2>&1
  rc=$?
  t1=$(python3 -c 'import time;print(time.time())')
  el=$(python3 -c "print(f'{$t1-$t0:.2f}')")
  info=$(python3 -c "import json;r=json.load(open('experiments/runs/$id/run.json'));print(r['master'], round(sum(x['wall_time_s'] for x in r['runs']),2))" 2>/dev/null || echo "NA NA")
  printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\n" "$id" "local" "${info% *}" "1.0" "$rc" "$el" "${info#* }" | tee -a "$SP/elapsed.tsv"
  nn=$((nn + 1))
done
