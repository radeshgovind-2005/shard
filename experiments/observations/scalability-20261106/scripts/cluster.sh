#!/usr/bin/env bash
# usage: cluster.sh <workers> <first-nn> <n-runs incl. warm-up>
# Fresh down / up with N workers, wait until N are ALIVE, record docker stats, then bench.sh at fraction 1.0.
set -u
REPO=/Users/radeshgovind/dev/nova-fct/parallel-computation/project/shard
SP=/private/tmp/claude-501/-Users-radeshgovind-dev-nova-fct-parallel-computation-project-shard/6b4e915e-f1b2-48bc-b14a-46be06a27b1c/scratchpad
N=$1; nn=$2; k=$3
cd "$REPO"
docker compose -f docker/docker-compose.yml down >/dev/null 2>&1
docker compose -f docker/docker-compose.yml up -d --scale spark-worker="$N" >/dev/null 2>&1
for i in $(seq 1 60); do
  a=$(curl -s localhost:8080/json/ 2>/dev/null | python3 -c "import json,sys;print(json.load(sys.stdin)['aliveworkers'])" 2>/dev/null)
  [ "$a" = "$N" ] && break; sleep 2
done
echo "== workers=$N up $(date -u +%FT%TZ): $(curl -s localhost:8080/json/ | python3 -c "import json,sys;d=json.load(sys.stdin);print('alive',d['aliveworkers'],'cores',d['cores'],'memMiB',d['memory'])")" | tee -a "$SP/cluster-up.txt"
fr=(); for _ in $(seq 1 "$k"); do fr+=(1.0); done
# docker stats sampled mid-run of the first measured run, in the background
( sleep 75; docker stats --no-stream --format '{{.Name}} {{.CPUPerc}} {{.MemUsage}}' >"$SP/dstats-w$N.txt" 2>&1 ) &
"$SP/bench.sh" "w$N" "$N" "$nn" "${fr[@]}"
wait
