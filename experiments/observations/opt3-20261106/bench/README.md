# OPT-3 measurement, 2026-11-06 (derived from run-ids)

Derived from the profiled runs `local-20261106-01` ... `-11` and `docker-20261106-01` ... `-11` (all sha `74aac23`; LOG -> Runs, "How the OPT-3 runs were run"). Before = `--skip-opts opt3` (the self-join SortMergeJoin; OPT-2 on in both), after = no flag, same jar. Warm-ups (discarded): local-01, docker-01 (on). Off: 02, 04, 06, 08, 10; on: 03, 05, 07, 09, 11, in both environments. Event logs stay in the gitignored `experiments/runs/<id>/eventlog/`; the tables taken from them are here. Commands run from the repo root; `R=experiments/runs`, `B=experiments/observations/opt3-20261106/bench`.

| File | What | Command |
|---|---|---|
| `summary-local.txt`, `summary-docker.txt` | per op and total: median [min-max] (spread) of wall, stages, tasks, shuffle R/W, spill, executor run time; before vs after; GC %; per-run walls | `python3 -I $B/scripts/compare.py $R local local-20261106-02,local-20261106-04,local-20261106-06,local-20261106-08,local-20261106-10 local-20261106-03,local-20261106-05,local-20261106-07,local-20261106-09,local-20261106-11` (Docker: same with `docker`/`docker-20261106-...`) |
| `o3-runs-{local,docker}.txt` | per run: O3 wall, stages/tasks, shuffle R/W, spill, O3 GC %, total wall and GC % | `python3 -I $B/scripts/o3-runs.py $R local <off ids> <on ids>` |
| `evsummary-<run>.txt` | BroadcastExchange driver metrics (rows >= 100,000), failed tasks/stages/jobs, lost executors, O3's stages with submit/complete times | `zstd -dc $R/<run>/eventlog/*/events_* \| python3 -I $B/scripts/evsummary.py $R/<run>/run.json` |
| `o3-timeline.txt` | per run and median: O3's final stage, the gap before it, the span, the broadcast metrics | `python3 -I $B/scripts/o3-timeline.py > $B/o3-timeline.txt` |
| `docker-risk.txt` | per Docker run: counts of `MemoryStore: Not enough space`, `OutOfMemoryError`, timeouts, task failures, WARN lines in the driver console and in each worker's executor stderr/stdout (`/opt/spark/work/<app>/*/`) | grep counts, taken right after each run (the worker work dirs are lost when the cluster is recreated) |
| `scripts/compare.py` | copied from `opt2-20261105/bench/scripts/` | |
| `scripts/evsummary.py`, `o3-runs.py`, `o3-timeline.py` | written for this measurement (Python 3 standard library) | |

The per-run per-op walls are in `experiments/analysis/opt3-20261106-wall.csv` (`measured` = 0 for the warm-ups; local and Docker rows in one file, written from each `run.json` by `compare.py`'s CSV option, once per environment, then concatenated).

**Who ran what:** Claude (`benchmark-runner`) ran every command, at the caller's request. Local: `make run ARGS="--input data --output output --years 2025 --run-id local-20261106-<n> --git-sha 74aac23 [--skip-opts opt3]"`; Docker: `docker compose -f docker/docker-compose.yml exec -T spark-master /opt/spark/bin/spark-submit --class com.radeshgovind.MainKt --master spark://spark-master:7077 /jars/shard-all.jar --input /data --output /output --years 2025 --run-id docker-20261106-<n> --git-sha 74aac23 --runs-dir /runs [--skip-opts opt3]`, all under `caffeinate -i`, one at a time, 5 s apart. Console output went to the session scratchpad; the printed tables were compared by md5 of their `|`/`+` lines (identical in all 22 runs).

**Caveat:** the Mac was in Low Power Mode on battery when checked after the runs (`pmset -g`), which probably explains why this session is ~40 % slower than the OPT-2 one. Not controlled for.
