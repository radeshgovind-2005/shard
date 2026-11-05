# OPT-1 measurement, 2026-11-05 (derived from run-ids)

Derived from the profiled runs `local-20261105-08` … `-16` and `docker-20261105-05` … `-11` (all sha `0d5657b`; LOG → Runs, "How the OPT-1 runs were run"). Before = `--skip-opts opt1`, after = no flag, same jar. Warm-ups (discarded): local-08, docker-05. Event logs stay in the gitignored `experiments/runs/<id>/eventlog/`; the tables taken from them are here. The first line of each file is its command (run from the repo root).

| File | What | Command |
|---|---|---|
| `summary-local.txt`, `summary-docker.txt` | per op and total: median [min-max] (spread) of wall, stages, tasks, shuffle R/W, spill, executor run time; before vs after; GC %; per-run walls | `python3 -I experiments/observations/opt1-20261105/bench/scripts/compare.py experiments/runs local local-20261105-09,-11,-13,-15 local-20261105-10,-12,-14,-16` (full ids, comma separated; Docker: `docker 06,08,10` vs `07,09,11`) |
| `attribution-local.txt`, `attribution-docker.txt` | baseline `attrib.py` per side: op wall, its largest-task-count stage ("scan"), median stage durations | `python3 -I experiments/observations/baseline-20261105/scripts/attrib.py experiments/runs <ids>` (before ids, then after ids) |
| `eventlog-<run>.txt` | per stage: job group, tasks, records read (48,722,602 = Parquet trips, 4,36x = cached batches); scan `size of files read` per SQL execution | `zstd -dc experiments/runs/<run>/eventlog/*/events_* \| python3 -I experiments/observations/baseline-20261105/scripts/eventlog.py` |
| `scanrecs.txt` | the predicted metric per run and op: stages that read Parquet records, cached-batch counts, scan sizes | `python3 -I bench/scripts/scanrecs.py bench/eventlog-*.txt` |
| `stages-<run>.txt` | every stage of local-09/10 and docker-06/07 | `python3 -I experiments/observations/baseline-20261105/scripts/stages.py experiments/runs <run>` |
| `o3-plan-nodes.txt` | node counts (Exchange, ReusedExchange, joins, cache scans) in the final and initial O3 plan, off vs on, both environments | counting script in the file header; reads `experiments/runs/<run>/plans/o3.txt` |
| `storage/<run>.json` | the last non-empty `/api/v1/applications/<app>/storage/rdd` answer of each `on` run (cache memory and disk, per partition, per executor) | polled from the host every 2 s: `curl localhost:4040/api/v1/applications/<app>/storage/rdd` (local and Docker; Docker publishes 4040) |
| `scripts/compare.py`, `scripts/scanrecs.py` | the two scripts written for this measurement (Python 3 standard library) | |

The per-run per-op walls are in `experiments/analysis/opt1-20261105-wall.csv`.

**Who ran what:** Claude (`benchmark-runner`) ran every command here, at the caller's request. Local runs: `make run ARGS="--input data --output output --years 2025 --run-id <id> --git-sha 0d5657b [--skip-opts opt1]"`; Docker: `docker compose -f docker/docker-compose.yml exec -T spark-master /opt/spark/bin/spark-submit --class com.radeshgovind.MainKt --master spark://spark-master:7077 /jars/shard-all.jar --input /data --output /output --years 2025 --run-id <id> --git-sha 0d5657b --runs-dir /runs [--skip-opts opt1]`.

**Caveat:** the storage poll ran only during `on` runs (a `curl` every 2 s), so `off` ran without it.
