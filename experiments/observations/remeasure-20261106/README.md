# Baseline and final pipeline, re-measured 2026-11-06 (derived from run-ids)

Derived from the profiled runs `local-20261106-12` … `-26` and `docker-20261106-12`, `-14` … `-36` (all sha `606dce5` = main; LOG → Runs, "How the re-measured baseline runs were run"). Baseline = `--skip-opts opt2` (O1's window, exactly the 2026-11-05 baseline's code path), final = no flag (OPT-2 on, the final pipeline). Event logs stay in the gitignored `experiments/runs/<id>/eventlog/`; nothing here needed them (every number comes from `run.json` / `stages.json`). The first line of each file is its command (run from the repo root; the lists are interleaved baseline/final order).

Sets used:

| | warm-up (discarded) | baseline (`--skip-opts opt2`) | final |
|---|---|---|---|
| local | 12 | 13, 15, 17, 19, 21, 23, 25 | 14, 16, 18, 20, 22, 24, 26 |
| Docker, 2 workers | 12 | 23, 25, 27, 29, 31, 33, 35 | 24, 26, 28, 30, 32, 34, 36 |

Pairs 23/24 and 25/26 (local) and 33/34 and 35/36 (Docker) were added because the spread stayed above 10 % (see the LOG note). **Docker runs 14, 16, 18, 20, 22 are not used**: the first attempt at the interleaved sequence had a quoting bug in the driver script (`--skip-opts opt2` reached `spark-submit` as one argument, which `shard` rejects with exit 2 after 0.5 s), so the baseline slots 13, 15, 17, 19, 21 produced no run (no `run.json`; they appear in `bench/state-docker.txt` and as empty md5 in `bench/printed-tables-md5.txt`) and the finals 14…22 ran without interleaved baselines. They are valid runs (sha 606dce5, same cluster) and kept in the CSV with `measured` = 0; the Docker sequence was then repeated interleaved with new ids 23…36 on the same cluster.

| File | What | Produced by |
|---|---|---|
| `table-{local,docker}.txt` | per op median [min-max] (spread) for both sides, shuffle, spill, tasks, stages, GC %, CPU ÷ run time, max task skew, paired total differences, delta of medians, the M1 scan-stage CPU-time indicator for every run | `scripts/table.py` (new, Python 3 stdlib) |
| `compare-{local,docker}.txt` | baseline vs final per metric and op (wall, stages, tasks, shuffle R/W, disk and memory spill, executor run time), per-run walls | `opt2-20261105/bench/scripts/compare.py` |
| `attribution-{local,docker}-{baseline,final}.txt` | per op: median wall, scan stage, cpu/run, GC, max skew; median stage durations | `baseline-20261105/scripts/attrib.py` |
| `summary-{local,docker}-{baseline,final}.txt` | per run and op totals (as the 2026-11-05 `summary.txt`) | `baseline-20261105/scripts/summarize.py` |
| `bench/state-{local,docker}.txt` | `pmset` low-power flag, battery line and `uptime` before every run; `state-*-start.txt` before each set | the driver loop |
| `bench/printed-tables-md5.txt` | md5 of the `|`/`+` table lines of each run's console output | `grep -E '^[|+]' <console> \| md5` |
| `scripts/table.py` | written for this measurement | |

The per-run per-op walls are in `experiments/analysis/remeasure-20261106-wall.csv` (columns as the OPT-2 CSV; `measured` = 0 for the warm-ups and the unused Docker finals).

**Who ran what:** Claude (`benchmark-runner`) ran every command at the caller's request, one run at a time under `caffeinate -i`. Local: `make run ARGS="--input data --output output --years 2025 --run-id local-20261106-<n> --git-sha 606dce5 [--skip-opts opt2]"`; Docker: `docker compose -f docker/docker-compose.yml exec -T spark-master /opt/spark/bin/spark-submit --class com.radeshgovind.MainKt --master spark://spark-master:7077 /jars/shard-all.jar --input /data --output /output --years 2025 --run-id docker-20261106-<n> --git-sha 606dce5 --runs-dir /runs [--skip-opts opt2]`. Console output went to the session scratchpad.
