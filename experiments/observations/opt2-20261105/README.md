# OPT-2 (H2) plan and correctness check, 2026-11-05

**These are correctness and plan runs, not measurements.** They show that the plan changed as intended and that the results are identical. Their times mean nothing (Docker was up during them, and nothing was repeated); `benchmark-runner` measures OPT-2 on 2025.

## Where it came from

One jar, built from HEAD `022fcfa` plus the uncommitted OPT-2 diff ([`uncommitted.diff`](uncommitted.diff): `git diff -- src` plus the three new files), branch `perf/o1-groupby-broadcast-stats`. `run.json` records `022fcfa`. After the runs, only one comment changed in `src` (`FareAnomalies.kt:21` now cites D31 too), so the diff is the code that ran. Each run ran O1 only (`--ops o1`), profiled into a throwaway runs folder outside the repo, so `experiments/runs/` stays clean and the event logs there aren't kept:

```sh
R=/private/tmp/claude-501/opt2-check
# 2025-01, off: the baseline window
make run ARGS="--input data --output output/opt2-off --years 2025-01 --ops o1 --skip-opts opt2 --run-id local-20261105-83 --runs-dir $R --git-sha 022fcfa"
# 2025-01, on: the default
make run ARGS="--input data --output output/opt2-on --years 2025-01 --ops o1 --run-id local-20261105-84 --runs-dir $R --git-sha 022fcfa"
# 2025, off / on: only for the 2025 final plans and stage table (AQE may plan one month differently)
make run ARGS="--input data --output output/opt2-off-2025 --years 2025 --ops o1 --skip-opts opt2 --run-id local-20261105-85 --runs-dir $R --git-sha 022fcfa"
make run ARGS="--input data --output output/opt2-on-2025 --years 2025 --ops o1 --run-id local-20261105-86 --runs-dir $R --git-sha 022fcfa"
```

`run.json` records `"skip_opts": "opt2"` for 83 and 85, and `""` for 84 and 86. All on local `local[*]` (10 cores), with no `spark.*` tuning. The Docker cluster was running (idle) at the same time.

| File | What |
|---|---|
| `plans/{off,on}-<scale>-local-20261105-8x/o1.txt` | O1's final plans (`isFinalPlan=true`), copied from `<runs-dir>/<run-id>/plans/` |
| `plan-nodes.txt` | per plan, counts of the nodes that matter in the final plan (command in its header), the hash exchanges with what feeds them, and the join keys |
| `stages-local-20261105-8x.txt` | O1's stages from `run.json`: tasks, shuffle write/read, disk and memory spill |
| `eventlog-local-20261105-8x.txt` | per stage: tasks, records read, shuffle records read; per SQL execution: the scan's `size of files read` (`baseline-20261105/scripts/eventlog.py`); then each `BroadcastExchange`'s driver metrics (`scripts/broadcast-size.py`). Commands on the `#` lines |
| `console-local-20261105-8x.txt` | the tables each run printed |
| `correctness.txt` | the off-vs-on comparison (rate tables on 2025-01 and 2025, trip level on 2025-01), commands, results and the negative control |
| `scripts/opt2-lib.scala`, `compare-opt2.scala`, `control-opt2.scala` | the comparison and the control, run in the Docker master's `spark-shell --master local[2] --jars /jars/shard-all.jar` |

## What changed in the plan

Final plans, node counts (`plan-nodes.txt`); the same on 2025-01 and 2025:

| | off (window) | on (groupBy + broadcast) |
|---|---|---|
| `FileScan parquet` (trips) | 1 | **2**: the statistics side scans the trips too (nothing is cached) |
| `Exchange hashpartitioning(PULocationID, hour_of_day, 200)` | 1, over **every trip** (the scan's projection) | 1, over the **partial aggregate** `HashAggregate(partial_avg, partial_stddev)` |
| `Sort` / `Window` | 1 / 1 | **0 / 0** |
| `HashAggregate` (of which partial) | 2 (1): the rate table | 4 (2): the rate table + the bucket statistics |
| `BroadcastExchange` / `BroadcastHashJoin` | 1 / 1 (zones) | 2 / 2 (zones + the statistics, `Inner, BuildRight`) |
| `SortMergeJoin` | 0 | 0 |

The statistics join's keys are `coalesce(key, 0), isnull(key)` for each of `PULocationID` and `hour_of_day`: that is how Spark plans the null-safe `<=>` as a hash join. The trip side's scan, filter, both broadcast joins and the rate table's partial aggregate run in one whole-stage-codegen stage. The bucket count (`count(1)`) is pruned from both plans, because nothing downstream reads `bucket_trips`.

## Stages, shuffle and spill (O1 block, `stages-*.txt`, `eventlog-*.txt`)

| | 2025-01 off (83) | 2025-01 on (84) | 2025 off (85) | 2025 on (86) |
|---|---|---|---|---|
| stages / tasks | 4 / 23 | 5 / 23 | 4 / 25 | 5 / 29 |
| shuffle write, total | 37,884,150 B (36.1 MiB) | 885,280 B (0.84 MiB) | **523,007,814 B (498.8 MiB)** | **3,411,111 B (3.25 MiB)** |
| … of which bucket exchange | 37,837,998 B, 3,203,825 records | 872,327 B, 18,632 records | 522,952,565 B, 43,599,832 records | 3,349,748 B, 68,546 records |
| disk spill / memory spill | 0 / 0 | 0 / 0 | **478,550,031 B (456.4 MiB) / 2,885,679,168 B** | **0 / 0** |
| Parquet records read | 3,475,226 (1 scan) | 6,950,452 (2 scans) | 48,722,602 (1 scan) | **97,445,204 (2 scans)** |
| `size of files read` (execution 0) | 59,170,569 B | 118,328,807 B | 829,985,630 B | 1,659,958,929 B |
| statistics broadcast: rows / `data size` | – | 5,535 / 17,039,344 B | – | 6,102 / 17,039,344 B |

- **The bucket exchange is 3.2 MiB on 2025, not under 1 MiB** (H2 predicted < 1 MiB). It carries partial-aggregate buffers, not buckets: each of the 12 scan tasks emits one row per bucket it saw (68,546 rows for 6,102 buckets), and each row holds `avg`'s sum and count plus `stddev`'s n, mean and m2. Still 99.3 % less than the 498.8 MiB the window shuffles.
- `data size` of a `BroadcastExchange` is the hashed relation's memory, including the page it allocates, not the bytes of its rows: the 263-row zone broadcast reports 67,110,952 B on the same metric, in both plans. Either is far below any driver or executor limit.
