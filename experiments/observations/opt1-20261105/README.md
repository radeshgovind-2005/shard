# OPT-1 (H1) plan and correctness check, 2026-11-05

**These are correctness runs on 2025-01, not measurements.** They show that the plan changed as intended and that the results are identical. Timings in these files mean nothing; `benchmark-runner` measures OPT-1 on 2025.

## Where it came from

The same jar was run twice, on HEAD `98578d6` plus the uncommitted OPT-1 diff (branch `perf/persist-cleaned-trips`). `run.json` records `98578d6`. Both runs were profiled into a throwaway runs folder outside the repo, so `experiments/runs/` stays clean, and the event logs there aren't kept:

```sh
# off: the baseline code path
make run ARGS="--input data --output output/opt1-off --years 2025-01 --skip-opts opt1 --run-id local-20261105-81 --runs-dir /private/tmp/claude-501/opt1-check --git-sha 98578d6"
# on: the default
make run ARGS="--input data --output output/opt1-on --years 2025-01 --run-id local-20261105-82 --runs-dir /private/tmp/claude-501/opt1-check --git-sha 98578d6"
```

`run.json` records `"skip_opts": "opt1"` for 81 and `"skip_opts": ""` for 82. Both use local `local[*]` (10 cores), with no `spark.*` tuning.

| File | What |
|---|---|
| `plans/off-local-20261105-81/<op>.txt`, `plans/on-local-20261105-82/<op>.txt` | final plans (`isFinalPlan=true`) of M2, M3, O1 and O3, copied from `<runs-dir>/<run-id>/plans/` |
| `plan-nodes.txt` | per op and side, counts of the nodes that matter in the final plan (command in its header), plus O3's initial-plan joins |
| `eventlog-local-20261105-81.txt`, `-82.txt` | per stage: job group, tasks, records read; per SQL execution: the scan's `size of files read`. Made by `baseline-20261105/scripts/eventlog.py` (command on line 1) |
| `correctness.txt` | the off-vs-on comparison of every output (commands, results, negative control) |
| `scripts/compare-opt1.scala`, `scripts/control-opt1.scala` | the comparison and the control, run in the Docker master's `spark-shell --master local[2]` |

## Reading the plans

A cached plan still *prints* `FileScan parquet`, because `InMemoryRelation` shows the plan it caches as an inner child. What matters is the node that runs: `TableCacheQueryStage → InMemoryTableScan` instead of `ColumnarToRow → FileScan parquet` under the operation's first aggregate. The event log confirms it:

| | off (81) | on (82) |
|---|---|---|
| M2 scan stage | 3,475,226 Parquet records | 3,475,226 Parquet records (stage 6, which builds the cache) + 322 cached batches (stage 8) |
| M3 / O1 scan stage | 3,475,226 Parquet records each | **322 cached batches** each (input records for a cache read count batches, not rows) |
| O3 scan stage | 3,475,226 Parquet records (one stage, its aggregate reused by the lag side) | **322 cached batches, twice** (stages 55 and 56) |
| `size of files read`, M3 / O1 / O3 executions | 59,170,569 B (trips 59,158,238 + zones 12,331) | **12,331 B** (zone CSV only) |
| bytes read from the cache, per cache scan | – | 174,182,779 B (the stored size of the cached blocks; 10 partitions) |

**O3 surprise.** Over the cache, O3's initial (pre-AQE) plan has 3 `SortMergeJoin`s instead of 0, so the grid and self-join sides are shuffled. AQE demotes all three to `BroadcastHashJoin` at runtime (`AQEShuffleRead local`), but the shuffles stay: 7 hash exchanges instead of 2, and 12 stages instead of 7. The lag side's trip aggregate is no longer reused either (`ReusedExchange` 2 → 1), so O3 scans the cache twice. Only the grid's exchange is reused. The likely cause is the `InMemoryRelation`'s size estimate, which covers all 13 cached columns, against the Parquet scan's estimate. That isn't verified. On 2025 the baseline already picks `SortMergeJoin` for both grid joins (LOG L15), so the effect there may differ: `benchmark-runner` should check O3's final plan on 2025.
