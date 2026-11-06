# H5 attempt (persist O3's zero-filled grid), 2026-11-06: stopped after the 2025 plan, not an OPT

**Outcome:** H5 was not pursued as OPT-3. The persist removes the duplicate zero-fill join, but at default settings the cached plan keeps 200 partitions (`spark.sql.optimizer.canChangeCachedPlanOutputPartitioning` is `false` in Spark 4.2.1), which made O3 worse (855 vs 76 tasks, +30 MB shuffle, 184 vs 101 s executor time in one run). With that setting on (run 91, diagnostic) the shuffle matches the baseline, but building the cache cost about as much executor time as the two copies it replaces, and it needs a non-default `spark.*` setting. OPT-3 became H6 instead. The code is in [`uncommitted.diff`](uncommitted.diff) plus `CachedHourlyGridTest.kt.txt` (the test it came with); it never landed. The run ids 89–91 are throwaway (runs folder outside the repo), not entries in LOG → Runs.


**These are plan and correctness runs, not measurements.** One run per configuration, with the Docker cluster up (idle). Their times say nothing about OPT-3's effect; `benchmark-runner` measures that. The work stopped before the 2025-01 pair (87/88) and before LOG / README entries, because the 2025 plan (below) hit the brief's stop condition.

## Where it came from

One build: HEAD `83591d8` (branch `perf/o3-reuse-zero-filled-grid`) plus the uncommitted OPT-3 diff ([`uncommitted.diff`](uncommitted.diff)). After the runs only one comment in `src` changed (the cache size in `Pipeline.kt`'s `cachedForOpt3` comment), so the diff is the code that ran. `run.json` records `83591d8`. O3 only, on local `local[*]` (10 cores), profiled into a throwaway runs folder outside the repo:

```sh
R=/private/tmp/claude-501/opt3-check
# off: --skip-opts opt3, no cache (the baseline code path)
make run ARGS="--input data --output output/opt3-off-2025 --years 2025 --ops o3 --skip-opts opt3 --run-id local-20261105-89 --runs-dir $R --git-sha 83591d8"
# on: the default, `hourly` persisted (MEMORY_AND_DISK) around O3's write
make run ARGS="--input data --output output/opt3-on-2025 --years 2025 --ops o3 --run-id local-20261105-90 --runs-dir $R --git-sha 83591d8"
# diag: the same as on, plus one Spark SQL conf passed as a JVM system property (a diagnostic, not in the code)
JAVA_TOOL_OPTIONS="-Dspark.sql.optimizer.canChangeCachedPlanOutputPartitioning=true" \
  make run ARGS="--input data --output output/opt3-diag-2025 --years 2025 --ops o3 --run-id local-20261105-91 --runs-dir $R --git-sha 83591d8"
```

`run.json` records `"skip_opts": "opt3"` for 89 and `""` for 90 and 91. Run 91's `spark_conf` holds `spark.sql.optimizer.canChangeCachedPlanOutputPartitioning=true`. That setting defaults to `false` in Spark 4.2.1 (checked with `SET` in the Docker master's `spark-sql`). During 90 and 91, a shell loop polled the driver's `/api/v1/applications/<app>/storage/rdd?details=true` every 2 s. The local driver's UI was on port 4051, because OrbStack publishes 4040–4050 for the Docker master.

| File | What |
|---|---|
| `plans/{off,on,diag}-2025-local-20261105-9x/o3.txt` | O3's final plans (`isFinalPlan=true`), copied from `<runs-dir>/<run-id>/plans/` |
| `stages-local-20261105-{89,90,91}.txt` | O3's stages from `stages.json`, plus the block totals from `run.json` |
| `storage/local-20261105-{90,91}-last-poll.json` | the last storage poll before `unpersist` (the cached RDD, per partition) |
| `console-local-20261105-{89,90,91}.txt` | the tables each run printed |
| `correctness.txt`, `scripts/` | off vs on and off vs diag on 2025, the commands, and the negative control |

## The plans (final plans, 2025)

| | off (89) | on (90), default confs | diag (91), cache coalesced |
|---|---|---|---|
| zero-fill `SortMergeJoin LeftOuter` | twice: once per self-join side, above 2 `ReusedExchange`s | **once**, inside the `InMemoryRelation`'s cached plan | **once**, same |
| self-join sides read | the zero-fill join, each recomputing it | `TableCacheQueryStage` → `InMemoryTableScan` (6 columns current side, 3 lag side) | same |
| `AQEShuffleRead coalesced` above the zero-fill join's inputs | yes | **no**: the cached plan keeps its output partitioning (`canChangeCachedPlanOutputPartitioning=false`), so its final stage runs at `spark.sql.shuffle.partitions` = 200 | yes |
| cached RDD | – | **200 / 200 partitions, 18,043,080 B in memory, 0 on disk** (≈ 90 KB each) | **11 / 11 partitions, 12,283,416 B in memory, 0 on disk** |
| self-join | `SortMergeJoin LeftOuter`, unchanged | same | same |

The cached plan prints twice in `on`/`diag`'s `o3.txt`, once under each `InMemoryRelation` reference. It is the same relation, built once.

## Stages (O3 block, `stages-*.txt`)

| | off (89) | on (90) | diag (91) |
|---|---|---|---|
| stages / tasks | 8 / 76 | 10 / **855** | 10 / 98 |
| stages reading the zero-fill input (34,530,083 B shuffle each) | **2** (7, 8; 11 tasks each) | **1** (7; **200 tasks**) | **1** (7; 11 tasks) |
| cache reads | – | 3 stages × 200 tasks, 18.0 MB each: 9 = current side, 10 = lag side; 8 reads the cache and writes nothing (72 ms of run time; by its metrics the second `TableCacheQueryStage`'s materialization, not verified) | 3 stages × 11 tasks, 12.3 MB each (same roles) |
| window exchange write (current side) | 36,538,179 B | **57,553,019 B** (200 map tasks) | 36,538,179 B |
| lag-side exchange write | 25,672,198 B | **34,847,262 B** | 25,672,198 B |
| shuffle write, block total | 164,069,755 B | **194,395,613 B** | 164,069,755 B (= off) |
| executor run time, block total | 100.6 s | **184.1 s** | 119.8 s |
| wall (single runs, **not measurements**) | 12.63 s | 20.91 s | 13.63 s |

## Why it stopped

With default confs, the cache does remove the duplicate zero-fill join (2 stages → 1). But it also fixes the zero-filled grid at 200 partitions, because AQE won't coalesce a cached plan's final stage while `spark.sql.optimizer.canChangeCachedPlanOutputPartitioning` is `false` (its default). Every stage that reads the cache then runs 200 tiny tasks, and its shuffle writes grow by 30 MB. O3's tasks go from 76 to 855, and its executor run time from 100.6 s to 184.1 s. That fits the brief's "the cache changes the plan in a way that makes it worse". So the remedy wasn't changed and H6 wasn't started.

Run 91 shows the cause: with that one conf, the cached plan is coalesced to 11 partitions (12.3 MB in memory), the zero-fill join still runs once, and every exchange writes exactly the off run's bytes. In that run, executor run time was still above off (119.8 s vs 100.6 s): building the cache (stage 7, 22.1 s) costs about as much as the two copies it replaces (13.8 + 9.7 s). That's one run, not a measurement.
