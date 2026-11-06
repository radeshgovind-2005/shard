## Project Overview

SHARD is ComPar 26/27 Project 1 (NOVA FCT): large-scale analytics over NYC TLC Yellow Taxi trips (2009–2026, Parquet) on Apache Spark, written in **Kotlin against Spark's JVM (Scala) API**. That is a deliberate choice: Spark runs on JVM bytecode, so Kotlin uses the Scala API directly.

What's graded is the **performance engineering**, not the analytics: profile → hypothesise → optimise → measure → repeat, written up as a 4-page report (`docs/report/`). Every optimisation needs a measured bottleneck before it and a before/after measurement after it.

Operations: M1 ingestion & cleaning, M2 temporal demand, M3 zone statistics, plus O1 fare anomaly detection (window shuffle and skew) and O3 demand forecasting features (self-join SortMergeJoin); the choice is in README → Why / Features. Deadline: 2026-11-06 23:59. Full spec: `docs/Project_Description.md`.

## Commands

The Makefile is the single entry point; hooks and CI call the same targets.

- `make setup` — install Node tooling and Husky Git hooks (run once after cloning)
- `make gh-setup` — one-time GitHub config: squash-only merges, title-only squash commits, main ruleset (needs CI on main first)
- `make build` — compile without tests
- `make test` — run tests
- `make lint` — check formatting (Spotless + ktlint)
- `make format` — apply formatting
- `make check` — lint + build + test (pre-push and CI)
- `make jar` — fat jar for `spark-submit` (`build/libs/shard-all.jar`: our code + Kotlin stdlib, no Spark)
- `make run ARGS="..."` — run locally (`local[*]`, Spark on the classpath from Gradle)
- `make data [YEARS=2025]` — download trips + zone lookup into `data/` (`YEARS=2011-2026`, `YEARS="2024 2025"`, `YEARS=2025-01`)
- `make data-sample` — one month (2025-01) for smoke tests
- `make data-size [YEARS=...]` — download size vs free disk, without downloading
- `make data-clean YEARS=...` — delete downloaded files to free disk (`YEARS=all` for everything; keeps `MANIFEST.tsv`)
- `make clean` — remove build outputs

Commits follow Conventional Commits (enforced by commitlint).

## Architecture Map

Current state: **M3 done (Phase 1d), profiler built (Phase 2, roadmap 01 Part B).** Argument parsing, input checks, trip loading, M1 (schema enforcement plus the per-year cleaning stats written to `<output>/m1/`), M2 (temporal demand per borough, day and hour, written to `<output>/m2/`) and M3 (zone-month statistics with rank, rolling share and anomaly windows, written to `<output>/m3/`) run end to end, O1 (fare anomalies per zone-hour bucket, weekday vs weekend rates written to `<output>/o1/`) and O3 (zone-hour demand features with a 4-week-lag self-join, written to `<output>/o3/`) are built, and `--run-id` profiles each operation's write into `experiments/runs/<run-id>/`. The profiled baseline (roadmap 01 Part C) are next.

Data flow, in call order (all under `src/main/kotlin/com/radeshgovind/`):

```
Main.kt  parseArgs ─► findTripFiles ─► missingZonesFile ─► [--run-id: prepareRunDir] ─► createSparkSession(eventLogDir).use { runPipeline(...) }
            │              │                  │                    │
            └─ Invalid ────┴─ Missing ────────┴─ Missing ──────────┴─ run dir exists ─► stderr + exit 2 (before Spark starts)

runPipeline:  profilerFor(args)   (SparkProfiler with --run-id, else NoProfiler)
              loadTrips(files, fraction) ─► enforceSchema ─┬─► profile("m1") { cleaningStats ─► <output>/m1/ }                     (M1)
                                                           └─► removeInvalidTrips ─┬─► profile("m2") { temporalDemand(loadZones) ─► <output>/m2/ }  (M2)
                                                                                   ├─► profile("m3") { zoneStatistics(loadZones) ─► <output>/m3/ }   (M3)
                                                                                   ├─► profile("o1") { fareAnomalyRates(loadZones, bucketStatsPlanFor(args)) ─► <output>/o1/ }  (O1)
                                                                                   └─► profile("o3") { demandForecast(zoneHourGrid(months), loadZones) ─► <output>/o3/ }  (O3)
              profiler.finish()  ─► <runs-dir>/<run-id>/run.json, stages.json, plans/<op>.txt + summary table
```

- `Main.kt`: `com.radeshgovind.MainKt`, the application and jar entry point. It owns the exit codes: 0 ok, 2 bad arguments or missing files, 1 a failure inside Spark.
- `cli/`: `ParseArgs.kt` (flags → `PipelineArgs`, or `ArgsParse.Invalid` with a one-line reason), `MonthRange.kt` (`--years`: `YYYY`, `YYYY-YYYY`, `YYYY-MM`, 2011 onwards), `Fraction.kt` (0 < f ≤ 1), `Operation.kt` (`M1`–`M3`, `O1`, `O3`), `Optimisation.kt` (`--skip-opts`: the OPT ids to switch off, all on by default: `opt2`; OPT-1 was reverted), `CommaList.kt` (the shared `a,b,c` parser), `ProfilingArgs.kt` (`--run-id`, `--runs-dir`, `--git-sha`; null when profiling is off)
- `ingest/`:
  - `TripFiles.kt`: the exact Parquet path for each month, with no globs, as `Found` or `Missing`
  - `LoadTrips.kt`: reads those files, adds `source_year`/`source_month` from each file's name (`_metadata.file_path`, right on the scan), then samples with a fixed seed (42) when `--fraction` < 1
  - `EnforceSchema.kt`: `enforceSchema()` selects and casts the 13 columns of `ENFORCED_SCHEMA` once (timestamps stay `TIMESTAMP_NTZ`) and drops everything else
  - `TripColumns.kt`: trip column names as constants
  - `ZonesFile.kt`: the zone lookup's path, and `missingZonesFile()`, checked before Spark starts when a requested operation has `readsZones` (M2, M3, O1, O3)
  - `LoadZones.kt` and `ZoneColumns.kt`: the zone table, read with an explicit schema
- `clean/` (M1):
  - `InvalidTrip.kt`: an enum of the five criteria, each a never-null `condition()` plus its stat column name. The same list drives the filter and the stats.
  - `RemoveInvalidTrips.kt`: `removeInvalidTrips()`, the clean trips that M2, M3 and O1 start from
  - `CleaningStats.kt`: `cleaningStats()`, one `groupBy(source_year)` holding every count. Decisions D1–D6 are in `roadmap/02-M1-CLEANING.md` and README → M1.
- `demand/` (M2):
  - `TemporalDemand.kt`: `temporalDemand(zones)`, the M2 story: borough join, trip times, then one `groupBy(pickup_borough, day_of_week, hour_of_day)` with `trip_count`, `avg_fare`, `avg_duration_min` and `avg_distance`
  - `WithPickupBorough.kt`: `withPickupBorough(zones)`, a plain inner join on `PULocationID` = `LocationID` with no hint (the baseline). The zones are a parameter, so a later join hint stays visible in `Pipeline.kt`.
  - `WithTripTimes.kt`: `withTripTimes()` adds `day_of_week` (`dayofweek`: 1 = Sunday … 7 = Saturday), `hour_of_day` and `duration_min`. Decision D7: the duration is NTZ minus NTZ, not `timestamp_diff`, which would apply the session time zone (README → M2).
  - `DemandColumns.kt`: M2's column names as constants (`pickup_borough` and `trip_count` are reused by M3)
- `zones/` (M3; decisions D8–D14 in README → M3):
  - `ZoneStatistics.kt`: `zoneStatistics(zones)`, the M3 story: zone-month stats, then the borough join (`withPickupBorough`, reused from `demand/`), the three windows, and a final `select` that fixes the column order and drops `LocationID`
  - `ZoneMonthStats.kt`: `zoneMonthStats()`, one `groupBy(PULocationID, source_year, source_month)` with `trip_count`, `avg`/`stddev` (sample) of fare and distance, plus `month_index = year*12 + month` (D8)
  - `WithVolumeRank.kt`: window 1, `rank()` over (`pickup_borough`, `month_index`) by `trip_count` desc
  - `WithRollingTripShare.kt`: window 2, zone and borough trip sums over `rangeBetween(-2, 0)` on `month_index`, and their ratio
  - `WithVolumeAnomaly.kt`: window 3, the zone's mean and σ over its previous 3 months (`rangeBetween(-3, -1)`), and `is_volume_anomaly` (null without a σ)
  - `ZoneStatsColumns.kt`: M3's column names as constants
- `anomalies/` (O1; decisions D15–D22 and D31 in README → O1):
  - `FareAnomalies.kt`: `fareAnomalyRates(zones, plan)`, the O1 story: `fareAnomalies(plan)` (trip times from `demand/withTripTimes`, fare per km, bucket stats, flag), then the borough join (`withPickupBorough`), the day type, and one `groupBy(day_type, payment_type, pickup_borough)` with `trips`, `flagged_trips`, `anomaly_rate`
  - `WithFarePerKm.kt`: `fare_per_km = fare_amount / (1.609344 × trip_distance)` (D15)
  - `WithBucketFareStats.kt`: `withBucketFareStats(plan)`, which picks how each trip gets its bucket's `count`/`avg`/`stddev` of `fare_per_km` (bucket = `PULocationID`, `hour_of_day`), plus the three statistics both plans share
  - `BucketStatsPlan.kt`: `WINDOW` (D22 baseline, `--skip-opts opt2`) or `BROADCAST_AGGREGATE` (OPT-2, the default; D31); `Pipeline.kt` chooses it from `args.isApplied(Optimisation.OPT2)`
  - `WithWindowBucketFareStats.kt`: the baseline window over `Window.partitionBy(PULocationID, hour_of_day)` (no `orderBy`: the whole bucket). Shuffles and sorts every trip (D22).
  - `WithBroadcastBucketFareStats.kt`: OPT-2, `groupBy(PULocationID, hour_of_day)` statistics `broadcast()` back onto the trips with a null-safe join. No trip-sized shuffle, but a second trip scan while nothing is cached.
  - `WithFareAnomalyFlag.kt`: `is_fare_anomaly` = `fare_per_km > mean + 3σ`, null without a σ (D17, D18; Samuelson: n ≤ 10 never flags)
  - `WithDayType.kt`: `weekend` for `dayofweek` ∈ {1, 7}, else `weekday` (D19)
  - `AnomalyColumns.kt`: O1's column names as constants
- `forecast/` (O3; decisions D23–D30 in README → O3):
  - `DemandForecast.kt`: `demandForecast(grid, zones)`, the O3 story: zero-filled hourly counts, bucket stats, the 4-week-lag self-join, the demand index, `service_zone`, and a final `select` that fixes the column order
  - `ZoneHourGrid.kt`: `SparkSession.zoneHourGrid(months, zoneIds = 1..263)`, zones × every hour of the range (`range` × `range`, hour bounds from `LocalDate` on the driver). The grid is a parameter, so its size stays visible in `Pipeline.kt`.
  - `HourIndex.kt`: `hourIndex(pickup)` = `unix_date(to_date(pickup)) × 24 + hour(pickup)`, zone-free on `TIMESTAMP_NTZ` (D24)
  - `HourlyTripCounts.kt`: `hourlyTripCounts()` (`groupBy(PULocationID, hour_index)`), and `withHourlyCounts(counts)`: the grid left-joined with them, missing counts → 0 (D25), plus `pickup_date`, `day_of_week` and `hour_of_day` derived from the index
  - `WithBucketStats.kt`: `avg` and `var_samp` of `trip_count` over the whole (`PULocationID`, `day_of_week`, `hour_of_day`) bucket (D26)
  - `WithFourWeekLag.kt`: `withFourWeekLag(hourly)`, the spec's self-join: `hourly` shifted by +672 hours and left-joined back on (`PULocationID`, `hour_index`), with no hint (D27)
  - `WithDemandIndex.kt`: `demand_index` (null when the mean is 0) and `is_demand_spike` (> 2.0, false when the mean is 0) (D28)
  - `WithServiceZone.kt`: a plain inner join with the zone lookup for `service_zone` (D29)
  - `ForecastColumns.kt`: O3's column names as constants
- `profiling/` (roadmap 01 Part B; output layout in README → Profiling):
  - `SparkProfiler.kt`: `Profiler` (`profile(name) { … }`, `finish()`), `NoProfiler` (just runs the block) and `SparkProfiler`. Each block runs under its own `setJobGroup`; the listener bus is drained (`listenerBus().waitUntilEmpty`) before and after it, because listener events are asynchronous. **One action per block.**
  - `MetricsListener.kt`: the `SparkListener`: job → group and stage ids, successful task durations per stage, completed stages
  - `StageMetrics.kt` / `ProfiledRun.kt`: the B3 metrics per stage (with `task_ms_median`, `task_ms_max`, `skew_ratio`) and per block
  - `PlanCapture.kt`: a `QueryExecutionListener` keeping each action's final (post-AQE) physical plan
  - `RunReport.kt`: writes `run.json`, `stages.json` and `plans/` with Jackson (from Spark's classpath, snake_case), prints the summary table, and names the `eventlog/` folder
  - `RunId.kt`: `^(local|docker|di)-\d{8}-\d{2}$`
- `SparkSessions.kt`: uses `local[*]` unless `spark-submit` already set `spark.master`. The only other `spark.*` config is the event log, and only for profiled runs.
- `Pipeline.kt`: `runPipeline`, the story of the pipeline and **the only place with Spark actions**. Each operation's write is wrapped in `profiler.profile("<op>") { … }`; read-backs and prints stay outside the block. Optimisations stay visible here too, each behind `args.isApplied(Optimisation.OPTn)`.
- Build: `build.gradle.kts` (Spark 4.2.1 as a `spark` configuration: compile + run + test, kept out of the fat jar; Java 21; Spark's JVM module flags), `gradle.properties` (`sparkVersion`)
- Data: `scripts/download-data.sh` → `data/trips/yellow_tripdata_YYYY-MM.parquet`, `data/taxi_zone_lookup.csv`, `data/MANIFEST.tsv`
- Cluster: `docker/docker-compose.yml` (standalone Spark 4.2.1 / Java 21; `data/` → `/data` read-only, `build/libs/` → `/jars`, `output/` → `/output`, `experiments/runs/` → `/runs` on the master only)

## Code Conventions

- **Style:** Kotlin official style, enforced by Spotless + ktlint (`intellij_idea`, 120 cols). Run `make format`; never hand-format.
- **Structure:** follow the `kotlin-stepdown` skill (`.claude/skills/kotlin-stepdown/`). Entry point first, helpers below in call order, one level of abstraction per function, one concept per file, packages by feature (`com.radeshgovind.ingest`, `.clean`, `.demand`, `.zones`, `.anomalies`, `.cli`).
- **Spark** (details in `references/spark.md` of that skill):
  - Steps are extension functions on `Dataset<Row>` that return `Dataset<Row>` and **never run actions**. Hidden `count()`/`show()` calls add Spark jobs and distort profiling.
  - Enforce the schema once at load; keep column names as constants; prefer built-in `functions.*` over UDFs.
  - Keep performance decisions (`persist`/`unpersist`, `broadcast()`, repartitioning) visible in the top-level pipeline function.
- **Kotlin ↔ Scala API:** use `Column` methods (`.gt()`, `.and()`, `.equalTo()`), since Scala operators aren't available. Avoid `Dataset.transform` (it needs a Scala `Function1`). Convert collections explicitly when an API wants a Scala `Seq`.
- **Inputs:** data paths and dataset scale come from CLI arguments, never hard-coded. The input schema drifts across years (2009–2010 have no zone IDs; types and column names change later): see README → Dataset. Any non-default `spark.*` setting is documented in README → Configuration.

## Testing and Verification

- `make check` = lint + build + test. CI and the `pre-push` hook run the same thing.
- `SparkSessionTest` runs a local Spark job with a shuffle. It guards the build setup (Spark on the classpath, Java 21, module flags).
- `cli/ParseArgsTest`, `cli/MonthRangeTest`, `ingest/TripFilesTest` and `ingest/ZonesFileTest` cover argument parsing and file resolution without Spark, so they run in milliseconds.
- `zones/ZoneStatisticsTest` builds 86 trips over 4 zones and 4 months: a zone with 10, 12, 11 trips and then 40 (flagged in April, not in March, null in January and February), a zone with no February (its April rolling share must leave January out), Manhattan ranks 10, 3, 3 → 1, 2, 2, a single-trip zone-month with a null σ, and Σ `trip_count` = input trips.
- `anomalies/FareAnomaliesTest` builds 22 trips in 3 buckets: an 11-trip bucket with hand-checked mean and σ whose one outlier is flagged (3.015 σ), a 10-trip bucket whose 1,000,000 fare is not (Samuelson: at most 2.846 σ), a single-trip bucket with null σ and flag, the weekend edges (Fri 23:59, Sat 00:00, Sun 23:59, Mon 00:00), the rate table per (day type, payment type, borough) with a null payment type kept, Σ `trips` = input trips and flagged ≤ trips.
- `ingest/EnforceSchemaTest`, `clean/CleaningTest` and `demand/TemporalDemandTest` run on a local Spark session from `localTestSession()` (`src/test/kotlin/com/radeshgovind/LocalSpark.kt`). The cleaning fixture has one row per failure case, a null-passenger row and an overlap row. The demand fixture has six trips across two boroughs with hand-checked counts and means, plus two time-zone tests: a trip in Lisbon's DST overlap hour pins D7, and an Asia/Tokyo session shows that `hour`/`dayofweek` ignore the session zone.
- `forecast/DemandForecastTest` runs O3 on a 2-zone January–February grid: rows = zones × hours; 3 trips in 1 of 9 Wednesdays give μ = 1/3 and variance 1 (zero-fill); 2025-02-03's lag is 2025-01-06's count, January 6 has a null lag and an empty lag hour gives 0; a 10-trip Thursday against 1-trip Thursdays has DI 5 (flagged) and the others 0.5; a zero mean gives a null DI and no spike; a 2024 trip is left out; and in a Europe/Lisbon session the hour index and each trip's hour are unchanged on 2025-03-30 and 2025-10-26.
- `profiling/SparkProfilerTest` covers roadmap 01 B6: a shuffle job (≥ 2 stages, shuffle R/W > 0), a job without a shuffle (`collect()`; `count()` has an `Exchange SinglePartition`), two blocks keeping their own stages, and the files written (parsed back with Jackson).
- Inside an extension on `Dataset<Row>`, a bare `col(...)` resolves to the receiver's member `Dataset.col`, not `functions.col`. Build columns in top-level helpers instead.
- Smoke check (verified 2026-11-05): `--years 2025-01` reads 3,475,226 trips both locally and on Docker. `--fraction 0.1` gives 347,645, the same on every run.
- TODO as the operations land: unit tests per operation, and a correctness check on a one-month sample (row counts plus an `except`-diff in both directions). Every optimisation must keep results identical.

## Workflow

Once `make gh-setup` has run, `main` is protected: every change goes through a PR, is squash-merged, and lands as one title-only Conventional Commit.

1. **Branch** — `<type>/<kebab-description>` from an up-to-date main, e.g. `feat/m2-temporal-demand`, `perf/broadcast-zone-lookup`. The `pre-push` hook rejects other names.
2. **Commit** — one logical change per commit, title only: `type(scope): subject`, ≤ 72 chars, imperative, lowercase. Types: `feat fix perf refactor test docs build ci chore style revert`. Scopes: `m1`–`m3`, `o1`–`o4`, `ingest`, `spark`, `report`, `ci`, `tooling`.
3. **PR** — the PR title follows the same convention, because it becomes the commit on main (CI lints it). Fill in the template; `perf` PRs cite H/OPT ids and run-ids from `experiments/LOG.md`.
4. **Merge** — squash only, after `build` and `commitlint` pass. The branch is deleted automatically.

Never commit dataset files (`*.parquet`, `data/`), and never force-push or bypass hooks.

## Boundaries

- **Never commit** dataset files (`*.parquet`, `data/`), Spark event logs, or secrets (`.env*`).
- **`data/` is read-only input.** The pipeline never writes there; outputs go to `output/` (or a CLI-given path). That's what makes `make data-clean` + re-download safe.
- **No speculative optimisation.** Only change performance-related code for a hypothesis logged in `experiments/LOG.md` with a measured symptom, as the assignment requires.
- **Never change what an operation computes** as a "performance fix". Verify that results are identical.
- **Every number** in the report or a PR traces to a run-id in `experiments/LOG.md`. Never estimate or round in your favour.
- **Read-only:** `docs/lectures/` (reference material), `docs/report/acmart.cls`, `docs/report/acm.bst`.
- **Git:** never force-push, use `--no-verify`, or merge with `--admin`.

## Further docs

- `docs/Project_Description.md`: assignment spec, report structure, grading
- `docs/lectures/index.md`: lecture knowledge base (start here; it has a keyword lookup). For numbers and formulas: `docs/lectures/formulas-cheatsheet.md`
- `docs/report/report.tex`: the report (acmart, from `template.tex`; charts read `experiments/analysis/*.csv`). Build with `latexmk -pdf -cd docs/report/report.tex`. `template.tex` doesn't build on TeX Live 2026 (the bundled `acmart.cls` loads `hyperxmp` before `hyperref`); `report.tex` works around it in its first lines
- `experiments/LOG.md`: environments, runs, observations (unmeasured leads from development runs), hypotheses, optimisations
- `docs/lab-inputs.md`: what is still needed from the labs and the course (Docker Spark, Lab 3 profiler, DI cluster, submission)
- `.claude/skills/kotlin-stepdown/`: code style, plus `references/spark.md` for Spark specifics
