## Project Overview

SHARD is ComPar 26/27 Project 1 (NOVA FCT): large-scale analytics over NYC TLC Yellow Taxi trips (2009–2026, Parquet) on Apache Spark, written in **Kotlin against Spark's JVM (Scala) API**. That is a deliberate choice: Spark runs on JVM bytecode, so Kotlin uses the Scala API directly.

What's graded is the **performance engineering**, not the analytics: profile → hypothesise → optimise → measure → repeat, written up as a 4-page report (`docs/report/`). Every optimisation needs a measured bottleneck before it and a before/after measurement after it.

Operations: M1 ingestion & cleaning, M2 temporal demand, M3 zone statistics, plus at least 2 of O1–O4 (TODO: which). Deadline: 2026-11-06 23:59. Full spec: `docs/Project_Description.md`.

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

Current state: **M2 done (Phase 1c).** Argument parsing, input checks, trip loading, M1 (schema enforcement plus the per-year cleaning stats written to `<output>/m1/`) and M2 (temporal demand per borough, day and hour, written to `<output>/m2/`) run end to end. M3 doesn't exist yet. The plan for the next pieces is in `roadmap/` (`00-ROADMAP.md` §1d, `01-SKELETON-AND-PROFILER.md`, `02-M1-CLEANING.md`).

Data flow, in call order (all under `src/main/kotlin/com/radeshgovind/`):

```
Main.kt  parseArgs ─► findTripFiles ─► createSparkSession().use { runPipeline(...) }
            │              │
            └─ Invalid ────┴─ Missing  ─► stderr + exit 2 (before Spark starts)

runPipeline:  loadTrips(files, fraction) ─► enforceSchema ─┬─► cleaningStats ─► <output>/m1/                          (M1)
                                                           └─► removeInvalidTrips ─┬─► temporalDemand(loadZones) ─► <output>/m2/  (M2)
                                                                                   └─► [M3 / O*]                  (not wired yet)
```

- `Main.kt`: `com.radeshgovind.MainKt`, the application and jar entry point. It owns the exit codes: 0 ok, 2 bad arguments or missing files, 1 a failure inside Spark.
- `cli/`: `ParseArgs.kt` (flags → `PipelineArgs`, or `ArgsParse.Invalid` with a one-line reason), `MonthRange.kt` (`--years`: `YYYY`, `YYYY-YYYY`, `YYYY-MM`, 2011 onwards), `Fraction.kt` (0 < f ≤ 1), `Operation.kt` (`M1`–`M3`)
- `ingest/`:
  - `TripFiles.kt`: the exact Parquet path for each month, with no globs, as `Found` or `Missing`
  - `LoadTrips.kt`: reads those files, adds `source_year`/`source_month` from each file's name (`_metadata.file_path`, right on the scan), then samples with a fixed seed (42) when `--fraction` < 1
  - `EnforceSchema.kt`: `enforceSchema()` selects and casts the 13 columns of `ENFORCED_SCHEMA` once (timestamps stay `TIMESTAMP_NTZ`) and drops everything else
  - `TripColumns.kt`: trip column names as constants
  - `LoadZones.kt` and `ZoneColumns.kt`: the zone table, read with an explicit schema
- `clean/` (M1):
  - `InvalidTrip.kt`: an enum of the five criteria, each a never-null `condition()` plus its stat column name. The same list drives the filter and the stats.
  - `RemoveInvalidTrips.kt`: `removeInvalidTrips()`, the clean trips that M2 and M3 will start from
  - `CleaningStats.kt`: `cleaningStats()`, one `groupBy(source_year)` holding every count. Decisions D1–D6 are in `roadmap/02-M1-CLEANING.md` and README → M1.
- `demand/` (M2):
  - `TemporalDemand.kt`: `temporalDemand(zones)`, the M2 story: borough join, trip times, then one `groupBy(pickup_borough, day_of_week, hour_of_day)` with `trip_count`, `avg_fare`, `avg_duration_min` and `avg_distance`
  - `WithPickupBorough.kt`: `withPickupBorough(zones)`, a plain inner join on `PULocationID` = `LocationID` with no hint (the baseline). The zones are a parameter, so a later join hint stays visible in `Pipeline.kt`.
  - `WithTripTimes.kt`: `withTripTimes()` adds `day_of_week` (`dayofweek`: 1 = Sunday … 7 = Saturday), `hour_of_day` and `duration_min`. Decision D7: the duration is NTZ minus NTZ, not `timestamp_diff`, which would apply the session time zone (README → M2).
  - `DemandColumns.kt`: M2's column names as constants
- `SparkSessions.kt`: uses `local[*]` unless `spark-submit` already set `spark.master`, and sets no other `spark.*` config
- `Pipeline.kt`: `runPipeline`, the story of the pipeline and **the only place with Spark actions**
- Planned packages: `.zones` (M3), `.profiling` (the Lab 3 profiler port, roadmap 01 Part B)
- Build: `build.gradle.kts` (Spark 4.2.1 as a `spark` configuration: compile + run + test, kept out of the fat jar; Java 21; Spark's JVM module flags), `gradle.properties` (`sparkVersion`)
- Data: `scripts/download-data.sh` → `data/trips/yellow_tripdata_YYYY-MM.parquet`, `data/taxi_zone_lookup.csv`, `data/MANIFEST.tsv`
- Cluster: `docker/docker-compose.yml` (standalone Spark 4.2.1 / Java 21; `data/` → `/data` read-only, `build/libs/` → `/jars`, `output/` → `/output`)

## Code Conventions

- **Style:** Kotlin official style, enforced by Spotless + ktlint (`intellij_idea`, 120 cols). Run `make format`; never hand-format.
- **Structure:** follow the `kotlin-stepdown` skill (`.claude/skills/kotlin-stepdown/`). Entry point first, helpers below in call order, one level of abstraction per function, one concept per file, packages by feature (`com.radeshgovind.ingest`, `.clean`, `.demand`, `.zones`, `.cli`).
- **Spark** (details in `references/spark.md` of that skill):
  - Steps are extension functions on `Dataset<Row>` that return `Dataset<Row>` and **never run actions**. Hidden `count()`/`show()` calls add Spark jobs and distort profiling.
  - Enforce the schema once at load; keep column names as constants; prefer built-in `functions.*` over UDFs.
  - Keep performance decisions (`persist`/`unpersist`, `broadcast()`, repartitioning) visible in the top-level pipeline function.
- **Kotlin ↔ Scala API:** use `Column` methods (`.gt()`, `.and()`, `.equalTo()`), since Scala operators aren't available. Avoid `Dataset.transform` (it needs a Scala `Function1`). Convert collections explicitly when an API wants a Scala `Seq`.
- **Inputs:** data paths and dataset scale come from CLI arguments, never hard-coded. The input schema drifts across years (2009–2010 have no zone IDs; types and column names change later): see README → Dataset. Any non-default `spark.*` setting is documented in README → Configuration.

## Testing and Verification

- `make check` = lint + build + test. CI and the `pre-push` hook run the same thing.
- `SparkSessionTest` runs a local Spark job with a shuffle. It guards the build setup (Spark on the classpath, Java 21, module flags).
- `cli/ParseArgsTest`, `cli/MonthRangeTest` and `ingest/TripFilesTest` cover argument parsing and file resolution without Spark, so they run in milliseconds.
- `ingest/EnforceSchemaTest`, `clean/CleaningTest` and `demand/TemporalDemandTest` run on a local Spark session from `localTestSession()` (`src/test/kotlin/com/radeshgovind/LocalSpark.kt`). The cleaning fixture has one row per failure case, a null-passenger row and an overlap row. The demand fixture has six trips across two boroughs with hand-checked counts and means, plus two time-zone tests: a trip in Lisbon's DST overlap hour pins D7, and an Asia/Tokyo session shows that `hour`/`dayofweek` ignore the session zone.
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
- `docs/report/template.tex`: report template (acmart). Build with `latexmk -pdf -cd docs/report/template.tex`
- `experiments/LOG.md`: environments, runs, observations (unmeasured leads from development runs), hypotheses, optimisations
- `docs/lab-inputs.md`: what is still needed from the labs and the course (Docker Spark, Lab 3 profiler, DI cluster, submission)
- `.claude/skills/kotlin-stepdown/`: code style, plus `references/spark.md` for Spark specifics
