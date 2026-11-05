# 00 — Roadmap: orientation, setup and starting the work

> Written 2026-11-04. Deadline: **2026-11-06 23:59**. Follow-up roadmaps (`01-…`, `02-…`) cover the later phases.

## 1. The point of the project

You're building a Spark pipeline over NYC Yellow Taxi trips, but **the analytics aren't what gets graded. The performance engineering is.** The report has to show this loop at least two or three times:

```
profile → hypothesise → optimise → measure → repeat
```

Each optimisation needs evidence of a bottleneck before you change anything (a stage time, shuffle bytes, or a skewed task) and a before/after measurement afterwards. Spec: [`docs/Project_Description.md`](../docs/Project_Description.md).

**Deliverables**
- [ ] The pipeline: M1, M2, M3 and at least 2 of O1–O4, taking CLI args for input path, dataset scale and output path
- [ ] The 4-page report (+1 page for bibliography, acknowledgements and contributions): [`docs/report/template.tex`](../docs/report/template.tex)
- [ ] A README with exact reproduction steps: Spark version, topology, `spark-submit` command, non-default configs
- [ ] Cluster results, **at least on Docker Spark** (mandatory). DI cluster results are only required if it's available.

**What "done" looks like:** every number in the report comes from a run-id in [`experiments/LOG.md`](../experiments/LOG.md), and each optimisation subsection reads *bottleneck → hypothesis → change → before/after*.

## 2. Where things stand

| Area | State |
|---|---|
| Build (Gradle, Spark 4.2.1, Kotlin, JDK 21, fat jar) | ✅ done: [`build.gradle.kts`](../build.gradle.kts), [`gradle.properties`](../gradle.properties) |
| Repo tooling (Makefile, hooks, commitlint, CI) | ✅ done: [`Makefile`](../Makefile), [`AGENTS.md`](../AGENTS.md) |
| Dataset | ✅ done: all of 2025 in `data/` (12 files, 791.5 MiB) |
| Experiment log structure | ✅ done, but empty: [`experiments/LOG.md`](../experiments/LOG.md) |
| Perf-loop agents and Git slash commands | ✅ done: `.claude/agents/`, `.claude/commands/` |
| Report template | ✅ there, nothing written yet |
| **Pipeline code** | 🟡 skeleton (1a), M1 (1b), M2 (1c) and M3 (1d) done; O1–O4 next: [`src/main/kotlin/com/radeshgovind/`](../src/main/kotlin/com/radeshgovind/) |
| **Profiler** | ❌ the Lab 3 one is Python. Needs a Kotlin port (Phase 2) |
| Docker Spark in this repo | ✅ done: [`docker/docker-compose.yml`](../docker/docker-compose.yml), Spark 4.2.1 / Java 21, SparkPi ran on 2 workers |
| DI cluster | ❓ no information anywhere in the course material |
| Choice of O1–O4 | ⏸ deliberately deferred until M1–M3 run |

## 3. Key concepts

### What a profiler is
Spark is lazy and splits work into jobs, stages and tasks across executors, so timing a whole run doesn't tell you *why* it's slow. A profiler records what Spark actually did for each piece of work:
- the number of stages and tasks
- shuffle read/write bytes (data moved over the network)
- memory and disk spill
- executor run time against CPU time
- per-task durations, which show skew

Those numbers are your "before" evidence and your "after" proof.

### The Lab 3 profiler
[`../../../labs/compar-lab03-spark-profiling/notebooks/spark_profiler.py`](../../../labs/compar-lab03-spark-profiling/notebooks/spark_profiler.py) is about 150 lines of Python. `with profiler.profile("name"):` does three things:
1. tags every job inside the block with `setJobGroup`
2. finds those jobs' stages through `statusTracker()`
3. sums the stage metrics from the Spark UI REST API (`/api/v1/applications/<app>/stages`)

`report()` and `compare(before, after)` then print the table and the % change.

### Doing it in Kotlin
The spec says you may modify the Lab 3 profiler as you like. Port it to `com.radeshgovind.profiling` and keep the same API shape and metric names, so the report can say it's the lab's method. One improvement is worth making: collect the metrics with a **`SparkListener`** (`onJobStart` and `onStageCompleted`) instead of HTTP calls. On the JVM it runs inside the driver, so it works on any cluster without access to port 4040, and it can also record per-task max and median durations for skew.

### The DI cluster
The spec makes it conditional: the scalability section is "subject to DI cluster availability", and Docker Spark results are the mandatory minimum. Plan everything on Docker. If DI access arrives in time, the same jar runs there with a different `--master`. What you'd need is listed in [`docs/lab-inputs.md`](../docs/lab-inputs.md): login and VPN, the resource manager and master URL, the Spark and Java versions, node sizes and your allocation, where the data lives, and quotas.

## 4. Phase 0: setup (today, about 1–2 h)

### Local
- [x] `make setup` (Git hooks)
- [x] `make data-sample` (zone lookup + 2025-01, for smoke tests)
- [x] `make check` (lint, build and the Spark smoke test should all pass)
- [x] `make data` (all of 2025, ~0.8 GiB). This is the development year. Check it fits first with `make data-size`.
- [x] Fill in the "Dataset" row of the Environment table in `experiments/LOG.md`

### Docker Spark on OrbStack
The lab environment is [`../../../labs/compar-spark-docker/docker-spark-env/`](../../../labs/compar-spark-docker/docker-spark-env/): one master (UI on `8080`, master on `7077`), workers at `--cores 2 --memory 2G`, image `spark:python3`.

- [x] Start OrbStack and pull the image. Run `java -version` and check the Spark version inside it, e.g. `docker run --rm --entrypoint sh spark:python3 -c 'java -version; ls /opt/spark/jars | grep spark-core'`.
- [x] **Java check.** *(Done: the image has Java 21, so no change needed.)* The official images usually ship **Java 17**, but the jar targets 21 (`jvmToolchain(21)` in `build.gradle.kts`). If the image has 17, change it to `jvmToolchain(17)` and update the comment and README. A jar built for 21 fails on a Java 17 cluster with `UnsupportedClassVersionError`.
- [x] **Spark check.** *(Done: there's no 4.3.0 image, so `sparkVersion=4.2.1` and the image is pinned to `spark:4.2.1-scala2.13-java21-ubuntu`.)* `sparkVersion` in `gradle.properties` must match the image (Scala 2.13). Pin the image to that exact tag instead of the floating `python3`, for reproducibility.
- [x] Create `docker/docker-compose.yml` in this repo, based on the lab file:
  - mount `../data` to `/data` (read-only) in the master and the workers, because workers read the Parquet files
  - mount `../build/libs` to `/jars` in the master
  - mount an output dir, e.g. `../output` to `/output`, in the master and the workers
  - map `4040-4050` on the master, for the application UI while a job runs
  - (optional) enable event logs and the history server, so DAG screenshots stay available after a run
  - drop the Jupyter service; you don't need it
- [x] Smoke submit once the pipeline exists. *(The cluster itself is verified: SparkPi ran on both workers and they can read `/data/trips`. The pipeline ran on Docker for `--years 2025-01` with M1 and M2, and the results match local: see `experiments/LOG.md` → Observations.)*
  ```bash
  docker compose -f docker/docker-compose.yml up -d --scale spark-worker=2
  ```
  ```bash
  docker compose -f docker/docker-compose.yml exec spark-master /opt/spark/bin/spark-submit --class com.radeshgovind.MainKt --master spark://spark-master:7077 /jars/shard-all.jar --input /data --years 2025-01 --output /output
  ```
- [x] Fill in the Docker column of the Environment table in `experiments/LOG.md`, and tick the Docker items in `docs/lab-inputs.md`

### Admin
- [ ] Ask the instructor or classmates whether the DI cluster is available for Project 1, and how to get access
- [ ] Find out the group members and the submission method (needed for the report's contributions page)

## 5. Phase 1: start building (tonight and tomorrow morning)

Workflow for each piece: `/new-branch feat <name>` → code → `make check` → `/commit` → `/pr` → `/merge`. Style rules: the [`kotlin-stepdown` skill](../.claude/skills/kotlin-stepdown/SKILL.md) and its [`references/spark.md`](../.claude/skills/kotlin-stepdown/references/spark.md).

> **Write the straightforward version first. Don't pre-optimise.** No `broadcast()`, `cache()` or repartitioning yet. The baseline should be the natural implementation, so the profiler has real bottlenecks to find. Optimising it up front leaves the report with nothing to show and goes against "no speculative optimisation".

### 1a. Skeleton and CLI (`feat/pipeline-skeleton`)
- [x] `com.radeshgovind.cli`: `PipelineArgs` with `--input` (dir containing `trips/` and `taxi_zone_lookup.csv`), `--years` (`2025`, `2024-2025`, `2025-01`), `--fraction` (sampling, e.g. `0.1`, for the 10–20% scale run), `--output`, and `--ops` (e.g. `m1,m2,m3`). Parse once at the edge.
- [x] `Main.kt`: build the `SparkSession`. Set `master("local[*]")` **only if `spark.master` isn't already set**, so `make run` works locally and `spark-submit --master` wins on a cluster.
- [x] `runPipeline(spark, args)`: the top-level story, as in `references/spark.md`. Actions (`write`, `collect`) happen only here.
- [x] Packages: `com.radeshgovind.ingest`, `.demand` (M2), `.zones` (M3), `.cli`, with `.profiling` later. One concept per file. Column names go in one `object TripColumns`.
- [x] Never write into `data/`. Outputs go under `--output` (e.g. `output/m1/`, `output/m2/`).

### 1b. M1: ingestion and cleaning (`feat/m1-ingestion`)
> **Full spec: [`02-M1-CLEANING.md`](02-M1-CLEANING.md).** It fixes the decisions below from the measured 2025 data: null passengers are kept, the year comes from the source file, timestamps stay `TIMESTAMP_NTZ`. It also lists the exact expected counts. Where it differs from this list, the spec wins.

- [x] Load `trips/yellow_tripdata_YYYY-MM.parquet` for the selected years, **2011 onwards only**. 2009–2010 have lat/lon and no zone IDs (README → Dataset).
- [x] `enforceSchema()`: select only the 11 spec columns, cast to the spec types (ints vary between `int32` and `int64` across years), and handle column-name case drift. Do it once, at load. Watch for files whose schemas don't merge on read; you may need to read per file or year, cast, then union. *(Done for 2025, whose 12 files share one schema. Name-case drift and per-file reads land only when other years are added.)*
- [x] Add a `year` column. Decide whether it comes from the file name or from the pickup timestamp: files contain stray out-of-range dates. Document the choice.
- [x] Five criteria as named `Column`s, combined with `.and()`:
  1. `fare_amount ≤ 0`
  2. `trip_distance ≤ 0`
  3. `passenger_count = 0` or `> 6`
  4. dropoff ≤ pickup
  5. `PULocationID` or `DOLocationID` outside 1–263
- [x] Decide how **nulls** count (e.g. a null `passenger_count`) and document it.
- [x] Cleaning stats: **per year, the number of records removed per criterion.** Use one `groupBy(year).agg(sum(when(criterion, 1).otherwise(0)) …)` rather than one `count()` per filter. Criteria overlap (one row can fail several), so say in the report that the counts aren't exclusive. Also report the total rows removed.
- [x] Output: the cleaned `Dataset<Row>` (returned, never acted on inside the step), plus a small stats table written to `output/m1/`.
- [x] Test: a hand-made DataFrame with one row per failure case, plus one valid row. Assert the cleaned count and the per-criterion counts.

### 1c. M2: temporal demand (`feat/m2-temporal-demand`)
- [x] Join the cleaned trips with the zone lookup on `PULocationID` to get `pickup_borough`. A plain join is fine for now; broadcasting is a later optimisation.
- [x] Derive `day_of_week` (`dayofweek`, 1 = Sunday … 7 = Saturday), `hour_of_day` (`hour`), and duration in minutes. *(Changed, D7: `timestamp_diff` turned out to cast `TIMESTAMP_NTZ` to `TIMESTAMP` in the session time zone, which is the D3 problem again. The duration is `(dropoff − pickup)` as an interval in seconds, divided by 60.0, which uses no time zone. See README → M2.)*
- [x] `groupBy(pickup_borough, day_of_week, hour_of_day)`: `count`, `avg(fare_amount)`, `avg(duration_min)`, `avg(trip_distance)`.
- [x] Write Parquet with `partitionBy("pickup_borough")` (the spec says "partitioned by borough").
- [x] Test: a handful of trips across two boroughs, then check the counts and means.

### 1d. M3: zone statistics (`feat/m3-zone-statistics`)
> Decisions D8–D14 are in README → M3. Code in `src/main/kotlin/com/radeshgovind/zones/`.

- [x] `groupBy(PULocationID, month)`: trip count, `avg` and `stddev` of `fare_amount` and `trip_distance`. Join the borough from the zone lookup. *(D8: month = `source_year`/`source_month` plus `month_index`. D9: aggregate first, then join. D10: sample σ, null for one trip.)*
- [x] Window 1: `rank()` of zones by monthly trip volume, `partitionBy(borough, month)`, ordered by count descending. *(D11)*
- [x] Window 2: rolling 3-month trip share, the zone's trips over months *m-2..m* divided by the borough's trips over the same months. Order by a **month index** (`year*12 + month`) with `rangeBetween(-2, 0)`, so missing months don't silently become "the previous 3 rows". *(D12: the borough total is one RANGE window over the borough's zone-month rows.)*
- [x] Window 3: flag a zone when its current-month volume deviates from **its own** rolling mean by more than 1.5 σ (`partitionBy(zone)`, rolling `avg` and `stddev`). Pick the window length, document it, and handle a null σ for the first months. *(D13: baseline = the previous 3 months, `rangeBetween(-3, -1)`, current month excluded because an inclusive 3-month window can never exceed 1.5 σ. Null flag without a σ. D14: what it can and can't show on 2025.)*
- [x] Write to `output/m3/`. *(Inside `profiler.profile("m3")`, not partitioned.)*
- [x] Test: one zone with an obvious spike month gets flagged, and the ranks within a borough come out right. *(`zones/ZoneStatisticsTest`, plus a missing-month rolling share and a null σ.)*
- [x] Verified: `make check`; Σ `trip_count` = M1 `kept_trips` on 2025-01 (3,203,825, local and Docker) and on 2025 (43,599,832); post-run plan in LOG → Observations → M3. *(The 2025 run flags 46 % of baselined rows, mostly city-wide seasonality. Decided: keep the count-based flag as the spec words it; the rate goes in the report's limitations.)*

### 1e. First end-to-end run
- [ ] `make run ARGS="--input data --years 2025-01 --output output"`, then `--years 2025`
- [ ] Open the Spark UI at http://localhost:4040 while it runs and look at the Jobs, Stages and SQL tabs. You'll need them for the profiling.
- [x] Update the "Architecture Map" TODO in `AGENTS.md` and the CLI section in the README

## 6. Later phases

Detailed roadmap files so far: [`01-SKELETON-AND-PROFILER.md`](01-SKELETON-AND-PROFILER.md) (skeleton, profiler, baseline protocol) and [`02-M1-CLEANING.md`](02-M1-CLEANING.md). The numbers below are phase order, not file names.

| # | Phase | One line |
|---|---|---|
| 01 | O1–O4 choice and build | Choose 2 after M1–M3 run. O2 (shuffle volume, double zone join) and O3 (self-join for the 4-week lag) or O1 (skewed buckets) give the most to optimise. Justify the choice by the bottleneck each one exposes. |
| 02 | Profiler port | Port `spark_profiler.py` to `com.radeshgovind.profiling` with a `SparkListener`, plus JSON per run in `experiments/runs/<run-id>/`. Wrap each operation's action in `runPipeline`. |
| 03 | Baseline | Full local run (shuffle bytes, task counts) and a Docker run (wall-clock, per-stage times). Use the `spark-profiler` agent to log run-ids and ranked hypotheses in `LOG.md`. Use `explain("extended")` **after** the action (AQE). |
| 04 | Optimisation loop | 2–3 optimisations, each through `spark-optimizer` (one change, results identical) and `benchmark-runner` (warm-up + n ≥ 3, median). Likely candidates: broadcast the zone lookup, column pruning in the scan, cache the cleaned trips that M2/M3/O* reuse, shuffle partitions/AQE, skew handling. |
| 05 | Scalability on Docker | 10–20% sample vs full year, and `--scale spark-worker=2` vs the most workers your 24 GB fits. Use the `scalability-analyst` agent for speedup, efficiency, Amdahl/Gustafson ([`formulas-cheatsheet.md`](../docs/lectures/formulas-cheatsheet.md)). |
| 06 | Report and README | Use the `report-writer` agent working from `LOG.md`: 1 page on the pipeline, the rest on measurements. Acknowledge the AI tools used. README with the exact reproduction steps. |

## 7. Timeline

| When | Target |
|---|---|
| **Wed 11-04 (evening)** | Phase 0 done: data, `make check` green, Docker image versions checked and compose file in `docker/`. Skeleton and M1 started. |
| **Thu 11-05 morning** | M1, M2 and M3 merged and running end to end on 2025 locally. Profiler port. |
| **Thu 11-05 afternoon** | 2 optional operations. Baseline (local + Docker) logged. Hypotheses ranked. |
| **Thu 11-05 evening** | First 1–2 optimisations measured. |
| **Fri 11-06 morning** | Last optimisation, scalability runs on Docker, charts in `experiments/analysis/`. |
| **Fri 11-06 afternoon** | Report and README finished. Final `make check`. Submit before 23:59. |

If you fall behind, keep the report's evidence chain intact before adding more optimisations. Two well-measured optimisations beat four unmeasured ones.

## 8. Gotchas checklist

Details are in the linked files, so they aren't repeated here.

- [ ] Schema drift across years, and 2009–2010 lacking zone IDs: [README → Dataset](../README.md#dataset)
- [ ] No hidden actions (`count`, `show`) inside steps: [AGENTS.md → Code Conventions](../AGENTS.md)
- [ ] Kotlin ↔ Scala: `.gt()`/`.and()`, no `Dataset.transform`, explicit `Seq` conversion: [`references/spark.md`](../.claude/skills/kotlin-stepdown/references/spark.md)
- [ ] AQE: read the **final** plan after the action, not the `explain()` before it: [`.claude/CLAUDE.md` → Gotchas](../.claude/CLAUDE.md)
- [ ] Java/Spark versions must match the cluster (see Phase 0): [`.claude/CLAUDE.md` → Gotchas](../.claude/CLAUDE.md)
- [ ] Before/after runs must read identical input. Don't re-download mid-experiment; checksums are in `data/MANIFEST.tsv`.
- [ ] Every non-default `spark.*` setting goes in [README → Configuration](../README.md#configuration) with its OPT id
- [ ] The PR title becomes the commit on `main`, so it has to pass commitlint
