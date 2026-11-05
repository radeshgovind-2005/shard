# Experiment Log

Single source of truth for every measurement in the report. Agents append here; nothing goes into the report without a run-id from this file.

## Environment

| Field | local | Docker Spark | DI cluster |
|---|---|---|---|
| Spark version | 4.2.1 (Scala 2.13), Temurin JDK 21, `local[*]` | 4.2.1 (Scala 2.13), Java 21, standalone (`spark://spark-master:7077`, client mode); image `spark:4.2.1-scala2.13-java21-ubuntu@sha256:e949a6029ea5d626153c64d5eebdf855ef4d07e3f3a5653938dd202fda84f9aa`; `docker/docker-compose.yml` | |
| Hardware / topology (workers × cores × memory) | 1 × Apple M5, 10 cores, 24 GB RAM (macOS) | N workers (`--scale spark-worker=N`; default 2), each offering 2 cores / 2 GB, but Spark's default `spark.executor.memory` = 1g, so 1 executor per worker with **2 cores, 1024 MiB**; driver in the master container (434.4 MiB storage memory); OrbStack on the same M5. Observed 2026-11-05 in the spark-submit log | |
| Non-default `spark.*` conf | none | none | |
| Dataset (years, files, size on disk) | 2025-01 – 2025-12: 12 trip files, 829,973,299 B (791.5 MiB) + `taxi_zone_lookup.csv` (12,331 B); downloaded 2026-11-04 21:17–21:19 UTC; sha256 per file in `data/MANIFEST.tsv` | same files as local, bind-mounted read-only at `/data` (master and workers) | |

## Runs

Run-id format: `<env>-<yyyymmdd>-<nn>` (e.g. `local-20261105-01`). Raw dumps in `experiments/runs/<run-id>/`.

| run-id | git sha | env | scale | variable under test | wall-clock (s) | shuffle R / W | #tasks | spill (disk) | GC % | notes |
|---|---|---|---|---|---|---|---|---|---|---|

## Observations (not measured)

What development runs showed before the profiler existed. **None of this is citable as a measurement:** these runs have no run-id, ran on uncommitted code, and their timings come from Spark INFO logs, not the profiler. They are correctness evidence, design facts and leads. Before any timing goes into the report or becomes a hypothesis, a profiled run with a run-id has to confirm it.

### M1: ingestion and cleaning (2026-11-05, uncommitted M1 working tree on `f706c75`)

**Correctness: verified.** Every number matches spec `roadmap/02-M1-CLEANING.md` §7, which was computed independently in PySpark from the same rules:

| Where | `--years` | raw | bad_fare | bad_distance | bad_passenger_count | bad_time_order | bad_zone | removed | kept | null_passenger_count |
|---|---|---|---|---|---|---|---|---|---|---|
| local `local[*]` | 2025-01 | 3,475,226 | 145,516 | 90,893 | 24,674 | 2,051 | 28,357 | 271,401 | 3,203,825 | 540,149 |
| Docker, 2 workers | 2025-01 | identical to local | | | | | | | | |
| local `local[*]` | 2025 | 48,722,602 | 2,870,234 | 1,402,958 | 260,208 | 546,304 | 371,657 | 5,122,770 | 43,599,832 | 11,611,894 |

The criteria overlap (D5): in January they sum to 291,491, against 271,401 removed trips. `make check` covers this with the 12-row fixture in `clean/CleaningTest`.

**Design facts, from the pre-execution plan** (`explain("formatted")`, 2025-01, local). Report §2 material:
- **Column pruning:** the scan's `ReadSchema` holds only 7 columns: `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `passenger_count`, `trip_distance`, `PULocationID`, `DOLocationID` and `fare_amount`, plus `_metadata.file_path`. Catalyst prunes down to what the stats query reads. The spec columns `RatecodeID`, `payment_type`, `tip_amount` and `total_amount` aren't read, and nor are the non-spec columns (`VendorID`, surcharges and so on). The spec expected 11. (The file's total column count wasn't checked.)
- **One pass:** `HashAggregate (partial) → Exchange hashpartitioning(source_year, 200) → HashAggregate (final)`. All 9 counts come from one scan (D6), not five `count()` jobs.
- **No pushdown:** `PushedFilters` is empty. The `coalesce`/`OR` criteria can't be pushed into Parquet, so every row group is read (spec §8).
- **The null trap (D1/D2):** 23.8 % of 2025 (11.6 M trips) has a null `passenger_count`. Without `coalesce(…, false)`, `filter(not(criterion))` would drop those trips without counting them under any criterion.
- **Time zones (D3):** timestamps stay `TIMESTAMP_NTZ`, and local and Docker give identical results, even though the local session runs on Europe/Lisbon and Docker on UTC.

**Leads for the profiler.** Each one needs a profiled run before it can become a hypothesis:
- **L1. File count sets the scan's parallelism.** Each monthly file (59–78 MB) is smaller than the split size, so the scan has one task per file. On 2025 that's 12 tasks for 10 local cores: 10 tasks ran in about 4.5–5.9 s, then the last 2 ran as a second wave (about 1.8–2.2 s). The scan stage took 6.7 s. On Docker, 2025-01 split into 4 tasks for 4 cores. Candidates: `spark.sql.files.maxPartitionBytes`, and task count against cores (2–3 tasks per core).
- **L2. Default shuffle partitions for a single key.** The plan has 200 shuffle partitions for one `source_year`, and AQE coalesced them to 1 at runtime. The shuffle itself is tiny (424 B for January, 1,272 B for 2025), so this is probably not worth optimising for M1. Recheck once M2 and M3 shuffle real volume.
- **L3. Repeated criteria.** `removed_trips` repeats all five criterion expressions inside the aggregate, so each might be evaluated twice per row. It only matters if the scan stage turns out to be CPU-bound (`executorCpuTime` ≈ `executorRunTime`).
- **L4. Recomputation, expected.** Nothing is cached. Once M2 and M3 read `removeInvalidTrips()`, each output will re-scan and re-clean the input (spec §8, and H1 in roadmap 01 Part C).

**Methodology notes** (for report §3 setup and §6 limitations):
- **The Docker master can lose its workers.** At 10:57 the master dropped both workers after 60 s without a heartbeat, probably while the Mac slept. They never re-registered, and the next submit waited forever with 0 cores. Measured runs need the Mac kept awake (`caffeinate`) and a fresh `down`/`up` before each set. README → Docker covers this.
- **Scope:** only 2025 is supported for now. Name-case drift and `int64` columns in other years aren't handled yet, which limits any scale-by-years experiment.

### M2: temporal demand (2026-11-05, uncommitted M2 working tree on `93a62bf`)

**Provenance.** No run-ids: development runs, uncommitted code, no profiler. Raw evidence for everything below is in [`observations/m2-20261105/`](observations/m2-20261105/README.md): the formatted plan, the console tables, the INFO stage lines, the local-vs-Docker diff and the `javap` output, each with the command that produced it. Who ran what: the user ran `make check` and the full-2025 local run, and pasted the output. Claude ran the 2025-01 local run with the temporary plan line, the Docker 2025-01 run, the diff and `javap`.

**What M2 computes** (report §2): cleaned trips ⋈ zone lookup on `PULocationID` = `LocationID` (plain inner join, no hint) → `day_of_week` (`dayofweek`: 1 = Sunday … 7 = Saturday), `hour_of_day` (both from the pickup, NYC wall-clock time, D3) and `duration_min` (D7) → one `groupBy(pickup_borough, day_of_week, hour_of_day)` with `trip_count`, `avg_fare`, `avg_duration_min` and `avg_distance` → Parquet with `partitionBy(pickup_borough)` in `<output>/m2/`. A row exists only for a (borough, day, hour) with at least one trip.

**Correctness: verified.** M2 has no precomputed numbers. The invariant: Σ `trip_count` over all output rows = M1 `kept_trips`. That holds because the inner join can't drop a trip: `taxi_zone_lookup.csv` has 265 rows with `LocationID` 1–265, each exactly once, and M1 keeps only zones 1–263.

| Where | `--years` | Σ `trip_count` | M1 `kept_trips` | borough values (trips) | rows | evidence |
|---|---|---|---|---|---|---|
| local `local[*]` | 2025-01 | **3,203,825** ✅ | 3,203,825 | 6: Bronx 12,735 · Brooklyn 55,321 · EWR 61 · Manhattan 2,874,748 · Queens 260,761 · Staten Island 199 | 815 (168 each for Bronx, Brooklyn, Manhattan and Queens; Staten Island 100, EWR 43) | `console-local-2025-01.txt` (2 runs, same tables) |
| Docker, 2 workers | 2025-01 | **3,203,825** ✅ | 3,203,825 (M1 table identical to local) | identical to local, borough by borough | 815, identical per borough | `console-docker-2025-01.txt` |
| local `local[*]` | 2025 | **43,599,832** ✅ | 43,599,832 (M1, verified earlier) | 6: Bronx 338,947 · Brooklyn 1,469,590 · EWR 1,182 · Manhattan 37,746,355 · Queens 4,040,112 · Staten Island 3,646 | 991 (168 for every borough except EWR, which has 151) | `console-local-2025.txt` (M1 table of this run not captured) |

- **Boroughs:** exactly the 6 expected ones. `Unknown` (264) and `N/A` (265) are removed by M1 and never appear. On disk, Staten Island's folder is `pickup_borough=Staten Island` (a literal space, which shows as `%20` in the scan's URI) and reads back as `Staten Island`.
- **Local vs Docker, cell by cell** (2025-01, `local-vs-docker-diff-2025-01.txt`). All 815 (borough, day, hour) keys and their `trip_count` are identical. An exact `EXCEPT ALL` still finds **294 differing rows in each direction**: only the three averages differ, by at most **5.1 × 10⁻¹⁶ relative** (|Δ| ≤ 1.4 × 10⁻¹⁴ for fare and duration, ≤ 2.3 × 10⁻¹³ for distance). That's 1–2 units in the last place of a double: floating-point addition isn't associative, and the partial sums are added in a different order (10 scan tasks locally vs 4 on Docker). See the methodology note on result checks below.
- **Unit tests** (`demand/TemporalDemandTest`, 5 tests, `make check` green): exact schema; hand-checked counts and means for 6 trips in 4 cells across 2 boroughs (2 zones folding into one borough, hour taken from the pickup, a Sunday 23:50 → Monday 00:20 trip counted on Sunday, a 30-second trip = 0.5 min); Σ `trip_count` = input trips; duration in a Europe/Lisbon session across the 2025-10-26 overlap hour = 20 min (`timestamp_diff` gives 80); day and hour unchanged in an Asia/Tokyo session.

**Design facts, from the 4.2.1 jars** (`javap-d7.txt`):
- **D7, `timestamp_diff` is not zone-free.** `TimestampDiff` is `ImplicitCastInputTypes` + `TimeZoneAwareExpression` with `inputTypes = (TimestampType, TimestampType)`, so `TIMESTAMP_NTZ` inputs are first cast to `TIMESTAMP` in the session time zone. In Lisbon's 2025-10-26 overlap hour, a 20-minute trip would come out at 80 minutes locally (Europe/Lisbon) and 20 on Docker (UTC). That breaks D3, and the 2025-01 Docker check alone wouldn't catch it, because January has no DST switch. `SubtractTimestamps` (NTZ − NTZ) picks its zone with `zoneIdForType`, which is UTC for NTZ, so M2 uses it: `duration_min = (dropoff − pickup)` cast to `interval second`, then to `decimal(18,6)`, divided by 60.0. The brief and roadmap §1c had specified `timestamp_diff`; the change is recorded there.
- `hour()` (`GetTimeField`, `AnyTimestampType`, `zoneIdForType`) and `dayofweek()` (`GetDateField`; NTZ → `DATE` is a truncation) are zone-free on NTZ.
- **Caveat (limitations):** the data stores no UTC offset, so a trip spanning NYC's own DST switch (2025-03-09 02:00, 2025-11-02 02:00) is measured in wall-clock time and is off by ±60 min. That's the same in every environment.

**Design facts, from the pre-execution plan** (`plan-formatted-local-2025-01.txt`: `explain("formatted")` on `temporalDemand`, 2025-01, local, `isFinalPlan=false`). Report §2 material:
- **The plain join is a broadcast join anyway.** `BroadcastHashJoin Inner BuildRight`, with the zones in a `BroadcastExchange`. There's no `SortMergeJoin` and no hint in the code. The zone CSV (12,331 B) is under the default `spark.sql.autoBroadcastJoinThreshold` (10 MB), so Catalyst broadcasts it by itself. **"Broadcast the zone lookup" is therefore not an available optimisation for M2**: the baseline already does it. Roadmap Phase 04 lists it as a likely candidate, so drop it, or measure it as "auto vs forced shuffle join" only if that's worth a report subsection.
- **One shuffle.** The only `Exchange` is `hashpartitioning(pickup_borough, day_of_week, hour_of_day, 200)` between the partial and final `HashAggregate`. The partial aggregate runs before the shuffle, so each task shuffles at most one partial row per (borough, day, hour), at most 6 × 168 = 1,008, not one row per trip.
- **Pruning.** The M2 scan's `ReadSchema` holds the same 7 trip columns as M1's: the 4 M2 needs plus `passenger_count` and `DOLocationID`, which only the cleaning filter reads. The CSV scan reads only `LocationID` and `Borough`. The plan still builds the `_metadata` struct (`file_path` and so on, plus `_tmp_metadata_row_index` in `ReadSchema`) and then drops it, because `source_year` isn't used by M2.
- **Constraint propagation.** Catalyst copied the cleaning's `PULocationID` 1–263 condition onto the zone side (`Filter (6)` on the CSV scan) and added `IsNotNull(PULocationID)` as a Parquet `PushedFilters` entry. M1's plan had no pushed filters.
- **D7 in the plan.** `duration_min` = `cast(cast(cast((dropoff - pickup) as interval second) as decimal(18,6)) as double) / 60.0`, so no time zone is involved. `hour(tpep_pickup_datetime, Some(Europe/Lisbon))` carries the session zone (which also confirms the local session runs on Europe/Lisbon), but it's ignored for `TIMESTAMP_NTZ`. The Asia/Tokyo test pins that.

**Runtime facts, from the Spark INFO logs** (`spark-info-stages-2025-01.txt`; task counts and sizes only, not timings):
- The M2 scan stage ran 10 tasks locally for 2025-01, 12 for 2025 (one per file, the L1 pattern), and 4 on Docker for 2025-01 (2 executors × 2 cores, 1024 MiB each).
- The final M2 aggregate fetched 214.9 KiB of shuffle for 2025-01 and 750.5 KiB for 2025 (local). AQE coalesced the 200 shuffle partitions so that a single task ran the final aggregate and the partitioned write.
- Output on disk for 2025-01: 6 borough folders with one Parquet file each, 2.8–7.0 KB per file (68 KB for the whole folder).
- Each M2 run has 2 Spark actions: the partitioned write (the one to profile) and the read-back `rollup` that prints the per-borough totals. The read-back only reads the ~1,000 written rows; it must stay outside the profiled block, as for M1's printed table.

**Leads for the profiler.** Each one needs a profiled run before it can become a hypothesis:
- **L4, as expected.** Nothing is cached, so M2's job scans and cleans the trip Parquet again after M1's job has. The profiler should show the input bytes twice.
- **L5. Borough skew.** Manhattan has 89.7 % of January's kept trips (2,874,748 of 3,203,825) and 86.6 % of 2025's (37,746,355 of 43,599,832). EWR has 61 and 1,182. The partial aggregate absorbs this before the shuffle (above), so the skew probably doesn't hurt M2. It matters for any borough-keyed operation that shuffles raw trips (M3's `partitionBy(borough, month)` windows, O1).
- **L6. Write layout.** After AQE coalescing, one task wrote all 6 borough folders, one file each. That's fine for ~1,000 rows. Note it if the report discusses `partitionBy` output layout.
- **L7. The `_metadata` columns are built and dropped.** It's cheap per row, but it's work M2 doesn't need. Probably not worth a hypothesis.

**Methodology notes** (for report §3 setup and §6 limitations):
- **Result checks need a tolerance on averages.** "Results identical" for M2 (and any later operation with `avg`/`stddev`) has to mean: identical keys and counts, plus averages equal within a relative tolerance (say 10⁻¹²). An exact `EXCEPT ALL` diff fails as soon as the partitioning changes, as the local-vs-Docker diff shows. That applies to every OPT that changes partition or task counts. `spark-optimizer` and `benchmark-runner` should use this comparison.
- **The plan is pre-execution** (`isFinalPlan=false`). The final plan (AQE) has to come from the Spark UI's SQL tab or an `explain` after the action. The join choice is already fixed before execution here, because it rests on file-size statistics.
- **Setup of these runs.** The worktree has no `data/`, so every run read the main checkout's `data/` (the files in `data/MANIFEST.tsv`). For Docker, the M2 jar (`make jar` in the worktree) was copied over the main checkout's `build/libs/shard-all.jar`, and Docker wrote to the main checkout's `output/`. The plan line was added only for the one local 2025-01 run and removed before the jar was built, and `make check` passed after removing it.
- **Development record** (AI-tools acknowledgement): Claude wrote the M2 code and tests. `javap` evidence changed the specified duration formula (D7). `make check` caught one bug before any data run: a bare `col()` inside a `Dataset` extension resolved to the receiver's `Dataset.col` (the known gotcha), fixed by moving it into a top-level helper.

## Hypotheses

Written by `spark-profiler`. Each needs evidence: metric + run-id.

<!--
### H1 [op] <bottleneck>
- Evidence: <metric = value> (run <id>, stage <n>); plan: <node>
- Hypothesis: <cause>
- Candidate remedy: <one change>
- Expected: <metric, direction>; upper bound ≈ <x%> of wall-clock
-->

## Optimisations

Written by `spark-optimizer` (applied) and `benchmark-runner` (measured).

<!--
### OPT-1 (H1) [op] <change>
- Status: applied, unmeasured | kept | reverted | inconclusive
- Change: <files>, toggle: <flag/conf>
- Plan: <before node> → <after node>
- Correctness: <row counts, except-diff>
- Before: <run-ids> → <median [min–max]>
- After:  <run-ids> → <median [min–max]>
- Δ wall-clock: <%>; Δ predicted metric: <value>
-->
