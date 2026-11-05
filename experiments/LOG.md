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

### M3: zone-level statistics (2026-11-05, uncommitted M3 working tree on `72f9bd0`)

**Provenance.** No run-ids in LOG → Runs: development runs on uncommitted code, all run by Claude. The post-run plan and stage table come from a throwaway profiled run (`--run-id local-20261105-90 --runs-dir <scratchpad>`), used only because the profiler writes the final (post-AQE) plan without a code change. Its times are **not** a baseline measurement. Raw evidence, with the command for each file, is in [`observations/m3-20261105/`](observations/m3-20261105/README.md).

**What M3 computes** (report §2; decisions D8–D14 in README → M3): cleaned trips → `groupBy(PULocationID, source_year, source_month)` with `trip_count`, `avg`/`stddev_samp` of fare and distance, and `month_index = year*12 + month` → borough join (`withPickupBorough`, the same plain join as M2) on the zone-month rows → window 1, `rank()` over (`pickup_borough`, `month_index`) → window 2, zone and borough sums over `rangeBetween(-2, 0)` and their ratio → window 3, the zone's mean and σ over its previous 3 months (`rangeBetween(-3, -1)`) and `is_volume_anomaly` → Parquet in `<output>/m3/`, not partitioned.

**Correctness: verified.** Σ `trip_count` over all zone-months = M1 `kept_trips`, and every borough's total equals M2's for the same input.

| Where | `--years` | Σ `trip_count` | M1 `kept_trips` | zone-month rows | anomalies / no_baseline | evidence |
|---|---|---|---|---|---|---|
| local `local[*]` | 2025-01 | **3,203,825** ✅ | 3,203,825 | 257 | 0 / 257 | `console-local-2025-01.txt` |
| Docker, 2 workers | 2025-01 | **3,203,825** ✅ | 3,203,825 | 257 | 0 / 257; identical to local, borough by borough | `console-docker-2025-01.txt` |
| local `local[*]` | 2025 | **43,599,832** ✅ | 43,599,832 | 3,083 | 1,166 / 528 | `console-local-2025.txt` |

- Borough totals match M2 exactly (for example, January Manhattan is 2,874,748, and its `borough_trips_3m` in January is that same number, because the frame holds one month). January has no anomalies and only no_baseline rows, as D13 requires.
- **Unit tests** (`zones/ZoneStatisticsTest`, 6 tests, `make check` green): exact schema; mean and sample σ on a 3-trip zone-month, and a null σ on a single trip (D10); Manhattan ranks 10, 3, 3 → 1, 2, 2 with Queens ranked separately (D11); a zone with no February gets an April rolling sum over February–April only (D12); a 10, 12, 11, 40 zone is flagged in April, not in March, and null in January and February (D13); Σ `trip_count` = input trips.

**Finding: the anomaly flag mostly tracks city-wide seasonality** (2025, `anomaly-breakdown-2025.txt`). 1,166 of the 2,555 rows that have a baseline are flagged (45.6 %). Per month:

| month | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12 |
|---|---|---|---|---|---|---|---|---|---|---|
| flagged | 240 | 18 | 161 | 90 | 62 | 159 | 83 | 103 | 68 | 182 |
| of which downward | 2 | 1 | 1 | 9 | 39 | 147 | 66 | 65 | 36 | 4 |

- March is flagged almost everywhere (240 of 253 baselined rows). Its baseline is January and February alone, 2 points, one of them a 28-day month. August is a citywide dip (147 of 159 flags are downward). The flag fires at every zone size: 225 of 522 baselined rows with a baseline mean ≥ 10k trips, 142 of 337 below 100.
- That is what D14 anticipated, and it's partly built into the method. Under pure noise, a new point compared with the mean and sample σ of n earlier points exceeds 1.5 s with probability P(|t₍ₙ₋₁₎| > 1.5/√(1+1/n)): about 0.44 for n = 2 and 0.32 for n = 3. On top of that, a whole-city swing moves every zone together. σ = 0 occurred in 10 rows (9 flagged).
- **Decided 2026-11-05 (user): keep the count-based flag.** It is the spec's wording ("current-month volume" against "their own rolling mean"). Flagging on the zone's **share** of its borough would cancel city-wide swings, and a longer baseline would shrink the t-noise, but both change what M3 computes. The report states the 46 % rate and its causes as a limitation (§6) instead.

**Post-run plan** (`plan-m3-final-local-2025-01.txt`, `isFinalPlan=true`) and stages (`stages-local-2025-01.json`), 2025-01, local:
- **Five `Exchange`s, not four.** Each is `hashpartitioning(…, 200)`, coalesced by AQE (`AQEShuffleRead coalesced`) to **1 task**:
  1. `(PULocationID, source_year, source_month)`, the aggregate
  2. `(pickup_borough, month_index)`, the rank
  3. `(PULocationID)`, the zone's rolling sum
  4. `(pickup_borough)`, the borough's rolling sum
  5. `(PULocationID)` **again**, for the baseline mean and σ

  Catalyst keeps the windows in code order, so the borough window between the two zone windows forces a second shuffle on the same key. The zone sum and the baseline windows don't share an `Exchange`, which the plan had expected.
- **The broadcast join again** (`BroadcastHashJoin … BuildRight`, the zone CSV in its own 1-task stage, 12,331 B). It runs right after the final aggregate, in the same stage, on the zone-month rows.
- **Stages (7, 16 tasks):**

  | stage | what | tasks | shuffle read | shuffle write |
  |---|---|---|---|---|
  | 6 | zone CSV, broadcast | 1 | – | – |
  | 7 | scan, clean, partial aggregate | 10 | – | 106,337 B |
  | 9 | final aggregate + join | 1 | 106,337 B | 12,340 B |
  | 12 | rank window | 1 | 12,340 B | 26,701 B |
  | 16 | zone rolling sum | 1 | 26,701 B | 14,825 B |
  | 21 | borough rolling sum | 1 | 14,825 B | 30,920 B |
  | 27 | baseline windows + write | 1 | 30,920 B | – (19,147 B output) |

  186.6 KiB of shuffle in all. The scan stage's task skew is 10.5 (median 122 ms, max 1,284 ms); the other stages have one task each.
- **Skew signal:** none in the windows. After aggregation, Manhattan is 797 of 3,083 zone-month rows for 2025 (25.9 %, 67 zones), against 86.6 % of the trips (L5). Queens has the most rows (828, 69 zones), EWR 12 (1 zone). With AQE folding every window shuffle into one task, there is nothing to skew.
- **Metric to check:** the scan stage reports `input_bytes` = 112,280 B for a ~60 MB file (M1's scan stage reports the same number). Check it against the Spark UI before using input bytes in Part C.

**Leads for the profiler** (each needs a profiled run first):
- **L4 again.** Nothing is cached, so with `--ops m1,m2,m3`, M3's job scans and cleans the trip Parquet a third time.
- **L8. Window order costs a shuffle.** The second `PULocationID` exchange exists only because of window order. Computing the zone rolling sum and the baseline next to each other should drop it. It's a pure plan change with identical results, so it's an OPT candidate. Each window shuffle is tiny, so the gain is per-stage overhead, not bytes. It's more visible on Docker than locally.
- **L9. Scan-stage task skew 10.5** for 2025-01. With 10 tasks on 1 file, the split looks uneven: one task took 1.3 s, the median 0.12 s. M1's scan stage shows 4.15. Needs per-task input sizes from the event log.
- **L5, settled for M3.** Borough skew doesn't reach M3's windows. They see rows per zone, not trips.

**Setup of these runs.** The worktree has no `data/`, so every run read the main checkout's `data/`. For Docker, the M3 jar was `docker cp`'d into the master container (`/tmp/shard-m3.jar`) instead of overwriting the main checkout's `build/libs/shard-all.jar`, and the output went to `/output/dev-m3` (the main checkout's `output/dev-m3/`, gitignored).

### O1: fare anomaly detection (2026-11-05, uncommitted O1 working tree on `361b669`)

**Provenance.** No run-ids in LOG → Runs: development runs on uncommitted code, all run by Claude. The post-run plans and stage tables come from throwaway profiled runs (`local-20261105-91` for 2025-01, `local-20261105-92` for 2025, `docker-20261105-91` for 2025-01 on Docker), with their run folders in a scratch folder or inside the container, not in `experiments/runs/`. They were used only because the profiler writes the final (post-AQE) plan and the stage metrics without a code change. Their times are **not** baseline measurements. Raw evidence, with the command for each file, is in [`observations/o1-20261105/`](observations/o1-20261105/README.md).

**What O1 computes** (report §2; decisions D15–D22 in README → O1): cleaned trips → `hour_of_day`, `day_of_week` (`withTripTimes`, NYC wall-clock) → `fare_per_km = fare_amount / (1.609344 × trip_distance)` → `avg` and `stddev_samp` of `fare_per_km` over `Window.partitionBy(PULocationID, hour_of_day)` (the whole bucket, the trip included) → `is_fare_anomaly = fare_per_km > mean + 3σ` (null without a σ) → borough join (`withPickupBorough`) → `day_type` (weekend = `dayofweek` ∈ {1, 7}) → `groupBy(day_type, payment_type, pickup_borough)` with `trips`, `flagged_trips`, `anomaly_rate` → Parquet in `<output>/o1/`.

**Correctness: verified.** Σ `trips` = M1 `kept_trips`, `flagged_trips` ≤ `trips` in every row, and Σ `flagged_trips` equals an independent re-computation in plain SQL (`bucket-diagnostics.sql`: M1's criteria and the same window written from scratch, run by `spark-sql` in the Docker master).

| Where | `--years` | Σ `trips` | M1 `kept_trips` | Σ `flagged_trips` | SQL flagged | flagged share | rows | evidence |
|---|---|---|---|---|---|---|---|---|
| local `local[*]` | 2025-01 | **3,203,825** ✅ | 3,203,825 | 14,506 | 14,506 ✅ | 0.453 % | 52 | `console-local-2025-01.txt`, `bucket-diagnostics-2025-01.txt` |
| Docker, 2 workers | 2025-01 | **3,203,825** ✅ | 3,203,825 | 14,506 | | | 52 | `console-docker-2025-01.txt`: every table identical to local, line by line |
| local `local[*]` | 2025 | **43,599,832** ✅ | 43,599,832 | 172,907 | 172,907 ✅ | 0.397 % | 60 | `console-local-2025.txt`, `bucket-diagnostics-2025.txt` |

- The rate table holds `payment_type` 0–4 (and one trip with 5 in 2025); no null `payment_type` survives M1 in 2025, and there is no 6.
- **Unit tests** (`anomalies/FareAnomaliesTest`, 8 tests; `cli/ParseArgsTest` +1; `make check` green): exact schema; an 11-trip bucket with hand-checked mean (200/11 $/mi) and σ (√89100/11), whose one outlier is flagged at 3.015 σ; a 10-trip bucket whose 1,000,000 fare is *not* flagged (Samuelson cap 2.846 σ), and the bound itself for n = 2..11; a single-trip bucket with a null σ and a null flag, kept apart from another zone at the same hour; the weekend edges (Fri 23:59 weekday, Sat 00:00 and Sun 23:59 weekend, Mon 00:00 weekday); the rate table per (day type, payment type, borough) with a null payment type kept as its own group; Σ `trips` = input trips and flagged ≤ trips.

**The statistics** (report §2 and §6; `bucket-diagnostics-*.txt`, queries 1–5):

| | 2025-01 | 2025 |
|---|---|---|
| buckets | 5,535 | 6,102 (of 6,312 possible) |
| buckets with n ≤ 10 (trips in them) | 1,788 (8,317) | 418 (1,788) |
| of which n = 1, null σ (trips) | 315 | 89 |
| flags in n ≤ 10 buckets | **0** (Samuelson) | **0** |
| flagged share | 0.453 % | 0.397 % |
| trips < 0.1 mi: trips, flagged, share of all flags | 16,298 · 10,854 · 74.8 % | 287,120 · 163,448 · **94.5 %** |
| `fare_per_km` p50 / p99 / p99.9 / max ($/km) | 4.46 / 15.5 / 1,308 / 335,297 | 4.49 / 24.0 / 1,823 / 335,297 |
| median bucket σ / mean | 7.5 | 9.1 |

- **Against the bounds.** The flagged share (0.40 % for 2025) is 3× what a normal distribution puts above mean + 3σ (0.135 %) and far below Cantelli's distribution-free cap, 1/(1 + 3²) = 10 %.
- **Samuelson (D18) holds on real data.** No bucket with 10 trips or fewer flags anything. On 2025 these are 418 buckets but only 1,788 trips (0.004 %), so the rule matters for coverage of zone-hours, not for the rate. On one month it covers 1,788 buckets, a third of them.
- **Heavy tails decide the flags.** `fare_per_km` has a tail to 335,297 $/km: a fare over a distance of a few hundredths of a mile. The typical bucket's σ is 9 × its mean, so mean + 3σ sits far above every normal fare, and the flag fires almost only on sub-0.1-mile trips: 94.5 % of 2025's flags, where 57 % of those trips are flagged. Above 1 mile, 1,114 of 34.4 M trips are flagged. So O1 as specified detects "near-zero recorded distance" more than "overcharged trip". Report §6 limitation; a robust variant (median/MAD, or a minimum distance) would compute something else, so the spec's definition stays.
- **Weekday vs weekend** (2025, `console-local-2025.txt`): 0.489 % of weekday trips are flagged, 0.169 % of weekend trips. Most of the gap is one payment code: weekday `payment_type` 0 (flex fare) has 117,935 flags out of 5,468,781 trips (2.16 %, 68 % of all flags), against 0.16 % on the weekend. Card (1) is 0.09 % / 0.13 % and cash (2) 0.26 % / 0.31 %. By borough, Manhattan is 0.45 % weekday / 0.11 % weekend. Why flex-fare weekday trips concentrate the flags (likely tiny distances) wasn't checked.

**Post-run plan** (`plan-o1-final-local-2025-01.txt` and `…-2025.txt`, `isFinalPlan=true`). Report §2 material:
- **One window `Exchange`, carrying every trip:** `hashpartitioning(PULocationID, hour_of_day, 200)` directly over the scan-and-clean stage, with no partial aggregate in front of it (unlike M2 and M3). Catalyst pruned it to 5 columns: `PULocationID`, `payment_type`, `day_of_week`, `hour_of_day`, `fare_per_km` (`duration_min` from `withTripTimes` and the unused `bucket_trips` count are gone). About 11.8 B per trip: 36.1 MiB for 2025-01, 498.8 MiB for 2025.
- Then `Sort (PULocationID, hour_of_day)` → `Window` (mean and σ, `RowFrame` unbounded both ways) → the zone CSV as `BroadcastHashJoin … BuildRight` (12,331 B, auto-broadcast as in M2) in the same stage → partial aggregate → a small second `Exchange hashpartitioning(day_type, payment_type, pickup_borough, 200)` → final aggregate and write.
- Both `Exchange`s read through `AQEShuffleRead coalesced`. **AQE coalesced the 200 window partitions to 11 tasks locally and 4 on Docker**, sized for parallelism (`ShufflePartitionsUtil`: advisory 64 MiB, but `parallelismFirst` targets about total ÷ cores).
- Profiler summary (not measurements): 2025-01 local, o1 4 stages / 23 tasks; 2025 local, 4 stages / 25 tasks, 498.8 MiB shuffle, **456.4 MiB spilled to disk**.

**Skew evidence** (for spark-profiler; per-task numbers from the event logs, `window-task-durations-*.txt`, `stages-*.json`):

| | 2025-01 local | 2025-01 Docker | 2025 local |
|---|---|---|---|
| window-stage tasks | 11 | 4 | 11 |
| task skew (max / median) | 1.01 | 1.00 | 1.03 |
| records per task | 291,766–317,672 (+ one of 129,474) | | 4.05–4.37 M (+ one of 1.74 M) |
| spill per task (memory / disk) | none | none | 218–336 MB / 47–49 MB, in 10 of 11 tasks |
| rows per hash partition before AQE (of 200): median, max, max/median | 15,227 · 49,349 · 3.24 | | 209,798 · 648,593 · 3.09 |
| largest bucket | zone 161 at 18h, 15,821 trips (0.49 %) | | zone 161 at 18h, 169,582 trips (0.39 %) |

- **The predicted straggler doesn't appear with default settings.** The biggest bucket is under 0.5 % of the trips; 881 buckets of more than 10k trips hold 87 % of 2025's trips, and the hash spreads them over the 200 partitions with a 3.1× max/median. AQE then merges about 18 partitions per task, which averages the skew out: every window task does the same work.
- **What does show is volume and spill:** every cleaned trip is shuffled (≈ 0.5 GB for 2025) and then sorted per task, and 10 of 11 window tasks spill. In the 2025 window stage, executor CPU is 63 s of 101 s run time.

**Leads for the profiler** (each needs a profiled run first):
- **L4 again.** Nothing is cached, so with every operation selected, O1's job scans and cleans the trip Parquet once more.
- **L10. The window shuffles and sorts every trip.** No partial aggregate shrinks the data before the `Exchange`, and on 2025 the sort spills in almost every task. Candidate rewrite: `groupBy(PULocationID, hour_of_day)` for count/mean/σ (at most 6,312 rows, partial aggregation before a tiny shuffle), then broadcast the bucket stats back to the trips: no trip-sized shuffle, no sort, no spill. Results must stay identical (counts exact; flags identical, mean and σ within the 10⁻¹² relative tolerance).
- **L11. Skew is latent, not gone.** The bucket skew is real before AQE (3.1× rows per hash partition) and is hidden by coalescing about 18 partitions per task. It would show with more shuffle partitions or more cores (DI cluster), or with AQE's coalescing off. Worth one profiled run with `spark.sql.adaptive.coalescePartitions.enabled=false` to measure it, and for the scalability section: as cores grow, fewer partitions get merged per task. AQE's skew handling only covers joins, so a window can't be split.

### O3: demand forecasting features (2026-11-05, branch `feat/o3-demand-forecasting` with main `9b4cc8a` merged in)

**Provenance.** No run-ids in LOG → Runs: development runs on uncommitted code. As for M3, the post-run plan and stage table come from a throwaway profiled run (`--run-id local-20261105-93 --runs-dir <scratchpad>`, `local-20261105-94` for 2025; O1 used -91 and -92), used only because the profiler writes the final (post-AQE) plan without a code change. Its times are **not** a baseline measurement. Raw evidence, with the command for each file, goes in [`observations/o3-20261105/`](observations/o3-20261105/README.md).

**What O3 computes** (report §2; decisions D23–D30 in README → O3):
1. Cleaned trips → `hour_index` (zone-free, D24) → `groupBy(PULocationID, hour_index)`.
2. The **grid** (`range(1..263) × range(hours of --years)`) is left-joined with those counts, and missing counts become 0 (D25). `pickup_date`, `day_of_week` and `hour_of_day` are derived from `hour_index`.
3. `avg` and `var_samp` over the whole (`PULocationID`, `day_of_week`, `hour_of_day`) window (D26).
4. **The 4-week-lag self-join:** the zero-filled grid, shifted by +672 hours, is left-joined back on (`PULocationID`, `hour_index`), with no hint (D27).
5. `demand_index` and `is_demand_spike` (D28), then the `service_zone` join (D29).
6. Parquet in `<output>/o3/`, not partitioned.

**Correctness:** _pending (the user's runs)_. The invariants:
- rows = 263 × hours in the range (195,672 for 2025-01, 2,303,880 for 2025)
- `rows_with_lag` = 263 × (hours − 672) (18,936 and 2,127,144)
- Σ `trip_count` = M1 `kept_trips` − trips whose pickup is outside the range (D24), at most 29 for 2025 per roadmap 02, with the gap checked by a one-off count query

| Where | `--years` | Σ `trip_count` | M1 `kept_trips` | out of range | rows | rows_with_lag | spikes | zero_mean | evidence |
|---|---|---|---|---|---|---|---|---|---|
| local `local[*]` | 2025-01 | _pending_ | 3,203,825 | _pending_ | _pending_ (195,672) | _pending_ (18,936) | _pending_ | _pending_ | `console-local-2025-01.txt` |
| Docker, 2 workers | 2025-01 | _pending_ | 3,203,825 | _pending_ | _pending_ | _pending_ | _pending_ | _pending_ | `console-docker-2025-01.txt` |
| local `local[*]` | 2025 | _pending_ | 43,599,832 | _pending_ | _pending_ (2,303,880) | _pending_ (2,127,144) | _pending_ | _pending_ | `console-local-2025.txt` |

- **Unit tests** (`forecast/DemandForecastTest`, 10 tests; `make check` _pending_):
  - the exact schema; rows = zones × hours
  - zero-fill: μ = 1/3 and variance 1, where averaging non-empty hours would give 3
  - the lag across the January–February boundary is exactly 28 days back; null before the range, 0 for an empty lag hour
  - a hand-made spike with DI 5 is flagged, and the rest of its bucket has 0.5; a zero mean gives a null DI and no spike
  - a 2024 trip is left out; `service_zone` comes from the lookup
  - in a Europe/Lisbon session, the hour index and each trip's hour are unchanged on 2025-03-30 and 2025-10-26

**Post-run plan** (`plan-o3-final-local-2025-01.txt` and `plan-o3-final-local-2025.txt`, `isFinalPlan=true`): _pending_. This is the profiler's input for the join story, so record for both scales:
- the join type of each join: the grid cross join (`BroadcastNestedLoopJoin` expected: both sides are `Range`s), the zero-fill left join, **the lag self-join**, and the `service_zone` join (`BroadcastHashJoin` expected, 12 KB)
- every `Exchange` and its key: the trip aggregate, both sides of the zero-fill join, the bucket window, both sides of the self-join
- whether a `ReusedExchange` appears: both self-join sides come from the same zero-filled grid, so the trip scan and aggregate could run once or twice
- each self-join side's shuffle bytes and rows (from `stages-local-*.json`)
- **Expected risk:** on 2025-01 a self-join side is about 196k rows (≈ 6 MB of shuffle), under the 10 MB broadcast threshold, so AQE may switch the SortMergeJoin to a broadcast join at runtime. On 2025 (2.3M rows per side) it should stay a SortMergeJoin. Whatever the plans show decides which scale carries the O3 join story in Part C.

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
