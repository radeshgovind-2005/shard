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
