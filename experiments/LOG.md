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
| local-20261105-01 | ff4adff | local `local[*]` | 2025, all ops | baseline (defaults) | 49.54 | 689.9 / 656.9 MiB | 146 | 456.4 MiB | 3.9 | **warm-up, discarded**. m1 8.84 · m2 5.57 · m3 10.31 · o1 14.33 · o3 10.49 |
| local-20261105-02 | ff4adff | local `local[*]` | 2025, all ops | baseline (defaults) | 39.72 | 689.9 / 656.9 MiB | 146 | 456.4 MiB | 3.5 | m1 6.98 · m2 4.69 · m3 8.19 · o1 12.09 · o3 7.77 |
| local-20261105-03 | ff4adff | local `local[*]` | 2025, all ops | baseline (defaults) | 40.61 | 689.9 / 656.9 MiB | 146 | 456.4 MiB | 3.5 | m1 6.88 · m2 4.98 · m3 8.05 · o1 11.98 · o3 8.72 |
| local-20261105-04 | ff4adff | local `local[*]` | 2025, all ops | baseline (defaults) | 43.17 | 689.9 / 656.9 MiB | 146 | 456.4 MiB | 3.4 | m1 6.94 · m2 5.30 · m3 9.59 · o1 11.85 · o3 9.49 |
| local-20261105-05 | ff4adff | local `local[*]` | 2025, all ops | baseline (defaults) | 43.65 | 689.9 / 656.9 MiB | 146 | 456.4 MiB | 3.5 | m1 7.89 · m2 4.99 · m3 8.81 · o1 12.16 · o3 9.80. Added because per-op spread > 10 % after 02–04. A short event-log parse (Python, 1 core, a few s) may have overlapped this run |
| local-20261105-06 | ff4adff | local `local[*]` | 2025, all ops | baseline (defaults) | 41.77 | 689.9 / 656.9 MiB | 146 | 456.4 MiB | 3.4 | m1 7.26 · m2 4.69 · m3 8.51 · o1 11.79 · o3 9.52 |
| local-20261105-07 | ff4adff | local `local[*]` | 2025, all ops | baseline (defaults) | 39.70 | 689.9 / 656.9 MiB | 146 | 456.4 MiB | 4.3 | m1 6.93 · m2 4.80 · m3 8.43 · o1 11.69 · o3 7.85 |
| docker-20261105-01 | ff4adff | Docker, 2 workers × 2 cores | 2025, all ops | baseline (defaults) | 57.65 | 683.3 / 650.0 MiB | 96 | 431.9 MiB | 5.3 | **warm-up, discarded**. m1 10.79 · m2 7.62 · m3 12.75 · o1 15.71 · o3 10.78 |
| docker-20261105-02 | ff4adff | Docker, 2 workers × 2 cores | 2025, all ops | baseline (defaults) | 58.28 | 683.3 / 650.0 MiB | 96 | 431.7 MiB | 5.0 | m1 9.84 · m2 7.06 · m3 12.02 · o1 15.57 · o3 13.79 |
| docker-20261105-03 | ff4adff | Docker, 2 workers × 2 cores | 2025, all ops | baseline (defaults) | 58.23 | 683.3 / 650.0 MiB | 96 | 431.8 MiB | 4.9 | m1 9.63 · m2 7.37 · m3 12.17 · o1 15.90 · o3 13.16 |
| docker-20261105-04 | ff4adff | Docker, 2 workers × 2 cores | 2025, all ops | baseline (defaults) | 59.96 | 683.3 / 650.0 MiB | 96 | 431.8 MiB | 5.0 | m1 10.09 · m2 7.36 · m3 12.23 · o1 16.18 · o3 14.10 |
| local-20261105-08 | 0d5657b | local `local[*]` | 2025, all ops | OPT-1 on | 35.20 | 689.9 / 672.9 MiB | 170 | 321.2 MiB | 4.9 | **warm-up, discarded**. m1 4.95 · m2 12.69 · m3 1.29 · o1 8.28 · o3 7.99. Cache (last poll): mem 1501 MiB + disk 777 MiB, 12/12 partitions. |
| local-20261105-09 | 0d5657b | local `local[*]` | 2025, all ops | OPT-1 off (`--skip-opts opt1`) | 25.95 | 689.9 / 656.9 MiB | 146 | 456.4 MiB | 3.5 | m1 4.52 · m2 3.00 · m3 4.80 · o1 7.89 · o3 5.74. |
| local-20261105-10 | 0d5657b | local `local[*]` | 2025, all ops | OPT-1 on | 32.56 | 689.9 / 672.9 MiB | 170 | 317.6 MiB | 4.0 | m1 4.57 · m2 12.77 · m3 1.16 · o1 7.29 · o3 6.77. Cache (last poll): mem 1533 MiB + disk 745 MiB, 12/12 partitions. |
| local-20261105-11 | 0d5657b | local `local[*]` | 2025, all ops | OPT-1 off (`--skip-opts opt1`) | 24.90 | 689.9 / 656.9 MiB | 146 | 456.4 MiB | 3.2 | m1 4.55 · m2 2.99 · m3 4.97 · o1 6.94 · o3 5.45. |
| local-20261105-12 | 0d5657b | local `local[*]` | 2025, all ops | OPT-1 on | 31.92 | 689.9 / 672.9 MiB | 170 | 369.8 MiB | 3.5 | m1 4.48 · m2 12.58 · m3 1.31 · o1 6.87 · o3 6.68. Cache (last poll): mem 1700 MiB + disk 580 MiB, 12/12 partitions. |
| local-20261105-13 | 0d5657b | local `local[*]` | 2025, all ops | OPT-1 off (`--skip-opts opt1`) | 25.12 | 689.9 / 656.9 MiB | 146 | 456.4 MiB | 2.9 | m1 4.28 · m2 2.99 · m3 4.73 · o1 7.24 · o3 5.88. |
| local-20261105-14 | 0d5657b | local `local[*]` | 2025, all ops | OPT-1 on | 38.91 | 689.9 / 672.9 MiB | 170 | 384.6 MiB | 4.8 | Outlier in `on`: m2 16.56 s, cpu/run 0.60 (cause not found). m1 4.51 · m2 16.56 · m3 1.39 · o1 9.18 · o3 7.27. Cache (last poll): mem 1676 MiB + disk 602 MiB, 12/12 partitions. |
| local-20261105-15 | 0d5657b | local `local[*]` | 2025, all ops | OPT-1 off (`--skip-opts opt1`) | 27.05 | 689.9 / 656.9 MiB | 146 | 456.4 MiB | 3.7 | Added (with 16) because the total spread of `on` was > 10 % after 10–14. m1 4.21 · m2 3.00 · m3 5.25 · o1 8.62 · o3 5.97. |
| local-20261105-16 | 0d5657b | local `local[*]` | 2025, all ops | OPT-1 on | 34.65 | 689.9 / 672.9 MiB | 170 | 369.8 MiB | 4.3 | Added pair, see 15. m1 4.77 · m2 13.44 · m3 1.24 · o1 7.82 · o3 7.38. Cache (last poll): mem 1721 MiB + disk 558 MiB, 12/12 partitions. |
| docker-20261105-05 | 0d5657b | Docker, 2 workers × 2 cores | 2025, all ops | OPT-1 on | 48.14 | 683.1 / 665.6 MiB | 116 | 427.3 MiB | 6.2 | **warm-up, discarded**. m1 5.97 · m2 19.83 · m3 3.16 · o1 10.02 · o3 9.16. Cache (last poll): mem 0 MiB + disk 5191 MiB, 10/10 partitions. |
| docker-20261105-06 | 0d5657b | Docker, 2 workers × 2 cores | 2025, all ops | OPT-1 off (`--skip-opts opt1`) | 34.80 | 683.3 / 650.0 MiB | 96 | 431.8 MiB | 4.9 | m1 6.39 · m2 4.45 · m3 7.21 · o1 8.87 · o3 7.88. |
| docker-20261105-07 | 0d5657b | Docker, 2 workers × 2 cores | 2025, all ops | OPT-1 on | 48.64 | 682.9 / 665.5 MiB | 116 | 418.9 MiB | 6.2 | m1 6.09 · m2 19.99 · m3 2.53 · o1 10.09 · o3 9.94. Cache (last poll): mem 0 MiB + disk 4828 MiB, 10/10 partitions. |
| docker-20261105-08 | 0d5657b | Docker, 2 workers × 2 cores | 2025, all ops | OPT-1 off (`--skip-opts opt1`) | 34.21 | 683.3 / 650.0 MiB | 96 | 431.9 MiB | 5.0 | m1 6.14 · m2 4.68 · m3 7.43 · o1 8.30 · o3 7.66. |
| docker-20261105-09 | 0d5657b | Docker, 2 workers × 2 cores | 2025, all ops | OPT-1 on | 48.73 | 683.0 / 665.6 MiB | 115 | 406.7 MiB | 5.8 | m1 5.91 · m2 19.70 · m3 2.62 · o1 10.49 · o3 10.01. Cache (last poll): mem 0 MiB + disk 5102 MiB, 10/10 partitions. |
| docker-20261105-10 | 0d5657b | Docker, 2 workers × 2 cores | 2025, all ops | OPT-1 off (`--skip-opts opt1`) | 33.82 | 683.3 / 650.0 MiB | 96 | 432.1 MiB | 5.5 | m1 6.21 · m2 4.46 · m3 7.14 · o1 8.49 · o3 7.52. |
| docker-20261105-11 | 0d5657b | Docker, 2 workers × 2 cores | 2025, all ops | OPT-1 on | 46.91 | 682.9 / 665.5 MiB | 115 | 420.7 MiB | 5.5 | m1 5.85 · m2 19.03 · m3 2.33 · o1 9.93 · o3 9.77. Cache (last poll): mem 0 MiB + disk 5689 MiB, 10/10 partitions. |

**How to read the columns.** Wall-clock is the sum of the five profiled blocks (`wall_time_s` in `run.json`), each of which is one operation's write. It excludes Spark startup and the unprofiled read-backs; the whole `make run` took 47–52 s locally and the `spark-submit` 66–69 s on Docker. Shuffle, tasks, spill and GC % (`jvm_gc_time` / `executor_run_time`) are summed over the five blocks. Every run used `--input data|/data --output output|/output --years 2025 --run-id <id> --git-sha ff4adff` (plus `--runs-dir /runs` on Docker), with no `spark.*` setting changed (`spark_conf` in `run.json` holds only Spark's own entries and the event-log pair). Docker ran on a fresh `down`/`up --scale spark-worker=2` with a rebuilt jar (`make jar`, 2026-11-05 21:17). Docker was stopped during the local runs, and everything ran under `caffeinate -i`. Every run's printed tables match the verified results (M1 kept 43,599,832; M2 991 rows; M3 3,083 rows with 1,166 flagged; O1 172,907 flags; O3 2,303,880 rows, Σ 43,599,803).

**How the OPT-1 runs were run** (`benchmark-runner`, 2026-11-05, all on the same jar, built from commit 0d5657b, `--git-sha 0d5657b`; the only difference is `--skip-opts opt1`, which `run.json` records as `args.skip_opts`). Same command line as the baseline otherwise (`--input data|/data --output output|/output --years 2025`, plus `--runs-dir /runs` on Docker), no `spark.*` setting, everything under `caffeinate -i`, one run at a time. Local: Docker stopped, run 08 = warm-up (on, discarded), then interleaved off/on pairs 09/10, 11/12, 13/14; the `on` total spread was 20.8 % after three pairs, so a fourth pair (15/16) was added. Docker: fresh `down` + `up -d --scale spark-worker=2` (both workers registered with 2 cores / 2.0 GiB, per the master log), run 05 = warm-up (on, discarded), then interleaved 06/07, 08/09, 10/11. During every `on` run (and the warm-ups) a shell loop polled the driver's `/api/v1/applications/<app>/storage/rdd` every 2 s from the host (port 4040, published by the master container for Docker) and kept the last non-empty answer; the `off` runs were not polled, so the poll (a `curl` every 2 s) is a small asymmetry that favours `off`. Every run's printed tables matched the verified results (M1 kept 43,599,832; M2 991 rows; M3 3,083 rows, 1,166 flagged; O1 172,907 flags; O3 2,303,880 rows, Σ trip_count 43,599,803). **Comparing with the baseline rows above is context only:** the `off` runs (same code path, same plans, same shuffle and spill) are 38 % (local) and 41 % (Docker) faster than the baseline medians, for a reason not found (machine state on the day; the baseline's `ff4adff` differs from `0d5657b` only by the OPT-1 plumbing). Before/after for OPT-1 uses only the same-session `off` rows. Derived tables: [`observations/opt1-20261105/bench/`](observations/opt1-20261105/bench/README.md).

### Baseline, 2025, default configuration

Derived tables and their commands: [`observations/baseline-20261105/`](observations/baseline-20261105/README.md).

| | local (n = 6: 02–07) | Docker, 2 workers (n = 3: 02–04) |
|---|---|---|
| **wall-clock, median [min–max]** | **41.19 s [39.70–43.65]**, spread 9.6 % | **58.28 s [58.23–59.96]**, spread 3.0 % |
| m1 | 6.96 [6.88–7.89] (16.9 %) | 9.84 [9.63–10.09] (16.9 %) |
| m2 | 4.89 [4.69–5.30] (11.9 %) | 7.36 [7.06–7.37] (12.6 %) |
| m3 | 8.47 [8.05–9.59] (20.6 %) | 12.17 [12.02–12.23] (20.9 %) |
| o1 | 11.91 [11.69–12.16] (28.9 %) | 15.90 [15.57–16.18] (27.3 %) |
| o3 | 9.11 [7.77–9.80] (22.1 %) | 13.79 [13.16–14.10] (23.7 %) |
| shuffle read / write | 689.9 / 656.9 MiB (O1 498.8 / 498.8, O3 189.4 / 156.5) | 683.3 / 650.0 MiB |
| tasks (stages) | 146 (24) on 10 cores | 96 (24) on 4 cores |
| disk spill | 456.4 MiB, all in O1's window stage | 431.7–431.8 MiB, same stage |
| GC % / CPU ÷ run time | 3.4–4.3 % / 0.69–0.74 | 4.9–5.0 % / 0.90 |
| max task skew (max / median) | 1.37 (O3) | 1.63 (O3) |

- **Spread.** The totals are inside 10 % in both environments. Locally, single operations still spread by 12–22 % (M2, M3, O3; O3 alternates between about 7.8 s and 9.5 s) even at n = 6, so a local OPT on one operation needs an effect clearly above about 20 %, or more runs. Docker is much steadier: ≤ 6.8 % per operation. Prefer Docker for before/after comparisons of single operations.
- **The warm-ups.** local-01 was 20 % slower than the median (probably a cold OS file cache after the Docker runs of the day), and docker-01's O3 was faster than every measured run (10.78 s against 13.16–14.10). Both were discarded as the protocol says. docker-01's O3 wasn't investigated.
- **Docker is 1.41× slower than local** with 4 cores against 10. Scaling is for `scalability-analyst`; it isn't interpreted here.

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

### O3: demand forecasting features (2026-11-05, `feat/o3-demand-forecasting` at `ee960bd`: O3 plus main `9b4cc8a`)

**Provenance.** No run-ids in LOG → Runs: development runs, all run by Claude. The post-run plans and stage tables come from throwaway profiled runs (`local-20261105-93` for 2025-01, `local-20261105-94` for 2025, `--runs-dir <scratchpad>`; O1 used -91 and -92). They are used only because the profiler writes the final (post-AQE) plan without a code change. Their times are **not** baseline measurements. Raw evidence, with the command for each file, is in [`observations/o3-20261105/`](observations/o3-20261105/README.md).

**What O3 computes** (report §2; decisions D23–D30 in README → O3):
1. Cleaned trips → `hour_index` (zone-free, D24) → `groupBy(PULocationID, hour_index)`.
2. The **grid** (`range(1..263) × range(hours of --years)`) is left-joined with those counts, and missing counts become 0 (D25). `pickup_date`, `day_of_week` and `hour_of_day` are derived from `hour_index`.
3. `avg` and `var_samp` over the whole (`PULocationID`, `day_of_week`, `hour_of_day`) window (D26).
4. **The 4-week-lag self-join:** the zero-filled grid, shifted by +672 hours, is left-joined back on (`PULocationID`, `hour_index`), with no hint (D27).
5. `demand_index` and `is_demand_spike` (D28), then the `service_zone` join (D29).
6. Parquet in `<output>/o3/`, not partitioned.

**Correctness: verified.** Every invariant holds, and the trips missing from Σ `trip_count` equal an independent count (`out-of-range-trips.txt`: M1's five criteria written from scratch in `spark-sql`, pickups outside the range).

| Where | `--years` | Σ `trip_count` | M1 `kept_trips` | out of range (SQL) | rows | rows_with_lag | spikes | zero_mean | evidence |
|---|---|---|---|---|---|---|---|---|---|
| local `local[*]` | 2025-01 | **3,203,803** ✅ | 3,203,825 | 22 ✅ | **195,672** ✅ | **18,936** ✅ | 15,685 | 60,155 | `console-local-2025-01.txt` |
| Docker, 2 workers | 2025-01 | **3,203,803** ✅ | 3,203,825 | | 195,672 | 18,936 | 15,685 | 60,155 | `console-docker-2025-01.txt`, identical per service zone |
| local `local[*]` | 2025 | **43,599,803** ✅ | 43,599,832 | 29 ✅ | **2,303,880** ✅ | **2,127,144** ✅ | 226,925 | 178,626 | `console-local-2025.txt` |

- **The out-of-range trips** are exactly roadmap 02's stray-dated trips outside 2025: pickups in 2007 (1), 2008 (1), 2009 (6) and 2024 (21). On 2025-01 they are the 22 January-file trips dated outside January.
- **Service zones:** 2 Airports zones, 205 Boro Zone, 1 EWR, 55 Yellow Zone: all 263, and no row is lost in the inner join (D29).
- **Local vs Docker, cell by cell** (2025-01, `local-vs-docker-diff-2025-01.txt`): 195,672 rows on both sides, all of them matched on (zone, date, hour).
  - Exactly equal: every key, count, lag, flag, mean and demand index, and the nulls in the same places.
  - Not equal: `hist_var_trips` in 27,588 rows (the exact `EXCEPT ALL` finds these, both ways), by at most **2.4 × 10⁻¹⁵ relative**. That's float summation order, as in M2's methodology note.
- **Unit tests** (`forecast/DemandForecastTest`, 10 tests; `cli/ParseArgsTest` +1; `make check` green):
  - the exact schema; rows = zones × hours
  - zero-fill: μ = 1/3 and variance 1, where averaging non-empty hours would give 3
  - the lag across the January–February boundary is exactly 28 days back; null before the range, 0 for an empty lag hour
  - a hand-made spike with DI 5 is flagged, and the rest of its bucket has 0.5; a zero mean gives a null DI and no spike
  - a 2024 trip is left out; `service_zone` comes from the lookup
  - in a Europe/Lisbon session, the hour index and each trip's hour are unchanged on 2025-03-30 and 2025-10-26

**Finding: the join strategy flips with scale** (`plan-o3-final-local-*.txt`, `join-size-estimates.txt`). Report §2 and the O3 justification:

| join | 2025-01 (initial and final plan) | 2025 (initial and final plan) |
|---|---|---|
| grid cross join (`Range` × `Range`) | `BroadcastNestedLoopJoin BuildLeft` | same |
| zero-fill left join (grid ⋈ trip counts) | `BroadcastHashJoin LeftOuter BuildRight` | `SortMergeJoin LeftOuter` |
| **4-week-lag self-join** | **`BroadcastHashJoin LeftOuter BuildRight`** | **`SortMergeJoin LeftOuter`** |
| `service_zone` join (12,331 B CSV) | `BroadcastHashJoin Inner BuildRight` | same |

- **The choice is static, not AQE's.** The initial plan already has the same join types as the final plan at both scales. Catalyst's estimate for the self-join's build side (the shifted grid) is **9,576,886 B on 2025-01**, 8.7 % under `autoBroadcastJoinThreshold` (10,485,760 B), and **122,444,757 B on 2025**. The zero-fill join's build side (the trip counts) is 5,724,990 B and 80,319,996 B.
- **The estimate is file bytes scaled by row width, not rows** (CBO is off). Replayed without `loadTrips`' `_metadata.file_path` projection, the same 2025-01 build side is estimated at 11,621,525 B, which is over the threshold. So the hidden metadata columns alone decide that one month broadcasts. The real 2025-01 lag side is 195,672 rows of three columns.
- **Consequence for Part C:** a one-month run doesn't show O3's SortMergeJoin. The O3 join story has to be profiled on 2025 (or more data), where both grid joins shuffle and sort.

**Exchanges and reuse** (final plans; byte counts from `stages-local-*.json`):
- **2025, five shuffles, 164.0 MB written and 198.6 MB read** (the profiler's 156.5 / 189.4 MiB):
  1. the trip aggregate `(PULocationID, hour_index)`: 16.7 MB from 12 scan tasks
  2. the grid side of the zero-fill join `(PULocationID, hour_index)`: 17.8 MB
  3. the bucket window `(PULocationID, day_of_week, hour_of_day)`: 36.5 MB
  4. the self-join's current side, re-shuffled after the window to `(PULocationID, hour_index)`: **67.3 MB**
  5. the self-join's lag side (shifted grid): **25.7 MB**
- **`ReusedExchange` appears, but only below the zero-fill join.** Both self-join sides reuse exchanges 1 and 2, so the trip scan and aggregate run once. The zero-fill **SortMergeJoin itself runs twice** (stages 12 and 13 each read the same 34.5 MB), once per self-join side.
- **2025-01, two shuffles, 5.2 MB** (the trip aggregate 1.8 MB, the window 3.3 MB). The trip counts' `BroadcastExchange` is reused (`ReusedExchange`) by the lag side, and the final stage (window, both broadcast joins, write) ran as 3 tasks after AQE coalescing.
- **Skew:** none in the O3 stages on 2025 (max/median ≤ 1.11). On 2025-01 the scan stage shows 10.0 (L9's single-file split pattern). Output: 23,986,074 B for 2025 and 941,907 B for 2025-01.

**Finding: the spike flag mostly catches sparse buckets** (2025, `spike-breakdown-2025.txt`). 226,925 of 2,303,880 rows are flagged (9.85 %):

| bucket mean | rows | spikes | of which 1 trip |
|---|---|---|---|
| < 0.5 | 760,156 | 97,141 | 81,368 |
| 0.5–1 | 302,406 | 50,249 | 0 |
| 1–10 | 785,833 | 74,451 | 0 |
| 10–100 | 304,779 | 4,849 | 0 |
| ≥ 100 | 150,706 | 235 | 0 |

- 98 % of the flags are in buckets averaging under 10 trips an hour, where DI > 2 is Poisson noise: in a bucket with mean < 0.5, a single trip is already a spike. The maximum DI is exactly **53**, the D26 cap (one trip in 53 weeks). Only 235 flags fall in buckets averaging ≥ 100 trips, mostly the airports (132, 138) and Penn Station (186) at night (top: zone 138, 2025-05-15 00:00, 444 trips against a mean of 106.8).
- This is the spec's ratio rule. A minimum count or a variance-scaled rule would change what O3 computes. For the report, the flag rate goes in the limitations (§6), as M3's 46 % did.

**Leads for the profiler** (each needs a profiled run on 2025 first; L10–L11 are O1's):
- **L4 again.** Nothing is cached, so with every operation selected, O3's job scans and cleans the trip Parquet once more.
- **L12. The self-join is a SortMergeJoin on 2025, sorting about 93 MB of shuffle.** It is the spec's required shape and O3's main optimisation target. The candidates are the ones the brief named: a broadcast hint on the 25.7 MB lag side, co-partitioning both sides on (`PULocationID`, `hour_index`), or replacing the join with a window `lag(672)` over `PULocationID` ordered by `hour_index`, which removes it. Each must keep the output identical (lag null before the range).
- **L13. The zero-fill join runs twice.** The self-join's two sides share the trip exchanges but not the joined grid, so the 34.5 MB zero-fill SortMergeJoin is read and computed twice. Persisting `hourly` (or the window-`lag` rewrite of L12) would compute it once.
- **L14. The window and the self-join want different partitioning.** The bucket window shuffles by (`PULocationID`, `day_of_week`, `hour_of_day`), and the self-join then re-shuffles the same 2.3M rows by (`PULocationID`, `hour_index`): the 67.3 MB exchange. Running the lag before the window, or partitioning both on `PULocationID` alone, could drop one exchange. This is a plan change with identical results.
- **L15. One month isn't representative.** On 2025-01 every grid join broadcasts, and the self-join's build-side estimate sits 8.7 % under the threshold. Any O3 baseline or OPT must be measured on 2025, and say so.

**Setup of these runs.** The worktree has no `data/`, so every run read the main checkout's `data/`. Local outputs and run folders went to the session scratchpad. For Docker, the merged jar was `docker cp`'d into the master (`/tmp/shard-o3.jar`) instead of overwriting the main checkout's `build/libs/shard-all.jar`, and the output went to `/output/dev-o3` (the main checkout's `output/dev-o3/`, gitignored). The SQL checks ran in the Docker master (`spark-sql --master local[2]`) over copies of the local outputs in `/tmp`.

## Hypotheses

Written by `spark-profiler`. Each needs evidence: metric + run-id.

### Baseline diagnosis (2026-11-05; runs local-20261105-02…07, docker-20261105-02…04; sha ff4adff; 2025, defaults)

Process: `spark-profiler`'s, applied by hand (Claude). The inputs are `stages.json` and the final plans (`plans/<op>.txt`, all `isFinalPlan=true`), plus the event logs for what `stages.json` doesn't hold: task timelines, records read and the scan's SQL metrics. The derived tables are in [`observations/baseline-20261105/`](observations/baseline-20261105/README.md). Shares are of the median total wall-clock: **41.19 s local, 58.28 s Docker**.

**Where the time goes** (median stage durations; `attribution.txt`, `bounds` in the README's scripts):

| | local s | % | Docker s | % |
|---|---|---|---|---|
| **5 trip-scan stages** (scan + clean + each op's first step), one per operation | 25.25 | **61.3** | 36.98 | **63.5** |
| … of which M3's (partial aggregate with 2 × `stddev_samp`) | 7.47 | 18.1 | 10.66 | 18.3 |
| … of which M1's (9 conditional counts) | 5.49 | 13.3 | 8.46 | 14.5 |
| … M2 / O1 / O3 | 4.19 / 4.10 / 4.00 | 10.2 / 10.0 / 9.7 | 6.39 / 6.46 / 5.01 | 11.0 / 11.1 / 8.6 |
| **O1 window stage** (sort + window + join + partial agg, spilling) | 7.36 | **17.9** | 9.05 | **15.5** |
| **O3 after its scan** (2 zero-fill SMJs, window, self-join SMJ, write) | 5.05 | 12.3 | 8.71 | 14.9 |
| everything else (1-task final aggregates, M3's 5 window stages, broadcasts, gaps between jobs) | ≈ 3.5 | ≈ 8.5 | ≈ 3.6 | ≈ 6 |

**Checks the task asked for:**
- **Recomputation: confirmed, and the biggest item.** Every operation reads all 12 files again: the scan's SQL metric `size of files read` is **829,973,299 B in each of the 5 jobs** (plus 12,331 B of zone CSV where it's joined), and each scan stage reads **48,722,602 records** (event log, local-02 and docker-02). Nothing is cached; no `InMemoryTableScan` appears in any plan. The profiler's own `input_bytes` (0.1–0.2 MiB per op) is wrong for Parquet; see the methodology note below.
- **Task count against cores.** Local: every scan stage has **12 tasks for 10 cores** (one per file, each smaller than the 83 MB split size = 830 MB / 10 cores). Docker: **10 tasks for 4 cores**, because the 128 MB split size packs the 12 files into 8 single-file tasks and 2 two-file tasks (7.47 M and 7.05 M records). Every shuffle downstream is coalesced by AQE, to 1 task for the tiny ones (M1, M2, M3: 200 → 1) and 9–11 for O1/O3. 146 tasks per run locally, 96 on Docker.
- **Spill: only O1's window stage.** 456.4 MiB to disk locally (10 of 11 tasks, ≈ 47 MB each), 431.7 MiB on Docker (8 of 9 tasks, ≈ 57–61 MB each). No other stage spills.
- **GC: not a bottleneck.** 3.4–4.3 % of executor run time locally, 4.9–5.0 % on Docker. The worst single stage is 7.7 % (Docker's M1 scan), under the 10 % threshold.
- **CPU ÷ run time:** 0.69–0.74 locally, 0.90 on Docker. The work is CPU-bound, not I/O-bound. Inside the scan stages, the Parquet reader's `scan time` is only 2.3–4.7 s of 20–56 s of task time per stage. The rest is the fused filter / casts / projections / partial aggregates (`accums` in `eventlog-*.txt`).
- **O1 window.** `Exchange hashpartitioning(PULocationID, hour_of_day, 200)` writes and reads **498.8 MiB** in both environments; AQE coalesces it to 11 tasks locally and 9 on Docker. **No skew:** max/median task = 1.03 locally (10 tasks of 4.05–4.37 M rows plus one of 1.74 M) and 1.56 on Docker. There, records are even (5.14–5.28 M rows, plus one of 2.46 M); the 1.56 comes from the first wave of tasks running slower (4.5–5.0 s) than the second (3.0–3.2 s), not from data.
- **O3 self-join.** The chosen join is **`SortMergeJoin LeftOuter`** on (`PULocationID`, `hour_index`) in both environments (final plans). Its sides are the current side, re-shuffled after the window (**64.2 MiB** written locally, 62.3 MiB on Docker), and the lag side, the shifted grid (**24.5 MiB**, 22.4 MiB). The self-join stage reads 88.7 / 84.7 MiB. **`ReusedExchange` appears twice**, for the trip aggregate's and the grid's exchanges, so the trip scan runs once for O3. But the zero-fill SortMergeJoin above them runs **twice**, once per self-join side: two stages each read 32.9 MiB / 3,661,330 records locally (33.3 MiB on Docker), concurrently.
- **Skew anywhere:** the max/median task ratio is ≤ 1.37 locally and ≤ 1.63 on Docker, never near 10.

**The leads, checked against the measurements:**

| lead | verdict | evidence |
|---|---|---|
| L1 file count sets scan parallelism | **confirmed → H3** | local-02 stage 1: 10 tasks end at 3.7–4.6 s, then 2 tasks run alone from 3.7 to 5.4 s; docker-02 stage 1: waves of 4 + 4 + 2, the last 2 (the two-file tasks) running 5.7 → 8.5 s on 2 of 4 cores |
| L2 200 shuffle partitions for tiny shuffles | **confirmed harmless** | AQE coalesces each to 1 task (M1 stage 3, M2 stage 9, M3 stages 17–35: 0.07–0.45 s each). No hypothesis |
| L3 repeated criteria in M1 | **not separable** | M1's scan stage is slower than M2's (5.49 vs 4.19 s local), but it also has the `regexp_extract`s and 9 aggregates; folded into H4 |
| L4 recomputation | **confirmed → H1** | 829,973,299 B and 48,722,602 records read in each of the 5 jobs |
| L5 borough skew | **settled** | M2 max skew 1.10–1.22; the partial aggregate absorbs it |
| L6 write layout | no issue | M2 writes 6 files (`number of written files`), in a 0.28–0.41 s stage |
| L7 `_metadata` built and dropped | **folded into H4** | `file_path` etc. are in every trip `FileScan`; only M1 and M3 compute `regexp_extract` on it |
| L8 M3 window order costs a shuffle | **confirmed, not worth it** | 5 `Exchange`s in `plans/m3.txt`; M3's 5 post-scan stages take 0.56 s together locally and 1.0 s on Docker, and dropping one saves ≈ 0.1–0.4 s (≤ 0.7 %), inside the noise. Not a hypothesis |
| L9 scan-stage task skew 10.5 (2025-01) | **not reproduced on 2025** | scan skew 1.04–1.15 locally, ≤ 1.58 on Docker (two-file tasks) |
| L10 O1 window shuffles and sorts every trip | **confirmed → H2** | 498.8 MiB shuffle, 456.4 MiB spill, 17.9 % / 15.5 % of wall |
| L11 O1 skew latent, hidden by AQE | **still latent** | skew 1.03 / 1.56 at defaults; measuring it needs `coalescePartitions.enabled=false`, a conf change: leave it for the scalability section |
| L12 O3 self-join is a SortMergeJoin | **confirmed → H6** | `SortMergeJoin LeftOuter`, sides 64.2 + 24.5 MiB, stage 1.33 s / 2.84 s |
| L13 O3 zero-fill join runs twice | **confirmed → H5** | two concurrent stages each reading 32.9 MiB / 3,661,330 records |
| L14 window and self-join want different partitioning | **confirmed, part of H6** | the 64.2 MiB re-shuffle by (`PULocationID`, `hour_index`) after the window by (`PULocationID`, `day_of_week`, `hour_of_day`) |
| L15 one month isn't representative | **consistent** | on 2025 both grid joins are SortMergeJoins, as predicted; the baseline is 2025 |

**The bounds overlap.** H1, H3 and H4 all act on the same five scan stages (61–64 % of wall), so their upper bounds can't be added. Each one is measured on top of whatever has landed before it.

### H1 [M2, M3, O1, O3] The trip Parquet is scanned and cleaned five times
- Evidence: `size of files read` = 829,973,299 B and 48,722,602 records in each of the 5 jobs (local-20261105-02, docker-20261105-02, event log). The 5 scan stages take 25.25 s of 41.19 s (61.3 %) locally and 36.98 s of 58.28 s (63.5 %) on Docker (runs 02–07 / 02–04). Plan: a `FileScan parquet` (12 paths) under every operation, with no `InMemoryTableScan`.
- Hypothesis: nothing is cached, so each operation re-runs the read, `enforceSchema`, the `_metadata` projection and the five-criterion filter on all 48.7 M rows.
- Candidate remedy: `persist(StorageLevel.MEMORY_AND_DISK)` the cleaned trips once (before M2), and `unpersist` after the last operation. M1 has to read the raw rows, so it keeps its scan, and M2's scan fills the cache.
- Expected: the scan stages of M3, O1 and O3 read 0 Parquet records (an `InMemoryTableScan` instead), and their duration falls. **Upper bound ≈ 37.8 % local / 38.0 % Docker** (those three scan stages: 15.57 s / 22.13 s). The real gain is lower, because the stages also do each operation's own first step, and the cache read and cache build aren't free.
- Risk: Docker has 1 GiB executors, so the cache will partly go to disk (check the Storage tab / `memoryBytesSpilled`). Verify results identical (M2's tolerance rule).

### H2 [O1] The bucket window shuffles, sorts and spills every trip
- Evidence: `Exchange hashpartitioning(PULocationID, hour_of_day, 200)` writes and reads 498.8 MiB. The window stage spills **456.4 MiB to disk locally** (431.7 MiB on Docker) in 10 of 11 tasks (8 of 9 on Docker). It takes **7.36 s = 17.9 %** of wall locally and **9.05 s = 15.5 %** on Docker. Runs local-02…07 / docker-02…04, stage 43; plan: `Sort → Window [avg, stddev_samp … RowFrame unbounded]` over the trip-sized exchange. No skew (max/median 1.03 / 1.56).
- Hypothesis: the window has no partial aggregation, so all 43.6 M cleaned trips cross the shuffle and are sorted per partition, and the sort buffers spill.
- Candidate remedy (L10): `groupBy(PULocationID, hour_of_day)` for count / mean / σ (≤ 6,312 rows, with a partial aggregate before a tiny shuffle), then `broadcast()` the bucket statistics back onto the trips.
- Expected: O1's shuffle write goes from 498.8 MiB to < 1 MiB, its spill from 456 MiB to 0, and the window stage disappears. **Upper bound ≈ 17.9 % local / 15.5 % Docker.**
- Caveat: the stats side and the trip side each scan the trips unless H1 has landed, and an extra scan costs ≈ 4.1 s / 6.5 s (O1's scan stage). So without H1 the net gain is at most ≈ 8 % local / 4.4 % Docker. **Measure H2 after H1.** Flags and counts must stay exact; mean and σ within the 10⁻¹² relative tolerance.

### H3 [all scans] Idle cores in each scan stage's last wave
- Evidence:
  - Local: 12 file-sized tasks on 10 cores. In local-02 stage 1, 10 tasks finish at 3.7–4.6 s, then 2 run alone until 5.4 s.
  - Docker: 10 tasks on 4 cores (8 single files plus 2 two-file tasks of 7.47 M / 7.05 M records). Waves of 4 + 4 + 2; the last 2 run 5.7 → 8.5 s on 2 of 4 cores (docker-02 stage 1).
  - Idle-core loss, summed over the 5 scan stages (stage duration − Σ task run time ÷ cores): **5.39 s = 13.1 %** local (runs 02–07, range 5.31–5.76) and **7.40 s = 12.7 %** Docker (02–04).
  - Each file has 4–5 row groups of ≈ 17.5 MB / 1,048,576 rows, 53 in all (`row-groups.txt`), so smaller splits are possible.
- Hypothesis: split granularity is a whole file (local) or up to two files (Docker), too coarse for the core count, so the tail of every scan stage runs on a fraction of the cores.
- Candidate remedy: `spark.sql.files.maxPartitionBytes` ≈ 18 MB, so that each task reads about one row group: ≈ 53 scan tasks, 5.3 per core locally and 13 per core on Docker.
- Expected: scan-stage task count 12 / 10 → ≈ 53, scan-stage duration → close to Σ task run time ÷ cores, and the idle-core loss → near 0. **Upper bound ≈ 13.1 % local / 12.7 % Docker**, less the per-task overhead of 4–5× more tasks.
- Note: O1's window stage shows the same last-wave tail (local 11 tasks on 10 cores, Docker 9 on 4, ≈ 1 s each), but that's AQE's coalescing target, a different knob: not part of this hypothesis.

### H4 [M1, M3] The per-row `regexp_extract` on `_metadata.file_path` (needs a diagnostic first)
- Evidence: the same scan of the same input costs more in the two operations that compute `source_year`/`source_month`:
  - M1: 5.49 s local / 8.46 s Docker; M3: 7.47 s / 10.66 s.
  - Against M2: 4.19 s / 6.39 s; O1: 4.10 / 6.46; O3: 4.00 / 5.01.
  - Only `plans/m1.txt` and `plans/m3.txt` contain `regexp_extract` (2 per row, on a per-row path string).
  - The excess over M2 is 4.58 s local (11.1 %) and 6.34 s Docker (10.9 %).
- **Confounded:** M1 also has 9 conditional counts (L3), and M3 has 2 × `stddev_samp` + 2 × `avg`. Nothing measured so far separates the regex from the aggregates.
- Hypothesis: two regex evaluations per row on a 100-character path, for 48.7 M rows per scan.
- Candidate remedy: derive year and month once per file instead of once per row (e.g., `substring` on `_metadata.file_name` at fixed offsets, or one read per file with `lit()` values), keeping D4's semantics.
- Before the OPT, `spark-optimizer` should measure the regex alone (the same scan with and without the two columns) to see whether H4 holds.
- Expected: M1 and M3 scan stages move towards M2's. **Upper bound ≈ 11 %** in both environments, and likely much less once the aggregate cost is separated.

### H5 [O3] The zero-fill SortMergeJoin is computed twice
- Evidence: both self-join sides reuse the trip-aggregate and grid exchanges (`ReusedExchange` ×2), but not the joined grid. Two stages each read 32.9 MiB / 3,661,330 records and run the same `SortMergeJoin LeftOuter` (local-02 stages 58, 59; docker-02 the same). The lag-side copy takes **2.13 s = 5.2 %** local and **3.74 s = 6.4 %** Docker (medians). It runs concurrently with the other copy, competing for the same cores.
- Hypothesis: `hourly` (the zero-filled grid) has two consumers, and Spark recomputes it per consumer.
- Candidate remedy: `persist` the zero-filled `hourly` grid (2.3 M rows × 3 columns), so that both self-join sides read it, then `unpersist`. The self-join itself stays, as the spec requires.
- Expected: one zero-fill stage instead of two, and O3's post-scan time (5.05 s / 8.71 s) falls by up to the second copy. **Upper bound ≈ 5.2 % local / 6.4 % Docker.** On 4 cores the copies compete harder, so expect more of the bound on Docker.

### H6 [O3] The 4-week-lag self-join is a SortMergeJoin with an extra re-shuffle
- Evidence: `SortMergeJoin LeftOuter` on (`PULocationID`, `hour_index`) in both final plans. The current side is re-shuffled after the window (64.2 MiB local / 62.3 MiB Docker, L14), the lag side is 24.5 / 22.4 MiB, and the join stage reads 88.7 / 84.7 MiB. That stage takes **1.33 s = 3.2 %** local and **2.84 s = 4.9 %** Docker (medians).
- Hypothesis: Catalyst estimates the lag side at 122 MB (O3 observation), over the 10 MB broadcast threshold, so it shuffles and sorts both sides, although the real lag side is 2.3 M rows × 3 columns.
- Candidate remedy: `broadcast()` hint on the lag side. That keeps the self-join (spec), removes its sort-merge and the current side's re-shuffle, and leaves the join in the window's stage.
- Expected: O3 loses one exchange (64.2 MiB) and the join stage, with `BroadcastHashJoin LeftOuter BuildRight` in the plan. **Upper bound ≈ 3.2 % local / 4.9 % Docker.**
- Risk: the hashed relation of 2.3 M rows has to fit in the 1 GiB Docker driver and executors; check the broadcast size and GC.

**Order for the loop:** H1 → H2 (after H1, see its caveat) → H3 → H5 → H6. H4 needs its diagnostic first. Measure each against the baseline above, or against the previous kept OPT, with the Part C protocol. Use Docker for single-operation effects under about 20 %, because local per-operation spread reaches 22 %.

### Methodology notes from the baseline
- **The profiler's `input_bytes` is wrong for Parquet scans.** Each scan task reports `Bytes Read` ≈ 11 KB (e.g., 11,224 B for a 3,574,091-record task, local-02 stage 1), while `Records Read` is right. The bytes really read are in the scan's SQL metric `size of files read` (event log), 829,973,299 B per job. Use records read or that SQL metric, never `input_bytes`, in the report. This confirms the M3 observation's "metric to check". Fixing the profiler is a separate change.
- **Stage durations overlap.** Stages run concurrently (O3's scan with the grid generation, and its two zero-fill stages together), so the per-stage durations in `stages.json` add up to more than the wall-clock (42.1 s vs 39.7 s, local-02). Shares above use the critical path or the operation's wall-clock, not a sum of stages.
- **Locally, the first wave of a stage is slower than later waves** (local-02 stage 1: 3.7–4.5 s for 10 concurrent tasks, 1.6–1.8 s for the 2 that ran alone). That's contention between 10 concurrent tasks (memory bandwidth, and probably the M5's mix of performance and efficiency cores; not verified). It inflates local skew ratios slightly and is one reason local runs vary more than Docker.
- **Stage ids aren't stable on Docker for O3** (window stage 61 or 63, join stage 67 or 69). Compare stages by role, not by id.

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

### OPT-1 (H1) [M2, M3, O1, O3] Persist the cleaned trips
- Status: **reverted** (measured 2026-11-05, `benchmark-runner`, commit 0d5657b: a regression in both environments; applied by `spark-optimizer` on HEAD 98578d6 + the uncommitted diff; branch `perf/persist-cleaned-trips`)
- Revert: the `persist`/`unpersist` code was removed again on the same branch, so `runPipeline` is back to the baseline's. The `--skip-opts` toggle stays for later OPTs, but the `OPT1` id is gone (`--skip-opts opt1` is now rejected). The measured code is commit **0d5657b**, which stays reachable through the branch's PR after the squash merge.
- Change: `cleaned.persist(StorageLevel.MEMORY_AND_DISK())` right after `removeInvalidTrips()`, and `cleaned.unpersist()` after the last operation that reads it, both in `runPipeline` (`src/main/kotlin/com/radeshgovind/Pipeline.kt:57-60`). The cache is used only when two or more of M2, M3, O1 and O3 are selected (`isCleanedCacheWorthIt`, `Pipeline.kt:65-69`), because a single reader would only pay to build it. M1 keeps its own scan of the raw trips (it counts the invalid rows), so M2's scan fills the cache (with `--ops` without M2, the first reader selected does). The cache holds all 13 enforced columns, `source_year`/`source_month` included, so M3 reads them from the cache too. The four writes moved unchanged into `writeCleanedOperations` (`Pipeline.kt:72`).
- Toggle: **`--skip-opts opt1`** switches it off (the baseline code path). The default, no flag, is OPT-1 on. It's a comma-separated list of OPT ids for later OPTs: `Optimisation` enum (`cli/Optimisation.kt`), parsed in `cli/ParseArgs.kt:27,31,55,81`, `PipelineArgs.skippedOptimisations` / `isApplied()` (`cli/PipelineArgs.kt:12,16`), and recorded in `run.json` as `args.skip_opts` (`"opt1"` or `""`, `cli/PipelineArgs.kt:25`). Tests: `ParseArgsTest` (default, valid list and case, unknown ids, empty or missing value). README → Usage has the flag. No `spark.*` setting changed.
- Plan (2025-01, final plans `isFinalPlan=true`, runs local-20261105-81 off / -82 on, throwaway runs folder; [`observations/opt1-20261105/`](observations/opt1-20261105/README.md)):
  - M3, O1, O3: `ColumnarToRow → FileScan parquet` **→ `TableCacheQueryStage → InMemoryTableScan → InMemoryRelation`** (`plans/{off,on}-local-20261105-8{1,2}/{m3,o1,o3}.txt`, `plan-nodes.txt`). The `FileScan parquet` still printed in these plans is the `InMemoryRelation`'s cached plan, shown as an inner child, not a scan that runs.
  - M2 builds the cache: `InMemoryTableScan → InMemoryRelation [13 columns], StorageLevel(disk, memory, deserialized, 1 replicas) → … FileScan parquet` (`plans/on-local-20261105-82/m2.txt`). The cache is filled by its own scan stage (stage 6, 3,475,226 Parquet records), then M2's aggregate reads it (stage 8, 322 cached batches). That adds a stage to M2: 3 → 4 stages, 12 → 22 tasks.
  - Event log (`eventlog-local-20261105-8{1,2}.txt`): the scan's `size of files read` in the M3, O1 and O3 executions goes from **59,170,569 B to 12,331 B** (only the zone CSV), and their scan stages read **0 Parquet records** (322 cached batches instead of 3,475,226 records). Each cache scan reads 174,182,779 B of cached blocks.
  - **Side effect on O3:** over the `InMemoryRelation`, O3's initial plan has 3 `SortMergeJoin`s instead of 0. AQE demotes all three to `BroadcastHashJoin` at runtime, but their shuffles stay (hash exchanges 2 → 7, stages 7 → 12, shuffle read 4.9 → 16.1 MiB on 2025-01). The lag side's trip aggregate is no longer reused (`ReusedExchange` 2 → 1), so O3 scans the cache twice (stages 55, 56). The cause is probably the cache's size estimate (all 13 columns); not verified. On 2025 the baseline O3 already uses `SortMergeJoin` for both grid joins (L15), so the 2025 effect may differ.
- Correctness (2025-01, `output/opt1-off/<op>` vs `output/opt1-on/<op>`, `spark-shell` in the Docker master, `local[2]`; [`observations/opt1-20261105/correctness.txt`](observations/opt1-20261105/correctness.txt)):
  - Rows off = on: M1 1, M2 815, M3 257, O1 52, O3 195,672. Schemas equal.
  - `exceptAll` empty both ways for all five (whole rows, so the doubles match bit for bit).
  - A null-safe full outer join on every non-fractional column (integers, strings, flags, dates exact) matches 1:1, and every double is within the relative tolerance of 1e-12. The largest relative difference is 0.0, so the tolerance wasn't needed.
  - Negative control: a 1e-9 relative bump of one O1 rate and +1 on one M3 count are both caught.
  - O3's output files are 1.9× larger on disk (941,907 → 1,765,302 B, same rows): the final join now streams from a hash exchange, so the row order inside the files changes.
- Predicted metric: Parquet records read in the M3, O1 and O3 scan stages → 0, and their `size of files read` → the zone CSV only (seen on 2025-01, above). Expect M3/O1/O3 to get shorter and M2 longer (it builds the cache).
- Risk for `benchmark-runner`:
  - **Cache size on 2025.** A linear extrapolation of 2025-01's 174.2 MB (3,203,825 cleaned rows) to 43.6 M rows gives ≈ 2.4 GB. That's an estimate, not a measurement. Docker's executors have 1 GiB each, at most ≈ 434 MiB of unified memory per executor ((1024 − 300) × 0.6, shared with execution), or ≈ 870 MiB for the 2 workers. So expect most of the cache on disk and possible eviction. Report the cached RDD's memory vs disk size, and any eviction. The event log doesn't hold them at default settings (stage `RDD Info` shows 0 cached partitions; no block-update events). Read them from the Spark UI's Storage tab or `/api/v1/applications/<app>/storage/rdd` before the run ends, or turn on `spark.eventLog.logBlockUpdates.enabled` as instrumentation and list it in the README.
  - **O3 on 2025:** check `ReusedExchange` and the number of cache scans in `plans/o3.txt`, because O3 may lose more than the scan saves.
  - The cache also stores `source_year`/`source_month` for every row, so M2's scan now computes the two `regexp_extract`s (H4) it used to prune.
- Before (OPT-1 off, `--skip-opts opt1`, same jar, same session; sha 0d5657b):
  - local: local-20261105-09, -11, -13, -15 (n = 4) → **25.54 s [24.90–27.05]**, spread 8.4 %. m1 4.40 · m2 3.00 · m3 4.88 · o1 7.56 · o3 5.81 s (medians).
  - Docker, 2 workers: docker-20261105-06, -08, -10 (n = 3) → **34.21 s [33.82–34.80]**, spread 2.9 %. m1 6.21 · m2 4.46 · m3 7.21 · o1 8.49 · o3 7.66 s.
- After (OPT-1 on, no flag):
  - local: local-20261105-10, -12, -14, -16 (n = 4; warm-up -08 discarded) → **33.61 s [31.92–38.91]**, spread 20.8 % (run 14 is an outlier; unexplained). m1 4.54 · m2 13.11 · m3 1.27 · o1 7.55 · o3 7.02 s.
  - Docker: docker-20261105-07, -09, -11 (n = 3; warm-up -05 discarded) → **48.64 s [46.91–48.73]**, spread 3.7 %. m1 5.91 · m2 19.70 · m3 2.53 · o1 10.09 · o3 9.94 s.
- Δ wall-clock (median, total of the five blocks): **local +8.07 s (+31.6 %)**, ranges disjoint (on min 31.92 > off max 27.05); **Docker +14.43 s (+42.2 %)**, ranges disjoint. Per operation, local / Docker: m2 +10.11 s (+338 %) / +15.24 s (+342 %); m3 −3.61 s (−74 %) / −4.68 s (−65 %); o1 −0.01 s (−0.1 %, within noise) / +1.60 s (+18.8 %); o3 +1.21 s (+20.8 %) / +2.28 s (+29.8 %); m1 +0.14 s / −0.30 s (noise, M1 is unchanged). Tables: `observations/opt1-20261105/bench/summary-{local,docker}.txt`; per-run CSV `analysis/opt1-20261105-wall.csv`.
- Δ predicted metric (**moved as predicted**, event log, `bench/eventlog-*.txt`, `bench/scanrecs.txt`): in the M3, O1 and O3 jobs the scan stage reads **0 Parquet records** (it reads 4,365 cached batches locally, 4,364 on Docker; 322 on 2025-01) and the scan's `size of files read` falls from **829,985,630 B to 12,331 B** (the zone CSV only), in every `on` run, both environments. M2 reads 48,722,602 Parquet records to fill the cache, then the 4,36x batches again. The M3 scan stage drops from 4.21 s to 0.67 s locally and from 6.32 s to 1.43 s on Docker (medians), which is the whole M3 gain. The predicted scan savings are real, but the cost of filling the cache is larger: M2's cache-filling scan stage takes 10.54 s locally and 15.11 s on Docker (medians), against 2.56 s and 3.93 s for the baseline M2 scan, and M2 gains one stage and 12 / 10 tasks (26 vs 14, 22 vs 12). Net, per the per-op deltas above: M3 gains 3.61 s / 4.68 s, O1 gains nothing, M2 and O3 lose 11.32 s / 17.52 s, so the total is a loss of +8.07 s / +14.43 s (M1 ±0.3 s).
- Other metrics (medians, off → on): stages 24 → 26; tasks local 146 → 170, Docker 96 → 115 (115–116); shuffle read unchanged (689.9 MiB local, 683.3 → 682.9 MiB Docker); shuffle write +16.0 MiB local (656.9 → 672.9) and +15.5 MiB Docker (650.0 → 665.5), all from O3; disk spill local 456.4 → 369.8 MiB [317.6–384.6] and Docker 431.9 → 418.9 MiB, still all in O1's window stage except O3 on Docker (6.8 MiB [5.5–8.3] disk, 458 MiB [367–666] memory spill, new); GC % of executor run time local 3.4 → 4.2, Docker 5.0 → 5.8.
- **Cache size, memory vs disk** (storage REST API, last non-empty poll before unpersist; `bench/storage/*.json`). The cached RDD (13 columns, `MEMORY_AND_DISK`) is **fully cached and never evicted** in every polled run: `numCachedPartitions` = `numPartitions` (12/12 local, 10/10 Docker), and the M3, O1 and O3 scans never went back to Parquet.
  - **local** (10 cores, driver heap not tuned): memory 1,501–1,721 MiB + disk 558–777 MiB (5 polled runs: 08, 10, 12, 14, 16), i.e. about 2.2–2.3 GiB in total; roughly 70 % in memory, the rest spilled to disk. Memory is the deserialized size and disk the serialized size, so they don't add up to one clean figure.
  - **Docker, 2 executors × 1 GiB** (455,501,414 B = 434 MiB of storage memory each): **memory 0, disk 4,828–5,689 MiB** (docker-05, 07, 09, 11; per executor 2.3–3.6 GB). Nothing fits in memory: each of the 10 partitions is 0.2–1.1 GB serialized, more than an executor's storage memory. So on Docker every cache read is a disk read and every build is a disk write. That is the cost of M2's +15.2 s, and it makes O1's and O3's cache scans no faster than the Parquet scan they replace (O1 scan stage 3.46 s → 3.63 s, o3's two cache scans 3.72 s and 6.04 s).
  - The earlier estimate in the Risk bullet (≈ 2.4 GB) fits the local figure; Docker's disk size is 2× that (serialized form).
- **O3 on 2025** (`bench/o3-plan-nodes.txt`, `plans/o3.txt` in local-09/10, docker-06/07, final plans): **worse, not just flat.** Wall +1.21 s (+20.8 %) local and +2.28 s (+29.8 %) Docker; stages 8 → 9, tasks 76 → 88 local, 36 → 45 Docker; shuffle write +16.0 / +15.5 MiB. The join strategy didn't change on 2025 (3 `SortMergeJoin` and 7 hash `Exchange` in the final plan and in the initial plan, both off and on; the 2025-01 jump from 0 to 3 `SortMergeJoin` doesn't recur because the baseline already picks `SortMergeJoin`). What changed is `ReusedExchange` 2 → 1: the lag side's trip aggregate is no longer reused, so O3 scans the cache twice (`TableCacheQueryStage` / `InMemoryTableScan` 2 → 4 in the plan text; stages 55 and 56, 12 tasks each) instead of once, and the extra scan stage is paid on top of an already-fast baseline. On Docker one of those cache scans takes 6.04 s (disk).
- Verdict: **reverted** (a regression of +31.6 % local and +42.2 % Docker, outside noise in both; the predicted metric moved, but only M3 gains, and M2's cache build plus O3's second cache scan cost more than M3's gain; on Docker the cache lives entirely on disk). H1 as remedied here (cache the whole 13-column relation with `MEMORY_AND_DISK`) is refuted for these environments. Not tried here, because each is a different change: persisting only the columns the downstream ops need, `MEMORY_ONLY_SER` or Kryo, or giving the Docker executors more memory (a `spark.*` change, so a different experiment); `spark-optimizer` decides whether any of them clears the bar.
- **Why the same-session `off` runs beat the logged baseline by ≈ 40 %** (checked after the measurement, `run.json`): the same work took about a third less CPU time. M1's scan stage (12 tasks, identical plan and records) used 32.6 s of executor CPU in local-20261105-02 and 21.4 s in local-20261105-09; on Docker, 22.3 s in docker-20261105-02 and 14.0 s in docker-20261105-06. Less CPU time for the same instructions means each core ran faster in the later session, so the difference is the machine's state (load from other processes, or the cores the threads landed on), not the code. `pmset` recorded no thermal or low-power state. Consequence: the logged baseline is not a valid "before" for any OPT; every OPT is compared against its own same-session `--skip-opts` runs, and the baseline should be re-measured on a quiet machine before the report quotes it.
- Context only, against the logged baseline (different session; see the note under the Runs table): local 41.19 s (02…07) → on 33.61 s (−18.4 %), but the same-session off is 25.54 s; Docker 58.28 s (02…04) → on 48.64 s (−16.5 %), same-session off 34.21 s. Don't read the −18 % / −17 % as an OPT-1 gain: the off path in the same session is faster still.
