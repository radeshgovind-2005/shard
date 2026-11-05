# SHARD
Storage, Health, Automation, Reliability & Data-layout engineering for a distributed analytics pipeline.
![SHARD — Big data. Measured performance.](docs/assets/shard_banner.png)
## Why / Features

SHARD is a large-scale analytics pipeline over the **NYC TLC Yellow Taxi trip records (2009–2026, Parquet)**, built on **Apache Spark** and written in **Kotlin against Spark's JVM (Scala) API**. It is the ComPar 26/27 Project 1 (NOVA FCT).

The focus isn't the analytics themselves. It's **performance engineering**: making the pipeline fast through a measurement-driven loop of *profile → hypothesise → optimise → measure → repeat*, where every optimisation is justified by a measured bottleneck and verified with before/after runs.

Operations:
- **M1 — Ingestion & cleaning:** schema enforcement, invalid-record filtering, per-year removal statistics
- **M2 — Temporal demand:** trips, mean fare, duration and distance per `(borough, day_of_week, hour)`, written partitioned by borough
- **M3 — Zone statistics:** monthly per-zone fare/distance stats, and window functions for volume rank, rolling 3-month share and deviation flags
- **Optional (≥ 2 of O1–O4):** TODO, choose and list them here, with the bottleneck each one exposes

Spec: [`docs/Project_Description.md`](docs/Project_Description.md).

## Requirements

- **JDK 21.** Gradle provisions it automatically through the toolchain resolver. Spark 4.2 supports Java 17/21/25 (not 24), and 21 matches what clusters typically run.
- **Apache Spark 4.2.1 (Scala 2.13).** No local install needed: Gradle fetches it for `make run` and the tests. `spark-submit` is only needed on a cluster, whose Spark version must match `sparkVersion` in `gradle.properties`.
- **Node.js** (LTS). Used only for the Git hooks (Husky + commitlint).
- **GNU Make** and **curl**
- **Disk:** about 0.8 GiB for one year of data, 30.2 GiB for all of 2009–2026 (plus room for Spark spill and outputs)
- **Cluster:** Docker (OrbStack or Docker Desktop) for the Spark cluster in [`docker/docker-compose.yml`](docker/docker-compose.yml). DI cluster details are pending ([`docs/lab-inputs.md`](docs/lab-inputs.md)).

## Installation

```sh
git clone https://github.com/radeshgovind-2005/shard.git
cd shard
make setup   # installs Git hooks (commit message + lint checks)
make build
```

### Dataset

[NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page), downloaded by `scripts/download-data.sh` from the TLC's CDN:
- Trips: `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet` (one file per month)
- Zones: `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv`

```sh
make data-size                 # size of the selection vs free disk, downloads nothing
make data                      # default selection: all of 2025 (12 files, 791.5 MiB)
make data YEARS=2011-2026      # a range; also YEARS="2024 2025" or a single month YEARS=2025-01
make data-sample               # one month (2025-01) for smoke tests
make data-clean YEARS=2009-2010   # delete files to free disk (YEARS=all for everything)
```

The script skips files already present (re-running resumes an interrupted download), skips months the TLC hasn't published yet, and refuses to start if the selection doesn't fit on disk with 2 GiB headroom.

**The input is read-only.** The pipeline only reads `data/` and writes its results to `output/`, so downloaded files can be deleted (`make data-clean`) and re-downloaded at will. `data-clean` requires an explicit `YEARS` and keeps `MANIFEST.tsv`, the download history that earlier runs refer to. Don't delete files during an experiment: before/after runs must read exactly the same input, and a re-download may differ if the TLC republished the file (compare checksums in `MANIFEST.tsv`).

Layout:

```
data/                          # gitignored; the pre-commit hook also blocks it
├── taxi_zone_lookup.csv
├── trips/yellow_tripdata_YYYY-MM.parquet
└── MANIFEST.tsv               # per file: bytes, sha256, UTC download time, URL
```

**Dataset version.** The TLC republishes files from time to time, so `MANIFEST.tsv` is the version record: report the download dates and checksums of the files used. The development year is **2025**: 12 files, downloaded 2026-11-04 (see `experiments/LOG.md` → Environment). The years used for the final experiments are recorded there with each run.

**The schema changes across years.** Checked on 2009-01, 2010-06, 2011-01, 2015-01, 2024-01 and 2026-01:
- **2009–2010 have no `PULocationID`/`DOLocationID`.** They store raw pickup/dropoff longitude/latitude, with different column names (2009: `Trip_Pickup_DateTime`, `Fare_Amt`, `Start_Lon`…; 2010: `pickup_datetime`, `pickup_longitude`…) and timestamps stored as strings. The zone-based operations (M2, M3, O1–O4) can't use them without a geospatial lookup.
- **2011 onwards** has the TLC schema used by the spec, but the files drift: `PULocationID`, `DOLocationID` and `VendorID` are `int64` in the 2011 and 2015 files and `int32` in 2024 and 2026; `airport_fee` (2015) becomes `Airport_fee` (2024); `cbd_congestion_fee` is in 2026-01 but not in 2024-01. M1 has to enforce one schema across files.

The dry run sends one HEAD request per missing month. During setup, after about 250 requests in a few minutes, the CDN started answering 403. If downloads fail with 403, wait a few minutes and re-run.

## Quick Start

```sh
make setup
make data-sample   # zone lookup + one month (2025-01)
make check         # lint, build, tests (includes a local Spark smoke test)
make run ARGS="--input data --output output --years 2025-01"   # writes output/m1/ and prints the cleaning stats
```

## Usage

### Pipeline arguments

```
shard --input <dir> --output <dir> [--years <spec>] [--fraction <f>] [--ops <list>] [--run-id <id> [--runs-dir <dir>] [--git-sha <sha>]]
```

| Flag | Required | Default | Meaning |
|---|---|---|---|
| `--input` | yes | – | folder holding `trips/` and `taxi_zone_lookup.csv` (`data` locally, `/data` in Docker) |
| `--output` | yes | – | folder for results (`output` locally, `/output` in Docker); each operation writes `<output>/<op>/` |
| `--years` | no | `2025` | `YYYY` (one year), `YYYY-YYYY` (whole years), or `YYYY-MM` (one month). 2011 onwards, because 2009–2010 have no zone IDs. |
| `--fraction` | no | `1.0` | share of trips to keep, 0 < f ≤ 1, sampled with a fixed seed (42), so the same fraction keeps the same rows every run |
| `--ops` | no | `m1,m2,m3` | which results to write, comma-separated |
| `--run-id` | no | none | turns profiling on: `<local\|docker\|di>-<yyyymmdd>-<nn>`, e.g. `local-20261105-01`. Never reuse one: a run folder that already exists exits with code 2. |
| `--runs-dir` | no | `experiments/runs` | where run folders go (`/runs` in Docker). Only with `--run-id`. |
| `--git-sha` | no | none | the commit that ran, recorded in `run.json`: pass `$(git rev-parse --short HEAD)`. Only with `--run-id`. |

Exit codes: `0` success, `2` bad arguments, missing input files or a reused run id (checked before Spark starts), `1` a failure while Spark runs.

### Profiling

With `--run-id`, each operation's write runs inside `profiler.profile("<op>") { … }`. A `SparkListener` records every job, stage and task of that block, using the metric names of the Lab 3 profiler (roadmap 01 B3) plus per-stage task skew. Without `--run-id`, the block just runs, so the measured code path is the same either way. Everything lands in `<runs-dir>/<run-id>/`:

| File | What |
|---|---|
| `run.json` | `run_id`, `git_sha`, `env`, `master`, `spark_version`, `args`, `spark_conf` (explicitly set only), `started_utc`, and `runs`: one entry per operation with `wall_time_s`, `num_jobs`/`num_stages`/`num_tasks`, run and CPU time, input/output/shuffle bytes, spill, GC time, and its stages |
| `stages.json` | every completed stage, flat, tagged with its `profile`: duration, the same byte/time/spill fields, `task_ms_median`, `task_ms_max`, `skew_ratio` (max / median; ≥ 10 is skew) |
| `plans/<op>.txt` | the physical plan after the write ran, so AQE's real choices show (`isFinalPlan=true`) |
| `eventlog/` | Spark's event log, for the History Server and DAG screenshots (gitignored) |

The run also prints one summary line per operation: `profile | wall s | stages | tasks | shuffle R | shuffle W | spill | max skew`. Skipped stages (shuffle output reused) aren't counted, which matches the Spark UI's Completed Stages.

```sh
make run ARGS="--input data --output output --years 2025-01 --run-id local-20261105-01 --git-sha $(git rev-parse --short HEAD)"
```

On Docker, add `--runs-dir /runs` (the master mounts `experiments/runs/` there). How runs are set up and compared is in roadmap 01 Part C, and every run gets a row in [`experiments/LOG.md`](experiments/LOG.md).

### M1: ingestion and cleaning

Every trip is cast once to the 11 spec columns (plus `source_year` and `source_month`). A trip is invalid if it fails any of the spec's five criteria: `fare_amount ≤ 0`, `trip_distance ≤ 0`, `passenger_count` = 0 or > 6, dropoff ≤ pickup, or a `PULocationID`/`DOLocationID` outside 1–263. Decisions:

- **D1:** a null `passenger_count` is valid, because it's neither 0 nor > 6. That's 23.8 % of 2025. These trips are kept and counted in `null_passenger_count`.
- **D2:** every other null fails the criterion that tests it. Each criterion is a never-null boolean, so no trip is dropped without being counted.
- **D3:** timestamps stay `TIMESTAMP_NTZ` (NYC wall-clock time as stored), so local and Docker runs agree whatever the session time zone.
- **D4:** a trip's year and month come from its source file name, not its pickup date. The 214 stray-dated trips in 2025 stay in their file's month.
- **D5:** the criteria overlap and are counted independently, so the per-criterion counts can add up to more than `removed_trips`.

Output: `<output>/m1/` (Parquet), one row per source year: `source_year, raw_trips, bad_fare, bad_distance, bad_passenger_count, bad_time_order, bad_zone, removed_trips, kept_trips, null_passenger_count`. The run also prints it.

### M2: temporal demand

Starts from M1's cleaned trips and joins the zone lookup on `PULocationID` = `LocationID` to get `pickup_borough` (a plain inner join, no hint). It then groups by `pickup_borough, day_of_week, hour_of_day` and computes `trip_count`, `avg_fare` (`fare_amount`), `avg_duration_min` and `avg_distance` (`trip_distance`).

- **Day and hour** come from the pickup, in NYC wall-clock time (D3). `day_of_week` is Spark's `dayofweek`: **1 = Sunday, 2 = Monday, …, 7 = Saturday**. A trip from Sunday 23:50 to Monday 00:20 counts at Sunday, hour 23.
- **D7: duration is computed without any time zone.** `duration_min = (dropoff − pickup)` in seconds, divided by 60.0. Subtracting two `TIMESTAMP_NTZ` values gives an interval with no time zone involved. `timestamp_diff()` would cast both sides to `TIMESTAMP` in the session time zone first, so local (Europe/Lisbon) and Docker (UTC) runs could disagree around Lisbon's DST switches. Caveat: a trip spanning NYC's own DST switch is measured in wall-clock time, so it's off by an hour. The data stores no UTC offset, so this can't be fixed.
- **No trips are lost in the join.** M1 keeps only zones 1–263, and the lookup holds every one of them, so the sum of `trip_count` equals M1's `kept_trips`. `Unknown` (264) and `N/A` (265) never appear. The boroughs are EWR, Bronx, Brooklyn, Manhattan, Queens and Staten Island.

Output: `<output>/m2/` (Parquet), partitioned by borough (`<output>/m2/pickup_borough=<name>/`), one row per borough, day and hour that had at least one trip (EWR has only 43 of the 168 in January): `day_of_week, hour_of_day, trip_count, avg_fare, avg_duration_min, avg_distance`, plus `pickup_borough` from the folder name. The run then prints the trips and rows per borough, with the total on the last row (`pickup_borough` = null).

### Local

`local[*]`, with Spark coming from Gradle:

```sh
make run ARGS="--input data --output output --years 2025-01"
make run ARGS="--input data --output output --years 2025 --fraction 0.1"
```

The Spark UI is at http://localhost:4040 while a job runs. If the Docker cluster is up, it holds ports 4040–4050, so the local UI moves to the next free port, usually **4051**. The log line `Successfully started service 'SparkUI' on port …` says which.

### Docker Spark cluster

A standalone Spark 4.2.1 / Java 21 cluster ([`docker/docker-compose.yml`](docker/docker-compose.yml)). The fat jar holds our code and the Kotlin stdlib; the cluster provides Spark. Run everything from the repo root:

```sh
make jar                                                                  # build/libs/shard-all.jar
docker compose -f docker/docker-compose.yml up -d --scale spark-worker=2  # master UI: http://localhost:8080
docker compose -f docker/docker-compose.yml exec spark-master /opt/spark/bin/spark-submit \
  --class com.radeshgovind.MainKt --master spark://spark-master:7077 \
  /jars/shard-all.jar --input /data --output /output --years 2025-01
docker compose -f docker/docker-compose.yml exec spark-master /opt/spark/bin/spark-submit \
  --class com.radeshgovind.MainKt --master spark://spark-master:7077 \
  /jars/shard-all.jar --input /data --output /output --years 2025-01 \
  --runs-dir /runs --run-id docker-20261105-01 --git-sha $(git rev-parse --short HEAD)   # profiled
docker compose -f docker/docker-compose.yml down                          # stop; nothing is lost
```

- **Mounts:** `data/` → `/data` (read-only, on master and workers), `build/libs/` → `/jars`, `output/` → `/output`, and `experiments/runs/` → `/runs` (master only, because the driver writes the run folder). Recreate the containers after pulling this mount: `up -d --force-recreate --scale spark-worker=2`.
- **Where things run:** the driver runs in the master container (client mode), and its job UI is at http://localhost:4040.
- **Changing the cluster size:** re-run `up -d` with another `--scale spark-worker=N`.
- **If a submit hangs on `Initial job has not accepted any resources`,** the master has lost its workers. This happens when the Mac sleeps: OrbStack pauses the containers, the master drops the workers after 60 s without a heartbeat, and they never register again. The master UI shows 0 workers, and its log says `Got heartbeat from unregistered worker`. Fix it with `down`, then `up -d --scale spark-worker=2`.
- **After `make clean`, recreate the containers** with `up -d --force-recreate --scale spark-worker=2`. `make clean` deletes `build/`, and a running container keeps pointing at the deleted folder, so `/jars/shard-all.jar` disappears (the error is `Failed to load class com.radeshgovind.MainKt`). `make jar` alone is fine.

### DI cluster

Pending ([`docs/lab-inputs.md`](docs/lab-inputs.md)). It takes the same jar and arguments, with that cluster's `--master`.

How to reproduce each experiment: every run is logged with its exact arguments in [`experiments/LOG.md`](experiments/LOG.md).

## Configuration

| Setting | Value | Where |
|---|---|---|
| Spark / Scala | 4.2.1 / 2.13 | `sparkVersion` in `gradle.properties` |
| JVM target | Java 21 (class file 65) | `jvmToolchain` in `build.gradle.kts` |
| JVM module flags for local runs and tests | Spark's `--add-opens` list (not `spark.*` settings; `spark-submit` adds them itself) | `sparkJvmArgs` in `build.gradle.kts` |

**Cluster topology** (workers, cores and memory per worker):
- **Local:** an Apple M5 with 10 cores and 24 GB.
- **Docker Spark:** N workers (default 2), each offering 2 cores / 2 GB. Spark's default `spark.executor.memory` is 1g, so each worker runs one executor with 2 cores and 1024 MiB. The image is `spark:4.2.1-scala2.13-java21-ubuntu`, running on OrbStack on the same M5.
- **DI cluster:** pending ([`docs/lab-inputs.md`](docs/lab-inputs.md)).

All environments are recorded in the `experiments/LOG.md` environment table.

**Non-default `spark.*` properties:** none for tuning yet. List each one here with the optimisation (OPT id) it belongs to.

| Property | Value | Why (OPT id) |
|---|---|---|
| `spark.eventLog.enabled` | `true`, **on only for profiled runs** (`--run-id`) | instrumentation, not tuning: the History Server and DAG screenshots |
| `spark.eventLog.dir` | `<runs-dir>/<run-id>/eventlog` (absolute), **on only for profiled runs** | same |

## Contributing

Full workflow and conventions: [`AGENTS.md`](AGENTS.md).

- Branch: `<type>/<kebab-description>`, e.g. `feat/m2-temporal-demand`
- Commits and PR titles: [Conventional Commits](https://www.conventionalcommits.org/), title only, e.g. `perf(o2): broadcast zone lookup`
- PRs are squash-merged into `main` after CI passes (`make check` + commitlint)
- Run `make check` before pushing (the `pre-push` hook does it too)
- Performance changes need evidence: a logged hypothesis plus before/after runs in `experiments/LOG.md`
