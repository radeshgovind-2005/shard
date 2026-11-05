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
make run ARGS="--input data --output output --years 2025-01"   # prints the trip count (M1–M3 still to come)
```

## Usage

### Pipeline arguments

```
shard --input <dir> --output <dir> [--years <spec>] [--fraction <f>] [--ops <list>]
```

| Flag | Required | Default | Meaning |
|---|---|---|---|
| `--input` | yes | – | folder holding `trips/` and `taxi_zone_lookup.csv` (`data` locally, `/data` in Docker) |
| `--output` | yes | – | folder for results (`output` locally, `/output` in Docker); each operation writes `<output>/<op>/` |
| `--years` | no | `2025` | `YYYY` (one year), `YYYY-YYYY` (whole years), or `YYYY-MM` (one month). 2011 onwards, because 2009–2010 have no zone IDs. |
| `--fraction` | no | `1.0` | share of trips to keep, 0 < f ≤ 1, sampled with a fixed seed (42), so the same fraction keeps the same rows every run |
| `--ops` | no | `m1,m2,m3` | which results to write, comma-separated |

Exit codes: `0` success, `2` bad arguments or missing input files (checked before Spark starts), `1` a failure while Spark runs.

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
docker compose -f docker/docker-compose.yml down                          # stop; nothing is lost
```

- **Mounts:** `data/` → `/data` (read-only, on master and workers), `build/libs/` → `/jars`, `output/` → `/output`.
- **Where things run:** the driver runs in the master container (client mode), and its job UI is at http://localhost:4040.
- **Changing the cluster size:** re-run `up -d` with another `--scale spark-worker=N`.
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

**Non-default `spark.*` properties:** none yet. List each one here with the optimisation (OPT id) it belongs to.

| Property | Value | Why (OPT id) |
|---|---|---|

## Contributing

Full workflow and conventions: [`AGENTS.md`](AGENTS.md).

- Branch: `<type>/<kebab-description>`, e.g. `feat/m2-temporal-demand`
- Commits and PR titles: [Conventional Commits](https://www.conventionalcommits.org/), title only, e.g. `perf(o2): broadcast zone lookup`
- PRs are squash-merged into `main` after CI passes (`make check` + commitlint)
- Run `make check` before pushing (the `pre-push` hook does it too)
- Performance changes need evidence: a logged hypothesis plus before/after runs in `experiments/LOG.md`
