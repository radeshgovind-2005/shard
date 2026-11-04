# SHARD
Storage, Health, Automation, Reliability & Data-layout engineering for a distributed analytics pipeline.

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
- **Cluster:** Docker Spark environment and DI cluster details are pending. See [`docs/lab-inputs.md`](docs/lab-inputs.md).

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

**Dataset version.** The TLC republishes files from time to time, so `MANIFEST.tsv` is the version record: report the download dates and checksums of the files used. Years used in the experiments: TODO, once chosen.

**The schema changes across years.** Checked on 2009-01, 2010-06, 2011-01, 2015-01, 2024-01 and 2026-01:
- **2009–2010 have no `PULocationID`/`DOLocationID`.** They store raw pickup/dropoff longitude/latitude, with different column names (2009: `Trip_Pickup_DateTime`, `Fare_Amt`, `Start_Lon`…; 2010: `pickup_datetime`, `pickup_longitude`…) and timestamps stored as strings. The zone-based operations (M2, M3, O1–O4) can't use them without a geospatial lookup.
- **2011 onwards** has the TLC schema used by the spec, but the files drift: `PULocationID`, `DOLocationID` and `VendorID` are `int64` in the 2011 and 2015 files and `int32` in 2024 and 2026; `airport_fee` (2015) becomes `Airport_fee` (2024); `cbd_congestion_fee` is in 2026-01 but not in 2024-01. M1 has to enforce one schema across files.

The dry run sends one HEAD request per missing month. During setup, after about 250 requests in a few minutes, the CDN started answering 403. If downloads fail with 403, wait a few minutes and re-run.

## Quick Start

```sh
make setup
make data-sample   # zone lookup + one month (2025-01)
make check         # lint, build, tests (includes a local Spark smoke test)
make run           # currently the Hello Kotlin template; becomes the pipeline entry point
```

## Usage

**Local** (`local[*]`, Spark from Gradle):

```sh
make run ARGS="<pipeline arguments>"
```

**Cluster** (Docker Spark, DI cluster): build the fat jar (our code + Kotlin stdlib; Spark is provided by the cluster), then submit it:

```sh
make jar   # build/libs/shard-all.jar
spark-submit --class com.kognito.MainKt --master <master-url> build/libs/shard-all.jar <pipeline arguments>
```

Still to document, as the assignment requires:
- The pipeline's CLI arguments (input path(s), dataset scale, output path), once the pipeline exists
- The exact `spark-submit` command for the Docker Spark environment and the DI cluster (pending [`docs/lab-inputs.md`](docs/lab-inputs.md))
- How to reproduce each experiment in [`experiments/LOG.md`](experiments/LOG.md)

## Configuration

| Setting | Value | Where |
|---|---|---|
| Spark / Scala | 4.2.1 / 2.13 | `sparkVersion` in `gradle.properties` |
| JVM target | Java 21 (class file 65) | `jvmToolchain` in `build.gradle.kts` |
| JVM module flags for local runs and tests | Spark's `--add-opens` list (not `spark.*` settings; `spark-submit` adds them itself) | `sparkJvmArgs` in `build.gradle.kts` |

**Cluster topology** (workers, cores and memory per worker): local is an Apple M5 with 10 cores and 24 GB. Docker Spark and DI cluster are pending ([`docs/lab-inputs.md`](docs/lab-inputs.md)). All environments are recorded in the `experiments/LOG.md` environment table.

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
