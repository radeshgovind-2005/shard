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

- **JDK 24.** Gradle provisions it automatically through the toolchain resolver.
- **Node.js** (LTS). Used only for the Git hooks (Husky + commitlint).
- **GNU Make**
- **Apache Spark:** TODO, version and Scala binary version (2.13) once the dependency is added
- **Cluster:** TODO, Docker Spark environment and DI cluster details

## Installation

```sh
git clone https://github.com/radeshgovind-2005/shard.git
cd shard
make setup   # installs Git hooks (commit message + lint checks)
make build
```

**Dataset.** Don't commit it. It goes in `data/` (gitignored, and the pre-commit hook blocks it).
- Source: [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page): monthly `yellow_tripdata_YYYY-MM.parquet` files, plus `taxi_zone_lookup.csv`
- TODO: exact download URLs, the years used, the download date (dataset version), and a download script

## Quick Start

TODO, once the pipeline runs: the shortest path from a fresh clone to one result, e.g. a smoke test on one month in `local[*]` mode.

## Usage

TODO. The assignment requires all of the following for reproducibility:
- CLI arguments: input path(s), dataset scale (one year / all years / fraction for smoke tests), output path
- The **exact `spark-submit` command** for each environment (local, Docker Spark, DI cluster)
- How to reproduce each experiment in [`experiments/LOG.md`](experiments/LOG.md)

## Configuration

TODO:
- Spark version
- Cluster topology: number of workers, cores and memory per worker
- **Every non-default `spark.*` property**, and which optimisation it belongs to

## Contributing

Full workflow and conventions: [`AGENTS.md`](AGENTS.md).

- Branch: `<type>/<kebab-description>`, e.g. `feat/m2-temporal-demand`
- Commits and PR titles: [Conventional Commits](https://www.conventionalcommits.org/), title only, e.g. `perf(o2): broadcast zone lookup`
- PRs are squash-merged into `main` after CI passes (`make check` + commitlint)
- Run `make check` before pushing (the `pre-push` hook does it too)
- Performance changes need evidence: a logged hypothesis plus before/after runs in `experiments/LOG.md`
