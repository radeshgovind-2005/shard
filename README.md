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
- **O1 — Fare anomaly detection:** flags trips whose fare per km is above their `(pickup zone, hour)` bucket's mean + 3σ, then compares weekday vs weekend anomaly rates by payment type and borough. Bottleneck: **window shuffle and skew**. The bucket statistics are a window over every trip, so all trips are shuffled and sorted (≈ 0.5 GB for 2025, spilling), and the bucket sizes are skewed (3.1× rows per hash partition); AQE splits skewed joins, not windows, and only its partition coalescing currently evens the tasks out.
- **O3 — Demand forecasting features:** per `(zone, day_of_week, hour)` historical mean, variance and a 4-week lag, with a demand index and `service_zone`. Bottleneck: **join strategy**. The lag is a self-join too big to broadcast, so it runs as a SortMergeJoin, on top of a zero-filled hourly grid.

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
shard --input <dir> --output <dir> [--years <spec>] [--fraction <f>] [--ops <list>] [--skip-opts <list>] [--run-id <id> [--runs-dir <dir>] [--git-sha <sha>]]
```

| Flag | Required | Default | Meaning |
|---|---|---|---|
| `--input` | yes | – | folder holding `trips/` and `taxi_zone_lookup.csv` (`data` locally, `/data` in Docker) |
| `--output` | yes | – | folder for results (`output` locally, `/output` in Docker); each operation writes `<output>/<op>/` |
| `--years` | no | `2025` | `YYYY` (one year), `YYYY-YYYY` (whole years), or `YYYY-MM` (one month). 2011 onwards, because 2009–2010 have no zone IDs. |
| `--fraction` | no | `1.0` | share of trips to keep, 0 < f ≤ 1, sampled with a fixed seed (42), so the same fraction keeps the same rows every run |
| `--ops` | no | every operation (`m1,m2,m3,o1,o3`) | which results to write, comma-separated |
| `--skip-opts` | no | none (every applied optimisation on) | optimisations to switch off, comma-separated OPT ids (`opt1`), so before/after runs use the same jar. Recorded in `run.json` as `skip_opts`. `opt1`: don't persist the cleaned trips ([`experiments/LOG.md`](experiments/LOG.md) → Optimisations). |
| `--run-id` | no | none | turns profiling on: `<local\|docker\|di>-<yyyymmdd>-<nn>`, e.g. `local-20261105-01`. Never reuse one: a run folder that already exists exits with code 2. |
| `--runs-dir` | no | `experiments/runs` | where run folders go (`/runs` in Docker). Only with `--run-id`. |
| `--git-sha` | no | none | the commit that ran, recorded in `run.json`: pass `$(git rev-parse --short HEAD)`. Only with `--run-id`. |

Exit codes: `0` success, `2` bad arguments, missing input files or a reused run id (checked before Spark starts), `1` a failure while Spark runs. The input check covers every requested month's trip file and, when M2, M3, O1 or O3 is requested, `taxi_zone_lookup.csv`.

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

### M3: zone-level statistics

Starts from M1's cleaned trips and groups them by `PULocationID` and month: `trip_count`, `avg_fare`/`stddev_fare` (`fare_amount`) and `avg_distance`/`stddev_distance` (`trip_distance`). It then joins `pickup_borough` from the zone lookup and adds three window columns.

- **D8: the month is the source file's month** (D4), plus `month_index = source_year × 12 + source_month`. Every rolling window orders by `month_index` with `rangeBetween`, so a month without trips is a gap, never "the previous row". December 2024 → January 2025 is +1.
- **D9: aggregate first, then join.** The borough join (the same plain join as M2, `withPickupBorough`) and all three windows run on the zone-month rows (about 260 zones × 12 months for 2025), not on the trips. No trip is lost, so Σ `trip_count` equals M1's `kept_trips`.
- **D10: σ is the sample standard deviation** (`stddev_samp`). A zone-month with a single trip has `stddev_fare` and `stddev_distance` = null, not 0.
- **D11: `volume_rank`** is `rank()` within (`pickup_borough`, `month_index`) by `trip_count` descending. 1 = the busiest zone; equal counts share a rank and the next one is skipped (10, 3, 3 → 1, 2, 2).
- **D12: `trip_share_3m`** = `zone_trips_3m` / `borough_trips_3m`, both summed over months m−2..m (`rangeBetween(-2, 0)` on `month_index`). The borough total is one RANGE window over the borough's zone-month rows. A month the zone had no trips counts as 0. In the first two months of the range, both sides cover only 1–2 months, so the ratio is still consistent.
- **D13: `is_volume_anomaly`** = |`trip_count` − `baseline_mean_trips`| > 1.5 × `baseline_stddev_trips`, where the baseline is the zone's **previous 3 months** (`rangeBetween(-3, -1)`), excluding the current month. With n points including the current one, |x − mean| / s can't exceed (n − 1)/√n, which is 1.15 for n = 3 and 1.5 for n = 4, so an inclusive window that short could never flag. The flag is **null** when the baseline has fewer than 2 months (no σ): "not enough history", not "normal". A flat history (σ = 0) flags any change. Months without trips are skipped in the baseline, not counted as 0.
- **D14: what the flag can show on 2025 alone.** It catches abrupt changes against a zone's last quarter (events, closures, a collapse in volume). It can't separate seasonality (no previous year) or month length (February has 28 days, about −10 %), and a σ from 2–3 points is noisy, so expect false positives. The first month that can be flagged is March; full 3-month baselines start in April. Measured on 2025: 46 % of the rows with a baseline are flagged, mostly as city-wide swings (almost every zone in March against a January–February baseline, a downward August), so read a flag as "unusual for this zone given the last quarter", not as an event (LOG → Observations → M3). The count-based flag is kept on purpose, because it is what the spec asks for. A share-of-borough flag would cancel city-wide swings, but it would compute something else.

Output: `<output>/m3/` (Parquet, not partitioned; the spec asks for borough partitioning only in M2), one row per zone and month with at least one trip: `PULocationID, source_year, source_month, month_index, pickup_borough, trip_count, avg_fare, stddev_fare, avg_distance, stddev_distance, volume_rank, zone_trips_3m, borough_trips_3m, trip_share_3m, baseline_mean_trips, baseline_stddev_trips, is_volume_anomaly`. The run then prints per borough the trips, rows, flagged rows (`anomalies`) and rows without a baseline (`no_baseline`), with the total on the last row (`pickup_borough` = null).

### O1: fare anomaly detection

Starts from M1's cleaned trips. Each trip gets `fare_per_km`, its bucket's trip count, mean and σ, and a flag; then the borough join and one `groupBy` turn the flags into weekday vs weekend rates.

- **D15: fare per km** = `fare_amount / (1.609344 × trip_distance)`, because `trip_distance` is in miles. It's a built-in expression, not a UDF. M1 keeps only positive fares and distances, so there's no division by zero (an error under ANSI mode).
- **D16: the bucket is `(PULocationID, hour_of_day)`**, the pickup hour in NYC wall-clock time (D3), pooled over every month of the run: with `--years 2025`, one bucket per zone and hour for the whole year (at most 263 × 24 = 6,312).
- **D17: mean and sample σ** (`stddev_samp`, as D10) of `fare_per_km` over the whole bucket, the trip itself included: a window with `partitionBy(PULocationID, hour_of_day)` and no `orderBy`. A trip is flagged when `fare_per_km > mean + 3σ`: one-sided (only expensive trips, as the spec says "exceeds") and strict.
- **D18: no σ, no flag.** A bucket with one trip has σ = null, so `is_fare_anomaly` is null. That trip counts in `trips` but not in `flagged_trips`. A bucket where all trips are equal (σ = 0) flags nothing. **Samuelson's inequality:** with the sample σ, no trip can be more than (n − 1)/√n σ from its bucket's mean. That's 2.85 for n = 10 and 3.02 for n = 11, so **a bucket of 10 trips or fewer can never flag a trip**, however extreme its fare. On 2025 that's 418 of 6,102 buckets but only 1,788 trips, and none of them is flagged (LOG → Observations → O1).
- **D19: weekend** = a pickup on Saturday or Sunday (`dayofweek` ∈ {1, 7}) in NYC wall-clock time, midnight to midnight. A Friday 23:50 → Saturday 00:20 trip is a weekday trip. Day and hour come from M2's `withTripTimes()`. `hour()` and `dayofweek()` don't apply the session time zone to `TIMESTAMP_NTZ` (D7), and O1 uses no other time function.
- **D20: the rate table** has one row per `(day_type, payment_type, pickup_borough)` with at least one trip: `trips`, `flagged_trips` and `anomaly_rate = flagged_trips / trips`. The borough comes from the same plain join as M2 (`withPickupBorough`), after the flags. `payment_type` is the TLC code: 0 flex fare, 1 credit card, 2 cash, 3 no charge, 4 dispute, 5 unknown, 6 voided trip. A null code is its own row, so Σ `trips` equals M1's `kept_trips`.
- **D21: only the rate table is written.** Neither the 43 M flagged trips nor the per-bucket statistics: the latter would need a second action on the same uncached input (another scan and window shuffle), or the groupBy rewrite kept for the optimisation loop.
- **What the flag catches** (2025): 0.40 % of trips, against 0.135 % for a normal distribution and Cantelli's 10 % cap for any distribution. `fare_per_km` is heavy-tailed (p99.9 = 1,823 $/km, from near-zero distances), so a bucket's σ is typically 9 × its mean and **94.5 % of the flags are trips under 0.1 mile**. Read a flag as "implausibly short recorded distance for the fare", not as an overcharge. The spec's mean + 3σ is kept as written.
- **D22: the baseline is the plain window.** No `broadcast()`, `cache()` or repartitioning. Computing the bucket statistics with a `groupBy` and broadcasting them back would avoid shuffling every trip, but it's an optimisation, so it waits for a profiled run (LOG → L10, L11).

Output: `<output>/o1/` (Parquet, not partitioned, under 100 rows): `day_type, payment_type, pickup_borough, trips, flagged_trips, anomaly_rate`. The run then prints the whole table, and the trips, flagged trips and rate per day type, with the total on the last row (`day_type` = null).

### O3: demand forecasting features

Starts from M1's cleaned trips. It counts trips per pickup zone and wall-clock hour, `c(zone, hour)`, on a dense grid of zones × hours, then adds per (`PULocationID`, `day_of_week`, `hour_of_day`) bucket the mean and variance of `c`. It joins each hour to the same hour 4 weeks earlier (the spec's self-join) and derives a demand index and a spike flag. The last step annotates each row with the zone's `service_zone`.

- **D23: "current" is one (zone, date, hour) observation.** The output has one row per grid cell, with its count, its bucket's mean and variance (repeated on each of the bucket's rows), its own 4-week-lag count, the demand index and the flag.
- **D24: the hour index has no time zone.** `hour_index = unix_date(to_date(pickup)) × 24 + hour(pickup)`. On `TIMESTAMP_NTZ` both functions read the stored wall-clock fields, so nothing is cast to `TIMESTAMP` (unlike `timestamp_diff`, D7), and a Europe/Lisbon session gives the same hours as UTC, also across Lisbon's DST switches.
  - Every day has 24 hours. NYC's spring-forward hour (2025-03-09 02:00) is always 0, and its fall-back hour (2025-11-02 01:00) holds two real hours. The data stores no UTC offset, so this can't be fixed.
  - The grid covers the `--years` range. A trip whose pickup falls outside it (the stray-dated trips of D4) has no grid row and is left out, so Σ `trip_count` = M1 `kept_trips` − those trips.
- **D25: zero-fill.** The grid is zones 1–263 (every zone M1 keeps) × every hour of the range: 195,672 rows for 2025-01 and 2,303,880 for 2025. The hourly counts are left-joined onto it, and a missing count is 0. Averaging only the hours that had trips would give Σc / k_nonzero ≥ Σc / n, a mean biased upwards, most of all in quiet buckets, which is where a spike matters.
- **D26: the bucket's mean (`hist_mean_trips`) and sample variance (`hist_var_trips`, as D10) cover every week of the range, the current one included.** That gives one value per bucket, as the spec words it, from one window over (`PULocationID`, `day_of_week`, `hour_of_day`). Every bucket spans at least 4 weeks, so the variance is never null.
  - Caveats: this isn't a causal forecast, because it uses later weeks too.
  - The current count is part of the mean, so the demand index can't exceed n, the number of weeks: 4–5 on one month, 52–53 on 2025.
- **D27: the 4-week lag is a left self-join.** The zero-filled grid is joined with itself on `PULocationID` and `hour_index − 672` (28 days × 24 hours, so the same weekday and hour). It's a plain join, with no hint or configuration change: the baseline. An hour whose lag hour is before the range keeps its row, with `lag_4w_trip_count` = **null** ("no history"; 0 would be a made-up observation). An hour whose lag hour had no trips gets 0. Catalyst picks the join by estimated size: on 2025 it's a SortMergeJoin, but on 2025-01 the shifted grid is estimated just under the 10 MB broadcast threshold, so one month runs it as a broadcast join (LOG → Observations → O3).
- **D28: `demand_index` = `trip_count` / `hist_mean_trips`, and `is_demand_spike` = `demand_index` > 2.0.** When the mean is 0, the index is null (ANSI raises on a division by zero) and the flag is false. Because the mean includes the current hour (D26), a mean of 0 means the count is 0 too: no demand, so not a spike.
- **D29: `service_zone`** comes from a plain inner join with the zone lookup (no hint, like M2's borough join). The lookup has all of zones 1–263, so no row is lost.
- **D30: one month shows little.** On 2025-01, each bucket has 4–5 weeks, and only January 29–31 have a lag (18,936 of 195,672 rows). 2025 is the meaningful run: 52–53 weeks per bucket, with a lag on 2,127,144 of 2,303,880 rows. On 2025, 9.85 % of rows are flagged, 98 % of them in buckets averaging under 10 trips, where a ratio above 2 is mostly noise (one trip in a bucket with a mean under 0.5 is already a spike). The spec's ratio rule is kept as written, and the rate goes in the report's limitations.

Output: `<output>/o3/` (Parquet, not partitioned), one row per zone and hour of the range: `PULocationID, service_zone, pickup_date, day_of_week, hour_of_day, trip_count, lag_4w_trip_count, hist_mean_trips, hist_var_trips, demand_index, is_demand_spike`. `day_of_week` is 1 = Sunday … 7 = Saturday, as in M2. The run then prints per service zone the trips, rows, rows with a lag (`rows_with_lag`), flagged rows (`spikes`) and rows with a zero mean (`zero_mean`), with the total on the last row (`service_zone` = null).

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

**Non-default `spark.*` properties:** none for tuning yet. List each one here with the optimisation (OPT id) it belongs to. OPT-1 (persisting the cleaned trips) is a code change toggled by `--skip-opts opt1`, not a `spark.*` setting.

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
