# 02 — M1: ingestion and cleaning (Phase 1b)

> Follows [`01-SKELETON-AND-PROFILER.md`](01-SKELETON-AND-PROFILER.md) Part A, which is merged. Every rule below is fixed. The data facts and expected numbers were measured on the downloaded 2025 files on 2026-11-05 (see §1 and §7).

**Branch:** `git switch -c feat/m1-ingestion`
**Time:** about 1.5–2 h.

**Goal:** turn the raw trips into one fixed schema, decide per trip whether it's valid under the spec's five criteria, write the per-year cleaning statistics, and hand M2 and M3 a clean `Dataset<Row>`.

```
loadTrips ─► withSourceMonth ─► (sample) ─► enforceSchema ─┬─► cleaningStats ─► output/m1/   (M1's output)
                                                           └─► removeInvalidTrips ─► M2, M3   (next roadmaps)
```

---

## 1. Facts about the data (measured, not assumed)

| Fact | Value | Consequence |
|---|---|---|
| Schema across the 12 files of 2025 | identical | One `read().parquet(all files)` is safe for 2025 |
| Timestamp type | `timestamp_ntz` (no time zone) | **Keep it `TIMESTAMP_NTZ`.** See decision D3. |
| `passenger_count`, `RatecodeID`, `payment_type` | `bigint` | Cast to `INT`, as the spec says |
| `PULocationID`, `DOLocationID` | `int`, range 1–265 | 264 and 265 are "Unknown" / "N/A" in the zone table, so criterion 5 removes them |
| Null `passenger_count` (and `RatecodeID`) | 540,149 of 3,475,226 in Jan (15.5 %), 11,611,894 of 48,722,602 in all of 2025 (23.8 %) | Decision D1 |
| Nulls in the other 9 spec columns | 0 in Jan | Still handled null-safely (D2) |
| Trips dated outside their file's month | 214 of 48.7 M in 2025, 29 of them outside 2025 (years 2007–2009 and 2024) | Decision D4 |
| Value extremes in Jan | fare down to −900, distance 0, passengers 0–9 | All caught by the criteria |

## 2. Decisions (fixed; copy them into README → M1 and the report)

**D1. A null `passenger_count` is not invalid.**
- The spec's criterion is "`passenger_count` = 0 or > 6", and null is neither.
- Dropping nulls would remove 23.8 % of 2025, a quarter of the data, for a column that M2 and M3 don't even use.
- These trips stay. The stats count them in a separate, informational column, `null_passenger_count`.

**D2. Every other null fails the criterion that tests it.**
- A null fare, distance, timestamp or zone ID makes the trip unusable, so it counts as invalid under that criterion.
- **Each criterion is a never-null boolean:** wrap it in `coalesce(…, lit(false))` after the explicit `isNull` checks.
- Why this matters: Spark's `filter(not(x))` **drops** rows where `x` is null. A null-producing criterion would silently remove the 540,149 null-passenger trips, and the stats would never count them.

**D3. Timestamps stay `TIMESTAMP_NTZ`, the NYC wall-clock time exactly as stored.**
- Casting to `TIMESTAMP` would attach the **session time zone**. That's Europe/Lisbon on your Mac and UTC in Docker.
- Around Lisbon's daylight-saving switch, some NYC times don't exist in Lisbon, so local and Docker results could differ.
- With NTZ, `hour()` and `dayofweek()` give NYC local time everywhere.
- For later years, older files may store `TIMESTAMP`. When those years are added, cast them to `TIMESTAMP_NTZ` in `enforceSchema` and check the hours against a known trip.

**D4. A trip's year and month come from its source file, not from its pickup date.**
- `source_year` and `source_month` come from the file name (`yellow_tripdata_2025-03.parquet` gives 2025 and 3).
- The 214 stray-dated trips stay in their file's month. They're 0.0004 % of the data, and the spec lists five criteria, not six.
- The stats then have exactly one row per requested year, and M3's "month" is well defined.
- How: `_metadata.file_path`, Spark's hidden per-file column, read **right after the scan**, before sampling.

**D5. The criteria overlap and are counted independently.**
- A trip with fare 0 and distance 0 counts under both `bad_fare` and `bad_distance`.
- `removed_trips` counts trips with at least one problem, so the per-criterion columns can add up to more than `removed_trips`.
- Say this in the report.

**D6. All criteria are computed in one pass.**
- The stats are one `groupBy(source_year).agg(…)` holding every count, not five `count()` actions.
- Each action is a separate Spark job re-reading all the input, so five counts would mean five full scans.

## 3. The five criteria (exact definitions)

`InvalidTrip`: one enum entry per criterion. The same list drives the filter and the stats, so the two can never disagree.

| Entry | Stat column | Condition (true = invalid; never null) |
|---|---|---|
| `BAD_FARE` | `bad_fare` | `fare_amount IS NULL OR fare_amount <= 0` |
| `BAD_DISTANCE` | `bad_distance` | `trip_distance IS NULL OR trip_distance <= 0` |
| `BAD_PASSENGER_COUNT` | `bad_passenger_count` | `coalesce(passenger_count = 0 OR passenger_count > 6, false)`. Null means valid (D1). |
| `BAD_TIME_ORDER` | `bad_time_order` | `pickup IS NULL OR dropoff IS NULL OR dropoff <= pickup` |
| `BAD_ZONE` | `bad_zone` | `PU IS NULL OR DO IS NULL OR PU NOT BETWEEN 1 AND 263 OR DO NOT BETWEEN 1 AND 263` |

Wrap every condition in `coalesce(…, lit(false))`. Only `BAD_PASSENGER_COUNT` strictly needs it, but wrapping all five makes "never null" an invariant rather than something to re-check per criterion.

## 4. Files

```
src/main/kotlin/com/radeshgovind/
├── cli/PipelineArgs.kt          ← (change) add fun outputOf(op: Operation): Path = output.resolve(op.name.lowercase())
├── ingest/
│   ├── TripColumns.kt           ← new: the 11 spec column names + SOURCE_YEAR, SOURCE_MONTH, as constants
│   ├── LoadTrips.kt             ← (change) read → withSourceMonth() → sampledTo(fraction)
│   └── EnforceSchema.kt         ← new: Dataset<Row>.enforceSchema()
├── clean/
│   ├── InvalidTrip.kt           ← new: enum of the 5 criteria (stat column + condition)
│   ├── RemoveInvalidTrips.kt    ← new: Dataset<Row>.removeInvalidTrips()
│   └── CleaningStats.kt         ← new: Dataset<Row>.cleaningStats()
└── Pipeline.kt                  ← (change) M1 write + summary; reportTripsRead() removed

src/test/kotlin/com/radeshgovind/
├── ingest/EnforceSchemaTest.kt  ← new
└── clean/CleaningTest.kt        ← new
```

`clean` is a new feature package. Add it to the package list in AGENTS.md → Code Conventions.

## 5. The functions

**`ingest/LoadTrips.kt`:** source columns first, then sampling. `_metadata` only resolves directly on the file scan.
```kotlin
fun SparkSession.loadTrips(files: TripFiles.Found, fraction: Fraction): Dataset<Row> =
    read().parquet(*paths(files)).withSourceMonth().sampledTo(fraction)

private fun Dataset<Row>.withSourceMonth(): Dataset<Row> =
    withColumn(SOURCE_YEAR, fileNamePart(1)).withColumn(SOURCE_MONTH, fileNamePart(2))

private fun fileNamePart(group: Int): Column =
    regexp_extract(col("_metadata.file_path"), SOURCE_FILE_PATTERN, group).cast("int")

private const val SOURCE_FILE_PATTERN = """yellow_tripdata_(\d{4})-(\d{2})\.parquet"""
```
The regex matches anywhere in the path, so it works both for real files and for the Parquet directories the tests write (`…/yellow_tripdata_2025-01.parquet/part-0000.parquet`).

**`ingest/EnforceSchema.kt`:** select exactly 13 columns in this order, each cast once:

| Column | Type |
|---|---|
| `tpep_pickup_datetime`, `tpep_dropoff_datetime` | `TIMESTAMP_NTZ` |
| `PULocationID`, `DOLocationID`, `passenger_count`, `RatecodeID`, `payment_type` | `INT` |
| `trip_distance`, `fare_amount`, `tip_amount`, `total_amount` | `DOUBLE` |
| `source_year`, `source_month` | `INT` (already int; listed so the order is fixed) |

```kotlin
fun Dataset<Row>.enforceSchema(): Dataset<Row> = select(*ENFORCED.map(::castColumn).toTypedArray())

private fun castColumn(column: EnforcedColumn): Column = col(column.name).cast(column.type).alias(column.name)
// EnforcedColumn(name: String, type: DataType): a private data class in the same file, with ENFORCED the list above
```
- Use `DataTypes.TimestampNTZType`, `DataTypes.IntegerType` and `DataTypes.DoubleType`.
- Every column the spec doesn't need is dropped here. Parquet then skips them on disk: check `ReadSchema` in the plan, which should list only these 11 columns plus `_metadata`.

**`clean/InvalidTrip.kt`:**
```kotlin
enum class InvalidTrip(val statColumn: String) {
    BAD_FARE("bad_fare") { override fun condition(): Column = isNullOrAtMostZero(FARE_AMOUNT) },
    BAD_DISTANCE("bad_distance") { override fun condition(): Column = isNullOrAtMostZero(TRIP_DISTANCE) },
    BAD_PASSENGER_COUNT("bad_passenger_count") { override fun condition(): Column = isZeroOrMoreThanSix(PASSENGER_COUNT) },
    BAD_TIME_ORDER("bad_time_order") { override fun condition(): Column = isMissingOrNotAfterPickup() },
    BAD_ZONE("bad_zone") { override fun condition(): Column = isMissingOrOutsideZones(PU_LOCATION_ID).or(isMissingOrOutsideZones(DO_LOCATION_ID)) };

    abstract fun condition(): Column    // true = invalid, never null

    companion object {
        fun anyInvalid(): Column = entries.map(InvalidTrip::condition).reduce(Column::or)
    }
}
// private helpers below, each wrapped in neverNull(c) = coalesce(c, lit(false))
```
Conditions are functions, not stored `Column`s. A `Column` is a cheap expression and needs no Spark session, but functions keep the enum free of construction-order surprises.

**`clean/RemoveInvalidTrips.kt`:**
```kotlin
fun Dataset<Row>.removeInvalidTrips(): Dataset<Row> = filter(not(InvalidTrip.anyInvalid()))
```

**`clean/CleaningStats.kt`:** one row per `source_year`, columns in exactly this order:

`source_year, raw_trips, bad_fare, bad_distance, bad_passenger_count, bad_time_order, bad_zone, removed_trips, kept_trips, null_passenger_count`

```kotlin
fun Dataset<Row>.cleaningStats(): Dataset<Row> =
    groupBy(SOURCE_YEAR).agg(rawTrips(), *criterionCounts(), removedTrips(), nullPassengerCounts())
        .withColumn(KEPT_TRIPS, col(RAW_TRIPS).minus(col(REMOVED_TRIPS)))
        .select(*STAT_COLUMNS)                        // fixes the order above
```
- `rawTrips()` is `count(lit(1)).alias("raw_trips")`.
- Each criterion is `sum(condition().cast("long")).alias(statColumn)`.
- `removedTrips()` is the same over `anyInvalid()`.
- `nullPassengerCounts()` is `sum(col(PASSENGER_COUNT).isNull().cast("long"))`.
- `agg(Column, Column...)` takes Java varargs: pass the first column, then spread the rest.
- **No `orderBy`:** it would add a sort, and a shuffle, to the plan for no reason. Sort only when printing.

**`cli/PipelineArgs.kt`:** add `fun outputOf(op: Operation): Path = output.resolve(op.name.lowercase())`, so M1 writes to `<output>/m1`.

**`Pipeline.kt`:**
```kotlin
fun runPipeline(spark: SparkSession, args: PipelineArgs, tripFiles: TripFiles.Found) {
    val trips = spark.loadTrips(tripFiles, args.fraction).enforceSchema()
    if (Operation.M1 in args.operations) writeCleaningStats(trips, args.outputOf(Operation.M1))
    // M2 and M3 (next roadmaps) start from: val cleaned = trips.removeInvalidTrips()
}

private fun writeCleaningStats(trips: Dataset<Row>, out: Path) {
    trips.cleaningStats().write().mode(SaveMode.Overwrite).parquet(out.toString())
    printCleaningStats(trips.sparkSession(), out)
}

// Reads back the few written rows: cheap, and it doesn't re-run the cleaning.
private fun printCleaningStats(spark: SparkSession, out: Path) {
    spark.read().parquet(out.toString()).orderBy(SOURCE_YEAR).show(false)
}
```
- `reportTripsRead()` goes, along with its `count()`.
- **Don't add `removeInvalidTrips()` to the pipeline yet.** A Dataset nothing writes is never computed, and an unused value only looks like work. It arrives with M2.
- The `SaveMode.Overwrite` write is the profiled action later: Part B wraps it in `profile("m1") { … }`.

## 6. Tests

**Spark in tests:** each test class creates its own session in `@BeforeAll` and stops it in `@AfterAll` (`org.junit.jupiter.api`, a `companion object` with `@JvmStatic`). Use `local[2]`, `spark.ui.enabled=false`, and `spark.sql.shuffle.partitions=2` so tests stay fast. That last one is test-only and never set in the pipeline.

### `clean/CleaningTest.kt`

Fixture: 12 rows built with `spark.createDataFrame(rows, schema)`, with the **enforced** schema, `source_year = 2025`, `source_month = 1`.

The baseline valid trip:
- fare 10.0, distance 2.0, passengers 1
- pickup `2025-01-15T10:00`, dropoff `2025-01-15T10:20` (`LocalDateTime`)
- PU 100, DO 200

Each row changes only what its line says:

| # | Change | Expected flags |
|---|---|---|
| 1 | none | valid |
| 2 | passengers `null` | valid (D1), counted in `null_passenger_count` |
| 3 | fare 0.0 | bad_fare |
| 4 | fare −5.0 | bad_fare |
| 5 | distance 0.0 | bad_distance |
| 6 | passengers 0 | bad_passenger_count |
| 7 | passengers 7 | bad_passenger_count |
| 8 | dropoff = pickup | bad_time_order |
| 9 | dropoff 1 min before pickup | bad_time_order |
| 10 | PU 264 | bad_zone |
| 11 | DO 0 | bad_zone |
| 12 | fare 0.0 **and** distance 0.0 | bad_fare + bad_distance (overlap, D5) |

Tests:
- **`removeInvalidTrips keeps exactly the valid and the null-passenger trip`:** count = 2. The null-passenger row survives, which proves D2's `coalesce` works. Without it, this test fails with count = 1.
- **`cleaningStats counts each criterion independently`:** exactly one row: `source_year=2025, raw=12, bad_fare=3, bad_distance=2, bad_passenger_count=2, bad_time_order=2, bad_zone=2, removed=10, kept=2, null_passenger_count=1`. The criteria sum to 11 against removed = 10, which is the overlap.
- **`no criterion is ever null`:** for every `InvalidTrip`, `fixture.filter(condition().isNull()).count() == 0`.

### `ingest/EnforceSchemaTest.kt`

- Write 2 raw rows to `@TempDir/trips/yellow_tripdata_2025-03.parquet`, with the **raw** 2025 types: `passenger_count` as `bigint`, timestamps as `timestamp_ntz`, plus an extra column such as `Airport_fee`.
- Load them with `spark.loadTrips(TripFiles.Found(listOf(path)), Fraction.WHOLE).enforceSchema()`.
- Assert:
  - the schema is exactly the 13 names and types of §5, in order
  - `source_year` = 2025 and `source_month` = 3 on every row
  - `Airport_fee` is gone

## 7. Done when

```bash
make check
```
```bash
make run ARGS="--input data --output output --years 2025-01"
```
It prints this table, and every number must match. They were computed independently in PySpark from the same rules on 2026-11-05:

| source_year | raw_trips | bad_fare | bad_distance | bad_passenger_count | bad_time_order | bad_zone | removed_trips | kept_trips | null_passenger_count |
|---|---|---|---|---|---|---|---|---|---|
| 2025 (Jan only) | 3,475,226 | 145,516 | 90,893 | 24,674 | 2,051 | 28,357 | 271,401 | 3,203,825 | 540,149 |

```bash
make run ARGS="--input data --output output --years 2025"
```

| source_year | raw_trips | bad_fare | bad_distance | bad_passenger_count | bad_time_order | bad_zone | removed_trips | kept_trips | null_passenger_count |
|---|---|---|---|---|---|---|---|---|---|
| 2025 (full year) | 48,722,602 | 2,870,234 | 1,402,958 | 260,208 | 546,304 | 371,657 | 5,122,770 | 43,599,832 | 11,611,894 |

**If a number differs**, the likeliest cause is null handling:
- `kept_trips` too low by about 540 k (Jan) means the null passengers were dropped. D2's `coalesce` is missing somewhere.
- `bad_zone` off means `between` was applied to the wrong column.

Then on Docker. `make jar` first; no container recreate is needed unless you ran `make clean`:
```bash
docker compose -f docker/docker-compose.yml exec spark-master /opt/spark/bin/spark-submit --class com.radeshgovind.MainKt --master spark://spark-master:7077 /jars/shard-all.jar --input /data --output /output --years 2025-01
```
It must print the same Jan row.

**Also check the plan once.** Temporarily add `trips.cleaningStats().explain("formatted")` before the write, and don't commit it. Two things to confirm:
- the `FileScan parquet` `ReadSchema` lists only the 11 spec columns, which shows the extra columns are pruned
- there's one `HashAggregate → Exchange → HashAggregate` pair, which means one pass, not five

Note both in your M1 notes; they're report material for section 2 ("Pipeline").

> **Measured 2026-11-05 (2025-01, local, pre-execution plan):** pruning goes further than expected. `ReadSchema` holds **7** columns, not 11: `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `passenger_count`, `trip_distance`, `PULocationID`, `DOLocationID`, `fare_amount` (plus `_metadata.file_path`). Catalyst prunes to what the stats query reads, so `RatecodeID`, `payment_type`, `tip_amount`, `total_amount` and `source_month` are never read. The plan has exactly one `HashAggregate (partial) → Exchange hashpartitioning(source_year, 200) → HashAggregate (final)`, and `PushedFilters` is empty, as expected. The run logs show AQE coalescing the 200 shuffle partitions to 1.

**Docs:**
- **README:** a short **M1** subsection under Usage (or a new "Operations" section): decisions D1–D5 in one line each, and the `output/m1/` columns.
- **AGENTS.md:** add `clean/` to the Architecture Map and to the package list.
- **Roadmap:** tick 1b in `00-ROADMAP.md`.

Then `/update-repo main`.

---

## 8. What this sets up for the performance work

Leave all of this in the code as it is. These are the effects the baseline is meant to measure:

- **The stats and the cleaned trips come from the same scan, and nothing is cached.** When M2 and M3 arrive, each output re-reads and re-cleans the Parquet files. The profiler will show `input_bytes` ≈ dataset size per operation. That becomes hypothesis H1: persist the cleaned trips.
- **The aggregation shuffle uses the default `spark.sql.shuffle.partitions` (200).** AQE coalesces them, but check the task counts in the stats stage. That becomes a possible hypothesis on partition sizing.
- **The cleaning filter can't be pushed into Parquet.** The `coalesce`/`OR` expressions aren't simple pushdown predicates, so every row group is read. That's expected; mention it in the report's discussion rather than "fixing" it, because the filters must stay exactly the spec's.
