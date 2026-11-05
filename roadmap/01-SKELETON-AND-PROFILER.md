# 01 — Pipeline skeleton (Phase 1a) and the profiler (Phase 2)

> Follows [`00-ROADMAP.md`](00-ROADMAP.md). Every name, flag, path and rule below is fixed, so you can build it top to bottom without re-deciding anything. API names were checked against the Spark 4.2.1 jars on 2026-11-05.

**Order:** Part A (skeleton) now → M1, M2, M3 (`00-ROADMAP.md` 1b–1d) → Part B (profiler) → Part C (baseline). Part B only needs Part A's action points, so a teammate can build it in parallel with M1–M3.

---

# Part A — the skeleton (Phase 1a)

**Goal:** a program that parses its arguments, opens a Spark session (local or cluster), loads the selected trip files, and runs one visible action. M1–M3 plug into it later without changing its shape.

**Branch:** `git switch -c feat/pipeline-skeleton`
**Time:** about 1–1.5 h.

## A1. File layout

```
src/main/kotlin/com/radeshgovind/
├── Main.kt                     ← moved from src/main/kotlin/Main.kt (package stays com.radeshgovind)
├── Pipeline.kt                 ← runPipeline(spark, args): the story; the only place with actions
├── SparkSessions.kt            ← createSparkSession()
├── cli/
│   ├── PipelineArgs.kt         ← data class with the parsed arguments
│   ├── ParseArgs.kt            ← parseArgs(Array<String>): ArgsParse (sealed)
│   ├── MonthRange.kt           ← which months to read, parsed from --years
│   ├── Fraction.kt             ← value class, 0 < f ≤ 1
│   └── Operation.kt            ← enum M1, M2, M3 (O1–O4 added later)
└── ingest/
    ├── TripFiles.kt            ← MonthRange → TripFiles.Found(paths) | TripFiles.Missing(paths)
    ├── LoadTrips.kt            ← SparkSession.loadTrips(files, fraction)
    ├── LoadZones.kt            ← SparkSession.loadZones(input), explicit schema
    └── ZoneColumns.kt          ← zone-table column names as constants

src/test/kotlin/com/radeshgovind/
├── SparkSessionTest.kt         ← exists already
├── cli/ParseArgsTest.kt
├── cli/MonthRangeTest.kt
└── ingest/TripFilesTest.kt
```

`mainClass` in `build.gradle.kts` stays `com.radeshgovind.MainKt`. Moving the file into the package folder doesn't change that name.

## A2. The command line (fixed)

```
shard --input <dir> --output <dir> [--years <spec>] [--fraction <f>] [--ops <list>] [--run-id <id>]
```

| Flag | Required | Default | Meaning |
|---|---|---|---|
| `--input` | yes | – | folder holding `trips/` and `taxi_zone_lookup.csv` (`data` locally, `/data` in Docker) |
| `--output` | yes | – | folder for results (`output` locally, `/output` in Docker). Each operation writes `<output>/<op>/`, overwriting. |
| `--years` | no | `2025` | which months to read (format below) |
| `--fraction` | no | `1.0` | share of trips to keep, e.g. `0.1` for the 10–20 % scale run |
| `--ops` | no | `m1,m2,m3` | which results to write, comma-separated, case-insensitive |
| `--run-id` | no | none | turns profiling on (Part B), e.g. `local-20261105-01` |

**`--years` formats.** These are checked in this order, and anything else is an error:

| Input | Regex | Means |
|---|---|---|
| `2025` | `^\d{4}$` | 2025-01 … 2025-12 |
| `2024-2025` | `^\d{4}-\d{4}$` | 2024-01 … 2025-12 (first ≤ second) |
| `2025-03` | `^\d{4}-\d{2}$` | that one month (month 01–12) |

The second part tells the year range and the single month apart: 4 digits means years, 2 digits means a month.

**Rules:**
- Years before **2011** are rejected, because 2009–2010 have no zone IDs.
- Repeating a flag is an error, and so is an unknown flag or a flag with no value.
- `--fraction` must be a number with 0 < f ≤ 1.
- `--ops` values must be `m1`, `m2` or `m3`.

**Exit codes:**
- `0`: success.
- `2`: bad arguments or missing input files. Print a one-line reason and the usage line to stderr.
- `1`: anything that fails while Spark runs (uncaught exception).

## A3. Types (the edge of the program)

```kotlin
data class PipelineArgs(
    val input: Path,            // java.nio.file.Path
    val output: Path,
    val months: MonthRange,
    val fraction: Fraction,
    val operations: Set<Operation>,
    val runId: RunId?,          // null = profiling off. RunId is added in Part B; until then leave it out.
)

// YearMonth is java.time.YearMonth: don't write your own.
data class MonthRange(val first: YearMonth, val last: YearMonth) {
    fun months(): List<YearMonth>   // first..last inclusive, in order
}

@JvmInline value class Fraction(val value: Double) {
    init { require(value > 0.0 && value <= 1.0) { "fraction must be in (0, 1]: $value" } }
}

enum class Operation { M1, M2, M3 }

sealed interface ArgsParse {
    data class Parsed(val args: PipelineArgs) : ArgsParse
    data class Invalid(val reason: String) : ArgsParse
}
```

`parseArgs` never throws for bad user input; it returns `Invalid`. That's rule 6 of the skill: parse at the edge, and code inside trusts the types.

## A4. The functions, top-down

**`Main.kt`**, the only place that knows about exit codes:
```kotlin
fun main(rawArgs: Array<String>) {
    when (val parsed = parseArgs(rawArgs)) {
        is ArgsParse.Parsed -> runWithInputFiles(parsed.args)
        is ArgsParse.Invalid -> exitWithUsage(parsed.reason)        // stderr + exitProcess(2)
    }
}
// runWithInputFiles: findTripFiles(...) → Found → runWithSpark, Missing → list them + exitProcess(2)
// runWithSpark: createSparkSession().use { runPipeline(it, args, files) }
// (SparkSession is Closeable, so .use stops it even on failure)
```
Missing files are checked **before** Spark starts, so a typo in `--years` fails in milliseconds instead of after the session spins up.

**`SparkSessions.kt`:**
```kotlin
fun createSparkSession(): SparkSession =
    SparkSession.builder().appName("shard").withLocalMasterUnlessSubmitted().getOrCreate()
// withLocalMasterUnlessSubmitted(): if (!SparkConf().contains("spark.master")) master("local[*]")
```
Why this works: `spark-submit --master …` sets the `spark.master` system property, and `SparkConf()` reads `spark.*` system properties. `make run` doesn't set it, so you get `local[*]`. Set no other `spark.*` config here: the baseline must use the defaults.

**`Pipeline.kt`.** In 1a the story is short. Each later step adds one line:
```kotlin
fun runPipeline(spark: SparkSession, args: PipelineArgs, tripFiles: TripFiles.Found) {
    val trips = spark.loadTrips(tripFiles, args.fraction)
    reportTripsRead(trips, args)                                // 1a only: prints the row count; remove when M1 lands
}
```
`reportTripsRead` calls `count()`. That's allowed because it sits at the top level, where every action belongs. It's temporary and goes away once M1 writes real output.

**`ingest/TripFiles.kt`:**
- Path of each month: `<input>/trips/yellow_tripdata_<YYYY>-<MM>.parquet`, built from `MonthRange.months()`. **No globs.** You pass Spark the exact file list, so the input is exactly the months you asked for.
- If any file is missing, return `TripFiles.Missing` listing the missing ones. `Main` prints them and exits 2. Don't silently skip: before/after runs must read identical input.

**`ingest/LoadTrips.kt`:**
```kotlin
fun SparkSession.loadTrips(files: TripFiles.Found, fraction: Fraction): Dataset<Row> =
    read().parquet(*files.paths.map(Path::toString).toTypedArray()).sampledTo(fraction)

private fun Dataset<Row>.sampledTo(fraction: Fraction): Dataset<Row> =
    if (fraction.value == 1.0) this else sample(false, fraction.value, SAMPLE_SEED)

private const val SAMPLE_SEED = 42L   // fixed, so the same fraction gives the same rows on every run
```
- Use `read().parquet(vararg String)`, which takes a Java varargs. Don't build a Scala `Seq`.
- **Schema drift warning:** reading several months in one `parquet(...)` call can fail, or widen types, when the files disagree (int32 vs int64 `PULocationID`). Within 2025 alone it's fine. Schema enforcement belongs to M1's `enforceSchema()`, not here. If multi-year reads break, M1 switches to reading each file, casting it, then `unionByName`.
- `loadZones(input)` (in `LoadZones.kt`): `read().option("header", "true").csv(...)` with an **explicit schema**: `LocationID int, Borough string, Zone string, service_zone string`. Never use `inferSchema`, because it costs an extra pass over the file (Lab 3, example 2).

## A5. Tests (exact cases)

**`cli/ParseArgsTest.kt`**: one test per row, asserting `Parsed` with the given fields or `Invalid`:

| Args | Expect |
|---|---|
| `--input data --output out` | Parsed: months 2025-01..2025-12, fraction 1.0, ops {M1,M2,M3}, runId null |
| `… --years 2025-03` | months 2025-03..2025-03 |
| `… --years 2024-2025` | months 2024-01..2025-12 |
| `… --fraction 0.1 --ops m2,M3` | fraction 0.1, ops {M2,M3} |
| `--output out` (no input) | Invalid, reason mentions `--input` |
| `… --years 2010` | Invalid (before 2011) |
| `… --years 2025-13` | Invalid |
| `… --years 2026-2025` | Invalid (first > second) |
| `… --fraction 0` / `1.5` / `abc` | Invalid |
| `… --ops m4` | Invalid |
| `… --input a --input b` | Invalid (repeated) |
| `… --verbose` | Invalid (unknown flag) |
| `… --years` (no value) | Invalid |

**`cli/MonthRangeTest.kt`:** `2024-11..2025-02` gives `[2024-11, 2024-12, 2025-01, 2025-02]`, so the range crosses the year boundary correctly.

**`ingest/TripFilesTest.kt`:** use JUnit's `@TempDir`. With all 3 files present, expect `Found` with the exact 3 paths in order. With 2 of 3, expect `Missing` holding exactly the 1 absent path.

No Spark is needed for these tests, so they run in milliseconds.

## A6. Done when (run all of these)

```bash
make check
```
```bash
make run ARGS="--input data --output output --years 2025-01"
```
This prints the row count of January 2025. Write the number into the M1 notes; it's your raw count before cleaning.
```bash
make run ARGS="--input data --output output --years 2025-01 --fraction 0.1"
```
This prints about 10 % of that number, the same on every run because of the fixed seed.
```bash
make run ARGS="--input data --output output --years 2010"
```
This exits with code 2 and a reason. Check with `echo $?`.

Then the same jar on Docker:
```bash
make jar
```
```bash
docker compose -f docker/docker-compose.yml exec spark-master /opt/spark/bin/spark-submit --class com.radeshgovind.MainKt --master spark://spark-master:7077 /jars/shard-all.jar --input /data --output /output --years 2025-01
```
It should print the same row count as the local run. If it does, the local and cluster runs see the same data.

Finally: update `AGENTS.md` → Architecture Map and `README.md` → Usage (the CLI table above), then `/update-repo main`.

---

# Part B — the profiler (Phase 2)

**Goal:** wrap each operation's action and record what Spark did, in the same metrics as the Lab 3 profiler. Write it as files the `spark-profiler` and `benchmark-runner` agents already expect (`.claude/agents/`).

**Branch:** `feat/profiler` (scope `profiling` isn't in the commit-scope list; use `feat(spark): …`)
**Time:** about 2 h.

## B1. How it works

```
runPipeline
  └─ profiler.profile("m2") { m2Result.write()… }     ← the action, wrapped
        1. sc.setJobGroup("m2-<8 hex>", "m2", false)   ← tags every job the block starts
        2. t0 = System.nanoTime(); run block; t1
        3. sc.clearJobGroup()
        4. wait until the listener has seen JobEnd for every job in the group (timeout 30 s)
        5. aggregate the stages of those jobs → ProfiledRun("m2", …)

MetricsListener : SparkListener   (registered once with sc.addSparkListener)
  onJobStart       → job id → group (properties["spark.jobGroup.id"]), job → stage ids
  onTaskEnd        → stage id → list of task durations (taskInfo.duration)
  onStageCompleted → stage id → StageMetrics (from stageInfo.taskMetrics)
  onJobEnd         → mark job finished
```

**Why a listener instead of Lab 3's REST calls:**
- It runs inside the driver, so it works on any cluster without access to port 4040.
- It sees every task, which gives skew (max/median task duration).

**Why step 4 matters:** listener events arrive **asynchronously**, so the block can return before the last `onStageCompleted` has been handled. Spark posts events in order (task ends → stage completed → job end), so once JobEnd has been handled for every job in the group, all the stage data is in.

## B2. Files

```
src/main/kotlin/com/radeshgovind/profiling/
├── RunId.kt              ← value class, must match ^(local|docker|di)-\d{8}-\d{2}$
├── SparkProfiler.kt      ← class SparkProfiler(spark, runDir); fun <T> profile(name, block: () -> T): T
├── MetricsListener.kt    ← SparkListener; thread-safe maps (ConcurrentHashMap), nothing else
├── StageMetrics.kt       ← data class: one completed stage
├── ProfiledRun.kt        ← data class: one profile(...) block, aggregated
├── PlanCapture.kt        ← QueryExecutionListener that keeps the final plan of each write
└── RunReport.kt          ← writes run.json / stages.json / plans/, prints the summary table
src/test/kotlin/com/radeshgovind/profiling/SparkProfilerTest.kt
```

## B3. Metrics (fixed names; Lab 3's plus the 🆕 extras)

**`ProfiledRun`**, one per `profile(...)` block. These names match Lab 3's `spark_profiler.py`, so the report can say it's the lab's method:

| Field | Source |
|---|---|
| `name` | the profile name, e.g. `m2` |
| `wall_time_s` | `(t1 − t0) / 1e9`, 2 decimals |
| `num_jobs`, `num_stages`, `num_tasks` | counted from the listener. `num_tasks` = Σ `stageInfo.numTasks` |
| `executor_run_time_ms` | Σ `taskMetrics.executorRunTime` (already ms) |
| `executor_cpu_time_ms` | Σ `taskMetrics.executorCpuTime` **/ 1,000,000** (Spark reports ns) |
| `input_bytes`, `output_bytes` | Σ `inputMetrics.bytesRead`, Σ `outputMetrics.bytesWritten` |
| `shuffle_read_bytes` | Σ `shuffleReadMetrics.totalBytesRead` |
| `shuffle_write_bytes` | Σ `shuffleWriteMetrics.bytesWritten` |
| `memory_spilled_bytes`, `disk_spilled_bytes` | Σ `memoryBytesSpilled`, Σ `diskBytesSpilled` |
| 🆕 `jvm_gc_time_ms` | Σ `jvmGCTime`. GC pressure when it's > 10 % of run time |
| 🆕 `stages` | list of `StageMetrics`, below |

**`StageMetrics`**, one per completed stage:
- `stage_id`, `name`, `num_tasks`
- `duration_ms` = `completionTime − submissionTime` (both are `scala.Option`; use `.isDefined` / `.get()`)
- the same byte, time and spill fields as above, for this stage only
- 🆕 `task_ms_median`, `task_ms_max`, and `skew_ratio` = max / median. A ratio ≥ 10 is the spec's skew signal, and AQE already splits partitions at 5.

**Kotlin ↔ Scala notes** (checked in 4.2.1):
- `jobStart.stageIds()` is a Scala `Seq<Object>`. Convert it with `scala.jdk.javaapi.CollectionConverters.asJava(...)` and cast the elements to `Int`.
- `taskEnd.taskMetrics()` can be `null` for failed tasks. Skip those.
- The job-group key is `"spark.jobGroup.id"` in `jobStart.properties()`.

## B4. Output files (fixed layout)

With `--run-id local-20261105-01`, everything goes to `experiments/runs/local-20261105-01/`:

```
experiments/runs/<run-id>/
├── run.json          ← { run_id, git_sha, env, master, spark_version, args, spark_conf (non-default only), started_utc, runs: [ProfiledRun…] }
├── stages.json       ← every StageMetrics of the run, flat list, with the profile name on each
├── plans/<op>.txt    ← final (post-AQE) physical plan of each profiled write
└── eventlog/         ← Spark event log (gitignored), for the History Server and DAG screenshots
```

- **Where the runs folder is:** locally, `experiments/runs` in the repo. In Docker, `/runs`, which needs a new mount on the master: `../experiments/runs:/runs`. Add a `--runs-dir` flag with default `experiments/runs`, and pass `--runs-dir /runs` on Docker.
- **`git_sha`:** pass it in with `--git-sha $(git rev-parse --short HEAD)`. Don't shell out from the JVM: the Docker container has no git.
- **Event log:** only when `--run-id` is set, the session builder adds `spark.eventLog.enabled=true` and `spark.eventLog.dir=<runs-dir>/<run-id>/eventlog`. Create that folder first, or Spark refuses to start. This is instrumentation, not tuning. Still, list it in README → Configuration as "on only for profiled runs".
- **Final plans:** `PlanCapture` registers through `spark.listenerManager().register(...)`. `onSuccess(funcName, qe, durationNs)` stores `qe.executedPlan().toString()`, which is the plan after the action ran, so AQE's real choices (`isFinalPlan=true`) appear. The callback is asynchronous too: after the block, wait (with a timeout) until one more plan has arrived than before it started.
- **Summary table:** print one line per `ProfiledRun` to stdout: `name | wall s | stages | tasks | shuffle R | shuffle W | spill | max skew`.

## B5. Wiring it into `runPipeline`

```kotlin
fun runPipeline(spark: SparkSession, args: PipelineArgs, tripFiles: TripFiles.Found) {
    val profiler = profilerFor(spark, args)          // a no-op profiler when runId == null
    val zones = spark.loadZones(args.input)
    val trips = spark.loadTrips(tripFiles, args.fraction)
    val cleaned = trips.enforceSchema().removeInvalidTrips()

    profiler.profile("m1") { trips.cleaningStats().write().mode(SaveMode.Overwrite).parquet(args.out("m1")) }
    profiler.profile("m2") { cleaned.temporalDemand(zones).writePartitionedByBorough(args.out("m2")) }
    profiler.profile("m3") { cleaned.zoneStatistics(zones).write().mode(SaveMode.Overwrite).parquet(args.out("m3")) }
    profiler.finish()                                // writes run.json, stages.json, plans/
}
```

- **One action per profile block,** so each operation's metrics are its own.
- **The no-op profiler** (`profile` just runs the block) means the measured code path is the same with and without profiling.
- **Don't fix the obvious waste:** `cleaned` isn't cached, so M2 and M3 each re-read and re-clean the Parquet files. That's correct for the baseline. The profiler will show it (input bytes read 3 times), and caching becomes a measured optimisation in Part C, not a guess.

## B6. Tests (`SparkProfilerTest`, `local[2]`, UI off, like `SparkSessionTest`)

| Test | Block | Assert |
|---|---|---|
| shuffle job | `spark.range(100_000).selectExpr("id % 10 AS k").groupBy("k").count().collect()` | `num_stages ≥ 2`, `shuffle_write_bytes > 0`, `shuffle_read_bytes > 0`, `num_tasks > 0`, `wall_time_s > 0` |
| no-shuffle job | `spark.range(1000).count()` | `shuffle_write_bytes == 0` |
| two blocks stay separate | run both, back to back | each `ProfiledRun` holds only its own stages (no stage id in both) |
| files | profiling into `@TempDir` | `run.json`, `stages.json` and `plans/` exist, and `run.json` parses with Jackson |

JSON writing uses **Jackson** (`com.fasterxml.jackson.databind.ObjectMapper`, 2.21 on the classpath through Spark). Don't add a new dependency.

## B7. Done when

```bash
make check
```
```bash
make run ARGS="--input data --output output --years 2025-01 --run-id local-20261105-01 --git-sha $(git rev-parse --short HEAD)"
```
It prints the summary table, and `experiments/runs/local-20261105-01/` holds `run.json`, `stages.json`, `plans/m1.txt`, `m2.txt`, `m3.txt` and `eventlog/`. Then compare against the Spark UI at http://localhost:4040 while a run is going: shuffle bytes and task counts in the Stages tab must match `stages.json`.

---

# Part C — the baseline and measurement protocol

Run these the same way every time, so the numbers can be compared.

**Run ids:**
- Format: `<env>-<yyyymmdd>-<nn>`, with `env` ∈ `local`, `docker`, `di`, and `nn` counting up per day per environment.
- Never reuse an id.
- One row per run in `experiments/LOG.md` → Runs.

**Before every measured run:**
1. Close heavy apps (browsers with many tabs, IDE indexing).
2. Same input: same `--years`, the same `--fraction`, `data/` untouched (check `data/MANIFEST.tsv`).
3. Same environment: locally, nothing changed in `local[*]`. On Docker, the same `--scale spark-worker=N`, and the cluster freshly started with `down` then `up`.
4. Commit first, so `git_sha` names exactly the code that ran.

**Baseline set:**

| What | Command | Runs |
|---|---|---|
| local, full 2025 | `make run ARGS="--input data --output output --years 2025 --run-id local-<date>-<nn> --git-sha <sha>"` | 1 warm-up (discarded) + 3 measured |
| Docker, 2 workers, full 2025 | `spark-submit … --input /data --output /output --years 2025 --runs-dir /runs --run-id docker-<date>-<nn> --git-sha <sha>` | 1 warm-up + 3 measured |

- **Report:** the median of the 3, with min–max. If the spread is above 10 % of the median, run more before concluding anything.
- **Locally** the spec asks for shuffle read/write and number of tasks. **On the cluster** it asks for wall-clock and per-stage duration. Your files hold both everywhere.
- **Then:** hand the run ids to the `spark-profiler` agent. It reads `stages.json` and `plans/`, then writes ranked hypotheses (H1, H2, …) into `LOG.md`. Each optimisation then goes through `spark-optimizer` (one change) and `benchmark-runner` (the same protocol, before against after).

**Expected first hypotheses.** Confirm these with the numbers; don't assume them:
- **M2 + M3:** recomputation of the cleaned trips. Evidence: `input_bytes` ≈ the dataset size, read once per operation. Remedy: `persist` the cleaned trips.
- **M2 + M3:** `SortMergeJoin` with the 265-row zone table. Evidence: in `plans/m2.txt`, unless AQE already turned it into a broadcast join. Remedy: `broadcast(zones)`.
- **M3:** large window partitions. Evidence: high `skew_ratio` or spill in the window stage.
- **Everything:** shuffle partition count. Evidence: 200 tasks on tiny shuffles (`spark.sql.shuffle.partitions`) against the 4 Docker cores.
