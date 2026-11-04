# Kotlin on Spark

How the stepdown rules apply to Spark's JVM API from Kotlin.

## The story is the pipeline

Chain steps as extension functions on `Dataset<Row>`, so the top function reads like the pipeline:
```kotlin
fun runPipeline(spark: SparkSession, args: PipelineArgs) {
    val zones = spark.loadZones(args.zonesPath)
    val trips = spark.loadTrips(args.tripsPath, args.scale)
        .enforceSchema()
        .removeInvalidTrips()

    trips.temporalDemand(zones).writePartitionedByBorough(args.out("m2"))
    trips.zoneStatistics(zones).write(args.out("m3"))
}
```
Avoid `Dataset.transform { }`: it expects a Scala `Function1`, which Kotlin lambdas don't convert to cleanly. Plain extension calls read better anyway.

## Details at the bottom

```kotlin
private fun Dataset<Row>.removeInvalidTrips(): Dataset<Row> =
    filter(hasValidFare.and(hasValidDistance).and(hasValidPassengers).and(hasValidDuration))

private val hasValidFare = col("fare_amount").gt(0)
private val hasValidPassengers = col("passenger_count").between(1, 6)
```
Kotlin has no Scala operator syntax, so use `.gt()`, `.leq()`, `.and()`, `.or()`, `.equalTo()`. Named `Column` values follow the Boolean-as-question rule.

## Where the type rules apply (and where they don't)

- Rows stay `Dataset<Row>`. Kotlin data and value classes don't have reliable Spark encoders, and typed Datasets add serialisation overhead.
- Apply rule 5 (types tell the truth) to everything *around* the data: CLI args (`PipelineArgs`, `Scale`), output paths, thresholds (O4's classification), and run results.
- Apply rule 6 (parse at the edge) to the schema: enforce it once in `enforceSchema()` at load, and never re-cast downstream.
- Column names repeat everywhere, so keep them as constants in one place (e.g. `object TripColumns`) instead of string literals.

## Spark-specific rules

- **Helpers return `Dataset<Row>` and never run actions.** A hidden `count()`, `show()`, `collect()` or `first()` triggers an extra Spark job, which skews profiling and wastes runtime. Actions belong at the top level, where they're visible.
- **Restructuring is free at runtime.** Spark is lazy and Catalyst optimises the whole plan, so extracting functions doesn't change execution. The "hot loop" exception in SKILL.md almost never applies to DataFrame code. If a refactor touches caching or join hints, confirm with `explain("extended")` that the plan is unchanged.
- **Performance decisions stay visible.** `persist`/`unpersist`, `broadcast()` hints and repartitioning belong in the story function (or a clearly named step like `withZonesBroadcast()`), not buried three levels down.
- **UDFs are a smell.** If a step needs a UDF, look for a built-in in `org.apache.spark.sql.functions` first, because UDFs block Catalyst optimisation and codegen.
