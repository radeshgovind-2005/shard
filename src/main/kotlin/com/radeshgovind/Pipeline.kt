package com.radeshgovind

import com.radeshgovind.anomalies.AnomalyColumns.ANOMALY_RATE
import com.radeshgovind.anomalies.AnomalyColumns.DAY_TYPE
import com.radeshgovind.anomalies.AnomalyColumns.FLAGGED_TRIPS
import com.radeshgovind.anomalies.AnomalyColumns.TRIPS
import com.radeshgovind.anomalies.fareAnomalyRates
import com.radeshgovind.clean.cleaningStats
import com.radeshgovind.clean.removeInvalidTrips
import com.radeshgovind.cli.Operation
import com.radeshgovind.cli.PipelineArgs
import com.radeshgovind.demand.DemandColumns.PICKUP_BOROUGH
import com.radeshgovind.demand.DemandColumns.TRIP_COUNT
import com.radeshgovind.demand.temporalDemand
import com.radeshgovind.ingest.TripColumns.PAYMENT_TYPE
import com.radeshgovind.ingest.TripColumns.SOURCE_YEAR
import com.radeshgovind.ingest.TripFiles
import com.radeshgovind.ingest.enforceSchema
import com.radeshgovind.ingest.loadTrips
import com.radeshgovind.ingest.loadZones
import com.radeshgovind.profiling.NoProfiler
import com.radeshgovind.profiling.Profiler
import com.radeshgovind.profiling.RunHeader
import com.radeshgovind.profiling.SparkProfiler
import com.radeshgovind.zones.ZoneStatsColumns.IS_VOLUME_ANOMALY
import com.radeshgovind.zones.zoneStatistics
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.asc_nulls_last
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.`when`
import java.nio.file.Path

// The story of the pipeline. Spark actions (write, show, collect) happen only in this file.
// Each operation's one write runs inside profiler.profile("<op>") { … }, and nothing else does.
// Nothing is cached yet: each operation re-reads and re-cleans the input (the baseline).
fun runPipeline(spark: SparkSession, args: PipelineArgs, tripFiles: TripFiles.Found) {
    val profiler = profilerFor(spark, args)
    val trips = spark.loadTrips(tripFiles, args.fraction).enforceSchema()
    if (Operation.M1 in args.operations) writeCleaningStats(trips, args.outputOf(Operation.M1), profiler)
    val cleaned = trips.removeInvalidTrips()
    if (Operation.M2 in args.operations) {
        writeTemporalDemand(cleaned, spark.loadZones(args.input), args.outputOf(Operation.M2), profiler)
    }
    if (Operation.M3 in args.operations) {
        writeZoneStatistics(cleaned, spark.loadZones(args.input), args.outputOf(Operation.M3), profiler)
    }
    if (Operation.O1 in args.operations) {
        writeFareAnomalyRates(cleaned, spark.loadZones(args.input), args.outputOf(Operation.O1), profiler)
    }
    profiler.finish()
}

// No --run-id → NoProfiler, which only runs the block: the measured code path is the same either way.
private fun profilerFor(spark: SparkSession, args: PipelineArgs): Profiler = args.profiling?.let {
    SparkProfiler(spark, it.runDir, RunHeader(it.runId, it.gitSha, args.asRecord()))
} ?: NoProfiler

private fun writeCleaningStats(trips: Dataset<Row>, out: Path, profiler: Profiler) {
    profiler.profile("m1") { trips.cleaningStats().write().mode(SaveMode.Overwrite).parquet(out.toString()) }
    printCleaningStats(trips.sparkSession(), out)
}

// Reads back the few written rows: cheap, and it doesn't re-run the cleaning. Outside the profile block on purpose.
private fun printCleaningStats(spark: SparkSession, out: Path) {
    spark.read().parquet(out.toString()).orderBy(SOURCE_YEAR).show(false)
}

// The zones are loaded only when M2 or M3 runs: read().csv() checks the path as soon as the Dataset is defined.
private fun writeTemporalDemand(cleaned: Dataset<Row>, zones: Dataset<Row>, out: Path, profiler: Profiler) {
    profiler.profile("m2") {
        cleaned.temporalDemand(zones).write().mode(SaveMode.Overwrite).partitionBy(PICKUP_BOROUGH)
            .parquet(out.toString())
    }
    printDemandTotals(cleaned.sparkSession(), out)
}

// Reads back the written rows (at most 6 boroughs × 168 hours): trips and rows per borough, then the total
// (the null-borough row of the rollup). The total must equal M1's kept_trips. Outside the profile block on purpose.
private fun printDemandTotals(spark: SparkSession, out: Path) {
    spark.read().parquet(out.toString())
        .rollup(PICKUP_BOROUGH)
        .agg(sum(TRIP_COUNT).alias(TRIP_COUNT), count(lit(1)).alias(DEMAND_ROWS))
        .orderBy(asc_nulls_last(PICKUP_BOROUGH))
        .show(false)
}

// No partitionBy: the spec asks for borough partitioning only for M2.
private fun writeZoneStatistics(cleaned: Dataset<Row>, zones: Dataset<Row>, out: Path, profiler: Profiler) {
    profiler.profile("m3") { cleaned.zoneStatistics(zones).write().mode(SaveMode.Overwrite).parquet(out.toString()) }
    printZoneTotals(cleaned.sparkSession(), out)
}

// Reads back the written zone-month rows (a few thousand): per borough the trips, rows, flagged rows and rows with
// too little history to flag, then the total (the null-borough row). The total must equal M1's kept_trips.
// Outside the profile block on purpose.
private fun printZoneTotals(spark: SparkSession, out: Path) {
    spark.read().parquet(out.toString())
        .rollup(PICKUP_BOROUGH)
        .agg(
            sum(TRIP_COUNT).alias(TRIP_COUNT),
            count(lit(1)).alias(DEMAND_ROWS),
            count(`when`(col(IS_VOLUME_ANOMALY), 1)).alias(ANOMALY_ROWS),
            count(`when`(col(IS_VOLUME_ANOMALY).isNull, 1)).alias(NO_BASELINE_ROWS),
        )
        .orderBy(asc_nulls_last(PICKUP_BOROUGH))
        .show(false)
}

// D21: only the rate table is written (at most 2 day types × 8 payment codes, null included, × 6 boroughs = 96 rows),
// never the flagged trips. No partitionBy: the table is tiny.
private fun writeFareAnomalyRates(cleaned: Dataset<Row>, zones: Dataset<Row>, out: Path, profiler: Profiler) {
    profiler.profile("o1") { cleaned.fareAnomalyRates(zones).write().mode(SaveMode.Overwrite).parquet(out.toString()) }
    printAnomalyRates(cleaned.sparkSession(), out)
}

// Reads back the written rate table (under 100 rows): the whole table, then the trips, flagged trips and rate per day
// type with the total on the last row (day_type = null). The total must equal M1's kept_trips.
// Outside the profile block on purpose.
private fun printAnomalyRates(spark: SparkSession, out: Path) {
    val rates = spark.read().parquet(out.toString())
    rates.orderBy(col(DAY_TYPE), col(PICKUP_BOROUGH), asc_nulls_last(PAYMENT_TYPE)).show(MAX_RATE_ROWS, false)
    rates.rollup(DAY_TYPE)
        .agg(sum(TRIPS).alias(TRIPS), sum(FLAGGED_TRIPS).alias(FLAGGED_TRIPS))
        .withColumn(ANOMALY_RATE, col(FLAGGED_TRIPS).divide(col(TRIPS)))
        .orderBy(asc_nulls_last(DAY_TYPE))
        .show(false)
}

private const val MAX_RATE_ROWS = 200
private const val DEMAND_ROWS = "rows"
private const val ANOMALY_ROWS = "anomalies"
private const val NO_BASELINE_ROWS = "no_baseline"
