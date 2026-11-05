package com.radeshgovind

import com.radeshgovind.clean.cleaningStats
import com.radeshgovind.cli.Operation
import com.radeshgovind.cli.PipelineArgs
import com.radeshgovind.ingest.TripColumns.SOURCE_YEAR
import com.radeshgovind.ingest.TripFiles
import com.radeshgovind.ingest.enforceSchema
import com.radeshgovind.ingest.loadTrips
import com.radeshgovind.profiling.NoProfiler
import com.radeshgovind.profiling.Profiler
import com.radeshgovind.profiling.RunHeader
import com.radeshgovind.profiling.SparkProfiler
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import java.nio.file.Path

// The story of the pipeline. Spark actions (write, show, collect) happen only in this file.
// Each operation's one write runs inside profiler.profile("<op>") { … }, and nothing else does.
fun runPipeline(spark: SparkSession, args: PipelineArgs, tripFiles: TripFiles.Found) {
    val profiler = profilerFor(spark, args)
    val trips = spark.loadTrips(tripFiles, args.fraction).enforceSchema()
    if (Operation.M1 in args.operations) writeCleaningStats(trips, args.outputOf(Operation.M1), profiler)
    // M2 and M3 start from: val cleaned = trips.removeInvalidTrips(), and wrap their write the same way:
    // profiler.profile("m2") { cleaned.temporalDemand(zones).writePartitionedByBorough(out) }
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
