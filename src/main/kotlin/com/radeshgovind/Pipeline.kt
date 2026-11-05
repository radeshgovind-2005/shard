package com.radeshgovind

import com.radeshgovind.cli.PipelineArgs
import com.radeshgovind.ingest.TripFiles
import com.radeshgovind.ingest.loadTrips
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

// The story of the pipeline. Spark actions (count, write, collect) happen only in this file.
fun runPipeline(spark: SparkSession, args: PipelineArgs, tripFiles: TripFiles.Found) {
    val trips = spark.loadTrips(tripFiles, args.fraction)
    reportTripsRead(trips, args)
}

// Phase 1a only: proves the load works end to end. Remove once M1 writes real output.
private fun reportTripsRead(trips: Dataset<Row>, args: PipelineArgs) {
    println("Trips read for ${args.months} (fraction ${args.fraction.value}): ${trips.count()}")
}
