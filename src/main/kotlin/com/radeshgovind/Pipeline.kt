package com.radeshgovind

import com.radeshgovind.clean.cleaningStats
import com.radeshgovind.cli.Operation
import com.radeshgovind.cli.PipelineArgs
import com.radeshgovind.ingest.TripColumns.SOURCE_YEAR
import com.radeshgovind.ingest.TripFiles
import com.radeshgovind.ingest.enforceSchema
import com.radeshgovind.ingest.loadTrips
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import java.nio.file.Path

// The story of the pipeline. Spark actions (write, show, collect) happen only in this file.
fun runPipeline(spark: SparkSession, args: PipelineArgs, tripFiles: TripFiles.Found) {
    val trips = spark.loadTrips(tripFiles, args.fraction).enforceSchema()
    if (Operation.M1 in args.operations) writeCleaningStats(trips, args.outputOf(Operation.M1))
    // M2 and M3 start from: val cleaned = trips.removeInvalidTrips()
}

private fun writeCleaningStats(trips: Dataset<Row>, out: Path) {
    trips.cleaningStats().write().mode(SaveMode.Overwrite).parquet(out.toString())
    printCleaningStats(trips.sparkSession(), out)
}

// Reads back the few written rows: cheap, and it doesn't re-run the cleaning.
private fun printCleaningStats(spark: SparkSession, out: Path) {
    spark.read().parquet(out.toString()).orderBy(SOURCE_YEAR).show(false)
}
