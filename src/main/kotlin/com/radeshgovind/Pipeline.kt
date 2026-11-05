package com.radeshgovind

import com.radeshgovind.clean.cleaningStats
import com.radeshgovind.clean.removeInvalidTrips
import com.radeshgovind.cli.Operation
import com.radeshgovind.cli.PipelineArgs
import com.radeshgovind.demand.DemandColumns.PICKUP_BOROUGH
import com.radeshgovind.demand.DemandColumns.TRIP_COUNT
import com.radeshgovind.demand.temporalDemand
import com.radeshgovind.ingest.TripColumns.SOURCE_YEAR
import com.radeshgovind.ingest.TripFiles
import com.radeshgovind.ingest.enforceSchema
import com.radeshgovind.ingest.loadTrips
import com.radeshgovind.ingest.loadZones
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.asc_nulls_last
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.sum
import java.nio.file.Path

// The story of the pipeline. Spark actions (write, show, collect) happen only in this file.
// Nothing is cached yet: each operation re-reads and re-cleans the input (the baseline).
fun runPipeline(spark: SparkSession, args: PipelineArgs, tripFiles: TripFiles.Found) {
    val trips = spark.loadTrips(tripFiles, args.fraction).enforceSchema()
    if (Operation.M1 in args.operations) writeCleaningStats(trips, args.outputOf(Operation.M1))
    val cleaned = trips.removeInvalidTrips()
    if (Operation.M2 in args.operations) {
        writeTemporalDemand(cleaned, spark.loadZones(args.input), args.outputOf(Operation.M2))
    }
}

private fun writeCleaningStats(trips: Dataset<Row>, out: Path) {
    trips.cleaningStats().write().mode(SaveMode.Overwrite).parquet(out.toString())
    printCleaningStats(trips.sparkSession(), out)
}

// Reads back the few written rows: cheap, and it doesn't re-run the cleaning.
private fun printCleaningStats(spark: SparkSession, out: Path) {
    spark.read().parquet(out.toString()).orderBy(SOURCE_YEAR).show(false)
}

// The zones are loaded only when M2 runs: read().csv() checks the path as soon as the Dataset is defined.
private fun writeTemporalDemand(cleaned: Dataset<Row>, zones: Dataset<Row>, out: Path) {
    cleaned.temporalDemand(zones).write().mode(SaveMode.Overwrite).partitionBy(PICKUP_BOROUGH).parquet(out.toString())
    printDemandTotals(cleaned.sparkSession(), out)
}

// Reads back the written rows (at most 6 boroughs × 168 hours): trips and rows per borough, then the total
// (the null-borough row of the rollup). The total must equal M1's kept_trips.
private fun printDemandTotals(spark: SparkSession, out: Path) {
    spark.read().parquet(out.toString())
        .rollup(PICKUP_BOROUGH)
        .agg(sum(TRIP_COUNT).alias(TRIP_COUNT), count(lit(1)).alias(DEMAND_ROWS))
        .orderBy(asc_nulls_last(PICKUP_BOROUGH))
        .show(false)
}

private const val DEMAND_ROWS = "rows"
