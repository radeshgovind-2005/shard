package com.radeshgovind.forecast

import com.radeshgovind.demand.DemandColumns.DAY_OF_WEEK
import com.radeshgovind.demand.DemandColumns.HOUR_OF_DAY
import com.radeshgovind.demand.DemandColumns.TRIP_COUNT
import com.radeshgovind.forecast.ForecastColumns.HIST_MEAN_TRIPS
import com.radeshgovind.forecast.ForecastColumns.HIST_VAR_TRIPS
import com.radeshgovind.ingest.TripColumns.PU_LOCATION_ID
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions.WindowSpec
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.var_samp

// D26: per (zone, day of week, hour of day) bucket, the mean and sample variance of the zero-filled hourly counts over
// every week of the range, the current one included. One value per bucket, repeated on each of its rows (D23).
fun Dataset<Row>.withBucketStats(): Dataset<Row> = withColumn(HIST_MEAN_TRIPS, bucketMean())
    .withColumn(HIST_VAR_TRIPS, bucketVariance())

// Top-level on purpose: inside an extension on Dataset, a bare col() resolves to the receiver's Dataset.col.

private fun bucketMean(): Column = avg(col(TRIP_COUNT)).over(bucket())

// D10's convention: the sample variance. Every bucket spans ≥ 4 weeks, so it's never null.
private fun bucketVariance(): Column = var_samp(col(TRIP_COUNT)).over(bucket())

// No orderBy: the frame is the whole bucket, every week of the range.
private fun bucket(): WindowSpec = Window.partitionBy(col(PU_LOCATION_ID), col(DAY_OF_WEEK), col(HOUR_OF_DAY))
