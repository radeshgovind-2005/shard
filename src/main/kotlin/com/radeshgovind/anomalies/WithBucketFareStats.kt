package com.radeshgovind.anomalies

import com.radeshgovind.anomalies.AnomalyColumns.BUCKET_MEAN_FARE_PER_KM
import com.radeshgovind.anomalies.AnomalyColumns.BUCKET_STDDEV_FARE_PER_KM
import com.radeshgovind.anomalies.AnomalyColumns.BUCKET_TRIPS
import com.radeshgovind.anomalies.AnomalyColumns.FARE_PER_KM
import com.radeshgovind.demand.DemandColumns.HOUR_OF_DAY
import com.radeshgovind.ingest.TripColumns.PU_LOCATION_ID
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions.WindowSpec
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.stddev

// D16/D17: every trip gets its bucket's trip count, mean and sample σ of fare_per_km. The bucket is
// (PULocationID, hour_of_day) over every month of the run, the trip itself included.
// D22, the baseline: a plain window, so every trip is shuffled by bucket. One window spec → one Window, one Exchange.
fun Dataset<Row>.withBucketFareStats(): Dataset<Row> = withColumn(BUCKET_TRIPS, bucketTrips())
    .withColumn(BUCKET_MEAN_FARE_PER_KM, bucketMean())
    .withColumn(BUCKET_STDDEV_FARE_PER_KM, bucketStddev())

// Top-level on purpose: inside an extension on Dataset, a bare col() resolves to the receiver's Dataset.col.

private fun bucketTrips(): Column = count(lit(1)).over(bucket())

private fun bucketMean(): Column = avg(col(FARE_PER_KM)).over(bucket())

// stddev is stddev_samp (as D10): null for a 1-trip bucket.
private fun bucketStddev(): Column = stddev(col(FARE_PER_KM)).over(bucket())

// No orderBy, so the frame is the whole bucket.
private fun bucket(): WindowSpec = Window.partitionBy(col(PU_LOCATION_ID), col(HOUR_OF_DAY))
