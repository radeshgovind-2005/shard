package com.radeshgovind.anomalies

import com.radeshgovind.anomalies.AnomalyColumns.BUCKET_MEAN_FARE_PER_KM
import com.radeshgovind.anomalies.AnomalyColumns.BUCKET_STDDEV_FARE_PER_KM
import com.radeshgovind.anomalies.AnomalyColumns.BUCKET_TRIPS
import com.radeshgovind.demand.DemandColumns.HOUR_OF_DAY
import com.radeshgovind.ingest.TripColumns.PU_LOCATION_ID
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions.WindowSpec
import org.apache.spark.sql.functions.col

// D22, the baseline (--skip-opts opt2): a plain window, so every trip is shuffled by bucket and sorted.
// One window spec → one Window, one Exchange. A window groups null keys together, like a groupBy.
internal fun Dataset<Row>.withWindowBucketFareStats(): Dataset<Row> =
    withColumn(BUCKET_TRIPS, bucketTrips().over(bucket()))
        .withColumn(BUCKET_MEAN_FARE_PER_KM, bucketMean().over(bucket()))
        .withColumn(BUCKET_STDDEV_FARE_PER_KM, bucketStddev().over(bucket()))

// Top-level on purpose: inside an extension on Dataset, a bare col() resolves to the receiver's Dataset.col.
// No orderBy, so the frame is the whole bucket.
private fun bucket(): WindowSpec = Window.partitionBy(col(PU_LOCATION_ID), col(HOUR_OF_DAY))
