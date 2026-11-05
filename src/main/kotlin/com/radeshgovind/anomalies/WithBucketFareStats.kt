package com.radeshgovind.anomalies

import com.radeshgovind.anomalies.AnomalyColumns.FARE_PER_KM
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.stddev

// D16/D17: every trip gets its bucket's trip count, mean and sample σ of fare_per_km. The bucket is
// (PULocationID, hour_of_day) over every month of the run, the trip itself included.
fun Dataset<Row>.withBucketFareStats(plan: BucketStatsPlan): Dataset<Row> = when (plan) {
    BucketStatsPlan.WINDOW -> withWindowBucketFareStats()
    BucketStatsPlan.BROADCAST_AGGREGATE -> withBroadcastBucketFareStats()
}

// The three statistics, shared by both plans: a window applies them over the bucket, the aggregate per bucket.
// Top-level on purpose: inside an extension on Dataset, a bare col() resolves to the receiver's Dataset.col.

// Every trip of the bucket, whatever its fare_per_km.
internal fun bucketTrips(): Column = count(lit(1))

// avg and stddev skip null fare_per_km values.
internal fun bucketMean(): Column = avg(col(FARE_PER_KM))

// stddev is stddev_samp (as D10): null for a 1-trip bucket.
internal fun bucketStddev(): Column = stddev(col(FARE_PER_KM))
