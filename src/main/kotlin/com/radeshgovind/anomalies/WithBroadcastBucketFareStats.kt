package com.radeshgovind.anomalies

import com.radeshgovind.anomalies.AnomalyColumns.BUCKET_MEAN_FARE_PER_KM
import com.radeshgovind.anomalies.AnomalyColumns.BUCKET_STDDEV_FARE_PER_KM
import com.radeshgovind.anomalies.AnomalyColumns.BUCKET_TRIPS
import com.radeshgovind.demand.DemandColumns.HOUR_OF_DAY
import com.radeshgovind.ingest.TripColumns.PU_LOCATION_ID
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.functions.col

// OPT-2 (H2): the statistics per bucket (at most 263 zones × 24 hours = 6,312 rows), broadcast back onto the trips.
// No trip crosses a shuffle and nothing is sorted. The statistics side reads the trips a second time (no cache).
internal fun Dataset<Row>.withBroadcastBucketFareStats(): Dataset<Row> = withStatsOf(broadcast(bucketFareStats()))

// One row per bucket. The keys are renamed so that both join sides keep unique column names.
private fun Dataset<Row>.bucketFareStats(): Dataset<Row> = groupBy(*statsKeys())
    .agg(
        bucketTrips().alias(BUCKET_TRIPS),
        bucketMean().alias(BUCKET_MEAN_FARE_PER_KM),
        bucketStddev().alias(BUCKET_STDDEV_FARE_PER_KM),
    )

// Inner is safe: every trip's bucket is in the statistics, which come from the same trips, once per bucket.
// The stats' columns land after the trip's, in the window plan's order, and their renamed keys are dropped.
private fun Dataset<Row>.withStatsOf(stats: Dataset<Row>): Dataset<Row> =
    join(stats, isSameBucket()).drop(STATS_PU_LOCATION_ID, STATS_HOUR_OF_DAY)

// Top-level on purpose: inside an extension on Dataset, a bare col() resolves to the receiver's Dataset.col.

private fun statsKeys(): Array<Column> =
    arrayOf(col(PU_LOCATION_ID).alias(STATS_PU_LOCATION_ID), col(HOUR_OF_DAY).alias(STATS_HOUR_OF_DAY))

// Null-safe (<=>), so a null key finds its bucket as in the window, where null keys form one partition.
// After M1 neither key can be null, so this only keeps the two plans equal for any input.
private fun isSameBucket(): Column = col(PU_LOCATION_ID).eqNullSafe(col(STATS_PU_LOCATION_ID))
    .and(col(HOUR_OF_DAY).eqNullSafe(col(STATS_HOUR_OF_DAY)))

private const val STATS_PU_LOCATION_ID = "bucket_PULocationID"
private const val STATS_HOUR_OF_DAY = "bucket_hour_of_day"
