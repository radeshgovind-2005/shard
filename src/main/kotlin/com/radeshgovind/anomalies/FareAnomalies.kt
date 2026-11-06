package com.radeshgovind.anomalies

import com.radeshgovind.anomalies.AnomalyColumns.ANOMALY_RATE
import com.radeshgovind.anomalies.AnomalyColumns.DAY_TYPE
import com.radeshgovind.anomalies.AnomalyColumns.FLAGGED_TRIPS
import com.radeshgovind.anomalies.AnomalyColumns.IS_FARE_ANOMALY
import com.radeshgovind.anomalies.AnomalyColumns.TRIPS
import com.radeshgovind.demand.DemandColumns.PICKUP_BOROUGH
import com.radeshgovind.demand.withPickupBorough
import com.radeshgovind.demand.withTripTimes
import com.radeshgovind.ingest.TripColumns.PAYMENT_TYPE
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.`when`

// O1's output: weekday vs weekend fare-anomaly rates per (day_type, payment_type, pickup_borough), from the cleaned
// trips. Decisions D15–D22 and D31 in README → O1.
// The flags are per trip (from the zone-hour bucket's statistics, computed by `plan`); only the rate table leaves this
// function (D21). No orderBy: it would add a sort and a shuffle for nothing.
fun Dataset<Row>.fareAnomalyRates(zones: Dataset<Row>, plan: BucketStatsPlan): Dataset<Row> = fareAnomalies(plan)
    .withPickupBorough(zones)
    .withDayType()
    .groupBy(*rateGroups())
    .agg(trips(), flaggedTrips())
    .withColumn(ANOMALY_RATE, anomalyRate())

// Every cleaned trip with its fare_per_km, its bucket's stats and is_fare_anomaly (D15–D18). withTripTimes adds
// hour_of_day and day_of_week in NYC wall-clock time (D3, D7); its duration_min is unused and pruned by Catalyst.
fun Dataset<Row>.fareAnomalies(plan: BucketStatsPlan): Dataset<Row> = withTripTimes()
    .withFarePerKm()
    .withBucketFareStats(plan)
    .withFareAnomalyFlag()

// Top-level on purpose: inside an extension on Dataset, a bare col() resolves to the receiver's Dataset.col.

// D20: payment_type is the raw TLC code; a null code stays its own group, so no trip is lost.
private fun rateGroups(): Array<Column> = arrayOf(col(DAY_TYPE), col(PAYMENT_TYPE), col(PICKUP_BOROUGH))

private fun trips(): Column = count(lit(1)).alias(TRIPS)

// D18: count() skips the nulls, so a trip without a σ counts as not flagged.
private fun flaggedTrips(): Column = count(`when`(col(IS_FARE_ANOMALY), 1)).alias(FLAGGED_TRIPS)

// Long / long is a double in Spark, and trips ≥ 1 in every group.
private fun anomalyRate(): Column = col(FLAGGED_TRIPS).divide(col(TRIPS))
