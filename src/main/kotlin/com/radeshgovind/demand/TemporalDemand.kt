package com.radeshgovind.demand

import com.radeshgovind.demand.DemandColumns.AVG_DISTANCE
import com.radeshgovind.demand.DemandColumns.AVG_DURATION_MIN
import com.radeshgovind.demand.DemandColumns.AVG_FARE
import com.radeshgovind.demand.DemandColumns.DAY_OF_WEEK
import com.radeshgovind.demand.DemandColumns.DURATION_MIN
import com.radeshgovind.demand.DemandColumns.HOUR_OF_DAY
import com.radeshgovind.demand.DemandColumns.PICKUP_BOROUGH
import com.radeshgovind.demand.DemandColumns.TRIP_COUNT
import com.radeshgovind.ingest.TripColumns.FARE_AMOUNT
import com.radeshgovind.ingest.TripColumns.TRIP_DISTANCE
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.lit

// M2's output: one row per (pickup_borough, day_of_week, hour_of_day), from the cleaned trips.
// No orderBy: it would add a sort and a shuffle for nothing.
fun Dataset<Row>.temporalDemand(zones: Dataset<Row>): Dataset<Row> = withPickupBorough(zones)
    .withTripTimes()
    .groupBy(*groupingColumns())
    .agg(tripCount(), *averages())

// Top-level on purpose: inside an extension on Dataset, a bare col() resolves to the receiver's Dataset.col.

private fun groupingColumns(): Array<Column> = arrayOf(col(PICKUP_BOROUGH), col(DAY_OF_WEEK), col(HOUR_OF_DAY))

private fun tripCount(): Column = count(lit(1)).alias(TRIP_COUNT)

private fun averages(): Array<Column> = arrayOf(
    averageOf(FARE_AMOUNT, AVG_FARE),
    averageOf(DURATION_MIN, AVG_DURATION_MIN),
    averageOf(TRIP_DISTANCE, AVG_DISTANCE),
)

private fun averageOf(column: String, alias: String): Column = avg(col(column)).alias(alias)
