package com.radeshgovind.forecast

import com.radeshgovind.demand.DemandColumns.DAY_OF_WEEK
import com.radeshgovind.demand.DemandColumns.HOUR_OF_DAY
import com.radeshgovind.demand.DemandColumns.TRIP_COUNT
import com.radeshgovind.forecast.ForecastColumns.HOUR_INDEX
import com.radeshgovind.forecast.ForecastColumns.PICKUP_DATE
import com.radeshgovind.ingest.TripColumns.PICKUP_DATETIME
import com.radeshgovind.ingest.TripColumns.PU_LOCATION_ID
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.coalesce
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.date_from_unix_date
import org.apache.spark.sql.functions.dayofweek
import org.apache.spark.sql.functions.floor
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.pmod

// c(zone, hour) from the cleaned trips: one row per zone and pickup hour that had at least one trip.
fun Dataset<Row>.hourlyTripCounts(): Dataset<Row> = withColumn(HOUR_INDEX, pickupHourIndex())
    .groupBy(*zoneAndHour())
    .agg(tripCount())

// D25: the grid keeps every hour, and an hour without trips gets trip_count = 0. Averaging only the hours that had
// trips would give Σc / k_nonzero ≥ Σc / n, a mean biased upwards. A trip whose pickup hour is outside the grid
// (stray-dated, D4) finds no grid row and is left out (D24).
fun Dataset<Row>.withHourlyCounts(counts: Dataset<Row>): Dataset<Row> = join(counts, ZONE_AND_HOUR, "left")
    .withColumn(TRIP_COUNT, zeroIfMissing())
    .withColumn(PICKUP_DATE, pickupDate())
    .withColumn(DAY_OF_WEEK, dayOfWeek())
    .withColumn(HOUR_OF_DAY, hourOfDay())

// Top-level on purpose: inside an extension on Dataset, a bare col() resolves to the receiver's Dataset.col.

private fun pickupHourIndex(): Column = hourIndex(col(PICKUP_DATETIME))

private fun zoneAndHour(): Array<Column> = ZONE_AND_HOUR.map(::col).toTypedArray()

private fun tripCount(): Column = count(lit(1)).alias(TRIP_COUNT)

private fun zeroIfMissing(): Column = coalesce(col(TRIP_COUNT), lit(0L))

// Back from the hour index to a DATE, with no time zone: days since 1970-01-01.
private fun pickupDate(): Column = date_from_unix_date(floor(col(HOUR_INDEX).divide(HOURS_PER_DAY)).cast("int"))

// 1 = Sunday … 7 = Saturday, as in M2.
private fun dayOfWeek(): Column = dayofweek(col(PICKUP_DATE))

private fun hourOfDay(): Column = pmod(col(HOUR_INDEX), lit(HOURS_PER_DAY))

// Join by name: one PULocationID and one hour_index in the result, no Scala Seq needed.
private val ZONE_AND_HOUR = arrayOf(PU_LOCATION_ID, HOUR_INDEX)
