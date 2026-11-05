package com.radeshgovind.zones

import com.radeshgovind.demand.DemandColumns.TRIP_COUNT
import com.radeshgovind.ingest.TripColumns.FARE_AMOUNT
import com.radeshgovind.ingest.TripColumns.PU_LOCATION_ID
import com.radeshgovind.ingest.TripColumns.SOURCE_MONTH
import com.radeshgovind.ingest.TripColumns.SOURCE_YEAR
import com.radeshgovind.ingest.TripColumns.TRIP_DISTANCE
import com.radeshgovind.zones.ZoneStatsColumns.AVG_DISTANCE
import com.radeshgovind.zones.ZoneStatsColumns.AVG_FARE
import com.radeshgovind.zones.ZoneStatsColumns.MONTH_INDEX
import com.radeshgovind.zones.ZoneStatsColumns.STDDEV_DISTANCE
import com.radeshgovind.zones.ZoneStatsColumns.STDDEV_FARE
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.stddev

// One row per pickup zone and source month (D4): the trip count, plus mean and σ of fare and distance.
fun Dataset<Row>.zoneMonthStats(): Dataset<Row> = groupBy(*zoneAndMonth())
    .agg(tripCount(), *meansAndStddevs())
    .withColumn(MONTH_INDEX, monthIndex())

// Top-level on purpose: inside an extension on Dataset, a bare col() resolves to the receiver's Dataset.col.

private fun zoneAndMonth(): Array<Column> = arrayOf(col(PU_LOCATION_ID), col(SOURCE_YEAR), col(SOURCE_MONTH))

private fun tripCount(): Column = count(lit(1)).alias(TRIP_COUNT)

// D10: stddev is the sample σ (stddev_samp), so a zone-month with one trip gets a null σ, not 0.
private fun meansAndStddevs(): Array<Column> = arrayOf(
    avg(col(FARE_AMOUNT)).alias(AVG_FARE),
    stddev(col(FARE_AMOUNT)).alias(STDDEV_FARE),
    avg(col(TRIP_DISTANCE)).alias(AVG_DISTANCE),
    stddev(col(TRIP_DISTANCE)).alias(STDDEV_DISTANCE),
)

// D8: December 2024 → January 2025 is +1, so rangeBetween over this index never treats a gap as adjacent.
private fun monthIndex(): Column = col(SOURCE_YEAR).multiply(MONTHS_PER_YEAR).plus(col(SOURCE_MONTH))

private const val MONTHS_PER_YEAR = 12
