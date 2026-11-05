package com.radeshgovind.zones

import com.radeshgovind.demand.DemandColumns.PICKUP_BOROUGH
import com.radeshgovind.demand.DemandColumns.TRIP_COUNT
import com.radeshgovind.ingest.TripColumns.PU_LOCATION_ID
import com.radeshgovind.zones.ZoneStatsColumns.BOROUGH_TRIPS_3M
import com.radeshgovind.zones.ZoneStatsColumns.MONTH_INDEX
import com.radeshgovind.zones.ZoneStatsColumns.TRIP_SHARE_3M
import com.radeshgovind.zones.ZoneStatsColumns.ZONE_TRIPS_3M
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions.WindowSpec
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.sum

// Window 2 (D12): the zone's trips over months m-2..m divided by its borough's trips over the same months.
// Both frames are RANGE frames over month_index, so a month without trips counts as 0, never as "the previous row".
fun Dataset<Row>.withRollingTripShare(): Dataset<Row> = withColumn(ZONE_TRIPS_3M, tripsOverLast3Months(zoneByMonth()))
    .withColumn(BOROUGH_TRIPS_3M, tripsOverLast3Months(boroughByMonth()))
    .withColumn(TRIP_SHARE_3M, tripShare())

private fun tripsOverLast3Months(partition: WindowSpec): Column =
    sum(col(TRIP_COUNT)).over(partition.rangeBetween(-(SHARE_MONTHS - 1), 0))

private fun zoneByMonth(): WindowSpec = Window.partitionBy(col(PU_LOCATION_ID)).orderBy(col(MONTH_INDEX))

// A RANGE frame takes every zone-row of the borough whose month is in m-2..m: the borough's 3-month total, in one
// window, with no separate borough-month aggregate.
private fun boroughByMonth(): WindowSpec = Window.partitionBy(col(PICKUP_BOROUGH)).orderBy(col(MONTH_INDEX))

// The borough total includes the zone's own trips (≥ 1), so this never divides by zero (ANSI is on in Spark 4).
private fun tripShare(): Column = col(ZONE_TRIPS_3M).cast("double").divide(col(BOROUGH_TRIPS_3M))

private const val SHARE_MONTHS = 3L
