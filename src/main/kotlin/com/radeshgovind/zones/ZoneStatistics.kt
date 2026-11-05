package com.radeshgovind.zones

import com.radeshgovind.demand.DemandColumns.PICKUP_BOROUGH
import com.radeshgovind.demand.DemandColumns.TRIP_COUNT
import com.radeshgovind.demand.withPickupBorough
import com.radeshgovind.ingest.TripColumns.PU_LOCATION_ID
import com.radeshgovind.ingest.TripColumns.SOURCE_MONTH
import com.radeshgovind.ingest.TripColumns.SOURCE_YEAR
import com.radeshgovind.zones.ZoneStatsColumns.AVG_DISTANCE
import com.radeshgovind.zones.ZoneStatsColumns.AVG_FARE
import com.radeshgovind.zones.ZoneStatsColumns.BASELINE_MEAN_TRIPS
import com.radeshgovind.zones.ZoneStatsColumns.BASELINE_STDDEV_TRIPS
import com.radeshgovind.zones.ZoneStatsColumns.BOROUGH_TRIPS_3M
import com.radeshgovind.zones.ZoneStatsColumns.IS_VOLUME_ANOMALY
import com.radeshgovind.zones.ZoneStatsColumns.MONTH_INDEX
import com.radeshgovind.zones.ZoneStatsColumns.STDDEV_DISTANCE
import com.radeshgovind.zones.ZoneStatsColumns.STDDEV_FARE
import com.radeshgovind.zones.ZoneStatsColumns.TRIP_SHARE_3M
import com.radeshgovind.zones.ZoneStatsColumns.VOLUME_RANK
import com.radeshgovind.zones.ZoneStatsColumns.ZONE_TRIPS_3M
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col

// M3's output: one row per (PULocationID, month) with at least one trip, from the cleaned trips.
// D9: aggregate first, then join the borough, so the join and all three windows run on zone-month rows, not trips.
// No orderBy: it would add a sort and a shuffle for nothing.
fun Dataset<Row>.zoneStatistics(zones: Dataset<Row>): Dataset<Row> = zoneMonthStats()
    .withPickupBorough(zones)
    .withVolumeRank()
    .withRollingTripShare()
    .withVolumeAnomaly()
    .select(*outputColumns())

// Fixes the column order and drops the join's LocationID (the same value as PULocationID).
private fun outputColumns(): Array<Column> = OUTPUT_COLUMNS.map(::col).toTypedArray()

private val OUTPUT_COLUMNS = listOf(
    PU_LOCATION_ID, SOURCE_YEAR, SOURCE_MONTH, MONTH_INDEX, PICKUP_BOROUGH,
    TRIP_COUNT, AVG_FARE, STDDEV_FARE, AVG_DISTANCE, STDDEV_DISTANCE,
    VOLUME_RANK, ZONE_TRIPS_3M, BOROUGH_TRIPS_3M, TRIP_SHARE_3M,
    BASELINE_MEAN_TRIPS, BASELINE_STDDEV_TRIPS, IS_VOLUME_ANOMALY,
)
