package com.radeshgovind.zones

// M3's derived and output column names. pickup_borough and trip_count are shared with M2 (DemandColumns).
object ZoneStatsColumns {
    // D8: source_year * 12 + source_month, so consecutive months differ by exactly 1, also across years.
    const val MONTH_INDEX = "month_index"
    const val AVG_FARE = "avg_fare"
    const val STDDEV_FARE = "stddev_fare"
    const val AVG_DISTANCE = "avg_distance"
    const val STDDEV_DISTANCE = "stddev_distance"
    const val VOLUME_RANK = "volume_rank"
    const val ZONE_TRIPS_3M = "zone_trips_3m"
    const val BOROUGH_TRIPS_3M = "borough_trips_3m"
    const val TRIP_SHARE_3M = "trip_share_3m"
    const val BASELINE_MEAN_TRIPS = "baseline_mean_trips"
    const val BASELINE_STDDEV_TRIPS = "baseline_stddev_trips"
    const val IS_VOLUME_ANOMALY = "is_volume_anomaly"
}
