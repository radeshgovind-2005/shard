package com.radeshgovind.anomalies

// O1's derived and output column names. pickup_borough, day_of_week and hour_of_day are shared with M2 (DemandColumns).
object AnomalyColumns {
    const val FARE_PER_KM = "fare_per_km"
    const val BUCKET_TRIPS = "bucket_trips"
    const val BUCKET_MEAN_FARE_PER_KM = "bucket_mean_fare_per_km"
    const val BUCKET_STDDEV_FARE_PER_KM = "bucket_stddev_fare_per_km"
    const val IS_FARE_ANOMALY = "is_fare_anomaly"
    const val DAY_TYPE = "day_type"
    const val TRIPS = "trips"
    const val FLAGGED_TRIPS = "flagged_trips"
    const val ANOMALY_RATE = "anomaly_rate"

    // D19: the two values of day_type.
    const val WEEKDAY = "weekday"
    const val WEEKEND = "weekend"
}
