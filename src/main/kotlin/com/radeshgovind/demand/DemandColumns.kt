package com.radeshgovind.demand

// M2's derived and output column names.
object DemandColumns {
    const val PICKUP_BOROUGH = "pickup_borough"

    // Spark's dayofweek: 1 = Sunday, 2 = Monday, …, 7 = Saturday.
    const val DAY_OF_WEEK = "day_of_week"
    const val HOUR_OF_DAY = "hour_of_day"
    const val DURATION_MIN = "duration_min"
    const val TRIP_COUNT = "trip_count"
    const val AVG_FARE = "avg_fare"
    const val AVG_DURATION_MIN = "avg_duration_min"
    const val AVG_DISTANCE = "avg_distance"
}
