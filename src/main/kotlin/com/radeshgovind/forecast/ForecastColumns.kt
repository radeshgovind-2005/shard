package com.radeshgovind.forecast

// O3's derived and output column names. day_of_week, hour_of_day and trip_count are shared with M2 (DemandColumns).
object ForecastColumns {
    // D24: days since 1970-01-01 × 24 + hour, in NYC wall-clock time. Consecutive hours differ by exactly 1.
    const val HOUR_INDEX = "hour_index"
    const val PICKUP_DATE = "pickup_date"
    const val LAG_4W_TRIP_COUNT = "lag_4w_trip_count"
    const val HIST_MEAN_TRIPS = "hist_mean_trips"
    const val HIST_VAR_TRIPS = "hist_var_trips"
    const val DEMAND_INDEX = "demand_index"
    const val IS_DEMAND_SPIKE = "is_demand_spike"
}
