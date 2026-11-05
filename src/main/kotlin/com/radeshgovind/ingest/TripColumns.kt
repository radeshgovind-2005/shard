package com.radeshgovind.ingest

// The 11 trip columns the spec uses, plus the month of the file each trip came from (D4).
object TripColumns {
    const val PICKUP_DATETIME = "tpep_pickup_datetime"
    const val DROPOFF_DATETIME = "tpep_dropoff_datetime"
    const val PU_LOCATION_ID = "PULocationID"
    const val DO_LOCATION_ID = "DOLocationID"
    const val PASSENGER_COUNT = "passenger_count"
    const val RATECODE_ID = "RatecodeID"
    const val PAYMENT_TYPE = "payment_type"
    const val TRIP_DISTANCE = "trip_distance"
    const val FARE_AMOUNT = "fare_amount"
    const val TIP_AMOUNT = "tip_amount"
    const val TOTAL_AMOUNT = "total_amount"
    const val SOURCE_YEAR = "source_year"
    const val SOURCE_MONTH = "source_month"
}
