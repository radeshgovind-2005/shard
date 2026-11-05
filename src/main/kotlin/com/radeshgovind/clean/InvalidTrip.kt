package com.radeshgovind.clean

import com.radeshgovind.ingest.TripColumns.DO_LOCATION_ID
import com.radeshgovind.ingest.TripColumns.DROPOFF_DATETIME
import com.radeshgovind.ingest.TripColumns.FARE_AMOUNT
import com.radeshgovind.ingest.TripColumns.PASSENGER_COUNT
import com.radeshgovind.ingest.TripColumns.PICKUP_DATETIME
import com.radeshgovind.ingest.TripColumns.PU_LOCATION_ID
import com.radeshgovind.ingest.TripColumns.TRIP_DISTANCE
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.coalesce
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.not

// The spec's five criteria. This one list drives both the filter and the stats, so the two can never disagree.
// Each condition is true when the trip is invalid, and never null (D2).
enum class InvalidTrip(val statColumn: String) {
    BAD_FARE("bad_fare") {
        override fun condition(): Column = isNullOrAtMostZero(FARE_AMOUNT)
    },
    BAD_DISTANCE("bad_distance") {
        override fun condition(): Column = isNullOrAtMostZero(TRIP_DISTANCE)
    },
    BAD_PASSENGER_COUNT("bad_passenger_count") {
        override fun condition(): Column = isZeroOrMoreThanSix(PASSENGER_COUNT)
    },
    BAD_TIME_ORDER("bad_time_order") {
        override fun condition(): Column = isMissingOrNotAfterPickup()
    },
    BAD_ZONE("bad_zone") {
        override fun condition(): Column =
            isMissingOrOutsideZones(PU_LOCATION_ID).or(isMissingOrOutsideZones(DO_LOCATION_ID))
    },
    ;

    abstract fun condition(): Column

    companion object {
        fun anyInvalid(): Column = entries.map(InvalidTrip::condition).reduce(Column::or)
    }
}

private fun isNullOrAtMostZero(column: String): Column = neverNull(col(column).isNull().or(col(column).leq(0)))

// D1: a null passenger count is neither 0 nor > 6, so the trip stays.
private fun isZeroOrMoreThanSix(column: String): Column =
    neverNull(col(column).equalTo(0).or(col(column).gt(MAX_PASSENGERS)))

private fun isMissingOrNotAfterPickup(): Column = neverNull(
    col(PICKUP_DATETIME).isNull()
        .or(col(DROPOFF_DATETIME).isNull())
        .or(col(DROPOFF_DATETIME).leq(col(PICKUP_DATETIME))),
)

private fun isMissingOrOutsideZones(column: String): Column =
    neverNull(col(column).isNull().or(not(col(column).between(FIRST_ZONE, LAST_VALID_ZONE))))

// filter(not(x)) drops the rows where x is null, so a null criterion would silently remove the trip uncounted.
private fun neverNull(condition: Column): Column = coalesce(condition, lit(false))

private const val MAX_PASSENGERS = 6
private const val FIRST_ZONE = 1

// 264 and 265 are "Unknown" / "N/A" in the zone table.
private const val LAST_VALID_ZONE = 263
