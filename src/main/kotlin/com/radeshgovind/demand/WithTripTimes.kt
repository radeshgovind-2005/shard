package com.radeshgovind.demand

import com.radeshgovind.demand.DemandColumns.DAY_OF_WEEK
import com.radeshgovind.demand.DemandColumns.DURATION_MIN
import com.radeshgovind.demand.DemandColumns.HOUR_OF_DAY
import com.radeshgovind.ingest.TripColumns.DROPOFF_DATETIME
import com.radeshgovind.ingest.TripColumns.PICKUP_DATETIME
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.dayofweek
import org.apache.spark.sql.functions.hour

// Day and hour come from the pickup, in NYC wall-clock time (the timestamps are TIMESTAMP_NTZ, D3).
fun Dataset<Row>.withTripTimes(): Dataset<Row> = withColumn(DAY_OF_WEEK, dayOfWeek())
    .withColumn(HOUR_OF_DAY, hourOfDay())
    .withColumn(DURATION_MIN, durationMin())

// 1 = Sunday … 7 = Saturday.
private fun dayOfWeek(): Column = dayofweek(col(PICKUP_DATETIME))

private fun hourOfDay(): Column = hour(col(PICKUP_DATETIME))

// D7: NTZ minus NTZ is an INTERVAL DAY TO SECOND computed without any time zone. timestamp_diff() would first cast
// both sides to TIMESTAMP in the session time zone, so local (Europe/Lisbon) and Docker (UTC) could disagree.
private fun durationMin(): Column = elapsedSeconds().divide(SECONDS_PER_MINUTE)

private fun elapsedSeconds(): Column =
    col(DROPOFF_DATETIME).minus(col(PICKUP_DATETIME)).cast("interval second").cast("decimal(18,6)")

private const val SECONDS_PER_MINUTE = 60.0
