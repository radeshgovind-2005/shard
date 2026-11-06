package com.radeshgovind.forecast

import com.radeshgovind.demand.DemandColumns.TRIP_COUNT
import com.radeshgovind.forecast.ForecastColumns.HOUR_INDEX
import com.radeshgovind.forecast.ForecastColumns.LAG_4W_TRIP_COUNT
import com.radeshgovind.ingest.TripColumns.PU_LOCATION_ID
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.functions.col

// D27: the spec's self-join on offset pickup dates. Each hour of `hourly` is moved 4 weeks forward and joined back
// onto the hour it lags, on zone and hour index: current.hour_index − 672 = previous.hour_index.
// Left join: an hour whose 4-weeks-back hour is before the range keeps its row, with a null lag ("no history", not 0).
// `plan` only decides how the join runs (LagJoinPlan); the join itself is the same in both plans.
fun Dataset<Row>.withFourWeekLag(hourly: Dataset<Row>, plan: LagJoinPlan): Dataset<Row> =
    join(hourly.lagSide().hintedFor(plan), ZONE_AND_HOUR, "left")

// The select gives the right side new column names (lag_4w_trip_count), so the self-join has no ambiguous columns.
private fun Dataset<Row>.lagSide(): Dataset<Row> = select(*fourWeeksLater())

// OPT-3 (H6): the lag side is the right side of a left join, so a broadcast hint builds it (BuildRight) and keeps
// every current row. 2,303,880 rows × 3 columns on 2025.
private fun Dataset<Row>.hintedFor(plan: LagJoinPlan): Dataset<Row> = when (plan) {
    LagJoinPlan.PLAIN -> this
    LagJoinPlan.BROADCAST_LAG_SIDE -> broadcast(this)
}

// Top-level on purpose: inside an extension on Dataset, a bare col() resolves to the receiver's Dataset.col.

private fun fourWeeksLater(): Array<Column> = arrayOf(
    col(PU_LOCATION_ID),
    col(HOUR_INDEX).plus(LAG_HOURS).alias(HOUR_INDEX),
    col(TRIP_COUNT).alias(LAG_4W_TRIP_COUNT),
)

// 4 weeks back, so the same day of week and hour of day. Zone-free: 28 days of 24 wall-clock hours (D24).
private const val LAG_HOURS = 28 * HOURS_PER_DAY

private val ZONE_AND_HOUR = arrayOf(PU_LOCATION_ID, HOUR_INDEX)
