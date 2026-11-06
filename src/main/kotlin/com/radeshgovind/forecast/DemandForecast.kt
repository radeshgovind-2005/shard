package com.radeshgovind.forecast

import com.radeshgovind.demand.DemandColumns.DAY_OF_WEEK
import com.radeshgovind.demand.DemandColumns.HOUR_OF_DAY
import com.radeshgovind.demand.DemandColumns.TRIP_COUNT
import com.radeshgovind.forecast.ForecastColumns.DEMAND_INDEX
import com.radeshgovind.forecast.ForecastColumns.HIST_MEAN_TRIPS
import com.radeshgovind.forecast.ForecastColumns.HIST_VAR_TRIPS
import com.radeshgovind.forecast.ForecastColumns.IS_DEMAND_SPIKE
import com.radeshgovind.forecast.ForecastColumns.LAG_4W_TRIP_COUNT
import com.radeshgovind.forecast.ForecastColumns.PICKUP_DATE
import com.radeshgovind.ingest.TripColumns.PU_LOCATION_ID
import com.radeshgovind.ingest.ZoneColumns.SERVICE_ZONE
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col

// O3's output: one row per zone and hour of the grid (D23), from the cleaned trips.
// The grid, the zones and the lag join's plan come in as parameters, so the grid's size and the join hint stay
// visible in Pipeline.kt. `hourly` feeds both sides of the 4-week self-join (D27). Nothing is cached.
// No orderBy: it would add a sort and a shuffle for nothing.
fun Dataset<Row>.demandForecast(grid: Dataset<Row>, zones: Dataset<Row>, lagJoin: LagJoinPlan): Dataset<Row> {
    val hourly = grid.withHourlyCounts(hourlyTripCounts())
    return hourly.withBucketStats()
        .withFourWeekLag(hourly, lagJoin)
        .withDemandIndex()
        .withServiceZone(zones)
        .select(*outputColumns())
}

// Fixes the column order and drops hour_index and the join's LocationID.
private fun outputColumns(): Array<Column> = OUTPUT_COLUMNS.map(::col).toTypedArray()

private val OUTPUT_COLUMNS = listOf(
    PU_LOCATION_ID, SERVICE_ZONE, PICKUP_DATE, DAY_OF_WEEK, HOUR_OF_DAY,
    TRIP_COUNT, LAG_4W_TRIP_COUNT, HIST_MEAN_TRIPS, HIST_VAR_TRIPS, DEMAND_INDEX, IS_DEMAND_SPIKE,
)
