package com.radeshgovind.zones

import com.radeshgovind.demand.DemandColumns.TRIP_COUNT
import com.radeshgovind.ingest.TripColumns.PU_LOCATION_ID
import com.radeshgovind.zones.ZoneStatsColumns.BASELINE_MEAN_TRIPS
import com.radeshgovind.zones.ZoneStatsColumns.BASELINE_STDDEV_TRIPS
import com.radeshgovind.zones.ZoneStatsColumns.IS_VOLUME_ANOMALY
import com.radeshgovind.zones.ZoneStatsColumns.MONTH_INDEX
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions.WindowSpec
import org.apache.spark.sql.functions.abs
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.stddev
import org.apache.spark.sql.functions.`when`

// Window 3 (D13): flag a zone-month whose trip count is more than 1.5 σ from the zone's own baseline, the mean and
// sample σ of its previous 3 months. The current month is left out of the baseline: with n points including it,
// |x − mean| / s can't exceed (n − 1) / √n, which is 1.15 for n = 3, so an inclusive 3-month window never flags.
fun Dataset<Row>.withVolumeAnomaly(): Dataset<Row> = withColumn(BASELINE_MEAN_TRIPS, baselineMean())
    .withColumn(BASELINE_STDDEV_TRIPS, baselineStddev())
    .withColumn(IS_VOLUME_ANOMALY, isVolumeAnomaly())

// Top-level on purpose: inside an extension on Dataset, a bare col() resolves to the receiver's Dataset.col.

private fun baselineMean(): Column = avg(col(TRIP_COUNT)).over(baseline())

private fun baselineStddev(): Column = stddev(col(TRIP_COUNT)).over(baseline())

// Months m-3..m-1 of the same zone. Months without trips have no row, so they are skipped, not counted as 0.
private fun baseline(): WindowSpec =
    Window.partitionBy(col(PU_LOCATION_ID)).orderBy(col(MONTH_INDEX)).rangeBetween(-BASELINE_MONTHS, -1)

// when() without otherwise() is null when σ is null (fewer than 2 baseline months): "not enough history", not false.
// A comparison instead of a z-score division, so σ = 0 (a flat history) flags any change instead of dividing by zero.
private fun isVolumeAnomaly(): Column = `when`(col(BASELINE_STDDEV_TRIPS).isNotNull, deviation().gt(threshold()))

private fun deviation(): Column = abs(col(TRIP_COUNT).minus(col(BASELINE_MEAN_TRIPS)))

private fun threshold(): Column = col(BASELINE_STDDEV_TRIPS).multiply(ANOMALY_SIGMAS)

private const val BASELINE_MONTHS = 3L
private const val ANOMALY_SIGMAS = 1.5
