package com.radeshgovind.zones

import com.radeshgovind.demand.DemandColumns.PICKUP_BOROUGH
import com.radeshgovind.demand.DemandColumns.TRIP_COUNT
import com.radeshgovind.zones.ZoneStatsColumns.MONTH_INDEX
import com.radeshgovind.zones.ZoneStatsColumns.VOLUME_RANK
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions.WindowSpec
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.rank

// Window 1: rank 1 = the busiest zone of its borough that month.
// D11: rank(), so equal counts share a rank and the next rank is skipped (5, 3, 3 → 1, 2, 2).
fun Dataset<Row>.withVolumeRank(): Dataset<Row> = withColumn(VOLUME_RANK, volumeRank())

private fun volumeRank(): Column = rank().over(boroughMonthByVolume())

private fun boroughMonthByVolume(): WindowSpec =
    Window.partitionBy(col(PICKUP_BOROUGH), col(MONTH_INDEX)).orderBy(col(TRIP_COUNT).desc())
