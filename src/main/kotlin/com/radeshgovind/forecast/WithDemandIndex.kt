package com.radeshgovind.forecast

import com.radeshgovind.demand.DemandColumns.TRIP_COUNT
import com.radeshgovind.forecast.ForecastColumns.DEMAND_INDEX
import com.radeshgovind.forecast.ForecastColumns.HIST_MEAN_TRIPS
import com.radeshgovind.forecast.ForecastColumns.IS_DEMAND_SPIKE
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.coalesce
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.`when`

// D28: demand index = this hour's count ÷ its bucket's mean, and a spike when it's above 2.
fun Dataset<Row>.withDemandIndex(): Dataset<Row> = withColumn(DEMAND_INDEX, demandIndex())
    .withColumn(IS_DEMAND_SPIKE, isDemandSpike())

// Top-level on purpose: inside an extension on Dataset, a bare col() resolves to the receiver's Dataset.col.

// Null when the mean is 0: ANSI (on in Spark 4) raises on a division by zero, and 0/0 has no meaning anyway.
private fun demandIndex(): Column = `when`(col(HIST_MEAN_TRIPS).gt(0), col(TRIP_COUNT).divide(col(HIST_MEAN_TRIPS)))

// The mean includes this hour (D26), so a mean of 0 means a count of 0: no demand, never a spike, so false, not null.
private fun isDemandSpike(): Column = coalesce(col(DEMAND_INDEX).gt(SPIKE_THRESHOLD), lit(false))

private const val SPIKE_THRESHOLD = 2.0
