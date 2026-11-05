package com.radeshgovind.anomalies

import com.radeshgovind.anomalies.AnomalyColumns.DAY_TYPE
import com.radeshgovind.anomalies.AnomalyColumns.WEEKDAY
import com.radeshgovind.anomalies.AnomalyColumns.WEEKEND
import com.radeshgovind.demand.DemandColumns.DAY_OF_WEEK
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.`when`

// D19: weekend = a pickup on Saturday or Sunday in NYC wall-clock time, from day_of_week (withTripTimes, D3).
// A Friday 23:50 → Saturday 00:20 trip is a weekday trip.
fun Dataset<Row>.withDayType(): Dataset<Row> = withColumn(DAY_TYPE, dayType())

// Top-level on purpose: inside an extension on Dataset, a bare col() resolves to the receiver's Dataset.col.
// day_of_week is never null (M1 guarantees a pickup before the dropoff, so the pickup isn't null).
private fun dayType(): Column = `when`(col(DAY_OF_WEEK).isin(SUNDAY, SATURDAY), WEEKEND).otherwise(WEEKDAY)

// Spark's dayofweek: 1 = Sunday … 7 = Saturday.
private const val SUNDAY = 1
private const val SATURDAY = 7
