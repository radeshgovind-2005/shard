package com.radeshgovind.forecast

import com.radeshgovind.cli.MonthRange
import com.radeshgovind.forecast.ForecastColumns.HOUR_INDEX
import com.radeshgovind.ingest.TripColumns.PU_LOCATION_ID
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

// D25: every zone × every hour of the requested months, so an hour without trips is a row (counted as 0 later),
// not a gap. 263 × 744 = 195,672 rows for 2025-01, 263 × 8,760 = 2,303,880 for 2025.
// The hour bounds are computed on the driver from LocalDate, so no time zone is involved (D24).
fun SparkSession.zoneHourGrid(months: MonthRange, zoneIds: IntRange = ALL_ZONES): Dataset<Row> =
    zonesOf(zoneIds).crossJoin(hoursOf(months))

private fun SparkSession.zonesOf(zoneIds: IntRange): Dataset<Row> =
    range(zoneIds.first.toLong(), zoneIds.last + 1L).select(idAs(PU_LOCATION_ID))

private fun SparkSession.hoursOf(months: MonthRange): Dataset<Row> =
    range(firstHour(months), endHour(months)).select(idAs(HOUR_INDEX))

private fun firstHour(months: MonthRange): Long = months.first.atDay(1).toEpochDay() * HOURS_PER_DAY

// Exclusive: the first hour of the month after the range.
private fun endHour(months: MonthRange): Long = months.last.plusMonths(1).atDay(1).toEpochDay() * HOURS_PER_DAY

// range() gives a bigint "id". INT matches PULocationID and the trips' hour index, so the join keys share a type.
private fun idAs(name: String): Column = col("id").cast("int").alias(name)

// What M1 keeps: 264 and 265 are "Unknown" / "N/A".
private val ALL_ZONES = 1..263
