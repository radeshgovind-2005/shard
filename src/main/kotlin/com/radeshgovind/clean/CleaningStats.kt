package com.radeshgovind.clean

import com.radeshgovind.ingest.TripColumns.PASSENGER_COUNT
import com.radeshgovind.ingest.TripColumns.SOURCE_YEAR
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.sum

// M1's output: one row per source year, with the trips removed under each criterion.
// The criteria overlap (D5), so they can add up to more than removed_trips.
// One groupBy holds every count, so the input is scanned once, not once per criterion (D6).
// No orderBy: it would add a sort and a shuffle for nothing. Sort only when printing.
fun Dataset<Row>.cleaningStats(): Dataset<Row> = groupBy(SOURCE_YEAR)
    .agg(rawTrips(), *criterionCounts(), removedTrips(), nullPassengerCounts())
    .withColumn(KEPT_TRIPS, keptTrips())
    .select(*statColumns())

// The helpers below are top-level on purpose: inside an extension on Dataset, a bare col() resolves to the
// receiver's member Dataset.col, not functions.col.

private fun rawTrips(): Column = count(lit(1)).alias(RAW_TRIPS)

private fun criterionCounts(): Array<Column> =
    InvalidTrip.entries.map { countWhere(it.condition()).alias(it.statColumn) }.toTypedArray()

private fun removedTrips(): Column = countWhere(InvalidTrip.anyInvalid()).alias(REMOVED_TRIPS)

// Informational: these trips are kept (D1).
private fun nullPassengerCounts(): Column = countWhere(col(PASSENGER_COUNT).isNull()).alias(NULL_PASSENGER_COUNT)

private fun keptTrips(): Column = col(RAW_TRIPS).minus(col(REMOVED_TRIPS))

private fun statColumns(): Array<Column> = STAT_COLUMNS.map(::col).toTypedArray()

private fun countWhere(condition: Column): Column = sum(condition.cast("long"))

private const val RAW_TRIPS = "raw_trips"
private const val REMOVED_TRIPS = "removed_trips"
private const val KEPT_TRIPS = "kept_trips"
private const val NULL_PASSENGER_COUNT = "null_passenger_count"

// The output's column order, fixed.
private val STAT_COLUMNS: List<String> =
    listOf(SOURCE_YEAR, RAW_TRIPS) +
        InvalidTrip.entries.map(InvalidTrip::statColumn) +
        listOf(REMOVED_TRIPS, KEPT_TRIPS, NULL_PASSENGER_COUNT)
