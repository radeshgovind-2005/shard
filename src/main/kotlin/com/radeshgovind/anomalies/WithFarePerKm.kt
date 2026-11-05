package com.radeshgovind.anomalies

import com.radeshgovind.anomalies.AnomalyColumns.FARE_PER_KM
import com.radeshgovind.ingest.TripColumns.FARE_AMOUNT
import com.radeshgovind.ingest.TripColumns.TRIP_DISTANCE
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col

// D15: the fare per kilometre. trip_distance is in miles. A built-in expression, no UDF.
// M1 keeps only trip_distance > 0 and fare_amount > 0, so the division never hits zero (an error under ANSI mode).
fun Dataset<Row>.withFarePerKm(): Dataset<Row> = withColumn(FARE_PER_KM, farePerKm())

// Top-level on purpose: inside an extension on Dataset, a bare col() resolves to the receiver's Dataset.col.
private fun farePerKm(): Column = col(FARE_AMOUNT).divide(col(TRIP_DISTANCE).multiply(KM_PER_MILE))

// The international mile, exactly.
const val KM_PER_MILE = 1.609344
