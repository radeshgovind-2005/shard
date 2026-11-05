package com.radeshgovind.anomalies

import com.radeshgovind.anomalies.AnomalyColumns.BUCKET_MEAN_FARE_PER_KM
import com.radeshgovind.anomalies.AnomalyColumns.BUCKET_STDDEV_FARE_PER_KM
import com.radeshgovind.anomalies.AnomalyColumns.FARE_PER_KM
import com.radeshgovind.anomalies.AnomalyColumns.IS_FARE_ANOMALY
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.`when`

// D17: a trip is a fare anomaly when its fare_per_km is above its bucket's mean + 3σ (one-sided, strict).
// D18: when() without otherwise() is null when σ is null (a 1-trip bucket): the rate table counts it as not flagged.
// Samuelson: with the sample σ, no trip is more than (n − 1)/√n σ from its bucket's mean, which is under 3 for
// n ≤ 10, so a bucket of 10 trips or fewer can never flag one. σ = 0 flags nothing, without a special case.
fun Dataset<Row>.withFareAnomalyFlag(): Dataset<Row> = withColumn(IS_FARE_ANOMALY, isFareAnomaly())

// Top-level on purpose: inside an extension on Dataset, a bare col() resolves to the receiver's Dataset.col.

private fun isFareAnomaly(): Column = `when`(col(BUCKET_STDDEV_FARE_PER_KM).isNotNull, col(FARE_PER_KM).gt(threshold()))

private fun threshold(): Column =
    col(BUCKET_MEAN_FARE_PER_KM).plus(col(BUCKET_STDDEV_FARE_PER_KM).multiply(ANOMALY_SIGMAS))

const val ANOMALY_SIGMAS = 3.0
