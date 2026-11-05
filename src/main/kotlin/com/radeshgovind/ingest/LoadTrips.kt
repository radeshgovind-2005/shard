package com.radeshgovind.ingest

import com.radeshgovind.cli.Fraction
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import java.nio.file.Path

// Schema enforcement belongs to M1 (enforceSchema), not here: this only reads and samples.
fun SparkSession.loadTrips(files: TripFiles.Found, fraction: Fraction): Dataset<Row> =
    read().parquet(*files.paths.map(Path::toString).toTypedArray()).sampledTo(fraction)

private fun Dataset<Row>.sampledTo(fraction: Fraction): Dataset<Row> =
    if (fraction.isWhole) this else sample(false, fraction.value, SAMPLE_SEED)

// Fixed, so the same fraction keeps the same rows on every run.
private const val SAMPLE_SEED = 42L
