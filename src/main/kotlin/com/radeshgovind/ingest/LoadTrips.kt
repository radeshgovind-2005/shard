package com.radeshgovind.ingest

import com.radeshgovind.cli.Fraction
import com.radeshgovind.ingest.TripColumns.SOURCE_MONTH
import com.radeshgovind.ingest.TripColumns.SOURCE_YEAR
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.regexp_extract
import java.nio.file.Path

// Reads the exact files, tags each trip with its file's month, then samples.
// Schema enforcement is M1's job (enforceSchema), not this file's.
fun SparkSession.loadTrips(files: TripFiles.Found, fraction: Fraction): Dataset<Row> =
    read().parquet(*paths(files)).withSourceMonth().sampledTo(fraction)

private fun paths(files: TripFiles.Found): Array<String> = files.paths.map(Path::toString).toTypedArray()

// D4: a trip's year and month come from its file name, not its pickup date.
// One projection directly on the scan, because the hidden _metadata column only resolves there.
private fun Dataset<Row>.withSourceMonth(): Dataset<Row> = select(*withSourceMonthColumns())

// Top-level, so col() is functions.col and not the receiver's member Dataset.col.
private fun withSourceMonthColumns(): Array<Column> =
    arrayOf(col("*"), fileNamePart(YEAR_GROUP).alias(SOURCE_YEAR), fileNamePart(MONTH_GROUP).alias(SOURCE_MONTH))

private fun fileNamePart(group: Int): Column = regexp_extract(col(FILE_PATH), SOURCE_FILE_PATTERN, group).cast("int")

private fun Dataset<Row>.sampledTo(fraction: Fraction): Dataset<Row> =
    if (fraction.isWhole) this else sample(false, fraction.value, SAMPLE_SEED)

private const val FILE_PATH = "_metadata.file_path"

// Matches anywhere in the path, so it also fits the part files inside a Parquet directory written by the tests.
private const val SOURCE_FILE_PATTERN = """yellow_tripdata_(\d{4})-(\d{2})\.parquet"""
private const val YEAR_GROUP = 1
private const val MONTH_GROUP = 2

// Fixed, so the same fraction keeps the same rows on every run.
private const val SAMPLE_SEED = 42L
