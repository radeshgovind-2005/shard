package com.radeshgovind.ingest

import com.radeshgovind.ingest.ZoneColumns.BOROUGH
import com.radeshgovind.ingest.ZoneColumns.LOCATION_ID
import com.radeshgovind.ingest.ZoneColumns.SERVICE_ZONE
import com.radeshgovind.ingest.ZoneColumns.ZONE
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import java.nio.file.Path

fun SparkSession.loadZones(input: Path): Dataset<Row> =
    read().option("header", "true").schema(ZONE_SCHEMA).csv(zonesFile(input).toString())

// Explicit schema: inferSchema would cost an extra pass over the file.
private const val ZONE_SCHEMA = "$LOCATION_ID INT, $BOROUGH STRING, $ZONE STRING, $SERVICE_ZONE STRING"
