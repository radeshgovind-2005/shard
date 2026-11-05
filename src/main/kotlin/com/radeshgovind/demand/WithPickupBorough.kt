package com.radeshgovind.demand

import com.radeshgovind.demand.DemandColumns.PICKUP_BOROUGH
import com.radeshgovind.ingest.TripColumns.PU_LOCATION_ID
import com.radeshgovind.ingest.ZoneColumns.BOROUGH
import com.radeshgovind.ingest.ZoneColumns.LOCATION_ID
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col

// Adds the pickup zone's borough. A plain inner join with no hint: this is the baseline.
// The zones come in as a parameter, so a join hint can later be added in Pipeline.kt, where it stays visible.
// Inner is safe: M1 keeps only zones 1–263, and the lookup holds every one of them.
fun Dataset<Row>.withPickupBorough(zones: Dataset<Row>): Dataset<Row> =
    join(zones.select(*zoneIdAndPickupBorough()), isPickupZone())

// Top-level on purpose: inside an extension on Dataset, a bare col() resolves to the receiver's Dataset.col.

private fun zoneIdAndPickupBorough(): Array<Column> = arrayOf(col(LOCATION_ID), col(BOROUGH).alias(PICKUP_BOROUGH))

// The names are unique across both sides, so the unqualified columns are unambiguous.
private fun isPickupZone(): Column = col(PU_LOCATION_ID).equalTo(col(LOCATION_ID))
