package com.radeshgovind.forecast

import com.radeshgovind.ingest.TripColumns.PU_LOCATION_ID
import com.radeshgovind.ingest.ZoneColumns.LOCATION_ID
import com.radeshgovind.ingest.ZoneColumns.SERVICE_ZONE
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col

// D29: the zone's service_zone, through a plain inner join with no hint (the baseline, like M2's withPickupBorough).
// Inner is safe: the grid holds zones 1–263, and the lookup has every one of them.
fun Dataset<Row>.withServiceZone(zones: Dataset<Row>): Dataset<Row> =
    join(zones.select(*zoneIdAndServiceZone()), isPickupZone())

// Top-level on purpose: inside an extension on Dataset, a bare col() resolves to the receiver's Dataset.col.

private fun zoneIdAndServiceZone(): Array<Column> = arrayOf(col(LOCATION_ID), col(SERVICE_ZONE))

// The names are unique across both sides, so the unqualified columns are unambiguous.
private fun isPickupZone(): Column = col(PU_LOCATION_ID).equalTo(col(LOCATION_ID))
