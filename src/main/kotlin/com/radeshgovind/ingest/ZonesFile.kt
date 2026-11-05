package com.radeshgovind.ingest

import com.radeshgovind.cli.Operation
import java.nio.file.Files
import java.nio.file.Path

fun zonesFile(input: Path): Path = input.resolve("taxi_zone_lookup.csv")

// Checked before Spark starts, like the trip files: null when no requested operation reads the zones,
// or when the file is there.
fun missingZonesFile(input: Path, operations: Set<Operation>): Path? =
    zonesFile(input).takeIf { readsZones(operations) && !Files.isRegularFile(it) }

private fun readsZones(operations: Set<Operation>): Boolean = operations.any(Operation::readsZones)
