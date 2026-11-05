package com.radeshgovind.ingest

import com.radeshgovind.cli.MonthRange
import java.nio.file.Files
import java.nio.file.Path
import java.time.YearMonth

sealed interface TripFiles {
    data class Found(val paths: List<Path>) : TripFiles

    data class Missing(val paths: List<Path>) : TripFiles
}

// Exact file list, no globs: before/after runs must read exactly the months that were asked for.
fun findTripFiles(input: Path, months: MonthRange): TripFiles {
    val expected = months.months().map { tripFile(input, it) }
    val missing = expected.filterNot(Files::isRegularFile)
    return if (missing.isEmpty()) TripFiles.Found(expected) else TripFiles.Missing(missing)
}

fun tripFile(input: Path, month: YearMonth): Path = input.resolve("trips").resolve("yellow_tripdata_$month.parquet")
