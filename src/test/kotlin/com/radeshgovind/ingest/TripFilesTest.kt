package com.radeshgovind.ingest

import com.radeshgovind.cli.MonthRange
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import java.time.YearMonth
import kotlin.test.Test
import kotlin.test.assertEquals

class TripFilesTest {
    @TempDir
    lateinit var input: Path

    private val firstQuarter = MonthRange(YearMonth.of(2025, 1), YearMonth.of(2025, 3))

    @Test
    fun `finds every month's file, in order`() {
        createTripFiles("2025-01", "2025-02", "2025-03")
        val expected = listOf("2025-01", "2025-02", "2025-03").map(::expectedPath)
        assertEquals(TripFiles.Found(expected), findTripFiles(input, firstQuarter))
    }

    @Test
    fun `reports exactly the missing months`() {
        createTripFiles("2025-01", "2025-03")
        assertEquals(TripFiles.Missing(listOf(expectedPath("2025-02"))), findTripFiles(input, firstQuarter))
    }

    private fun createTripFiles(vararg months: String) {
        Files.createDirectories(input.resolve("trips"))
        months.forEach { Files.createFile(expectedPath(it)) }
    }

    private fun expectedPath(month: String): Path = input.resolve("trips/yellow_tripdata_$month.parquet")
}
