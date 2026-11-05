package com.radeshgovind.ingest

import com.radeshgovind.cli.Operation
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class ZonesFileTest {
    @TempDir
    lateinit var input: Path

    @Test
    fun `reports the zone lookup when an operation that reads it is requested`() {
        assertEquals(input.resolve("taxi_zone_lookup.csv"), missingZonesFile(input, setOf(Operation.M1, Operation.M2)))
    }

    @Test
    fun `accepts a missing zone lookup when only M1 runs`() {
        assertNull(missingZonesFile(input, setOf(Operation.M1)))
    }

    @Test
    fun `accepts the zone lookup when it is there`() {
        Files.createFile(input.resolve("taxi_zone_lookup.csv"))
        assertNull(missingZonesFile(input, Operation.ALL))
    }
}
