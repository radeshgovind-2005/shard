package com.radeshgovind.cli

import java.time.YearMonth
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

class MonthRangeTest {
    @Test
    fun `lists every month in order across a year boundary`() {
        val range = MonthRange(month("2024-11"), month("2025-02"))
        assertEquals(listOf("2024-11", "2024-12", "2025-01", "2025-02").map(::month), range.months())
    }

    @Test
    fun `a whole year has twelve months`() {
        assertEquals(12, MonthRange.parse("2025")?.months()?.size)
    }

    @Test
    fun `parses the three formats`() {
        assertEquals(MonthRange(month("2025-01"), month("2025-12")), MonthRange.parse("2025"))
        assertEquals(MonthRange(month("2011-01"), month("2012-12")), MonthRange.parse("2011-2012"))
        assertEquals(MonthRange(month("2025-07"), month("2025-07")), MonthRange.parse("2025-07"))
    }

    @Test
    fun `rejects years without zone IDs`() {
        assertNull(MonthRange.parse("2010"))
        assertNull(MonthRange.parse("2010-2011"))
        assertNull(MonthRange.parse("2010-12"))
    }

    @Test
    fun `cannot be built reversed`() {
        assertFailsWith<IllegalArgumentException> { MonthRange(month("2025-02"), month("2025-01")) }
    }

    private fun month(text: String): YearMonth = YearMonth.parse(text)
}
