package com.radeshgovind.cli

import java.time.YearMonth
import java.time.format.DateTimeParseException

data class MonthRange(val first: YearMonth, val last: YearMonth) {
    init {
        require(!first.isAfter(last)) { "month range is reversed: $first..$last" }
    }

    fun months(): List<YearMonth> = generateSequence(first, YearMonth::nextMonth).takeWhile(::isInRange).toList()

    override fun toString(): String = "$first..$last"

    private fun isInRange(month: YearMonth): Boolean = !month.isAfter(last)

    companion object {
        // 2009–2010 store lat/lon instead of zone IDs, so the zone-based operations can't use them.
        const val FIRST_SUPPORTED_YEAR = 2011

        private val SINGLE_YEAR = Regex("""^\d{4}$""")
        private val YEAR_RANGE = Regex("""^\d{4}-\d{4}$""")
        private val SINGLE_MONTH = Regex("""^\d{4}-\d{2}$""")

        fun parse(spec: String): MonthRange? = parseShape(spec)?.takeIf(::isSupported)

        private fun parseShape(spec: String): MonthRange? = when {
            SINGLE_YEAR.matches(spec) -> wholeYears(spec.toInt(), spec.toInt())
            YEAR_RANGE.matches(spec) -> yearRange(spec)
            SINGLE_MONTH.matches(spec) -> singleMonth(spec)
            else -> null
        }

        private fun yearRange(spec: String): MonthRange? {
            val (first, last) = spec.split("-").map(String::toInt)
            return wholeYears(first, last)
        }

        private fun wholeYears(first: Int, last: Int): MonthRange? =
            if (first <= last) MonthRange(YearMonth.of(first, 1), YearMonth.of(last, 12)) else null

        private fun singleMonth(spec: String): MonthRange? = yearMonthOrNull(spec)?.let { MonthRange(it, it) }

        private fun yearMonthOrNull(spec: String): YearMonth? = try {
            YearMonth.parse(spec)
        } catch (_: DateTimeParseException) {
            null
        }

        private fun isSupported(range: MonthRange): Boolean = range.first.year >= FIRST_SUPPORTED_YEAR
    }
}

private fun YearMonth.nextMonth(): YearMonth = plusMonths(1)
