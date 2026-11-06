package com.radeshgovind.forecast

import com.radeshgovind.cli.MonthRange
import com.radeshgovind.ingest.ENFORCED_SCHEMA
import com.radeshgovind.localTestSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructType
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import java.time.DayOfWeek
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.YearMonth
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

// (zone, date, hour of day)
private typealias HourKey = Triple<Int, LocalDate, Int>

class DemandForecastTest {
    @Test
    fun `demandForecast has the O3 schema`() = forEachPlan { plan ->
        val forecast = fixture(TRIPS).demandForecast(janFebGrid(), zones(), plan)
        assertEquals(EXPECTED_SCHEMA, forecast.schema().fields().map { it.name() to it.dataType() }, "$plan")
    }

    // OPT-3 (H6): both lag-join plans give every grid cell the same values; doubles within TOLERANCE.
    @Test
    fun `both lag-join plans give the same forecast`() {
        val plain = janFeb(LagJoinPlan.PLAIN)
        val broadcast = janFeb(LagJoinPlan.BROADCAST_LAG_SIDE)
        assertEquals(plain.keys, broadcast.keys)
        plain.forEach { (key, hour) -> assertSameHour(hour, broadcast.getValue(key), "$key") }
    }

    // OPT-3 (H6): the hint is what makes the plans differ, so it must be on the lag side's join only in that plan.
    // On this tiny grid Catalyst broadcasts either way, so the physical join can't tell them apart.
    @Test
    fun `only the broadcast plan hints the lag side of the self-join`() {
        assertEquals(1, broadcastHintsIn(LagJoinPlan.BROADCAST_LAG_SIDE))
        assertEquals(0, broadcastHintsIn(LagJoinPlan.PLAIN))
    }

    // D25: one row per zone and hour, whether it had trips or not.
    @Test
    fun `the grid has a row for every zone and hour`() = forEachPlan { plan ->
        assertEquals(2 * JAN_FEB_DAYS * 24, janFeb(plan).size, "$plan")
    }

    // D25: 3 trips in 1 of the 9 Wednesdays at 10:00. Zero-filled, μ = 3/9; averaging only the non-empty hour would
    // give 3. var_samp of (3, 0 × 8) = (8 × (1/3)² + (8/3)²) / 8 = 1.
    @Test
    fun `an hour without trips counts as 0 in the mean`() = forEachPlan { plan ->
        val wednesdayTen = bucket(plan, 1, DayOfWeek.WEDNESDAY, 10)
        assertEquals(9, wednesdayTen.size, "$plan")
        wednesdayTen.values.forEach {
            assertEquals(1.0 / 3.0, it.histMean, TOLERANCE, "$plan")
            assertEquals(1.0, it.histVar, TOLERANCE, "$plan")
        }
        assertEquals(3L, wednesdayTen.getValue(Triple(1, LocalDate.of(2025, 1, 15), 10)).tripCount, "$plan")
    }

    // D27: 2025-02-03 − 28 days = 2025-01-06, across the month boundary. January 6's own lag is in December, outside
    // the range: null, not 0. An hour whose lag hour had no trips gets 0.
    @Test
    fun `the lag is the count exactly 28 days back`() = forEachPlan { plan ->
        val hours = janFeb(plan)
        assertEquals(2L, hours.getValue(Triple(2, LocalDate.of(2025, 2, 3), 8)).lag4w, "$plan")
        assertNull(hours.getValue(Triple(2, LocalDate.of(2025, 1, 6), 8)).lag4w, "$plan")
        assertEquals(0L, hours.getValue(Triple(2, LocalDate.of(2025, 2, 3), 9)).lag4w, "$plan")
        assertEquals(2L, hours.getValue(Triple(2, LocalDate.of(2025, 1, 6), 8)).tripCount, "$plan")
    }

    // D28: zone 1's Thursdays at 18:00 have 1 trip each, except 10 on January 23. 9 Thursdays, 18 trips: μ = 2.
    @Test
    fun `a spike hour has a demand index above 2 and is flagged`() = forEachPlan { plan ->
        val thursdays = bucket(plan, 1, DayOfWeek.THURSDAY, 18)
        assertEquals(9, thursdays.size, "$plan")
        val spike = thursdays.getValue(Triple(1, LocalDate.of(2025, 1, 23), 18))
        assertEquals(5.0, spike.demandIndex!!, TOLERANCE, "$plan")
        assertTrue(spike.isSpike, "$plan")
        (thursdays - Triple(1, LocalDate.of(2025, 1, 23), 18)).values.forEach {
            assertEquals(0.5, it.demandIndex!!, TOLERANCE, "$plan")
            assertFalse(it.isSpike, "$plan")
        }
    }

    // D28: a bucket with no trips at all has μ = 0: no demand index, and not a spike.
    @Test
    fun `a zero mean gives a null demand index and no spike`() = forEachPlan { plan ->
        val empty = janFeb(plan).getValue(Triple(1, LocalDate.of(2025, 1, 23), 3))
        assertEquals(0.0, empty.histMean, TOLERANCE, "$plan")
        assertNull(empty.demandIndex, "$plan")
        assertFalse(empty.isSpike, "$plan")
    }

    // D24: the trip dated 2024-12-31 has no grid row, so it's left out.
    @Test
    fun `trip counts add up to the input trips inside the range`() = forEachPlan { plan ->
        assertEquals(TRIPS.size.toLong() - 1, janFeb(plan).values.sumOf(Hour::tripCount), "$plan")
    }

    @Test
    fun `service zone comes from the zone lookup`() = forEachPlan { plan ->
        val serviceZones = janFeb(plan).entries.associate { it.key.first to it.value.serviceZone }
        assertEquals(mapOf(1 to "EWR", 2 to "Boro Zone"), serviceZones, "$plan")
    }

    // D24: Lisbon springs forward on 2025-03-30 (01:00 → 02:00) and falls back on 2025-10-26 (02:00 → 01:00).
    // A TIMESTAMP cast in that session would shift or merge these hours; the NTZ hour index doesn't.
    @Test
    fun `the hour index ignores Lisbon's DST switches`() {
        withSessionTimeZone("Europe/Lisbon") {
            val indices = fixture(DST_TRIPS).select(hourIndex(col(PICKUP))).collectAsList().map { it.getInt(0) }
            assertEquals(DST_TRIPS.map { it.pickup.toLocalDate().toEpochDay().toInt() * 24 + it.pickup.hour }, indices)
        }
    }

    @Test
    fun `each trip keeps its own hour across Lisbon's DST switches`() {
        withSessionTimeZone("Europe/Lisbon") {
            val hours = forecastHours(fixture(DST_TRIPS), spark.zoneHourGrid(MARCH_TO_OCTOBER, 1..1), LagJoinPlan.PLAIN)
            DST_DATES.forEach { date ->
                assertEquals((0..23).toList(), hours.keys.filter { it.second == date }.map { it.third }.sorted())
            }
            DST_TRIPS.forEach {
                assertEquals(1L, hours.getValue(Triple(1, it.pickup.toLocalDate(), it.pickup.hour)).tripCount)
            }
        }
    }

    private fun forEachPlan(check: (LagJoinPlan) -> Unit) = LagJoinPlan.entries.forEach(check)

    private fun janFeb(plan: LagJoinPlan): Map<HourKey, Hour> = janFebHours.getValue(plan)

    private fun bucket(plan: LagJoinPlan, zone: Int, day: DayOfWeek, hour: Int): Map<HourKey, Hour> =
        janFeb(plan).filterKeys { it.first == zone && it.second.dayOfWeek == day && it.third == hour }

    // Exact on everything but the doubles, which may differ in the last bits if the summation order changes.
    private fun assertSameHour(expected: Hour, actual: Hour, key: String) {
        assertEquals(expected.exactFields(), actual.exactFields(), key)
        assertEquals(expected.histMean, actual.histMean, TOLERANCE, key)
        assertEquals(expected.histVar, actual.histVar, TOLERANCE, key)
        assertEquals(expected.demandIndex ?: 0.0, actual.demandIndex ?: 0.0, TOLERANCE, key)
    }

    private fun Hour.exactFields(): List<Any?> = listOf(serviceZone, tripCount, lag4w, isSpike, demandIndex == null)

    // Counts the joins of the optimised logical plan that carry a broadcast hint on either side.
    private fun broadcastHintsIn(plan: LagJoinPlan): Int =
        fixture(TRIPS).demandForecast(janFebGrid(), zones(), plan).queryExecution().optimizedPlan().toString()
            .split("\n").count { it.contains("Join ") && it.contains("strategy=broadcast") }

    private fun withSessionTimeZone(zone: String, block: () -> Unit) {
        val previous = spark.conf().get(SESSION_TIME_ZONE)
        spark.conf().set(SESSION_TIME_ZONE, zone)
        try {
            block()
        } finally {
            spark.conf().set(SESSION_TIME_ZONE, previous)
        }
    }

    private data class Hour(
        val serviceZone: String,
        val tripCount: Long,
        val lag4w: Long?,
        val histMean: Double,
        val histVar: Double,
        val demandIndex: Double?,
        val isSpike: Boolean,
    )

    private data class TestTrip(val pickupZone: Int, val pickup: LocalDateTime) {
        // Positional, in ENFORCED_SCHEMA order. Only the pickup zone and time matter to O3.
        fun toRow(): Row = RowFactory.create(
            pickup, pickup.plusMinutes(15), pickupZone, DROPOFF_ZONE, 1, 1, 1,
            2.0, 10.0, 2.0, 12.0, pickup.year, pickup.monthValue,
        )
    }

    companion object {
        private lateinit var spark: SparkSession

        // One forecast run per lag-join plan, shared by the Jan–Feb tests: built on first use, after startSpark().
        private val janFebHours: Map<LagJoinPlan, Map<HourKey, Hour>> by lazy {
            LagJoinPlan.entries.associateWith { forecastHours(fixture(TRIPS), janFebGrid(), it) }
        }

        @JvmStatic
        @BeforeAll
        fun startSpark() {
            spark = localTestSession()
        }

        @JvmStatic
        @AfterAll
        fun stopSpark() {
            spark.stop()
        }

        private fun forecastHours(trips: Dataset<Row>, grid: Dataset<Row>, plan: LagJoinPlan): Map<HourKey, Hour> =
            trips.demandForecast(grid, zones(), plan).collectAsList().associate(::keyAndHour)

        // The JVM's default zone converts the DATE to java.sql.Date and back, so the date is unchanged.
        private fun keyAndHour(row: Row): Pair<HourKey, Hour> = Triple(
            row.getAs<Int>("PULocationID"),
            row.getAs<java.sql.Date>("pickup_date").toLocalDate(),
            row.getAs<Int>("hour_of_day"),
        ) to Hour(
            row.getAs("service_zone"),
            row.getAs("trip_count"),
            row.getAs("lag_4w_trip_count"),
            row.getAs("hist_mean_trips"),
            row.getAs("hist_var_trips"),
            row.getAs("demand_index"),
            row.getAs("is_demand_spike"),
        )

        private fun fixture(trips: List<TestTrip>): Dataset<Row> =
            spark.createDataFrame(trips.map(TestTrip::toRow), ENFORCED_SCHEMA)

        private fun zones(): Dataset<Row> = spark.createDataFrame(ZONES, ZONE_SCHEMA)

        private fun janFebGrid(): Dataset<Row> = spark.zoneHourGrid(JAN_TO_FEB, 1..2)

        private const val SESSION_TIME_ZONE = "spark.sql.session.timeZone"
        private const val PICKUP = "tpep_pickup_datetime"
        private const val DROPOFF_ZONE = 200
        private const val TOLERANCE = 1e-12
        private const val JAN_FEB_DAYS = 31 + 28

        private val JAN_TO_FEB = MonthRange(YearMonth.of(2025, 1), YearMonth.of(2025, 2))
        private val MARCH_TO_OCTOBER = MonthRange(YearMonth.of(2025, 3), YearMonth.of(2025, 10))

        private val ZONE_SCHEMA: StructType =
            StructType.fromDDL("LocationID INT, Borough STRING, Zone STRING, service_zone STRING")

        private val ZONES: List<Row> = listOf(
            RowFactory.create(1, "EWR", "Newark Airport", "EWR"),
            RowFactory.create(2, "Queens", "Jamaica Bay", "Boro Zone"),
        )

        private fun at(month: Int, day: Int, hour: Int, minute: Int): LocalDateTime =
            LocalDateTime.of(2025, month, day, hour, minute)

        // Zone 1: 3 trips on Wednesday 15 January at 10:00 (zero-fill); 1 trip on each Thursday at 18:00, plus 9 more
        // on 23 January (the spike). Zone 2: 2 trips on Monday 6 January at 08:00 (the lag). One trip from 2024.
        private val THURSDAYS: List<Pair<Int, Int>> =
            listOf(1 to 2, 1 to 9, 1 to 16, 1 to 23, 1 to 30, 2 to 6, 2 to 13, 2 to 20, 2 to 27)

        private val TRIPS: List<TestTrip> =
            List(3) { TestTrip(1, at(1, 15, 10, 10 * it)) } +
                THURSDAYS.map { (month, day) -> TestTrip(1, at(month, day, 18, 5)) } +
                List(9) { TestTrip(1, at(1, 23, 18, 10 + it)) } +
                List(2) { TestTrip(2, at(1, 6, 8, 20 * it)) } +
                TestTrip(1, LocalDateTime.of(2024, 12, 31, 23, 30))

        private val DST_DATES = listOf(LocalDate.of(2025, 3, 30), LocalDate.of(2025, 10, 26))

        private val DST_TRIPS: List<TestTrip> =
            DST_DATES.flatMap { date -> listOf(0, 1, 2).map { TestTrip(1, date.atTime(it, 30)) } }

        private val EXPECTED_SCHEMA: List<Pair<String, DataType>> = listOf(
            "PULocationID" to DataTypes.IntegerType,
            "service_zone" to DataTypes.StringType,
            "pickup_date" to DataTypes.DateType,
            "day_of_week" to DataTypes.IntegerType,
            "hour_of_day" to DataTypes.IntegerType,
            "trip_count" to DataTypes.LongType,
            "lag_4w_trip_count" to DataTypes.LongType,
            "hist_mean_trips" to DataTypes.DoubleType,
            "hist_var_trips" to DataTypes.DoubleType,
            "demand_index" to DataTypes.DoubleType,
            "is_demand_spike" to DataTypes.BooleanType,
        )
    }
}
