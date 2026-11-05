package com.radeshgovind.demand

import com.radeshgovind.ingest.ENFORCED_SCHEMA
import com.radeshgovind.localTestSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructType
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import java.time.LocalDateTime
import kotlin.test.Test
import kotlin.test.assertEquals

class TemporalDemandTest {
    @Test
    fun `temporalDemand has the M2 schema`() {
        val demand = fixture(TRIPS).temporalDemand(zones())
        assertEquals(EXPECTED_SCHEMA, demand.schema().fields().map { it.name() to it.dataType() })
    }

    @Test
    fun `temporalDemand counts and averages per borough, day and pickup hour`() {
        assertEquals(EXPECTED_CELLS, demandCells(fixture(TRIPS)))
    }

    @Test
    fun `trip counts add up to the input trips`() {
        assertEquals(TRIPS.size.toLong(), demandCells(fixture(TRIPS)).sumOf(DemandCell::trips))
    }

    // D7: the trip is in Lisbon's repeated hour. timestamp_diff() would give 80 minutes here.
    @Test
    fun `duration ignores the session time zone`() {
        val inDstOverlap = TestTrip(100, LocalDateTime.of(2025, 10, 26, 1, 50), LocalDateTime.of(2025, 10, 26, 2, 10))
        withSessionTimeZone("Europe/Lisbon") {
            assertEquals(listOf(20.0), demandCells(fixture(listOf(inDstOverlap))).map(DemandCell::avgDurationMin))
        }
    }

    // D3: the plan shows hour(..., Some(<session zone>)), but on TIMESTAMP_NTZ the zone is ignored.
    // Tokyo is UTC+9, so any shift would move the trip to another hour.
    @Test
    fun `day and hour ignore the session time zone`() {
        val wednesdayTen = TestTrip(100, jan(15, 10, 0), jan(15, 10, 20))
        withSessionTimeZone("Asia/Tokyo") {
            val dayAndHour = demandCells(fixture(listOf(wednesdayTen))).map { it.dayOfWeek to it.hourOfDay }
            assertEquals(listOf(4 to 10), dayAndHour)
        }
    }

    private fun demandCells(trips: Dataset<Row>): Set<DemandCell> =
        trips.temporalDemand(zones()).collectAsList().map(::toDemandCell).toSet()

    private fun toDemandCell(row: Row): DemandCell = DemandCell(
        row.getAs("pickup_borough"),
        row.getAs("day_of_week"),
        row.getAs("hour_of_day"),
        row.getAs("trip_count"),
        row.getAs("avg_fare"),
        row.getAs("avg_duration_min"),
        row.getAs("avg_distance"),
    )

    private fun withSessionTimeZone(zone: String, block: () -> Unit) {
        val previous = spark.conf().get(SESSION_TIME_ZONE)
        spark.conf().set(SESSION_TIME_ZONE, zone)
        try {
            block()
        } finally {
            spark.conf().set(SESSION_TIME_ZONE, previous)
        }
    }

    private fun fixture(trips: List<TestTrip>): Dataset<Row> =
        spark.createDataFrame(trips.map(TestTrip::toRow), ENFORCED_SCHEMA)

    private fun zones(): Dataset<Row> = spark.createDataFrame(ZONES, ZONE_SCHEMA)

    private data class DemandCell(
        val borough: String,
        val dayOfWeek: Int,
        val hourOfDay: Int,
        val trips: Long,
        val avgFare: Double,
        val avgDurationMin: Double,
        val avgDistance: Double,
    )

    private data class TestTrip(
        val pickupZone: Int,
        val pickup: LocalDateTime,
        val dropoff: LocalDateTime,
        val fare: Double = 10.0,
        val distance: Double = 2.0,
    ) {
        // Positional, in ENFORCED_SCHEMA order.
        fun toRow(): Row = RowFactory.create(
            pickup, dropoff, pickupZone, DROPOFF_ZONE, PASSENGERS, RATECODE, PAYMENT,
            distance, fare, TIP, fare + TIP, pickup.year, pickup.monthValue,
        )
    }

    companion object {
        private lateinit var spark: SparkSession

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

        private const val SESSION_TIME_ZONE = "spark.sql.session.timeZone"
        private const val DROPOFF_ZONE = 200
        private const val PASSENGERS = 1
        private const val RATECODE = 1
        private const val PAYMENT = 1
        private const val TIP = 2.0

        private val ZONE_SCHEMA: StructType =
            StructType.fromDDL("LocationID INT, Borough STRING, Zone STRING, service_zone STRING")

        private val ZONES: List<Row> = listOf(
            RowFactory.create(100, "Manhattan", "Zone 100", "Yellow Zone"),
            RowFactory.create(101, "Manhattan", "Zone 101", "Yellow Zone"),
            RowFactory.create(132, "Queens", "Zone 132", "Airports"),
            RowFactory.create(138, "Queens", "Zone 138", "Airports"),
        )

        // 2025-01-15 is a Wednesday (4), 2025-01-18 a Saturday (7), 2025-01-19 a Sunday (1).
        private fun jan(day: Int, hour: Int, minute: Int, second: Int = 0): LocalDateTime =
            LocalDateTime.of(2025, 1, day, hour, minute, second)

        private val TRIPS: List<TestTrip> = listOf(
            TestTrip(100, jan(15, 10, 0), jan(15, 10, 20), fare = 10.0, distance = 2.0),
            TestTrip(101, jan(15, 10, 30), jan(15, 11, 10), fare = 20.0, distance = 4.0),
            TestTrip(100, jan(15, 11, 0), jan(15, 11, 15), fare = 8.0, distance = 1.0),
            TestTrip(132, jan(19, 23, 50), jan(20, 0, 20), fare = 50.0, distance = 15.0),
            TestTrip(138, jan(19, 23, 5), jan(19, 23, 50), fare = 30.0, distance = 10.0),
            TestTrip(132, jan(18, 8, 0), jan(18, 8, 0, 30), fare = 5.0, distance = 0.1),
        )

        private val EXPECTED_SCHEMA: List<Pair<String, DataType>> = listOf(
            "pickup_borough" to DataTypes.StringType,
            "day_of_week" to DataTypes.IntegerType,
            "hour_of_day" to DataTypes.IntegerType,
            "trip_count" to DataTypes.LongType,
            "avg_fare" to DataTypes.DoubleType,
            "avg_duration_min" to DataTypes.DoubleType,
            "avg_distance" to DataTypes.DoubleType,
        )

        // Two Manhattan zones share a cell, the hour comes from the pickup, a trip over midnight stays on Sunday,
        // and 30 seconds is 0.5 minutes.
        private val EXPECTED_CELLS: Set<DemandCell> = setOf(
            DemandCell("Manhattan", 4, 10, 2, 15.0, 30.0, 3.0),
            DemandCell("Manhattan", 4, 11, 1, 8.0, 15.0, 1.0),
            DemandCell("Queens", 1, 23, 2, 40.0, 37.5, 12.5),
            DemandCell("Queens", 7, 8, 1, 5.0, 0.5, 0.1),
        )
    }
}
