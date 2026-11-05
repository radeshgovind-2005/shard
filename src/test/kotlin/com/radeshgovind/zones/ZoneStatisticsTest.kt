package com.radeshgovind.zones

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
import kotlin.test.assertNull

class ZoneStatisticsTest {
    @Test
    fun `zoneStatistics has the M3 schema`() {
        val stats = fixture(TRIPS).zoneStatistics(zones())
        assertEquals(EXPECTED_SCHEMA, stats.schema().fields().map { it.name() to it.dataType() })
    }

    // D10: one trip has no sample σ, so it stays null instead of becoming 0.
    @Test
    fun `mean and sample stddev per zone and month`() {
        val cells = zoneMonths()
        val threeTrips = cells.getValue(102 to 1)
        assertEquals(3L, threeTrips.tripCount)
        assertEquals(20.0, threeTrips.avgFare, TOLERANCE)
        assertEquals(10.0, threeTrips.stddevFare!!, TOLERANCE)
        assertEquals(2.0, threeTrips.avgDistance, TOLERANCE)
        assertEquals(1.0, threeTrips.stddevDistance!!, TOLERANCE)
        val oneTrip = cells.getValue(132 to 1)
        assertEquals(50.0, oneTrip.avgFare, TOLERANCE)
        assertNull(oneTrip.stddevFare)
        assertNull(oneTrip.stddevDistance)
    }

    // D11: Manhattan's 10, 3, 3 rank 1, 2, 2. Queens is ranked on its own.
    @Test
    fun `zones are ranked by volume within their borough and month`() {
        val januaryRanks = zoneMonths().filterKeys { it.second == 1 }.mapKeys { it.key.first }
            .mapValues { it.value.volumeRank }
        assertEquals(mapOf(100 to 1, 101 to 2, 102 to 2, 132 to 1), januaryRanks)
    }

    // D12: zone 101 has no February. In April the frame is February–April, so January's 3 trips stay out
    // (rowsBetween(-2, 0) would take January as "two rows back").
    @Test
    fun `rolling share skips a missing month instead of reaching further back`() {
        val april = zoneMonths().getValue(101 to 4)
        assertEquals(2L + 4L, april.zoneTrips3m)
        assertEquals((12L + 11L + 40L) + (2L + 4L), april.boroughTrips3m)
        assertEquals(6.0 / 69.0, april.tripShare3m, TOLERANCE)
    }

    // D13: baseline = the previous 3 months. January has none and February only one (σ undefined) → null.
    // March: 11 against 10, 12 → not flagged. April: 40 against 10, 12, 11 (mean 11, σ 1) → flagged.
    @Test
    fun `a spike month is flagged against the zone's own previous months`() {
        val spikeZone = zoneMonths().filterKeys { it.first == 100 }.mapKeys { it.key.second }
        assertEquals(mapOf(1 to null, 2 to null, 3 to false, 4 to true), spikeZone.mapValues { it.value.isAnomaly })
        assertEquals(11.0, spikeZone.getValue(4).baselineMean!!, TOLERANCE)
        assertEquals(1.0, spikeZone.getValue(4).baselineStddev!!, TOLERANCE)
    }

    @Test
    fun `trip counts add up to the input trips`() {
        assertEquals(TRIPS.size.toLong(), zoneMonths().values.sumOf(ZoneMonth::tripCount))
    }

    private fun zoneMonths(): Map<Pair<Int, Int>, ZoneMonth> = fixture(TRIPS).zoneStatistics(zones()).collectAsList()
        .associate { (it.getAs<Int>("PULocationID") to it.getAs<Int>("source_month")) to toZoneMonth(it) }

    private fun toZoneMonth(row: Row): ZoneMonth = ZoneMonth(
        row.getAs("trip_count"),
        row.getAs("avg_fare"),
        row.getAs("stddev_fare"),
        row.getAs("avg_distance"),
        row.getAs("stddev_distance"),
        row.getAs("volume_rank"),
        row.getAs("zone_trips_3m"),
        row.getAs("borough_trips_3m"),
        row.getAs("trip_share_3m"),
        row.getAs("baseline_mean_trips"),
        row.getAs("baseline_stddev_trips"),
        row.getAs("is_volume_anomaly"),
    )

    private fun fixture(trips: List<TestTrip>): Dataset<Row> =
        spark.createDataFrame(trips.map(TestTrip::toRow), ENFORCED_SCHEMA)

    private fun zones(): Dataset<Row> = spark.createDataFrame(ZONES, ZONE_SCHEMA)

    private data class ZoneMonth(
        val tripCount: Long,
        val avgFare: Double,
        val stddevFare: Double?,
        val avgDistance: Double,
        val stddevDistance: Double?,
        val volumeRank: Int,
        val zoneTrips3m: Long,
        val boroughTrips3m: Long,
        val tripShare3m: Double,
        val baselineMean: Double?,
        val baselineStddev: Double?,
        val isAnomaly: Boolean?,
    )

    private data class TestTrip(
        val pickupZone: Int,
        val month: Int,
        val fare: Double = 10.0,
        val distance: Double = 2.0,
    ) {
        // Positional, in ENFORCED_SCHEMA order. source_year/source_month stand in for the file name (D4).
        fun toRow(): Row {
            val pickup = LocalDateTime.of(YEAR, month, 15, 10, 0)
            return RowFactory.create(
                pickup, pickup.plusMinutes(20), pickupZone, DROPOFF_ZONE, PASSENGERS, RATECODE, PAYMENT,
                distance, fare, TIP, fare + TIP, YEAR, month,
            )
        }
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

        private const val TOLERANCE = 1e-9
        private const val YEAR = 2025
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
            RowFactory.create(102, "Manhattan", "Zone 102", "Yellow Zone"),
            RowFactory.create(132, "Queens", "Zone 132", "Airports"),
        )

        private fun trips(zone: Int, month: Int, count: Int): List<TestTrip> = List(count) { TestTrip(zone, month) }

        // Zone 100: 10, 12, 11, then a spike of 40 in April. Zone 101: January, March, April only (no February).
        // Zone 102: January only, three trips with known fares and distances. Zone 132: one Queens trip.
        private val TRIPS: List<TestTrip> =
            trips(100, 1, 10) + trips(100, 2, 12) + trips(100, 3, 11) + trips(100, 4, 40) +
                trips(101, 1, 3) + trips(101, 3, 2) + trips(101, 4, 4) +
                listOf(
                    TestTrip(102, 1, fare = 10.0, distance = 1.0),
                    TestTrip(102, 1, fare = 20.0, distance = 2.0),
                    TestTrip(102, 1, fare = 30.0, distance = 3.0),
                    TestTrip(132, 1, fare = 50.0, distance = 15.0),
                )

        private val EXPECTED_SCHEMA: List<Pair<String, DataType>> = listOf(
            "PULocationID" to DataTypes.IntegerType,
            "source_year" to DataTypes.IntegerType,
            "source_month" to DataTypes.IntegerType,
            "month_index" to DataTypes.IntegerType,
            "pickup_borough" to DataTypes.StringType,
            "trip_count" to DataTypes.LongType,
            "avg_fare" to DataTypes.DoubleType,
            "stddev_fare" to DataTypes.DoubleType,
            "avg_distance" to DataTypes.DoubleType,
            "stddev_distance" to DataTypes.DoubleType,
            "volume_rank" to DataTypes.IntegerType,
            "zone_trips_3m" to DataTypes.LongType,
            "borough_trips_3m" to DataTypes.LongType,
            "trip_share_3m" to DataTypes.DoubleType,
            "baseline_mean_trips" to DataTypes.DoubleType,
            "baseline_stddev_trips" to DataTypes.DoubleType,
            "is_volume_anomaly" to DataTypes.BooleanType,
        )
    }
}
