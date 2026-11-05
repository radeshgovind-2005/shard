package com.radeshgovind.clean

import com.radeshgovind.ingest.ENFORCED_SCHEMA
import com.radeshgovind.localTestSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DataTypes
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import java.time.LocalDateTime
import kotlin.test.Test
import kotlin.test.assertEquals

class CleaningTest {
    @Test
    fun `removeInvalidTrips keeps exactly the valid and the null-passenger trip`() {
        assertEquals(2L, fixture().removeInvalidTrips().count())
    }

    @Test
    fun `cleaningStats counts each criterion independently`() {
        val stats = fixture().cleaningStats()
        assertEquals(EXPECTED_SCHEMA, stats.schema().fields().map { it.name() to it.dataType() })
        assertEquals(listOf(EXPECTED_STATS), stats.collectAsList().map(::countsByColumn))
    }

    @Test
    fun `no criterion is ever null`() {
        InvalidTrip.entries.forEach {
            assertEquals(0L, fixture().filter(it.condition().isNull()).count(), "$it")
        }
    }

    private fun fixture(): Dataset<Row> = spark.createDataFrame(FIXTURE.map(TestTrip::toRow), ENFORCED_SCHEMA)

    // Types are checked against EXPECTED_SCHEMA, so the values compare as plain numbers.
    private fun countsByColumn(row: Row): Map<String, Long> = row.schema().fieldNames().associateWith {
        row.getAs<Number>(it).toLong()
    }

    // A valid trip by default. Each fixture row changes only what makes it fail (or pass) one rule.
    private data class TestTrip(
        val fare: Double = 10.0,
        val distance: Double = 2.0,
        val passengers: Int? = 1,
        val pickup: LocalDateTime = PICKUP,
        val dropoff: LocalDateTime = PICKUP.plusMinutes(20),
        val pickupZone: Int = 100,
        val dropoffZone: Int = 200,
    ) {
        // Positional, in ENFORCED_SCHEMA order.
        fun toRow(): Row = RowFactory.create(
            pickup, dropoff, pickupZone, dropoffZone, passengers, RATECODE, PAYMENT,
            distance, fare, TIP, fare + TIP, YEAR, MONTH,
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

        private val PICKUP: LocalDateTime = LocalDateTime.of(2025, 1, 15, 10, 0)
        private const val RATECODE = 1
        private const val PAYMENT = 1
        private const val TIP = 2.0
        private const val YEAR = 2025
        private const val MONTH = 1

        private val FIXTURE = listOf(
            TestTrip(),
            TestTrip(passengers = null),
            TestTrip(fare = 0.0),
            TestTrip(fare = -5.0),
            TestTrip(distance = 0.0),
            TestTrip(passengers = 0),
            TestTrip(passengers = 7),
            TestTrip(dropoff = PICKUP),
            TestTrip(dropoff = PICKUP.minusMinutes(1)),
            TestTrip(pickupZone = 264),
            TestTrip(dropoffZone = 0),
            TestTrip(fare = 0.0, distance = 0.0),
        )

        // The criteria sum to 11 against 10 removed: the last fixture row fails two of them.
        private val EXPECTED_SCHEMA: List<Pair<String, DataType>> =
            listOf("source_year" to DataTypes.IntegerType) +
                listOf(
                    "raw_trips", "bad_fare", "bad_distance", "bad_passenger_count", "bad_time_order", "bad_zone",
                    "removed_trips", "kept_trips", "null_passenger_count",
                ).map { it to DataTypes.LongType }

        private val EXPECTED_STATS: Map<String, Long> = linkedMapOf(
            "source_year" to 2025L,
            "raw_trips" to 12L,
            "bad_fare" to 3L,
            "bad_distance" to 2L,
            "bad_passenger_count" to 2L,
            "bad_time_order" to 2L,
            "bad_zone" to 2L,
            "removed_trips" to 10L,
            "kept_trips" to 2L,
            "null_passenger_count" to 1L,
        )
    }
}
