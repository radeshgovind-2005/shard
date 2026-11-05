package com.radeshgovind.anomalies

import com.radeshgovind.anomalies.AnomalyColumns.WEEKDAY
import com.radeshgovind.anomalies.AnomalyColumns.WEEKEND
import com.radeshgovind.demand.withTripTimes
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
import kotlin.math.sqrt
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class FareAnomaliesTest {
    @Test
    fun `fareAnomalyRates has the O1 schema`() {
        val rates = fixture(TRIPS).fareAnomalyRates(zones())
        assertEquals(EXPECTED_SCHEMA, rates.schema().fields().map { it.name() to it.dataType() })
    }

    // D15–D17: bucket (100, 10h) holds ten fares of 10 and one of 100, all 1 mile. By hand, in $ per mile:
    // mean = 200/11, sample σ = √89100 / 11 (deviations −90/11 ×10 and 900/11). Per km, both divide by 1.609344.
    // The 100 is 3.015 σ above the mean (10/√11), so it is flagged; the tens are below the mean.
    @Test
    fun `mean, sample stddev and flag per zone-hour bucket`() {
        val bucket = flaggedTrips().filter { it.zone == 100 && it.hour == 10 }
        assertEquals(List(11) { 11L }, bucket.map(FlaggedTrip::bucketTrips))
        bucket.forEach {
            assertEquals(200.0 / 11 / KM_PER_MILE, it.mean, TOLERANCE)
            assertEquals(sqrt(89_100.0) / 11 / KM_PER_MILE, it.stddev!!, TOLERANCE)
            assertEquals(it.fare >= 100.0, it.isAnomaly)
        }
    }

    // D18, Samuelson: with n trips and the sample σ, no trip is more than (n − 1)/√n σ from the mean. Bucket
    // (100, 11h) has nine fares of 10 and one of 1,000,000: n = 10, so even that fare is only 2.846 σ out.
    @Test
    fun `a bucket of 10 trips can never flag, however extreme the fare`() {
        val bucket = flaggedTrips().filter { it.zone == 100 && it.hour == 11 }
        assertEquals(10, bucket.size)
        assertTrue(bucket.all { it.isAnomaly == false })
    }

    @Test
    fun `samuelson's bound is under 3 sigma up to 10 trips and over it from 11`() {
        (2..10).forEach { assertTrue(samuelsonBound(it) < ANOMALY_SIGMAS, "n = $it") }
        assertTrue(samuelsonBound(11) > ANOMALY_SIGMAS)
    }

    // D18: one trip has no sample σ, so its flag is null (no flag), not false. Zone 132 at 10h is its own bucket:
    // it shares the hour with zone 100, but not the zone.
    @Test
    fun `a single-trip bucket has a null stddev and a null flag`() {
        val single = flaggedTrips().single { it.zone == 132 }
        assertEquals(1L, single.bucketTrips)
        assertEquals(100.0 / KM_PER_MILE, single.mean, TOLERANCE)
        assertNull(single.stddev)
        assertNull(single.isAnomaly)
    }

    // D19: Saturday and Sunday by the pickup's wall-clock date, midnight to midnight.
    @Test
    fun `weekend is a pickup on Saturday or Sunday`() {
        val dayTypes = fixture(WEEKEND_EDGES).withTripTimes().withDayType().collectAsList()
            .associate { it.getAs<LocalDateTime>("tpep_pickup_datetime") to it.getAs<String>("day_type") }
        assertEquals(WEEKEND_EDGES.associate { it.pickup to it.expectedDayType }, dayTypes)
    }

    // D20: the outlier of bucket (100, 10h) is a Saturday cash trip, so it is the only flagged row. The Queens trip
    // has a null payment_type, which stays its own group.
    @Test
    fun `rate table breaks the flags down by day type, payment type and borough`() {
        val expected = mapOf(
            RateKey(WEEKDAY, 1, "Manhattan") to Rate(trips = 20, flagged = 0, rate = 0.0),
            RateKey(WEEKEND, 2, "Manhattan") to Rate(trips = 1, flagged = 1, rate = 1.0),
            RateKey(WEEKEND, null, "Queens") to Rate(trips = 1, flagged = 0, rate = 0.0),
        )
        assertEquals(expected, rates())
    }

    @Test
    fun `rate table counts every input trip once and never more flags than trips`() {
        val rates = rates().values
        assertEquals(TRIPS.size.toLong(), rates.sumOf(Rate::trips))
        assertTrue(rates.all { it.flagged <= it.trips })
    }

    private fun samuelsonBound(n: Int): Double = (n - 1) / sqrt(n.toDouble())

    private fun flaggedTrips(): List<FlaggedTrip> = fixture(TRIPS).fareAnomalies().collectAsList().map {
        FlaggedTrip(
            zone = it.getAs("PULocationID"),
            hour = it.getAs("hour_of_day"),
            fare = it.getAs("fare_amount"),
            bucketTrips = it.getAs("bucket_trips"),
            mean = it.getAs("bucket_mean_fare_per_km"),
            stddev = it.getAs("bucket_stddev_fare_per_km"),
            isAnomaly = it.getAs("is_fare_anomaly"),
        )
    }

    private fun rates(): Map<RateKey, Rate> = fixture(TRIPS).fareAnomalyRates(zones()).collectAsList().associate {
        RateKey(it.getAs("day_type"), it.getAs("payment_type"), it.getAs("pickup_borough")) to
            Rate(it.getAs("trips"), it.getAs("flagged_trips"), it.getAs("anomaly_rate"))
    }

    private fun fixture(trips: List<TestTrip>): Dataset<Row> =
        spark.createDataFrame(trips.map(TestTrip::toRow), ENFORCED_SCHEMA)

    private fun zones(): Dataset<Row> = spark.createDataFrame(ZONES, ZONE_SCHEMA)

    private data class FlaggedTrip(
        val zone: Int,
        val hour: Int,
        val fare: Double,
        val bucketTrips: Long,
        val mean: Double,
        val stddev: Double?,
        val isAnomaly: Boolean?,
    )

    private data class RateKey(val dayType: String, val paymentType: Int?, val borough: String)

    private data class Rate(val trips: Long, val flagged: Long, val rate: Double)

    private data class TestTrip(
        val pickupZone: Int,
        val pickup: LocalDateTime,
        val fare: Double = 10.0,
        val payment: Int? = 1,
        val expectedDayType: String = WEEKDAY,
    ) {
        // Positional, in ENFORCED_SCHEMA order. Every trip is 1 mile, so fare_per_km = fare / 1.609344.
        fun toRow(): Row = RowFactory.create(
            pickup, pickup.plusMinutes(20), pickupZone, DROPOFF_ZONE, PASSENGERS, RATECODE, payment,
            DISTANCE_MILES, fare, TIP, fare + TIP, pickup.year, pickup.monthValue,
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

        private const val TOLERANCE = 1e-9
        private const val DROPOFF_ZONE = 200
        private const val PASSENGERS = 1
        private const val RATECODE = 1
        private const val DISTANCE_MILES = 1.0
        private const val TIP = 2.0

        // 2025-01-15 is a Wednesday, 2025-01-18 a Saturday, 2025-01-19 a Sunday.
        private fun wednesday(hour: Int, minute: Int): LocalDateTime = LocalDateTime.of(2025, 1, 15, hour, minute)

        private val ZONE_SCHEMA: StructType =
            StructType.fromDDL("LocationID INT, Borough STRING, Zone STRING, service_zone STRING")

        private val ZONES: List<Row> = listOf(
            RowFactory.create(100, "Manhattan", "Zone 100", "Yellow Zone"),
            RowFactory.create(132, "Queens", "Zone 132", "Airports"),
        )

        // Bucket (100, 10h): ten weekday card fares of 10, and a Saturday cash fare of 100 (flagged).
        // Bucket (100, 11h): nine weekday fares of 10 and one of 1,000,000 (n = 10, so never flagged).
        // Bucket (132, 10h): a single Sunday trip with no payment type.
        private val TRIPS: List<TestTrip> =
            List(10) { TestTrip(100, wednesday(10, it)) } +
                TestTrip(100, LocalDateTime.of(2025, 1, 18, 10, 30), fare = 100.0, payment = 2) +
                List(9) { TestTrip(100, wednesday(11, it)) } +
                TestTrip(100, wednesday(11, 30), fare = 1_000_000.0) +
                TestTrip(132, LocalDateTime.of(2025, 1, 19, 10, 0), fare = 100.0, payment = null)

        // Friday 23:59 and Monday 00:00 are weekdays; Saturday 00:00 and Sunday 23:59 are the weekend's edges.
        private val WEEKEND_EDGES: List<TestTrip> = listOf(
            TestTrip(100, LocalDateTime.of(2025, 1, 17, 23, 59), expectedDayType = WEEKDAY),
            TestTrip(100, LocalDateTime.of(2025, 1, 18, 0, 0), expectedDayType = WEEKEND),
            TestTrip(100, LocalDateTime.of(2025, 1, 19, 23, 59), expectedDayType = WEEKEND),
            TestTrip(100, LocalDateTime.of(2025, 1, 20, 0, 0), expectedDayType = WEEKDAY),
        )

        private val EXPECTED_SCHEMA: List<Pair<String, DataType>> = listOf(
            "day_type" to DataTypes.StringType,
            "payment_type" to DataTypes.IntegerType,
            "pickup_borough" to DataTypes.StringType,
            "trips" to DataTypes.LongType,
            "flagged_trips" to DataTypes.LongType,
            "anomaly_rate" to DataTypes.DoubleType,
        )
    }
}
