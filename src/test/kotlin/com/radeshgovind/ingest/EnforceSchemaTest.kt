package com.radeshgovind.ingest

import com.radeshgovind.cli.Fraction
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
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import java.time.LocalDateTime
import java.time.YearMonth
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse

class EnforceSchemaTest {
    @TempDir
    lateinit var input: Path

    @Test
    fun `keeps exactly the spec columns, cast to the spec types, in order`() {
        assertEquals(EXPECTED_COLUMNS, loadEnforced().schema().fields().map { it.name() to it.dataType() })
    }

    @Test
    fun `tags every trip with its source file's year and month`() {
        val rows = loadEnforced().select(TripColumns.SOURCE_YEAR, TripColumns.SOURCE_MONTH).collectAsList()
        assertEquals(listOf(2025 to 3, 2025 to 3), rows.map { it.getInt(0) to it.getInt(1) })
    }

    @Test
    fun `drops columns the spec does not use`() {
        assertFalse(EXTRA_COLUMN in loadEnforced().columns())
    }

    private fun loadEnforced(): Dataset<Row> =
        spark.loadTrips(TripFiles.Found(listOf(writeRawMonth())), Fraction.WHOLE).enforceSchema()

    // Written with the raw 2025 types: bigint passenger_count, RatecodeID and payment_type, plus an unused column.
    private fun writeRawMonth(): Path {
        val file = tripFile(input, YearMonth.of(2025, 3))
        spark.createDataFrame(RAW_ROWS, RAW_SCHEMA).write().parquet(file.toString())
        return file
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

        private const val EXTRA_COLUMN = "Airport_fee"

        private val EXPECTED_COLUMNS: List<Pair<String, DataType>> = listOf(
            "tpep_pickup_datetime" to DataTypes.TimestampNTZType,
            "tpep_dropoff_datetime" to DataTypes.TimestampNTZType,
            "PULocationID" to DataTypes.IntegerType,
            "DOLocationID" to DataTypes.IntegerType,
            "passenger_count" to DataTypes.IntegerType,
            "RatecodeID" to DataTypes.IntegerType,
            "payment_type" to DataTypes.IntegerType,
            "trip_distance" to DataTypes.DoubleType,
            "fare_amount" to DataTypes.DoubleType,
            "tip_amount" to DataTypes.DoubleType,
            "total_amount" to DataTypes.DoubleType,
            "source_year" to DataTypes.IntegerType,
            "source_month" to DataTypes.IntegerType,
        )

        private val RAW_SCHEMA: StructType = StructType()
            .add("VendorID", DataTypes.IntegerType)
            .add("tpep_pickup_datetime", DataTypes.TimestampNTZType)
            .add("tpep_dropoff_datetime", DataTypes.TimestampNTZType)
            .add("passenger_count", DataTypes.LongType)
            .add("trip_distance", DataTypes.DoubleType)
            .add("RatecodeID", DataTypes.LongType)
            .add("PULocationID", DataTypes.IntegerType)
            .add("DOLocationID", DataTypes.IntegerType)
            .add("payment_type", DataTypes.LongType)
            .add("fare_amount", DataTypes.DoubleType)
            .add("tip_amount", DataTypes.DoubleType)
            .add("total_amount", DataTypes.DoubleType)
            .add(EXTRA_COLUMN, DataTypes.DoubleType)

        private val RAW_ROWS: List<Row> = listOf(rawTrip(10), rawTrip(11))

        private fun rawTrip(hour: Int): Row {
            val pickup = LocalDateTime.of(2025, 3, 10, hour, 0)
            return RowFactory.create(2, pickup, pickup.plusMinutes(15), 1L, 2.5, 1L, 100, 200, 1L, 12.0, 2.0, 15.0, 0.0)
        }
    }
}
