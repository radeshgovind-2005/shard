package com.radeshgovind.ingest

import com.radeshgovind.ingest.TripColumns.DO_LOCATION_ID
import com.radeshgovind.ingest.TripColumns.DROPOFF_DATETIME
import com.radeshgovind.ingest.TripColumns.FARE_AMOUNT
import com.radeshgovind.ingest.TripColumns.PASSENGER_COUNT
import com.radeshgovind.ingest.TripColumns.PAYMENT_TYPE
import com.radeshgovind.ingest.TripColumns.PICKUP_DATETIME
import com.radeshgovind.ingest.TripColumns.PU_LOCATION_ID
import com.radeshgovind.ingest.TripColumns.RATECODE_ID
import com.radeshgovind.ingest.TripColumns.SOURCE_MONTH
import com.radeshgovind.ingest.TripColumns.SOURCE_YEAR
import com.radeshgovind.ingest.TripColumns.TIP_AMOUNT
import com.radeshgovind.ingest.TripColumns.TOTAL_AMOUNT
import com.radeshgovind.ingest.TripColumns.TRIP_DISTANCE
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

// The one place the trip schema is fixed: downstream code never re-casts.
// Every column the spec doesn't use is dropped here, so Parquet never reads it.
fun Dataset<Row>.enforceSchema(): Dataset<Row> = select(*ENFORCED_SCHEMA.fields().map(::castTo).toTypedArray())

// D3: timestamps stay TIMESTAMP_NTZ, NYC wall-clock time as stored, whatever the session time zone.
val ENFORCED_SCHEMA: StructType = StructType(
    arrayOf(
        nullable(PICKUP_DATETIME, DataTypes.TimestampNTZType),
        nullable(DROPOFF_DATETIME, DataTypes.TimestampNTZType),
        nullable(PU_LOCATION_ID, DataTypes.IntegerType),
        nullable(DO_LOCATION_ID, DataTypes.IntegerType),
        nullable(PASSENGER_COUNT, DataTypes.IntegerType),
        nullable(RATECODE_ID, DataTypes.IntegerType),
        nullable(PAYMENT_TYPE, DataTypes.IntegerType),
        nullable(TRIP_DISTANCE, DataTypes.DoubleType),
        nullable(FARE_AMOUNT, DataTypes.DoubleType),
        nullable(TIP_AMOUNT, DataTypes.DoubleType),
        nullable(TOTAL_AMOUNT, DataTypes.DoubleType),
        nullable(SOURCE_YEAR, DataTypes.IntegerType),
        nullable(SOURCE_MONTH, DataTypes.IntegerType),
    ),
)

private fun castTo(field: StructField): Column = col(field.name()).cast(field.dataType()).alias(field.name())

private fun nullable(name: String, type: DataType): StructField = DataTypes.createStructField(name, type, true)
