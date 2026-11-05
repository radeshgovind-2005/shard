package com.radeshgovind.clean

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.not

// Keeps the trips that fail none of the five criteria. Null passenger counts survive (D1).
fun Dataset<Row>.removeInvalidTrips(): Dataset<Row> = filter(not(InvalidTrip.anyInvalid()))
