package com.radeshgovind.forecast

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.hour
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.functions.unix_date

// D24: the pickup's wall-clock hour as days since 1970-01-01 × 24 + hour, with no time zone involved.
// On TIMESTAMP_NTZ, to_date() truncates and hour() reads the field, so no TIMESTAMP cast happens (unlike
// timestamp_diff, D7). Every day has 24 hours: NYC's spring-forward hour stays empty, its fall-back hour holds two.
fun hourIndex(pickup: Column): Column = unix_date(to_date(pickup)).multiply(HOURS_PER_DAY).plus(hour(pickup))

const val HOURS_PER_DAY = 24
