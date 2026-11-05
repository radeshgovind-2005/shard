package com.radeshgovind

import org.apache.spark.sql.SparkSession
import kotlin.test.Test
import kotlin.test.assertEquals

// Guards the build setup: Spark on the test classpath, the JDK toolchain and the --add-opens flags.
class SparkSessionTest {
    @Test
    fun `local session runs a job with a shuffle`() {
        val spark = localSession()
        try {
            val groups = spark.range(100).selectExpr("id % 10 AS key").groupBy("key").count()
            assertEquals(10L, groups.count())
        } finally {
            spark.stop()
        }
    }

    private fun localSession(): SparkSession = SparkSession
        .builder()
        .master("local[2]")
        .appName("shard-smoke-test")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
}
