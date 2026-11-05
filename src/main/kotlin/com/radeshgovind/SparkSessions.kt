package com.radeshgovind

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.nio.file.Path

// No spark.* tuning here: the baseline runs on Spark's defaults, and every later change is a logged optimisation.
// The event log is instrumentation, not tuning: it's on only for profiled runs (eventLogDir != null).
fun createSparkSession(eventLogDir: Path?): SparkSession = SparkSession
    .builder()
    .appName(APP_NAME)
    .withLocalMasterUnlessSubmitted()
    .withEventLogIn(eventLogDir)
    .getOrCreate()

private const val APP_NAME = "shard"
private const val LOCAL_MASTER = "local[*]"

private fun SparkSession.Builder.withLocalMasterUnlessSubmitted(): SparkSession.Builder =
    if (isSubmitted()) this else master(LOCAL_MASTER)

// spark-submit --master sets the spark.master system property, which SparkConf reads; `make run` doesn't.
private fun isSubmitted(): Boolean = SparkConf().contains("spark.master")

// The folder must already exist, or Spark refuses to start.
private fun SparkSession.Builder.withEventLogIn(dir: Path?): SparkSession.Builder = if (dir == null) {
    this
} else {
    config("spark.eventLog.enabled", "true").config("spark.eventLog.dir", dir.toAbsolutePath().toString())
}
