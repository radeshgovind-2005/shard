package com.radeshgovind

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

// No spark.* tuning here: the baseline runs on Spark's defaults, and every later change is a logged optimisation.
fun createSparkSession(): SparkSession = SparkSession
    .builder()
    .appName(APP_NAME)
    .withLocalMasterUnlessSubmitted()
    .getOrCreate()

private const val APP_NAME = "shard"
private const val LOCAL_MASTER = "local[*]"

private fun SparkSession.Builder.withLocalMasterUnlessSubmitted(): SparkSession.Builder =
    if (isSubmitted()) this else master(LOCAL_MASTER)

// spark-submit --master sets the spark.master system property, which SparkConf reads; `make run` doesn't.
private fun isSubmitted(): Boolean = SparkConf().contains("spark.master")
