package com.radeshgovind

import org.apache.spark.sql.SparkSession

// Shared by the Spark test classes. shuffle.partitions=2 keeps tests fast; it's test-only, the pipeline uses defaults.
fun localTestSession(): SparkSession = SparkSession
    .builder()
    .master("local[2]")
    .appName("shard-test")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
