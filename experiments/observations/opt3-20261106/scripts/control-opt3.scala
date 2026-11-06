// Negative control: the same checks must fail on perturbed copies of the off output (in memory, nothing written).
val off = spark.read.parquet("/output/opt3-off-2025/o3")
val isOne = col("PULocationID") === 161 && col("pickup_date") === lit("2025-05-15").cast("date") && col("hour_of_day") === 18
val bumped = off.withColumn("hist_var_trips", when(isOne, col("hist_var_trips") * lit(1 + 1e-9)).otherwise(col("hist_var_trips")))
Opt3Compare.compare(s"CONTROL var x(1+1e-9) on ${off.filter(isOne).count()} row", off, bumped)
val plusOne = off.withColumn("lag_4w_trip_count", when(isOne, col("lag_4w_trip_count") + 1).otherwise(col("lag_4w_trip_count")))
Opt3Compare.compare("CONTROL lag+1 on the same row", off, plusOne)
Opt3Compare.compare("CONTROL one row dropped", off, off.filter(!isOne))
System.exit(0)
