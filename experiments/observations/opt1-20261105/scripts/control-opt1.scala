// Negative control: the same checks must fail on a perturbed copy (in memory, nothing written).
import org.apache.spark.sql.functions._
object Control {
  def run(): Unit = {
    val spark = org.apache.spark.sql.SparkSession.active
    val off = spark.read.parquet("/output/opt1-off/o1")
    val bumped = off.withColumn("anomaly_rate",
      when(col("payment_type") === 1 && col("day_type") === "weekday" && col("pickup_borough") === "Manhattan",
        col("anomaly_rate") * lit(1 + 1e-9)).otherwise(col("anomaly_rate")))
    println(s"CONTROL o1 rate x(1+1e-9) on 1 row: exceptAll=${off.exceptAll(bumped).count()} " +
      s"beyond_tol=${off.join(bumped, Seq("day_type", "payment_type", "pickup_borough", "trips", "flagged_trips"))
        .filter(abs(off("anomaly_rate") - bumped("anomaly_rate")) > greatest(abs(off("anomaly_rate")), abs(bumped("anomaly_rate"))) * lit(1e-12)).count()}")
    val m3 = spark.read.parquet("/output/opt1-off/m3")
    println(s"CONTROL m3 is_volume_anomaly non-null rows: ${m3.filter(col("is_volume_anomaly").isNotNull).count()}")
    val flipped = m3.withColumn("trip_count",
      when(col("PULocationID") === 132, col("trip_count") + 1).otherwise(col("trip_count")))
    println(s"CONTROL m3 trip_count+1 for zone 132: exceptAll=${m3.exceptAll(flipped).count()} / ${flipped.exceptAll(m3).count()}")
  }
}
Control.run()
System.exit(0)
