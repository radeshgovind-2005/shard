// Negative control for compare-opt2.scala: the same checks must fail on perturbed copies (in memory, nothing written).
// Feed after opt2-lib.scala: cat opt2-lib.scala control-opt2.scala | spark-shell --jars /jars/shard-all.jar ...
import org.apache.spark.sql.functions._
import com.radeshgovind.anomalies.BucketStatsPlan
object Opt2Control {
  def run(): Unit = {
    val spark = org.apache.spark.sql.SparkSession.active
    // 1. one rate × (1 + 1e-9): exceptAll and the tolerance must both catch it.
    val rates = spark.read.parquet("/output/opt2-on/o1")
    val isOneRow = col("payment_type") === 1 && col("day_type") === "weekday" && col("pickup_borough") === "Manhattan"
    val bumped = rates.withColumn("anomaly_rate", when(isOneRow, col("anomaly_rate") * lit(1 + 1e-9)).otherwise(col("anomaly_rate")))
    val keys = Seq("day_type", "payment_type", "pickup_borough", "trips", "flagged_trips")
    val (u1, b1, m1) = Opt2Compare.keyedCompare(rates, bumped, keys, Seq("anomaly_rate"))
    println(s"CONTROL rate x(1+1e-9) on ${rates.filter(isOneRow).count()} row: exceptAll=${rates.exceptAll(bumped).count()} unmatched=$u1 beyond_tol=$b1 max_rel=$m1")
    // 2. one trip's flag flipped (true → false, the trip at zone 161, the largest bucket, earliest flagged pickup).
    val on = Opt2Compare.flaggedTrips(BucketStatsPlan.BROADCAST_AGGREGATE)
    val victim = on.filter(col("is_fare_anomaly") && col("PULocationID") === 161).agg(min("tpep_pickup_datetime")).first().get(0)
    val isVictim = col("is_fare_anomaly") && col("PULocationID") === 161 && col("tpep_pickup_datetime") === lit(victim)
    val flipped = on.withColumn("is_fare_anomaly", when(isVictim, lit(false)).otherwise(col("is_fare_anomaly")))
    val noStats = on.columns.filterNot(Opt2Compare.Stats.contains).map(col).toSeq
    println(s"CONTROL flag flipped on ${on.filter(isVictim).count()} trip: exceptAll_without_stats=" +
      s"${on.select(noStats: _*).exceptAll(flipped.select(noStats: _*)).count()} / ${flipped.select(noStats: _*).exceptAll(on.select(noStats: _*)).count()}")
    // 3. one bucket's mean × (1 + 1e-9), and one bucket's count + 1: the bucket comparison must catch both.
    val b = on.select((Opt2Compare.Bucket ++ Opt2Compare.Stats).map(col): _*).distinct().cache()
    val isBucket = col("PULocationID") === 161 && col("hour_of_day") === 18
    val meanBumped = b.withColumn("bucket_mean_fare_per_km",
      when(isBucket, col("bucket_mean_fare_per_km") * lit(1 + 1e-9)).otherwise(col("bucket_mean_fare_per_km")))
    val countBumped = b.withColumn("bucket_trips", when(isBucket, col("bucket_trips") + 1).otherwise(col("bucket_trips")))
    val doubles = Seq("bucket_mean_fare_per_km", "bucket_stddev_fare_per_km")
    val (u2, b2, m2) = Opt2Compare.keyedCompare(b, meanBumped, Opt2Compare.Bucket :+ "bucket_trips", doubles)
    val (u3, b3, _) = Opt2Compare.keyedCompare(b, countBumped, Opt2Compare.Bucket :+ "bucket_trips", doubles)
    println(s"CONTROL bucket (161, 18h) mean x(1+1e-9): unmatched=$u2 beyond_tol=$b2 max_rel=$m2")
    println(s"CONTROL bucket (161, 18h) bucket_trips+1: unmatched=$u3 beyond_tol=$b3")
  }
}
Opt2Control.run()
System.exit(0)
