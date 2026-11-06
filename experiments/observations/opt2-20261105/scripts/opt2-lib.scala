// OPT-2 correctness check (the definitions; compare-opt2.scala and control-opt2.scala run them), in the Docker master's spark-shell with the pipeline jar (--jars /jars/shard-all.jar).
// 1. Rate tables (what O1 writes): /output/opt2-off[-2025]/o1 (--skip-opts opt2) vs /output/opt2-on[-2025]/o1.
//    exceptAll both ways on whole rows; then a null-safe full outer join on the non-fractional columns (counts exact)
//    and the rate within a relative tolerance of 1e-12 (both null or both NaN count as equal).
// 2. Trip level, 2025-01: the flagged trips built both ways by the jar's own functions (loadTrips → enforceSchema →
//    removeInvalidTrips → fareAnomalies(WINDOW | BROADCAST_AGGREGATE)), never written.
//    a. exceptAll both ways on whole rows (every column, the bucket stats included): bit-for-bit equality.
//    b. the flag: exceptAll both ways on every column except the 3 bucket stats, so each trip (its input columns)
//       carries the same is_fare_anomaly; plus the true / false / null counts per side.
//    c. the stats per bucket: on each side every trip of a bucket has the same (count, mean, σ) (distinct rows =
//       buckets), then a null-safe join of the two bucket tables: counts exact, mean and σ within 1e-12 relative.
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.radeshgovind.anomalies.{BucketStatsPlan, FareAnomaliesKt}
import com.radeshgovind.clean.RemoveInvalidTripsKt
import com.radeshgovind.ingest.{EnforceSchemaKt, LoadTripsKt, TripFiles}

object Opt2Compare {
  val Tol = 1e-12
  def isFractional(t: DataType) = t == DoubleType || t == FloatType || t.isInstanceOf[DecimalType]
  def closeEnough(a: Column, b: Column): Column =
    (a.isNull && b.isNull) || (isnan(a) && isnan(b)) ||
      (a.isNotNull && b.isNotNull && abs(a - b) <= greatest(abs(a), abs(b)) * lit(Tol))
  def relDiff(a: Column, b: Column): Column =
    when(a =!= b, abs(a - b) / greatest(abs(a), abs(b))).otherwise(lit(0.0))

  // Exact keys (null-safe) + doubles within Tol. Returns (unmatched, beyondTol, maxRel).
  def keyedCompare(off: DataFrame, on: DataFrame, keys: Seq[String], doubles: Seq[String]): (Long, Long, String) = {
    val l = off.select(off.columns.map(c => col(c).alias(s"l_$c")) :+ lit(1).alias("l_present"): _*)
    val r = on.select(on.columns.map(c => col(c).alias(s"r_$c")) :+ lit(1).alias("r_present"): _*)
    val joined = l.join(r, keys.map(k => col(s"l_$k") <=> col(s"r_$k")).reduce(_ && _), "full_outer").cache()
    val unmatched = joined.filter(col("l_present").isNull || col("r_present").isNull).count()
    val beyond = joined.filter(doubles.map(d => !closeEnough(col(s"l_$d"), col(s"r_$d"))).reduce(_ || _)).count()
    val maxRel = joined.select(doubles.map(d => max(relDiff(col(s"l_$d"), col(s"r_$d")))): _*).first()
    joined.unpersist()
    (unmatched, beyond, doubles.zipWithIndex.map { case (d, i) => s"$d:${maxRel.get(i)}" }.mkString(","))
  }

  def rates(label: String, offPath: String, onPath: String): Unit = {
    val spark = SparkSession.active
    val (off, on) = (spark.read.parquet(offPath), spark.read.parquet(onPath))
    val keys = off.schema.fields.filterNot(f => isFractional(f.dataType)).map(_.name).toSeq
    val doubles = off.schema.fields.filter(f => isFractional(f.dataType)).map(_.name).toSeq
    val keyUnique = off.select(keys.map(col): _*).distinct().count() == off.count() &&
      on.select(keys.map(col): _*).distinct().count() == on.count()
    val (unmatched, beyond, maxRel) = keyedCompare(off, on, keys, doubles)
    val totals = (d: DataFrame) => d.agg(sum("trips"), sum("flagged_trips")).first()
    println(f"RESULT rates $label schema_equal=${off.schema == on.schema} rows_off=${off.count()}%,d rows_on=${on.count()}%,d " +
      s"exceptAll(off-on)=${off.exceptAll(on).count()} exceptAll(on-off)=${on.exceptAll(off).count()} " +
      s"key_unique=$keyUnique unmatched_keys=$unmatched beyond_tol=$beyond max_rel_diff=$maxRel " +
      s"totals_off(trips,flagged)=${totals(off)} totals_on=${totals(on)}")
  }

  def flaggedTrips(plan: BucketStatsPlan): DataFrame = {
    val spark = SparkSession.active
    val found = new TripFiles.Found(java.util.List.of(java.nio.file.Path.of("/data/trips/yellow_tripdata_2025-01.parquet")))
    // loadTrips takes a Fraction, a Kotlin value class, so its JVM name is mangled and it takes the raw double.
    val load = classOf[LoadTripsKt].getMethod("loadTrips-mXa3Sx8", classOf[SparkSession], classOf[TripFiles.Found], java.lang.Double.TYPE)
    val trips = load.invoke(null, spark, found, Double.box(1.0)).asInstanceOf[DataFrame]
    FareAnomaliesKt.fareAnomalies(RemoveInvalidTripsKt.removeInvalidTrips(EnforceSchemaKt.enforceSchema(trips)), plan)
  }

  val Stats = Seq("bucket_trips", "bucket_mean_fare_per_km", "bucket_stddev_fare_per_km")
  val Bucket = Seq("PULocationID", "hour_of_day")

  def trips(): Unit = {
    val off = flaggedTrips(BucketStatsPlan.WINDOW)
    val on = flaggedTrips(BucketStatsPlan.BROADCAST_AGGREGATE)
    println(s"TRIPS schema_equal=${off.schema == on.schema} columns=${off.columns.mkString(",")}")
    println(f"TRIPS rows_off=${off.count()}%,d rows_on=${on.count()}%,d " +
      s"exceptAll_whole_rows(off-on)=${off.exceptAll(on).count()} (on-off)=${on.exceptAll(off).count()}")
    val noStats = off.columns.filterNot(Stats.contains).map(col).toSeq
    println(s"FLAG exceptAll_without_stats(off-on)=${off.select(noStats: _*).exceptAll(on.select(noStats: _*)).count()} " +
      s"(on-off)=${on.select(noStats: _*).exceptAll(off.select(noStats: _*)).count()}")
    val flagCounts = (d: DataFrame) => d.groupBy("is_fare_anomaly").count().collect()
      .map(r => s"${r.get(0)}=${r.getLong(1)}").sorted.mkString(" ")
    println(s"FLAG counts_off: ${flagCounts(off)} | counts_on: ${flagCounts(on)}")
    println(s"KEYS null PULocationID or hour_of_day: off=${off.filter(col("PULocationID").isNull || col("hour_of_day").isNull).count()} " +
      s"on=${on.filter(col("PULocationID").isNull || col("hour_of_day").isNull).count()}")
    val buckets = (d: DataFrame) => d.select((Bucket ++ Stats).map(col): _*).distinct().cache()
    val (bOff, bOn) = (buckets(off), buckets(on))
    val nBuckets = (d: DataFrame) => d.select(Bucket.map(col): _*).distinct().count()
    println(f"BUCKETS distinct(bucket,stats)_off=${bOff.count()}%,d buckets_off=${nBuckets(off)}%,d " +
      f"distinct(bucket,stats)_on=${bOn.count()}%,d buckets_on=${nBuckets(on)}%,d")
    val (unmatched, beyond, maxRel) =
      keyedCompare(bOff, bOn, Bucket :+ "bucket_trips", Seq("bucket_mean_fare_per_km", "bucket_stddev_fare_per_km"))
    println(s"BUCKETS joined_on(PULocationID,hour_of_day,bucket_trips exact) unmatched=$unmatched " +
      s"mean_or_stddev_beyond_tol=$beyond max_rel_diff=$maxRel " +
      s"exceptAll(off-on)=${bOff.exceptAll(bOn).count()} (on-off)=${bOn.exceptAll(bOff).count()} " +
      s"null_stddev_buckets_off=${bOff.filter(col("bucket_stddev_fare_per_km").isNull).count()} " +
      s"on=${bOn.filter(col("bucket_stddev_fare_per_km").isNull).count()}")
    // How far the closest trip is from its threshold (mean + 3σ), relative: the stats' last-bit differences can
    // only flip a flag for a trip closer than about 1e-14 to it.
    val margin = (d: DataFrame) => {
      val t = col("bucket_mean_fare_per_km") + col("bucket_stddev_fare_per_km") * lit(3.0)
      d.filter(col("bucket_stddev_fare_per_km").isNotNull).agg(min(abs(col("fare_per_km") - t) / abs(t))).first().get(0)
    }
    println(s"MARGIN min |fare_per_km - threshold| / threshold: off=${margin(off)} on=${margin(on)}")
  }
}
