// OPT-3 correctness check, in the Docker master's spark-shell (local[2]; /output = the repo's output/).
// O3 written with --skip-opts opt3 (no cache, the baseline) vs with the cached grid, default confs (on) and with
// spark.sql.optimizer.canChangeCachedPlanOutputPartitioning=true (diag). Per pair: schema, rows, exceptAll both ways
// (whole rows, multiset: 0 = bit for bit), then a full outer join on the key (PULocationID, pickup_date, hour_of_day):
// every non-double column exact (null-safe), every double within |a-b| <= 1e-12 * max(|a|,|b|) (both null = equal).
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
object Opt3Compare {
  val Tol = 1e-12
  val Key = Seq("PULocationID", "pickup_date", "hour_of_day")
  def close(a: Column, b: Column): Column =
    (a.isNull && b.isNull) || (a.isNotNull && b.isNotNull && abs(a - b) <= greatest(abs(a), abs(b)) * lit(Tol))
  def rel(a: Column, b: Column): Column = when(a =!= b, abs(a - b) / greatest(abs(a), abs(b))).otherwise(lit(0.0))
  def compare(label: String, off: DataFrame, on: DataFrame): Unit = {
    val doubles = off.schema.fields.filter(_.dataType == DoubleType).map(_.name).toSeq
    val exact = off.columns.filterNot(c => doubles.contains(c) || Key.contains(c)).toSeq
    val l = off.select(off.columns.map(c => col(c).alias(s"l_$c")) :+ lit(1).alias("l_present"): _*)
    val r = on.select(on.columns.map(c => col(c).alias(s"r_$c")) :+ lit(1).alias("r_present"): _*)
    val j = l.join(r, Key.map(k => col(s"l_$k") === col(s"r_$k")).reduce(_ && _), "full_outer").cache()
    val unmatched = j.filter(col("l_present").isNull || col("r_present").isNull).count()
    val exactDiff = j.filter(exact.map(c => !(col(s"l_$c") <=> col(s"r_$c"))).reduce(_ || _)).count()
    val beyond = j.filter(doubles.map(d => !close(col(s"l_$d"), col(s"r_$d"))).reduce(_ || _)).count()
    val maxRel = j.select(doubles.map(d => max(rel(col(s"l_$d"), col(s"r_$d")))): _*).first()
    val keyUnique = off.select(Key.map(col): _*).distinct().count() == off.count()
    val sums = (d: DataFrame) => d.agg(sum("trip_count"), count(col("lag_4w_trip_count")), count(when(col("is_demand_spike"), 1))).first()
    println(f"RESULT $label schema_equal=${off.schema == on.schema} rows_off=${off.count()}%,d rows_on=${on.count()}%,d " +
      s"exceptAll(off-on)=${off.exceptAll(on).count()} exceptAll(on-off)=${on.exceptAll(off).count()} key_unique=$keyUnique " +
      s"unmatched_keys=$unmatched exact_cols_differ=$exactDiff doubles_beyond_tol=$beyond " +
      s"max_rel_diff=${doubles.zipWithIndex.map { case (d, i) => s"$d:${maxRel.get(i)}" }.mkString(",")} " +
      s"off(sum_trips,rows_with_lag,spikes)=${sums(off)} on=${sums(on)}")
    j.unpersist()
  }
  def pair(label: String, offPath: String, onPath: String): Unit =
    compare(label, spark.read.parquet(offPath), spark.read.parquet(onPath))
}
