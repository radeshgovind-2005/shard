// OPT-1 correctness check: /output/opt1-off/<op> (--skip-opts opt1) vs /output/opt1-on/<op> (default), 2025-01.
// Exact: exceptAll both ways on whole rows. Tolerance: a null-safe full outer join on every non-fractional column
// (ints, longs, strings, booleans, dates) as the key, so integers and flags must match exactly, then each double
// within a relative tolerance of 1e-12 (both null or both NaN count as equal).
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Opt1Compare {
val Tol = 1e-12
def isFractional(t: DataType) = t == DoubleType || t == FloatType || t.isInstanceOf[DecimalType]
def closeEnough(a: Column, b: Column): Column =
  (a.isNull && b.isNull) || (isnan(a) && isnan(b)) ||
    (a.isNotNull && b.isNotNull && abs(a - b) <= greatest(abs(a), abs(b)) * lit(Tol))

def compare(op: String): Unit = {
  val spark = org.apache.spark.sql.SparkSession.active
  val off = spark.read.parquet(s"/output/opt1-off/$op")
  val on = spark.read.parquet(s"/output/opt1-on/$op")
  val sameSchema = off.schema == on.schema
  val (nOff, nOn) = (off.count(), on.count())
  val offMinusOn = off.exceptAll(on).count()
  val onMinusOff = on.exceptAll(off).count()
  val keys = off.schema.fields.filterNot(f => isFractional(f.dataType)).map(_.name)
  val doubles = off.schema.fields.filter(f => isFractional(f.dataType)).map(_.name)
  val keyUnique = off.select(keys.map(col): _*).distinct().count() == nOff &&
    on.select(keys.map(col): _*).distinct().count() == nOn
  val l = off.select(off.columns.map(c => col(c).alias(s"l_$c")) :+ lit(1).alias("l_present"): _*)
  val r = on.select(on.columns.map(c => col(c).alias(s"r_$c")) :+ lit(1).alias("r_present"): _*)
  val joined = l.join(r, keys.map(k => col(s"l_$k") <=> col(s"r_$k")).reduce(_ && _), "full_outer")
  val unmatched = joined.filter(col("l_present").isNull || col("r_present").isNull).count()
  val beyondTol = if (doubles.isEmpty) 0L else
    joined.filter(doubles.map(d => !closeEnough(col(s"l_$d"), col(s"r_$d"))).reduce(_ || _)).count()
  val maxRel = if (doubles.isEmpty) "n/a" else joined.select(doubles.map(d =>
    max(when(col(s"l_$d") =!= col(s"r_$d"), abs(col(s"l_$d") - col(s"r_$d")) / greatest(abs(col(s"l_$d")), abs(col(s"r_$d"))))
      .otherwise(lit(0.0)))).reduce(greatest(_, _)).alias("m")).first().get(0).toString
  println(f"RESULT $op%-3s schema_equal=$sameSchema rows_off=$nOff%,d rows_on=$nOn%,d " +
    f"exceptAll(off-on)=$offMinusOn exceptAll(on-off)=$onMinusOff key_unique=$keyUnique " +
    f"unmatched_keys=$unmatched doubles=${doubles.length} beyond_tol=$beyondTol max_rel_diff=$maxRel")
  println(s"KEYS $op: ${keys.mkString(",")}")
  println(s"DOUBLES $op: ${doubles.mkString(",")}")
}

}
Seq("m1", "m2", "m3", "o1", "o3").foreach(Opt1Compare.compare)
System.exit(0)
