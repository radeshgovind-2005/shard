// Catalyst's size estimates for every join in O3's optimized plan (what the static join choice reads).
// Run: spark-shell --master local[2] --jars /tmp/shard-o3.jar < /tmp/join-size-estimates.scala (Docker master).
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.functions.lit
import java.time.YearMonth
import com.radeshgovind.cli.MonthRange
import org.apache.spark.sql.{DataFrame, Dataset, Row}

def estimates(label: String, first: YearMonth, last: YearMonth): Unit = {
  val months = Iterator.iterate(first)(_.plusMonths(1)).takeWhile(!_.isAfter(last)).toSeq
  val files = months.map(m => f"/data/trips/yellow_tripdata_${m.getYear}%d-${m.getMonthValue}%02d.parquet")
  // The pipeline's own loader (LoadTripsKt.loadTrips, Fraction 1.0), so the scan carries _metadata.file_path as in a real run.
  val found = com.radeshgovind.ingest.TripFilesKt.findTripFiles(java.nio.file.Path.of("/data"), new MonthRange(first, last)).asInstanceOf[com.radeshgovind.ingest.TripFiles.Found]
  // Fraction is a Kotlin value class, so the JVM name is mangled and Scala reaches it only by reflection.
  val raw = Class.forName("com.radeshgovind.ingest.LoadTripsKt")
    .getMethod("loadTrips-mXa3Sx8", classOf[org.apache.spark.sql.SparkSession], classOf[com.radeshgovind.ingest.TripFiles.Found], java.lang.Double.TYPE)
    .invoke(null, spark, found, Double.box(1.0)).asInstanceOf[DataFrame]
  val cleaned: DataFrame = com.radeshgovind.clean.RemoveInvalidTripsKt.removeInvalidTrips(com.radeshgovind.ingest.EnforceSchemaKt.enforceSchema(raw)).asInstanceOf[DataFrame]
  val grid = com.radeshgovind.forecast.ZoneHourGridKt.zoneHourGrid$default(spark, new MonthRange(first, last), null, 2, null).asInstanceOf[DataFrame]
  val zones = com.radeshgovind.ingest.LoadZonesKt.loadZones(spark, java.nio.file.Path.of("/data")).asInstanceOf[DataFrame]
  val plan = com.radeshgovind.forecast.DemandForecastKt.demandForecast(cleaned, grid, zones).queryExecution.optimizedPlan
  println(s"## $label (autoBroadcastJoinThreshold = ${spark.conf.get("spark.sql.autoBroadcastJoinThreshold")})")
  println(f"## input files: ${files.size}, bytes on disk: ${files.map(f => new java.io.File(f).length).sum}%,d")
  plan.collect { case j: Join => j }.foreach { j =>
    println(f"JOIN ${j.joinType}%-10s keys=${j.condition.map(_.references.map(_.name).toSeq.distinct.mkString(",")).getOrElse("-")}%-55s left=${j.left.stats.sizeInBytes}%,28d B  right=${j.right.stats.sizeInBytes}%,22d B")
  }
}
estimates("2025-01", YearMonth.of(2025, 1), YearMonth.of(2025, 1))
estimates("2025", YearMonth.of(2025, 1), YearMonth.of(2025, 12))
:quit
