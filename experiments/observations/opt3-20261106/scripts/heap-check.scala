// OPT-3 (H6) feasibility check for 1 GiB heaps, in the Docker master's spark-shell (local[2], default driver memory
// = 1g, so one 1 GiB JVM builds the broadcast as the driver AND runs the tasks as the executor). A plan check, not a
// measurement. Runs O3 on 2025 from the pipeline jar's own functions with the lag-join plan named in `planName`
// (prepended by the command line), writes to the `noop` sink, and prints the JVM's max heap, the peak used heap
// (sampled every 50 ms), G1 Old Gen's peak, GC count/time, the heap still live after a full GC once the write is done
// (the broadcast relations are still held then, by the block manager and the plan), and whether the final plan has
// the broadcast self-join.
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.radeshgovind.cli.MonthRange
import com.radeshgovind.clean.RemoveInvalidTripsKt
import com.radeshgovind.forecast.{DemandForecastKt, LagJoinPlan, ZoneHourGridKt}
import com.radeshgovind.ingest.{EnforceSchemaKt, LoadTripsKt, LoadZonesKt, TripFiles}
import java.lang.management.ManagementFactory
import scala.jdk.CollectionConverters._

val plan = LagJoinPlan.valueOf(planName)
val months = new MonthRange(java.time.YearMonth.of(2025, 1), java.time.YearMonth.of(2025, 12))
val found = new TripFiles.Found((1 to 12).map(m => java.nio.file.Path.of(f"/data/trips/yellow_tripdata_2025-$m%02d.parquet")).asJava)
// loadTrips takes a Fraction (a Kotlin value class), so its JVM name is mangled and it takes the raw double.
val load = classOf[LoadTripsKt].getMethod("loadTrips-mXa3Sx8", classOf[SparkSession], classOf[TripFiles.Found], java.lang.Double.TYPE)
val cleaned = RemoveInvalidTripsKt.removeInvalidTrips(EnforceSchemaKt.enforceSchema(load.invoke(null, spark, found, Double.box(1.0)).asInstanceOf[DataFrame]))
val grid = ZoneHourGridKt.zoneHourGrid(spark, months, new kotlin.ranges.IntRange(1, 263))
val zones = LoadZonesKt.loadZones(spark, java.nio.file.Path.of("/data"))
val forecast = DemandForecastKt.demandForecast(cleaned, grid, zones, plan)

val rt = Runtime.getRuntime
val pools = ManagementFactory.getMemoryPoolMXBeans.asScala.filter(_.getType == java.lang.management.MemoryType.HEAP)
val gcs = ManagementFactory.getGarbageCollectorMXBeans.asScala
System.gc(); pools.foreach(_.resetPeakUsage())
val gc0 = gcs.map(g => (g.getCollectionCount, g.getCollectionTime)).reduce((a, b) => (a._1 + b._1, a._2 + b._2))
@volatile var peakUsed = 0L; @volatile var sampling = true
val sampler = new Thread(() => while (sampling) { peakUsed = math.max(peakUsed, rt.totalMemory - rt.freeMemory); Thread.sleep(50) })
sampler.setDaemon(true); sampler.start()
val t0 = System.nanoTime
val outcome = try { forecast.write.format("noop").mode("overwrite").save(); "ok" } catch { case e: Throwable => s"FAILED ${e.getClass.getName}: ${e.getMessage.take(300)}" }
val secs = (System.nanoTime - t0) / 1e9
sampling = false; sampler.join()
System.gc(); Thread.sleep(500); System.gc()
val liveAfter = rt.totalMemory - rt.freeMemory
val gc1 = gcs.map(g => (g.getCollectionCount, g.getCollectionTime)).reduce((a, b) => (a._1 + b._1, a._2 + b._2))
val mib = (b: Long) => f"${b / 1048576.0}%.0f MiB"
val finalPlan = forecast.queryExecution.executedPlan.toString
println(s"HEAP plan=$plan outcome=$outcome max_heap=${mib(rt.maxMemory)} peak_used_sampled=${mib(peakUsed)} " +
  pools.map(p => s"peak[${p.getName}]=${mib(p.getPeakUsage.getUsed)}").mkString(" ") +
  s" live_after_full_gc=${mib(liveAfter)} gc_count=${gc1._1 - gc0._1} gc_time_ms=${gc1._2 - gc0._2} " +
  s"bhj_leftouter_buildright=${"BroadcastHashJoin \\[PULocationID#\\d+, hour_index#\\d+\\], \\[PULocationID#\\d+, hour_index#\\d+\\], LeftOuter, BuildRight".r.findAllIn(finalPlan).size} " +
  s"smj_leftouter=${"SortMergeJoin \\[PULocationID#\\d+, hour_index#\\d+\\], \\[PULocationID#\\d+, hour_index#\\d+\\], LeftOuter".r.findAllIn(finalPlan).size} " +
  f"elapsed_s=$secs%.1f (single run, not a measurement)")
System.exit(0)
