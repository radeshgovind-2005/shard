package com.radeshgovind.profiling

import org.apache.spark.sql.SparkSession
import scala.jdk.javaapi.CollectionConverters
import java.nio.file.Path
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

// Wraps each operation's action and records what Spark did (roadmap 01 Part B). One action per profile block,
// so each operation's metrics are its own.
sealed interface Profiler {
    fun <T> profile(name: String, block: () -> T): T

    fun finish()
}

// Without --run-id: runs the block as it is, so the measured code path is the same with and without profiling.
object NoProfiler : Profiler {
    override fun <T> profile(name: String, block: () -> T): T = block()

    override fun finish() = Unit
}

// What run.json records about the run beyond what the session knows itself.
data class RunHeader(val runId: RunId, val gitSha: String?, val args: Map<String, String>)

class SparkProfiler(private val spark: SparkSession, runDir: Path, private val header: RunHeader) : Profiler {
    private val metrics = MetricsListener()
    private val plans = PlanCapture()
    private val report = RunReport(runDir)
    private val startedUtc = Instant.now().truncatedTo(ChronoUnit.SECONDS)
    private val profiled = mutableListOf<ProfiledRun>()
    private val plansByProfile = linkedMapOf<String, String>()

    val runs: List<ProfiledRun> get() = profiled.toList()

    init {
        spark.sparkContext().addSparkListener(metrics)
        spark.listenerManager().register(plans)
    }

    override fun <T> profile(name: String, block: () -> T): T {
        require(name !in plansByProfile) { "profile name used twice: $name" }
        awaitListenerEvents()
        val plansBefore = plans.count()
        val group = newJobGroup(name)
        val timed = timeInJobGroup(group, name, block)
        awaitListenerEvents()
        record(name, group, timed.nanos, plansBefore)
        return timed.result
    }

    override fun finish() {
        report.write(runRecord(), plansByProfile)
        report.printSummary(profiled)
        spark.sparkContext().removeSparkListener(metrics)
        spark.listenerManager().unregister(plans)
    }

    // Every job the block starts carries the group, including AQE's stage jobs and broadcasts.
    private fun <T> timeInJobGroup(group: String, name: String, block: () -> T): Timed<T> {
        val sc = spark.sparkContext()
        sc.setJobGroup(group, name, false)
        try {
            val start = System.nanoTime()
            val result = block()
            return Timed(result, System.nanoTime() - start)
        } finally {
            sc.clearJobGroup()
        }
    }

    private fun record(name: String, group: String, nanos: Long, plansBefore: Int) {
        val jobs = metrics.jobsIn(group)
        check(metrics.haveAllEnded(jobs)) { "profile $name: the listener hasn't seen every job of $group end" }
        profiled += ProfiledRun.of(name, nanos, jobs.size, metrics.completedStagesOf(jobs))
        plansByProfile[name] = plans.textSince(plansBefore)
    }

    // Listener events arrive asynchronously. By the time an action returns, Spark has posted all of its events
    // (TaskEnd → StageCompleted → JobEnd → SQLExecutionEnd), so once the bus is empty the listeners have seen them
    // all. Waiting before the block too keeps a previous unprofiled action's plan out of this block.
    // listenerBus() is private[spark] in Scala but public in bytecode; Spark's own test suites wait the same way.
    private fun awaitListenerEvents() = spark.sparkContext().listenerBus().waitUntilEmpty(LISTENER_TIMEOUT_MS)

    private fun runRecord(): RunRecord = RunRecord(
        runId = header.runId.value,
        gitSha = header.gitSha,
        env = header.runId.env,
        master = spark.sparkContext().master(),
        sparkVersion = spark.version(),
        args = header.args,
        sparkConf = explicitlySetConf(),
        startedUtc = startedUtc.toString(),
        runs = profiled.toList(),
    )

    // Only what was set (builder, spark-submit, spark-defaults.conf), so Spark's defaults aren't listed.
    private fun explicitlySetConf(): Map<String, String> =
        CollectionConverters.asJava(spark.conf().getAll()).toSortedMap()

    private fun newJobGroup(name: String): String = "$name-${UUID.randomUUID().toString().take(GROUP_SUFFIX_LENGTH)}"

    private class Timed<T>(val result: T, val nanos: Long)

    private companion object {
        const val LISTENER_TIMEOUT_MS = 30_000L
        const val GROUP_SUFFIX_LENGTH = 8
    }
}
