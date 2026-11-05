package com.radeshgovind.profiling

import org.apache.spark.scheduler.StageInfo
import scala.Option

// One completed stage. Serialised in snake_case (RunReport), so the names match Lab 3's profiler (roadmap 01 B3).
data class StageMetrics(
    val stageId: Int,
    val name: String,
    val numTasks: Int,
    val durationMs: Long?,
    val executorRunTimeMs: Long,
    val executorCpuTimeMs: Long,
    val inputBytes: Long,
    val outputBytes: Long,
    val shuffleReadBytes: Long,
    val shuffleWriteBytes: Long,
    val memorySpilledBytes: Long,
    val diskSpilledBytes: Long,
    val jvmGcTimeMs: Long,
    val taskMsMedian: Double?,
    val taskMsMax: Long?,
    // max / median task duration. ≥ 10 is the spec's skew signal; AQE already splits partitions at 5.
    val skewRatio: Double?,
) {
    companion object {
        fun of(info: StageInfo, taskDurationsMs: List<Long>): StageMetrics {
            val metrics = info.taskMetrics()
            val tasks = TaskDurations(taskDurationsMs.sorted())
            return StageMetrics(
                stageId = info.stageId(),
                name = info.name(),
                numTasks = info.numTasks(),
                durationMs = durationOf(info),
                executorRunTimeMs = metrics.executorRunTime(),
                executorCpuTimeMs = metrics.executorCpuTime() / NANOS_PER_MILLI,
                inputBytes = metrics.inputMetrics().bytesRead(),
                outputBytes = metrics.outputMetrics().bytesWritten(),
                shuffleReadBytes = metrics.shuffleReadMetrics().totalBytesRead(),
                shuffleWriteBytes = metrics.shuffleWriteMetrics().bytesWritten(),
                memorySpilledBytes = metrics.memoryBytesSpilled(),
                diskSpilledBytes = metrics.diskBytesSpilled(),
                jvmGcTimeMs = metrics.jvmGCTime(),
                taskMsMedian = tasks.median(),
                taskMsMax = tasks.max(),
                skewRatio = tasks.skewRatio(),
            )
        }

        private fun durationOf(info: StageInfo): Long? {
            val submitted = info.submissionTime().millisOrNull() ?: return null
            val completed = info.completionTime().millisOrNull() ?: return null
            return completed - submitted
        }

        private fun Option<Any>.millisOrNull(): Long? = if (isDefined) get() as Long else null

        private const val NANOS_PER_MILLI = 1_000_000L
    }
}

// Durations of a stage's successful tasks, sorted ascending.
private class TaskDurations(private val sortedMs: List<Long>) {
    fun max(): Long? = sortedMs.lastOrNull()

    fun median(): Double? = when {
        sortedMs.isEmpty() -> null
        sortedMs.size % 2 == 1 -> sortedMs[sortedMs.size / 2].toDouble()
        else -> (sortedMs[sortedMs.size / 2 - 1] + sortedMs[sortedMs.size / 2]) / 2.0
    }

    // null when the median is 0 ms: the ratio would be infinite and says nothing about skew.
    fun skewRatio(): Double? {
        val median = median()?.takeIf { it > 0.0 } ?: return null
        return roundTo2Decimals(max()!! / median)
    }
}

internal fun roundTo2Decimals(value: Double): Double = Math.round(value * 100.0) / 100.0
