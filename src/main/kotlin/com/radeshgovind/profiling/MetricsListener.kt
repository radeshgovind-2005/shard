package com.radeshgovind.profiling

import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.SparkListenerJobEnd
import org.apache.spark.scheduler.SparkListenerJobStart
import org.apache.spark.scheduler.SparkListenerStageCompleted
import org.apache.spark.scheduler.SparkListenerTaskEnd
import scala.jdk.javaapi.CollectionConverters
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue

// Records what Spark reports, keyed by job group. Called on Spark's listener thread, read on the driver thread
// after SparkProfiler has drained the listener bus, so it holds nothing but concurrent maps.
internal class MetricsListener : SparkListener() {
    private val groupOfJob = ConcurrentHashMap<Int, String>()
    private val stagesOfJob = ConcurrentHashMap<Int, List<Int>>()
    private val endedJobs = ConcurrentHashMap.newKeySet<Int>()
    private val taskDurationsMs = ConcurrentHashMap<Int, ConcurrentLinkedQueue<Long>>()
    private val completedStages = ConcurrentHashMap<Int, StageMetrics>()

    override fun onJobStart(jobStart: SparkListenerJobStart) {
        val group = jobStart.properties()?.getProperty(JOB_GROUP_KEY) ?: return
        groupOfJob[jobStart.jobId()] = group
        stagesOfJob[jobStart.jobId()] = CollectionConverters.asJava(jobStart.stageIds()).map { it as Int }
    }

    // Failed and killed attempts are left out: they'd distort the skew ratio.
    override fun onTaskEnd(taskEnd: SparkListenerTaskEnd) {
        val task = taskEnd.taskInfo()
        if (task.successful()) durationsOf(taskEnd.stageId()).add(task.duration())
    }

    // Spark posts every TaskEnd of a stage before its StageCompleted, so the durations are complete here.
    override fun onStageCompleted(stageCompleted: SparkListenerStageCompleted) {
        val info = stageCompleted.stageInfo()
        val durations = taskDurationsMs.remove(info.stageId())?.toList().orEmpty()
        completedStages[info.stageId()] = StageMetrics.of(info, durations)
    }

    override fun onJobEnd(jobEnd: SparkListenerJobEnd) {
        endedJobs += jobEnd.jobId()
    }

    fun jobsIn(group: String): List<Int> = groupOfJob.filterValues { it == group }.keys.sorted()

    fun haveAllEnded(jobs: List<Int>): Boolean = endedJobs.containsAll(jobs)

    // Stages skipped because their shuffle output was reused never complete, so they don't count
    // (the Spark UI lists them as "Skipped Stages").
    fun completedStagesOf(jobs: List<Int>): List<StageMetrics> = jobs
        .flatMap { stagesOfJob[it].orEmpty() }
        .distinct()
        .mapNotNull { completedStages[it] }
        .sortedBy(StageMetrics::stageId)

    private fun durationsOf(stageId: Int): ConcurrentLinkedQueue<Long> =
        taskDurationsMs.computeIfAbsent(stageId) { ConcurrentLinkedQueue() }

    private companion object {
        const val JOB_GROUP_KEY = "spark.jobGroup.id"
    }
}
