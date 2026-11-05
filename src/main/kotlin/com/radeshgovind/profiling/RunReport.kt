package com.radeshgovind.profiling

import com.fasterxml.jackson.annotation.JsonUnwrapped
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.PropertyNamingStrategies
import com.fasterxml.jackson.databind.SerializationFeature
import java.nio.file.Files
import java.nio.file.Path
import java.util.Locale

// run.json: what ran, where, on which code, and every profiled block.
data class RunRecord(
    val runId: String,
    val gitSha: String?,
    val env: String,
    val master: String,
    val sparkVersion: String,
    val args: Map<String, String>,
    val sparkConf: Map<String, String>,
    val startedUtc: String,
    val runs: List<ProfiledRun>,
)

// One line of stages.json: a stage, tagged with the profile block it ran in.
data class ProfiledStage(val profile: String, @get:JsonUnwrapped val stage: StageMetrics)

// The fixed layout of experiments/runs/<run-id>/ (roadmap 01 B4).
internal class RunReport(private val runDir: Path) {
    fun write(record: RunRecord, plansByProfile: Map<String, String>) {
        Files.createDirectories(runDir.resolve(PLANS_DIR))
        writeJson(runDir.resolve(RUN_FILE), record)
        writeJson(runDir.resolve(STAGES_FILE), stagesOf(record.runs))
        plansByProfile.forEach { (profile, plan) -> writePlan(profile, plan) }
    }

    fun printSummary(runs: List<ProfiledRun>) {
        println("profiled run $runDir")
        println(SUMMARY_FORMAT.format(*SUMMARY_HEADER))
        runs.forEach { println(summaryLine(it)) }
    }

    private fun writeJson(file: Path, value: Any) = JSON.writeValue(file.toFile(), value)

    private fun summaryLine(run: ProfiledRun): String = SUMMARY_FORMAT.format(
        run.name,
        twoDecimals(run.wallTimeS),
        run.numStages,
        run.numTasks,
        formatBytes(run.shuffleReadBytes),
        formatBytes(run.shuffleWriteBytes),
        formatBytes(run.diskSpilledBytes),
        run.maxSkewRatio()?.let(::twoDecimals) ?: "-",
    )

    private fun stagesOf(runs: List<ProfiledRun>): List<ProfiledStage> =
        runs.flatMap { run -> run.stages.map { ProfiledStage(run.name, it) } }

    private fun writePlan(profile: String, plan: String) {
        Files.writeString(runDir.resolve(PLANS_DIR).resolve("$profile.txt"), plan)
    }

    companion object {
        const val RUN_FILE = "run.json"
        const val STAGES_FILE = "stages.json"
        const val PLANS_DIR = "plans"

        // Spark's event log goes here. It must exist before the session starts, or Spark refuses to start.
        fun eventLogDirOf(runDir: Path): Path = runDir.resolve("eventlog")

        // snake_case, so wallTimeS is written as wall_time_s: the B3 names, from the Kotlin property names.
        private val JSON: ObjectMapper = ObjectMapper()
            .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
            .enable(SerializationFeature.INDENT_OUTPUT)

        // spill = disk spill, the signal that partitions don't fit in memory.
        private const val SUMMARY_FORMAT = "%-10s | %8s | %6s | %6s | %10s | %10s | %10s | %8s"
        private val SUMMARY_HEADER =
            arrayOf("profile", "wall s", "stages", "tasks", "shuffle R", "shuffle W", "spill", "max skew")
    }
}

// Locale.ROOT: a decimal point, whatever the machine's locale.
private fun twoDecimals(value: Double): String = String.format(Locale.ROOT, "%.2f", value)

// Binary units, one decimal: 1536 → "1.5 KiB".
internal fun formatBytes(bytes: Long): String {
    val unit = BYTE_UNITS.lastOrNull { bytes >= it.second } ?: return "$bytes B"
    return String.format(Locale.ROOT, "%.1f %s", bytes.toDouble() / unit.second, unit.first)
}

private val BYTE_UNITS = listOf("KiB" to (1L shl 10), "MiB" to (1L shl 20), "GiB" to (1L shl 30), "TiB" to (1L shl 40))
