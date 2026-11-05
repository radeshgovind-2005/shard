package com.radeshgovind.profiling

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.radeshgovind.localTestSession
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class SparkProfilerTest {
    @TempDir
    lateinit var runDir: Path

    @Test
    fun `a job with a shuffle records its stages, tasks and shuffle bytes`() {
        val run = profiledRun { shuffleJob() }
        assertTrue(run.numStages >= 2, "stages: ${run.numStages}")
        assertTrue(run.shuffleWriteBytes > 0, "shuffle write: ${run.shuffleWriteBytes}")
        assertTrue(run.shuffleReadBytes > 0, "shuffle read: ${run.shuffleReadBytes}")
        assertTrue(run.numTasks > 0, "tasks: ${run.numTasks}")
        assertTrue(run.wallTimeS > 0.0, "wall time: ${run.wallTimeS}")
    }

    // collect(), not count(): count() plans an Exchange SinglePartition, which is a (tiny) shuffle.
    @Test
    fun `a job without a shuffle writes no shuffle bytes`() {
        assertEquals(0L, profiledRun { noShuffleJob() }.shuffleWriteBytes)
    }

    @Test
    fun `two blocks back to back each keep only their own stages`() {
        val (first, second) = profiledRuns(listOf("shuffle" to ::shuffleJob, "no-shuffle" to ::noShuffleJob))
        val firstStages = first.stages.map(StageMetrics::stageId).toSet()
        val secondStages = second.stages.map(StageMetrics::stageId).toSet()
        assertTrue(firstStages.isNotEmpty() && secondStages.isNotEmpty())
        assertTrue(firstStages.intersect(secondStages).isEmpty(), "shared stages: $firstStages, $secondStages")
    }

    @Test
    fun `finish writes run json, stages json and the plans`() {
        profiledRuns(listOf("m1" to ::shuffleJob))
        val run = readJson(RunReport.RUN_FILE)
        assertEquals("local-20261105-01", run["run_id"].asText())
        assertEquals("local", run["env"].asText())
        assertEquals("m1", run["runs"][0]["name"].asText())
        assertTrue(run["runs"][0].has("wall_time_s"))
        val stage = readJson(RunReport.STAGES_FILE)[0]
        listOf("profile", "stage_id", "task_ms_median", "task_ms_max", "skew_ratio").forEach {
            assertTrue(stage.has(it), "stages.json has no $it: $stage")
        }
        assertTrue(Files.readString(runDir.resolve(RunReport.PLANS_DIR).resolve("m1.txt")).isNotBlank())
    }

    private fun profiledRun(block: () -> Unit): ProfiledRun = profiledRuns(listOf("block" to block)).single()

    private fun profiledRuns(blocks: List<Pair<String, () -> Unit>>): List<ProfiledRun> {
        val profiler = SparkProfiler(spark, runDir, HEADER)
        blocks.forEach { (name, block) -> profiler.profile(name, block) }
        profiler.finish()
        return profiler.runs
    }

    private fun shuffleJob() {
        spark.range(100_000).selectExpr("id % 10 AS k").groupBy("k").count().collect()
    }

    private fun noShuffleJob() {
        spark.range(1000).collect()
    }

    private fun readJson(file: String): JsonNode = ObjectMapper().readTree(runDir.resolve(file).toFile())

    companion object {
        private lateinit var spark: SparkSession

        private val HEADER = RunHeader(RunId("local-20261105-01"), "abc1234", mapOf("input" to "data"))

        @JvmStatic
        @BeforeAll
        fun startSpark() {
            spark = localTestSession()
        }

        @JvmStatic
        @AfterAll
        fun stopSpark() {
            spark.stop()
        }
    }
}
