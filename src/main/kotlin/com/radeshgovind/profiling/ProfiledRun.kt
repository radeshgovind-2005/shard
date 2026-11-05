package com.radeshgovind.profiling

// One profile(...) block, aggregated over its completed stages. Field names match Lab 3's spark_profiler.py (B3).
data class ProfiledRun(
    val name: String,
    val wallTimeS: Double,
    val numJobs: Int,
    val numStages: Int,
    val numTasks: Int,
    val executorRunTimeMs: Long,
    val executorCpuTimeMs: Long,
    val inputBytes: Long,
    val outputBytes: Long,
    val shuffleReadBytes: Long,
    val shuffleWriteBytes: Long,
    val memorySpilledBytes: Long,
    val diskSpilledBytes: Long,
    val jvmGcTimeMs: Long,
    val stages: List<StageMetrics>,
) {
    // A function, not a property, so it stays out of run.json.
    fun maxSkewRatio(): Double? = stages.mapNotNull(StageMetrics::skewRatio).maxOrNull()

    companion object {
        fun of(name: String, wallNanos: Long, numJobs: Int, stages: List<StageMetrics>): ProfiledRun = ProfiledRun(
            name = name,
            wallTimeS = roundTo2Decimals(wallNanos / NANOS_PER_SECOND),
            numJobs = numJobs,
            numStages = stages.size,
            numTasks = stages.sumOf { it.numTasks },
            executorRunTimeMs = stages.sumOf { it.executorRunTimeMs },
            executorCpuTimeMs = stages.sumOf { it.executorCpuTimeMs },
            inputBytes = stages.sumOf { it.inputBytes },
            outputBytes = stages.sumOf { it.outputBytes },
            shuffleReadBytes = stages.sumOf { it.shuffleReadBytes },
            shuffleWriteBytes = stages.sumOf { it.shuffleWriteBytes },
            memorySpilledBytes = stages.sumOf { it.memorySpilledBytes },
            diskSpilledBytes = stages.sumOf { it.diskSpilledBytes },
            jvmGcTimeMs = stages.sumOf { it.jvmGcTimeMs },
            stages = stages,
        )

        private const val NANOS_PER_SECOND = 1e9
    }
}
