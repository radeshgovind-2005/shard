package com.radeshgovind.cli

import java.nio.file.Path

data class PipelineArgs(
    val input: Path,
    val output: Path,
    val months: MonthRange,
    val fraction: Fraction,
    val operations: Set<Operation>,
    val profiling: ProfilingArgs? = null,
    val skippedOptimisations: Set<Optimisation> = emptySet(),
) {
    fun outputOf(operation: Operation): Path = output.resolve(operation.name.lowercase())

    fun isApplied(optimisation: Optimisation): Boolean = optimisation !in skippedOptimisations

    // The arguments as run.json records them, so a run can be repeated exactly.
    fun asRecord(): Map<String, String> = mapOf(
        "input" to input.toString(),
        "output" to output.toString(),
        "years" to months.toString(),
        "fraction" to fraction.value.toString(),
        "ops" to operations.sorted().joinToString(",") { it.name.lowercase() },
        "skip_opts" to skippedOptimisations.sorted().joinToString(",") { it.id },
    )
}
