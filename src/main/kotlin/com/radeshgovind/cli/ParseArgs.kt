package com.radeshgovind.cli

import com.radeshgovind.profiling.RunId
import java.nio.file.Path

sealed interface ArgsParse {
    data class Parsed(val args: PipelineArgs) : ArgsParse

    data class Invalid(val reason: String) : ArgsParse
}

const val USAGE =
    "usage: shard --input <dir> --output <dir> [--years YYYY|YYYY-YYYY|YYYY-MM] [--fraction (0,1]]" +
        " [--ops m1,m2,m3,o3] [--run-id <env>-<yyyymmdd>-<nn> [--runs-dir <dir>] [--git-sha <sha>]]"

fun parseArgs(raw: Array<String>): ArgsParse {
    val pairs = raw.toList().chunked(2)
    val problem = pairs.firstNotNullOfOrNull(::problemWith) ?: repeatedFlag(pairs)
    return if (problem == null) fromFlags(pairs.associate { it[0] to it[1] }) else ArgsParse.Invalid(problem)
}

private const val INPUT = "--input"
private const val OUTPUT = "--output"
private const val YEARS = "--years"
private const val FRACTION = "--fraction"
private const val OPS = "--ops"
private const val RUN_ID = "--run-id"
private const val RUNS_DIR = "--runs-dir"
private const val GIT_SHA = "--git-sha"
private val KNOWN_FLAGS = setOf(INPUT, OUTPUT, YEARS, FRACTION, OPS, RUN_ID, RUNS_DIR, GIT_SHA)

private const val DEFAULT_YEARS = "2025"
private const val DEFAULT_RUNS_DIR = "experiments/runs"
private val GIT_SHA_PATTERN = Regex("""^[0-9a-f]{7,40}$""")

private fun problemWith(pair: List<String>): String? = when {
    pair[0] !in KNOWN_FLAGS -> "unknown flag '${pair[0]}'"
    pair.size < 2 || isMissingValue(pair[1]) -> "${pair[0]} needs a value"
    else -> null
}

private fun isMissingValue(value: String): Boolean = value.isBlank() || value.startsWith("--")

private fun repeatedFlag(pairs: List<List<String>>): String? = pairs.groupingBy {
    it[0]
}.eachCount().filterValues { it > 1 }.keys.firstOrNull()?.let { "$it given more than once" }

private fun fromFlags(flags: Map<String, String>): ArgsParse {
    val input = flags[INPUT] ?: return invalid("$INPUT is required")
    val output = flags[OUTPUT] ?: return invalid("$OUTPUT is required")
    val months = MonthRange.parse(flags[YEARS] ?: DEFAULT_YEARS) ?: return invalid(YEARS_RULE)
    val fraction = flags[FRACTION]?.let { Fraction.parse(it) ?: return invalid(FRACTION_RULE) } ?: Fraction.WHOLE
    val operations = flags[OPS]?.let { Operation.parseList(it) ?: return invalid(OPS_RULE) } ?: Operation.ALL
    profilingProblem(flags)?.let { return invalid(it) }
    return ArgsParse.Parsed(
        PipelineArgs(Path.of(input), Path.of(output), months, fraction, operations, profilingFrom(flags)),
    )
}

private fun profilingProblem(flags: Map<String, String>): String? = when {
    RUN_ID !in flags -> PROFILING_FLAGS.firstOrNull { it in flags }?.let { "$it only works with $RUN_ID" }
    RunId.parse(flags.getValue(RUN_ID)) == null -> RUN_ID_RULE
    flags[GIT_SHA]?.let(GIT_SHA_PATTERN::matches) == false -> GIT_SHA_RULE
    else -> null
}

private val PROFILING_FLAGS = listOf(RUNS_DIR, GIT_SHA)

// Assumes profilingProblem found nothing.
private fun profilingFrom(flags: Map<String, String>): ProfilingArgs? = flags[RUN_ID]?.let {
    ProfilingArgs(RunId(it), Path.of(flags[RUNS_DIR] ?: DEFAULT_RUNS_DIR), flags[GIT_SHA])
}

private fun invalid(reason: String): ArgsParse = ArgsParse.Invalid(reason)

private const val YEARS_RULE = "$YEARS must be YYYY, YYYY-YYYY or YYYY-MM, from ${MonthRange.FIRST_SUPPORTED_YEAR} on"
private const val FRACTION_RULE = "$FRACTION must be a number in (0, 1]"
private const val OPS_RULE = "$OPS must be a comma-separated list of m1, m2, m3, o3"
private const val RUN_ID_RULE = "$RUN_ID must be <local|docker|di>-<yyyymmdd>-<nn>, e.g. local-20261105-01"
private const val GIT_SHA_RULE = "$GIT_SHA must be 7–40 lowercase hex digits (git rev-parse --short HEAD)"
