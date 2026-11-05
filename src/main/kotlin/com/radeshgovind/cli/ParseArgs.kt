package com.radeshgovind.cli

import java.nio.file.Path

sealed interface ArgsParse {
    data class Parsed(val args: PipelineArgs) : ArgsParse

    data class Invalid(val reason: String) : ArgsParse
}

const val USAGE =
    "usage: shard --input <dir> --output <dir> [--years YYYY|YYYY-YYYY|YYYY-MM] [--fraction (0,1]] [--ops m1,m2,m3]"

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
private val KNOWN_FLAGS = setOf(INPUT, OUTPUT, YEARS, FRACTION, OPS)

private const val DEFAULT_YEARS = "2025"

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
    return ArgsParse.Parsed(PipelineArgs(Path.of(input), Path.of(output), months, fraction, operations))
}

private fun invalid(reason: String): ArgsParse = ArgsParse.Invalid(reason)

private const val YEARS_RULE = "$YEARS must be YYYY, YYYY-YYYY or YYYY-MM, from ${MonthRange.FIRST_SUPPORTED_YEAR} on"
private const val FRACTION_RULE = "$FRACTION must be a number in (0, 1]"
private const val OPS_RULE = "$OPS must be a comma-separated list of m1, m2, m3"
