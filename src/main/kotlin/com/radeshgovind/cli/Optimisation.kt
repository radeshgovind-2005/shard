package com.radeshgovind.cli

// The applied optimisations (experiments/LOG.md → Optimisations). Each is on unless --skip-opts names it, so the
// before and after runs of an OPT use the same jar, and the default pipeline is the optimised one.
enum class Optimisation {
    // H1: persist the cleaned trips, so only the first operation that reads them scans the Parquet files.
    OPT1,
    ;

    val id: String get() = name.lowercase()

    companion object {
        fun parse(raw: String): Optimisation? = entries.find { it.id == raw.trim().lowercase() }

        fun parseList(raw: String): Set<Optimisation>? = parseCommaList(raw, ::parse)
    }
}
