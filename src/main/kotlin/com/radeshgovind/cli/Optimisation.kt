package com.radeshgovind.cli

// The applied optimisations (experiments/LOG.md → Optimisations). Each is on unless --skip-opts names it, so the
// before and after runs of an OPT use the same jar, and the default pipeline is the optimised one.
enum class Optimisation {
    // None yet. OPT-1 (H1, persist the cleaned trips) was measured and reverted, so its id is gone; the code that was
    // measured is at 0d5657b. Each kept or pending OPT adds an entry here.
    ;

    val id: String get() = name.lowercase()

    companion object {
        fun parse(raw: String): Optimisation? = entries.find { it.id == raw.trim().lowercase() }

        fun parseList(raw: String): Set<Optimisation>? = parseCommaList(raw, ::parse)
    }
}
