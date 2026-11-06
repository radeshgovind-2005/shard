package com.radeshgovind.cli

// The applied optimisations (experiments/LOG.md → Optimisations). Each is on unless --skip-opts names it, so the
// before and after runs of an OPT use the same jar, and the default pipeline is the optimised one.
enum class Optimisation {
    // OPT-1 (H1, persist the cleaned trips) was measured and reverted, so its id is gone; the measured code is at
    // 0d5657b. Each kept or pending OPT adds an entry here.

    // H2: O1's bucket statistics by a groupBy broadcast back onto the trips, instead of a window over every trip.
    OPT2,

    // H6: broadcast the 4-week-lag side of O3's self-join, instead of a SortMergeJoin that re-shuffles both sides.
    OPT3,
    ;

    val id: String get() = name.lowercase()

    companion object {
        fun parse(raw: String): Optimisation? = entries.find { it.id == raw.trim().lowercase() }

        fun parseList(raw: String): Set<Optimisation>? = parseCommaList(raw, ::parse)
    }
}
