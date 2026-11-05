package com.radeshgovind.cli

// readsZones: the operation joins taxi_zone_lookup.csv, so Main checks that file before Spark starts.
enum class Operation(val readsZones: Boolean) {
    M1(readsZones = false),
    M2(readsZones = true),

    // M3 joins the borough from the zone lookup too (roadmap 00 §1d).
    M3(readsZones = true),

    // O1 joins the pickup borough for the weekday vs weekend rate table.
    O1(readsZones = true),

    // O3 annotates each zone with its service_zone.
    O3(readsZones = true),
    ;

    companion object {
        val ALL: Set<Operation> = entries.toSet()

        fun parse(raw: String): Operation? = entries.find { it.name.equals(raw.trim(), ignoreCase = true) }

        fun parseList(raw: String): Set<Operation>? = parseCommaList(raw, ::parse)
    }
}
