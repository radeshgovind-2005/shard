package com.radeshgovind.cli

enum class Operation {
    M1,
    M2,
    M3,
    ;

    companion object {
        val ALL: Set<Operation> = entries.toSet()

        fun parse(raw: String): Operation? = entries.find { it.name.equals(raw.trim(), ignoreCase = true) }

        fun parseList(raw: String): Set<Operation>? = raw.split(",").map(::parse).toSetOrNull()

        private fun List<Operation?>.toSetOrNull(): Set<Operation>? = if (null in
            this
        ) {
            null
        } else {
            filterNotNull().toSet()
        }
    }
}
