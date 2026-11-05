package com.radeshgovind.profiling

// <env>-<yyyymmdd>-<nn>, e.g. local-20261105-01. Never reused: each id names exactly one run in experiments/LOG.md.
@JvmInline
value class RunId(val value: String) {
    init {
        require(PATTERN.matches(value)) { "run id must look like local-20261105-01: $value" }
    }

    val env: String get() = value.substringBefore('-')

    override fun toString(): String = value

    companion object {
        private val PATTERN = Regex("""^(local|docker|di)-\d{8}-\d{2}$""")

        fun parse(raw: String): RunId? = raw.takeIf(PATTERN::matches)?.let(::RunId)
    }
}
