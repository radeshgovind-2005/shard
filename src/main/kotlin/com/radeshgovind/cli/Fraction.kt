package com.radeshgovind.cli

@JvmInline
value class Fraction(val value: Double) {
    init {
        require(isInRange(value)) { "fraction must be in (0, 1]: $value" }
    }

    val isWhole: Boolean get() = value == 1.0

    companion object {
        val WHOLE = Fraction(1.0)

        fun parse(raw: String): Fraction? = raw.toDoubleOrNull()?.takeIf(::isInRange)?.let(::Fraction)

        private fun isInRange(value: Double): Boolean = value > 0.0 && value <= 1.0
    }
}
