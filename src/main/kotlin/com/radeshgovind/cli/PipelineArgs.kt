package com.radeshgovind.cli

import java.nio.file.Path

data class PipelineArgs(
    val input: Path,
    val output: Path,
    val months: MonthRange,
    val fraction: Fraction,
    val operations: Set<Operation>,
)
