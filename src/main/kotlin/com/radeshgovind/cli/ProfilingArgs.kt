package com.radeshgovind.cli

import com.radeshgovind.profiling.RunId
import java.nio.file.Path

// Present only with --run-id: profiling is on, and everything about it lands in runDir.
data class ProfilingArgs(val runId: RunId, val runsDir: Path, val gitSha: String?) {
    val runDir: Path get() = runsDir.resolve(runId.value)
}
