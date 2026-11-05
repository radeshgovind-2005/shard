package com.radeshgovind

import com.radeshgovind.cli.ArgsParse
import com.radeshgovind.cli.PipelineArgs
import com.radeshgovind.cli.USAGE
import com.radeshgovind.cli.parseArgs
import com.radeshgovind.ingest.TripFiles
import com.radeshgovind.ingest.findTripFiles
import com.radeshgovind.ingest.missingZonesFile
import java.nio.file.Path
import kotlin.system.exitProcess

fun main(rawArgs: Array<String>) {
    when (val parsed = parseArgs(rawArgs)) {
        is ArgsParse.Parsed -> runWithInputFiles(parsed.args)
        is ArgsParse.Invalid -> exitWithUsage(parsed.reason)
    }
}

private fun runWithInputFiles(args: PipelineArgs) {
    when (val files = findTripFiles(args.input, args.months)) {
        is TripFiles.Found -> runWithZonesFile(args, files)
        is TripFiles.Missing -> exitWithMissingFiles(files.paths)
    }
}

private fun runWithZonesFile(args: PipelineArgs, files: TripFiles.Found) {
    val missing = missingZonesFile(args.input, args.operations)
    if (missing == null) runWithSpark(args, files) else exitWithMissingFiles(listOf(missing))
}

private fun runWithSpark(args: PipelineArgs, files: TripFiles.Found) {
    createSparkSession().use { spark -> runPipeline(spark, args, files) }
}

private fun exitWithUsage(reason: String): Nothing {
    System.err.println("shard: $reason")
    System.err.println(USAGE)
    exitProcess(BAD_INPUT)
}

private fun exitWithMissingFiles(paths: List<Path>): Nothing {
    System.err.println("shard: missing input files (run `make data`):")
    paths.forEach { System.err.println("  $it") }
    exitProcess(BAD_INPUT)
}

private const val BAD_INPUT = 2
