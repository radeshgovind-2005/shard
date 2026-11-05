package com.radeshgovind.cli

import com.radeshgovind.profiling.RunId
import java.nio.file.Path
import java.time.YearMonth
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertIs

class ParseArgsTest {
    @Test
    fun `defaults to all of 2025, the whole dataset and every operation`() {
        val expected =
            PipelineArgs(Path.of("data"), Path.of("out"), months("2025-01", "2025-12"), Fraction.WHOLE, Operation.ALL)
        assertEquals(expected, parsed())
    }

    @Test
    fun `reads a single month`() {
        assertEquals(months("2025-03", "2025-03"), parsed("--years", "2025-03").months)
    }

    @Test
    fun `reads a range of whole years`() {
        assertEquals(months("2024-01", "2025-12"), parsed("--years", "2024-2025").months)
    }

    @Test
    fun `reads fraction and operations, ignoring case`() {
        val args = parsed("--fraction", "0.1", "--ops", "m2,M3")
        assertEquals(Fraction(0.1), args.fraction)
        assertEquals(setOf(Operation.M2, Operation.M3), args.operations)
    }

    @Test
    fun `reads the optional operation O1`() {
        assertEquals(setOf(Operation.O1), parsed("--ops", "o1").operations)
        assertEquals(setOf(Operation.M1, Operation.O1), parsed("--ops", "m1,O1").operations)
    }

    @Test
    fun `reads the optional operation O3`() {
        assertEquals(setOf(Operation.M1, Operation.O3), parsed("--ops", "m1,o3").operations)
    }

    @Test
    fun `requires input and output`() {
        assertContains(reasonFor("--output", "out"), "--input")
        assertContains(reasonFor("--input", "data"), "--output")
    }

    @Test
    fun `rejects years before 2011, impossible months and reversed ranges`() {
        listOf("2010", "2025-13", "2026-2025", "25", "2025-1", "2025/01").forEach {
            assertContains(reasonWithRequired("--years", it), "--years", message = "for --years $it")
        }
    }

    @Test
    fun `rejects fractions that are not above 0 and at most 1`() {
        listOf("0", "1.5", "-0.2", "abc", "NaN").forEach {
            assertContains(reasonWithRequired("--fraction", it), "--fraction", message = "for --fraction $it")
        }
    }

    @Test
    fun `rejects unknown operations`() {
        assertContains(reasonWithRequired("--ops", "m4"), "--ops")
        assertContains(reasonWithRequired("--ops", "o4"), "--ops")
        assertContains(reasonWithRequired("--ops", "m1,,m2"), "--ops")
    }

    @Test
    fun `rejects repeated, unknown and value-less flags`() {
        assertContains(reasonWithRequired("--input", "other"), "more than once")
        assertContains(reasonWithRequired("--verbose", "yes"), "unknown flag")
        assertContains(reasonWithRequired("--years"), "needs a value")
        assertContains(reasonFor("--input", "--output", "out"), "needs a value")
    }

    @Test
    fun `a run id turns profiling on, with the default runs folder`() {
        val expected = ProfilingArgs(RunId("local-20261105-01"), Path.of("experiments/runs"), "abc1234")
        assertEquals(expected, parsed("--run-id", "local-20261105-01", "--git-sha", "abc1234").profiling)
    }

    @Test
    fun `reads the runs folder, and leaves the git sha out when it isn't given`() {
        val expected = ProfilingArgs(RunId("docker-20261105-02"), Path.of("/runs"), null)
        assertEquals(expected, parsed("--run-id", "docker-20261105-02", "--runs-dir", "/runs").profiling)
    }

    @Test
    fun `rejects malformed run ids`() {
        listOf("prod-20261105-01", "local-2026115-01", "local-20261105-1", "local-20261105").forEach {
            assertContains(reasonWithRequired("--run-id", it), "--run-id", message = "for --run-id $it")
        }
    }

    @Test
    fun `rejects profiling flags without a run id, and a malformed git sha`() {
        assertContains(reasonWithRequired("--git-sha", "abc1234"), "only works with --run-id")
        assertContains(reasonWithRequired("--runs-dir", "/runs"), "only works with --run-id")
        assertContains(reasonWithRequired("--run-id", "local-20261105-01", "--git-sha", "xyz"), "--git-sha")
    }

    private val required = arrayOf("--input", "data", "--output", "out")

    private fun parsed(vararg extra: String): PipelineArgs =
        assertIs<ArgsParse.Parsed>(parseArgs(required + extra)).args

    private fun reasonWithRequired(vararg extra: String): String = reasonFor(*(required + extra))

    private fun reasonFor(vararg raw: String): String = assertIs<ArgsParse.Invalid>(parseArgs(arrayOf(*raw))).reason

    private fun months(first: String, last: String) = MonthRange(YearMonth.parse(first), YearMonth.parse(last))
}
