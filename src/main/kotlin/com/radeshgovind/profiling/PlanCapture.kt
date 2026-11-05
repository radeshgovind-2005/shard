package com.radeshgovind.profiling

import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener
import java.util.concurrent.CopyOnWriteArrayList

// Keeps the physical plan of every successful action. onSuccess runs after the action, so with AQE the plan
// is the final one (isFinalPlan=true): the joins and partition counts AQE actually chose.
internal class PlanCapture : QueryExecutionListener {
    private val plans = CopyOnWriteArrayList<String>()

    override fun onSuccess(funcName: String, qe: QueryExecution, durationNs: Long) {
        plans += "== $funcName ==\n${qe.executedPlan()}"
    }

    override fun onFailure(funcName: String, qe: QueryExecution, exception: Exception) = Unit

    fun count(): Int = plans.size

    fun textSince(index: Int): String = plans.drop(index).joinToString("\n\n")
}
