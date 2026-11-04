# MapReduce Fault Tolerance, Limitations, and What Came After

> **Source:** `ComPar-Lec03.pdf`, "MapReduce Fault Tolerance", "Iterative/Interactive Procedure in MapReduce" (printed 78–79), "MapReduce Limitations", "Beyond MapReduce" (80) · **Back to:** [index](../index.md)

## Fault tolerance: how MapReduce handles the inevitable
Commodity hardware fails. What makes MapReduce fault-tolerant:

| Mechanism | How it works |
|---|---|
| **Stateless mappers** | Each map task reads from HDFS (replicated). There is no state to recover; just restart on a different machine. |
| **HDFS replication** | Input data has **3× replicas**. If one machine fails, the data is still readable from another replica. |
| **Progress monitoring** | The master **pings workers every few seconds**. If there is no heartbeat, the task is re-scheduled elsewhere automatically. |
| **Idempotent tasks** | Map & reduce tasks are **pure functions**: re-running produces the same output, so they are safe to restart at any time. |

**Task failure scenario:**
| t | Event |
|---|---|
| 0 s | Master assigns map tasks to 4 workers |
| 30 s | Worker 2 fails (disk crash) |
| 31 s | Master detects no heartbeat from W2 |
| 32 s | Master re-assigns W2's tasks to W3 |
| 60 s | W3 completes the re-assigned tasks |
| 61 s | Job continues, and the user sees no interruption |

## Iterative & interactive procedures in MapReduce
**Iterative:** each iteration is a chain of jobs (MR1 → MR2 → MR3), and **every iteration does an HDFS read at the start and an HDFS write at the end**, so data goes back to disk between iterations.
```
Iteration 1: HDFS read → MR1 → MR2 → MR3 → HDFS write → Data on disk
Iteration 2: HDFS read → MR1 → MR2 → MR3 → HDFS write → Data on disk ...
```
**Interactive:** each query (Query 1, 2, 3…) **re-reads the data from disk (HDFS)** independently.

## MapReduce limitations: why iterative algorithms exposed fundamental bottlenecks
MapReduce was transformative, but **not designed for iterative algorithms** (machine learning, graph processing).

| Limitation | Details |
|---|---|
| **Disk I/O at every step** | Each job writes output to HDFS and the next job reads it back. For **k-means (100 iterations) over 1 TB: 100× read + 100× write to HDFS**. Disk I/O is **10–100× slower than RAM**. |
| **No native iteration support** | Iteration requires an external loop launching N separate MapReduce jobs. Job startup overhead (seconds to minutes) compounds across iterations. There is no way to express "repeat until convergence" inside MapReduce. |
| **Rigid two-phase model** | Only map → shuffle → reduce. Joins, filters, and aggregations can't be expressed in a single job without awkward workarounds. Complex pipelines become chains of jobs with HDFS as a slow inter-stage buffer. |
| **No streaming or interactive** | Batch-only: you must wait for the entire job to finish before seeing results. No low-latency queries, so the feedback loop for data exploration is slow. Spark SQL and Flink eventually addressed this. |

These limitations **directly motivated the design of Apache Spark (2012)**.

## Beyond MapReduce
| Need | Systems |
|---|---|
| Streaming | Spark, Flink, Storm, Hadoop streaming, … |
| Interactive | Spark, Flink, … |
| SQL on Hadoop | Hive, Tez, … |
| In memory | Spark, In-Memory MapReduce, … |
| DAG of many vertices | Spark, Tez, … |
| Machine learning | Mahout, Spark ML, … |

## Related
- [Spark motivation & RDDs](09-spark-motivation-rdd-architecture.md)
- [RDD fault tolerance via lineage](11-spark-dag-stages-caching-fault-tolerance.md)
