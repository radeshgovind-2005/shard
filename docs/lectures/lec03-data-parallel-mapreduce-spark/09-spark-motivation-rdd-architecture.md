# Apache Spark: Motivation, RDDs, Languages, Deployment, Architecture

> **Source:** `ComPar-Lec03.pdf`, Part III "Apache Spark: In-memory cluster computing for iterative and interactive workloads" · **Back to:** [index](../index.md)
> Bibliography: Apache Spark Tutorial (https://www.tutorialspoint.com/apache_spark/index.htm). Part I of Chambers & Zaharia, *Spark: The Definitive Guide: Big Data Processing Made Simple*, O'Reilly, 2018.

## Why Spark? MapReduce vs Spark
| MapReduce | Apache Spark |
|---|---|
| Intermediate data written to HDFS disk | Intermediate data **kept in RAM (RDDs)** |
| Job startup overhead per iteration | Single JVM process per job, low overhead |
| No iterative or interactive support | First-class iteration: `RDD.cache()` |
| Rigid map → reduce pipeline | **Rich DAG**: joins, filters, aggregations |
| Typically 10–100× slower for ML workloads | **Up to 100× faster for iterative ML** (e.g. k-means) |

**Benchmark:** logistic regression (10 iterations, 100 GB). MapReduce takes **110 s/iter**, Spark **0.96 s/iter**.

## Key idea
- Work with **distributed collections as you would with local ones**.
- Concept: **Resilient Distributed Datasets (RDDs)**:
  - **Immutable** collections of objects spread across a cluster
  - Built through **parallel transformations** (map, filter, etc.)
  - **Automatically rebuilt on failure**
  - **Controllable persistence** (e.g. caching in RAM)

## Resilient Distributed Datasets (RDD): the fundamental abstraction
> An RDD is a **read-only, partitioned collection of records** distributed across a cluster, with a **lineage graph for fault recovery**.

| Property | Meaning |
|---|---|
| **Distributed** | Data is split into **partitions**. Each partition lives on one executor, and partitions are processed in parallel. |
| **Resilient** | On failure, lost partitions are **recomputed from the lineage graph**. No need for data replication. |
| **Immutable** | RDDs are never modified in place. Transformations produce new RDDs. This enables deterministic recomputation. |
| **Lazy** | Transformations build a logical plan, and execution happens only when an action is called. This lets the optimizer reorder steps. |

**Lineage** = the sequence of transformations that produced this RDD: *the recipe, not the data*.

## Languages
- APIs in **Java, Scala, and Python**
- Interactive shells in **Scala and Python** (Java developers: consider using Scala for the console to learn the API)
- Performance: Java/Scala will be faster (statically typed), but Python can do well for numerical work with NumPy

## How to run it
- **Local multicore:** just a library in your program
- **EC2:** scripts for launching a Spark cluster
- **Private cluster:** Mesos, YARN, Standalone Mode

## Spark architecture: Driver · Cluster Manager · Executors
```
Driver Program                       Cluster Manager                 Executor 1 / Executor 2
 SparkContext / SparkSession   ──requests resources──▶  (YARN / Mesos /   (JVM process on worker node)
 - Builds logical plan (DAG)                            Standalone / K8s)  - Runs Task threads
 - Schedules tasks on executors                                            - In-memory RDD storage
 - Collects results                                                        - Writes shuffle files
 - User code runs here                                                     Tasks: Task 1, Task 2, …
```
**Spark separates concerns:** the **Driver owns the logic**, the **Cluster Manager owns the resources**, and the **Executors own the computation**.

## Related
- [RDD API: transformations & actions](10-spark-rdd-api-transformations-actions.md)
- [DAG, stages, caching, lineage fault tolerance](11-spark-dag-stages-caching-fault-tolerance.md)
- [Spark execution model in depth (Lec04)](../lec04-spark-part2/02-spark-execution-model-lineage-dependencies.md)
