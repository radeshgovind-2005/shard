# Spark Execution Model: Driver/Executors, Lineage, RDD Representation, Narrow vs Wide Dependencies, Stages & Tasks

> **Source:** `ComPar-Lec04.pdf`, slides 2 and 14–45 · **Back to:** [index](../index.md)

## Spark execution model
- The **Driver program** connects to a **cluster manager** to allocate resources.
- It acquires **Executors** (workers) on cluster nodes, which run parallel tasks and cache data.
- It **sends application code** to the workers.
- It **sends tasks** for the executors to run. Results come back to the driver.
- RDDs are evaluated across the executors **in partitions**.
- **Each worker can have multiple partitions, but a partition cannot be spread across multiple workers.**

## RDD lineage
- There are two types of operations on RDDs:
  - **Transformations** (lazy, not computed immediately)
  - **Actions** (return a value to the app, or to storage)
- A **transformed RDD is recomputed** each time an action runs on it…
- …**but it can be persisted** into memory or disk.

```scala
1. lines  = spark.textFile("hdfs://..")
2. errors = lines.filter(_.startsWith("ERROR"))
3. errors.persist()
4. errors.count()
5. errors.filter(_.contains("HDFS"))
         .map(_.split('\t')(3))
         .collect()
```
- Lineage is based on **coarse-grained transformations** (one operation applied to the whole dataset, not fine-grained per-record updates).
- **Lineage helps avoid the overhead of checkpointing RDDs.**

## How RDDs are represented
- Lineage must be tracked across a wide range of transformations.
- A **graph-based representation**.
- **A common interface with:**
  1. a **set of partitions**: atomic pieces of the dataset
  2. a **set of dependencies on parent RDDs**
  3. a **function for computing the dataset** based on its parents
  4. **metadata** about its partitioning scheme and data placement

## RDD dependencies
### Narrow dependencies
Each partition of the parent RDD is used by **at most one** partition of the child RDD.
- Good for **pipelined execution on one cluster node**, e.g. apply a map followed by a filter element by element.
- **Efficient recovery**, as only the lost parent partitions need to be recomputed (in parallel on different nodes).
- Examples: `map`, `filter`, `flatMap`, `union`, join with co-partitioned inputs.

### Wide dependencies
Each partition of the parent RDD is used by **multiple** child partitions.
- Requires data from all parent partitions to be available and **shuffled across the nodes** using a MapReduce-like operation.
- **Expensive recovery**, as a complete re-execution of all parent partitions is needed.
- Examples: `groupByKey`, `reduceByKey`, join with inputs not co-partitioned.

(Slide 23 has the narrow vs wide diagram, adapted from *High Performance Spark* by Holden Karau & Rachel Warren.)

**Key facts:**
- Narrow dependencies **do not require data to be moved across partitions**, and narrow transformations don't require communication with the driver node.
- **Each series of narrow transformations can be computed in the same "stage"** of the query execution plan.

## How Spark works
- RDD: a parallel collection with partitions
- The user application creates RDDs, transforms them, and runs actions
- These result in a **DAG of operators**
- The **DAG is compiled into stages**
- **Each stage is executed as a series of tasks**

## Example: from code to execution graph
```scala
sc.textFile("/some-hdfs-data")                    // RDD[String]
  .map(line => line.split("\t"))                  // RDD[List[String]]
  .map(parts => (parts[0], int(parts[1])))        // RDD[(String, Int)]
  .reduceByKey(_ + _, 3)                          // RDD[(String, Int)]  (3 partitions)
  .collect()                                      // List[(String, Int)]
```
Operators: `textFile → map → map → reduceByKey → collect`

| Stage 1 | Stage 2 |
|---|---|
| read HDFS split | read shuffle data |
| apply both maps | final reduce |
| **partial reduce** | send result to driver |
| write shuffle data | |

## Stage execution
- **Create a task for each partition** in the new RDD
- **Serialize** the task
- **Schedule and ship** tasks to the workers (slides say "slaves")

## Task execution
The **task is the fundamental unit of execution** in Spark:
1. **Fetch input** from an InputFormat or a shuffle
2. **Execute** the task
3. **Materialize task output** as shuffle data or a driver result

These steps run as **pipelined execution**.

## Spark executor
Each executor has several cores. **Each core runs one task at a time** (fetch input → execute → write output), and runs tasks back to back, so the executor processes multiple tasks concurrently.

## Summary of components
| Component | Definition |
|---|---|
| **Task** | Fundamental unit of work |
| **Stage** | Set of tasks that run in parallel |
| **DAG** | Logical graph of RDD operations |
| **RDD** | Parallel dataset with partitions |

## groupByKey vs reduceByKey (why gain a deeper understanding?)
Data: `(patrick, $24), (matei, $30), (patrick, $1), (aaron, $23), (aaron, $2), (reynold, $10), (aaron, $10) …`
```python
# Copies ALL data over the network
spendPerUser = rdd.groupByKey() \
                  .map(lambda pair: sum(pair[1])) \
                  .collect()

# Reduces locally before shuffling (map-side combine, like a MapReduce combiner)
spendPerUser = rdd.reduceByKey(lambda x, y: x + y) \
                  .collect()
```

## Related
- [PageRank co-partitioning](01-pagerank-example.md)
- [Lec03 DAG execution & caching](../lec03-data-parallel-mapreduce-spark/11-spark-dag-stages-caching-fault-tolerance.md)
- [MapReduce combiner](../lec03-data-parallel-mapreduce-spark/07-mapreduce-example-shuffle-combiner.md)
