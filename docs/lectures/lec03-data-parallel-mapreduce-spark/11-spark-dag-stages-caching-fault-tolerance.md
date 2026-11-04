# Spark: Log-Mining Example, RDD Fault Tolerance (Lineage), DAG Execution, Caching & Persistence

> **Source:** `ComPar-Lec03.pdf`, Part III (printed slides 89–92, "DAG Execution Model", "Caching & Persistence") · **Back to:** [index](../index.md)

## Example: mining console logs
Load error messages from a log into memory, then search them interactively for patterns.

```python
lines    = spark.textFile("hdfs://...")                       # Base RDD
errors   = lines.filter(lambda s: s.startswith("ERROR"))      # Transformed RDD
messages = errors.map(lambda s: s.split('\t')[2])
messages.cache()

messages.filter(lambda s: "foo" in s).count()   # Action #1
messages.filter(lambda s: "bar" in s).count()   # Action #2
. . .
```

**What happens (step by step, as shown in the animation):**
1. Defining `lines`, `errors`, and `messages` creates RDDs on the **Driver** only. Nothing executes (lazy).
2. The first `count()` is an action: the **Driver ships tasks** to Workers, each holding one **HDFS block** (Block 1, 2, 3).
3. Each Worker **reads its block from HDFS**, applies filter → map, and because of `cache()` stores the result in memory (**Cache 1, 2, 3**). Results are sent back to the Driver.
4. The second `count()` ("bar") ships new tasks, but now the Workers **read from their cache**, with **no HDFS read**. This makes interactive queries fast.

## RDD fault tolerance
RDDs **track the transformations used to build them (their lineage)** so they can recompute lost data.
```python
messages = textFile(...).filter(lambda s: s.contains("ERROR")) \
                        .map(lambda s: s.split('\t')[2])
```
Lineage chain: `HadoopRDD (path = hdfs://…) → FilteredRDD (func = contains(...)) → MappedRDD (func = split(…))`

### Fault recovery test (iteration time in seconds)
| Iteration | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 |
|---|---|---|---|---|---|---|---|---|---|---|
| Time (s) | 119 | 57 | 56 | 58 | 58 | **81** (failure happens) | 57 | 59 | 57 | 59 |

- Iteration 1 is slow because it loads the data and builds the cache.
- At iteration 6 a failure happens. Only the lost partitions are recomputed from lineage, so the iteration takes 81 s instead of restarting the job, and the following iterations return to ~57 s.

## DAG execution model: Spark's physical execution plan
When an action is called, Spark's **DAGScheduler** converts the RDD lineage graph into **Stages** and **Tasks**.

**Narrow dependencies**
- Each parent partition maps to **at most one** child partition.
- **No data movement** across the network.
- `map()`, `filter()`, `flatMap()` are narrow.

**Wide dependencies (shuffle boundary)**
- A child partition depends on **multiple** parent partitions.
- Requires **data movement across all executors**.
- `reduceByKey()`, `join()`, `groupByKey()` are wide.
- **Spark creates a new Stage at each shuffle boundary.**

**Stages & Tasks**
- Each Stage = a set of narrow-dependency RDDs.
- **Tasks = one per partition per stage.**
- Stage 1 tasks run in parallel (no shuffling needed).
- **Stage 2 starts only after Stage 1 is complete.**

Word-count example: Stage 1 (narrow deps) = `textFile() RDD[String] → flatMap() RDD[String] → map() RDD[(String,Int)]`. Stage 2 (wide dep/shuffle) = `reduceByKey() RDD[(String,Int)]`.

## Caching & persistence: the key to Spark's advantage over MapReduce for iterative workloads
**Without caching, every action recomputes the entire lineage from the source files**, including expensive transformations.

```python
from pyspark import StorageLevel

# cache(): default — MEMORY_ONLY
frequent_rdd = bigRDD.filter(lambda x: x['active']).cache()
frequent_rdd.count()            # triggers computation + caches
frequent_rdd.map(f1).collect()  # reuses cache — fast!
frequent_rdd.map(f2).collect()  # reuses cache — fast!

# persist() with explicit storage level
rdd.persist(StorageLevel.MEMORY_AND_DISK)   # spill to disk if RAM full
rdd.persist(StorageLevel.MEMORY_ONLY_2)     # 2× replicated in RAM
rdd.persist(StorageLevel.DISK_ONLY)         # disk only

# unpersist when done to free memory
frequent_rdd.unpersist()
```

| Storage level | Notes |
|---|---|
| `MEMORY_ONLY` | Fast. May recompute if there is not enough RAM. (default for `cache()`) |
| `MEMORY_AND_DISK` | Spills to disk. More reliable. |
| `DISK_ONLY` | Slowest. Use only for very large RDDs. |
| `OFF_HEAP` | Avoids GC pressure. Experimental. |

**Rule of thumb:** `cache()` any RDD that is **used more than once** in your program, especially in iterative loops. For k-means over 10 iterations, caching the data RDD avoids 10 re-reads from HDFS.

## Related
- [Spark execution model, narrow/wide in depth, stages/tasks (Lec04)](../lec04-spark-part2/02-spark-execution-model-lineage-dependencies.md)
- [When (not) to cache, cache vs persist (Lec04)](../lec04-spark-part2/05-spark-optimization-guide.md)
- [MapReduce fault tolerance (contrast)](08-mapreduce-fault-tolerance-limitations.md)
