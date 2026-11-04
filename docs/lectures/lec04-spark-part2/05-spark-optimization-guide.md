# Spark Optimization Guide: UDFs, Caching, Gzip Trap, Schemas, Partitioning, Filters, Broadcast Joins, Profiling

> **Source:** `ComPar-Lec04.pdf`, slides 74–84 (printed numbers 75–85) · **Back to:** [index](../index.md)

## Optimization reasoning: who is responsible?
| Level | Meaning |
|---|---|
| **High programmer concern** | Must be done **manually** |
| **Shared concern** | **Programmer guided, Spark executed**: you set the strategy, and Spark does the heavy lifting |
| **Low programmer concern** | **Mostly automated** by Spark |

---

## HIGH programmer concern

### 1. Prefer built-in functions over user-defined functions (UDFs)
**Advantages of Spark's built-in functions (`pyspark.sql.functions`):**
- Fully optimized by the **Catalyst optimizer**.
- Can operate on **entire columns at once**, which performs better than row-by-row execution.
- Simpler, more readable, and maintainable code.

**Problems with UDFs:**
- Spark must **serialize** data, send it to the UDF, and then **deserialize** the result, which adds computation and I/O latency.
- **The Catalyst optimizer cannot analyze or optimize UDFs**, so you lose optimizations such as predicate pushdown and execution plan tuning.
- UDFs typically process data **row by row**, while built-in functions can use **vectorized operations** over entire columns.

### 2. Caching and persisting data
- Spark **re-runs the entire lineage on every action**, and caching avoids this.
  - Cache a DataFrame that is **accessed by 2+ actions** or is **expensive to recompute**.
  - Essential for **iterative algorithms** (PageRank, ML) and exploration.
- **When NOT to cache:**
  - **Single-use** DataFrames (caching adds overhead and wastes memory).
  - DataFrames **larger than the executor heap** (GC pressure / eviction).
- **Always `unpersist()`** when the data is no longer needed.

```python
movies = spark.read.csv('movies.csv', header=True)
movies.cache()   # or: movies.persist(StorageLevel.MEMORY_AND_DISK)  — lazy!
movies.count()   # triggers caching
```
- Use **`cache`** for small to medium datasets that easily fit into cluster memory.
- Use **`persist`** for large datasets, production environments, and complex data pipelines.
- Key storage levels: `MEMORY_ONLY` (fastest, may evict) · `MEMORY_AND_DISK` (spills to disk) · `MEMORY_AND_DISK_SER` (serialised).

### 3. The gzip partitioning trap (gzip is not splittable)
- You get **1–2 partitions even on a 10-node cluster**.
- One executor reads serially while the rest sit idle.
```python
df = spark.read.csv('data.csv.gz')
df.rdd.getNumPartitions()     # 1

# Fix: repartition after load
df = df.repartition(200)
```

### 4. File format performance: explicit schemas
`inferSchema` **scans the file twice**:
```python
df = spark.read.csv('data.csv', inferSchema=True)
```
An explicit schema needs only a **single scan**:
```python
from pyspark.sql.types import *
schema = StructType([...])
df = spark.read.schema(schema).csv('data.csv')
```

---

## SHARED concern

### Optimize data partitioning
- AQE can dynamically merge tiny partitions at runtime, **but**:
  - it cannot perfectly guess the ideal partition count for complex wide transformations,
  - nor can it fix heavily **skewed source files**.
  - Knowing when to use **`.coalesce()`** (to avoid shuffles) versus **`.repartition()`** remains a vital developer skill.
- **`coalesce()`**: use it to **reduce** the number of partitions, especially after filtering or other transformations that shrink the dataset. It **avoids a full shuffle**, which minimizes data movement.
- **`repartition()`**: performs a **full shuffle** to either increase or decrease the number of partitions. It is more expensive, but it ensures **evenly distributed** data, which matters for a balanced workload.
- **Guidelines:**
  - Start with **2–3 tasks per CPU core** in the cluster. For example, a 10-node cluster with 4 cores each should target **80–120 partitions**.
  - Aim for partitions of about **100–200 MB each**. Estimate the partition count as total dataset size / target size.
  - For wide transformations, consider **smaller partitions** to distribute the workload better.

---

## LOW programmer concern

### Minimize shuffle operations: apply filters early
- Filter out unnecessary data as soon as possible in the pipeline.
- Reducing the dataset size before expensive operations significantly decreases the amount of data shuffled.
```python
# Less efficient (filter after shuffle)
df_bad  = df.groupBy("department").agg(F.avg("salary")).filter(F.col("department") != "IT")
# More efficient (filter before shuffle)
df_good = df.filter(F.col("department") != "IT").groupBy("department").agg(F.avg("salary"))
```
- Writing the filter before a groupBy is good practice, but **Catalyst's predicate pushdown rearranges the code automatically**. Spark moves the filter to the earliest possible step regardless of where you wrote it in the DataFrame chain.

### Minimize shuffle operations: use broadcast joins
- If one DataFrame is **much smaller** than the other, use a **broadcast join**: Spark replicates the smaller DataFrame to all executor nodes, which **eliminates the shuffle**.
- **Default: Sort-Merge Join**, where both sides are shuffled (expensive for large tables).
- **Broadcast hint** (bypasses the threshold):
```python
from pyspark.sql.functions import broadcast
result = ratings.join(broadcast(movies), 'movieId')
result.explain()   # verify: BroadcastHashJoin or SortMergeJoin
```
- Historically developers had to apply the `broadcast()` hint manually. **Today AQE automatically detects** when a table drops below the broadcast threshold mid-query and converts the join strategy dynamically.
- The manual hint is still useful to **force** a broadcast and bypass Spark's default threshold constraints.

---

## Profiling with the Spark UI
The Spark UI is an excellent tool for identifying and analyzing shuffle operations.
- **Stage tab:** look at the **Shuffle Read** and **Shuffle Write** metrics. Large values mean heavy shuffling, which may signal optimization opportunities.
  - **Shuffle Write/Read**: data moved across the network
  - **Spill (Disk)**: the partition is too large for the heap
  - **GC Time > 10% of Task Time** indicates memory pressure
- **Storage tab:** **Fraction Cached < 1** means you should switch to `MEMORY_AND_DISK`.
- **Executors tab:** CPU vs wall-clock time reveals I/O wait.

## Programmatic profiling via the REST API
Tag jobs to correlate them with UI stages:
```python
sc = spark.sparkContext
sc.setJobGroup('join', 'baseline')
ratings.join(movies, 'movieId').count()   # triggers stages
```
Key fields in each stage record:
- `shuffleWriteBytes`, `shuffleReadBytes`
- `memoryBytesSpilled`, `diskBytesSpilled`
- `executorRunTime` vs `executorCpuTime`

## Related
- [Catalyst & AQE](04-catalyst-optimizer-explain-plans.md) · [PageRank co-partitioning](01-pagerank-example.md)
- [Caching & storage levels (Lec03)](../lec03-data-parallel-mapreduce-spark/11-spark-dag-stages-caching-fault-tolerance.md)
