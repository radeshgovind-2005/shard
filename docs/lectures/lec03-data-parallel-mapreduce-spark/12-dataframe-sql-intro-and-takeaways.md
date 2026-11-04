# DataFrame & Spark SQL (Lecture 03 intro) + Lecture 03 Key Takeaways

> **Source:** `ComPar-Lec03.pdf`, Part IV "DataFrame & Spark SQL" + "Key Takeaways" · **Back to:** [index](../index.md)
> Lecture 04 **repeats and extends** this material. The full DataFrame/SQL content (RDD→DataFrame, operations, SQL, RDD vs DF table, log-analysis case study) is in [lec04/03-dataframe-api-spark-sql.md](../lec04-spark-part2/03-dataframe-api-spark-sql.md), and Catalyst/Tungsten is in [lec04/04-catalyst-optimizer-explain-plans.md](../lec04-spark-part2/04-catalyst-optimizer-explain-plans.md).
> Bibliography: Apache Spark Tutorial (tutorialspoint). Beginning of Part II of Chambers & Zaharia, *Spark: The Definitive Guide*, 2018.

Lecture 03 covered these DataFrame & Spark SQL slides, which are identical in Lecture 04:
- **From RDD to DataFrame**: RDD API (2012) → DataFrame API (Spark 1.3, 2015) → Dataset API (Spark 1.6, 2016)
- **DataFrame operations (PySpark)**: select, filter/where, groupBy+agg, join
- **Spark SQL**: `createOrReplaceTempView` + `spark.sql(...)` with window functions (`RANK() OVER (PARTITION BY … ORDER BY …)`)
- **RDD vs DataFrame: when to use each** (comparison table)
- **Catalyst Optimizer & Tungsten Engine** (pipeline: Unresolved Logical Plan → Resolved → Optimized → Physical Plan(s) → Selected Physical Plan → RDDs + Code Gen)
- **Case study: web server log analysis** (regexp_extract, top 5xx endpoints, p99 response size)

**Bottom line:** *Today, use DataFrames unless you need custom object types (Dataset in Scala) or unstructured binary data.* **Start with DataFrame, and fall back to RDD only when DataFrame cannot express what you need.**

## Lecture 03 key takeaways
| Topic | Takeaway |
|---|---|
| **Data Parallelism** | Same op on each element. Embarrassingly parallel means no coordination needed. |
| **BSP Model** | Supersteps: local compute → communicate → barrier sync. Cost = **W + g·h + l**. |
| **MapReduce** | Map (parallel) + Shuffle (**expensive!**) + Reduce. Disk I/O kills iterative workloads. |
| **Spark RDD** | Lazy DAG, immutable, fault-tolerant lineage; `cache()` for iterative algorithms. |
| **DataFrame & SQL** | Concise declarative API. |
| **Choose wisely** | Use **DataFrame** for structured data (SQL, ETL, ML features); **RDD** for unstructured/custom types. |
