# DataFrame API & Spark SQL: RDD→DataFrame, Operations, SQL, RDD vs DF, Log-Analysis Case Study

> **Source:** `ComPar-Lec04.pdf`, slides 49–57 and 73 (also present in `ComPar-Lec03.pdf` Part IV) · **Back to:** [index](../index.md)
> Bibliography: Apache Spark Tutorial (tutorialspoint) · Beginning of Part II of Chambers & Zaharia, *Spark: The Definitive Guide*, 2018 · https://www.edureka.co/blog/spark-sql-tutorial/ · https://www.tutorialspoint.com/spark_sql/spark_sql_introduction.htm · Paper: *Spark SQL: Relational Data Processing in Spark* (https://dl.acm.org/doi/pdf/10.1145/2723372.2742797)

*Structured data processing with schema awareness and automatic optimization.*

## From RDD to DataFrame: why structured data deserves a structured API
Timeline: **RDD API (2012)** → **DataFrame API (Spark 1.3, 2015)** → **Dataset API (Spark 1.6, 2016)**

| | **RDD** | **DataFrame** |
|---|---|---|
| Advantages | Full control over data · Works with any object type · No schema required · Good for unstructured data | Structured schema (Spark SQL types) · **Catalyst query optimizer** · **Tungsten code generation** · Concise SQL-like API |
| Limitations | No automatic optimization · No predicate pushdown · Verbose API for SQL-like ops · Difficult to debug | Type safety only at runtime · Less control than RDD · Schema inference can be slow |

**Today: use DataFrames unless you need custom object types (Dataset in Scala) or unstructured binary data.**

## Programming interface
- **Seamlessly mix SQL queries with Spark programs**, using either SQL or the DataFrame API.
- Apply functions to the results of SQL queries, e.g.
  ```python
  results = spark.sql("SELECT * FROM people")
  names = results.map(lambda p: p.name)
  ```
- Connect to any data source the same way.
- Run SQL or HiveQL queries on existing warehouses.

## DataFrame API
- A DataFrame (DF) is conceptually equivalent to a **table in a relational database** or a data frame in R/Python.
- Supports **all the RDD operations**, but these may return an RDD rather than a DF.
- Scales from kB of data on a single laptop to petabytes on a large cluster.
- Supports a wide array of data formats and storage systems.
- Supports user-defined types.
- State-of-the-art optimization and code generation through the **Spark SQL Catalyst optimizer**.

### Transformations vs actions
| Transformations (return a DataFrame) | Actions (return a "result") |
|---|---|
| `filter`, `select`, `join`, `intersect`, `drop`, `groupBy` | `count`, `collect`, `show`, `take`, `head` |

## DataFrame operations: PySpark
```python
from pyspark.sql.functions import col, avg, count, desc

df = spark.read.csv("employees.csv", header=True, inferSchema=True)
# schema: name(string), dept(string), salary(double)

# Select specific columns
df.select("name", "salary").show(5)

# Filter rows — two equivalent styles
df.filter(col("salary") > 50000).show()
df.where("dept = 'Engineering'").show()

# Group and aggregate
df.groupBy("dept") \
  .agg(
      avg("salary").alias("avg_salary"),
      count("*").alias("headcount")
  ) \
  .orderBy(desc("avg_salary")) \
  .show()

# Join two DataFrames
dept_df = spark.read.csv("departments.csv", header=True)
joined = df.join(dept_df, on="dept_id", how="inner")
joined.select("name", "dept_name", "salary").show()
```

## Spark SQL: SQL queries over DataFrames
Full ANSI SQL on distributed data, with the same optimizer and the same performance.
```python
spark = SparkSession.builder \
    .appName("SparkSQL") \
    .getOrCreate()

emp  = spark.read.parquet("hdfs://data/employees/")
dept = spark.read.parquet("hdfs://data/departments/")

# Register as temporary SQL views
emp.createOrReplaceTempView("employees")
dept.createOrReplaceTempView("departments")

# SQL and DataFrame API use the same Catalyst optimizer pipeline
result = spark.sql("""
    SELECT e.name, d.dept_name, e.salary,
           RANK() OVER (
             PARTITION BY d.dept_name
             ORDER BY e.salary DESC
           ) AS dept_rank
    FROM employees e
    JOIN departments d
      ON e.dept_id = d.dept_id
    WHERE e.salary > 60000
    LIMIT 20
""")
result.show()
result.write.parquet("output/")
```
**SQL and the DataFrame API are equivalent**: Spark compiles both through Catalyst into the same physical plan.

## RDD vs DataFrame: when to use each
| Dimension | RDD API | DataFrame API |
|---|---|---|
| Abstraction | Distributed collection of any JVM/Python objects | Distributed table with named, typed columns |
| Schema | No schema; type checked only at runtime | Explicit schema, validated at query planning time |
| Optimization | None; you write the physical plan yourself | Catalyst optimizer + Tungsten code gen applied automatically |
| API style | Functional: map, filter, reduce, join… | Declarative: select, where, groupBy, SQL strings |
| Performance | Slower for structured data (no optimization) | Often **10–100× faster** due to Catalyst + columnar I/O |
| Serialization | JVM/Python serialization (slow, large) | Tungsten binary format (compact, CPU-cache friendly) |
| Best for | Unstructured data, custom types, binary formats, fine-grained control | Structured/semi-structured data, SQL analytics, ETL pipelines |

**Recommendation: start with DataFrame, and fall back to RDD only when DataFrame cannot express what you need.**

## Case study: web server log analysis
Task: parse Apache access logs. Which endpoints return the most 5xx errors? What is the 99th-percentile response size?
```python
from pyspark.sql.functions import (
    regexp_extract, col, count, percentile_approx, desc
)
raw = spark.read.text("hdfs://logs/access/*.log")
PAT = (r'(\S+) \S+ \S+ \[.*?\]'
       r' "(\S+) (\S+).*?" (\d{3}) (\d+|-)')
logs = raw.select(
    regexp_extract("value", PAT, 3).alias("endpoint"),
    regexp_extract("value", PAT, 4).cast("int").alias("status"),
    regexp_extract("value", PAT, 5).cast("int").alias("bytes"),
)

# Q1: Top 5xx error endpoints
logs.filter(col("status") >= 500) \
    .groupBy("endpoint") \
    .agg(count("*").alias("errors")) \
    .orderBy(desc("errors")) \
    .show(10)

# Q2: 99th-pctile response size
logs.agg(
    percentile_approx("bytes", 0.99).alias("p99_bytes")
).show()

# Q3: cache for multiple queries
logs.cache()
```

## Related
- [Catalyst optimizer, explain() plans, Tungsten, AQE](04-catalyst-optimizer-explain-plans.md)
- [Optimization guide (UDFs, caching, partitioning, broadcast joins)](05-spark-optimization-guide.md)
