# Catalyst Optimizer: Pipeline, Code Generation, Reading `explain()` Plans, Tungsten, AQE

> **Source:** `ComPar-Lec04.pdf`, slides 58–72 · **Back to:** [index](../index.md)
> Reference: "Deep Dive into Spark SQL's Catalyst Optimizer", https://www.databricks.com/blog/2015/04/13/deep-dive-into-spark-sqls-catalyst-optimizer.html

## The Catalyst pipeline (why DataFrames outperform hand-tuned RDD code)
```
Unresolved Logical Plan ──▶ Resolved Logical Plan ──▶ Optimized Logical Plan ──▶ Physical Plan(s) ──▶ Selected Physical Plan ──▶ RDDs + Code Gen
   parse SQL or              resolve column             pushdown, prune,          enumerate physical     cost-based              Tungsten: generate
   DataFrame API             types & names              constant fold             implementations        selection               bytecode
```
Phases: **Analysis → Logical Optimization → Physical Planning → Code Generation**.

## Code generation example
Consider an expression tree with nodes `Add`, `Attribute`, `Literal`, e.g. `(x + y) + 1`.
- **Without code generation:**
  - Each row of data must be processed by **interpreting the tree**,
  - which requires walking through the nodes (Add, Attribute, Literal).
  - This produces many **branches and virtual function calls**, which slow execution.
- **Catalyst transforms the tree into Scala code**, e.g. `result = x + y + 1`,
  - which is then **compiled into Java bytecode**.
  - At runtime, the compiled code is executed directly by the JVM, with no tree walking and no virtual calls, just straight-line arithmetic.
  - **Result: far fewer overheads and much faster execution.**

## DataFrame example used to read `explain()`
```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("test").master("local").getOrCreate()
srcDF = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/home/jovyan/sales_src.csv")

unitsBy2      = srcDF.withColumn("Units Sold", F.col("Units Sold") * 2)  # transformation
filterOrderId = unitsBy2.filter(F.col("Order Id") > 100)                 # transformation
select        = filterOrderId.select(F.col("Region"))                    # transformation
select.take(10)                                                          # action
select.explain(extended=True)   # "spark, please tell me what you did under the hood"
```

### Full `explain(extended=True)` output
```
== Parsed Logical Plan ==
'Project [unresolvedalias('Region, None)]
+- AnalysisBarrier
  +- Filter (Order Id#16 > 100)
   +- Project [Region#10, Country#11, Item Type#12, Sales Channel#13, Order Priority#14, Order Date#15, Order ID#16, Ship Date#17, (Units Sold#18 * 2) AS Units Sold#38, Unit Price#19, Unit Cost#20, Total Revenue#21, Total Cost#22, Total Profit#23]
      +- Relation[Region#10,Country#11,Item Type#12,Sales Channel#13,Order Priority#14,Order Date#15,Order ID#16,Ship Date#17,Units Sold#18,Unit Price#19,Unit Cost#20,Total Revenue#21,Total Cost#22,Total Profit#23] csv
== Analyzed Logical Plan ==
Region: string
Project [Region#10]
+- Filter (Order Id#16 > 100)
 +- Project [Region#10, ..., (Units Sold#18 * 2) AS Units Sold#38, ...]
   +- Relation[Region#10, ..., Total Profit#23] csv
== Optimized Logical Plan ==
Project [Region#10]
+- Filter (isnotnull(Order Id#16) && (Order Id#16 > 100))
 +- Relation[Region#10, ..., Total Profit#23] csv
== Physical Plan ==
*(1) Project [Region#10]
+- *(1) Filter (isnotnull(Order Id#16) && (Order Id#16 > 100))
  +- *(1) FileScan csv [Region#10,Order ID#16] Batched: false, Format: CSV, Location: InMemoryFileIndex[file:/home/jovyan/sales_src.csv], PartitionFilters: [], PushedFilters: [IsNotNull(Order ID), GreaterThan(Order ID,100)], ReadSchema: struct<Region:string,Order ID:int>
```

## Phase 1: Analysis
- Spark SQL begins with a **relation to be computed**, which can come from:
  - an **abstract syntax tree (AST)** produced by a SQL parser, or
  - a **DataFrame object** built via the API.
- **Unresolved references:** at this stage the relation may contain unresolved attributes or unresolved relations.
  - Example: `SELECT col FROM sales`. We don't yet know whether `col` is valid or what its type is until we look up the table `sales`.
  - An attribute is **unresolved** if its **type is unknown** or it has **not been matched to an input table (or alias)**.
- Spark SQL uses **Catalyst rules** and a **Catalog** object (which tracks tables in all data sources) to resolve these attributes.
- The **unresolved logical plan** tree initially contains unbound attributes and unspecified data types.
- **Resolution rules:**
  - Look up relations by name from the catalog.
  - Map named attributes (e.g. `col`) to inputs provided by the operator's children.
  - **Disambiguate** attributes by giving them **unique IDs** when they refer to the same value (enabling optimizations like simplifying `col = col`).
  - **Propagate and coerce types** through expressions. Example: the return type of `1 + col` can't be known until `col` is resolved, and if needed its subexpressions are cast to compatible types.

**Reading the Parsed Logical Plan:** this is the raw syntax-check stage. Spark builds an AST strictly in the order you wrote the code. `'Region` appears as an **unresolved alias**: Spark doesn't yet know whether a column "Region" exists in the CSV or what type it has. It just maps your logic: Read → Multiply Units Sold → Filter Order Id → Select Region.

**Reading the Analyzed Logical Plan:** this is the semantic validation stage. Spark looks up its internal Catalog (schema metadata) to verify column names and data types.
- **Resolution:** `unresolvedalias('Region)` becomes `Project [Region#10]`. **`#10` is a unique internal ID** for that column.
- **Type assignment:** it infers that Region is a `string` and Order Id is an integer (`Order Id#16`), which confirms the code is semantically valid.

## Phase 2: Logical optimizations
- Applies **rule-based optimizations** to the logical plan.
- **Cost-based optimization (CBO)** generates multiple plans using rules, then computes and compares their costs.
- Examples of rule-based optimizations:
  - **Constant folding**: evaluate constant expressions at compile time.
  - **Predicate pushdown**: move filters closer to the data source.
  - **Projection pruning**: remove unused columns.
  - **Null propagation**: simplify expressions involving null values.
  - **Boolean expression simplification**: remove redundant or impossible conditions (e.g. `x > 10 AND x > 20` becomes `x > 20`).
  - **Join reordering**: rearrange joins to minimize data shuffling.

**Reading the Optimized Logical Plan** ("this is where the magic happens"):
- **Projection pruning:** the `withColumn("Units Sold" * 2)` step has **completely vanished**. The final line only requests `Region`, so computing Units Sold would waste CPU, and Spark drops that operation entirely.
- **Null filtering:** Spark automatically adds `isnotnull(Order Id#16)` to the filter condition, to optimize data scanning before applying `Order Id > 100`.

## Phase 3: Physical planning
- Spark SQL takes the logical plan and generates **one or more physical plans**.
- A **cost model** selects the best plan:
  - It considers factors like data size, partition count, and cluster resources.
  - It selects the plan with the lowest estimated cost (CPU, memory, I/O).
  - Currently, **CBO is applied mainly to join algorithms**. If a relation is known to be small, Spark SQL uses a **broadcast join**, which uses Spark's peer-to-peer broadcast facility.
- **Rule-based physical optimizations:**
  - **Pipelining projections and filters into a single Spark map operation.**
  - **Pushing down** operations (predicates or projections) into data sources that support them.
- **Extensibility:** Spark SQL supports pushing logic into data sources that can handle predicate or projection pushdown.

**Reading the Physical Plan** (the final executable blueprint; read bottom to top):
- `FileScan csv`: reads the CSV. **`PushedFilters: [IsNotNull(Order ID), GreaterThan(Order ID,100)]`** means the filter is pushed down to the file-reader level, so rows that don't match are skipped right away instead of reading everything into memory and filtering afterwards.
- **`ReadSchema: struct<Region:string,Order ID:int>`**: only 2 columns are read. Even though the CSV has more than 10 columns, Spark drops the rest during the file read to minimize memory.
- `Filter`: validates the filtered rows.
- **`*(1) Project [Region#10]`**: the **asterisk `*` indicates Whole-Stage Code Generation**. Spark fuses these steps into a single, highly optimized Java function instead of passing data between separate functions. `(1)` is the codegen stage id.

## Catalyst optimizations & Tungsten execution engine (summary)
| Catalyst optimizations | Tungsten execution engine |
|---|---|
| **Predicate pushdown**: filter rows as early as possible | **Off-heap memory management**: avoids JVM GC pauses |
| **Column pruning**: read only needed columns from Parquet/ORC | **Cache-aware data structures**: exploit CPU cache lines |
| **Constant folding**: evaluate constant expressions at compile time | **Whole-stage code generation**: compiles operators into a single tight loop (no per-row virtual dispatch) |
| **Join reordering**: put the smaller table on the build side | **Vectorized readers**: read Parquet in columnar batches |
| **Partial aggregation**: push groupBy work to each partition | |

## Adaptive Query Execution (AQE)
AQE **re-optimises at runtime using actual statistics**. It is the default since **Spark 3.2** (`spark.sql.adaptive.enabled`).
1. **Coalesce small shuffle partitions**: tiny post-shuffle partitions are merged automatically.
2. **Dynamic join strategy switching**: `SortMergeJoin → BroadcastHashJoin` if one side fits.
3. **Skew join handling**: partitions larger than **5× the median** are split and replicated.

## Related
- [DataFrame API & Spark SQL](03-dataframe-api-spark-sql.md)
- [Optimization guide (what you vs Spark must do)](05-spark-optimization-guide.md)
- [Map fusion (same idea as whole-stage codegen)](../lec03-data-parallel-mapreduce-spark/02-map-pattern-and-fusion.md)
