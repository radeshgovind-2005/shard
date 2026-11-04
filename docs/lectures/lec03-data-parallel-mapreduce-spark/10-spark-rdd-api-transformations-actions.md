# Spark RDD API: Transformations vs Actions, Core Ops, Pair RDDs, Word Count

> **Source:** `ComPar-Lec03.pdf`, Part III ("Transformations vs Actions", "Core RDD Operations", "Pair RDD Operations", "Word Count: Spark RDD Walkthrough") · **Back to:** [index](../index.md)

## Transformations vs actions: lazy evaluation
**Nothing runs until an action is called.**

| Transformations (lazy): return a new RDD and build the DAG | | Actions (eager): trigger DAG execution and produce a result or output | |
|---|---|---|---|
| `map(f)` | Apply f to each element | `collect()` | Return all elements to the Driver |
| `filter(f)` | Keep elements where f returns True | `count()` | Return number of elements |
| `flatMap(f)` | map + flatten nested lists | `take(n)` | Return first n elements |
| `reduceByKey(f)` | Aggregate by key with f (pair RDD) | `first()` | Return first element |
| `groupByKey()` | Group values by key (**avoid if possible!**) | `reduce(f)` | Aggregate all elements with f |
| `join(other)` | Inner join on key (pair RDDs) | `saveAsTextFile(p)` | Write RDD to text files at path p |
| `sortByKey()` | Sort pairs by key | `foreach(f)` | Apply f to each element (side effect) |
| `distinct()` | Remove duplicates | `countByKey()` | Count values per key (pair RDD) |
| `union(other)` | Concatenate two RDDs | | |

**Key insight:** *transformations are recipes; actions are the oven.* Spark optimizes the whole recipe before baking.

## Core RDD operations (PySpark examples)
```python
# Element-wise transformations
nums    = sc.parallelize([1, 2, 3, 4, 5, 6])
squares = nums.map(lambda x: x*x)            # [1,4,9,16,25,36]
evens   = nums.filter(lambda x: x%2==0)      # [2, 4, 6]
lines   = sc.parallelize(["a b", "c d"])
words   = lines.flatMap(str.split)           # ["a","b","c","d"]

# Aggregation actions
rdd = sc.parallelize([3,1,4,1,5,9,2,6])
rdd.count()                    # 8
rdd.reduce(lambda a,b: a+b)    # 31
rdd.take(3)                    # [3, 1, 4]
rdd.collect()                  # caution: loads all to Driver!

# Set operations   (a=[1,2,3,4]  b=[3,4,5,6])
a.union(b).distinct()   # [1,2,3,4,5,6]
a.intersection(b)       # [3, 4]
a.subtract(b)           # [1, 2]
a.cartesian(b)          # 16 (key,val) pairs

# Statistics
nums = sc.parallelize([1.0,2.0,3.0,4.0,5.0])
nums.mean()              # 3.0
nums.stdev()             # 1.414
nums.max()               # 5.0
nums.sample(False, 0.3)  # random 30% sample
```

## Pair RDD operations: key-value RDDs, the workhorse of distributed data processing
Pair RDDs are RDDs of `(key, value)` tuples. Most grouped operations require a pair RDD.

```python
pairs = sc.parallelize([("a",1),("b",2),("a",3),("b",4)])

# reduceByKey: partial reduce per executor (efficient)
result = pairs.reduceByKey(lambda x,y: x+y)    # [("a", 4), ("b", 6)]

# groupByKey: sends ALL values to one node
grouped = pairs.groupByKey()                   # [("a", [1,3]), ("b", [2,4])]
# Avoid — use reduceByKey when possible!

names  = sc.parallelize([(1,"Alice"),(2,"Bob")])
scores = sc.parallelize([(1,95),(2,88),(4,72)])

joined = names.join(scores)          # inner join: [(1,("Alice",95)),(2,("Bob",88))]
names.leftOuterJoin(scores)          # keeps unmatched keys, e.g. [(1,("Alice",Some(95))),(2,...),(3,("Charlie",None))]
upper = names.mapValues(lambda n: n.upper())   # transform value, keep key: [(1,"ALICE"),(2,"BOB")]
```
**`join` triggers a shuffle**: both RDDs are repartitioned by key across the cluster.

## Word count: Spark RDD walkthrough (tracing the DAG step by step)
```python
from pyspark import SparkContext
sc = SparkContext("local[*]", "WordCount")

# Step 1: Read input → RDD[String] (one line per element)
rdd = sc.textFile("hdfs://data/books/*.txt")
# Step 2: Split each line into words → RDD[String]
words = rdd.flatMap(lambda line: line.split(" "))
# Step 3: Emit (word, 1) pairs → RDD[(String, Int)]
pairs = words.map(lambda word: (word, 1))
#                                                  ← shuffle boundary
# Step 4: Sum counts by key → RDD[(String, Int)]
counts = pairs.reduceByKey(lambda a, b: a + b)
#                                                  ← shuffle boundary
# Step 5: Sort by count descending
sorted_counts = counts.sortBy(lambda x: -x[1])
# ACTION: trigger execution and return top 10
result = sorted_counts.take(10)
print(result)
```
DAG (logical plan): `textFile() → flatMap() → map() ║ reduceByKey() ║ sortBy() → .take(10)` (ACTION, which executes!)

**Stages:** Stage 1 = textFile → flatMap → map (**narrow**). Stage 2 = reduceByKey (**wide/shuffle**). Stage 3 = sortBy (**wide**).

## Related
- [DAG execution, caching, log mining example, lineage](11-spark-dag-stages-caching-fault-tolerance.md)
- [MapReduce word count & combiner](07-mapreduce-example-shuffle-combiner.md)
- [groupByKey vs reduceByKey in depth (Lec04)](../lec04-spark-part2/02-spark-execution-model-lineage-dependencies.md)
