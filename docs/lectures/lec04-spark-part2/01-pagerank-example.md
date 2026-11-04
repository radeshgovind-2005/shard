# PageRank on Spark: Algorithm, Implementation, Co-Partitioning Optimization

> **Source:** `ComPar-Lec04.pdf` (Lecture 04, "Distributed Data Parallel Processing (Part 2)"), slides 3–13 and 47–48 · **Back to:** [index](../index.md)

## Idea
Give pages ranks (scores) based on the links to them:
- Links from **many pages** give a high rank
- A link from a **high-rank page** gives a high rank

(Image: en.wikipedia.org/wiki/File:PageRank-hi-res-2.png)

## Formula (from Google's paper)
```
PR(A) = (1-d) + d · ( PR(T1)/C(T1) + ... + PR(Tn)/C(Tn) )
```
- "We assume page A has pages T1...Tn which point to it (i.e., are citations)."
- "The parameter **d** is a **damping factor** which can be set between 0 and 1. We usually set **d to 0.85**."
- "**C(A)** is defined as the number of links going out of page A."
- "The PageRanks form a probability distribution over web pages, so the sum of all web pages' PageRanks will be one."

## Simplified iterative algorithm (as used on the slides)
1. Start each page at a **rank of 1**.
2. On each iteration, have page *p* contribute **rank_p / |neighbors_p|** to its neighbors.
3. Set each page's rank to **0.15 + 0.85 × contribs**.

### Worked example (4-page graph from the slides)
Pages: **L**(eft), **T**(op), **R**(ight), **B**(ottom). Links: `L→T`, `T→R`, `R→T`, `R→B`, `B→L`, `B→T`.

| Page | Out-links | Iter 0 | Iter 1 | … | Final |
|---|---|---|---|---|---|
| T | R | 1.0 | 0.15+0.85·(1 + 0.5 + 0.5) = **1.85** | … | **1.44** |
| R | T, B | 1.0 | 0.15+0.85·1 = **1.0** | … | **1.37** |
| B | L, T | 1.0 | 0.15+0.85·0.5 = **0.58** | … | **0.73** |
| L | T | 1.0 | 0.15+0.85·0.5 = **0.58** | … | **0.46** |

Iteration 1 contributions: L sends 1 to T. T sends 1 to R. R sends 0.5 to T and 0.5 to B. B sends 0.5 to L and 0.5 to T. (Slide 10 shows an intermediate state with ranks such as 1.31, 1.72, 0.39, 0.58.)

## Implementation (PySpark)
```python
# Helper function to compute contributions for each neighbor
def compute_contribs(url_data):
    url, (neighbors, rank) = url_data
    for dest in neighbors:
        yield (dest, rank / len(neighbors))

# PageRank Iteration Loop
for i in range(ITERATIONS):
    # Join links and ranks to get (url, (neighbors, rank))
    contribs = links.join(ranks).flatMap(compute_contribs)
    # Sum up contributions and apply the damping factor formula
    ranks = contribs.reduceByKey(lambda x, y: x + y) \
                    .mapValues(lambda rank: 0.15 + 0.85 * rank)

# Save the results
ranks.saveAsTextFile(...)
```
- `links`: RDD of `(url, neighbors)`. `ranks`: RDD of `(url, rank)`.

## Optimizing placement
`links` and `ranks` are **repeatedly joined**:
```
Links (url, neighbors)   Ranks0 (url, rank)
            └──── join ────┘
                Contribs0 ── reduce ──▶ Ranks1
Links ──────────── join ───────────────┘
                Contribs1 ── reduce ──▶ Ranks2 . . .
```
**Is this a problem? Yes.** Each `join` is a wide dependency, so without a known partitioner both RDDs are shuffled across the network **on every iteration**, even though `links` never changes.

**Fix: co-partition them** (e.g. hash both on URL) to avoid shuffles:
```python
links = links.partitionBy(new URLPartitioner())   # slide pseudo-code (Scala-style)
# PySpark equivalent: links = links.partitionBy(numPartitions, partitionFunc).cache()
```
Once `links` is hash-partitioned (and cached), the join with `ranks` (whose keys are derived from `links`) needs no reshuffle of `links`.

## PageRank performance (time per iteration)
| System | Time/iteration |
|---|---|
| Hadoop | **171 s** |
| Basic Spark | **72 s** |
| Spark + Controlled Partitioning | **23 s** |

## Related
- [Spark execution model, narrow/wide dependencies](02-spark-execution-model-lineage-dependencies.md)
- [Pair RDD `join`, `reduceByKey`, `mapValues` (Lec03)](../lec03-data-parallel-mapreduce-spark/10-spark-rdd-api-transformations-actions.md)
- [Caching for iterative algorithms](05-spark-optimization-guide.md)
