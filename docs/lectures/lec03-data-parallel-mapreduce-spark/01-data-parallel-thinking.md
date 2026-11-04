# Data-Parallel Thinking & Embarrassingly Parallel Problems

> **Source:** `ComPar-Lec03.pdf` (Lecture 03, "Data-Parallel Thinking & Distributed Data Parallel Processing"), slides 2–8 · **Back to:** [index](../index.md)
> Bibliography: Chapters 4 and 5 of McCool, Robison, Reinders, *Structured Parallel Programming: Patterns for Efficient Computation*, Morgan Kaufmann (2012), ISBN 978-0-12-415993-8 (slides print "Arch M."). Also Bisseling, R. H., "The Bulk-Synchronous Parallel Model," in *Foundations of Computation and Machine Learning: The Work of Leslie Valiant*, 2026, pp. 493–540.

## Lecture 03 agenda
| Part | Topics |
|---|---|
| I. Data Parallelism & Decomposition | Embarrassingly parallel problems · Block / cyclic / block-cyclic · BSP model |
| II. MapReduce | Origins & primitives · Word Count walkthrough · Fault tolerance & limitations |
| III. Apache Spark & RDD API | Architecture overview · RDD transformations & actions · DAG execution & caching |
| IV. DataFrame & Spark SQL | Structured API & Catalyst · DataFrame operations · Case study: log analysis |

## Classic parallel thinking (worker-centric)
The classical strategy is to think in terms of **what each worker does** (addressed later in the course).

Example: pairwise addition of two vectors using N workers (e.g. threads):
1. Divide the vectors into N partitions of roughly the same size
2. Assign each partition to a worker
3. Each thread performs the computation over its partition
4. Assemble the result from the N partial results

## Data-parallel thinking (transformation-centric)
The focus shifts to thinking in terms of **data transformations**:
- Organise computation as **operations over sequences of data** (sequence = ordered collection of elements)
- Use sequence operations such as **Map**, **Filter**, **Reduce** (a.k.a. **fold**), and **Scan**
- These operations may have **optimized parallel implementations**
- Algorithms written with these primitives are often **elegant, efficient, and architecture independent**

## Data parallelism: definition & intuition
**Definition:** apply the **same function f to each element** of a large dataset **independently**, exploiting parallelism **across data items** rather than across program steps.

**Intuition (parallel salary raise):**
```python
# Sequential
for emp in employees:
    emp.salary *= 1.10

# Parallel (data-parallel)
employees.map(lambda e: e.salary * 1.10)
```

| Property | Meaning |
|---|---|
| **Independent** | No data dependency between items |
| **Scalable** | More workers give proportional speedup |
| **Uniform** | Same operation on every element |

## Embarrassingly parallel problems
> "No significant communication or synchronization required between parallel tasks." The parallel implementation is trivial, so the parallelism comes for free.

| Problem | Why it's embarrassingly parallel |
|---|---|
| **Image processing** | Apply a filter to each pixel independently; resize/convert millions of images |
| **Monte Carlo simulation** | Each random trial is independent (option pricing, π estimation) |
| **Web crawling** | Fetch each URL independently; parse HTML in parallel |
| **Parameter search** | Train a model with each hyperparameter setting independently |
| **Encryption** | Encrypt each block/file independently (ECB mode) |
| **Log processing** | Parse and classify each log line independently |

Contrast these with **tightly-coupled problems** (e.g. solving a linear system), where inter-task communication is unavoidable.

## Related
- [Map pattern & fusion](02-map-pattern-and-fusion.md) · [Reduction](03-reduction-pattern.md) · [Data decomposition](04-data-decomposition-block-cyclic.md) · [BSP](05-bsp-model.md)
- [Embarrassingly parallel in performance terms (Lec07)](../lec07-performance/01-performance-metrics-speedup-efficiency-cost.md)
