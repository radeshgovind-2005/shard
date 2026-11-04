# The Reduction Pattern (Serial vs Parallel/Tree Reduction)

> **Source:** `ComPar-Lec03.pdf`, slides 15–17 · **Back to:** [index](../index.md)

## Definition
- **Reduction** combines every element in a collection using an **associative "combiner function"**.
- Because the combiner function is associative, **different orderings of the reduction are possible**.
- Examples of combiner functions: **addition, multiplication, maximum, minimum, Boolean AND, OR, XOR**.

## Are reductions embarrassingly parallel?
- **No.** The partial results must be combined, so there is communication/coordination.
- **But are reductions such as the one below parallelizable? Yes.**
```c
for (int i = 0; i < N; i++)
    s += a[i];
```

## Serial vs parallel reduction (example: sum of `1 2 5 4 9 7 0 1` = 29)

**Serial reduction:** 7 dependent steps, a linear chain.
```
1 → 1+2=3 → 3+5=8 → 8+4=12 → 12+9=21 → 21+7=28 → 28+0=28 → 28+1=29
step: 1      2       3        4         5         6         7
```

**Parallel (tree) reduction:** the same 7 operations, but only **3 levels** (log₂ 8).
```
level 0:   1   2   5   4   9   7   0   1
level 1:     3       9      16       1        (4 additions in parallel)
level 2:        12              17            (2 additions in parallel)
level 3:               29                     (1 addition)
```
Associativity is what lets us regroup `((((1+2)+5)+4)+…)` into `((1+2)+(5+4)) + ((9+7)+(0+1))`.

In work-span terms (see [Lec07](../lec07-performance/04-work-span-model-brents-lemma.md)), the work is n−1 operations and the span drops from n−1 to ⌈log₂ n⌉.

## In practice
- OpenMP: `#pragma omp parallel for reduction(+:sum)`. Each thread keeps a private partial sum, and the partial sums are combined at the end. See [OpenMP](../lec06-programming-models/02-shared-memory-model-pthreads-openmp.md).
- BSP vector sum: local sums, then send to W0. See [BSP](05-bsp-model.md).
- Spark: `rdd.reduce(f)`, `reduceByKey(f)`. See [RDD API](10-spark-rdd-api-transformations-actions.md).
- A **prefix scan** (`a[i] = a[i] + a[i-1]`) has a loop-carried true dependence that cannot simply be removed. It needs a different parallel algorithm. See [loop dependences](../lec05-algorithm-design/04-loop-dependences-and-removal.md).

## Related
- [Map pattern](02-map-pattern-and-fusion.md) · [MapReduce reduce primitive](06-mapreduce-model-and-architecture.md)
