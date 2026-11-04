# The Map Pattern (Unary / N-ary) and Map Fusion

> **Source:** `ComPar-Lec03.pdf`, slides 9–14 · **Back to:** [index](../index.md)

## Map
- **Map** performs a function over **every element of a collection**.
- Map replicates a serial iteration pattern where:
  - each iteration is **independent** of the others,
  - the **number of iterations is known in advance**, and
  - computation **only depends on the iteration count and data from the input collection**.
- The replicated function is called the **"elemental function"**.

```
Input data  →  [elemental function applied to each element]  →  Output
```

## Serial vs parallel map
- Serial: the elemental function runs element after element.
- Parallel: the elemental function runs on many elements at once. The time saved is the **speedup**, because the parallel map finishes early while the serial map is still running.

## Unary map: 1 input, 1 output
Example with `f(x) = 2x`:
```
index   0  1  2  3  4  5  6  7  8  9 10 11
x       3  7  0  1  4  0  0  4  5  3  1  0
result  6 14  0  2  8  0  0  8 10  6  2  0
```

## N-ary map: N inputs, 1 output
Example with `f(x, y) = x + y`:
```
index   0  1  2  3  4  5  6  7  8  9 10 11
x       3  7  0  1  4  0  0  4  5  3  1  0
y       2  4  2  1  8  3  9  5  5  1  2  1
result  5 11  2  2 12  3  9  9 10  4  3  1
```

## Optimization: sequences of maps
- Often several map operations occur **in sequence**.
  - Vector math consists of many small operations, such as additions and multiplications, applied as maps.
- A **naïve implementation may write each intermediate result to memory**, which wastes memory bandwidth.

## Map fusion
- You can sometimes **"fuse"** the operations together and perform them at once.
- This **adds arithmetic intensity** and **reduces memory/cache usage**.
- Ideally, the operations can be performed **using registers alone**.

```
# unfused: tmp = map(f, x); out = map(g, tmp)   → tmp written to and read back from memory
# fused:   out = map(lambda v: g(f(v)), x)       → one pass, intermediates stay in registers
```

(Spark does the same thing automatically: narrow transformations are pipelined within a stage, and Tungsten whole-stage code generation fuses operators. See [Spark execution model](../lec04-spark-part2/02-spark-execution-model-lineage-dependencies.md).)

## Related
- [Data-parallel thinking](01-data-parallel-thinking.md) · [Reduction](03-reduction-pattern.md)
- [Spark `map`/`filter`/`flatMap`](10-spark-rdd-api-transformations-actions.md)
