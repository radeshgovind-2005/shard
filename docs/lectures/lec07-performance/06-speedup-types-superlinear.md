# Types of Speedup: Linear, Sublinear, Superlinear (and Its Causes)

> **Source:** `ComPar-Lec07.pdf`, slides 59–61 · **Back to:** [index](../index.md)

## Types of speedup
| Type | Condition | Notes |
|---|---|---|
| **Linear** | **S(p) = p** | Each processor does exactly 1/p of the work with zero overhead. **Rarely achieved**, but a useful upper bound for most problems. |
| **Sublinear** | **S(p) < p** | **Typical in practice.** Limited by the **sequential fraction (Amdahl's law)** and by **communication, synchronisation, and load imbalance**. |
| **Superlinear** | **S(p) > p** | Seems impossible, yet it occurs (see causes below). |

## Superlinear speedup: causes
**1. Cache / memory hierarchy effects**
- Sequential: the entire dataset may not fit in cache, causing frequent cache misses.
- Parallel: each processor handles a **smaller partition**, which **fits in L2/L3 cache**.
- The cache miss rate drops sharply, so memory bandwidth is no longer the bottleneck.
- The effect is strongest when the data size is **just above one cache but fits in p caches**.

**2. Search / branch-and-bound algorithms**
- Multiple processors explore **different branches** of the search space simultaneously.
- As soon as one finds a good bound, **all others can prune their subtrees earlier**.
- **Total nodes visited < sequential**, so the combined work is genuinely less.
- Examples: constraint satisfaction, backtracking SAT, game tree search.

**Rule of thumb:** superlinear speedup signals a **memory or algorithm effect, not a measurement error**. Investigate it to understand (and exploit) it.

## Related
- [Speedup/efficiency/cost definitions](01-performance-metrics-speedup-efficiency-cost.md)
- [Memory hierarchy numbers (Lec01)](../lec01-why-parallel/03-applications-and-hardware-landscape.md)
