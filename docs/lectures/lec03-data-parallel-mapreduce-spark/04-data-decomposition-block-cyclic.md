# Data Decomposition Strategies: Block, Cyclic, Block-Cyclic

> **Source:** `ComPar-Lec03.pdf`, slide 18 · **Back to:** [index](../index.md)

How to partition a dataset across **P** parallel workers.

Running example: `A = [a₀, a₁, a₂, a₃, a₄, a₅, a₆, a₇, a₈, a₉, a₁₀, a₁₁]` (N = 12 elements, P = 3 workers).

| | **Block** | **Cyclic** | **Block-Cyclic** |
|---|---|---|---|
| Rule | Worker *i* gets elements `[i·⌊N/P⌋, (i+1)·⌊N/P⌋)` | Worker *i* gets elements where `index mod P == i` | Block size B; assign blocks **round-robin** to workers |
| W0 | `[a₀, a₁, a₂, a₃]` | `[a₀, a₃, a₆, a₉]` | `[a₀, a₁, a₆, a₇]` (B = 2) |
| W1 | `[a₄, a₅, a₆, a₇]` | `[a₁, a₄, a₇, a₁₀]` | `[a₂, a₃, a₈, a₉]` |
| W2 | `[a₈, a₉, a₁₀, a₁₁]` | `[a₂, a₅, a₈, a₁₁]` | `[a₄, a₅, a₁₀, a₁₁]` |
| ✓ Pros | Good **spatial locality**; simple addressing | Better **load balance** for irregular data | **Balance + locality**; used in ScaLAPACK (https://www.netlib.org/scalapack/) |
| ✗ Cons | **Load imbalance** if the data is non-uniform | **Poor cache locality**; complex access pattern | More complex to implement |

**Spark uses block decomposition by default. The number of partitions controls granularity.**

## Formulas (for implementation)
- Block: `owner(k) = k / ⌈N/P⌉` (the slide uses ⌊N/P⌋ when P divides N)
- Cyclic: `owner(k) = k mod P`
- Block-cyclic with block size B: `owner(k) = (k / B) mod P`

## Related
- [Data (domain) decomposition, 2D/3D grids, ghost cells (Lec05)](../lec05-algorithm-design/02-decomposition-task-vs-data.md)
- [Spark partitioning guidelines (Lec04)](../lec04-spark-part2/05-spark-optimization-guide.md)
