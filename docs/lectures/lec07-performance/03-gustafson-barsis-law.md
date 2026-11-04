# Gustafson-Barsis' Law (Scaled Speedup / Weak Scaling) and Amdahl vs Gustafson

> **Source:** `ComPar-Lec07.pdf`, slides 20–30 · **Back to:** [index](../index.md)

> "…speedup should be measured by **scaling the problem to the number of processors**, not by fixing the problem size." (John Gustafson)

## Motivation
- We are often interested in **larger problems** when scaling:
  - How big a problem can be run (e.g. **HPC Linpack**)
  - **Constrain the problem size by the parallel time** (fixed wall-clock time)
- As processors (and the problem) grow, the **serial share of the accumulated execution time shrinks**:

| Processors | Serial work as a share of accumulated execution time (slide example) |
|---|---|
| T₁ | ≈ 16% |
| T₂ | ≈ 9% |
| T₄ | ≈ 5% |
| T₈ | ≈ 3% |

## Derivation
- Execution time of a parallel program: **T₁ = a + b**
  - **a**: the part that is **not parallelizable**
  - **b**: the part that is **parallelizable**
- Because we scale the problem (data being processed), with **P** processors the equivalent sequential work is:
  ```
  TP = a + P·b
  ```
- The **wall-clock execution time is always the same**, so scaled speedup is computed from the **size of the problem processed** (proportional to the total/accumulated execution time):
  ```
  Sp ≤ TP / T1 = (a + P·b) / (a + b)
  ```
- Let **α = a / (a + b)** be the **sequential fraction of the parallel execution time**. Then:
  ```
  Sp ≤ α + P·(1 − α) = P − α·(P − 1)
  ```
- **If α → 0 then Sₚ → P.**

## Worked example
**An application executing on 64 processors spends 5% of the total time on non-parallelizable computations. What is the scaled speedup?**
```
S(64) ≤ P − α·(P − 1)
      ≤ 64 − 0.05·(64 − 1)
      ≤ 60.85
```
(Slide 28 shows a chart with the values **57.7** and **64**. 57.7 matches α = 0.1: 64 − 0.1·63 = 57.7. 64 is the ideal linear speedup.)

## Gustafson's law and scalability
- **When does Gustafson's law apply?**
  - When the **problem size can increase as the number of processors increases**
  - The **speedup function includes the number of processors!**
  - It can **maintain or increase parallel efficiency** as the problem scales (**weak scaling**)

## Amdahl vs Gustafson-Barsis
| | **Amdahl** | **Gustafson-Barsis** |
|---|---|---|
| Problem size | Fixed | Grows with P |
| What's held constant | Total work | Wall-clock (parallel) time |
| Time measured | **Wall-clock time** | **CPU time** (accumulated work) |
| Serial part | **Tends to dominate** the computation | **Tends to become irrelevant** |
| Scalability | **Upper bound on scalability** (1/f) | **No upper bound on scalability** |
| Speedup formula | Sₚ ≤ 1 / (f + (1−f)/p) | Sₚ ≤ P − α(P−1) |
| Scaling type | Strong scaling | Weak scaling |

Diagram (slide 30): in Amdahl, the serial block stays the same while the parallel block is split across P = 1, 2, 4, 8, so Tₚ shrinks toward the serial time. In Gustafson-Barsis, each processor gets a full-size parallel block, so Tₚ stays constant and the total work grows with P.

## But… (slide 31)
- **Can all applications be decomposed simply into a serial part and a parallel part?** For my particular application, what speedup should I expect?
- **Most applications are not embarrassingly parallel**, because they have a complex organization with **dependencies between code blocks**. This motivates the [DAG / work-span model](04-work-span-model-brents-lemma.md).

## Related
- [Amdahl's law](02-amdahls-law.md) · [Work-span model](04-work-span-model-brents-lemma.md)
