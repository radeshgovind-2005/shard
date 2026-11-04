# Parallel Performance: What It Is, Scalability, and Metrics (Speedup, Efficiency, Cost)

> **Source:** `ComPar-Lec07.pdf` (Lecture 07, "Fundamentals: Parallel Performance"), slides 2–9 · **Back to:** [index](../index.md)
> Bibliography: Chapter 2 of McCool, Robison, Reinders, *Structured Parallel Programming: Patterns for Efficient Computation*, Morgan Kaufmann (2012), ISBN 978-0-12-415993-8.
> Other sources: Parallel Computing CIS 410/510 (Univ. of Oregon). MIT 6.172 Lecture 13 "Parallelism and Performance" (Leiserson, 2010): https://ocw.mit.edu/courses/electrical-engineering-and-computer-science/6-172-performance-engineering-of-software-systems-fall-2010/video-lectures/lecture-13-parallelism-and-performance/MIT6_172F10_lec13.pdf

## Lecture 07 outline
- Performance scalability
  - Analytical performance measures
  - **Amdahl's law**
  - **Gustafson-Barsis' law**
  - **Work-span and Brent's lemma**

## What is performance?
- In computing, performance is defined by 2 factors:
  - **Computational requirements** (what needs to be done?)
  - **Computing resources** (how much will it cost?), which is where **efficiency** comes in
- Computational problems translate to requirements.
- Computing resources interplay and trade off: **Hardware, Time, Energy… and ultimately Money**.

## What is parallel performance?
- Parallel processing includes the techniques and technologies needed to compute in parallel: hardware, networks, operating systems, parallel libraries, languages, compilers, algorithms, tools, …
- We care about performance issues when using a parallel computing environment: **parallel performance vs sequential performance**.
- **If the "performance" is not better, parallelism is not necessary.**
- **Parallelism must deliver performance.** How? How well?

## Performance expectation (loss)
- If each processor is rated at **f MFLOPS** and there are **p** processors, should we see **f × p MFLOPS**?
  - If it takes 100 s on 1 processor, shouldn't it take 10 s on 10 processors?
- Several causes affect performance:
  - Each must be understood separately,
  - but they **interact in complex ways**: the solution to one problem may create another, and one problem may mask another.
- **Scaling (system, problem size) can change conditions.**
- You need to understand the **performance space**.

## Embarrassingly parallel computations
- A computation that can be **obviously divided into completely independent parts** that can be executed simultaneously.
  - There is **no interaction** between separate processes, except the (initial) **work distribution** and (final) **results collection and combination**.
- They have the potential to achieve **maximal speedup**: if it takes **T** sequentially, it could take **T/P** with P processors.
- **Why is this not the (usual) case?** Because of the sequential parts (distribution/collection), overheads (thread creation, communication, synchronization), load imbalance, and contention for shared resources such as memory bandwidth.

## Scalability
- The **ability of a parallel algorithm to achieve performance gains proportional to the number of processors and the size of the problem**.
- How do we measure scalability, and how do we judge how good it is?
  - Comparative evaluation:
    - If we double the number of processors, what should we expect?
    - Is scalability linear?
    - Is efficiency retained as the problem size increases?
  - Apply **performance metrics**.

## Performance and scalability
- **Sequential runtime (Tseq or T1)** is a function of the **problem size and architecture**.
- **Parallel runtime (Tpar)** is a function of the **problem size and parallel architecture**, and of **the number of processors used**.
- Performance is affected by **algorithm + architecture**.

## Performance metrics and formulas
| Symbol | Name | Formula |
|---|---|---|
| T₁ | Execution time on a single processor | |
| Tₚ | Execution time on a p-processor system | |
| **S(p)** | **Speedup** | **S(p) = T₁ / Tₚ** |
| **E(p)** | **Efficiency** | **E(p) = S(p) / p** |
| **C(p)** | **Cost** | **C(p) = p × Tₚ** |

**A parallel algorithm is cost-optimal if** Σ parallel time = sequential time, i.e. **E(p) = 100%** and **C(p) = T₁**.

## Related
- [Amdahl's law](02-amdahls-law.md) · [Gustafson-Barsis](03-gustafson-barsis-law.md) · [Work-span & Brent](04-work-span-model-brents-lemma.md) · [Types of speedup](06-speedup-types-superlinear.md)
- [Formulas cheat sheet](../formulas-cheatsheet.md)
