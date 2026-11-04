# What Is Parallel Computing? Concurrency vs. Parallelism

> **Source:** `ComPar-Lec01.pdf` (Lecture 01, "Why Parallel Computing and Course Overview"), slides 3–5 · **Back to:** [index](../index.md)

## Core definitions

| Term | Definition |
|---|---|
| **Parallel Computer** | A system with multiple processing elements that can work simultaneously: multicore CPUs, many-core GPUs, networked clusters. |
| **Parallel Computing** | An execution model in which many calculations or processes are executed concurrently, exploiting multiple processing elements. |
| **Parallel Programming** | A programming model that expresses concurrent computations and their coordination, through threads, processes, or higher-level abstractions. |

Broader definition (slide 5): *the simultaneous use of multiple compute resources to solve a computational problem, achieved by dividing the work among processing units that execute concurrently.*

## Concurrency vs. Parallelism

These terms are often used interchangeably, but they are **not** the same thing.

| | **Concurrency** | **Parallelism** |
|---|---|---|
| Essence | *Structure*: dealing with multiple things at once | *Execution*: doing multiple things at the same time |
| Timing | Multiple tasks make progress over overlapping time periods | Multiple tasks execute simultaneously on multiple hardware units |
| Hardware | May take turns on a single CPU (time-slicing) | Requires actual parallel hardware (multiple cores or processors) |
| Main concern | **Correctness**: tasks interact and must be coordinated | **Performance**: how fast can we finish the total work? |
| Example | A web server handling thousands of connections, even on one core | Summing an array, with each core summing a different chunk simultaneously |

- A program can be **concurrent without being parallel** (e.g., cooperative multitasking).
- A program can be **parallel without explicit concurrency abstractions** (e.g., SIMD).
- **This course focuses on parallelism.**

## Four goals of parallel computing

| Goal | Meaning |
|---|---|
| **Speed** | Solve problems faster than any single processor ever could |
| **Scale** | Handle data and models too large for one machine's memory |
| **Throughput** | Process more requests or work items per unit of time |
| **Efficiency** | Deliver more performance per watt (increasingly critical) |

## Related
- [Why parallel: the three walls and multicore](02-three-walls-and-multicore.md)
- [Flynn's taxonomy (SIMD = parallel without concurrency abstractions)](../lec02-architectures/01-flynn-taxonomy-sisd-simd-misd.md)
