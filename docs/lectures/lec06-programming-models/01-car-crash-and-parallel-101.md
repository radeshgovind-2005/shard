# Running Example (Car Crash FEM): Sequential Version & "Parallel Computing 101"

> **Source:** `ComPar-Lec06.pdf` (Lecture 06, "Fundamentals: Base Parallel Programming Models"), slides 2–11 · **Back to:** [index](../index.md)

## Recap: car crash simulation
- Ford Motor Company crash safety simulation (Stout & Jablonowski, University of Michigan)
- **Finite Element Method (FEM)**: a mesh of **~300,000 elements**
- Each time step: compute stress/strain in each element · detect contact/collision between surfaces · update node positions and velocities
- Total: **~1,500 steps, ~10 billion arithmetic operations**
- The car is a **triangulated surface**. Each element's movement depends on its **physically adjacent** elements, and in a crash elements may touch others that weren't touching initially. Element **state** = location, velocity, and info such as whether it is metal that is bending.

(More detail in [Lec05 decomposition](../lec05-algorithm-design/02-decomposition-task-vs-data.md#example-car-crash-simulation-finite-element-method).)

## Sequential crash simulation
```
for all elements
    read ( State(element), Properties(element), Neighbor_list(element) )
for step = 1 to end_of_simulation
    for element = 1 to num_elements
        Compute State(element) for next step, based on the previous state
        of the element and its neighbors and the properties of the element
```

## Parallel computing 101
- **Decompose** the problem among the processing execution flows (processes / software threads / …). This may take several steps.
- **All execution flows work on their part** of the problem.
- Often **communication of data, or simply synchronization**, is needed at key points of the algorithm.

**Three key questions:**
1. **Where does the car live?** All elements in a single memory, or partitioned?
2. **What work will be done by each execution flow?**
3. The execution flows need to **coordinate to get a single result**. How?

## Target #1: MIMD shared-memory architectures
- **Non-uniform memory access (NUMA)**, e.g. **each node of DI's cluster**.
- (Image from Mattson, Sanders, Massingill, *Patterns for Parallel Programming*, 2004.)

## Decomposition for shared memory
For **E elements** and **T execution flows** (typically threads), each one processes approximately **E/T elements** (the figure shows a decomposition for 3 threads).
- With 4 cores, each core computes E/4 elements in parallel (each core = Fetch/Decode + ALU (Execute) + Execution Context).
- With 16 cores, each core computes E/16 elements in parallel.

## Related
- [Shared-memory model, pthreads, OpenMP version](02-shared-memory-model-pthreads-openmp.md)
- [Distributed shared memory](03-distributed-shared-memory-dsm.md) · [Message-passing version & hybrid](04-message-passing-and-hybrid.md)
