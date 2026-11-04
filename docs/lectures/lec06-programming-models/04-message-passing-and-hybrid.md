# Message-Passing Programming Model, Ghost Cells, Model×Architecture Matrix, Hybrid Programming

> **Source:** `ComPar-Lec06.pdf`, slides 27–34 · **Back to:** [index](../index.md)

## Base parallel computing model #2: message passing
- **(Relatively) harder to program:**
  - Data distribution
  - **Communication via messages**
  - **Synchronization via messages**
- **Programming methodology:**
  - **Explicit message passing**
    - Explicit data distribution
    - Explicit communication via messages
    - Explicit synchronization via messages
    - Plenty of libraries to choose from (**MPI dominates**): send-receive, one-sided, active messages
  - **Distributed computing frameworks**
    - **Explicit** data distribution
    - **Implicit** communication and synchronization via messages
    - **Spark, MapReduce, …**

## Domain distribution: ghost cells
**Is it enough to simply distribute the car? No.** Each node also needs the **state of the elements on the boundaries of the part of the car it received (ghost cells)**.

## Basic parallel version of the crash simulation (message passing)
First attempt:
```
Concurrently for P processors
    for all elements assigned to P
        read ( State(element), Properties(element), Neighbor_list(element) )
    for step = 1 to end_of_simulation
        for element = 1 to num_elements_in_P
            Compute State(element) for next step, based on the previous state
            of the element and its neighbors and the properties of the element
```
**Is synchronization enough? No.** With separate memories, a processor **cannot see** its neighbours' updated boundary elements. The data must be **sent**.

Correct version:
```
Concurrently for P processors
    for all elements assigned to P
        read ( State(element), Properties(element), Neighbor_list(element) )
    for step = 1 to end_of_simulation
        for element = 1 to num_elements_in_P
            Compute State(element) for next step, based on the previous state
            of the element and its neighbors and the properties of the element
        communicate the contents of the ghost cells to neighbors
```
(This is a BSP-style superstep: local compute, then halo exchange. See [BSP](../lec03-data-parallel-mapreduce-spark/05-bsp-model.md).)

## Programming model vs architecture
| Programming model \ Architecture | Shared-memory architecture | Distributed-memory architecture |
|---|---|---|
| **Shared memory** | √ | √ (via virtually shared memory / DSM) |
| **Message passing** | √ | √ |

**Either model can run on either architecture.** The model is an abstraction, not a hardware constraint.

## Hybrid parallel programming
**Two-level parallelism:**
- **Message passing between main tasks** (e.g. MPI processes, one per node)
- **Shared-memory parallelism within each main task** (sub-tasks, e.g. OpenMP threads/CUDA)

Applied to the car: **the local memory of each node stores a part of the car**, and **multiple threads work on the partition of the car residing in local memory**, using a shared-memory programming model.

This matches today's typical cluster: [NUMA nodes + GPUs connected by a network](../lec02-architectures/04-distributed-memory-clusters-top500.md), programmed as "(MPI, Spark) + (OpenMP, CUDA)" ([architecture summary](../lec02-architectures/05-architecture-summary-takeaways.md)).

## Related
- [Shared-memory model](02-shared-memory-model-pthreads-openmp.md) · [DSM](03-distributed-shared-memory-dsm.md)
- [Ghost cells / halo regions in decomposition (Lec05)](../lec05-algorithm-design/02-decomposition-task-vs-data.md)
- [Spark/MapReduce as distributed frameworks](../lec03-data-parallel-mapreduce-spark/09-spark-motivation-rdd-architecture.md)
