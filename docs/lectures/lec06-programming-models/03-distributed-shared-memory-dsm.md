# Shared Memory on Distributed-Memory Hardware: Virtually Shared Memory (DSM)

> **Source:** `ComPar-Lec06.pdf`, slides 18–26 · **Back to:** [index](../index.md)

## Target #2: MIMD distributed-memory architectures
(Image from Mattson, Sanders, Massingill, *Patterns for Parallel Programming*, 2004.) Each node has its own local memory, and nodes are connected by a network.

**Can we still use a shared-memory model? Yes.**

## Virtually shared memory
```
 Node   Node   Node   Node   Node   Node      ← each with Local Memory
 ─────────── Virtually shared memory ───────────
               Execution flows
```
All nodes' local memories are presented to the execution flows as **one virtual shared address space**.

**What is the main disadvantage? Transparent non-uniform access to memory.**
- An access to a virtual memory position may be **mapped onto a remote memory position**.
- It **may be much slower**, and **there is no way to control this programmatically**.

## Virtually shared memory (DSM): summary
- **Abstraction:** distributed nodes **appear to share one address space**.
- The **OS transparently moves pages** between nodes on access.

**Advantages:**
- A familiar shared-memory programming model

**Disadvantages:**
- **False sharing**: 4 KB pages vs 64 B cache lines, which leads to cache thrashing
- **Unpredictable latency spikes** on remote page faults
- **Seldom used in HPC**: **MPI is preferred for predictable performance**

## The alternative: explicitly partition the car
The **local memory of each node stores a part of the car**. This leads to [message passing](04-message-passing-and-hybrid.md).

## Related
- [NUMA (hardware non-uniform access)](../lec02-architectures/03-numa.md)
- [Programming model × architecture matrix](04-message-passing-and-hybrid.md#programming-model-vs-architecture)
