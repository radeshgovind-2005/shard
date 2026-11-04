# Bulk Synchronous Parallel (BSP) Model

> **Source:** `ComPar-Lec03.pdf`, slides 19–22 · **Back to:** [index](../index.md)
> Paper: https://dl.acm.org/doi/abs/10.1145/3749740.3749759 (Bisseling, "The Bulk-Synchronous Parallel Model"). BSP is due to **Valiant (1990)** and is a formal model for analysing parallel algorithms.

## What BSP is
- BSP is the **most common execution framework for synchronous data parallelism**.
- **BSP requirements:**
  - A set of **processor–memory pairs**
  - A **point-to-point communication network**
  - A mechanism for **efficient barrier synchronization** of all processors

```
        Communication Network
   ┌────────┬────────┬─────┬────────┐
 Memory   Memory   Memory  …  Memory
 Processor Processor Processor … Processor
```

## Supersteps
- A BSP computation consists of a **sequence of supersteps**.
- In each superstep, processes **execute computations using locally available data** and **issue communication requests**.
- Processes are **synchronized at the end of the superstep**, at which point **all communications issued have been completed**.

```
p0  p1  p2  p3  p4
 |   |   |   |   |    Local compute
 |──→|   |←──|   |    Communicate (send/recv)
 ════════════════════ Barrier synchronization
 (repeat for each superstep)
```

## BSP parameters & cost model
| Param | Meaning |
|---|---|
| **p** | Number of processors |
| **l** | Barrier synchronization latency |
| **g** | Communication bandwidth ratio (time per word communicated) |

**Cost of one superstep:**
```
W + g·h + l
  W = local computation (max over processors)
  h = max messages sent/received by any processor
  l = barrier latency cost
```
**Total cost** = Σₛ (Wₛ + g·hₛ + l) over all supersteps s.

## Example: distributed vector sum
Compute Σ aᵢ over N = 12 elements with P = 3 workers, with the result needed on Worker 0.

**Superstep 1: local compute** (no communication)
- W0: `local_sum = a₀+a₁+a₂+a₃`
- W1: `local_sum = a₄+a₅+a₆+a₇`
- W2: `local_sum = a₈+a₉+a₁₀+a₁₁`
- Barrier sync

**Superstep 2: communicate + reduce**
- W1: `send(local_sum → W0)`; W2: `send(local_sum → W0)`
- W0: `total = sum0 + recv(W1) + recv(W2)`
- Barrier sync

**BSP cost:** 2 supersteps → `2l + W₁ + W₂ + g·h`, where **h = 2** (W0 receives 2 messages; W1 and W2 each send 1 message in superstep 2).

## Related
- [Reduction](03-reduction-pattern.md) · [MapReduce (map → shuffle barrier → reduce is BSP-like)](06-mapreduce-model-and-architecture.md)
- [Spark stages: a stage starts only after the previous one completes](11-spark-dag-stages-caching-fault-tolerance.md)
