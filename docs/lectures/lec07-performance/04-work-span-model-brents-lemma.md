# DAG Model, Work-Span Model, Work & Span Laws, Composition Rules, Brent's Lemma, Parallel Slack

> **Source:** `ComPar-Lec07.pdf`, slides 32–47 · **Back to:** [index](../index.md)
> Composition diagrams © 2010 Charles E. Leiserson (MIT 6.172).

## DAG model of computation
- Think of a program as a **directed acyclic graph (DAG) of tasks**:
  - A task **cannot execute until all its inputs are available**.
  - The inputs come from the **outputs of earlier executing tasks**.
  - The DAG shows the **task dependencies** explicitly.
- Think of the hardware as consisting of **workers (processors)**.
- Consider a **greedy scheduler** of the DAG tasks onto workers: **no worker is idle while there are tasks still to execute**.

## Work-span model
| Symbol | Name | Meaning |
|---|---|---|
| **T_P** | | Time to run with P workers |
| **T₁** | **Work** | Time for serial execution: all tasks executed by 1 worker = **sum of all work** |
| **T∞** | **Span** | Time along the **critical path**: the path through the DAG that takes the longest to execute. Assumes an **infinite number of workers**. |

- **WORK LAW:** **T_P ≥ T₁ / P**
- **SPAN LAW:** **T_P ≥ T∞**

## Composition rules
| | Work | Span |
|---|---|---|
| **Series composition** (A then B) | T₁(A∪B) = T₁(A) + T₁(B) | T∞(A∪B) = T∞(A) + T∞(B) |
| **Parallel composition** (A ∥ B) | T₁(A∪B) = T₁(A) + T₁(B) | **T∞(A∪B) = max{T∞(A), T∞(B)}** |

## Work-span example (7-task DAG, each task = 1 time unit)
DAG shape (from the slide figure): a source task, which fans out to three branches of different lengths, which join and lead to a sink task. The longest path has 5 tasks.
- **T₁ = 7**: all tasks have to be executed. Executed serially, they can run in **any order that respects the dependencies** (a topological order).
- **T∞ = 5**: the time along the critical path, i.e. the longest path length through the DAG respecting the dependencies.

## Lower and upper bounds on greedy scheduling
Suppose we only have **P** workers.
- **Lower bound (work-span):**
  ```
  TP ≥ max(T1/P, T∞)
  SP = T1/TP ≤ T1/T∞        (T1/T∞ = parallelism = maximum possible speedup)
  ```
- **Upper bound, Brent's lemma:**
  - T∞ is the best possible execution time.
  - It captures the **additional cost of executing the other tasks not on the critical path**,
  - assuming this can be done **without overhead**:
  ```
  TP ≤ (T1 − T∞) / P + T∞
  ```

## Brent's lemma for 2 processors (same 7-task DAG)
```
T1 = 7,  T∞ = 5
T2 ≤ (T1 − T∞)/P + T∞ = (7 − 5)/2 + 5 = 6
max(7/2, 5) ≤ T2 ≤ 6   ⇒   5 ≤ T2 ≤ 6
```
**Can we do better? No!** (This is the slide's answer.)

## "Amdahl was an optimist!"
Treat the same DAG with Amdahl, assuming a **2/7 sequential fraction f** (the source and sink tasks).
| Model | Predicted max speedup | Comment |
|---|---|---|
| **Amdahl's law** | f = 2/7, so as P → ∞, **S → 7/2 = 3.5** | **Over-optimistic.** It ignores the internal structural dependencies between tasks and assumes all parallel parts can run at the same time. |
| **Work-span bound** | **S ≤ T₁/T∞ = 7/5 = 1.4** | Accounts for the critical-path limit: speedup can never exceed the total work divided by the longest sequential chain. |
| **Brent's lemma** | S ≥ T₁ / ((T₁−T∞)/P + T∞), e.g. P=2 → 7/6 ≈ 1.17 | A **realistic lower-bound estimate** of the actual speedup achieved by a realistic (greedy) scheduler managing P processors. |

Chart (P = 1…8): the Amdahl curve climbs toward ~2.7 by P = 8, while the work-span bound is flat at 1.4 from P = 2. Brent's lemma rises from 1 toward ~1.35, and the achievable speedup lies in the band between the Brent and work-span curves.

## Estimating running time
- **Scalability requires that T∞ be dominated by T₁.**
  ```
  TP ≤ (T1 − T∞)/P + T∞
  TP ≈ T1/P + T∞      if T∞ << T1
  ```
- **Increasing work hurts parallel execution proportionately.**
- **The span impacts scalability.**

## Parallel slack
**Sufficient parallelism implies linear speedup:**
```
TP ≈ T1/P     if   T1/T∞ >> P
  (linear speedup)   (parallel slack)
```
**Parallel slack** = T₁/T∞ (the parallelism) divided by P. When the algorithm has much more parallelism than processors, the greedy scheduler achieves near-linear speedup. (The slide labels this "parallel stack", a typo for slack.)

## Related
- [Applying work-span to fib() and quicksort; algorithm table](05-task-parallel-analysis-fib-quicksort.md)
- [Amdahl's law](02-amdahls-law.md) · [Reduction tree: span log n](../lec03-data-parallel-mapreduce-spark/03-reduction-pattern.md)
