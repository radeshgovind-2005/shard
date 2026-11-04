# NBody Case Study: Analysing the Parallelisation Candidates + Lecture 05 Summary

> **Source:** `ComPar-Lec05.pdf`, slides 75–79 · **Back to:** [index](../index.md)

The profiler (see [profiling](01-four-steps-and-profiling.md)) gave four candidates: `run_simulation`, `all_move_particles`, `calculate_forces`, `compute_force`. Which can be parallelised?

| Candidate | Verdict | Reason |
|---|---|---|
| **`run_simulation`** (`while (t < T_FINAL && ...) { t += dt; all_move_particles(dt); dt = 0.1*max_speed/max_acc; }`) | ✗ **No** | Each iteration uses state from the previous step (`dt`, positions). **Loop-carried true dependence**, so it is NOT parallelisable. |
| **`all_move_particles`** (`calculate_forces(); move_all_particles(step);`) | ✗ **No** | `calculate_forces()` **writes** `particle.x_force/y_force`, and `move_all_particles()` **reads** those forces. **True dependence between the two calls**, so they must stay sequential. |
| **`calculate_forces`** (outer `for i`, inner `for j`, `compute_force(&particles[i], …)`) | ✓ **Yes** | Iteration *i* writes **only** `particles[i].x_force / y_force`. Different iterations write different particles, so the **outer loop is DOALL**. |
| **`compute_force`** (the per-pair arithmetic) | ! **Not worth it** | Some statements are independent, but a single execution of `compute_force` is far too little work to justify parallelization (grain too fine). |

**Conclusion:** parallelise the **outer `i` loop of `calculate_forces`** (each thread handles N/T particles and reads all N particles read-only, matching the [geometric decomposition](02-decomposition-task-vs-data.md#example-nbody-decomposition)).

Note: the *inner* `j` loop accumulates into the same `particles[i].x_force` (`p->x_force += …`), so parallelising it would need a reduction.

## Lecture 05 summary
- **Profile first**: parallelise where the time is actually spent.
- **Decompose**: task (pipeline/DAG) or data (partition large structures).
- **Dependence theory:**
  - **RAW = true** (ordering required). **WAR/WAW = name** (removable).
  - Use **IN/OUT sets** to compute dependences formally.
- **Loops:** DOALL if there are no loop-carried deps; otherwise **privatise / split / wavefront** (or realign, peel, or change the algorithm).

## Bibliography
1. T. G. Mattson, B. A. Sanders, B. L. Massingill, *Patterns for Parallel Programming*, Addison-Wesley, 2004. ISBN 0-321-22811-1
2. Y. Solihin, *Fundamentals of Parallel Computer Architecture*, Solihin Books, 2009. ISBN 978-0-98-416300-7
