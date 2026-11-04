# Parallel Algorithm Design: Why It's Hard, the Four Steps, and Profiling for Hotspots

> **Source:** `ComPar-Lec05.pdf` (Lecture 05, "Fundamentals: Parallel Algorithm Design"), slides 2–13 · **Back to:** [index](../index.md)
> Bibliography: Mattson, Sanders, Massingill, *Patterns for Parallel Programming*, Addison-Wesley, 2004 (ISBN 0-321-22811-1). Y. Solihin, *Fundamentals of Parallel Computer Architecture*, Solihin Books, 2009 (ISBN 978-0-98-416300-7).

## Parallel programming model
- To exploit parallel hardware **you must write parallel programs**.
- This requires understanding the parallel hardware, the programming model, languages, and libraries.

**Why is parallel programming hard?**
- You must **correctly decompose** the problem into concurrent tasks.
- You must manage **data distribution, communication, and synchronisation**.
- Bugs are **non-deterministic** (race conditions, deadlocks).
- Performance depends on **load balance** and **minimising communication**.

## Four steps to parallel programming
| Step | What |
|---|---|
| **1. Find opportunities for parallelisation** | Identify where time is spent (**profile first**) |
| **2. Structure the algorithm** | Decompose into tasks; analyse dependencies; group into threads |
| **3. Implement in a parallel programming environment** | OpenMP, MPI, CUDA, Spark, pthreads, … |
| **4. Execute and tune performance** | Measure, identify bottlenecks, iterate |

## How to create a parallel application
1. **Decompose** into tasks (task or data decomposition)
2. **Analyse task dependencies** and data sharing
3. **Group tasks** into execution units (threads/processes)
4. **Map** execution units to processors/cores
5. **Code** using a parallel programming environment

**Iterate:** each step may require revisiting earlier choices.

---

## Step 1: Find opportunities for parallelisation

### Before writing parallel code
- Most parallel programs **start as sequential programs**:
  - Easier to write, debug, and validate
  - Many production codes have existing, tested sequential versions
- Identify **hot spots**: the functions consuming most CPU time
- Start parallelising the **most expensive code first**
- Make **small incremental changes** and test after each one
- (Parallel) **patterns** provide reusable guidance for common structures

### Profile first: find the hotspots
- **Parallel speedup is limited by the sequential fraction** (see [Amdahl's law](../lec07-performance/02-amdahls-law.md)).
- It is not worth parallelising code that takes little time.
- How to find hotspots:
  - **Code inspection**: manually identify inner loops and dominant paths
  - **Performance profilers**: measure the actual execution time per function
- Tools: **perf** (Linux), **gprof**, **Intel VTune**, **CLion profiler** (many tools integrate into IDEs)
- Output: **flat profile** (time per function) + **call graph**

## Running example: NBody simulation
```c
// run_simulation
// T_FINAL – final time step; number_particles – number of particles in the simulation
while (t < T_FINAL && number_particles > 0) {
    t += dt;
    all_move_particles(dt);
    dt = 0.1 * max_speed / max_acc;
}

// all_move_particles
calculate_forces();          // modifies particles
move_all_particles(step);    // moves particles

// calculate_forces
for (int i = 0; i < n_particles; i++) {
    ...
    for (int j = 0; j < n_particles; j++) {
        ...
        compute_force(&particles[i], p->x_pos, p->y_pos, p->mass);
    }
}

// compute_force
x_sep = x_pos - p->x_pos;
y_sep = y_pos - p->y_pos;
dist_sq = MAX((x_sep * x_sep) + (y_sep * y_sep), 0.01);
grav_base = GRAV_CONSTANT * (p->mass) * (mass) / dist_sq;
p->x_force += grav_base * x_sep;
p->y_force += grav_base * y_sep;
```

### Profiling result (CLion profiler, 17,650 samples)
| Function | % of samples | Samples |
|---|---|---|
| `nbody::main` | 100% | 17,650 |
| `run_simulation` | 100% | 17,650 |
| `all_move_particles` | 100% | 17,650 |
| `calculate_forces` | **98.0%** | 17,302 |
| `compute_force` (inlined call) | **80.3%** | 14,179 |
| `compute_force` | 1.8% | 315 |
| `move_all_particles` | < 1% | 33 |

**Candidates for parallelisation:** run_simulation, all_move_particles, calculate_forces, compute_force.
The profiler shows `calculate_forces()` consuming ~90%+ of runtime: an **O(N²)** double loop, which is the dominant cost.

The candidate analysis (which of these is parallelisable) is in [NBody case study](05-nbody-case-study-and-summary.md).

## Related
- [Decomposition (Step 2)](02-decomposition-task-vs-data.md) · [Dependence theory](03-dependence-theory.md) · [Loop dependences](04-loop-dependences-and-removal.md)
