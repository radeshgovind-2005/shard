# Shared-Memory Programming Model: Characteristics, Challenges, pthreads, OpenMP, Parallel Crash Simulation

> **Source:** `ComPar-Lec06.pdf`, slides 12–17 · **Back to:** [index](../index.md)

## Base parallel computing model #1: shared memory
- **Shared memory address space**
- **(Typically) easier to program**
  - **Implicit communication** via (shared) data
  - **Explicit synchronization** to access data
- **Programming methodology:**
  | Level | Example |
  |---|---|
  | **Manual** | Explicit threading (e.g. **POSIX threads**) |
  | **Guided** | **OpenMP** parallelism directives; **CUDA** for GPUs |
  | **Automatic** | Parallelizing compilers |

## Key challenges
- **Scheduling and load balancing**
- **Data locality**: the layout of the data in the cache
- **Synchronization overhead**
- Avoiding **race conditions and deadlocks** (studied in the *Concurrent Systems* course)

## POSIX threads (pthreads)
Lower-level API: explicit **create, join, mutex, condition variables**.
```c
pthread_t tid;
pthread_create(&tid, NULL, worker, arg);
pthread_join(tid, NULL);
```
Widely used in system software. It gives **more control, but more boilerplate**.

## OpenMP example: parallel loop (dot product)
```c
double sum = 0.0;
#pragma omp parallel for reduction(+:sum)
for (int i = 0; i < N; i++)
    sum += a[i] * b[i];   // dot product
```
**What the runtime does:**
- Spawns **T threads**, and each gets **N/T iterations**
- Each thread keeps a **private partial sum**
- The **reduction combines the partial sums**, with an **implicit barrier at the end**

## Basic parallel version of the crash simulation (shared memory)
```
parallel for all elements
    read ( State(element), Properties(element), Neighbor_list(element) )
for step = 1 to end_of_simulation
    parallel for element = 1 to num_elements
        Compute State(element) for next step, based on the previous state
        of the element and its neighbors and the properties of the element
    synchronize          ← Why?
```
**Why synchronize?** Step *s+1* of an element needs the **step-s state of its neighbours**, which other threads compute. All threads must finish step *s* before any thread starts step *s+1* (a barrier). Otherwise a thread could read a neighbour's state that is still old or only half updated. The time-step loop itself stays **sequential** (a loop-carried dependence across steps).

## Same thing in OpenMP
```c
#pragma omp parallel for
for all elements
    read ( State(element), Properties(element), Neighbor_list(element) )
                                        // ← implicit synchronization (barrier)
for step = 1 to end_of_simulation
    #pragma omp parallel for
    for element = 1 to num_elements
        Compute State(element) for next step, ...
                                        // ← implicit synchronization (barrier)
```
The `omp parallel for` construct ends with an **implicit barrier**, so the explicit `synchronize` is not needed.

## Related
- [Shared-memory architectures (UMA/SMP/NUMA)](../lec02-architectures/02-mimd-shared-memory-uma-smp-soc-gpu.md)
- [Loop dependences: when `parallel for` is legal](../lec05-algorithm-design/04-loop-dependences-and-removal.md)
- [OpenMP tasks: `fib`, quicksort (Lec07)](../lec07-performance/05-task-parallel-analysis-fib-quicksort.md)
- [Message passing version](04-message-passing-and-hybrid.md)
