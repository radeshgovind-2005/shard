# Work-Span Analysis of Task-Parallel Programs: OpenMP `fib()`, Quicksort, Cilkview, Algorithm Table

> **Source:** `ComPar-Lec07.pdf`, slides 48–58 · **Back to:** [index](../index.md)
> Figures © 2010 Charles E. Leiserson (MIT 6.172), originally in Cilk (`cilk_spawn` / `cilk_sync`).

## OpenMP `fib()` implementation
```c
int fib(int n) {
    if (n < 2) return n;
    else {
        int x, y;
        #pragma omp task shared(x)    // launch task; x shared between spawner and task
        x = fib(n-1);
        #pragma omp task shared(y)    // launch task; y shared between spawner and task
        y = fib(n-2);
        #pragma omp taskwait          // wait for tasks
        return x + y;
    }
}
```
- `#pragma omp task` **launches a task**.
- `shared(x)` **shares the variable** between the spawner and the task (so the result is visible to the parent).
- `#pragma omp taskwait` **waits for the child tasks** to finish.
- Cilk equivalent: `x = cilk_spawn fib(n-1); y = fib(n-2); cilk_sync; return x+y;`

## Execution model
- The code is **"processor oblivious"**: it does not say how many processors to use.
- **The computation DAG unfolds dynamically.** Example: `fib(4)` spawns `fib(3)` and `fib(2)`, `fib(3)` spawns `fib(2)` and `fib(1)`, and so on down to `fib(1)` and `fib(0)` leaves.

## Example: fib(4) work & span
Assume for simplicity that **each strand in fib(4) takes unit time** to execute.
```
Work:        T1 = 17
Span:        T∞ = 8
Parallelism: T1/T∞ = 2.125     (= maximum speedup)
```
**Using many more than 2 processors can yield only marginal performance gains.**

## Quicksort analysis
```c
void qsort(void *a, size_t low, size_t high,
           int (*compar)(const void *, const void *)) {
    int p = partition(a, low, high, compar);
    #pragma omp task shared(a) firstprivate(low,p)
    { qsort(a, low, p-1, compar); }
    #pragma omp task shared(a) firstprivate(high,p)
    { qsort(a, p, high, compar); }
    #pragma omp taskwait
}
```
- **`firstprivate`**: the variables are **private** to the task and **initialized with the value they had before the task was spawned**.
- `shared(a)`: all tasks sort the same array in place.

Analysis bullets as on the slide (slide 52):
- Partition in Θ(1) time
- 2 recursive sorts of an n/2 array
- Merge array sequentially: Θ(n)
- Span for partition: Θ(1)
- Span for recursive sort of an n/2 array
- Span for merging two arrays sequentially: Θ(n)

(These bullets mix in merge-sort wording. For this quicksort the Θ(n) sequential term is the `partition` step, which scans all n elements before the two recursive tasks can start. That gives span T∞(n) = T∞(n/2) + Θ(n) = Θ(n).)

**Result (slide 53):**
```
Expected Work = O(n log n)
Expected Span = O(n)
⇒ Parallelism = O(log n)
```

## Measured parallel performance (Cilkview, sorting 100,000,000 numbers)
- The Cilkview output plots **measured speedup** vs number of cores against the theoretical bounds.
- **Parallelism = 11.21** (T₁/T∞ for this run).
- The **Span Law** line (horizontal at the parallelism, 11.21) caps speedup.
- The **Work Law** line (linear speedup, S = P) caps speedup for small P.
- Measured speedup follows the work-law line at first, then flattens below the span-law ceiling.

## Interesting practical algorithms
| Algorithm | Work | Span | Parallelism |
|---|---|---|---|
| Quick sort | Θ(n lg n) | Θ(n) | **Θ(lg n)** |
| Merge sort | Θ(n lg n) | Θ(lg³ n) | Θ(n / lg² n) |
| Matrix multiplication | Θ(n³) | Θ(lg n) | Θ(n³ / lg n) |
| Strassen | Θ(n^lg7) | Θ(lg² n) | Θ(n^lg7 / lg² n) |
| LU-decomposition | Θ(n³) | Θ(n lg n) | Θ(n² / lg n) |
| Tableau construction | Θ(n²) | Θ(n^lg3) | Θ(n^(2−lg3)) |
| FFT | Θ(n lg n) | Θ(lg² n) | Θ(n / lg n) |

## Related
- [Work-span model & Brent's lemma](04-work-span-model-brents-lemma.md)
- [OpenMP `parallel for` & reduction (Lec06)](../lec06-programming-models/02-shared-memory-model-pthreads-openmp.md)
- [Task decomposition: divide-and-conquer (Lec05)](../lec05-algorithm-design/02-decomposition-task-vs-data.md)
