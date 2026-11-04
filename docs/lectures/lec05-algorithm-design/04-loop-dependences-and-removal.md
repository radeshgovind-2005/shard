# Loop-Level Parallelism: DOALL, Loop-Carried Dependences, Rule of Thumb, Examples, and Removal Techniques

> **Source:** `ComPar-Lec05.pdf`, slides 47–74 · **Back to:** [index](../index.md)

## Loop-level parallelism
Significant parallelism can be found **within loops**.
```c
for (i=0; i<100; i++)                 parallel_for (i=0; i<100; i++) {
    S1: a[i] = i;                         S1: a[i] = i;
                                          S2: b[i] = 2*a[i];
                                      }
```
- Dependences? What about `i`, the loop index? (Each iteration gets its own private copy of `i`.)
- Are the iterations of the second loop independent? Yes. `S1 → S2` is a true dependence **within** the same iteration (loop-independent), not across iterations.
- **DOALL (or `parallel for`) loop:**
  - All iterations are **independent** of each other.
  - "All statements will be executed in parallel at the same time." **Is this really true?** Not literally: the iterations *may* run in parallel, but how many actually run at once depends on how many processors/threads are available and how the iterations are scheduled.

## Loop-carried dependences
- Statement dependences include true, anti-, and output dependences.
- **Loop dependences also include those carried from one execution of the loop to another.**
- A **loop-carried dependence** is a dependence between two statement instances in **two different iterations** of a loop. Otherwise it is **loop-independent**.
- **Loop-carried dependences can prevent loop iteration parallelization.**

| Kind | Straight-line code | Loop-carried |
|---|---|---|
| **True** | `S1: a = 5; S2: b = a;` → **S1 δ S2** | `for (i=1;i<n;i++) S1: a[i] = a[i-1];` → `a[j]` is written before it is read in the next iteration: **S1[j] δ S1[j+1]** |
| **Anti** | `S1: b = a; S2: a = 5;` → **S1 δ⁻¹ S2** | `for (i=0;i<n-1;i++) S1: a[i] = a[i+1];` → `a[j]` is read before it is written in the next iteration: **S1[j] δ⁻¹ S1[j+1]** |
| **Output** | `S1: c = 8; S2: c = 15;` → **S1 δ⁰ S2** | `for (i=0;i<n-1;i++){ S1: a[i] = i; S2: a[i+1] = 5; }` → `a[j]` written by S2, then again by S1 in the next iteration: **S2[j] δ⁰ S1[j+1]** |

**Example of a loop that cannot be parallelized (without rewriting):**
```c
a[0] = 1;
for (i=1; i<N; i++)
    a[i] = a[i] + a[i-1];
// i=1: a[1] = a[1] + a[0];
// i=2: a[2] = a[2] + a[1];   ← each iteration depends on the result of the preceding one
// i=3: a[3] = a[3] + a[2]; ...
```

## Detecting dependences
Analyse how each variable is used within a loop iteration:
- Is the variable **read and never written**? Then there are **no dependences**.
- For each **written** variable: can there be any accesses to it in **iterations other than the current one**? Then **there are dependences**.

## Simple rule of thumb
A loop that matches **all** of these criteria has no dependences and can be parallelized:
1. **All assignments to shared data are to arrays;**
2. **Each element is assigned by at most one iteration;** and
3. **No iteration reads elements assigned by any other iteration.**

## Is this loop parallelizable? (Examples 1–6)

**Example 1: YES**
```c
for (i=1; i<N; i+=2)
    a[i] = a[i] + a[i-1];
// i=1: a[1] = a[1] + a[0];  i=3: a[3] = a[3] + a[2];  i=5: a[5] = a[5] + a[4]; ...
```
Odd elements are written and even elements are only read. **No dependences, so it is parallelizable.**

**Example 2: YES**
```c
for (i=0; i<N/2; i++)
    a[i] = a[i] + a[i+N/2];
// i=0: a[0] = a[0] + a[N/2]; ... i=N/2-1: a[N/2-1] = a[N/2-1] + a[N-1];
```
The first half is written and the second half is only read. **No dependences, so it is parallelizable.**

**Example 3: NO**
```c
for (i=0; i<=N/2; i++)          // note: <=
    a[i] = a[i] + a[i+N/2];
// i=0: a[0] = a[0] + a[N/2];   ← reads a[N/2]
// ...
// i=N/2: a[N/2] = a[N/2] + a[N];  ← writes a[N/2]
```
**Loop-carried anti-dependence** (iteration 0 reads `a[N/2]`, which iteration N/2 writes). **NOT parallelizable.**
**Fix: take the dependent iteration out of the loop:**
```c
for (i=0; i<N/2; i++)
    a[i] = a[i] + a[i+N/2];     // now DOALL
a[N/2] = a[N/2] + a[N];         // done sequentially afterwards
```

**Example 4: NO (indirect indexing)**
```c
for (i=0; i<N; i++)
    a[idx[i]] = a[idx[i]] + b[idx[i]];
// i=0: a[?1] = ...;  i=1: a[?2] = ...;  — unknown which index each iteration accesses
```
We don't know at compile time which index each iteration accesses (`idx` may contain duplicates). **NOT parallelizable** in general.

**Example 5: DEPENDS (aliasing)**
```c
for (i=1; i<N; i++)
    a[i] = b[i-1];
// a[1] = b[0]; a[2] = b[1]; a[3] = b[2]; ...
```
**Parallelizable only if `a` and `b` do not overlap.** If we give the compiler that information (in C, via the **`restrict`** keyword), it is parallelizable. Otherwise it is **NOT** parallelizable.

**Example 6: NO (2D stencil, in place)**
```c
for (i=1; i<=n; i++)
  for (j=1; j<=n; j++)
    a[i][j] = a[i][j-1] + a[i][j+1] + a[i-1][j] + a[i+1][j];
```
- `a[i][j-1]` and `a[i-1][j]` were **already updated** in this sweep, giving **true dependences** (from the left and from above).
- `a[i][j+1]` and `a[i+1][j]` are **old values** that will be overwritten later, giving **anti-dependences** (to the right and below).
- Neither the `i` loop nor the `j` loop is DOALL.

## Techniques for removing dependences

### 1. Privatization (removes name dependences on scalars)
```c
for (i=0; i<N; i++) {
    x = (b[i] + c[i]) / 2;
    a[i] = a[i+1] + x;
}
```
Dependences:
- **True** dependence inside the loop (on `x`)
- **Output** dependence between iterations (on `x`)
- **Anti**-dependence between iterations (on `x`)
- **Anti**-dependence between iterations (on `a[i]`: iteration i reads `a[i+1]`, which iteration i+1 writes)

**To remove the dependences on `x`, privatize it** (each iteration/thread gets its own `x`, e.g. `int x` declared inside the loop or OpenMP `private(x)`).

### 2. Copying to a temporary array / loop fission (removes the anti-dependence on `a`)
```c
for (i=0; i<N; i++)
    a2[i] = a[i+1];             // snapshot old values

for (i=0; i<N; i++) {
    int x = (b[i] + c[i]) / 2;  // x privatized
    a[i] = a2[i] + x;
}
```
**Both `for` loops are parallelizable!** *Should we do it?* It is a trade-off: an extra array (memory) plus an extra pass over the data (memory bandwidth), and an implicit barrier between the two loops. That is worth paying only if the parallel speedup outweighs it.

### 3. Loop alignment, guided by the dataflow (removes a loop-carried true dependence)
```c
for (i=1; i<N; i++) {
    b[i] += a[i-1];     // reads a[i-1], written by the previous iteration → loop-carried true dep
    a[i] += c[i];
}
// i=1:   b[1]=b[1]+a[0];     a[1]=a[1]+c[1]
// i=2:   b[2]=b[2]+a[1];     a[2]=a[2]+c[2]
// i=N-1: b[N-1]=b[N-1]+a[N-2]; a[N-1]=a[N-1]+c[N-1]
```
Look at the dataflow: `a[i]` is produced from `c[i]`, and `b[i+1]` consumes `a[i]`. **Realign** so the producer and consumer of `a[i]` are in the **same** iteration:
```c
b[1] += a[0];                      // peeled first consumer
for (i=1; i<N-1; i++) {
    a[i]   += c[i];
    b[i+1] += a[i];                // dependence is now loop-independent
}
a[N-1] += c[N-1];                  // peeled last producer
```
(Intermediate version on slide 66: `b[1] += a[0]; for (i=2; i<N; i++){ a[i-1] += c[i-1]; b[i] += a[i-1]; } a[N-1] += c[N-1];`.) Now every iteration is independent, so the loop is **DOALL**.

### 4. Wavefront / anti-diagonal traversal (for Example 6's stencil)
In the 2D stencil, the points on the **same anti-diagonal** (i + j = const) do not depend on each other. Each depends only on points of the previous anti-diagonal (true deps from left/up) and on old values of the next one (anti deps to right/down).
```
calculate number of anti-diagonals
foreach anti-diagonal do {                      // sequential
    calculate number of points in current anti-diagonal
    foreach point in the current anti-diagonal do {   // PARALLEL
        compute the current point in the matrix
    }
}
```
(Slide 70 diagram: grid of points i=1..n−1, j=1..n−1; solid arrows = true dependences, dashed = anti-dependences; dashed diamonds group each anti-diagonal.)

## Not all loops can be made parallel! (but some have alternatives)
- Some loop-carried dependences **cannot be removed**.
- Classic example: the **prefix scan**
  ```c
  for (i=1; i<N; i++)
      a[i] = a[i] + a[i-1];
  ```
  - A **true RAW loop-carried dependence**: `a[i]` depends on `a[i-1]`.
  - It **cannot be removed**, so you must use a **different algorithm** (one that can be parallelized, e.g. a parallel scan).
- **Recurrences and reductions often require algorithmic restructuring**, not just dependency removal. **Recognise the pattern and use the right algorithm** (see [reduction](../lec03-data-parallel-mapreduce-spark/03-reduction-pattern.md)).

## Related
- [Dependence theory (RAW/WAR/WAW, IN/OUT sets)](03-dependence-theory.md)
- [NBody candidate analysis](05-nbody-case-study-and-summary.md)
- [OpenMP `parallel for` / `reduction` (Lec06)](../lec06-programming-models/02-shared-memory-model-pthreads-openmp.md)
