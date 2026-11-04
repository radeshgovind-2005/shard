# Step 2, Structuring the Algorithm: Decomposition (Task vs Data), Grids, Ghost Cells, NBody & Car-Crash Examples

> **Source:** `ComPar-Lec05.pdf`, slides 14–28 · **Back to:** [index](../index.md)

## Parallel decomposition framework: five design concerns
| Concern | Question |
|---|---|
| **Decomposition** | How do we divide the work into concurrent tasks? |
| **Dependency analysis** | Which tasks must precede others? |
| **Group tasks** | How do we aggregate tasks into threads/processes? |
| **Order tasks** | How do we schedule tasks within each execution unit? |
| **Data sharing** | Which data is shared and which is private? |

## Who does the decomposition?
- **Usually the programmer**, because automatic decomposition is still hard.
- Compiler challenges:
  - Must analyse **all dependencies statically**
  - Dependencies may be **input-dependent**, so they are only known at runtime
  - Some success for **loops without loop-carried dependencies**
- **Parallel patterns** (map, reduce, scan, …) abstract the decomposition.
- **Libraries** (OpenMP `parallel for`, MPI collectives) help too.

## Task (functional) decomposition
- Decompose the work into **distinct tasks** to be performed.
- Tasks **communicate to coordinate** operations.
- Examples:
  - **Pipeline**: read → parse → compute → write (each stage is a task)
  - **Divide-and-conquer**: sub-problems solved independently, then merged
  - **Event-driven**: tasks triggered by events (server request handling)
- **Best when:** there are distinct phases with **different operations on the same data**.

### Task decomposition guidelines
- **Flexibility:** the number and size of tasks should **not be tied to a specific architecture**. Parameterise tasks so they can scale with the available processors.
- **Efficiency:** tasks must have **enough work to amortise creation/management cost**, and should be sufficiently **independent** (so dependencies don't become a bottleneck).
- **Simplicity:** code must remain readable, maintainable, and debuggable.

## Data (domain) decomposition
- View the problem as **operations on a large distributed data structure**.
- The data is distributed across processes, and **each one processes its local portion**.
- Examples:
  - **Matrix multiplication**: distribute rows to threads
  - **Image processing**: distribute image tiles to processes
  - **Stencil computations**: each process owns a sub-domain
- **Best when:** the **same operation is applied to many independent data elements**.

### Data decomposition guidelines
- **Flexibility:** chunk size and count should support a wide range of thread/process counts.
- **Efficiency:**
  - Chunks must generate substantial work (**adequate grain**)
  - **Comparable chunk sizes** give good load balance
  - **Minimise communication volume** (locality of access)
- **Simplicity:** complex decompositions are hard to manage and debug.

## Common data decomposition patterns
**Geometric data structures (arrays, grids):**
- **1D:** decompose an N-element array into contiguous **strips**
- **2D:** **row, column, or block** decomposition of matrices
- **3D:** **slab or pencil** decomposition (climate, CFD)

**Recursive data structures:**
- Lists, trees, graphs: partition by **subtree** or **connected component**

**Ghost cells / halo regions** are needed at **subdomain boundaries**.

## Decomposing 2D / 3D grids
For a 2D N×M grid distributed across P processes:
| Decomposition | What each process owns | Notes |
|---|---|---|
| **Row** | N/P contiguous rows | Good for row-major access (C arrays); easy to implement |
| **Column** | M/P contiguous columns | Good for column-major access (Fortran); transpose may help |
| **Block** | √P × √P blocks | **Better surface/volume ratio → less communication**; preferred for 2D stencils and dense linear algebra |

(See also block / cyclic / block-cyclic 1D distributions in [Lec03](../lec03-data-parallel-mapreduce-spark/04-data-decomposition-block-cyclic.md).)

## Example: NBody decomposition
**All-pairs algorithm (O(N²)), geometric decomposition:**
- Distribute N particles equally, so each of T threads gets **N/T particles**.
- Each thread computes the forces on its N/T particles.
- This requires **reading all N particles**, which is safe as a **read-only broadcast**.

**Barnes-Hut algorithm (O(N log N)), recursive decomposition:**
- Recursively subdivide space into **octree** cells.
- Far-away clusters are approximated as a **single mass**.
- Decompose by **octree subtrees**, a naturally recursive structure.

## Example: car crash simulation (Finite Element Method)
- Ford Motor Company crash safety simulation (Stout & Jablonowski, University of Michigan).
- **FEM mesh of ~300,000 elements.**
- Each time step:
  - Compute stress/strain in each element
  - Detect contact/collision between surfaces
  - Update node positions and velocities
- Total: **~1,500 steps → ~10 billion arithmetic operations**.

**FEM model:**
- The car is modelled by a **triangulated surface** (elements).
- The simulation models the movement of the elements, incorporating the forces on them to determine their new positions.
- In each time step, the movement of each element depends on its **interaction with the elements it is physically adjacent to**.
- In a crash, elements may end up **touching other elements that were not touching initially**.
- The **state** of an element is its location, velocity, and information such as whether it is metal that is bending.

**Possible decomposition:** for E elements and T execution flows (typically threads), each one processes approximately **E/T elements** (the slide writes N/T; the figure shows a car split for 3 threads).

**In a distributed-memory setting:** is it enough to simply distribute the car? **No.** Each node also needs the **state of the elements on the boundaries of the part of the car it received (ghost cells)**.

## Related
- [Dependence analysis](03-dependence-theory.md)
- [Car crash with shared memory vs message passing (Lec06)](../lec06-programming-models/01-car-crash-and-parallel-101.md)
- [Task-parallel DAGs & work-span (Lec07)](../lec07-performance/04-work-span-model-brents-lemma.md)
