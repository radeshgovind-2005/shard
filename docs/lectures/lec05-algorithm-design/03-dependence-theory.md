# Dependence Analysis: Statement Dependences (RAW / WAR / WAW), Sequential Consistency, Dependence Graphs, IN/OUT Sets

> **Source:** `ComPar-Lec05.pdf`, slides 29–46 · **Back to:** [index](../index.md)
> Bibliography: (part of) Chapter 4 of Yan Solihin, *Fundamentals of Parallel Computer Architecture*, Solihin Books (2009), ISBN 978-0-98-416300-7.

## Parallelism, correctness, and dependences
- Parallel execution is always **constrained by the sequence of operations needed for a correct result**.
- Parallel execution must address **control, data, and system dependences**.
- A **dependence** arises when one operation (B) depends on an earlier operation (A) to complete and produce a result before B can be performed.
  - The notion extends to **resources**, since some operations may depend on certain resources (e.g. because of where the data is located).

## Executing two statements in parallel
```
Single processor:        Two processors:
  Statement 1;             P1: Statement 1;   P2: Statement 2;
  Statement 2;
```
**Fundamental (concurrent) execution assumption:**
- Processors execute **independently** of each other.
- **No assumptions are made about the speed of processor execution.**

So with 2 processors, any of these interleavings can happen:
- Case 1: S1 and S2 at the same time
- Case 2: S1 before S2
- Case 3: S2 before S1

## Sequential consistency in shared memory
- A shared-memory system supports the **sequential consistency model** if **all processes see the same order of all memory access operations** on the shared memory.
- The exact order in which memory accesses are interleaved **does not matter**.
- If the statements **do not interfere with (are independent of) each other**, the result equals either "S1 then S2" or "S2 then S1". ✅ Sequential consistency.

## Independent vs dependent statements
- Two statements are **independent** if executing `S1; S2;` is equivalent to executing `S2; S1;`, meaning **their order of execution does not matter**.
- Two statements are **dependent** when the **order of their execution affects the computation outcome**.

## The three kinds of data dependence
Given `S1; S2;` (S1 before S2 in program order):

| Type | Also called | Condition | Pattern | Notation |
|---|---|---|---|---|
| **True (flow) dependence** | **RAW**: Read After Write | S2 **reads** a value **written** by S1 | `X = …` then `… = X` | S1 δ S2 (or S1 →ᵀ S2) |
| **Anti-dependence** | **WAR**: Write After Read | S2 **writes** a value **read** by S1 | `… = X` then `X = …` | S1 δ⁻¹ S2 (or S1 →ᴬ S2) |
| **Output dependence** | **WAW**: Write After Write | S2 **writes** a variable **written** by S1 | `X = …` then `X = …` | S1 δ⁰ S2 (or S1 →ᴼ S2) |

- **Anti- and output dependences are "name" dependences**: they come from **reusing a variable name**, not from a real flow of values.
- **How can we get rid of them?** By **renaming** (using a different variable/storage location), e.g. privatization or extra arrays. True dependences cannot be removed by renaming.

## Examples
| # | Code | Dependence | Can it be removed? |
|---|---|---|---|
| 1 | `S1: a=1; S2: b=1;` | **Independent** | Nothing to remove |
| 2 | `S1: a=1; S2: b=a;` | **True (flow)**: S2 depends on S1 | Not by renaming. Here, constant propagation (`b=1`) breaks it.* |
| 3 | `S1: a=f(x); S2: a=b;` | **Output**: S2 depends on S1 | Yes: rename one of the `a`s (or drop S1 if its `a` is never read)* |
| 4 | `S1: a=b; S2: b=1;` | **Anti**: S2 depends on S1 (the slide phrases it as "first is dependent on second") | Yes: rename the `b` written by S2 (e.g. `b2=1`)* |

\*The slides pose "Can you remove dependence? How?" as a question. These answers follow the standard technique (renaming for name dependences).

## Statement dependence graphs
Dependence relationships can be drawn as graphs. Example:
```
S1: a=1;
S2: b=a;
S3: a=b+1;
S4: c=a;
```
| Edge | Meaning |
|---|---|
| S1 δ S2 (S1 →ᵀ S2) | S2 is **flow-dependent** on S1 (reads `a`) |
| S2 δ S3 (S2 →ᵀ S3) | S3 is **flow-dependent** on S2 (reads `b`) |
| S3 δ S4 (S3 →ᵀ S4) | S4 is **flow-dependent** on S3 (reads `a`) |
| S1 δ⁰ S3 (S1 →ᴼ S3) | S3 is **output-dependent** on S1 (both write `a`) |
| S2 δ⁻¹ S3 (S2 →ᴬ S3) | S3 is **anti-dependent** on S2 (S2 reads `a`, S3 writes `a`) (the slide prints `S2 →A S1`, a typo) |

## When can two statements execute in parallel?
Statements S1 and S2 can execute in parallel **if and only if there are no dependences between them**: no true dependences, no anti-dependences, and no output dependences.

Some dependences can be removed by modifying the program:
- **Rearranging statements**
- **Eliminating statements**
- (and renaming, for name dependences)

## Computing dependences formally: IN / OUT sets
Data dependence relations can be found by **comparing the IN and OUT sets** of each node.
- **IN(S)**: set of memory locations (variables) that **may be used (read)** in S
- **OUT(S)**: set of memory locations (variables) that **may be modified (written)** by S
- These sets include **all** memory locations that may be fetched or modified, so they can be **conservatively large**.

Assuming there is an execution path from S1 to S2:
```
OUT(S1) ∩ IN(S2)  ≠ ∅   ⇒  S1 δ   S2   flow dependence
IN(S1)  ∩ OUT(S2) ≠ ∅   ⇒  S1 δ⁻¹ S2   anti-dependence
OUT(S1) ∩ OUT(S2) ≠ ∅   ⇒  S1 δ⁰  S2   output dependence
```

## Worked example and solving name dependences
```
S1: B = A
S2: C = C + B
S3: B = D
S4: E = B
```
| Statement | IN | OUT |
|---|---|---|
| S1 | {A} | {B} |
| S2 | {C, B} | {C} |
| S3 | {D} | {B} |
| S4 | {B} | {E} |

Dependences: **S1 δ S2** (B) · **S1 δ⁰ S3** (B) · **S2 δ⁻¹ S3** (B) · **S3 δ S4** (B)

**Solving the name dependences by renaming B in S3/S4 to F:**
```
S1: B = A
S2: C = C + B
S3: F = D
S4: E = F
```
Only the true dependences remain: **S1 δ S2** and **S3 δ S4**. The two chains `{S1→S2}` and `{S3→S4}` are now independent and **can run in parallel**.

## Related
- [Loop-carried dependences & how to remove them](04-loop-dependences-and-removal.md)
- [Summary: RAW = true, WAR/WAW = name (removable)](05-nbody-case-study-and-summary.md)
