# Course Presentation: Info, Goals, Syllabus, Bibliography, Projects, Evaluation

> **Source:** `ComPar-Lec01.pdf`, slides 2 and 18–24 · **Back to:** [index](../index.md)

Course: **Parallel Computing (Computação Paralela) · 2026/27** · Departamento de Informática · NOVA FCT

## Basic information

| | |
|---|---|
| Instructor (lectures & labs) | Hervé Paulino |
| Office | Dep. Informática · Building II · Room P2/16 |
| Extension | 10729 |
| Office hours | Mondays 11:00 – 12:30 |
| Online resources | Class web page; CLIP (assignments & handouts); Discussion forum: https://piazza.com/fct.unl.pt/fall2026/compar13436 |

**Piazza rules:** share experiences and observations, not just questions · use smart, descriptive titles · share ideas and approaches, **not solutions** · all enrolled students were invited via their @campus.fct.unl.pt address, and anyone not included can register with the link above.

## Course goals

**Understand:**
- Specificities of parallel vs sequential programming
- Parallel architectures: shared / distributed / GPU
- Shared memory and message passing models
- Metrics for evaluating parallel programs
- GPU programming and execution models

**Be able to:**
- Identify parallelisation opportunities
- Partition a problem across processors
- Implement parallel algorithms correctly
- Reason about correctness and behaviour
- Measure, analyse, and optimise performance

**Know:**
- Abstract models for parallel computation
- Programming languages and libraries for multicore, GPU, and distributed memory
- Algorithmic strategies for parallelism and optimisation techniques

## Syllabus

| # | Part | Topics |
|---|---|---|
| 1 | **Why Parallel Computing** | Sequential limitations (Power/Memory/ILP walls) · Forms of parallelism · Programming models overview |
| 2 | **Fundamentals** | Parallel architectures and programming models · Designing parallel programs · Performance analysis & metrics |
| 3 | **Shared Memory** | Dealing with shared state · GPU computing (CUDA) · Performance optimisation |
| 4 | **Data Parallel Computing** | Thinking data-parallel · Distributed Data Parallel (Apache Spark) · Performance optimisation |

## Bibliography

There is no single textbook (widespread practice in parallel/HPC courses). Key books:
- **Pacheco, P.**, *An Introduction to Parallel Programming*, Morgan Kaufmann, 2011
- **Kumar, V. et al.** (Grama, Gupta, Karypis, Kumar), *Introduction to Parallel Computing*, 2nd ed., Addison-Wesley, 2003
- **Kirk, D. & Hwu, W.**, *Programming Massively Parallel Processors*, 4th ed., Morgan Kaufmann, 2022
- **Mattson, T. et al.**, *Patterns for Parallel Programming*, Addison-Wesley, 2004
- **Apache Spark**: Resilient Distributed Datasets (RDD), https://spark.apache.org/docs/latest/sql-programming-guide.html
- **NVIDIA Corporation**: *CUDA C++ Programming Guide*, docs.nvidia.com (latest)
- For "Why Parallel Computing": e.g. Part 1, Section 1 of Robert Robey & Yuliana Zamora, *Parallel and High Performance Computing*, Simon and Schuster (2021), ISBN 1617296465

## Lab classes & projects

- Lab classes: design and implement parallel programs.
- **Groups of 2** (all exceptions require authorisation; members may be in different lab sections).

| | Project 1: GPU Computing | Project 2: Data Parallel Distributed |
|---|---|---|
| Start | First week of October | Beginning of November 2026 |
| Deadline | TBA | TBA |
| Discussion | TBA | TBA |

**Project practices:**
- Use **Git** extensively. A submission is a **commit ID**. Each group has one **private repo**.
- Ask questions (teacher, colleagues, Piazza). **Cite any source that inspired your work.** Cited material is not cheating.

## Evaluation

### CTP: Theoretical-Practical Component (weight 60%)
- Two individual in-person **tests** during the semester, **or** an individual **exam**.
- In person, **without consultation**. **Minimum: CTP ≥ 8.50**.
- No electronic devices capable of internet access or Bluetooth (smartphones, smartwatches, smartglasses, tablets, laptops), **even if turned off**. A violation means immediate failure by exclusion, reported to the Scientific Committee.

### CL: Laboratory Component (weight 40%)
```
CL = 0.6 × CG + 0.4 × CI
CG = Group grade (two projects + reports, groups of 2, same grade for both members)
CI = Individual grade (practical test on each project + possible oral discussion)
```
- **Frequency (lab approval): CL ≥ 8.5**. Frequency from the previous two years is accepted. Students may still attend to improve, and the final CL is the better of the two years.

### Final grade
```
CF = 0.6 × CTP + 0.4 × CL
```
