# Why Parallel Computing: Processor Evolution, the Three Walls, and Multicore

> **Source:** `ComPar-Lec01.pdf`, slides 6–13 and 17 · **Back to:** [index](../index.md)

**Sequential speedup is over, and has been for a long time.** There are two reasons with one solution:
1. **Processor evolution** (this file)
2. **Software requirements** (see [applications & hardware landscape](03-applications-and-hardware-landscape.md))

**The solution: parallel computing.**

## Reason 1: Processor evolution (42 years of microprocessor trend data)

Slides 7–10 use Karl Rupp's chart *"42 Years of Microprocessor Trend Data"* (https://www.karlrupp.net/2018/02/42-years-of-microprocessor-trend-data/).
- Power density rose to "hot plate", then toward "nuclear reactor" and "rocket nozzle" levels if trends had continued.
- **Moore's Law still holds**: how do we use the extra transistor space?
- **Single-thread performance still increases, but not as before.**
- **Sequential programming by itself is not enough anymore.**
- **Multi-core to the rescue**: more cores and more cache.

## The Three Walls: why frequency scaling ended

| Wall | Cause | Response |
|---|---|---|
| **Power Wall** | Dynamic power **P ∝ C × V² × f**. To double frequency, voltage must rise, so power grows roughly **cubically**. At 3–4 GHz, chips dissipate 100–300 W, and cooling becomes physically impossible. | Lower voltage, **stop raising frequency**. |
| **Memory Wall** | CPU speed >> memory bandwidth. RAM latency (~100 ns, **200–400 cycles**) dominates. Caches help but don't eliminate the gap. | Add cache levels, NUMA, prefetching. |
| **ILP Wall** | Instruction-Level Parallelism is limited. Out-of-order, superscalar, and branch prediction extract implicit parallelism, but typical programs hit a **~3–6 IPC** (Instructions Per Cycle) ceiling regardless of hardware width. | **Require explicit parallelism from the programmer.** |

Recommended reading: Herb Sutter, *"The Free Lunch Is Over"*, Dr. Dobb's Journal, 2005.

## The multicore response

**Moore's Law still holds:** transistor count continues to double every ~2 years. But the extra transistors are **no longer used for faster sequential execution**. They pay for:
- **More cores** (parallelism)
- **Larger caches** (latency hiding)
- **Specialised units** (vector, crypto, AI)

**Consequence:** sequential programs no longer automatically get faster. *The free lunch is over.*

**What this means for software:**
- A program that runs on a single core gets **zero benefit from 16 cores**.
- To exploit modern hardware, **parallelism must be expressed explicitly by the programmer**.

## Modern processor architecture, 18 years apart (slide 13)

| | Intel Xeon (2005) | Intel Core i9-13900K (2023) |
|---|---|---|
| Cores | 1 core @ 3.8 GHz | 16 P-cores @ 3.0 GHz + 8 E-cores @ 2.2 GHz |
| Hardware threads | 1 | 32 |
| L1 | one L1 | 16 L1 caches of 32 KB + 32 KB |
| L2 | 2 MB | 16 L2 caches of 256 KB |
| L3 | — | 36 MB |

(Note: Lec02 lists the same i9-13900K as 24 cores = 8 P + 16 E. The Lec01 slide labels are as transcribed above.)

## The conclusion (slide 17)

**Sequential programming by itself is not enough anymore.**
- Hardware no longer provides automatic speedup, because frequency is frozen.
- Writing parallel programs is hard: you have to **identify parallelism, partition work, and synchronise/communicate**.
- Many solutions are **architecture-dependent**, so the right approach depends on the platform.

The course covers writing parallel programs for **shared memory (particularly GPU platforms)** and **distributed memory**.

## Related
- [Definitions](01-definitions-concurrency-vs-parallelism.md)
- [Hardware landscape & memory hierarchy numbers](03-applications-and-hardware-landscape.md)
- [Parallel architectures (Lec02)](../lec02-architectures/01-flynn-taxonomy-sisd-simd-misd.md)
