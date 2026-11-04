# Flynn's Taxonomy: SISD, SIMD, MISD (and the MIMD overview)

> **Source:** `ComPar-Lec02.pdf` (Lecture 02, "Fundamentals: Parallel Architectures"), slides 3–10 · **Back to:** [index](../index.md)
> Bibliography: section on Parallel Architectures of any course book, e.g. Ananth Grama et al., *Introduction to Parallel Computing*, 2nd ed., 2003.

Flynn's Taxonomy is a classification of computer architectures proposed in 1966 and still the standard today (M. J. Flynn, "Some Computer Organizations and Their Effectiveness," *IEEE Trans. Computers*, 1972). It classifies machines by the number of **instruction streams** and **data streams**.

## The 2×2 grid

| | **Single Instruction** | **Multiple Instructions** |
|---|---|---|
| **Single Data** | **SISD**: classical sequential (von Neumann). One instruction stream, one data stream. *e.g. single-core CPU* | **MISD**: rarely used in practice. Systolic arrays and fault-tolerant systems. *e.g. Space Shuttle flight control* |
| **Multiple Data** | **SIMD**: one instruction applied to many data elements simultaneously. *e.g. AVX-512, ARM SVE, GPU warps* | **MIMD**: independent processors with independent instruction & data streams. *e.g. multicore CPUs, clusters* |

## SISD: Single Instruction, Single Data (the classical sequential computer)
- **Sequential execution:** instructions are fetched and executed one at a time from a single instruction stream.
- **Single data stream:** each instruction operates on one piece of data (or one small word) at a time.
- **ILP optimizations:** modern SISD CPUs hide latency with pipelining, superscalar issue, and out-of-order execution, but there is fundamentally still **one logical thread**.
- **The baseline:** all sequential programs are SISD. The other three classes describe departures from this model toward parallel hardware.
- Example: a single-core Intel Pentium 4 running a C program (one thread, one CPU). This is the **von Neumann architecture**.

## SIMD: Single Instruction, Multiple Data (one instruction, many operands in parallel)

**How SIMD works:**
- Scalar (SISD): `A[0]→r`, `A[1]→r`, `A[2]→r`, `A[3]→r` takes **4 separate instructions**.
- SIMD (e.g. AVX-512): `A[0] | A[1] | … | A[15]` takes **1 instruction for 16 operands at once**.
- Vector add diagram: registers X = (x7…x0) and Y = (y7…y0) give X+Y = (x7+y7 … x0+y0) in a single instruction (lanes of 64 bits shown; SSE ⊂ AVX ⊂ AVX10 register widths).

**Key use cases:** image/video processing · ML matrix ops · signal processing · physics simulations.

### SIMD implementations
- **x86 SSE/AVX:** 128-bit SSE (1999) → 256-bit AVX2 (2013) → 512-bit AVX-512 (2017). Found in Intel/AMD desktop & server CPUs.
- **ARM SVE / SVE2:** Scalable Vector Extension, **vector-length agnostic (128–2048 bits)**. Used in Arm Neoverse V2 (AWS Graviton4) and the Fugaku supercomputer.
- **GPU SIMT (Single Instruction, Multiple Threads):** GPU **warps execute one instruction across 32 threads simultaneously**. They are SIMD at the hardware level inside a **Streaming Multiprocessor (SM)** (GPU details come later in the semester).

> Note: **MISD is largely theoretical**, and almost no commercial hardware uses it. The lecture skips to MIMD.

### SIMD hardware evolution (timeline)
| Year | Hardware | Notes |
|---|---|---|
| 1976 | **CRAY-1** | First successful vector supercomputer. 64 × 64-bit elements per vector register. Peaked at 160 MFLOPS. Los Alamos National Lab. |
| 1999–now | **x86 SSE → AVX-512 → AVX10** | SSE (128-bit, 4×float) → AVX (256-bit, 8×float) → AVX-512 (512-bit, 16×float) → AVX10 uniformization across CPUs |
| 2016–now | **ARM SVE / SVE2** | Vector-length agnostic: same code runs on 128-bit to 2048-bit hardware. SVE2 extends to integer & cryptography workloads. Arm Neoverse V2 (AWS Graviton4, Google Axion). |
| 2006–now | **NVIDIA SIMT** | GeForce RTX 5090 (Blackwell architecture): 128 CUDA cores per SM, 4 warp schedulers, 32-thread warps. Tensor Cores with native FP4/FP6 support for matrix SIMD. |

**Trend:** ever-wider SIMD registers, plus more parallelism within a single instruction stream.

## MIMD: Multiple Instruction, Multiple Data (slide 10)

Diagram: N independent units, each with its own **control unit** (own instruction stream) and **processor** (own input data → output data), all connected through an **interconnect network**.

MIMD splits into:
- **Shared-memory MIMD**: UMA/SMP, SoC, NUMA. See [shared memory](02-mimd-shared-memory-uma-smp-soc-gpu.md) and [NUMA](03-numa.md).
- **Distributed-memory MIMD**: clusters and supercomputers. See [distributed memory](04-distributed-memory-clusters-top500.md).

## Related
- [Architecture summary table & takeaways](05-architecture-summary-takeaways.md)
- [Map pattern (data-parallel op, maps naturally to SIMD)](../lec03-data-parallel-mapreduce-spark/02-map-pattern-and-fusion.md)
