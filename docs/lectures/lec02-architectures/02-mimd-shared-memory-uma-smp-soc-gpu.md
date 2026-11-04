# Shared-Memory MIMD: UMA / SMP, Heterogeneous SoCs, GPU Classification

> **Source:** `ComPar-Lec02.pdf`, slides 11–15 · **Back to:** [index](../index.md)

## UMA / SMP: Uniform Memory Access · Symmetric Multiprocessor

**Uniform Memory Access (UMA):**
- Every processor sees **identical latency to any memory address**.
- The memory **bus is shared**, so only one processor can access it at a time.

**Symmetric Multiprocessor (SMP):**
- Multiple identical CPUs connected to a shared memory bus.
- **Hardware cache-coherence (MESI protocol)** keeps caches consistent.

| Property | Value |
|---|---|
| Scalability | **Limited**: the bus becomes a bottleneck beyond **~16 cores** |
| Programming | **Simplest model**: shared variables, pthreads, OpenMP |
| Cache coherence | Hardware enforced (MESI / MOESI protocol) |

**Example: Intel Core i9-13900K (Raptor Lake)**
| | |
|---|---|
| Cores | 24 (8 P-core + 16 E-core) |
| Threads | 32 |
| L3 cache | 36 MB shared |
| TDP | 125 W base / 253 W PL2 |
| ISA | AVX-512 (P-cores) |
| Organisation | Shared LLC (Last-Level Cache) and shared RAM |

## Heterogeneous SoC: Apple M4 (2024)

All compute on one die, with a **Unified Memory Architecture**. Announced May 2024 · TSMC 3 nm (N3E) · used in iPad Pro, MacBook Pro, Mac Mini.

| Block | Details |
|---|---|
| **CPU** | 10 cores: 4× Firestorm P-core + 6× Icestorm E-core; up to 4.4 GHz (P) |
| **GPU** | 10-core GPU; hardware ray tracing; mesh shading; ~4.6 TFLOPS FP32 |
| **Neural Engine** | 38 TOPS; on-chip ML accelerator; "faster than H100 for on-device inference" (per slide) |
| **Media Engine** | H.264/H.265/AV1 hardware encode/decode; ProRes accelerator; 4K/8K video |
| **Unified Memory** | 16–32 GB LPDDR5X; 120 GB/s bandwidth; **all engines share the same physical memory pool** |
| **I/O & Fabric** | PCIe Gen 4; Thunderbolt 4/USB4; die interconnect >500 GB/s internal BW |

**Why it matters:** unified memory **eliminates the CPU ↔ GPU copy overhead**. All accelerators share data in place, which is a key advantage over discrete-GPU systems.

Further SoC examples shown as images: **Intel Arrow Lake-based CPU (2024–)** (slide 13) and **Apple A20 Pro (iPhone 18)** (slide 14).

## GPU: where does it fit in Flynn's taxonomy? (slide 15)

> At the **Streaming Multiprocessor (SM)** level the execution is **SIMD**, but the overall execution, which includes multiple SMs, is **Shared-Memory MIMD**.

(The slide text says "Symmetric Multiprocessor (SM)", but SM normally stands for Streaming Multiprocessor, as slide 8 says.)

## Related
- [NUMA (scaling shared memory beyond one bus)](03-numa.md)
- [Shared-memory programming model (pthreads, OpenMP)](../lec06-programming-models/02-shared-memory-model-pthreads-openmp.md)
- [Flynn's taxonomy / SIMT](01-flynn-taxonomy-sisd-simd-misd.md)
