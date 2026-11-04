# NUMA: Non-Uniform Memory Access (scaling shared memory beyond a single bus)

> **Source:** `ComPar-Lec02.pdf`, slides 16–17 · **Back to:** [index](../index.md)

## The problem with UMA at scale
A shared bus limits scalability to **~16 cores**. Adding more CPUs creates a bottleneck, because every memory access competes for the same bus.

## The NUMA solution
Each processor (socket) has its **own local memory**. Cross-socket access goes over an **interconnect** (Intel UPI, AMD Infinity Fabric). It is slower, but possible. All memory is still in **one shared address space**.

| Aspect | Value |
|---|---|
| **Local access** | ~50–80 ns (fast path) |
| **Remote access** | ~100–200 ns, **2–4× slower** |
| **Interconnect** | Intel UPI (12 GT/s), AMD Infinity Fabric (up to 92 GB/s) |
| **OS awareness** | `numactl`, NUMA-aware allocators |

## Example: dual AMD EPYC 9654 (Genoa)
| | |
|---|---|
| Cores/socket | 96 (2 × 96 = **192 total**) |
| L3 cache | 384 MB per socket |
| Memory | 12-channel DDR5 |
| Bandwidth | 460 GB/s per socket |
| Interconnect | AMD Infinity Fabric |
| Use case | Database, HPC, cloud |

Two sockets × 96 cores = 192 cores **sharing one OS**.

## Note on "CPU" in NUMA diagrams (slide 17)
In the diagram, a "CPU" is a generalisation for a CPU **or hardware thread**. For example, a 2-core CPU with two hardware threads per core gives **4 logical CPUs** that may access the bus.

## Related
- [UMA/SMP](02-mimd-shared-memory-uma-smp-soc-gpu.md)
- [Target: MIMD shared-memory architectures (NUMA nodes in DI cluster)](../lec06-programming-models/02-shared-memory-model-pthreads-openmp.md)
- [Memory Wall](../lec01-why-parallel/02-three-walls-and-multicore.md)
