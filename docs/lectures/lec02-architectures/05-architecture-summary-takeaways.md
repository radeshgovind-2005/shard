# Architecture Comparison, Key Takeaways & Sources (Lecture 02)

> **Source:** `ComPar-Lec02.pdf`, slides 22–24 · **Back to:** [index](../index.md)

## When to use each architecture

| Architecture | Model | Hardware | Programming | Typical use |
|---|---|---|---|---|
| **SISD** | Sequential | Single-core CPU | C, Fortran, Python | Legacy code, serial algorithms |
| **SIMD** | Data parallel | CPU AVX / GPU warps | Intrinsics, auto-vectorize | Image/ML ops, physics kernels |
| **SMP/UMA** | Shared memory | Multicore CPU | OpenMP, pthreads | Task & loop parallelism |
| **NUMA** | Shared memory | Multi-socket server | OpenMP + numactl | Databases, large in-memory workloads |
| **GPU/SIMT** | Massively parallel | NVIDIA H100, AMD MI300X | CUDA, HIP, OpenCL | Deep learning, molecular dynamics |
| **Cluster** | Distributed memory | Multiple nodes | (MPI, Spark) + (OpenMP, CUDA) | Weather, genomics, large simulations |

## Key takeaways
1. **Flynn's Taxonomy** classifies all parallel computers: **SIMD for data parallelism, MIMD for task parallelism**.
2. **SIMD is present in modern CPUs/GPUs.**
3. **Shared-memory systems (SMP, NUMA, SoC)** provide a simpler programming model but **limited scalability** (hundreds of cores).
4. **GPUs** deliver massive throughput via **SIMT**: thousands of threads, Tensor Cores, and HBM bandwidth. They are ideal for DL and HPC kernels.
5. **Distributed-memory clusters scale to exascale**: El Capitan reaches 1.742 ExaFLOPS with AMD MI300A across 44K nodes.
6. All these architectures demand **different programming models** (OpenMP, CUDA, MPI), covered in the rest of the course.

## Sources & bibliography (slide 24)
- Book: *Introduction to Parallel Computing*, Ananth Grama et al., Addison-Wesley, 2nd ed., 2003
- Book: *Parallel Programming in OpenMP*, Rohit Chandra et al., Morgan Kaufmann, 2001 (classic shared-memory reference)
- Book: *Patterns for Parallel Programming*, Mattson, Sanders & Massingill, Addison-Wesley, 2004
- Whitepaper: *NVIDIA H100 Tensor Core GPU Architecture*, NVIDIA, 2022 (nvidia.com/en-us/data-center/h100/)
- Web: Intel ARK product specifications (ark.intel.com): CPU core counts, cache sizes, TDP, ISA extensions
- Web: TOP500 list (top500.org)
- Paper: M. J. Flynn, "Some Computer Organizations and Their Effectiveness," *IEEE Trans. Computers*, 1972
- Web: Apple Silicon M4 specifications (apple.com/mac, developer.apple.com/news/)

## Related
- [Flynn's taxonomy](01-flynn-taxonomy-sisd-simd-misd.md) · [Shared memory](02-mimd-shared-memory-uma-smp-soc-gpu.md) · [NUMA](03-numa.md) · [Distributed memory](04-distributed-memory-clusters-top500.md)
- [Programming model vs architecture matrix (Lec06)](../lec06-programming-models/04-message-passing-and-hybrid.md)
