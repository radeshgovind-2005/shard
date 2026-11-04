# Software Requirements & the Parallel Hardware Landscape

> **Source:** `ComPar-Lec01.pdf`, slides 14–16 · **Back to:** [index](../index.md)

## Reason 2: Software requirements

Modern applications demand more compute than any single core can deliver.

| Domain | Examples |
|---|---|
| **Scientific Simulation** | Climate models, protein folding, astrophysics: billions of coupled equations solved simultaneously |
| **AI & Deep Learning** | GPT-4 training: ~25,000 A100 GPUs over months; inference at billions of requests/day |
| **Finance & HPC** | Real-time risk analysis, high-frequency trading, Monte Carlo option pricing at microsecond latency |
| **Graphics & Games** | Real-time ray tracing of millions of polygons; physics simulation at 60+ fps |
| **Big Data Analytics** | Processing petabytes of logs, genomics, sensor streams: Spark, Hadoop, cloud data warehouses |
| **Medical Imaging** | Real-time MRI/CT reconstruction; AI-assisted diagnosis on 3-D volumetric data |

## Parallel hardware landscape (slide 15)

| | **Multicore CPU** | **GPU** | **Cluster / HPC** |
|---|---|---|---|
| Cores | 4 – 192 cores | 1,000s of cores | Millions of cores |
| Memory hierarchy | L1 (per core): ~32 KB, <1 ns | Registers: per-thread, ~1 cycle | Local DRAM per node: ~100 ns |
| | L2 (per core): ~256 KB, ~4 ns | Shared mem: per-block, ~5 cycles | Intra-node NVLink / PCIe |
| | L3 (shared): 32–768 MB, ~30 ns | L2 cache: device-wide, ~200 cycles | InfiniBand: ~1 µs, 400 Gb/s |
| | DRAM: up to TBs, ~100 ns | HBM/GDDR: 80 GB/s – 3 TB/s | Parallel FS (Lustre): ~ms latency |
| Typical use | General-purpose · OS · databases · servers | Graphics · deep learning · scientific | Petascale sim · big data · cloud |

## High-performance solutions: GPUs + clusters (slide 16)

### GPU: many-core processor
- NVIDIA H100: **16,896 CUDA cores**, compared with 16–32 cores on a CPU.
- Designed for **throughput, not latency**.
- Thousands of lightweight threads execute the same instruction on different data: **SIMT (Single Instruction Multiple Threads)**.
- **Best for:** matrix ops, linear algebra, deep learning, image/video processing, physics simulation.

### Cluster: distributed computing
- Hundreds to millions of nodes (Top500 #1: ~10 M cores).
- Multiple computers linked by a high-speed network (InfiniBand, RoCE).
- Each node has its **own local memory**.
- Communication via **message passing (MPI)** or **data-parallel frameworks (Spark)**.
- **Best for:** big data, distributed ML, HPC simulations, fault-tolerant long-running jobs.

## Related
- [Three walls & multicore](02-three-walls-and-multicore.md)
- [Shared-memory architectures (UMA/SMP/SoC/GPU)](../lec02-architectures/02-mimd-shared-memory-uma-smp-soc-gpu.md)
- [Distributed memory & Top500](../lec02-architectures/04-distributed-memory-clusters-top500.md)
