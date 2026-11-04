# Distributed-Memory MIMD: Clusters, Clouds, Supercomputers & Top500

> **Source:** `ComPar-Lec02.pdf`, slides 18–21 · **Back to:** [index](../index.md)

## Architecture
- Each node has its **own private memory**.
- Nodes communicate by **sending and receiving messages** over an interconnect.
- There is **no hardware-enforced global address space**.

### Advantages
- **Scales to millions of cores** (no bus bottleneck)
- **No cache-coherence overhead**
- **Commodity hardware**, so cost-effective

### Challenges
- **Explicit message passing (MPI)**
- Network latency & bandwidth limits
- Harder to program than shared memory
- Frameworks such as **Spark reduce this complexity at the cost of some performance**

## Cluster interconnects
| Interconnect | Typical use |
|---|---|
| **InfiniBand** | Supercomputers, HPC, LLM / AI training clusters |
| **Ethernet** | Cloud data centers, enterprise networks, distributed databases |
| **NVIDIA NVLink / NVSwitch** | GPU-to-GPU multi-node scaling (e.g. NVIDIA DGX clusters) |
| **Ultra Ethernet (UEC)** | Ethernet-based, open, interoperable, high-performance full communications stack for growing AI & HPC network demands |

## Typical distributed-memory MIMD architecture of today (slide 20)
- Multiple **nodes** connected via high-speed local networks
- **Each node is a NUMA node** with multiple multicore processors
- Each node **may also have 1 or more GPUs**
- Examples:
  - DI cluster: http://cluster.di.fct.unl.pt
  - Top500: https://top500.org/lists/top500/2026/06/

## Exascale supercomputers: Top500, June 2026 (slide 21)
| Rank | System | Site | Rmax | Nodes / Cores | Hardware |
|---|---|---|---|---|---|
| #1 | **LineShine** | National Supercomputing Center (NSCS), China | 2.198 ExaFLOPS (slide prints "2,198") | 20,480 nodes, 13,789,440 cores | CPU: LX2 304C 1.55 GHz, 304 cores each |
| #2 | **El Capitan** | Lawrence Livermore National Laboratory (LLNL), USA | 1.742 ExaFLOPS | 44,544 nodes, 11,340,000 cores | CPU: AMD EPYC Genoa · GPU: AMD Instinct **MI300A APU** (CPU+GPU+HBM on one die) |
| #3 | **Frontier** | Oak Ridge National Laboratory (ORNL), USA | 1.206 ExaFLOPS | 9,408 nodes, 9,066,176 cores | CPU: AMD EPYC Trento (64 cores) · GPU: 4× AMD Instinct MI250X per node |

## Related
- [Message-passing programming model](../lec06-programming-models/04-message-passing-and-hybrid.md)
- [MapReduce / Spark (data-parallel on distributed memory)](../lec03-data-parallel-mapreduce-spark/06-mapreduce-model-and-architecture.md)
- [Architecture summary](05-architecture-summary-takeaways.md)
