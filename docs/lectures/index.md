# Parallel Computing 2026/27 (NOVA FCT): Lecture Knowledge Base Index

> Markdown transcription of the 7 lecture PDFs in `docs/lecture/` (`ComPar-Lec01.pdf` … `ComPar-Lec07.pdf`, Prof. Hervé Paulino), split into one file per topic.
> **For agents:** read this index, pick the file(s) for your topic from the tables below, and open only those. Every file starts with its source PDF and slide numbers, so you can check the original if needed. Paths are relative to this `md/` directory.

## How to find things
1. **By lecture**: see [Lectures at a glance](#lectures-at-a-glance), then the per-lecture file tables.
2. **By keyword**: see the [Keyword lookup](#keyword-lookup) at the bottom (e.g. "Amdahl", "ghost cells", "reduceByKey").
3. **Just need a formula or number?** Go to [formulas-cheatsheet.md](formulas-cheatsheet.md).

## Lectures at a glance
| # | PDF | Title | Folder | Main topics |
|---|---|---|---|---|
| 01 | `ComPar-Lec01.pdf` | Why Parallel Computing & Course Overview | [lec01-why-parallel/](lec01-why-parallel/) | definitions, concurrency vs parallelism, power/memory/ILP walls, hardware landscape, course logistics & grading |
| 02 | `ComPar-Lec02.pdf` | Fundamentals: Parallel Architectures | [lec02-architectures/](lec02-architectures/) | Flynn's taxonomy, SIMD, UMA/SMP, SoC, GPU, NUMA, clusters, Top500 |
| 03 | `ComPar-Lec03.pdf` | Data-Parallel Thinking & Distributed Data Parallel Processing | [lec03-data-parallel-mapreduce-spark/](lec03-data-parallel-mapreduce-spark/) | map/reduce patterns, decomposition, BSP, MapReduce, Spark RDDs, DAG, caching |
| 04 | `ComPar-Lec04.pdf` | Distributed Data Parallel Processing (Part 2) | [lec04-spark-part2/](lec04-spark-part2/) | PageRank, Spark execution model, narrow/wide deps, DataFrames, Spark SQL, Catalyst, AQE, optimization |
| 05 | `ComPar-Lec05.pdf` | Fundamentals: Parallel Algorithm Design | [lec05-algorithm-design/](lec05-algorithm-design/) | four steps, profiling, task/data decomposition, dependences (RAW/WAR/WAW), loop-carried deps, NBody |
| 06 | `ComPar-Lec06.pdf` | Fundamentals: Base Parallel Programming Models | [lec06-programming-models/](lec06-programming-models/) | shared memory (pthreads, OpenMP), DSM, message passing (MPI), ghost cells, hybrid |
| 07 | `ComPar-Lec07.pdf` | Fundamentals: Parallel Performance | [lec07-performance/](lec07-performance/) | speedup/efficiency/cost, Amdahl, Gustafson-Barsis, work-span, Brent, fib/quicksort, superlinear |

---

## Lecture 01: Why Parallel Computing & Course Overview
| File | Contents |
|---|---|
| [01-definitions-concurrency-vs-parallelism.md](lec01-why-parallel/01-definitions-concurrency-vs-parallelism.md) | Parallel computer/computing/programming definitions; concurrency vs parallelism table; the four goals (speed, scale, throughput, efficiency) |
| [02-three-walls-and-multicore.md](lec01-why-parallel/02-three-walls-and-multicore.md) | Processor evolution, Moore's law; **Power / Memory / ILP walls**; the multicore response; "free lunch is over"; Xeon 2005 vs i9-13900K |
| [03-applications-and-hardware-landscape.md](lec01-why-parallel/03-applications-and-hardware-landscape.md) | Application domains needing parallelism; multicore vs GPU vs cluster with **memory-hierarchy latencies**; H100, SIMT, clusters |
| [04-course-info-evaluation.md](lec01-why-parallel/04-course-info-evaluation.md) | Instructor, Piazza, goals, syllabus, bibliography, **projects (GPU, Spark)**, **evaluation formulas (CTP/CL/CF)** |

## Lecture 02: Parallel Architectures
| File | Contents |
|---|---|
| [01-flynn-taxonomy-sisd-simd-misd.md](lec02-architectures/01-flynn-taxonomy-sisd-simd-misd.md) | **Flynn's taxonomy** (SISD, SIMD, MISD, MIMD); SIMD mechanics; SSE/AVX/AVX-512/AVX10, ARM SVE, GPU SIMT; SIMD timeline (CRAY-1 → Blackwell) |
| [02-mimd-shared-memory-uma-smp-soc-gpu.md](lec02-architectures/02-mimd-shared-memory-uma-smp-soc-gpu.md) | **UMA/SMP**, MESI cache coherence, ~16-core bus limit; Apple M4 SoC & unified memory; GPU = SIMD within an SM, MIMD overall |
| [03-numa.md](lec02-architectures/03-numa.md) | **NUMA**: local vs remote latency, UPI/Infinity Fabric, numactl; dual EPYC 9654 example; logical CPUs |
| [04-distributed-memory-clusters-top500.md](lec02-architectures/04-distributed-memory-clusters-top500.md) | **Distributed-memory MIMD**: pros/cons, interconnects (InfiniBand, Ethernet, NVLink, UEC), typical cluster, **Top500 June 2026** |
| [05-architecture-summary-takeaways.md](lec02-architectures/05-architecture-summary-takeaways.md) | "When to use each architecture" table (model, hardware, programming, use); key takeaways; sources |

## Lecture 03: Data-Parallel Thinking, MapReduce, Spark
| File | Contents |
|---|---|
| [01-data-parallel-thinking.md](lec03-data-parallel-mapreduce-spark/01-data-parallel-thinking.md) | Lecture agenda; worker-centric vs **data-parallel thinking**; map/filter/reduce/scan; **embarrassingly parallel** problems |
| [02-map-pattern-and-fusion.md](lec03-data-parallel-mapreduce-spark/02-map-pattern-and-fusion.md) | **Map** pattern, elemental function, unary/N-ary map examples, **map fusion** |
| [03-reduction-pattern.md](lec03-data-parallel-mapreduce-spark/03-reduction-pattern.md) | **Reduction**, associativity, serial vs **tree reduction** worked example |
| [04-data-decomposition-block-cyclic.md](lec03-data-parallel-mapreduce-spark/04-data-decomposition-block-cyclic.md) | **Block / cyclic / block-cyclic** decomposition with N=12, P=3 example; pros/cons |
| [05-bsp-model.md](lec03-data-parallel-mapreduce-spark/05-bsp-model.md) | **BSP** (Valiant): requirements, supersteps, parameters p/l/g, **cost W + g·h + l**, vector-sum example |
| [06-mapreduce-model-and-architecture.md](lec03-data-parallel-mapreduce-spark/06-mapreduce-model-and-architecture.md) | **MapReduce** origins, timeline, Hadoop; map/reduce signatures; master/worker architecture (steps 1–6); partition & combine functions; why it's popular |
| [07-mapreduce-example-shuffle-combiner.md](lec03-data-parallel-mapreduce-spark/07-mapreduce-example-shuffle-combiner.md) | Worked **(A,B)… edge-count example**; **shuffle phase** (partition, spill, copy, merge); **combiner** |
| [08-mapreduce-fault-tolerance-limitations.md](lec03-data-parallel-mapreduce-spark/08-mapreduce-fault-tolerance-limitations.md) | **Fault tolerance** (stateless, HDFS 3×, heartbeats, idempotent); iterative/interactive pain; **limitations**; beyond MapReduce |
| [09-spark-motivation-rdd-architecture.md](lec03-data-parallel-mapreduce-spark/09-spark-motivation-rdd-architecture.md) | **Why Spark** vs MapReduce (benchmarks); **RDD** properties (distributed, resilient, immutable, lazy); lineage; languages; **Driver / Cluster Manager / Executors** |
| [10-spark-rdd-api-transformations-actions.md](lec03-data-parallel-mapreduce-spark/10-spark-rdd-api-transformations-actions.md) | **Transformations vs actions** table; PySpark examples (map, filter, flatMap, set ops, stats); **pair RDDs** (reduceByKey, groupByKey, join, mapValues); **word count** walkthrough & stages |
| [11-spark-dag-stages-caching-fault-tolerance.md](lec03-data-parallel-mapreduce-spark/11-spark-dag-stages-caching-fault-tolerance.md) | **Log-mining** example with cache; **lineage-based fault recovery** test; **DAG scheduler, narrow/wide deps, stages, tasks**; **cache/persist & storage levels** |
| [12-dataframe-sql-intro-and-takeaways.md](lec03-data-parallel-mapreduce-spark/12-dataframe-sql-intro-and-takeaways.md) | Pointer to the DataFrame/SQL content (full version in Lec04 files); **Lecture 03 key takeaways** |

## Lecture 04: Spark Part 2 (Execution Model, DataFrames, Catalyst, Optimization)
| File | Contents |
|---|---|
| [01-pagerank-example.md](lec04-spark-part2/01-pagerank-example.md) | **PageRank** formula, iterative algorithm, 4-page worked example, PySpark code, **co-partitioning** (`partitionBy`), Hadoop vs Spark timings |
| [02-spark-execution-model-lineage-dependencies.md](lec04-spark-part2/02-spark-execution-model-lineage-dependencies.md) | Driver/executors/partitions; **lineage**; RDD interface (partitions, deps, compute, metadata); **narrow vs wide** deps; DAG → **stages → tasks**; executor cores; **groupByKey vs reduceByKey** |
| [03-dataframe-api-spark-sql.md](lec04-spark-part2/03-dataframe-api-spark-sql.md) | RDD → DataFrame → Dataset; **DataFrame API** & transformations/actions; PySpark DF ops; **Spark SQL** with temp views & window functions; **RDD vs DataFrame** table; **log-analysis case study** |
| [04-catalyst-optimizer-explain-plans.md](lec04-spark-part2/04-catalyst-optimizer-explain-plans.md) | **Catalyst pipeline**; code generation; full **`explain(extended=True)`** walkthrough (parsed → analyzed → optimized → physical); rule list; **Tungsten**; **AQE** |
| [05-spark-optimization-guide.md](lec04-spark-part2/05-spark-optimization-guide.md) | High/shared/low programmer concern: **UDFs vs built-ins**, caching rules, **gzip trap**, explicit schema, **coalesce vs repartition** + partition sizing, filter early, **broadcast joins**, **Spark UI & REST profiling** |

## Lecture 05: Parallel Algorithm Design
| File | Contents |
|---|---|
| [01-four-steps-and-profiling.md](lec05-algorithm-design/01-four-steps-and-profiling.md) | Why parallel programming is hard; **four steps**; five-step application creation; **profile first / hotspots** (perf, gprof, VTune); **NBody code & profile** |
| [02-decomposition-task-vs-data.md](lec05-algorithm-design/02-decomposition-task-vs-data.md) | Five design concerns; who decomposes; **task vs data decomposition** + guidelines; 1D/2D/3D & recursive patterns; **row/column/block grids**; **ghost cells**; NBody (all-pairs, Barnes-Hut); **car-crash FEM** |
| [03-dependence-theory.md](lec05-algorithm-design/03-dependence-theory.md) | Dependences & correctness; **sequential consistency**; independent statements; **true/anti/output (RAW/WAR/WAW)**; examples; **dependence graphs**; **IN/OUT sets**; renaming to remove name deps |
| [04-loop-dependences-and-removal.md](lec05-algorithm-design/04-loop-dependences-and-removal.md) | **DOALL**; **loop-carried** true/anti/output; detection; **3-rule rule of thumb**; Examples 1–6 (incl. `restrict`, indirect indexing, 2D stencil); removal: **privatization, temp array/fission, loop alignment/peeling, wavefront**; prefix scan can't be fixed |
| [05-nbody-case-study-and-summary.md](lec05-algorithm-design/05-nbody-case-study-and-summary.md) | Which NBody candidates are parallelisable and why; **Lecture 05 summary**; bibliography |

## Lecture 06: Base Parallel Programming Models
| File | Contents |
|---|---|
| [01-car-crash-and-parallel-101.md](lec06-programming-models/01-car-crash-and-parallel-101.md) | Car-crash FEM recap; **sequential pseudo-code**; "parallel computing 101" & three key questions; shared-memory target (NUMA); E/T decomposition |
| [02-shared-memory-model-pthreads-openmp.md](lec06-programming-models/02-shared-memory-model-pthreads-openmp.md) | **Shared-memory model** (implicit comm, explicit sync; manual/guided/automatic); challenges; **pthreads**; **OpenMP `parallel for reduction`**; parallel crash sim & **why synchronize** |
| [03-distributed-shared-memory-dsm.md](lec06-programming-models/03-distributed-shared-memory-dsm.md) | Shared memory on distributed hardware: **virtually shared memory / DSM**, pros/cons (false sharing 4 KB vs 64 B), why MPI is preferred |
| [04-message-passing-and-hybrid.md](lec06-programming-models/04-message-passing-and-hybrid.md) | **Message-passing model** (MPI vs frameworks like Spark); **ghost-cell exchange** pseudo-code; **programming model × architecture matrix**; **hybrid** (MPI + threads) |

## Lecture 07: Parallel Performance
| File | Contents |
|---|---|
| [01-performance-metrics-speedup-efficiency-cost.md](lec07-performance/01-performance-metrics-speedup-efficiency-cost.md) | What performance is; expectation loss; embarrassingly parallel; **scalability**; **speedup, efficiency, cost**, cost-optimality |
| [02-amdahls-law.md](lec07-performance/02-amdahls-law.md) | **Amdahl's law** derivation, curves, **f=0.1 example (4.7×)**, strong scaling, 1/f bound |
| [03-gustafson-barsis-law.md](lec07-performance/03-gustafson-barsis-law.md) | **Gustafson-Barsis** derivation, **64-proc example (60.85)**, weak scaling, **Amdahl vs Gustafson** table |
| [04-work-span-model-brents-lemma.md](lec07-performance/04-work-span-model-brents-lemma.md) | **DAG model**, greedy scheduler; **work & span laws**; series/parallel composition; 7-task example; **Brent's lemma**; "**Amdahl was an optimist**"; **parallel slack** |
| [05-task-parallel-analysis-fib-quicksort.md](lec07-performance/05-task-parallel-analysis-fib-quicksort.md) | **OpenMP tasks** (`task`, `shared`, `firstprivate`, `taskwait`); **fib(4)** work/span; **quicksort** analysis; Cilkview; **work/span/parallelism table** of algorithms |
| [06-speedup-types-superlinear.md](lec07-performance/06-speedup-types-superlinear.md) | **Linear / sublinear / superlinear** speedup; causes (cache effects, search pruning) |

## Cross-cutting
| File | Contents |
|---|---|
| [formulas-cheatsheet.md](formulas-cheatsheet.md) | Every formula and key number (Amdahl, Gustafson, work-span, Brent, BSP, dependence tests, PageRank, latencies, Spark tuning, grading) |

---

## Keyword lookup
| Keyword | File(s) |
|---|---|
| Actions (Spark) | [lec03/10](lec03-data-parallel-mapreduce-spark/10-spark-rdd-api-transformations-actions.md), [lec04/03 (DataFrame)](lec04-spark-part2/03-dataframe-api-spark-sql.md) |
| Adaptive Query Execution (AQE) | [lec04/04](lec04-spark-part2/04-catalyst-optimizer-explain-plans.md#adaptive-query-execution-aqe), [lec04/05](lec04-spark-part2/05-spark-optimization-guide.md) |
| Amdahl's law | [lec07/02](lec07-performance/02-amdahls-law.md), [lec07/04 "optimist"](lec07-performance/04-work-span-model-brents-lemma.md) |
| Anti-dependence (WAR) | [lec05/03](lec05-algorithm-design/03-dependence-theory.md), [lec05/04](lec05-algorithm-design/04-loop-dependences-and-removal.md) |
| Anti-diagonal / wavefront | [lec05/04](lec05-algorithm-design/04-loop-dependences-and-removal.md#4-wavefront--anti-diagonal-traversal-for-example-6s-stencil) |
| Apple M4 / SoC / unified memory | [lec02/02](lec02-architectures/02-mimd-shared-memory-uma-smp-soc-gpu.md) |
| Barnes-Hut | [lec05/02](lec05-algorithm-design/02-decomposition-task-vs-data.md) |
| Barrier | [lec03/05 BSP](lec03-data-parallel-mapreduce-spark/05-bsp-model.md), [lec06/02 OpenMP implicit barrier](lec06-programming-models/02-shared-memory-model-pthreads-openmp.md) |
| Block / cyclic / block-cyclic | [lec03/04](lec03-data-parallel-mapreduce-spark/04-data-decomposition-block-cyclic.md) |
| Brent's lemma | [lec07/04](lec07-performance/04-work-span-model-brents-lemma.md) |
| Broadcast join | [lec04/05](lec04-spark-part2/05-spark-optimization-guide.md), [lec04/04](lec04-spark-part2/04-catalyst-optimizer-explain-plans.md) |
| BSP | [lec03/05](lec03-data-parallel-mapreduce-spark/05-bsp-model.md) |
| cache() / persist() / storage levels | [lec03/11](lec03-data-parallel-mapreduce-spark/11-spark-dag-stages-caching-fault-tolerance.md), [lec04/05](lec04-spark-part2/05-spark-optimization-guide.md) |
| Car crash simulation (FEM) | [lec05/02](lec05-algorithm-design/02-decomposition-task-vs-data.md), [lec06/01](lec06-programming-models/01-car-crash-and-parallel-101.md), [lec06/04](lec06-programming-models/04-message-passing-and-hybrid.md) |
| Catalyst optimizer / explain() | [lec04/04](lec04-spark-part2/04-catalyst-optimizer-explain-plans.md) |
| coalesce vs repartition | [lec04/05](lec04-spark-part2/05-spark-optimization-guide.md) |
| Combiner | [lec03/07](lec03-data-parallel-mapreduce-spark/07-mapreduce-example-shuffle-combiner.md) |
| Concurrency vs parallelism | [lec01/01](lec01-why-parallel/01-definitions-concurrency-vs-parallelism.md) |
| Cost (C = p·Tp), cost-optimal | [lec07/01](lec07-performance/01-performance-metrics-speedup-efficiency-cost.md) |
| Critical path / span | [lec07/04](lec07-performance/04-work-span-model-brents-lemma.md) |
| DAG (Spark) | [lec03/11](lec03-data-parallel-mapreduce-spark/11-spark-dag-stages-caching-fault-tolerance.md), [lec04/02](lec04-spark-part2/02-spark-execution-model-lineage-dependencies.md) |
| DAG (computation model) | [lec07/04](lec07-performance/04-work-span-model-brents-lemma.md) |
| DataFrame / Dataset / Spark SQL | [lec04/03](lec04-spark-part2/03-dataframe-api-spark-sql.md), [lec03/12](lec03-data-parallel-mapreduce-spark/12-dataframe-sql-intro-and-takeaways.md) |
| Data decomposition / domain decomposition | [lec05/02](lec05-algorithm-design/02-decomposition-task-vs-data.md), [lec03/04](lec03-data-parallel-mapreduce-spark/04-data-decomposition-block-cyclic.md) |
| Dependence graph, IN/OUT sets | [lec05/03](lec05-algorithm-design/03-dependence-theory.md) |
| DOALL / parallel for | [lec05/04](lec05-algorithm-design/04-loop-dependences-and-removal.md), [lec06/02](lec06-programming-models/02-shared-memory-model-pthreads-openmp.md) |
| DSM / virtually shared memory | [lec06/03](lec06-programming-models/03-distributed-shared-memory-dsm.md) |
| Efficiency | [lec07/01](lec07-performance/01-performance-metrics-speedup-efficiency-cost.md), [lec07/02](lec07-performance/02-amdahls-law.md) |
| Embarrassingly parallel | [lec03/01](lec03-data-parallel-mapreduce-spark/01-data-parallel-thinking.md), [lec07/01](lec07-performance/01-performance-metrics-speedup-efficiency-cost.md) |
| Evaluation / grading / projects | [lec01/04](lec01-why-parallel/04-course-info-evaluation.md) |
| Fault tolerance (MapReduce) | [lec03/08](lec03-data-parallel-mapreduce-spark/08-mapreduce-fault-tolerance-limitations.md) |
| Fault tolerance (Spark lineage) | [lec03/11](lec03-data-parallel-mapreduce-spark/11-spark-dag-stages-caching-fault-tolerance.md), [lec04/02](lec04-spark-part2/02-spark-execution-model-lineage-dependencies.md) |
| fib() OpenMP tasks | [lec07/05](lec07-performance/05-task-parallel-analysis-fib-quicksort.md) |
| firstprivate / shared / taskwait | [lec07/05](lec07-performance/05-task-parallel-analysis-fib-quicksort.md) |
| Flynn's taxonomy (SISD/SIMD/MISD/MIMD) | [lec02/01](lec02-architectures/01-flynn-taxonomy-sisd-simd-misd.md) |
| Flow / true dependence (RAW) | [lec05/03](lec05-algorithm-design/03-dependence-theory.md) |
| Ghost cells / halo | [lec05/02](lec05-algorithm-design/02-decomposition-task-vs-data.md), [lec06/04](lec06-programming-models/04-message-passing-and-hybrid.md) |
| GPU / SIMT / warp / SM | [lec01/03](lec01-why-parallel/03-applications-and-hardware-landscape.md), [lec02/01](lec02-architectures/01-flynn-taxonomy-sisd-simd-misd.md), [lec02/02](lec02-architectures/02-mimd-shared-memory-uma-smp-soc-gpu.md) |
| groupByKey vs reduceByKey | [lec03/10](lec03-data-parallel-mapreduce-spark/10-spark-rdd-api-transformations-actions.md), [lec04/02](lec04-spark-part2/02-spark-execution-model-lineage-dependencies.md) |
| Gustafson-Barsis | [lec07/03](lec07-performance/03-gustafson-barsis-law.md) |
| gzip not splittable | [lec04/05](lec04-spark-part2/05-spark-optimization-guide.md) |
| Hadoop / HDFS | [lec03/06](lec03-data-parallel-mapreduce-spark/06-mapreduce-model-and-architecture.md), [lec03/08](lec03-data-parallel-mapreduce-spark/08-mapreduce-fault-tolerance-limitations.md) |
| Hotspots / profiling (perf, gprof, VTune) | [lec05/01](lec05-algorithm-design/01-four-steps-and-profiling.md) |
| Hybrid programming (MPI + OpenMP) | [lec06/04](lec06-programming-models/04-message-passing-and-hybrid.md) |
| ILP / power / memory wall | [lec01/02](lec01-why-parallel/02-three-walls-and-multicore.md) |
| inferSchema / explicit schema | [lec04/05](lec04-spark-part2/05-spark-optimization-guide.md) |
| Interconnects (InfiniBand, NVLink, UEC) | [lec02/04](lec02-architectures/04-distributed-memory-clusters-top500.md) |
| Lazy evaluation | [lec03/09](lec03-data-parallel-mapreduce-spark/09-spark-motivation-rdd-architecture.md), [lec03/10](lec03-data-parallel-mapreduce-spark/10-spark-rdd-api-transformations-actions.md) |
| Lineage | [lec03/09](lec03-data-parallel-mapreduce-spark/09-spark-motivation-rdd-architecture.md), [lec03/11](lec03-data-parallel-mapreduce-spark/11-spark-dag-stages-caching-fault-tolerance.md), [lec04/02](lec04-spark-part2/02-spark-execution-model-lineage-dependencies.md) |
| Log analysis (Spark) | [lec03/11 console logs](lec03-data-parallel-mapreduce-spark/11-spark-dag-stages-caching-fault-tolerance.md), [lec04/03 Apache access logs](lec04-spark-part2/03-dataframe-api-spark-sql.md) |
| Loop alignment / peeling / fission | [lec05/04](lec05-algorithm-design/04-loop-dependences-and-removal.md) |
| Loop-carried dependence | [lec05/04](lec05-algorithm-design/04-loop-dependences-and-removal.md) |
| Map pattern / map fusion | [lec03/02](lec03-data-parallel-mapreduce-spark/02-map-pattern-and-fusion.md) |
| MapReduce | [lec03/06](lec03-data-parallel-mapreduce-spark/06-mapreduce-model-and-architecture.md), [lec03/07](lec03-data-parallel-mapreduce-spark/07-mapreduce-example-shuffle-combiner.md), [lec03/08](lec03-data-parallel-mapreduce-spark/08-mapreduce-fault-tolerance-limitations.md) |
| Memory hierarchy latencies | [lec01/03](lec01-why-parallel/03-applications-and-hardware-landscape.md), [formulas](formulas-cheatsheet.md) |
| MESI / cache coherence | [lec02/02](lec02-architectures/02-mimd-shared-memory-uma-smp-soc-gpu.md) |
| Message passing / MPI | [lec06/04](lec06-programming-models/04-message-passing-and-hybrid.md), [lec02/04](lec02-architectures/04-distributed-memory-clusters-top500.md) |
| Moore's law / free lunch | [lec01/02](lec01-why-parallel/02-three-walls-and-multicore.md) |
| Narrow vs wide dependencies | [lec04/02](lec04-spark-part2/02-spark-execution-model-lineage-dependencies.md), [lec03/11](lec03-data-parallel-mapreduce-spark/11-spark-dag-stages-caching-fault-tolerance.md) |
| NBody | [lec05/01](lec05-algorithm-design/01-four-steps-and-profiling.md), [lec05/02](lec05-algorithm-design/02-decomposition-task-vs-data.md), [lec05/05](lec05-algorithm-design/05-nbody-case-study-and-summary.md) |
| NUMA | [lec02/03](lec02-architectures/03-numa.md), [lec06/01](lec06-programming-models/01-car-crash-and-parallel-101.md) |
| OpenMP | [lec06/02](lec06-programming-models/02-shared-memory-model-pthreads-openmp.md), [lec07/05](lec07-performance/05-task-parallel-analysis-fib-quicksort.md) |
| Output dependence (WAW) | [lec05/03](lec05-algorithm-design/03-dependence-theory.md) |
| PageRank | [lec04/01](lec04-spark-part2/01-pagerank-example.md) |
| Pair RDD / join / mapValues | [lec03/10](lec03-data-parallel-mapreduce-spark/10-spark-rdd-api-transformations-actions.md) |
| Parallel slack / parallelism T1/T∞ | [lec07/04](lec07-performance/04-work-span-model-brents-lemma.md) |
| Partitioning (Spark), partitionBy | [lec04/01](lec04-spark-part2/01-pagerank-example.md), [lec04/05](lec04-spark-part2/05-spark-optimization-guide.md) |
| Predicate pushdown / projection pruning | [lec04/04](lec04-spark-part2/04-catalyst-optimizer-explain-plans.md) |
| Prefix scan | [lec05/04](lec05-algorithm-design/04-loop-dependences-and-removal.md) |
| Privatization | [lec05/04](lec05-algorithm-design/04-loop-dependences-and-removal.md) |
| pthreads | [lec06/02](lec06-programming-models/02-shared-memory-model-pthreads-openmp.md) |
| Quicksort (parallel) | [lec07/05](lec07-performance/05-task-parallel-analysis-fib-quicksort.md) |
| RDD | [lec03/09](lec03-data-parallel-mapreduce-spark/09-spark-motivation-rdd-architecture.md), [lec03/10](lec03-data-parallel-mapreduce-spark/10-spark-rdd-api-transformations-actions.md), [lec04/02](lec04-spark-part2/02-spark-execution-model-lineage-dependencies.md) |
| RDD vs DataFrame | [lec04/03](lec04-spark-part2/03-dataframe-api-spark-sql.md) |
| Reduction | [lec03/03](lec03-data-parallel-mapreduce-spark/03-reduction-pattern.md), [lec06/02](lec06-programming-models/02-shared-memory-model-pthreads-openmp.md) |
| restrict (C) / aliasing | [lec05/04](lec05-algorithm-design/04-loop-dependences-and-removal.md) |
| Scalability (strong / weak scaling) | [lec07/01](lec07-performance/01-performance-metrics-speedup-efficiency-cost.md), [lec07/02](lec07-performance/02-amdahls-law.md), [lec07/03](lec07-performance/03-gustafson-barsis-law.md) |
| Sequential consistency | [lec05/03](lec05-algorithm-design/03-dependence-theory.md) |
| Shared-memory model | [lec06/02](lec06-programming-models/02-shared-memory-model-pthreads-openmp.md) |
| Shuffle | [lec03/07](lec03-data-parallel-mapreduce-spark/07-mapreduce-example-shuffle-combiner.md), [lec04/02](lec04-spark-part2/02-spark-execution-model-lineage-dependencies.md), [lec04/05](lec04-spark-part2/05-spark-optimization-guide.md) |
| SIMD (SSE/AVX/SVE) | [lec02/01](lec02-architectures/01-flynn-taxonomy-sisd-simd-misd.md) |
| Spark architecture (driver/executor/cluster manager) | [lec03/09](lec03-data-parallel-mapreduce-spark/09-spark-motivation-rdd-architecture.md), [lec04/02](lec04-spark-part2/02-spark-execution-model-lineage-dependencies.md) |
| Spark UI / REST profiling | [lec04/05](lec04-spark-part2/05-spark-optimization-guide.md) |
| Speedup (linear/sub/superlinear) | [lec07/01](lec07-performance/01-performance-metrics-speedup-efficiency-cost.md), [lec07/06](lec07-performance/06-speedup-types-superlinear.md) |
| Stages & tasks | [lec03/11](lec03-data-parallel-mapreduce-spark/11-spark-dag-stages-caching-fault-tolerance.md), [lec04/02](lec04-spark-part2/02-spark-execution-model-lineage-dependencies.md) |
| Stencil (2D) | [lec05/02](lec05-algorithm-design/02-decomposition-task-vs-data.md), [lec05/04](lec05-algorithm-design/04-loop-dependences-and-removal.md) |
| Task decomposition | [lec05/02](lec05-algorithm-design/02-decomposition-task-vs-data.md) |
| Top500 / exascale | [lec02/04](lec02-architectures/04-distributed-memory-clusters-top500.md) |
| Transformations (Spark) | [lec03/10](lec03-data-parallel-mapreduce-spark/10-spark-rdd-api-transformations-actions.md) |
| Tungsten / whole-stage codegen | [lec04/04](lec04-spark-part2/04-catalyst-optimizer-explain-plans.md) |
| UDFs | [lec04/05](lec04-spark-part2/05-spark-optimization-guide.md) |
| UMA / SMP | [lec02/02](lec02-architectures/02-mimd-shared-memory-uma-smp-soc-gpu.md) |
| Word count | [lec03/06](lec03-data-parallel-mapreduce-spark/06-mapreduce-model-and-architecture.md), [lec03/10](lec03-data-parallel-mapreduce-spark/10-spark-rdd-api-transformations-actions.md) |
| Work / span / work law / span law | [lec07/04](lec07-performance/04-work-span-model-brents-lemma.md) |

---

### Notes on fidelity
- The content follows the slides. Where a slide asked a question without answering it on the page, or had an obvious typo, the file says so explicitly ("answer not on slide", "slide prints …").
- Content that appears only in figures (Amdahl formula, charts, profiler screenshot, PageRank graph, stencil diagram) was read from the rendered slides and transcribed in text.
- Lecture 03 Part IV (DataFrame & SQL) is duplicated in Lecture 04. It is stored once, in the Lec04 files.
