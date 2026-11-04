# Formulas & Numbers Cheat Sheet (all lectures)

> Every formula, cost model, and worked number from the 7 lectures in one place. Each row links to the full explanation. · **Back to:** [index](index.md)

## Performance metrics (Lec07)
| Quantity | Formula | Details |
|---|---|---|
| Speedup | S(p) = T₁ / Tₚ | [metrics](lec07-performance/01-performance-metrics-speedup-efficiency-cost.md) |
| Efficiency | E(p) = S(p) / p | |
| Cost | C(p) = p · Tₚ | cost-optimal ⇔ C(p) = T₁ (E = 100%) |
| Linear / sub / super-linear | S = p / S < p / S > p | [speedup types](lec07-performance/06-speedup-types-superlinear.md) |

## Amdahl (fixed size, strong scaling) (Lec07)
```
f = sequential fraction
Tp = f·T1 + (1−f)·T1/p
Sp ≤ 1 / ( f + (1−f)/p )
S∞ ≤ 1 / f
```
Example: f = 0.1 → S(6) ≤ 4, S(8) ≈ 4.7, S(16) ≤ 6.4, S(∞) = 10. · [details](lec07-performance/02-amdahls-law.md)

## Gustafson-Barsis (scaled speedup, weak scaling) (Lec07)
```
T1 = a + b   (a serial, b parallel)     TP = a + P·b
α = a/(a+b)
Sp ≤ (a + P·b)/(a + b) = α + P(1−α) = P − α(P−1)
```
Example: P = 64, α = 0.05 → S ≤ 60.85 (α = 0.1 → 57.7). · [details](lec07-performance/03-gustafson-barsis-law.md)

## Work-span / Brent (Lec07)
```
T1 = work,  T∞ = span (critical path),  parallelism = T1/T∞
Work law:  TP ≥ T1/P
Span law:  TP ≥ T∞
Lower:     TP ≥ max(T1/P, T∞)        SP ≤ T1/T∞
Brent:     TP ≤ (T1 − T∞)/P + T∞
           TP ≈ T1/P + T∞   if T∞ << T1
Slack:     TP ≈ T1/P        if T1/T∞ >> P
Series:    T1 = T1(A)+T1(B),  T∞ = T∞(A)+T∞(B)
Parallel:  T1 = T1(A)+T1(B),  T∞ = max(T∞(A), T∞(B))
```
Examples: 7-task DAG T₁=7, T∞=5 → 5 ≤ T₂ ≤ 6, max S = 1.4 (Amdahl with f=2/7 says 3.5). fib(4): T₁=17, T∞=8, parallelism 2.125. Quicksort: work O(n log n), span O(n), parallelism O(log n). · [work-span](lec07-performance/04-work-span-model-brents-lemma.md) · [fib/qsort/table](lec07-performance/05-task-parallel-analysis-fib-quicksort.md)

## BSP cost model (Lec03)
```
superstep cost = W + g·h + l
total = Σs (Ws + g·hs + l)
p = #processors, g = time per word communicated, l = barrier latency, h = max msgs sent/recv by any proc
```
Vector-sum example (12 elems, 3 workers): 2 supersteps → 2l + W₁ + W₂ + g·h, h = 2. · [details](lec03-data-parallel-mapreduce-spark/05-bsp-model.md)

## Data decomposition (Lec03)
- Block: worker i gets `[i·⌊N/P⌋, (i+1)·⌊N/P⌋)`
- Cyclic: worker i gets indices with `index mod P == i`
- Block-cyclic: blocks of size B assigned round-robin
· [details](lec03-data-parallel-mapreduce-spark/04-data-decomposition-block-cyclic.md)

## Dependence tests (Lec05)
```
OUT(S1) ∩ IN(S2)  ≠ ∅  ⇒ flow/true (RAW)   S1 δ S2
IN(S1)  ∩ OUT(S2) ≠ ∅  ⇒ anti (WAR)        S1 δ⁻¹ S2
OUT(S1) ∩ OUT(S2) ≠ ∅  ⇒ output (WAW)      S1 δ⁰ S2
```
DOALL rule of thumb: (1) shared writes only to arrays, (2) each element is assigned by ≤ 1 iteration, (3) no iteration reads elements assigned by another. · [theory](lec05-algorithm-design/03-dependence-theory.md) · [loops](lec05-algorithm-design/04-loop-dependences-and-removal.md)

## PageRank (Lec04)
```
PR(A) = (1−d) + d · Σ PR(Ti)/C(Ti),   d = 0.85
Iterative: rank = 0.15 + 0.85 × Σ contribs,  contrib = rank_p / |neighbors_p|
```
· [details](lec04-spark-part2/01-pagerank-example.md)

## Hardware: power & latency numbers (Lec01–02)
| Item | Value |
|---|---|
| Dynamic power | P ∝ C · V² · f (roughly cubic in f, since V must rise with f) |
| RAM latency | ~100 ns = 200–400 cycles |
| Typical IPC ceiling | ~3–6 |
| L1 / L2 / L3 / DRAM | ~32 KB <1 ns / ~256 KB ~4 ns / 32–768 MB ~30 ns / TBs ~100 ns |
| GPU regs / shared / L2 / HBM | ~1 cycle / ~5 cycles / ~200 cycles / 80 GB/s–3 TB/s |
| InfiniBand | ~1 µs, 400 Gb/s |
| NUMA local / remote | ~50–80 ns / ~100–200 ns (2–4× slower) |
| UMA bus scalability limit | ~16 cores |
| GPU warp | 32 threads |

## Spark tuning numbers (Lec04)
| Item | Value |
|---|---|
| Partitions | 2–3 tasks per CPU core (10 nodes × 4 cores → 80–120 partitions) |
| Partition size | ~100–200 MB |
| AQE skew threshold | partition > 5× median |
| AQE default since | Spark 3.2 (`spark.sql.adaptive.enabled`) |
| GC warning | GC time > 10% of task time |
| PageRank s/iter | Hadoop 171 · Spark 72 · Spark + partitioning 23 |
| Logistic regression s/iter | MapReduce 110 · Spark 0.96 |

## Grading (Lec01)
```
CL = 0.6·CG + 0.4·CI      (CL ≥ 8.5 for frequency)
CF = 0.6·CTP + 0.4·CL     (CTP ≥ 8.50)
```
· [details](lec01-why-parallel/04-course-info-evaluation.md)
