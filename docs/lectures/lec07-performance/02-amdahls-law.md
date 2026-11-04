# Amdahl's Law (Fixed-Size Speedup / Strong Scaling)

> **Source:** `ComPar-Lec07.pdf`, slides 10–19 · **Back to:** [index](../index.md)
> Original paper: http://inst.eecs.berkeley.edu/~n252/sp07/Papers/Amdahl.pdf

## Motivation
- We want to **solve the same problem faster**, which means **reducing execution time**.
- The problem size is **fixed**. As processors are added, the parallel part shrinks but the serial part does not, so **the serial fraction of the execution time grows**:

| Processors | Serial work as a share of execution time (slide example, f = 0.16) |
|---|---|
| T₁ | ≈ 16% |
| T₂ | ≈ 25% |
| T₄ | ≈ 40% |
| T₈ | ≈ 60% |

## Derivation
- Let **f** be the fraction of the program that is **sequential**, so **(1 − f)** is the fraction that can be parallelized.
- Let T₁ be the execution time on 1 processor and Tₚ the execution time on p processors.

```
Tp = f·T1 + (1 − f)·T1 / p
```
(Slide 14 picture with f = 0.16, 1 − f = 0.84: the f·T₁ block stays the same while (1−f)·T₁ is split into /2, /4, /8.)

## Amdahl's law
```
Sp ≤ T1 / Tp = T1 / ( f·T1 + (1−f)·T1/p )

Sp ≤ 1 / ( f + (1−f)/p )

S(p→∞) ≤ 1 / f
```

**Speedup curves (slide 15)** for parallel portion 50% / 75% / 90% / 95% (f = 0.50 / 0.25 / 0.10 / 0.05): the speedups saturate at **2 / 4 / 10 / 20** no matter how many processors (up to 65,536) are added.

**Maximal speedup & efficiency (slide 17)** for serial fraction 0.1% / 1% / 10% / 30% / 50%, from 1 to 2048 workers:
- Speedup plateaus around **672** (0.1%, at 2048 workers), **100** (1%), **10** (10%), **3** (30%), and **2** (50%).
- **Efficiency = Sₚ / p** falls toward 0% as workers increase. Even with 0.1% serial, efficiency drops to ~33% at 2048 workers.

## Worked example
**If 90% of the computation can be parallelized, what is the max speedup achievable using 8 processors?**
```
f = 10% = 0.1
S(8)  ≤ 1 / (0.1 + (1−0.1)/8)  = 1 / 0.2125 ≈ 4.7
S(6)  ≤ 1 / (0.1 + 0.9/6)      = 1 / 0.25   = 4
S(16) ≤ 1 / (0.1 + 0.9/16)     = 1 / 0.15625 = 6.4
S(∞)  ≤ 1 / 0.1 = 10
```

## Amdahl's law and scalability
- **When does Amdahl's law apply?**
  - When the **problem size is fixed**
  - **Strong scaling**: p → ∞, Sₚ = S∞ → **1 / f**
  - **The speedup bound is determined by the degree of sequential execution time in the computation, not by the number of processors!**
  - That is bad news. Why? Even a small serial fraction caps speedup (f = 5% → max 20×), so adding processors brings diminishing returns.
  - **Perfect efficiency is hard to achieve.**

## Related
- [Gustafson-Barsis (the counter-view: scaled speedup)](03-gustafson-barsis-law.md)
- ["Amdahl was an optimist!": work-span is tighter](04-work-span-model-brents-lemma.md#amdahl-was-an-optimist)
- [Profile first (parallelise the hot spots)](../lec05-algorithm-design/01-four-steps-and-profiling.md)
