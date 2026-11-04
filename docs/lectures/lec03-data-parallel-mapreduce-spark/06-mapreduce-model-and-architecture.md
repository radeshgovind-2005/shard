# Google MapReduce: Origins, Primitives, Architecture, Execution Model

> **Source:** `ComPar-Lec03.pdf`, Part II "Data-Parallel Programming on Distributed Memory Architectures: Google MapReduce" (printed slides ~21–30, 68–69) · **Back to:** [index](../index.md)
> Bibliography: Jeffrey Dean & Sanjay Ghemawat, "MapReduce: simplified data processing on large clusters," OSDI'04 (https://www.usenix.org/legacy/publications/library/proceedings/osdi04/tech/full_papers/dean/dean.pdf). Chapters 1–2 of Jimmy Lin & Chris Dyer, *Data-Intensive Text Processing with MapReduce*, Morgan & Claypool (https://lintool.github.io/MapReduceAlgorithms/ed1n/MapReduce-algorithms.pdf).

*"The programming model that started the big-data era."*

## Origins (Dean & Ghemawat, OSDI 2004)

**The problem at Google (2003):**
- Crawl and index the entire web
- Process petabytes of logs daily
- Build inverted indexes for search
- Run on thousands of commodity machines
- Handle frequent machine failures gracefully

**The insight:** *"Most computations apply the same operation to each element of an input set, then combine results, just like map & reduce in Lisp."*

**Impact timeline:**
| Year | Event |
|---|---|
| 2004 | OSDI paper published |
| 2006 | Apache Hadoop created (open source) |
| 2008 | Yahoo runs Hadoop on 10,000 nodes |
| 2012 | Hadoop adopted by Facebook, Twitter, LinkedIn |
| 2014 | Apache Spark supersedes MapReduce for iterative workloads |

## Implementations
- MapReduce is a **programming model**.
- Google has a proprietary implementation in **C++**, with bindings in Java and Python.
- **Hadoop** is an open-source implementation in **Java**. The project was led by Yahoo, is used in production, and has a rapidly expanding software ecosystem.

## Map & Reduce primitives (inspired by functional programming: map, filter, fold)

| `map(k1, v1) → list(k2, v2)` | `reduce(k2, list(v2)) → list(v3)` |
|---|---|
| Processes each input record **independently** | Receives **all values for a given key** |
| Emits **zero or more** key-value pairs | **Aggregates/combines** into final output |
| Workers run in parallel, with **no communication** | **One reducer per unique key** (configurable) |
| Output is **intermediate** key-value pairs | Output written to **distributed storage** |

**Word count example:**
```python
# Map
# Input: "hello world hello"
# Emits: ("hello", 1), ("world", 1), ("hello", 1)

# Reduce
# Input: ("hello", [1, 1, 1])  → Output: ("hello", 3)
# Input: ("world", [1])        → Output: ("world", 1)
```

## Architecture (redrawn from Dean & Ghemawat, OSDI 2004)
```
                    User Program
               (1) fork  (1) fork  (1) fork
                        Master
         (2) assign map        (2) assign reduce
Input files      Map phase        Intermediate files       Reduce phase     Output files
 split 0 ┐                       (on local disk)
 split 1 ├─(3) read→ worker ─(4) local write→ ... ─(5) remote read→ worker ─(6) write→ output file 0
 split 2 │           worker                                          worker ─────────→ output file 1
 split 3 │           worker
 split 4 ┘
```
1. The user program **forks** the master and workers.
2. The master **assigns map tasks and reduce tasks** to idle workers.
3. Map workers **read** their input split.
4. Map workers **write** intermediate (k, v) pairs **to local disk**, partitioned per reducer.
5. Reduce workers **remote-read** their partitions from all mappers.
6. Reduce workers **write** the final output files (to the distributed FS).

## Distribute data at load time & execution model
- Input is typically **(key, value) pairs**, **partitioned for multiple mappers at load time**.
- Map and Reduce are performed by a number of processors.

## What the programmer specifies
- Programmers specify two functions:
  - `map (k, v) → <k', v'>*`
  - `reduce (k', v') → <k', v'>*`
  - All values with the same key are reduced together.
- The runtime handles everything else…
- …well, not quite. Usually programmers may also have to specify:
  - `partition (k', number of partitions) → partition for k'`
    - Often a simple hash of the key, e.g. `hash(k') mod n`
    - Divides up the key space for parallel reduce operations
  - `combine (k', v') → <k', v'>*` (see [combiner](07-mapreduce-example-shuffle-combiner.md))

## Why is MapReduce popular? (Ack: slide by Junghoon Kang)
- **Before MapReduce**, distributed computation meant answering: how to divide the workload among multiple machines? How to distribute data and program to other machines? How to schedule tasks? What happens if a task fails while running? …and more.
- **After MapReduce**: how to write the Map function? How to write the Reduce function?
- Designed for **clusters of 1,000+ commodity machines, where failures are the norm, not the exception**.

## Related
- [Worked example, shuffle phase & combiner](07-mapreduce-example-shuffle-combiner.md)
- [Fault tolerance & limitations](08-mapreduce-fault-tolerance-limitations.md)
- [BSP model](05-bsp-model.md)
