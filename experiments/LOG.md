# Experiment Log

Single source of truth for every measurement in the report. Agents append here; nothing goes into the report without a run-id from this file.

## Environment

| Field | local | Docker Spark | DI cluster |
|---|---|---|---|
| Spark version | 4.2.1 (Scala 2.13), Temurin JDK 21, `local[*]` | 4.2.1 (Scala 2.13), Java 21, standalone (`spark://spark-master:7077`, client mode); image `spark:4.2.1-scala2.13-java21-ubuntu@sha256:e949a6029ea5d626153c64d5eebdf855ef4d07e3f3a5653938dd202fda84f9aa`; `docker/docker-compose.yml` | |
| Hardware / topology (workers × cores × memory) | 1 × Apple M5, 10 cores, 24 GB RAM (macOS) | N × 2 cores × 2 GB workers (`--scale spark-worker=N`; default 2) + 1 master container running the driver; OrbStack on the same M5 | |
| Non-default `spark.*` conf | none | none | |
| Dataset (years, files, size on disk) | 2025-01 – 2025-12: 12 trip files, 829,973,299 B (791.5 MiB) + `taxi_zone_lookup.csv` (12,331 B); downloaded 2026-11-04 21:17–21:19 UTC; sha256 per file in `data/MANIFEST.tsv` | same files as local, bind-mounted read-only at `/data` (master and workers) | |

## Runs

Run-id format: `<env>-<yyyymmdd>-<nn>` (e.g. `local-20261105-01`). Raw dumps in `experiments/runs/<run-id>/`.

| run-id | git sha | env | scale | variable under test | wall-clock (s) | shuffle R / W | #tasks | spill (disk) | GC % | notes |
|---|---|---|---|---|---|---|---|---|---|---|

## Hypotheses

Written by `spark-profiler`. Each needs evidence: metric + run-id.

<!--
### H1 [op] <bottleneck>
- Evidence: <metric = value> (run <id>, stage <n>); plan: <node>
- Hypothesis: <cause>
- Candidate remedy: <one change>
- Expected: <metric, direction>; upper bound ≈ <x%> of wall-clock
-->

## Optimisations

Written by `spark-optimizer` (applied) and `benchmark-runner` (measured).

<!--
### OPT-1 (H1) [op] <change>
- Status: applied, unmeasured | kept | reverted | inconclusive
- Change: <files>, toggle: <flag/conf>
- Plan: <before node> → <after node>
- Correctness: <row counts, except-diff>
- Before: <run-ids> → <median [min–max]>
- After:  <run-ids> → <median [min–max]>
- Δ wall-clock: <%>; Δ predicted metric: <value>
-->
