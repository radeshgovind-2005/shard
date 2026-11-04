# Experiment Log

Single source of truth for every measurement in the report. Agents append here; nothing goes into the report without a run-id from this file.

## Environment

| Field | local | Docker Spark | DI cluster |
|---|---|---|---|
| Spark version | 4.3.0 (Scala 2.13), Temurin JDK 21, `local[*]` | | |
| Hardware / topology (workers × cores × memory) | 1 × Apple M5, 10 cores, 24 GB RAM (macOS) | | |
| Non-default `spark.*` conf | none | | |
| Dataset (years, files, size on disk) | not downloaded yet (`make data`; versions in `data/MANIFEST.tsv`) | | |

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
