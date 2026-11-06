# Scalability study, 2026-11-06 (derived from run-ids)

Derived from the profiled runs `docker-20261106-37` … `-79` and `local-20261106-27` … `-39`. All are sha `3cc3a25` (the final pipeline: baseline + OPT-2), all ops, `--years 2025`, default `spark.*` settings, on battery with Low Power Mode off. LOG → Runs and "How the scalability runs were run" describe the protocol; LOG → Scalability has the results. Event logs stay in the gitignored `experiments/runs/<id>/eventlog/`, and only `records-read.txt` needed them. Every other number comes from `run.json` / `stages.json` and the timings in `bench/elapsed.tsv`. The first line of each derived file is the command that produced it (run from the repo root).

Sets used:

| study | warm-ups (discarded) | measured |
|---|---|---|
| dataset scale, Docker 2 workers | 37 | fraction 0.1: 38, 42, 46, 50, 76 · 0.2: 39, 43, 47, 51, 77 · 0.5: 40, 44, 48, 52, 78 · 1.0: 41, 45, 49, 53, 79 |
| cluster size, Docker, full year | 54 (3 w), 58 and 70 (4 w), 62 (5 w), 66 and 74 (2 w) | 2 workers: 67, 68, 69, 75 · 3: 55, 56, 57 · 4: 59, 60, 61, 71, 72, 73 · 5: 63, 64, 65 |
| `local[n]` supplement, full year | local 27 (`local[*]`) | `local[2]`: 28, 32, 36 · `local[4]`: 29, 33, 37 · `local[8]`: 30, 34, 38 · `local[*]`: 31, 35, 39 |

Order of execution: 37–49 (scale rounds 1–3), 50–53 (round 4), 54–57 (3 workers), 58–61 (4), 62–65 (5), 66–69 (2), 70–73 (second 4-worker block), 74–75 (the 2-worker restore step, see below), 76–79 (scale round 5, on that same 2-worker cluster), then local 27–39.

**Runs 74 and 75** came from `cluster.sh 2 74 0`, which was meant only to restore 2 workers. macOS (BSD) `seq 1 0` prints `1 0`, not nothing, so the loop ran two full-year runs. They ran on a fresh 2-worker cluster with the same protocol as the other blocks: 74 counts as the warm-up and 75 is used in T₂.

| File | What | Produced by |
|---|---|---|
| `scaling.txt` | cluster-size and `local[n]` tables (T, S, ideal, E, C, f_est, executor CPU, CPU ÷ T, single-task stage time, Amdahl least-squares and serial-stage fits), the T₂ sensitivity, and the dataset-scale table with its linear fit | `scripts/scaling.py` (also writes `experiments/analysis/speedup.csv` and `scale.csv`) |
| `summary-scale.txt`, `summary-cluster.txt`, `summary-local.txt` | per configuration: total and spark-submit elapsed, the time outside the profiled blocks, per-op wall / stage time / tasks / shuffle / spill / executor run time / slot utilisation, GC, CPU ÷ run, every scan stage (tasks, duration, skew, median task), the M1 scan (tasks, waves, CPU), and the time in stages with fewer tasks than cores | `scripts/scal.py` |
| `records-read.txt` | Input Metrics records read per scan stage, from the zstd event logs: 48,722,602 per trip scan at every fraction and every worker count | `scripts/records.py` |
| `bench/elapsed.tsv` | run-id, label, workers (or the local master), fraction, exit code, the whole `spark-submit` / `make run` time (s), the total of the profiled blocks (s) | the driver scripts |
| `bench/state-*.txt` | `pmset` Low Power Mode, the battery line, `uptime` and the alive workers / cores before every run | `scripts/bench.sh`, `scripts/local.sh` |
| `bench/cluster-up.txt`, `bench/dstats-w*.txt` | alive workers / cores / memory after each fresh `up`; one `docker stats` sample ≈ 75 s after `up` (the 5-worker sample fell between runs) | `scripts/cluster.sh` |
| `bench/printed-tables-md5.txt` | md5 of each run's printed result tables: identical within each fraction, and every full-year run (Docker at any N, any `local[n]`) equals the re-measure session's `11e17578…` | `grep -E '^[\|+]' <console> \| md5` |
| `bench/roles.tsv` | run-id → "variable under test" and notes, for the LOG rows | input to `scripts/rows.py` |
| `scripts/` | the driver scripts and the analysis (Python 3 stdlib, `zstd` for the event logs) | written for this study |

The scripts carry the absolute path of the session scratchpad they ran from (`SP=…`), where the console output of each run went. That output is not kept: the printed tables are summarised by their md5, and the metrics are in `run.json`.

**Who ran what:** Claude ran every command at the user's request, one run at a time under `caffeinate -dim`, with the user's choice of power (stay on battery).
