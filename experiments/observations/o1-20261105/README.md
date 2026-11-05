# O1 development evidence (2026-11-05)

Raw evidence behind `experiments/LOG.md` → Observations → O1. **None of it is a measurement**: no run-id in LOG → Runs,
uncommitted code on top of `361b669`, all run by Claude. The plan and stage files come from throwaway profiled runs whose run folders live
in a scratch folder (or inside the container), not in `experiments/runs/`. `<shard>` is the main checkout, `<worktree>` the session's worktree,
`<scratch>` a temporary folder outside the repo.

| File | What | How it was produced |
|---|---|---|
| `console-local-2025-01.txt` | M1 and O1 tables, plus the profiler summary line | `make run ARGS="--input <shard>/data --output output --years 2025-01 --ops m1,o1 --run-id local-20261105-91 --runs-dir <scratch>/runs"` |
| `plan-o1-final-local-2025-01.txt`, `plan-o1-final-local-2025.txt` | Post-AQE physical plan of the O1 write (`isFinalPlan=true`): the window's `Exchange` keys and AQE's coalescing | `plans/o1.txt` of that run folder (and of `local-20261105-92` for 2025) |
| `stages-local-2025-01.json`, `stages-docker-2025-01.json`, `stages-local-2025.json` | Per-stage tasks, shuffle bytes, spill, task skew (M1 and O1 blocks) | `stages.json` of `local-20261105-91`, `docker-20261105-91` (copied out of the container) and `local-20261105-92` |
| `window-task-durations-local-2025-01.txt`, `window-task-durations-local-2025.txt` | Every task of the window stage: duration, shuffle-read bytes, records (and spill for 2025) | `zstd -dc` + `jq` over `eventlog/` of the run folder (command in the file's header) |
| `console-docker-2025-01.txt` | M1 and O1 tables from Docker (2 workers × 2 cores, 1024 MiB) | Command in the file's header |
| `console-local-2025.txt` | M1 and O1 tables for the full year, plus the profiler summary line | `make run ARGS="--input <shard>/data --output output-2025 --years 2025 --ops m1,o1 --run-id local-20261105-92 --runs-dir <scratch>/runs"` |
| `bucket-diagnostics.sql` | Independent SQL re-computation of the flags, plus bucket sizes, the largest buckets, flags by distance, the heavy tail and rows per shuffle partition | Command in the file's header |
| `bucket-diagnostics-2025-01.txt`, `bucket-diagnostics-2025.txt` | Its output for one month and the full year | Same, with `--hivevar files=…2025-01.parquet` or `…2025-*.parquet` |
