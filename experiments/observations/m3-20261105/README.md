# M3 development evidence (2026-11-05)

Raw evidence behind `experiments/LOG.md` → Observations → M3. **None of it is a measurement**: no run-id in LOG → Runs,
uncommitted code on top of `72f9bd0`, all run by Claude. The plan and stage files come from a throwaway profiled run whose
run folder lives in a scratchpad, not in `experiments/runs/`. `<shard>` is the main checkout, `<worktree>` the session's
worktree, `<scratchpad>` a temporary folder outside the repo.

| File | What | How it was produced |
|---|---|---|
| `console-local-2025-01.txt` | M1 and M3 tables, plus the profiler summary line | `make run ARGS="--input <shard>/data --output output --years 2025-01 --ops m1,m3 --run-id local-20261105-90 --runs-dir <scratchpad>/runs"` |
| `plan-m3-final-local-2025-01.txt` | Post-AQE physical plan of the M3 write (`isFinalPlan=true`) | `plans/m3.txt` of that run folder |
| `stages-local-2025-01.json` | Per-stage tasks, shuffle bytes, task skew (M1 and M3 blocks) | `stages.json` of that run folder |
| `console-local-2025.txt` | M1 and M3 tables for the full year | `make run ARGS="--input <shard>/data --output output-2025 --years 2025 --ops m1,m3"` |
| `console-docker-2025-01.txt` | M1 and M3 tables from Docker (2 workers × 2 cores, 1024 MiB) | Command in the file's header |
| `anomaly-breakdown-2025.txt` | Flags per month, per baseline size, σ = 0 rows, top flagged large zones, rows per borough, Manhattan's top-2 rolling shares | `spark-sql --master local[2]` in the Docker master over a copy of the 2025 M3 output; queries in the file's header |
