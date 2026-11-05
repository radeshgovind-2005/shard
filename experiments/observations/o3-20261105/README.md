# O3 development evidence (2026-11-05)

Raw evidence behind `experiments/LOG.md` → Observations → O3. **None of it is a measurement.** No run-id is in LOG →
Runs, the code is uncommitted on top of `361b669`, and the plan and stage files come from throwaway profiled runs
whose run folders live in a scratchpad, not in `experiments/runs/`. `<shard>` is the main checkout, and `<scratchpad>`
is a temporary folder outside the repo.

| File | What | How it was produced |
|---|---|---|
| `console-local-2025-01.txt` | M1 and O3 tables, plus the profiler summary line | `make run ARGS="--input <shard>/data --output output --years 2025-01 --ops m1,o3 --run-id local-20261105-91 --runs-dir <scratchpad>/runs"` |
| `plan-o3-final-local-2025-01.txt` | Post-AQE physical plan of the O3 write (`isFinalPlan=true`) | `plans/o3.txt` of that run folder |
| `stages-local-2025-01.json` | Per-stage tasks, shuffle bytes and task skew (M1 and O3 blocks) | `stages.json` of that run folder |
| `console-local-2025.txt` | M1 and O3 tables for the full year | `make run ARGS="--input <shard>/data --output output-2025 --years 2025 --ops m1,o3 --run-id local-20261105-92 --runs-dir <scratchpad>/runs"` |
| `plan-o3-final-local-2025.txt` | Post-AQE plan at full-year scale (does the self-join stay a SortMergeJoin?) | `plans/o3.txt` of that run folder |
| `stages-local-2025.json` | Stages at full-year scale | `stages.json` of that run folder |
| `console-docker-2025-01.txt` | M1 and O3 tables from Docker (2 workers × 2 cores) | The command goes in the file's header |
| `out-of-range-trips.txt` | Kept trips whose pickup is outside the `--years` range: the expected gap between M1 `kept_trips` and Σ `trip_count` (D24) | A one-off `spark-sql` query; the query goes in the file's header |

_All files pending (the user's runs)._
