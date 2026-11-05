# O3 development evidence (2026-11-05)

Raw evidence behind `experiments/LOG.md` → Observations → O3. **None of it is a measurement:** no run-id is in LOG →
Runs. Everything ran on `feat/o3-demand-forecasting` at `ee960bd` (O3 plus main `9b4cc8a`), all of it by Claude. The
plan and stage files come from throwaway profiled runs whose run folders live in a scratchpad, not in
`experiments/runs/`.

Placeholders used below:
- `<shard>`: the main checkout
- `<scratchpad>`: a temporary folder outside the repo
- `<master>`: the Docker master container

| File | What | How it was produced |
|---|---|---|
| `console-local-2025-01.txt` | M1 and O3 tables, plus the profiler summary line | `make run ARGS="--input <shard>/data --output <scratchpad>/out-2025-01 --years 2025-01 --ops m1,o3 --run-id local-20261105-93 --runs-dir <scratchpad>/runs"` |
| `plan-o3-final-local-2025-01.txt` | Post-AQE physical plan of the O3 write (`isFinalPlan=true`, with the initial plan below it): every grid join is broadcast | `plans/o3.txt` of run `local-20261105-93` |
| `stages-local-2025-01.json` | Per-stage tasks, shuffle bytes and task skew (M1 and O3 blocks) | `stages.json` of that run |
| `console-local-2025.txt` | M1 and O3 tables for the full year | Same as above with `--years 2025 --output <scratchpad>/out-2025 --run-id local-20261105-94` |
| `plan-o3-final-local-2025.txt` | Post-AQE plan at full-year scale: the self-join and the zero-fill join are SortMergeJoins | `plans/o3.txt` of run `local-20261105-94` |
| `stages-local-2025.json` | Stages at full-year scale (the five shuffles in LOG → O3) | `stages.json` of that run |
| `console-docker-2025-01.txt` | M1 and O3 tables from Docker (2 workers × 2 cores, 1024 MiB) | Command in the file's header |
| `out-of-range-trips.sql` / `.txt` | Kept trips whose pickup is outside the range (22 for 2025-01, 29 for 2025), which is exactly the gap between M1 `kept_trips` and Σ `trip_count` (D24) | `spark-sql --master local[2] -f out-of-range-trips.sql` in `<master>` |
| `local-vs-docker-o3.sql` / `local-vs-docker-diff-2025-01.txt` | Cell-by-cell comparison of the two 2025-01 outputs | The same `spark-sql` call, over a copy of the local output in `<master>:/tmp` |
| `join-size-estimates.scala` / `.txt` | Catalyst's static `sizeInBytes` for each join side (why 2025-01 broadcasts and 2025 sort-merges) | `spark-shell --master local[2] --jars /tmp/shard-o3.jar < join-size-estimates.scala` in `<master>`, with the merged jar copied there |
| `spike-breakdown.sql` / `spike-breakdown-2025.txt` | Spikes by bucket mean and by count, the DI distribution, and the top spikes in busy buckets | `spark-sql` in `<master>`, over a copy of the 2025 output |
