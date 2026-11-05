# M2 development evidence (2026-11-05)

Raw evidence behind `experiments/LOG.md` → Observations → M2. **None of it is a measurement**: no run-id, uncommitted
code on top of `93a62bf`, and any times in these files are Spark INFO log lines, not profiler output. `<shard>` stands for
the main checkout path; nothing else is edited.

| File | What | How it was produced |
|---|---|---|
| `plan-formatted-local-2025-01.txt` | `temporalDemand(zones).explain("formatted")`, pre-execution (`isFinalPlan=false`) | Temporary line in `writeTemporalDemand`, removed afterwards; `make run ARGS="--input <shard>/data --output output --years 2025-01"` from the worktree, run by Claude |
| `console-local-2025-01.txt` | M1 and M2 tables printed by that run, plus Spark/Java version | Same run |
| `console-docker-2025-01.txt` | M1 and M2 tables from Docker, plus the 2 granted executors | `make jar` in the worktree, jar copied to `<shard>/build/libs/`, `spark-submit` from `<shard>` (2 workers), run by Claude |
| `console-local-2025.txt` | M2 table for the full year | Run by the user, transcribed from the pasted output |
| `spark-info-stages-2025-01.txt` | Stage task counts, shuffle fetch sizes and INFO durations, local and Docker | `grep` over the two run logs above |
| `local-vs-docker-diff-2025-01.txt` | Cell-by-cell diff of the two 2025-01 M2 outputs | `spark-sql` in the master container; query and verbatim result inside |
| `javap-d7.txt` | Bytecode evidence for decision D7 (`timestamp_diff` vs NTZ subtraction) | `javap` against the Spark 4.2.1 jars in the Gradle cache |
