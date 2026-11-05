# Baseline evidence, 2026-11-05 (derived from run-ids)

Unlike the other `observations/` folders, everything here is **derived from measured, profiled runs**: `local-20261105-01` … `-07` and `docker-20261105-01` … `-04` (LOG → Runs). The event logs are gitignored, so the tables taken from them are kept here, together with the scripts that made them. Re-run any file's command from the repo root; the first line of each file is its command.

| File | What | From |
|---|---|---|
| `summary.txt` | per-run, per-operation totals (wall, stages, tasks, shuffle, spill, run time, CPU/run, GC %), then median [min–max] and spread | `run.json` |
| `attribution.txt` | per operation: median wall, its trip-scan stage, the rest; then median duration per stage id | `run.json` |
| `stages-<run>.txt` | every stage of one run: duration, tasks, shuffle, spill, CPU/run, GC %, median/max task, skew | `stages.json` |
| `eventlog-<run>.txt` | per stage: job group, tasks, max concurrent tasks, span, records read, executors; the scan's `size of files read` per SQL execution; task timelines of stage 1 (M1 scan) and 43 (O1 window); per-stage SQL metric sums (`scan time`, `sort time`, spill size, …) | `eventlog/*.zstd` (`zstd -dc`) |
| `row-groups.txt` | row groups per 2025 file (rows and compressed bytes) | Parquet footers, `spark-shell` in the Docker master |
| `scripts/` | `summarize.py`, `attrib.py`, `stages.py`, `eventlog.py`, `accums.py` (Python 3, standard library only; run with `python3 -I`) | |

The per-operation wall-clock of every run is also in `experiments/analysis/baseline-20261105-wall.csv`, for the report's charts.

**Why the event log is needed:** the profiler's `input_bytes` is wrong for Parquet. It reports about 11 KB per scan task (0.1–0.2 MiB per operation) for files of 59–78 MB, while `Records Read` is right. The real bytes read come from the scan's SQL metric `size of files read` (829,973,299 B per operation, the size of the 12 files), which only the event log has.

**Who ran what:** Claude ran every command here and the runs themselves, at the user's request ("You run them").
