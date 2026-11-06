# OPT-3 (H6): broadcast the 4-week-lag side of O3's self-join, plan and correctness runs (2026-11-06)

**These are plan and correctness runs, not measurements.** One run per configuration, on local `local[*]` (10 cores) with the Docker cluster up and idle. Their times say nothing about OPT-3's effect; `benchmark-runner` measures that. The run ids 81–84 are throwaway (runs folder outside the repo), not entries in LOG → Runs.

H5 (persist the zero-filled grid) was tried first for OPT-3 and dropped: [`../h5-attempt-20261105/`](../h5-attempt-20261105/README.md).

## Where it came from

One build: HEAD `ec653c7` (branch `perf/o3-broadcast-lag-side`) plus the uncommitted OPT-3 diff ([`uncommitted.diff`](uncommitted.diff): `git diff -- src` plus the new `forecast/LagJoinPlan.kt`). `run.json` records `ec653c7`. The diff is the code that ran: `src` didn't change after the runs. O3 only, profiled into a throwaway runs folder:

```sh
R=/private/tmp/claude-501/opt3h6-check
# 2025, off: --skip-opts opt3 (the plain self-join, D27) / on: the default (broadcast() on the lag side)
make run ARGS="--input data --output output/opt3-off-2025 --years 2025 --ops o3 --skip-opts opt3 --run-id local-20261106-81 --runs-dir $R --git-sha ec653c7"
make run ARGS="--input data --output output/opt3-on-2025 --years 2025 --ops o3 --run-id local-20261106-82 --runs-dir $R --git-sha ec653c7"
# 2025-01, off / on
make run ARGS="--input data --output output/opt3-off-2025-01 --years 2025-01 --ops o3 --skip-opts opt3 --run-id local-20261106-83 --runs-dir $R --git-sha ec653c7"
make run ARGS="--input data --output output/opt3-on-2025-01 --years 2025-01 --ops o3 --run-id local-20261106-84 --runs-dir $R --git-sha ec653c7"
```

`run.json` records `"skip_opts": "opt3"` for 81 and 83, and `""` for 82 and 84. No `spark.*` setting was changed.

| File | What |
|---|---|
| `plans/{off,on}-2025[-01]-local-20261106-8x/o3.txt` | O3's final plans (`isFinalPlan=true`), copied from `<runs-dir>/<run-id>/plans/` |
| `plan-nodes.txt` | node counts in each final plan (the `== Final Plan ==` section only, ids stripped) |
| `stages-local-20261106-8x.txt` | O3's stages and block totals from `run.json` (`scripts/stages-block.py`) |
| `broadcast-local-20261106-8x.txt` | each `BroadcastExchange`'s driver metrics from the event log (`../opt2-20261105/scripts/broadcast-size.py`); command on the `#` line |
| `console-local-20261106-8x.txt` | the tables each run printed |
| `correctness.txt`, `scripts/compare-opt3.scala`, `run-pairs.scala`, `control-opt3.scala` | off vs on at both scales, the command, and the negative control |
| `heap-check.txt`, `scripts/heap-check.scala` | O3 on 2025 in one 1 GiB JVM (Docker master, `local[2]`), with and without the hint |

The 2025 `off` plan (81) is identical to H5's reference `off` plan (`../h5-attempt-20261105/plans/off-2025-local-20261105-89/o3.txt`) once the expression ids are stripped.

## The plans (final plans)

| | 2025 off (81) | 2025 on (82) | 2025-01 off (83) = on (84) |
|---|---|---|---|
| **4-week-lag self-join** | `SortMergeJoin LeftOuter` on (`PULocationID`, `hour_index`) | **`BroadcastHashJoin LeftOuter, BuildRight`** | `BroadcastHashJoin LeftOuter, BuildRight` (Catalyst already broadcasts) |
| current side of the self-join | `Exchange hashpartitioning(PULocationID, hour_index, 200)` after the window (stage 12) | **no exchange**: the join runs in the window's stage | no exchange |
| lag side | `Exchange hashpartitioning(PULocationID, hour_index, 200)` + `Sort` | `BroadcastExchange HashedRelationBroadcastMode` (keys packed into one long: `PULocationID << 32 \| hour_index`) | `BroadcastExchange` |
| zero-fill join (grid ⋈ trip counts) | `SortMergeJoin LeftOuter` **×2** (H5's duplicate) | **×2, still** (one copy feeds the window, the other the broadcast) | `BroadcastHashJoin` ×2 |
| `ReusedExchange` | 2 | 2 | 2 |
| `SortMergeJoin` / `Sort` / hash `Exchange` | 3 / 7 / 7 | 2 / 5 / 5 | 0 / 1 / 2 |
| `BroadcastExchange` / `BroadcastHashJoin` | 2 / 1 | 3 / 2 | 6 / 4 |

On 2025-01 the hint changes nothing physical: the plain plan already broadcasts the lag side (its estimate is 8.7 % under the threshold, LOG → Observations → O3), and the two final plans have the same nodes, stages, tasks and shuffle bytes.

## Stages (O3 block, `stages-*.txt`)

| 2025 | off (81) | on (82) |
|---|---|---|
| stages / tasks | 8 / 76 | **7 / 65** |
| trip scan + partial aggregate; grid | 3 (12 tasks, 16,731,216 B written); 4 (10, 17,798,867 B) | same |
| zero-fill join, current side | stage 8: reads 34,530,083 B, writes the window exchange 36,538,179 B | stage 7: same bytes |
| zero-fill join, lag side | stage 7: reads 34,530,083 B, writes the lag exchange **25,672,198 B** | stage 8: reads 34,530,083 B, writes **0** (collected for the broadcast) |
| window | stage 12: reads 36,538,179 B, writes the re-shuffle **67,329,295 B** | stage 12: window + **broadcast self-join** + zone join + write, reads 36,538,179 B |
| self-join | stage 18 (11 tasks): `SortMergeJoin` + zone join + write, reads 93,001,493 B | **gone** |
| shuffle write / read, block | 164,069,755 / 198,599,838 B | **71,068,262 / 105,598,345 B** (−93,001,493 B, the two self-join exchanges) |
| output bytes (same 2,303,880 rows, bit-identical) | 23,986,074 B (11 files) | 12,305,510 B (10 files) |
| spill | 0 | 0 |

The output is half the size on disk. The final stage now writes the rows in the window's order (bucket by bucket, where `hist_mean_trips` and `hist_var_trips` repeat) instead of the sort-merge join's (`PULocationID`, `hour_index`) order, which probably encodes better. Not verified.

## The broadcast (`broadcast-local-20261106-82.txt`)

- **2,303,880 rows** (the shifted grid: `PULocationID`, `hour_index`, `lag_4w_trip_count`), as Catalyst can't know: its estimate was 122,444,757 B.
- `data size` **268,435,424 B (256 MiB)**. As OPT-2 found, that's the hashed relation's allocation, not its rows: the 263-row zone broadcast reports 67,110,968 B on the same metric. The 2025-01 lag broadcast (195,672 rows) already reports 75,497,456 B in the baseline.
- `time to collect` 3,211 ms (it includes running the lag side's zero-fill stage, which overlaps the current side's), `time to build` 515 ms, `time to broadcast` 329 ms (driver side, local).
- Spark's INFO log in run 82 (not kept; the local MemoryStore has 3.4 GiB): `broadcast_11` stored as values in memory at an estimated **256.0 MiB**, and its serialized, compressed form as **13 pieces of 4 MiB (48.1 MiB)**, which is what each executor fetches.
- In a 1 GiB JVM (`heap-check.txt`), Spark's size estimate of the value is **at least 215.9 MiB**, and it doesn't fit the memory store: `WARN MemoryStore: Not enough space to cache broadcast_11 in memory!`, so the block goes to disk. O3 still completes. G1 Old Gen peaks about 200 MiB higher than without the hint (922–930 vs 700–728 MiB), and nothing of that size stays live after the job.

## Correctness (`correctness.txt`)

Bit-identical at both scales: 195,672 = 195,672 and 2,303,880 = 2,303,880 rows, `exceptAll` 0 both ways, every key matched once, max relative difference 0.0. Σ `trip_count` 3,203,803 / **43,599,803**, `rows_with_lag` 18,936 / 2,127,144, spikes 15,685 / 226,925 on both sides. The printed rollups are identical line by line. The negative control catches a 1e-9 relative bump of one variance, +1 on one lag and one dropped row.
