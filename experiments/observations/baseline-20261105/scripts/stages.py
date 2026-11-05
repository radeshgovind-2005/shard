#!/usr/bin/env python3
"""Per-stage table of one run, sorted by profile then stage id.

usage: stages.py <runs-dir> <run-id>
"""
import json, sys
from pathlib import Path

MiB = 1 << 20
runs_dir, rid = sys.argv[1], sys.argv[2]
stages = json.loads((Path(runs_dir) / rid / "stages.json").read_text())
total = sum(s["duration_ms"] or 0 for s in stages)
print(f"{rid}: {len(stages)} stages, sum of stage durations {total/1000:.2f}s")
print(f"{'op':4} {'id':>4} {'dur_s':>6} {'%':>5} {'tasks':>5} {'in MiB':>7} {'shR MiB':>8} {'shW MiB':>8} "
      f"{'spD MiB':>8} {'spM MiB':>8} {'run_s':>6} {'cpu/r':>5} {'gc%':>5} {'med_ms':>7} {'max_ms':>7} {'skew':>6}  name")
for s in stages:
    d = s["duration_ms"] or 0
    run = s["executor_run_time_ms"]
    print(f"{s['profile']:4} {s['stage_id']:4} {d/1000:6.2f} {100*d/total:5.1f} {s['num_tasks']:5} "
          f"{s['input_bytes']/MiB:7.1f} {s['shuffle_read_bytes']/MiB:8.1f} {s['shuffle_write_bytes']/MiB:8.1f} "
          f"{s['disk_spilled_bytes']/MiB:8.1f} {s['memory_spilled_bytes']/MiB:8.1f} {run/1000:6.1f} "
          f"{(s['executor_cpu_time_ms']/run if run else 0):5.2f} {(100*s['jvm_gc_time_ms']/run if run else 0):5.1f} "
          f"{(s['task_ms_median'] or 0):7.0f} {(s['task_ms_max'] or 0):7} {str(s['skew_ratio']):>6}  {s['name'][:50]}")
