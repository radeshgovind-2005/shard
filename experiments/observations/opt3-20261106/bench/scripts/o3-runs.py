#!/usr/bin/env python3
"""Per-run O3 table (and totals) from run.json: usage: o3-runs.py <runs-dir> <env> <off-ids,comma> <on-ids,comma>"""
import json, statistics as st, sys
from pathlib import Path
d, env, off, on = sys.argv[1], sys.argv[2], sys.argv[3].split(","), sys.argv[4].split(",")
MiB = 1 << 20
def rec(i):
    r = json.loads((Path(d) / i / "run.json").read_text())["runs"]
    o3 = next(x for x in r if x["name"] == "o3")
    tot = {k: sum(x[k] for x in r) for k in ["wall_time_s", "jvm_gc_time_ms", "executor_run_time_ms", "num_tasks", "num_stages", "shuffle_read_bytes", "shuffle_write_bytes", "disk_spilled_bytes"]}
    return o3, tot
print(f"# env={env}; O3 per run: wall s | stages/tasks | shuffle R/W MiB | disk spill MiB | GC ms / exec ms = GC % ; total wall, GC %")
for label, ids in (("off (--skip-opts opt3)", off), ("on", on)):
    print(f"\n## {label}")
    gcs, gtot = [], []
    for i in ids:
        o, t = rec(i)
        g = 100 * o["jvm_gc_time_ms"] / o["executor_run_time_ms"]; gt = 100 * t["jvm_gc_time_ms"] / t["executor_run_time_ms"]
        gcs.append(g); gtot.append(gt)
        print(f"{i}: o3 {o['wall_time_s']:.2f} s | {o['num_stages']}/{o['num_tasks']} | {o['shuffle_read_bytes']/MiB:.1f}/{o['shuffle_write_bytes']/MiB:.1f} | {o['disk_spilled_bytes']/MiB:.1f} | {o['jvm_gc_time_ms']}/{o['executor_run_time_ms']} = {g:.1f} % | total {t['wall_time_s']:.2f} s, GC {gt:.1f} %")
    print(f"median O3 GC % {st.median(gcs):.1f} [{min(gcs):.1f}-{max(gcs):.1f}]; total GC % {st.median(gtot):.1f} [{min(gtot):.1f}-{max(gtot):.1f}]")
