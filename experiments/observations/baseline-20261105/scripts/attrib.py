#!/usr/bin/env python3
"""Attribute wall-clock per op to its trip-scan stage vs the rest, across runs; plus selected stages.

usage: attrib.py <runs-dir> <run-id>...
Scan stage = the stage of that profile with the largest input_bytes... input_bytes is broken (Parquet reader),
so it's the stage with the most tasks among those with zero shuffle read (the FileScan stage).
"""
import json, statistics, sys
from pathlib import Path

runs_dir, ids = sys.argv[1], sys.argv[2:]
rows = {}  # op -> list of dicts
for rid in ids:
    rec = json.loads((Path(runs_dir) / rid / "run.json").read_text())
    for r in rec["runs"]:
        st = r["stages"]
        cands = [s for s in st if s["shuffle_read_bytes"] == 0 and s["num_tasks"] >= 4 and (s["duration_ms"] or 0) > 500]
        scan = max(cands, key=lambda s: s["executor_run_time_ms"]) if cands else None
        biggest = max(st, key=lambda s: s["duration_ms"] or 0)
        rows.setdefault(r["name"], []).append({
            "wall": r["wall_time_s"],
            "scan": (scan["duration_ms"] / 1000) if scan else 0.0,
            "scan_tasks": scan["num_tasks"] if scan else 0,
            "scan_skew": scan["skew_ratio"] if scan else None,
            "run_s": r["executor_run_time_ms"] / 1000,
            "gc": 100 * r["jvm_gc_time_ms"] / max(1, r["executor_run_time_ms"]),
            "cpu": r["executor_cpu_time_ms"] / max(1, r["executor_run_time_ms"]),
            "spill": r["disk_spilled_bytes"] / 2**20,
            "maxskew": max((s["skew_ratio"] or 0) for s in st),
            "stage_durs": {s["stage_id"]: (s["duration_ms"] or 0) / 1000 for s in st},
        })

med = statistics.median
tot = med([sum(rows[o][i]["wall"] for o in rows) for i in range(len(ids))])
print(f"median total wall {tot:.2f}s over {len(ids)} runs")
print(f"{'op':4} {'wall':>6} {'[min-max]':>13} {'%tot':>5} {'scan':>6} {'%op':>4} {'%tot':>5} {'rest':>6} {'tasks':>5} {'run_s':>6} {'cpu/r':>5} {'gc%':>4} {'spill':>6} {'maxskew':>7}")
for op, rs in rows.items():
    w = med([r["wall"] for r in rs]); sc = med([r["scan"] for r in rs])
    print(f"{op:4} {w:6.2f} [{min(r['wall'] for r in rs):5.2f}-{max(r['wall'] for r in rs):5.2f}] {100*w/tot:5.1f} "
          f"{sc:6.2f} {100*sc/w:4.0f} {100*sc/tot:5.1f} {w-sc:6.2f} {rs[0]['scan_tasks']:5} {med([r['run_s'] for r in rs]):6.1f} "
          f"{med([r['cpu'] for r in rs]):5.2f} {med([r['gc'] for r in rs]):4.1f} {rs[0]['spill']:6.1f} {max(r['maxskew'] for r in rs):7.2f}")
print("\nmedian stage durations (s) per op, by stage id (stage ids are stable across runs of one env)")
for op, rs in rows.items():
    ids_ = sorted(rs[0]["stage_durs"])
    print(f"  {op}: " + ", ".join(f"{i}={med([r['stage_durs'].get(i, 0) for r in rs]):.2f}" for i in ids_))
