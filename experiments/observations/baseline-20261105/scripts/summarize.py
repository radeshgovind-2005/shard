#!/usr/bin/env python3
"""Summarise profiled runs: per-op and total metrics, median [min-max], spread.

usage: summarize.py <runs-dir> <run-id> [<run-id> ...]
"""
import json, statistics, sys
from pathlib import Path

MiB = 1 << 20


def load(runs_dir, rid):
    return json.loads((Path(runs_dir) / rid / "run.json").read_text())


def totals(rec):
    keys = ["wall_time_s", "num_stages", "num_tasks", "executor_run_time_ms", "executor_cpu_time_ms",
            "input_bytes", "shuffle_read_bytes", "shuffle_write_bytes", "memory_spilled_bytes",
            "disk_spilled_bytes", "jvm_gc_time_ms"]
    t = {k: sum(r[k] for r in rec["runs"]) for k in keys}
    return t


def fmt_row(name, r):
    gc = 100 * r["jvm_gc_time_ms"] / r["executor_run_time_ms"] if r["executor_run_time_ms"] else 0
    cpu = r["executor_cpu_time_ms"] / r["executor_run_time_ms"] if r["executor_run_time_ms"] else 0
    return (f"{name:6} wall={r['wall_time_s']:7.2f}s stages={r['num_stages']:3} tasks={r['num_tasks']:4} "
            f"in={r['input_bytes']/MiB:8.1f}MiB shR={r['shuffle_read_bytes']/MiB:7.1f} shW={r['shuffle_write_bytes']/MiB:7.1f}MiB "
            f"spillD={r['disk_spilled_bytes']/MiB:6.1f} spillM={r['memory_spilled_bytes']/MiB:7.1f}MiB "
            f"run={r['executor_run_time_ms']/1000:7.1f}s cpu/run={cpu:.2f} gc={gc:4.1f}%")


def main():
    runs_dir, ids = sys.argv[1], sys.argv[2:]
    per_op = {}
    tot_walls = []
    for rid in ids:
        rec = load(runs_dir, rid)
        print(f"### {rid} sha={rec['git_sha']} env={rec['env']} master={rec['master']} conf={rec['spark_conf']}")
        for r in rec["runs"]:
            print("  " + fmt_row(r["name"], r))
            per_op.setdefault(r["name"], []).append(r["wall_time_s"])
        t = totals(rec)
        print("  " + fmt_row("TOTAL", t))
        tot_walls.append(t["wall_time_s"])
    print("\n### median [min-max] wall, spread = (max-min)/median")
    for name, ws in list(per_op.items()) + [("TOTAL", tot_walls)]:
        m = statistics.median(ws)
        share = ""
        print(f"  {name:6} {m:7.2f} [{min(ws):.2f}-{max(ws):.2f}] spread={100*(max(ws)-min(ws))/m:5.1f}%")
    m_tot = statistics.median(tot_walls)
    print("\n### share of median total wall")
    for name, ws in per_op.items():
        print(f"  {name:6} {100*statistics.median(ws)/m_tot:5.1f}%")


if __name__ == "__main__":
    main()
