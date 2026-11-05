#!/usr/bin/env python3
"""Before/after comparison of OPT-1 runs (run.json only).

usage: compare.py <runs-dir> <env> <before-ids,comma> <after-ids,comma> [<csv-out> <csv-ids,comma>]
"""
import json, statistics as st, sys, csv
from pathlib import Path

MiB = 1 << 20
runs_dir, env, before, after = sys.argv[1], sys.argv[2], sys.argv[3].split(","), sys.argv[4].split(",")


def load(rid):
    return json.loads((Path(runs_dir) / rid / "run.json").read_text())


def per_op(rec):
    d = {r["name"]: r for r in rec["runs"]}
    tot = {k: sum(r[k] for r in rec["runs"]) for k in
           ["wall_time_s", "num_stages", "num_tasks", "shuffle_read_bytes", "shuffle_write_bytes",
            "disk_spilled_bytes", "memory_spilled_bytes", "jvm_gc_time_ms", "executor_run_time_ms"]}
    d["total"] = tot
    return d


def fmt(vals, unit=1.0, nd=2):
    m = st.median(vals)
    sp = 100 * (max(vals) - min(vals)) / m if m else 0
    return f"{m/unit:.{nd}f} [{min(vals)/unit:.{nd}f}-{max(vals)/unit:.{nd}f}] ({sp:.1f}%)", m


def side(ids):
    return [per_op(load(i)) for i in ids]


B, A = side(before), side(after)
print(f"# env={env} before(off)={before} after(on)={after}")
print("# cell = median [min-max] (spread=(max-min)/median)")
for metric, key, unit, nd in [("wall s", "wall_time_s", 1, 2), ("stages", "num_stages", 1, 0), ("tasks", "num_tasks", 1, 0),
                              ("shuffle R MiB", "shuffle_read_bytes", MiB, 1), ("shuffle W MiB", "shuffle_write_bytes", MiB, 1),
                              ("disk spill MiB", "disk_spilled_bytes", MiB, 1), ("mem spill MiB", "memory_spilled_bytes", MiB, 1),
                              ("exec run s", "executor_run_time_ms", 1000, 1)]:
    print(f"\n## {metric}")
    for op in ["m1", "m2", "m3", "o1", "o3", "total"]:
        sb, mb = fmt([x[op][key] for x in B], unit, nd)
        sa, ma = fmt([x[op][key] for x in A], unit, nd)
        d = (ma - mb) / unit
        pct = f"{100*(ma-mb)/mb:+.1f}%" if mb else "n/a"
        print(f"  {op:6} before {sb:32} after {sa:32} delta {d:+.{nd}f} ({pct})")
gcb = [100 * x["total"]["jvm_gc_time_ms"] / x["total"]["executor_run_time_ms"] for x in B]
gca = [100 * x["total"]["jvm_gc_time_ms"] / x["total"]["executor_run_time_ms"] for x in A]
print(f"\n## GC % of executor run time: before {st.median(gcb):.1f} [{min(gcb):.1f}-{max(gcb):.1f}]  after {st.median(gca):.1f} [{min(gca):.1f}-{max(gca):.1f}]")
print("\n## per run wall (s): " + "; ".join(f"{i}: " + " ".join(f"{op} {r[op]['wall_time_s']:.2f}" for op in ['m1','m2','m3','o1','o3']) + f" | total {r['total']['wall_time_s']:.2f}" for i, r in zip(before + after, B + A)))
if len(sys.argv) > 6:
    with open(sys.argv[5], "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["run_id", "env", "op", "wall_time_s", "measured"])
        for rid in sys.argv[6].split(","):
            r = per_op(load(rid))
            meas = 0 if rid.endswith(sys.argv[7]) else 1 if len(sys.argv) > 7 else 1
            for op in ["m1", "m2", "m3", "o1", "o3"]:
                w.writerow([rid, env, op, f"{r[op]['wall_time_s']:.2f}", meas])
