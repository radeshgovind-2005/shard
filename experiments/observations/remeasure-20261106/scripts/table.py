#!/usr/bin/env python3
"""Baseline/final table for LOG.md, CPU-time indicator and paired deltas (run.json + stages.json only).

usage: table.py <runs-dir> <env> <baseline-ids,comma> <final-ids,comma> [<csv-out> <csv-ids,comma> <warmup-ids,comma>]
Baseline and final ids are listed in interleaved order (pair i = baseline[i], final[i]).
"""
import csv, json, statistics as st, sys
from pathlib import Path

MiB = 1 << 20
runs_dir, env, B, F = sys.argv[1], sys.argv[2], sys.argv[3].split(","), sys.argv[4].split(",")
OPS = ["m1", "m2", "m3", "o1", "o3"]


def run(rid):
    return json.loads((Path(runs_dir) / rid / "run.json").read_text())


def stages(rid):
    return json.loads((Path(runs_dir) / rid / "stages.json").read_text())


def fmt(v, nd=2):
    m = st.median(v)
    return f"{m:.{nd}f} [{min(v):.{nd}f}-{max(v):.{nd}f}] ({100*(max(v)-min(v))/m:.1f}%)"


def tot(rec, k):
    return sum(r[k] for r in rec["runs"])


def side(name, ids):
    recs = [run(i) for i in ids]
    print(f"\n## {name} (n = {len(ids)}: {', '.join(i.rsplit('-', 1)[1] for i in ids)}), median [min-max] (spread)")
    for op in OPS:
        print(f"  {op:6} wall s {fmt([next(r['wall_time_s'] for r in rec['runs'] if r['name'] == op) for rec in recs])}")
    walls = [tot(r, "wall_time_s") for r in recs]
    print(f"  total  wall s {fmt(walls)}")
    print(f"  shuffle R / W MiB (all runs) {sorted({round(tot(r,'shuffle_read_bytes')/MiB,1) for r in recs})} / {sorted({round(tot(r,'shuffle_write_bytes')/MiB,1) for r in recs})}")
    for op in OPS:
        print(f"    {op}: R {sorted({round(next(x for x in r['runs'] if x['name']==op)['shuffle_read_bytes']/MiB,1) for r in recs})} W {sorted({round(next(x for x in r['runs'] if x['name']==op)['shuffle_write_bytes']/MiB,1) for r in recs})} MiB, disk spill {sorted({round(next(x for x in r['runs'] if x['name']==op)['disk_spilled_bytes']/MiB,1) for r in recs})} MiB")
    print(f"  tasks {sorted({tot(r,'num_tasks') for r in recs})}, stages {sorted({tot(r,'num_stages') for r in recs})}")
    print(f"  disk spill MiB {sorted({round(tot(r,'disk_spilled_bytes')/MiB,1) for r in recs})}, mem spill MiB {sorted({round(tot(r,'memory_spilled_bytes')/MiB,1) for r in recs})}")
    gc = [100 * tot(r, "jvm_gc_time_ms") / tot(r, "executor_run_time_ms") for r in recs]
    cpu = [tot(r, "executor_cpu_time_ms") / tot(r, "executor_run_time_ms") for r in recs]
    print(f"  GC % {fmt(gc, 1)}; CPU/run {fmt(cpu, 2)}; executor run s {fmt([tot(r,'executor_run_time_ms')/1000 for r in recs], 1)}")
    sk = {}
    for i in ids:
        for s in stages(i):
            sk[s["profile"]] = max(sk.get(s["profile"], 0), s["skew_ratio"] or 0)
    print("  max task skew (max/median) per op, max over runs: " + " ".join(f"{k}={v:.2f}" for k, v in sk.items()) + f"; overall {max(sk.values()):.2f}")
    sk2, small = {}, 0.0
    for i in ids:
        for s in stages(i):
            if (s["task_ms_median"] or 0) >= 100:
                sk2[s["profile"]] = max(sk2.get(s["profile"], 0), s["skew_ratio"] or 0)
            elif (s["skew_ratio"] or 0) > small:
                small = s["skew_ratio"]
    print("  same, stages with median task >= 100 ms only: " + " ".join(f"{k}={v:.2f}" for k, v in sk2.items()) + f"; overall {max(sk2.values()):.2f} (max ratio among the shorter stages: {small:.2f})")
    return walls


wb = side("baseline (--skip-opts opt2)", B)
wf = side("final (OPT-2 on)", F)
print("\n## paired differences (final minus the baseline run just before it), total s")
d = [f - b for b, f in zip(wb, wf)]
print("  " + ", ".join(f"{x:+.2f}" for x in d) + f"; all negative: {all(x < 0 for x in d)}; median {st.median(d):+.2f}")
mb, mf = st.median(wb), st.median(wf)
print(f"\n## delta of medians: total {mf-mb:+.2f} s ({100*(mf-mb)/mb:+.1f} %)")
for op in OPS:
    b = st.median([next(r["wall_time_s"] for r in run(i)["runs"] if r["name"] == op) for i in B])
    f = st.median([next(r["wall_time_s"] for r in run(i)["runs"] if r["name"] == op) for i in F])
    print(f"  {op}: {b:.2f} -> {f:.2f}  {f-b:+.2f} s ({100*(f-b)/b:+.1f} %)")

print("\n## machine-state indicator: M1 scan stage (stage_id 1) executor_cpu_time_ms, per run (reference sessions: local-20261105-02 32566, local-20261105-09 21414, docker-20261105-02 22294, docker-20261105-06 14025)")
allids = sys.argv[7].split(",") + B + F if len(sys.argv) > 7 else B + F
vals = {}
for i in sorted(set(allids), key=lambda x: int(x.rsplit("-", 1)[1])):
    s1 = next(s for s in stages(i) if s["stage_id"] == 1)
    assert s1["profile"] == "m1"
    vals[i] = s1["executor_cpu_time_ms"]
    print(f"  {i} {run(i)['args'].get('skip_opts') or 'final':8} {s1['executor_cpu_time_ms']:6} ms  (run {s1['executor_run_time_ms']} ms, duration {s1['duration_ms']} ms)")
v = list(vals.values())
print(f"  median of the listed runs {st.median(v):.0f} ms [{min(v)}-{max(v)}]")

if len(sys.argv) > 6:
    with open(sys.argv[5], "a", newline="") as f:
        w = csv.writer(f)
        warm = set(sys.argv[7].split(",")) if len(sys.argv) > 7 else set()
        for rid in sys.argv[6].split(","):
            r = {x["name"]: x for x in run(rid)["runs"]}
            for op in OPS:
                w.writerow([rid, env, op, f"{r[op]['wall_time_s']:.2f}", 0 if rid in warm else 1])
