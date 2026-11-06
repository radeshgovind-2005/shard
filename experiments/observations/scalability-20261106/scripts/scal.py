#!/usr/bin/env python3
"""Per-configuration summary for the scalability study (run.json + stages.json only, Python 3 stdlib).

usage: scal.py <runs-dir> <elapsed.tsv> <label>=<id,id,...>:<cores> [<label>=...]
For each configuration: total wall (sum of the five profiled blocks) and spark-submit elapsed,
median [min-max] (spread); per-op wall medians; per-op stage time; tasks, stages, shuffle,
spill, GC, CPU; slot utilisation = executor run time / (wall x cores); scan-stage tasks and
duration; stages with fewer tasks than cores and their summed duration; skew.
"""
import json, statistics as st, sys
from pathlib import Path

MiB = 1 << 20
runs_dir = Path(sys.argv[1])
elapsed = {}
for line in Path(sys.argv[2]).read_text().splitlines():
    f = line.split("\t")
    elapsed[f[0]] = float(f[5])
OPS = ["m1", "m2", "m3", "o1", "o3"]


def fmt(v, nd=2):
    m = st.median(v)
    return f"{m:.{nd}f} [{min(v):.{nd}f}-{max(v):.{nd}f}] ({(100*(max(v)-min(v))/m if m else 0):.1f}%)"


def med(v):
    return st.median(v)


for spec in sys.argv[3:]:
    label, rest = spec.split("=")
    ids, cores = rest.split(":")
    ids, cores = ids.split(","), int(cores)
    R = [json.loads((runs_dir / i / "run.json").read_text()) for i in ids]
    S = [json.loads((runs_dir / i / "stages.json").read_text()) for i in ids]
    print(f"\n### {label} (cores {cores}; n = {len(ids)}: {', '.join(i.rsplit('-',1)[1] for i in ids)})")
    print(f"  fraction {sorted({r['args']['fraction'] for r in R})}, sha {sorted({r['git_sha'] for r in R})}")
    tot = lambda r, k: sum(x[k] for x in r["runs"])
    walls = [tot(r, "wall_time_s") for r in R]
    print(f"  total wall s        {fmt(walls)}   per run: {', '.join(f'{w:.2f}' for w in walls)}")
    print(f"  spark-submit s      {fmt([elapsed.get(i, 0.0) for i in ids])}")
    print(f"  outside blocks s    {fmt([elapsed.get(i, 0.0)-w for i, w in zip(ids, walls)])}  (start-up, read-backs, shutdown)")
    for op in OPS:
        ow = [next(x for x in r["runs"] if x["name"] == op) for r in R]
        stsum = [sum(s["duration_ms"] for s in x["stages"]) / 1000 for x in ow]
        print(f"    {op}: wall {fmt([x['wall_time_s'] for x in ow])}  stage-dur sum {med(stsum):.2f}  tasks {sorted({x['num_tasks'] for x in ow})} stages {sorted({x['num_stages'] for x in ow})}"
              f"  shufW {med([x['shuffle_write_bytes'] for x in ow])/MiB:.1f} MiB  spill {med([x['disk_spilled_bytes'] for x in ow])/MiB:.1f} MiB"
              f"  exec run {med([x['executor_run_time_ms'] for x in ow])/1000:.1f} s  util {med([x['executor_run_time_ms']/1000/(x['wall_time_s']*cores) for x in ow]):.2f}")
    run_s = [tot(r, "executor_run_time_ms") / 1000 for r in R]
    cpu_s = [tot(r, "executor_cpu_time_ms") / 1000 for r in R]
    gc = [100 * tot(r, "jvm_gc_time_ms") / tot(r, "executor_run_time_ms") for r in R]
    print(f"  tasks {sorted({tot(r,'num_tasks') for r in R})}, stages {sorted({tot(r,'num_stages') for r in R})}")
    print(f"  shuffle R / W MiB   {med([tot(r,'shuffle_read_bytes') for r in R])/MiB:.1f} / {med([tot(r,'shuffle_write_bytes') for r in R])/MiB:.1f}")
    print(f"  spill disk / mem    {sorted({round(tot(r,'disk_spilled_bytes')/MiB,1) for r in R})} / {sorted({round(tot(r,'memory_spilled_bytes')/MiB,1) for r in R})} MiB")
    print(f"  GC %                {fmt(gc,1)}")
    print(f"  executor run s      {fmt(run_s,1)};  CPU s {fmt(cpu_s,1)};  CPU/run {med([c/r for c,r in zip(cpu_s,run_s)]):.2f}")
    print(f"  slot utilisation    {fmt([rs/(w*cores) for rs,w in zip(run_s,walls)],2)}  (= executor run time / (wall x {cores} cores))")
    # scan stages: stages that read input (input_bytes > 0)
    scan = {}
    for s_list in S:
        for s in s_list:
            if s["input_bytes"] > 0:
                scan.setdefault((s["profile"], s["num_tasks"]), []).append(s)
    scans = [(k, v) for k, v in scan.items()]
    print("  scan stages (op, tasks): " + "; ".join(f"{k[0]}/{k[1]}t dur {med([s['duration_ms'] for s in v])/1000:.2f}s skew {med([s['skew_ratio'] for s in v]):.2f} taskmed {med([s['task_ms_median'] for s in v])/1000:.2f}s x{len(v)}" for k, v in sorted(scans)))
    m1scan = [next(s for s in sl if s["profile"] == "m1" and s["input_bytes"] > 0) for sl in S]
    print(f"  M1 scan: tasks {sorted({s['num_tasks'] for s in m1scan})}, waves {sorted({-(-s['num_tasks']//cores) for s in m1scan})}, dur {fmt([s['duration_ms']/1000 for s in m1scan])}, CPU s {fmt([s['executor_cpu_time_ms']/1000 for s in m1scan],1)}, max/med task {fmt([s['skew_ratio'] for s in m1scan])}")
    under = [sum(s["duration_ms"] for s in sl if s["num_tasks"] < cores) / 1000 for sl in S]
    one = [sum(s["duration_ms"] for s in sl if s["num_tasks"] == 1) / 1000 for sl in S]
    allst = [sum(s["duration_ms"] for s in sl) / 1000 for sl in S]
    print(f"  stage-duration sum s {fmt(allst)}; in stages with tasks < cores {fmt(under)}; in 1-task stages {fmt(one)}")
    print(f"  #stages with tasks < cores {sorted({sum(1 for s in sl if s['num_tasks'] < cores) for sl in S})}; 1-task {sorted({sum(1 for s in sl if s['num_tasks']==1) for sl in S})}")
    big = [max((s["skew_ratio"] or 0) for s in sl if (s["task_ms_median"] or 0) >= 100) for sl in S]
    print(f"  max skew (stages w/ median task >= 100 ms) {fmt(big)}")
