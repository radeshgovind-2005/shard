#!/usr/bin/env python3
"""Read a (decompressed) Spark event log on stdin and print, per stage:
job group, tasks, records read, executors, max concurrent tasks, waves, span,
and per execution id the scan's 'size of files read' / 'number of files read' SQL metrics.

usage: zstd -dc events_* | python3 -I eventlog.py [--tasks <stage-id>]
"""
import json, sys
from collections import defaultdict

want_tasks = int(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[1] == "--tasks" else None

metric_names = {}          # accumulator id -> metric name
exec_metric_vals = defaultdict(lambda: defaultdict(int))  # exec id -> name -> value
stage_group = {}
stage_exec = {}
job_group = {}
tasks = defaultdict(list)  # stage -> [(launch, finish, executor, records, dur_ms, rec_written?)]
stage_attempt_tasks = {}
executors = {}


def walk(plan, exec_id):
    for m in plan.get("metrics", []):
        metric_names[m["accumulatorId"]] = m["name"]
    for c in plan.get("children", []):
        walk(c, exec_id)


for line in sys.stdin:
    e = json.loads(line)
    ev = e["Event"]
    if ev.endswith("SQLExecutionStart") or ev.endswith("SQLAdaptiveExecutionUpdate"):
        walk(e["sparkPlanInfo"], e["executionId"])
    elif ev.endswith("SQLAdaptiveSQLMetricUpdates"):
        for m in e.get("sqlPlanMetrics", []):
            metric_names[m["accumulatorId"]] = m["name"]
    elif ev.endswith("DriverAccumUpdates"):
        for acc_id, val in e["accumUpdates"]:
            name = metric_names.get(acc_id)
            if name:
                exec_metric_vals[e["executionId"]][name] += val
    elif ev == "SparkListenerJobStart":
        props = e.get("Properties", {})
        g = props.get("spark.jobGroup.id", "-")
        x = props.get("spark.sql.execution.id", "-")
        for s in e["Stage IDs"]:
            stage_group[s] = g
            stage_exec[s] = x
    elif ev == "SparkListenerExecutorAdded":
        executors[e["Executor ID"]] = e["Executor Info"]["Total Cores"]
    elif ev == "SparkListenerTaskEnd":
        ti, tm = e["Task Info"], e.get("Task Metrics") or {}
        if ti.get("Failed") or ti.get("Killed"):
            continue
        rec = (tm.get("Input Metrics") or {}).get("Records Read", 0)
        shr = (tm.get("Shuffle Read Metrics") or {})
        sh_rec = shr.get("Total Records Read", 0)
        tasks[e["Stage ID"]].append((ti["Launch Time"], ti["Finish Time"], ti["Executor ID"], rec, sh_rec,
                                     tm.get("Disk Bytes Spilled", 0)))


def max_concurrency(ts):
    pts = sorted([(t[0], 1) for t in ts] + [(t[1], -1) for t in ts], key=lambda p: (p[0], p[1]))
    cur = best = 0
    for _, d in pts:
        cur += d
        best = max(best, cur)
    return best


print("executors (cores):", executors)
print(f"{'stage':>5} {'group':6} {'exec':>4} {'tasks':>5} {'conc':>4} {'span_s':>6} {'recRead':>12} {'shRecRead':>12} executors")
for s in sorted(tasks):
    ts = tasks[s]
    span = (max(t[1] for t in ts) - min(t[0] for t in ts)) / 1000
    exs = defaultdict(int)
    for t in ts:
        exs[t[2]] += 1
    print(f"{s:5} {stage_group.get(s, '-'):6} {stage_exec.get(s, '-'):>4} {len(ts):5} {max_concurrency(ts):4} {span:6.2f} "
          f"{sum(t[3] for t in ts):12,} {sum(t[4] for t in ts):12,} {dict(exs)}")

print("\nSQL scan metrics per execution id")
for x, vals in sorted(exec_metric_vals.items()):
    picked = {k: v for k, v in vals.items() if "files" in k or k.startswith("metadata")}
    print(f"  exec {x}: {picked}")

if want_tasks is not None:
    ts = sorted(tasks[want_tasks])
    t0 = ts[0][0]
    print(f"\ntasks of stage {want_tasks} (launch_s, finish_s, dur_ms, executor, records, shuffle records, disk spill)")
    for t in ts:
        print(f"  {(t[0]-t0)/1000:6.2f} {(t[1]-t0)/1000:6.2f} {t[1]-t[0]:6} {t[2]:>6} {t[3]:>10,} {t[4]:>10,} {t[5]:>12,}")
