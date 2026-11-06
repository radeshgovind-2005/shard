#!/usr/bin/env python3
"""Read a decompressed Spark event log on stdin; print
 1. every BroadcastExchange's driver metrics, per SQL execution (data size, rows, collect/build/broadcast ms),
 2. failed tasks (TaskEnd reason != Success), failed stages/jobs, lost executors,
 3. O3's stages (ids from the run's run.json, argv[1]): submission/completion (s since O3's first stage), tasks, shuffle R/W bytes.
usage: zstd -dc experiments/runs/<run>/eventlog/*/events_* | python3 -I evsummary.py experiments/runs/<run>/run.json
"""
import json, sys
from collections import defaultdict

names = {}                       # accumulator id -> (exec id, node, metric)
vals = defaultdict(int)
fails, stage_fail, job_fail, lost = [], 0, 0, 0
jobgroup, stage_job = {}, {}
st = {}                          # stage id -> dict
def walk(p, ex):
    for m in p.get("metrics", []):
        names[m["accumulatorId"]] = (ex, p["nodeName"], m["name"])
    for c in p.get("children", []):
        walk(c, ex)
for line in sys.stdin:
    e = json.loads(line)
    ev = e.get("Event", "").rsplit(".", 1)[-1]
    if ev in ("SparkListenerSQLExecutionStart", "SparkListenerSQLAdaptiveExecutionUpdate"):
        walk(e["sparkPlanInfo"], e["executionId"])
    elif ev == "SparkListenerDriverAccumUpdates":
        for a, v in e["accumUpdates"]:
            vals[a] += v
    elif ev == "SparkListenerJobStart":
        g = (e.get("Properties") or {}).get("spark.jobGroup.id", "")
        for s in e["Stage Infos"]:
            stage_job[s["Stage ID"]] = g
    elif ev == "SparkListenerStageSubmitted":
        i = e["Stage Info"]
        st.setdefault(i["Stage ID"], {})["sub"] = i.get("Submission Time")
        st[i["Stage ID"]]["tasks"] = i["Number of Tasks"]
    elif ev == "SparkListenerStageCompleted":
        i = e["Stage Info"]
        d = st.setdefault(i["Stage ID"], {})
        d["done"] = i.get("Completion Time")
        if i.get("Failure Reason"):
            stage_fail += 1
    elif ev == "SparkListenerTaskEnd":
        r = e.get("Task End Reason", {}).get("Reason", "Success")
        if r != "Success":
            fails.append((e["Stage ID"], r))
        m = e.get("Task Metrics") or {}
        d = st.setdefault(e["Stage ID"], {})
        d["shr"] = d.get("shr", 0) + (m.get("Shuffle Read Metrics", {}).get("Remote Bytes Read", 0) + m.get("Shuffle Read Metrics", {}).get("Local Bytes Read", 0))
        d["shw"] = d.get("shw", 0) + m.get("Shuffle Write Metrics", {}).get("Shuffle Bytes Written", 0)
    elif ev == "SparkListenerJobEnd":
        if e["Job Result"]["Result"] != "JobSucceeded":
            job_fail += 1
    elif ev == "SparkListenerExecutorRemoved":
        lost += 1
print("# BroadcastExchange driver metrics per SQL execution (rows >= 100000 only; the others are the small zone broadcasts)")
by = defaultdict(dict)
for a, (ex, node, m) in names.items():
    if node.startswith("BroadcastExchange") and a in vals:
        by[ex][m] = vals[a]
for ex, d in sorted(by.items()):
    if d.get("number of output rows", 0) >= 100000:
        print(f"execution {ex}: " + "; ".join(f"{k} = {v:,}" for k, v in sorted(d.items())))
print(f"# failed tasks {len(fails)} {sorted(set(fails))}; failed stages {stage_fail}; failed jobs {job_fail}; executors removed {lost}")
o3ids = {x["stage_id"] for r in json.load(open(sys.argv[1]))["runs"] if r["name"] == "o3" for x in r["stages"]}
o3 = sorted((s for s in st if s in o3ids and st[s].get("sub")), key=lambda s: st[s]["sub"])
if o3:
    t0 = st[o3[0]]["sub"]
    print("# O3 stages: id, tasks, submitted s, completed s, duration s, shuffle read B, shuffle write B")
    for s in o3:
        d = st[s]
        print(f"{s}\t{d['tasks']}\t{(d['sub']-t0)/1000:.2f}\t{(d['done']-t0)/1000:.2f}\t{(d['done']-d['sub'])/1000:.2f}\t{d.get('shr',0)}\t{d.get('shw',0)}")
