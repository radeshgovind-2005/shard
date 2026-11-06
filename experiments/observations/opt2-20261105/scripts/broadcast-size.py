#!/usr/bin/env python3
"""Read a decompressed Spark event log on stdin; print each BroadcastExchange's driver metrics (data size, rows,
time to build) for SQL execution 0 (the profiled write), using the last adaptive plan."""
import json, sys
nodes = {}   # accumulator id -> (node name, metric name)
vals = {}
def walk(p):
    for m in p.get("metrics", []):
        nodes[m["accumulatorId"]] = (p["nodeName"], m["name"])
    for c in p.get("children", []):
        walk(c)
for line in sys.stdin:
    e = json.loads(line)
    ev = e.get("Event", "")
    if ev.endswith("SparkListenerSQLExecutionStart") or ev.endswith("SparkListenerSQLAdaptiveExecutionUpdate"):
        if e.get("executionId") == 0:
            walk(e["sparkPlanInfo"])
    elif ev.endswith("SparkListenerDriverAccumUpdates") and e.get("executionId") == 0:
        for acc, v in e["accumUpdates"]:
            vals[acc] = vals.get(acc, 0) + v
for acc, (node, name) in sorted(nodes.items()):
    if node.startswith("BroadcastExchange") and acc in vals:
        print(f"{node} | {name} = {vals[acc]:,}")
