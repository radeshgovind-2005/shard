#!/usr/bin/env python3
"""Per-stage sums of task-level SQL accumulables (timings in ms unless noted). stdin: decompressed event log."""
import json, sys
from collections import defaultdict
KEEP = ["scan time", "time in aggregation build", "duration", "sort time", "peak memory", "spill size",
        "shuffle write time", "fetch wait time", "remote bytes read", "local bytes read", "data size",
        "internal.metrics.executorRunTime", "internal.metrics.executorCpuTime", "internal.metrics.jvmGCTime",
        "number of output rows"]
acc = defaultdict(lambda: defaultdict(int))
group = {}
for line in sys.stdin:
    e = json.loads(line)
    if e["Event"] == "SparkListenerJobStart":
        g = e.get("Properties", {}).get("spark.jobGroup.id", "-")
        for s in e["Stage IDs"]: group[s] = g.split("-")[0]
    if e["Event"] != "SparkListenerTaskEnd": continue
    for a in e["Task Info"]["Accumulables"]:
        n = a.get("Name")
        if n in KEEP and isinstance(a.get("Update"), (int, str)):
            try: acc[e["Stage ID"]][n] += int(a["Update"])
            except ValueError: pass
for s in sorted(acc):
    d = acc[s]
    run = d["internal.metrics.executorRunTime"]
    if run < 500: continue
    print(f"stage {s:3} [{group.get(s,'-')}] run={run/1000:6.1f}s cpu={d['internal.metrics.executorCpuTime']/1e9:6.1f}s "
          f"gc={d['internal.metrics.jvmGCTime']/1000:5.1f}s scan={d['scan time']/1000:5.1f}s agg={d['time in aggregation build']/1000:5.1f}s "
          f"sort={d['sort time']/1000:5.1f}s wscg={d['duration']/1000:6.1f}s shWtime={d['shuffle write time']/1e9:5.1f}s "
          f"fetchWait={d['fetch wait time']/1000:5.1f}s spill={d['spill size']/2**20:7.1f}MiB peakMem={d['peak memory']/2**20:7.1f}MiB")
