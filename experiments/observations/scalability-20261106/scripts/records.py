#!/usr/bin/env python3
"""Records read and bytes read (Input Metrics) per stage, summed over tasks, from a run's event log."""
import json, sys, glob, collections, subprocess
for rid in sys.argv[2:]:
    f = sorted(glob.glob(f"{sys.argv[1]}/{rid}/eventlog/*/events_*"))
    rec, byt, ntask = collections.Counter(), collections.Counter(), collections.Counter()
    for line in subprocess.run(["zstd","-dc",*f],capture_output=True,text=True).stdout.splitlines():
        if '"SparkListenerTaskEnd"' not in line: continue
        e = json.loads(line); m = e.get("Task Metrics") or {}
        im = m.get("Input Metrics", {})
        if im.get("Records Read", 0):
            rec[e["Stage ID"]] += im["Records Read"]; byt[e["Stage ID"]] += im.get("Bytes Read", 0); ntask[e["Stage ID"]] += 1
    tot = sum(rec.values())
    print(rid, "total records read", tot, "bytes", sum(byt.values()), "| per scan stage:", {s: (rec[s], ntask[s]) for s in sorted(rec) if rec[s] > 1_000_000})
