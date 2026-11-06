#!/usr/bin/env python3
"""O1 stage timeline from a decompressed event log on stdin: per stage of the o1 job group,
submission and completion (s since the first O1 stage was submitted), duration, tasks, input records,
shuffle write bytes, and whether it overlapped the previous stage.
usage: zstd -dc events_* | python3 -I o1timeline.py [group-prefix=o1]
"""
import json, sys
pref = sys.argv[1] if len(sys.argv) > 1 else "o1"
grp = {}; st = {}
for line in sys.stdin:
    e = json.loads(line); ev = e["Event"]
    if ev.endswith("SparkListenerJobStart"):
        g = (e.get("Properties") or {}).get("spark.jobGroup.id", "-")
        for s in e["Stage Infos"]: grp[s["Stage ID"]] = g
    elif ev.endswith("SparkListenerStageCompleted"):
        i = e["Stage Info"]
        acc = {a["Name"]: a.get("Value") for a in i.get("Accumulables", []) if "Name" in a}
        st[i["Stage ID"]] = (i.get("Submission Time"), i.get("Completion Time"), i["Number of Tasks"],
                             acc.get("internal.metrics.input.recordsRead", 0), acc.get("internal.metrics.shuffle.write.bytesWritten", 0))
rows = sorted((v[0], k, v) for k, v in st.items() if grp.get(k, "-").startswith(pref) and v[0])
t0 = rows[0][0]; prev_end = None
print("stage  submit_s  complete_s  dur_s  tasks  input_records  shuffle_write_B  overlaps_prev")
for sub, k, v in rows:
    ov = "" if prev_end is None else ("yes" if sub < prev_end else "no")
    print(f"{k:5} {(sub-t0)/1000:9.2f} {(v[1]-t0)/1000:11.2f} {(v[1]-v[0])/1000:6.2f} {v[2]:6} {v[3]:14,} {v[4]:16,}  {ov}")
    prev_end = max(prev_end or 0, v[1])
