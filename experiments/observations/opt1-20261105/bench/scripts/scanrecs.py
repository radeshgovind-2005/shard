#!/usr/bin/env python3
"""From eventlog-*.txt files: per run, per op group: max records read in one stage (48,722,602 = Parquet trips; 4,365 / 322-ish = cached batches),
count of stages reading >=48M records, and the scan 'size of files read' per execution.
usage: scanrecs.py <eventlog-file>...
"""
import re, sys
for f in sys.argv[1:]:
    ops = {}; sizes = {}
    sect = 0
    for line in open(f):
        if line.startswith("SQL scan metrics"): sect = 1; continue
        if sect == 0:
            m = re.match(r"\s+(\d+) (\S+)\s+(\S+)\s+(\d+)\s+(\d+)\s+([\d.]+)\s+([\d,]+)\s+([\d,]+)", line)
            if m and m.group(2) != "-":
                op = m.group(2).split("-")[0]
                rec = int(m.group(7).replace(",", ""))
                o = ops.setdefault(op, {"pq_stages": 0, "other_scan_recs": []})
                if rec >= 40_000_000: o["pq_stages"] += 1
                elif rec > 0 and int(m.group(4)) >= 10: o["other_scan_recs"].append(rec)
        else:
            m = re.match(r"\s+exec (\d+): .*'size of files read': (\d+)", line)
            if m: sizes[int(m.group(1))] = int(m.group(2))
    print(f.split("/")[-1], {k: v for k, v in ops.items()}, "scan sizes:", sizes)
