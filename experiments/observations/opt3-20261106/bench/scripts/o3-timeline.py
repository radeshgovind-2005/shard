#!/usr/bin/env python3
"""From evsummary-<run>.txt: per run, O3's final stage duration, the gap before it, the span, and (on) the broadcast metrics.
usage (repo root): python3 -I experiments/observations/opt3-20261106/bench/scripts/o3-timeline.py > experiments/observations/opt3-20261106/bench/o3-timeline.txt"""
import re, statistics as st
B = 'experiments/observations/opt3-20261106/bench'
print("# per run: broadcast driver metrics (on), O3 final stage (off: self-join stage; on: window+join stage) duration, gap between the last earlier stage's completion and the final stage's submission, O3 span first submit -> last done. From evsummary-<run>.txt (scripts/evsummary.py).")
for env, off, on in (("local", [2, 4, 6, 8, 10], [3, 5, 7, 9, 11]), ("docker", [2, 4, 6, 8, 10], [3, 5, 7, 9, 11])):
    for lab, ids in (("off", off), ("on", on)):
        print(f"\n## {env} {lab}")
        gaps, fin, span = [], [], []
        for n in ids:
            rid = f"{env}-20261106-{n:02d}"
            t = open(f"{B}/evsummary-{rid}.txt").read().splitlines()
            bc = [l for l in t if l.startswith("execution")]
            rows = [l.split("\t") for l in t if re.match(r"^\d+\t", l)]
            sub = [float(r[2]) for r in rows]; done = [float(r[3]) for r in rows]; dur = [float(r[4]) for r in rows]
            gap = sub[-1] - max(done[:-1]); gaps.append(gap); fin.append(dur[-1]); span.append(max(done))
            print(f"{rid}: final-stage {dur[-1]:.2f} s, gap before it {gap:.2f} s, span {max(done):.2f} s" + (f" | {bc[0].split(': ', 1)[1]}" if bc else ""))
        f = lambda v: f"{st.median(v):.2f} [{min(v):.2f}-{max(v):.2f}]"
        print(f"median final-stage {f(fin)} s; gap {f(gaps)} s; span {f(span)} s")
