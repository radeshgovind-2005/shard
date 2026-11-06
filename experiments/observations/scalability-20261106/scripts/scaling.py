#!/usr/bin/env python3
"""Speedup / efficiency / cost / Amdahl tables and the chart CSVs for the scalability study.

usage: scaling.py <runs-dir> <out-dir>
Run sets are fixed below (run-ids in experiments/LOG.md). T = total of the five profiled blocks (wall_time_s).
"""
import csv, json, statistics as st, sys
from pathlib import Path

runs, out = Path(sys.argv[1]), Path(sys.argv[2])
D = "docker-20261106-"
L = "local-20261106-"
CLUSTER = {2: [67, 68, 69, 75], 3: [55, 56, 57], 4: [59, 60, 61, 71, 72, 73], 5: [63, 64, 65]}
T2_ALL = [41, 45, 49, 53, 79, 67, 68, 69, 75]  # every measured 2-worker full-year run (sensitivity)
SCALE = {0.1: [38, 42, 46, 50, 76], 0.2: [39, 43, 47, 51, 77], 0.5: [40, 44, 48, 52, 78], 1.0: [41, 45, 49, 53, 79]}
LOCAL = {2: [28, 32, 36], 4: [29, 33, 37], 8: [30, 34, 38], 10: [31, 35, 39]}  # 10 = local[*]


def rec(rid):
    return json.loads((runs / rid / "run.json").read_text())


def stg(rid):
    return json.loads((runs / rid / "stages.json").read_text())


def T(rid):
    return sum(x["wall_time_s"] for x in rec(rid)["runs"])


def tot(rid, k):
    return sum(x[k] for x in rec(rid)["runs"])


def one_task_s(rid):
    return sum(s["duration_ms"] for s in stg(rid) if s["num_tasks"] == 1) / 1000


def summ(ids):
    v = [T(i) for i in ids]
    m = st.median(v)
    return m, min(v), max(v), 100 * (max(v) - min(v)) / m


def amdahl(f, pp):
    return 1 / (f + (1 - f) / pp)


def fit_f(points):  # least squares over f in [0, 1], grid 1e-4
    best = min((sum((amdahl(f / 10000, pp) - s) ** 2 for pp, s in points), f / 10000) for f in range(0, 10001))
    return best[1]


def table(name, sets, prefix, rel, unit):
    rows = {p: summ([f"{prefix}{i:02d}" for i in ids]) for p, ids in sets.items()}
    Tr = rows[rel][0]
    pts = []
    res = []
    for p, (m, lo, hi, sp) in rows.items():
        pp = p / rel
        S = Tr / m
        E = S / pp
        f = (pp / S - 1) / (pp - 1) if p != rel else None
        ids = [f"{prefix}{i:02d}" for i in sets[p]]
        cpu = st.median([tot(i, "executor_cpu_time_ms") / 1000 for i in ids])
        run = st.median([tot(i, "executor_run_time_ms") / 1000 for i in ids])
        one = st.median([one_task_s(i) for i in ids])
        res.append(dict(p=p, pp=pp, n=len(ids), T=m, lo=lo, hi=hi, spread=sp, S=S, ideal=pp, E=E, C=p * m, f=f, cpu=cpu, run=run, cores_busy=cpu / m, one=one))
        if p != rel:
            pts.append((pp, S))
    ffit = fit_f(pts)
    f_ser = st.median([one_task_s(f"{prefix}{i:02d}") / T(f"{prefix}{i:02d}") for i in sets[rel]])
    print(f"\n{name} (relative to {rel} {unit}; Amdahl least-squares f = {ffit:.4f}; f from 1-task stages at {rel} = {f_ser:.4f})")
    print(f"{unit:>6} n  T median [min-max] (spread)     S      ideal  E       C ({unit}*s)  f_est   CPU s  run s  CPU/T  1-task s  Amdahl-fit  Amdahl(f_1task)")
    for r in res:
        fe = "—" if r["f"] is None else f"{r['f']:.3f}"
        r["afit"] = amdahl(ffit, r["pp"])
        r["aser"] = amdahl(f_ser, r["pp"])
        print(f"{r['p']:>6} {r['n']}  {r['T']:.2f} [{r['lo']:.2f}-{r['hi']:.2f}] ({r['spread']:.1f}%)  {r['S']:.3f}  {r['ideal']:.2f}   {100*r['E']:.1f}%  {r['C']:.1f}  {fe:>6}  {r['cpu']:.1f}  {r['run']:.1f}  {r['cores_busy']:.2f}  {r['one']:.2f}  {r['afit']:.3f}  {r['aser']:.3f}")
    return res, ffit, f_ser


res, ffit, fser = table("Docker cluster size, full 2025", CLUSTER, D, 2, "workers")
t2all = summ([f"{D}{i}" for i in T2_ALL])
print(f"T2 sensitivity: all {len(T2_ALL)} 2-worker full-year runs {t2all[0]:.2f} [{t2all[1]:.2f}-{t2all[2]:.2f}] ({t2all[3]:.1f}%) -> S(3,4,5) = "
      + ", ".join(f"{t2all[0]/r['T']:.3f}" for r in res if r['p'] != 2))
lres, lfit, lser = table("local[n] supplement, full 2025", LOCAL, L, 2, "cores")

with (out / "speedup.csv").open("w", newline="") as fh:
    w = csv.writer(fh)
    w.writerow(["env", "nodes", "cores", "p_rel", "n_runs", "T_median_s", "T_min_s", "T_max_s", "speedup", "speedup_ideal", "efficiency", "cost_node_s",
                "amdahl_f_est", "speedup_amdahl_fit", "amdahl_fit_f", "speedup_amdahl_serial_stages", "amdahl_serial_f", "executor_cpu_s", "run_ids"])
    for env, rr, ff, fs, sets, pre in [("docker", res, ffit, fser, CLUSTER, D), ("local", lres, lfit, lser, LOCAL, L)]:
        for r in rr:
            nodes = r["p"] if env == "docker" else ""
            cores = 2 * r["p"] if env == "docker" else r["p"]
            w.writerow([env, nodes, cores, f"{r['pp']:.2f}", r["n"], f"{r['T']:.2f}", f"{r['lo']:.2f}", f"{r['hi']:.2f}", f"{r['S']:.3f}", f"{r['ideal']:.2f}",
                        f"{r['E']:.3f}", f"{r['C']:.1f}" if env == "docker" else "", "" if r["f"] is None else f"{r['f']:.3f}", f"{r['afit']:.3f}", f"{ff:.4f}",
                        f"{r['aser']:.3f}", f"{fs:.4f}", f"{r['cpu']:.1f}", " ".join(f"{pre}{i:02d}" for i in sets[r['p']])])

# dataset scale
print("\nDocker dataset scale, 2 workers (4 cores), relative to the full year")
rows = {f: summ([f"{D}{i}" for i in ids]) for f, ids in SCALE.items()}
T1 = rows[1.0][0]
xs, ys = list(rows), [rows[f][0] for f in rows]
mx, my = st.mean(xs), st.mean(ys)
b = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / sum((x - mx) ** 2 for x in xs)
a = my - b * mx
print(f"linear fit T(f) = {a:.2f} + {b:.2f} f  (fraction-independent part {100*a/T1:.1f}% of T(1.0))")
OPS = ["m1", "m2", "m3", "o1", "o3"]
with (out / "scale.csv").open("w", newline="") as fh:
    w = csv.writer(fh)
    w.writerow(["fraction", "n_runs", "T_median_s", "T_min_s", "T_max_s", "T_rel", "data_ratio", "T_linear_ideal_s", "T_fit_s", "spark_submit_median_s",
                "shuffle_read_mib", "executor_cpu_s"] + [f"{o}_s" for o in OPS] + ["run_ids"])
    for f, ids in SCALE.items():
        m, lo, hi, sp = rows[f]
        rid = [f"{D}{i}" for i in ids]
        ss = st.median([float(l.split("\t")[5]) for l in open(sys.argv[3]) if l.split("\t")[0] in rid]) if len(sys.argv) > 3 else float("nan")
        shr = st.median([tot(i, "shuffle_read_bytes") / (1 << 20) for i in rid])
        cpu = st.median([tot(i, "executor_cpu_time_ms") / 1000 for i in rid])
        opm = [st.median([next(x["wall_time_s"] for x in rec(i)["runs"] if x["name"] == o) for i in rid]) for o in OPS]
        print(f"  f={f}: n {len(ids)}  T {m:.2f} [{lo:.2f}-{hi:.2f}] ({sp:.1f}%)  T/T(1) {m/T1:.3f}  data {f}  linear-ideal {f*T1:.2f}  fit {a+b*f:.2f}  submit {ss:.2f}  shufR {shr:.1f} MiB  CPU {cpu:.1f} s  ops "
              + " ".join(f"{o} {v:.2f}" for o, v in zip(OPS, opm)))
        w.writerow([f, len(ids), f"{m:.2f}", f"{lo:.2f}", f"{hi:.2f}", f"{m/T1:.3f}", f, f"{f*T1:.2f}", f"{a+b*f:.2f}", f"{ss:.2f}", f"{shr:.1f}", f"{cpu:.1f}"]
                   + [f"{v:.2f}" for v in opm] + [" ".join(rid)])
