#!/usr/bin/env python3
"""LOG.md Runs rows for the scalability runs (run.json + stages.json + elapsed.tsv).

usage: rows.py <runs-dir> <elapsed.tsv> <role-file>
role-file lines: <run-id>\t<variable under test>\t<notes prefix>
"""
import json, sys
from pathlib import Path

MiB = 1 << 20
runs = Path(sys.argv[1])
el = {l.split("\t")[0]: l.rstrip("\n").split("\t") for l in Path(sys.argv[2]).read_text().splitlines()}
for line in Path(sys.argv[3]).read_text().splitlines():
    rid, var, note = line.split("\t")
    r = json.loads((runs / rid / "run.json").read_text())
    S = json.loads((runs / rid / "stages.json").read_text())
    t = lambda k: sum(x[k] for x in r["runs"])
    m = r["master"]
    if m.startswith("spark://"):
        w = int(el[rid][2])
        env = f"Docker, {w} workers × 2 cores"
    else:
        env = f"local `{m}`"
    frac = r["args"]["fraction"]
    scale = "2025, all ops" if frac == "1.0" else f"2025 × {frac}, all ops"
    ops = " · ".join(f"{x['name']} {x['wall_time_s']:.2f}" for x in r["runs"])
    s1 = next(s for s in S if s["profile"] == "m1" and s["input_bytes"] > 0)
    gc = 100 * t("jvm_gc_time_ms") / t("executor_run_time_ms")
    tail = (f"{ops}. spark-submit / make run elapsed {float(el[rid][5]):.2f} s. Executor run / CPU {t('executor_run_time_ms')/1000:.1f} / "
            f"{t('executor_cpu_time_ms')/1000:.1f} s. M1 scan stage {s1['num_tasks']} tasks, {s1['duration_ms']/1000:.2f} s, CPU {s1['executor_cpu_time_ms']:,} ms.")
    notes = f"{note} {tail}".strip()
    print(f"| {rid} | {r['git_sha']} | {env} | {scale} | {var} | {t('wall_time_s'):.2f} | {t('shuffle_read_bytes')/MiB:.1f} / {t('shuffle_write_bytes')/MiB:.1f} MiB | "
          f"{t('num_tasks')} | {t('disk_spilled_bytes')/MiB:.1f} MiB | {gc:.1f} | {notes} |")
