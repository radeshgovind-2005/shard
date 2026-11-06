#!/usr/bin/env python3
"""O3's stages from <runs-dir>/<run-id>/run.json (bytes, exact), plus the block totals. usage: stages-block.py <runs-dir> <run-id>"""
import json, sys
from pathlib import Path
run = json.loads((Path(sys.argv[1]) / sys.argv[2] / "run.json").read_text())
print(f"# from {sys.argv[2]}/run.json; args: {json.dumps(run['args'])}")
for b in run["runs"]:
    print("# block", {k: v for k, v in b.items() if k != "stages"})
    print(f"{'stage':>5} {'tasks':>5} {'run_ms':>7} {'shuffle_read_B':>15} {'shuffle_write_B':>15} {'output_B':>10} {'spill_disk_B':>12}")
    for s in b["stages"]:
        print(f"{s['stage_id']:5} {s['num_tasks']:5} {s['executor_run_time_ms']:7} {s['shuffle_read_bytes']:15} "
              f"{s['shuffle_write_bytes']:15} {s['output_bytes']:10} {s['disk_spilled_bytes']:12}")
