#!/usr/bin/env bash
# Master orchestrator -- runs Phase A, B, C in sequence, then summarises.
#
# Exit code:
#   0 -- every phase reported FAIL=0
#   1 -- one or more phases reported FAIL>=1 or did not produce a summary
#
# Result tree:
#   $RESULT_DIR/
#     phase_a_linux_default_summary.json
#     phase_a_linux_default.pcap
#     phase_a_linux_default_decode.txt
#     phase_b_flex_files_summary.json
#     phase_b_flex_files_pynfs.json
#     phase_b_flex_files.log
#     phase_c_strict_n_to_1_summary.json
#     phase_c_strict_n_to_1.pcap
#     phase_c_strict_n_to_1_decode.txt
#     overall_summary.json
#     PASS_REPORT.md
#
# (c) PEAK:AIO Mark Klarzynski

set -u
set -o pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"

: "${RESULT_DIR:=/tmp/lattice_empirical_$(date +%Y%m%d_%H%M%S)}"
export RESULT_DIR
mkdir -p "$RESULT_DIR"

OVERALL_LOG="$RESULT_DIR/run_all.log"

log() { printf '[run_all %(%H:%M:%S)T] %s\n' -1 "$*" | tee -a "$OVERALL_LOG"; }

log "RESULT_DIR=$RESULT_DIR"
log "MDS_HOST=${MDS_HOST:-192.168.100.11}"

phase_rc=0

run_phase() {
    local script="$1" name="$2"
    log "--- $name ---"
    bash "$HERE/$script" 2>&1 | tee -a "$OVERALL_LOG"
    local rc=${PIPESTATUS[0]}
    if [ "$rc" -ne 0 ]; then
        log "$name returned non-zero ($rc)"
        phase_rc=1
    fi
}

run_phase phase_a_linux_default.sh "Phase A: Linux default"
run_phase phase_b_flex_files.sh    "Phase B: Flex Files"
run_phase phase_c_strict_n_to_1.sh "Phase C: strict N-to-1"
run_phase phase_d_pynfs.sh         "Phase D: full pynfs"

# Aggregate the three phase summaries into one report.
python3 - "$RESULT_DIR" <<'PY' | tee -a "$OVERALL_LOG"
import json
import os
import sys
from pathlib import Path

root = Path(sys.argv[1])
totals = {"pass": 0, "fail": 0, "skip": 0, "total": 0}
phases = []
for p in sorted(root.glob("phase_*_summary.json")):
    try:
        d = json.load(p.open())
    except Exception as exc:
        d = {"phase": p.stem, "pass": 0, "fail": 1, "skip": 0,
             "total": 1, "error": str(exc)}
    phases.append(d)
    for k in totals:
        totals[k] += int(d.get(k, 0))

overall = {"phases": phases, "totals": totals}
(root / "overall_summary.json").write_text(json.dumps(overall, indent=2))

# Markdown report.
md = ["# Lattice empirical pass report",
      "",
      f"MDS host: `{phases[0].get('mds_host','?') if phases else '?'}`",
      f"Result dir: `{root}`",
      "",
      "## Per-phase",
      "",
      "| phase | pass | fail | skip | total |",
      "|---|---|---|---|---|"]
for p in phases:
    md.append("| {phase} | {pass} | {fail} | {skip} | {total} |".format(
        **{k: p.get(k, '?') for k in ('phase','pass','fail','skip','total')}))
md += ["",
       "## Totals",
       "",
       f"- pass: **{totals['pass']}**",
       f"- fail: **{totals['fail']}**",
       f"- skip: **{totals['skip']}**",
       f"- total: **{totals['total']}**",
       "",
       "## Verdict",
       "",
       "**PASS**" if totals['fail'] == 0 else "**FAIL**"]
(root / "PASS_REPORT.md").write_text("\n".join(md) + "\n")
print(json.dumps(overall, indent=2))
print("\n--- PASS_REPORT.md ---")
print("\n".join(md))
PY

if [ "$phase_rc" -eq 0 ]; then
    log "overall: PASS"
    exit 0
else
    log "overall: FAIL (one or more phases reported failures)"
    exit 1
fi
