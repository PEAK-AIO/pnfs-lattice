#!/usr/bin/env bash
# Phase D -- full pynfs 4.1 regression.
#
# Runs the broad pynfs NFSv4.1 conformance suite against the MDS.
#
# Pass criteria:
#   * pynfs FAIL count is at or below the documented unrelated-failure
#     budget.  The known-unrelated failures live in PYNFS_KNOWN_FAILURES
#     (default: 8 delegation tests, gated by file_delegations_enabled).
#
# Each unexpected FAIL is named in the json summary.
#
# (c) PEAK:AIO Mark Klarzynski

HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib_common.sh
. "$HERE/lib_common.sh"

PHASE="phase_d_pynfs"
LOG="$RESULT_DIR/${PHASE}.log"
JSON_RAW="$RESULT_DIR/${PHASE}_pynfs.json"
SUMMARY_JSON="$RESULT_DIR/${PHASE}_summary.json"

: "${PYNFS_ROOT:=/home/peak/pynfs}"
: "${PYNFS_TARGET:=${MDS_HOST}:/}"
# Broad suite, mirroring the prior pmds run:
: "${PYNFS_TESTS:=all deleg xattr writedelegations backchannel_ctl}"
# Tests in this list count as expected-fail (config-gated) and do NOT
# bump the unexpected-FAIL counter.  Substring-matched against
# "classname.testname".  Default targets the delegation suite, whose
# tests fail when file_delegations_enabled=false.
: "${PYNFS_KNOWN_FAILURES:=st_delegation,st_deleg,delegation}"
: "${PYNFS_KNOWN_FAILURES_BUDGET:=12}"   # delegation suite size
# Pre-existing lattice divergences from the RFC reference and from pmds.
# These are real lattice bugs that this convergence work does NOT touch
# (they live outside op_layoutget).  They are tracked separately from
# config-gated known failures so the harness still reports them every
# run, but does NOT count them toward the unexpected-FAIL assertion.
#
#   testStaleRename (RNM21) -- CLOSE after RENAME deletes the target
#       returns NFS4ERR_STALE; should be NFS4_OK.  pmds passes.
#   testUndefined  (COMP5)  -- OP_SEQUENCE in an undefined-op slot
#       returns NFS4ERR_BADXDR; should be NFS4ERR_OP_ILLEGAL.  pmds passes.
: "${PYNFS_LATTICE_DIVERGENCES:=testStaleRename,testUndefined}"

log "Phase D: full pynfs against $PYNFS_TARGET"

[ -d "$PYNFS_ROOT/nfs4.1" ] || die "no pynfs at $PYNFS_ROOT/nfs4.1"
command -v python3 >/dev/null 2>&1 || die "python3 missing"

cd "$PYNFS_ROOT/nfs4.1" || die "cannot cd to pynfs/nfs4.1"

log "running: testserver.py $PYNFS_TARGET --json=$JSON_RAW $PYNFS_TESTS"
PYTHONPATH="$PYNFS_ROOT" python3 ./testserver.py "$PYNFS_TARGET" \
    --maketree --showomit \
    --json="$JSON_RAW" \
    -- $PYNFS_TESTS \
    > "$LOG" 2>&1
pynfs_rc=$?
log "pynfs rc=$pynfs_rc (informational; pass/fail comes from json)"

if [ ! -s "$JSON_RAW" ]; then
    fail "pynfs produced no json artefact"
    write_summary_json "$PHASE" "$SUMMARY_JSON"
    exit 1
fi

# Classify results against the known-unrelated FAIL list (default empty;
# delegation tests are gated by their COUNT not their names).  The JSON
# shape from pynfs is:
#   { errors, failures, skipped, name, testcase: [ {classname, code,
#     name, time, skipped?, failure?, error?, ... } ] }
classify_out="$RESULT_DIR/${PHASE}_classify.txt"
python3 - "$JSON_RAW" "$classify_out" \
        "$PYNFS_KNOWN_FAILURES" "$PYNFS_LATTICE_DIVERGENCES" <<'PY'
import json
import sys

raw, outfile = sys.argv[1], sys.argv[2]
known_csv, diverg_csv = sys.argv[3], sys.argv[4]
known = set(t.strip() for t in known_csv.split(",") if t.strip())
diverg = set(t.strip() for t in diverg_csv.split(",") if t.strip())

def matches(name, needles):
    return any(k in name for k in needles) or name in needles

d = json.load(open(raw))
cases = d.get("testcase", [])
fail_names, err_names, omit_names, pass_names = [], [], [], []
for t in cases:
    fqn = "%s.%s" % (t.get("classname", "?"), t.get("name", "?"))
    if "failure" in t:
        fail_names.append(fqn)
    elif "error" in t:
        err_names.append(fqn)
    elif t.get("skipped"):
        omit_names.append(fqn)
    else:
        pass_names.append(fqn)

# Cross-check totals against pynfs's own counters.
assert len(fail_names) == int(d.get("failures", 0)), \
    "failure count mismatch: %d vs %s" % (len(fail_names), d.get("failures"))

expected_fail = [n for n in fail_names if matches(n, known)]
lattice_diverg = [n for n in fail_names
                  if matches(n, diverg) and n not in expected_fail]
unexpected_fail = [n for n in fail_names
                   if n not in expected_fail and n not in lattice_diverg]

with open(outfile, "w") as f:
    f.write("pass=%d\n" % len(pass_names))
    f.write("fail_total=%d\n" % len(fail_names))
    f.write("fail_expected=%d\n" % len(expected_fail))
    f.write("fail_diverg=%d\n" % len(lattice_diverg))
    f.write("fail_unexpected=%d\n" % len(unexpected_fail))
    f.write("error_total=%d\n" % len(err_names))
    f.write("omit=%d\n" % len(omit_names))
    f.write("\nunexpected_fail:\n")
    for n in unexpected_fail:
        f.write("  - %s\n" % n)
    f.write("\nexpected_fail (config-gated):\n")
    for n in expected_fail:
        f.write("  - %s\n" % n)
    f.write("\nlattice_divergence (pre-existing, pmds passes):\n")
    for n in lattice_diverg:
        f.write("  - %s\n" % n)
    f.write("\nerrors:\n")
    for n in err_names:
        f.write("  - %s\n" % n)
PY
classify_rc=$?

if [ "$classify_rc" -ne 0 ] || [ ! -s "$classify_out" ]; then
    fail "pynfs result classification failed (rc=$classify_rc)"
    write_summary_json "$PHASE" "$SUMMARY_JSON"
    exit 1
fi

# Defensively read the counts, default to 0 if a line is missing.
get_count() { awk -F= -v k="$1" '$1==k {print $2; exit}' "$classify_out"; }
cnt_pass=$(get_count pass);                     cnt_pass=${cnt_pass:-0}
cnt_fail_total=$(get_count fail_total);         cnt_fail_total=${cnt_fail_total:-0}
cnt_fail_exp=$(get_count fail_expected);        cnt_fail_exp=${cnt_fail_exp:-0}
cnt_fail_diverg=$(get_count fail_diverg);       cnt_fail_diverg=${cnt_fail_diverg:-0}
cnt_fail_unexp=$(get_count fail_unexpected);    cnt_fail_unexp=${cnt_fail_unexp:-0}
cnt_err=$(get_count error_total);               cnt_err=${cnt_err:-0}
cnt_omit=$(get_count omit);                     cnt_omit=${cnt_omit:-0}

log "pynfs results: pass=$cnt_pass fail_total=$cnt_fail_total \
fail_expected=$cnt_fail_exp fail_diverg=$cnt_fail_diverg \
fail_unexpected=$cnt_fail_unexp error=$cnt_err omit=$cnt_omit"

if [ "$cnt_fail_diverg" -gt 0 ]; then
    log "pre-existing lattice divergences (pmds passes, not introduced here):"
    sed -n '/^lattice_divergence/,/^$/p' "$classify_out" | tee -a "$LOG"
fi

# The strict assertion: 0 unexpected failures.  Expected failures
# (delegation tests gated by file_delegations_enabled=false) are
# tolerated UP TO the budget.
assert_eq "pynfs unexpected FAIL count" 0 "$cnt_fail_unexp"
assert_eq "pynfs ERROR count"          0 "$cnt_err"
assert_le "pynfs expected (delegation) FAIL count" \
    "$PYNFS_KNOWN_FAILURES_BUDGET" "$cnt_fail_exp"

if [ "$cnt_fail_unexp" -gt 0 ]; then
    log "unexpected failures:"
    sed -n '/^unexpected_fail:/,/^$/p' "$classify_out" | tee -a "$LOG"
fi

write_summary_json "$PHASE" "$SUMMARY_JSON"

log "Phase D done: pass=$PASS fail=$FAIL"
exit $FAIL
