#!/usr/bin/env bash
# Phase B -- Flex Files.
#
# Exercises the LAYOUT4_FLEX_FILES path in op_layoutget() using pynfs's
# Flex-Files Layout Access (FFLA) and related tests.  We use pynfs
# because the standard Linux kernel client does not expose a knob to
# request LAYOUT4_FLEX_FILES (4); pynfs sends the correct loga_layout_type
# unconditionally.
#
# Pass criteria:
#   * pynfs FFLA + FFLG subset returns 100% pass (any failures fail
#     the phase; record per-test status in the json summary).
#
# Note: the broader pynfs run is Phase D; this phase intentionally
# scopes itself to the flex-files layout coverage.
#
# (c) PEAK:AIO Mark Klarzynski

HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib_common.sh
. "$HERE/lib_common.sh"

PHASE="phase_b_flex_files"
LOG="$RESULT_DIR/${PHASE}.log"
JSON_RAW="$RESULT_DIR/${PHASE}_pynfs.json"
SUMMARY_JSON="$RESULT_DIR/${PHASE}_summary.json"

: "${PYNFS_ROOT:=/home/peak/pynfs}"
: "${PYNFS_TARGET:=${MDS_HOST}:/}"     # use root export
# pynfs categorises Flex-Files tests behind the `flex` flag
# (e.g. FFLA1 testFlexLayoutTestAccess, FFLG2 testFlexLayoutGet).
: "${PYNFS_TESTS:=flex}"

log "Phase B: pynfs flex-files subset against $PYNFS_TARGET"

[ -d "$PYNFS_ROOT/nfs4.1" ] || die "no pynfs at $PYNFS_ROOT/nfs4.1"
command -v python3 >/dev/null 2>&1 || die "python3 missing"

cd "$PYNFS_ROOT/nfs4.1" || die "cannot cd to pynfs/nfs4.1"

# pynfs testserver.py emits a json artefact when --json is given.  The
# testserver imports rpc.rpc relative to the pynfs root, so we have to
# put $PYNFS_ROOT on PYTHONPATH; cd-ing to nfs4.1 alone is not enough.
log "running: testserver.py $PYNFS_TARGET --json=$JSON_RAW $PYNFS_TESTS"
PYTHONPATH="$PYNFS_ROOT" python3 ./testserver.py "$PYNFS_TARGET" \
    --maketree --showomit \
    --json="$JSON_RAW" \
    -- $PYNFS_TESTS \
    > "$LOG" 2>&1
pynfs_rc=$?
log "pynfs rc=$pynfs_rc (non-zero is informational; pass/fail comes from json)"

if [ ! -s "$JSON_RAW" ]; then
    fail "pynfs produced no json artefact"
    write_summary_json "$PHASE" "$SUMMARY_JSON"
    exit 1
fi

# pynfs JSON shape:
#   { "errors": N, "failures": N, "skipped": N, "name": "...",
#     "testcase": [ {"classname":..., "code":..., "name":...,
#                    "time":..., "skipped"?:1, "failure"?:..., ... }, ... ] }
read -r ff_pass ff_fail ff_omit ff_err <<EOF
$(python3 -c "import json,sys
d=json.load(open(sys.argv[1]))
cases=d.get('testcase',[])
fail=int(d.get('failures',0))
err=int(d.get('errors',0))
omit=int(d.get('skipped',0))
pas=len(cases)-fail-err-omit
print(pas, fail, omit, err)" "$JSON_RAW")
EOF

log "pynfs FF results: pass=$ff_pass fail=$ff_fail omit=$ff_omit error=$ff_err"

assert_eq "pynfs flex-files FAIL count" 0 "$ff_fail"
assert_eq "pynfs flex-files ERROR count" 0 "$ff_err"

# Surface a list of failed/errored test names if any.
if [ "$ff_fail" -gt 0 ] || [ "$ff_err" -gt 0 ]; then
    python3 -c "import json,sys
d=json.load(open(sys.argv[1]))
for t in d.get('testcase',[]):
    if 'failure' in t or 'error' in t:
        msg = (t.get('failure') or t.get('error') or '')
        if isinstance(msg, dict):
            msg = msg.get('#text', '') or msg.get('message', '')
        print('  - %s.%s :: %s' % (t.get('classname','?'), t.get('name','?'), str(msg)[:200]))" \
        "$JSON_RAW" | tee -a "$LOG"
fi

write_summary_json "$PHASE" "$SUMMARY_JSON"

log "Phase B done: pass=$PASS fail=$FAIL"
exit $FAIL
