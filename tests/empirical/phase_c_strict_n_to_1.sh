#!/usr/bin/env bash
# Phase C -- Strict N-to-1 sync.
#
# Drives two parallel dd writers from the SAME client to the SAME file
# in disjoint byte ranges and verifies that NO CB_LAYOUTRECALL traffic
# is generated between them.  This validates the narrow-stripe-lease
# decoupling commit: the lease scope is now tracked at a->minlength
# granularity, so two same-client writers do not see each other as
# lease-scope conflicts.
#
# Pass criteria:
#   * LAYOUTGET reply count > 0 (test reached the pNFS path)
#   * CB_LAYOUTRECALL count == 0 (no spurious recalls between same-client
#     writers).  Pre-commit baseline showed 2.
#
# (c) PEAK:AIO Mark Klarzynski

HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib_common.sh
. "$HERE/lib_common.sh"

PHASE="phase_c_strict_n_to_1"
PCAP="$RESULT_DIR/${PHASE}.pcap"
DECODE="$RESULT_DIR/${PHASE}_decode.txt"
SUMMARY_JSON="$RESULT_DIR/${PHASE}_summary.json"

# Half each, written by two writers in parallel.
TARGET_BYTES=$((256 * 1024 * 1024))
HALF=$((TARGET_BYTES / 2))
TARGET="${MOUNT}/.phase_c_$$_n_to_1"

log "Phase C: strict N-to-1 (two writers, same client, $TARGET_BYTES bytes)"

for tool in dd tcpdump tshark mount.nfs; do
    command -v "$tool" >/dev/null 2>&1 \
        || die "missing tool: $tool"
done

ensure_mount

# Start capture BEFORE preallocate so the initial LAYOUTGET fires
# inside the captured window (otherwise the parallel writers reuse
# the client-cached layout and no LAYOUTGET PDUs appear on the wire).
start_capture "$PCAP"

# Pre-create the file at full size so both writers seek + write
# disjoint regions.
log "preallocate $TARGET ($TARGET_BYTES bytes)"
dd if=/dev/zero of="$TARGET" bs=1M count=$((TARGET_BYTES / 1024 / 1024)) \
    status=none 2>>"$RESULT_DIR/${PHASE}_prealloc.err" \
    || die "preallocate failed"
sync

# Writer 1: bytes [0, HALF)
# Writer 2: bytes [HALF, TARGET_BYTES)
log "launching two parallel writers"
( dd if=/dev/urandom of="$TARGET" bs=1M count=$((HALF / 1024 / 1024)) \
    seek=0 conv=notrunc,fsync status=none \
    2>>"$RESULT_DIR/${PHASE}_w1.err" ) &
W1=$!
( dd if=/dev/urandom of="$TARGET" bs=1M count=$((HALF / 1024 / 1024)) \
    seek=$((HALF / 1024 / 1024)) conv=notrunc,fsync status=none \
    2>>"$RESULT_DIR/${PHASE}_w2.err" ) &
W2=$!

wait "$W1" && pass "writer 1 finished" || fail "writer 1"
wait "$W2" && pass "writer 2 finished" || fail "writer 2"
sync
sleep 2  # let any CB_LAYOUTRECALL traffic arrive

stop_capture

# -- decode -------------------------------------------------------------
tshark -r "$PCAP" -Y "nfs.opcode == 50 || nfs.cb_opcode == 5" -V \
    > "$DECODE" 2>/dev/null

lg_count=$(count_layoutget_replies "$PCAP")
cb_count=$(count_cb_layoutrecall "$PCAP")

log "LAYOUTGET replies decoded: $lg_count"
log "CB_LAYOUTRECALL refs: $cb_count"

if [ "$lg_count" -ge 1 ]; then
    pass "captured at least 1 LAYOUTGET reply ($lg_count)"
else
    fail "no LAYOUTGET replies captured -- client did not use pNFS"
fi

# THE assertion: no spurious CB_LAYOUTRECALL between same-client writers.
assert_eq "CB_LAYOUTRECALL count between same-client writers" 0 "$cb_count"

rm -f "$TARGET" 2>/dev/null

write_summary_json "$PHASE" "$SUMMARY_JSON"

log "Phase C done: pass=$PASS fail=$FAIL"
exit $FAIL
