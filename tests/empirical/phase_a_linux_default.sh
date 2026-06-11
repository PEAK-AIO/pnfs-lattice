#!/usr/bin/env bash
# Phase A -- Linux default writes.
#
# Mounts the lattice MDS on .15 with the default Linux NFSv4.1 client,
# runs a write workload across six sizes (4 KiB to 256 MiB) while
# tcpdumping the wire, then decodes:
#   * LAYOUTGET reply count (must be > 0)
#   * FF_FLAGS_STRIPE_LEASE (bit 0x10) occurrences in ff_flags
#     -- MUST be 0 after commit drop-stripe-lease-wire-extension
#
# Re-runs are idempotent: each invocation creates its own RESULT_DIR
# and unique canary files under $MOUNT.
#
# (c) PEAK:AIO Mark Klarzynski

HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib_common.sh
. "$HERE/lib_common.sh"

PHASE="phase_a_linux_default"
PCAP="$RESULT_DIR/${PHASE}.pcap"
DECODE="$RESULT_DIR/${PHASE}_decode.txt"
SUMMARY_JSON="$RESULT_DIR/${PHASE}_summary.json"

SIZES=(4K 16K 64K 1M 16M 256M)
CANARY_PREFIX=".phase_a_$$"

log "Phase A: Linux default writes against $MDS_HOST"

# -- preflight ----------------------------------------------------------
for tool in dd tcpdump tshark mount.nfs; do
    command -v "$tool" >/dev/null 2>&1 \
        || die "missing tool: $tool"
done

ensure_mount
start_capture "$PCAP"

# -- write workload -----------------------------------------------------
for sz in "${SIZES[@]}"; do
    target="${MOUNT}/${CANARY_PREFIX}_${sz}"
    log "dd: $sz -> $target"
    dd if=/dev/urandom of="$target" bs="$sz" count=1 \
        conv=fsync,fdatasync status=none 2>>"$RESULT_DIR/${PHASE}_dd.err" \
        && pass "write $sz" \
        || fail "write $sz"
    sync
done
sleep 2  # let async LAYOUTGET-after-WRITE settle

stop_capture

# -- decode -------------------------------------------------------------
tshark -r "$PCAP" -Y "nfs.opcode == 50" -V > "$DECODE" 2>/dev/null

lg_count=$(count_layoutget_replies "$PCAP")
ff_count=$(count_ff_stripe_lease_on_wire "$PCAP")

log "LAYOUTGET replies decoded: $lg_count"
log "FF_FLAGS_STRIPE_LEASE on wire: $ff_count"

# Must have observed at least one LAYOUTGET, otherwise the client did
# not actually go through pNFS and the wire-flag assertion is vacuous.
if [ "$lg_count" -ge 1 ]; then
    pass "captured at least 1 LAYOUTGET reply ($lg_count)"
else
    fail "no LAYOUTGET replies captured -- client did not use pNFS"
fi

# THE assertion: FF_FLAGS_STRIPE_LEASE must be 0 after the wire-flag
# removal commit.  Pre-commit baseline showed 25 occurrences.
assert_eq "FF_FLAGS_STRIPE_LEASE bit set on the wire" 0 "$ff_count"

# -- cleanup canary files (mount stays for chained phases) -------------
for sz in "${SIZES[@]}"; do
    rm -f "${MOUNT}/${CANARY_PREFIX}_${sz}" 2>/dev/null
done

write_summary_json "$PHASE" "$SUMMARY_JSON"

log "Phase A done: pass=$PASS fail=$FAIL"
exit $FAIL
