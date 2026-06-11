#!/usr/bin/env bash
# Shared helpers for the empirical-test harness.
#
# Conventions
#   - Every phase script sources this file.
#   - Pass/fail accounting via increment_pass / increment_fail.
#   - Capture artefacts (pcaps, decode txt, summary json) land under
#     $RESULT_DIR (default: /tmp/lattice_empirical_<ts>).
#   - Cleanup trap unmounts and kills any leftover tcpdump on exit.
#   - No `set -e` -- assertions are accounted, not aborted on.
#
# (c) PEAK:AIO Mark Klarzynski

set -u
set -o pipefail

# -- environment --------------------------------------------------------
: "${MDS_HOST:=192.168.100.11}"     # lattice MDS under test
: "${MOUNT:=/mnt/pnfs_empirical}"   # local mount point on client (.15)
: "${TEST_IFACE:=}"                 # auto-detect when empty
: "${RESULT_DIR:=/tmp/lattice_empirical_$(date +%Y%m%d_%H%M%S)}"
: "${SUDO:=sudo}"

PASS=0
FAIL=0
SKIP=0

mkdir -p "$RESULT_DIR"

# -- logging ------------------------------------------------------------
log()  { printf '[empirical %(%H:%M:%S)T] %s\n' -1 "$*"; }
warn() { printf '[empirical %(%H:%M:%S)T] WARN: %s\n' -1 "$*" >&2; }
die()  { printf '[empirical %(%H:%M:%S)T] FATAL: %s\n' -1 "$*" >&2; exit 1; }

# -- pass/fail accounting ----------------------------------------------
pass() { PASS=$((PASS+1)); log "PASS: $*"; }
fail() { FAIL=$((FAIL+1)); log "FAIL: $*"; }
skip() { SKIP=$((SKIP+1)); log "SKIP: $*"; }

assert_eq() {
    local label="$1" expected="$2" actual="$3"
    if [ "$expected" = "$actual" ]; then
        pass "$label (expected=$expected actual=$actual)"
    else
        fail "$label (expected=$expected actual=$actual)"
    fi
}

assert_le() {
    local label="$1" limit="$2" actual="$3"
    if [ "$actual" -le "$limit" ]; then
        pass "$label (limit=$limit actual=$actual)"
    else
        fail "$label (limit=$limit actual=$actual)"
    fi
}

# -- interface auto-detect ---------------------------------------------
detect_iface() {
    if [ -n "$TEST_IFACE" ]; then
        echo "$TEST_IFACE"
        return
    fi
    # Pick the interface that owns the route to MDS_HOST.
    local iface
    iface=$(ip -o route get "$MDS_HOST" 2>/dev/null \
            | awk '{ for (i=1;i<=NF;i++) if ($i=="dev") { print $(i+1); exit } }')
    [ -n "$iface" ] || die "could not detect interface to $MDS_HOST"
    echo "$iface"
}

# -- mount/umount ------------------------------------------------------
ensure_mount() {
    local opts="${1:-vers=4.2,rsize=1048576,wsize=1048576}"
    if mountpoint -q "$MOUNT"; then
        log "already mounted: $MOUNT"
        return 0
    fi
    $SUDO mkdir -p "$MOUNT"
    log "mounting ${MDS_HOST}:/ -> $MOUNT (opts=$opts)"
    $SUDO mount -t nfs -o "$opts" "${MDS_HOST}:/" "$MOUNT" \
        || die "mount failed"
}

ensure_umount() {
    if mountpoint -q "$MOUNT"; then
        log "umount $MOUNT"
        $SUDO umount "$MOUNT" 2>/dev/null \
            || $SUDO umount -fl "$MOUNT" 2>/dev/null \
            || warn "umount $MOUNT failed"
    fi
}

# -- tcpdump helpers ---------------------------------------------------
TCPDUMP_PID=""

start_capture() {
    local pcap="$1" iface
    iface=$(detect_iface)
    log "tcpdump start: iface=$iface pcap=$pcap"
    $SUDO tcpdump -i "$iface" -s 0 -w "$pcap" \
        "host $MDS_HOST and (port 2049 or port 1110)" \
        >/dev/null 2>&1 &
    TCPDUMP_PID=$!
    # Wait until tcpdump is actually capturing.
    sleep 1
    kill -0 "$TCPDUMP_PID" 2>/dev/null || die "tcpdump failed to start"
}

stop_capture() {
    if [ -n "$TCPDUMP_PID" ] && kill -0 "$TCPDUMP_PID" 2>/dev/null; then
        log "tcpdump stop: pid=$TCPDUMP_PID"
        $SUDO kill -INT "$TCPDUMP_PID" 2>/dev/null || true
        wait "$TCPDUMP_PID" 2>/dev/null || true
    fi
    TCPDUMP_PID=""
}

# -- tshark decode helpers ---------------------------------------------
# Count occurrences of an NFSv4 opcode/field across a pcap.
count_in_pcap() {
    local pcap="$1" filter="$2"
    tshark -r "$pcap" -Y "$filter" 2>/dev/null | wc -l | awk "{print \$1}"
}

# Count LAYOUTGET reply (op 50) PDUs.  Some tshark builds do not expose
# the rpc.msgtyp filter, so we count occurrences of the LAYOUTGET opcode
# label in the verbose decode -- it appears once per LAYOUTGET PDU.
count_layoutget_replies() {
    local pcap="$1" n
    n=$(tshark -r "$pcap" -V 2>/dev/null \
        | grep -cE "Opcode: LAYOUTGET[ (]*50\)?") || n=0
    echo "$n"
}

# Count CB_LAYOUTRECALL PDUs.  Backchannel callback opcode field-name
# varies across tshark versions; the literal "CB_LAYOUTRECALL" appears
# in the verbose decode regardless, so count that.
count_cb_layoutrecall() {
    local pcap="$1" n
    n=$(tshark -r "$pcap" -V 2>/dev/null \
        | grep -cE "CB_LAYOUTRECALL") || n=0
    echo "$n"
}

# Count occurrences of FF_FLAGS_STRIPE_LEASE (bit 0x10) being set in any
# ff_flags field of any LAYOUTGET reply.
#
# We extract every "ff_flags: 0x????????" line from the verbose tshark
# decode and test bit 0x10.  This avoids fragile regexes over hex.  The
# upstream wireshark NFS dissector emits this label for both files and
# flex-files layout types.
count_ff_stripe_lease_on_wire() {
    local pcap="$1" n
    n=$(tshark -r "$pcap" -V 2>/dev/null \
        | python3 -c '
import re, sys
hits = 0
pattern = re.compile(r"ff_flags[^0-9a-fA-Fx]*0x([0-9a-fA-F]+)")
for line in sys.stdin:
    m = pattern.search(line)
    if m and (int(m.group(1), 16) & 0x10):
        hits += 1
print(hits)
') || n=0
    echo "$n"
}

# -- summary writer ----------------------------------------------------
write_summary_json() {
    local phase="$1" outfile="$2"
    local total=$((PASS+FAIL))
    cat >"$outfile" <<EOF
{
  "phase": "$phase",
  "mds_host": "$MDS_HOST",
  "pass": $PASS,
  "fail": $FAIL,
  "skip": $SKIP,
  "total": $total,
  "completed_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "result_dir": "$RESULT_DIR"
}
EOF
}

# -- cleanup trap ------------------------------------------------------
empirical_cleanup() {
    stop_capture
    ensure_umount
}
trap empirical_cleanup EXIT
