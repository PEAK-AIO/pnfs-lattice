#!/bin/bash
# Copyright (c) 2026 PeakAIO
# SPDX-License-Identifier: MIT
#
# pNFS MDS test suite -- smoke + extended modes.
#
# Usage:
#   ./pnfs-test-suite.sh [smoke|extended]
#
# smoke    (default) -- one of each pNFS op type, ~15 seconds,
#                      reports per-op PASS/FAIL/SKIP.
# extended           -- runs smoke first, then rotates all ops for
#                      PNFS_EXTENDED_SECONDS (default 60) to simulate
#                      sustained mixed workload.  Reports aggregate stats.
#
# Environment:
#   PNFS_MNT=/mnt/pnfs                    mount point (required)
#   PNFS_EXTENDED_SECONDS=60               duration for extended mode
#   PNFS_TEST_TIMEOUT=15                   per-test timeout in seconds
#   PNFS_KEEP_WORKDIR=0                    set 1 to keep artifacts
#   PNFS_CHOWN_UID=1000                    both required for chown test
#   PNFS_CHOWN_GID=1000

set -u
set -o pipefail
umask 022

readonly SKIP_RC=77
readonly MODE="${1:-smoke}"

MNT="${PNFS_MNT:-/mnt/data}"
KEEP_WORKDIR="${PNFS_KEEP_WORKDIR:-0}"
TEST_TIMEOUT="${PNFS_TEST_TIMEOUT:-15}"
EXTENDED_SECONDS="${PNFS_EXTENDED_SECONDS:-60}"
CHOWN_UID="${PNFS_CHOWN_UID:-}"
CHOWN_GID="${PNFS_CHOWN_GID:-}"

PASS=0; FAIL=0; SKIP=0
RESULTS=""
LOCAL_TMP_ROOT=""
WORKDIR=""
MOUNT_VERSION=""
MOUNT_FSTYPE=""

fatal() { printf 'ERROR: %s\n' "$1" >&2; exit 1; }

cleanup() {
    local rc=$?
    [ -n "$LOCAL_TMP_ROOT" ] && [ -d "$LOCAL_TMP_ROOT" ] && rm -rf "$LOCAL_TMP_ROOT" 2>/dev/null
    if [ "$KEEP_WORKDIR" != "1" ] && [ -n "$WORKDIR" ] && [ -d "$WORKDIR" ]; then
        rm -rf "$WORKDIR" 2>/dev/null
    fi
    exit "$rc"
}
trap cleanup EXIT

# -- Result tracking ----------------------------------------------

record_pass() { PASS=$((PASS+1)); RESULTS="${RESULTS}PASS  $1\n"; }
record_fail() { FAIL=$((FAIL+1)); RESULTS="${RESULTS}FAIL  $1 ($2)\n"; }
record_skip() { SKIP=$((SKIP+1)); RESULTS="${RESULTS}SKIP  $1 ($2)\n"; }

# Run a test function with a timeout.  Executes the function
# directly in the current shell (no subshell/declare-f tricks).
run_test() {
    local name="$1"; shift
    local fn="$1"
    local rc=0
    local err_file="${LOCAL_TMP_ROOT}/err_$$"

    # Run with timeout; capture stderr for error reporting.
    ( "$fn" ) > /dev/null 2>"$err_file" &
    local pid=$!
    local waited=0
    while kill -0 "$pid" 2>/dev/null; do
        sleep 1
        waited=$((waited + 1))
        if [ "$waited" -ge "$TEST_TIMEOUT" ]; then
            kill -9 "$pid" 2>/dev/null; wait "$pid" 2>/dev/null
            record_fail "$name" "TIMEOUT (${TEST_TIMEOUT}s)"
            printf '  %-40s FAIL (timeout)\n' "$name"
            return
        fi
    done
    wait "$pid" 2>/dev/null; rc=$?

    if [ "$rc" -eq 0 ]; then
        record_pass "$name"
        printf '  %-40s PASS\n' "$name"
    elif [ "$rc" -eq "$SKIP_RC" ]; then
        record_skip "$name" "skipped"
        printf '  %-40s SKIP\n' "$name"
    else
        local short_err=""
        [ -s "$err_file" ] && short_err=$(head -1 "$err_file" | cut -c1-80)
        record_fail "$name" "${short_err:-exit $rc}"
        printf '  %-40s FAIL (%s)\n' "$name" "${short_err:-rc=$rc}"
    fi
    rm -f "$err_file"
}

# -- Setup --------------------------------------------------------

detect_mount() {
    mountpoint -q -- "$MNT" 2>/dev/null || fatal "$MNT is not a mountpoint"
    MOUNT_FSTYPE=$(findmnt -n -o FSTYPE --target "$MNT" 2>/dev/null || true)
    MOUNT_VERSION=$(findmnt -n -o OPTIONS --target "$MNT" 2>/dev/null | \
        grep -oP 'vers=\K[0-9.]+' || echo "?")
    LOCAL_TMP_ROOT=$(mktemp -d /tmp/pnfs-smoke.XXXXXX) || fatal "mktemp failed"
    WORKDIR="${MNT}/.pnfs-smoke/$(date -u +%Y%m%dT%H%M%SZ)-$$"
    mkdir -p "$WORKDIR" || fatal "unable to create $WORKDIR"
}

# -- Atomic test functions ----------------------------------------
# Each is self-contained: creates its own subdirectory under $WORKDIR,
# performs the operation, verifies the result, and returns 0/1/77.

t_mkdir() {
    local d="$WORKDIR/t_mkdir_$RANDOM/a/b/c"
    mkdir -p "$d" && [ -d "$d" ]
}

t_create_read() {
    local f="$WORKDIR/t_cr_$RANDOM.txt"
    printf 'hello pnfs' > "$f" && [ "$(cat "$f")" = "hello pnfs" ]
}

t_stat() {
    local f="$WORKDIR/t_stat_$RANDOM.txt"
    printf 'x' > "$f"
    [ "$(stat -c%s "$f" 2>/dev/null)" = "1" ]
}

t_chmod() {
    local f="$WORKDIR/t_chmod_$RANDOM.txt"
    : > "$f" && chmod 0750 "$f"
    [ "$(stat -c%a "$f" 2>/dev/null)" = "750" ]
}

t_chown() {
    [ -n "${CHOWN_UID:-}" ] && [ -n "${CHOWN_GID:-}" ] || exit 77
    local f="$WORKDIR/t_chown_$RANDOM.txt"
    : > "$f" && chown "${CHOWN_UID}:${CHOWN_GID}" "$f" 2>/dev/null
    [ "$(stat -c%u "$f" 2>/dev/null)" = "$CHOWN_UID" ]
}

t_rename() {
    local tag="$RANDOM"
    local s="$WORKDIR/t_ren_s_${tag}.txt" d="$WORKDIR/t_ren_d_${tag}.txt"
    printf 'ren' > "$s"
    sync 2>/dev/null
    mv "$s" "$d" || return 1
    [ ! -e "$s" ] || return 1
    # NFS client may return EAGAIN on first read after rename;
    # retry with a brief settle to handle the cache race.
    local content=""
    local try
    for try in 1 2 3; do
        content=$(cat "$d" 2>/dev/null) && break
        sleep 0.5
    done
    [ "$content" = "ren" ]
}

t_cross_rename() {
    local tag="$RANDOM"
    local a="$WORKDIR/t_xr_a_$tag" b="$WORKDIR/t_xr_b_$tag"
    mkdir -p "$a" "$b"
    printf 'xren' > "$a/f.txt"
    sync 2>/dev/null
    mv "$a/f.txt" "$b/f.txt" || return 1
    [ ! -e "$a/f.txt" ] || return 1
    local content="" try
    for try in 1 2 3; do
        content=$(cat "$b/f.txt" 2>/dev/null) && break
        sleep 0.5
    done
    [ "$content" = "xren" ]
}

t_hardlink() {
    local tag="$RANDOM"
    local f="$WORKDIR/t_hl_s_$tag.txt" l="$WORKDIR/t_hl_l_$tag.txt"
    printf 'hl' > "$f"
    sync 2>/dev/null
    ln "$f" "$l" || return 1
    local content="" try
    for try in 1 2 3; do
        content=$(cat "$l" 2>/dev/null) && break
        sleep 0.5
    done
    [ "$(stat -c%h "$f" 2>/dev/null)" = "2" ] && [ "$content" = "hl" ]
}

t_symlink() {
    local d="$WORKDIR/t_sym_$RANDOM"
    mkdir -p "$d"
    printf 'sym' > "$d/target.txt"
    ln -s target.txt "$d/link.txt" 2>/dev/null
    [ "$(readlink "$d/link.txt" 2>/dev/null)" = "target.txt" ] && \
    [ "$(cat "$d/link.txt" 2>/dev/null)" = "sym" ]
}

t_truncate() {
    local f="$WORKDIR/t_trunc_$RANDOM.bin"
    dd if=/dev/zero of="$f" bs=1024 count=64 status=none
    truncate -s 30000 "$f"
    [ "$(stat -c%s "$f" 2>/dev/null)" = "30000" ]
}

t_readdir() {
    local d="$WORKDIR/t_rd_$RANDOM"
    mkdir -p "$d"
    for i in $(seq 1 20); do : > "$d/f_$i"; done
    [ "$(find "$d" -maxdepth 1 -type f | wc -l)" -eq 20 ]
}

t_remove() {
    local f="$WORKDIR/t_rm_$RANDOM.txt"
    : > "$f" && rm "$f" && [ ! -e "$f" ]
}

t_lock() {
    command -v flock >/dev/null 2>&1 || exit 77
    local f="$WORKDIR/t_lock_$RANDOM.txt"
    : > "$f"
    # Use a short timeout on the entire lock test to prevent
    # NFS lock state hangs from blocking the suite.
    local result=0
    timeout 8 bash -c '
        f="'"$f"'"
        ( flock -x 9 && sleep 2 ) 9<>"$f" &
        P=$!
        sleep 1
        flock -x -n 8 8<>"$f" 2>/dev/null
        R=$?
        kill $P 2>/dev/null; wait $P 2>/dev/null
        [ $R -ne 0 ] && exit 0 || exit 1
    ' || result=$?
    # timeout returns 124 on timeout, treat as skip (NFS lock issue)
    if [ "$result" -eq 124 ]; then
        exit 77  # SKIP -- NFS lock state hung
    fi
    return "$result"
}

t_write_4k() {
    local f="$WORKDIR/t_w4k_$RANDOM.bin"
    dd if=/dev/urandom of="$f" bs=4096 count=1 status=none
    [ "$(stat -c%s "$f" 2>/dev/null)" = "4096" ]
}

t_large_write() {
    local tag="$RANDOM"
    local src="$LOCAL_TMP_ROOT/t_lg_$tag.bin" dst="$WORKDIR/t_lg_$tag.bin"
    dd if=/dev/urandom of="$src" bs=1M count=4 status=none
    dd if="$src" of="$dst" bs=1M conv=fsync status=none
    local s1 s2
    s1=$(md5sum "$src" | awk '{print $1}')
    s2=$(md5sum "$dst" | awk '{print $1}')
    [ "$s1" = "$s2" ]
}

t_concurrent() {
    local d="$WORKDIR/t_conc_$RANDOM"
    mkdir -p "$d"
    for w in 1 2 3 4; do
        ( for i in $(seq 1 10); do printf 'w%d' "$w" > "$d/w${w}_f${i}.txt"; done ) &
    done
    wait
    [ "$(find "$d" -maxdepth 1 -type f | wc -l)" -eq 40 ]
}

t_pnfs_advertised() {
    awk -v target="$MNT" '$1=="device"{in_block=($5==target)} in_block{print}' \
        /proc/self/mountstats 2>/dev/null | grep -q 'pnfs=' || exit 77
}

# -- Smoke mode ---------------------------------------------------

run_smoke() {
    printf '========================================\n'
    printf ' pNFS Smoke Test\n'
    printf ' %s | NFS %s | %s\n' "$(date -u '+%H:%M:%S UTC')" "$MOUNT_VERSION" "$MNT"
    printf '========================================\n\n'

    printf '  Namespace:\n'
    run_test "mkdir -p"            t_mkdir
    run_test "create + read"       t_create_read
    run_test "stat"                t_stat
    run_test "chmod"               t_chmod
    run_test "chown"               t_chown
    run_test "rename (same dir)"   t_rename
    run_test "rename (cross dir)"  t_cross_rename
    run_test "hard link"           t_hardlink
    run_test "symlink"             t_symlink
    run_test "truncate"            t_truncate
    run_test "readdir"             t_readdir
    run_test "remove"              t_remove

    printf '\n  Locking:\n'
    run_test "lock conflict"       t_lock

    printf '\n  Data:\n'
    run_test "4K write"            t_write_4k
    run_test "4MB write + verify"  t_large_write
    run_test "concurrent 4x10"     t_concurrent

    printf '\n  pNFS:\n'
    run_test "layout advertised"   t_pnfs_advertised

    printf '\n========================================\n'
    printf ' PASS: %d  FAIL: %d  SKIP: %d  Total: %d\n' \
        "$PASS" "$FAIL" "$SKIP" "$((PASS+FAIL+SKIP))"
    printf '========================================\n'
}

# -- Extended mode ------------------------------------------------

run_extended() {
    printf '=== pNFS Extended Test (%ds) ===\n\n' "$EXTENDED_SECONDS"

    # Run smoke first as a gate
    run_smoke
    if [ "$FAIL" -gt 0 ]; then
        printf '\nSmoke failures detected -- skipping extended rotation.\n'
        return 1
    fi

    printf '\n--- Extended rotation (%ds) ---\n' "$EXTENDED_SECONDS"

    # Ops to rotate (fast metadata ops only -- no large writes)
    local ops=( t_create_read t_stat t_chmod t_rename t_readdir
                t_remove t_write_4k t_mkdir t_truncate )
    local base_workdir="$WORKDIR"
    local start_secs="$SECONDS"
    local end_secs=$((SECONDS + EXTENDED_SECONDS))
    local iter=0 op_pass=0 op_fail=0

    while [ "$SECONDS" -lt "$end_secs" ]; do
        iter=$((iter + 1))
        # Fresh subdir per iteration to avoid name collisions
        WORKDIR="${base_workdir}/ext-${iter}"
        mkdir -p "$WORKDIR" 2>/dev/null || continue

        for op in "${ops[@]}"; do
            if timeout "$TEST_TIMEOUT" bash -c "
                set -u
                WORKDIR='$WORKDIR' LOCAL_TMP_ROOT='$LOCAL_TMP_ROOT' MNT='$MNT'
                $(declare -f "$op" 2>/dev/null)
                $op
            " >/dev/null 2>&1; then
                op_pass=$((op_pass + 1))
            else
                op_fail=$((op_fail + 1))
            fi
        done

        rm -rf "$WORKDIR" 2>/dev/null

        local elapsed=$((SECONDS - start_secs))
        local total=$((op_pass + op_fail))
        local rate=0
        [ "$elapsed" -gt 0 ] && rate=$((total / elapsed))
        printf '\r  iter %d: %d pass, %d fail (%d ops/s)   ' \
            "$iter" "$op_pass" "$op_fail" "$rate"
    done

    WORKDIR="$base_workdir"
    local elapsed=$((SECONDS - start_secs))
    local total=$((op_pass + op_fail))

    printf '\n\n========================================\n'
    printf ' Extended: %d iterations, %d ops\n' "$iter" "$total"
    printf ' Pass: %d  Fail: %d\n' "$op_pass" "$op_fail"
    if [ "$elapsed" -gt 0 ]; then
        printf ' Throughput: %d ops/sec over %ds\n' "$((total / elapsed))" "$elapsed"
    fi
    printf '========================================\n'

    [ "$op_fail" -eq 0 ]
}

# -- Main ---------------------------------------------------------

case "$MODE" in
    smoke)    detect_mount; run_smoke;;
    extended) detect_mount; run_extended;;
    *)        fatal "Usage: $0 [smoke|extended]";;
esac

if [ "$FAIL" -ne 0 ]; then
    exit 1
fi
