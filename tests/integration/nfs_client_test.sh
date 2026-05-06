#!/bin/bash
# Copyright (c) 2026 PeakAIO
# SPDX-License-Identifier: MIT
#
# nfs_client_test.sh -- Real NFS client integration tests against pNFS MDS.
#
# Runs on the NFS client machine against a mounted pNFS MDS export.
# Tests NFSv4.1/4.2 operations from the Linux kernel NFS client perspective.
#
# Usage: sudo bash nfs_client_test.sh /mnt/test
#

MOUNT="${1:-/mnt/test}"
PASS=0
FAIL=0
SKIP=0
TOTAL=0

# Colors for output
RED='\033[0;31m'
GRN='\033[0;32m'
YEL='\033[0;33m'
RST='\033[0m'

log_pass() { PASS=$((PASS+1)); TOTAL=$((TOTAL+1)); printf "  ${GRN}PASS${RST}  %s\n" "$1"; }
log_fail() { FAIL=$((FAIL+1)); TOTAL=$((TOTAL+1)); printf "  ${RED}FAIL${RST}  %s -- %s\n" "$1" "$2"; }
log_skip() { SKIP=$((SKIP+1)); TOTAL=$((TOTAL+1)); printf "  ${YEL}SKIP${RST}  %s -- %s\n" "$1" "$2"; }

cleanup() {
    rm -rf "${MOUNT:?}/nfs_test_dir" 2>/dev/null
    rm -f "${MOUNT:?}"/nfs_test_* 2>/dev/null
}

assert_eq() {
    local desc="$1" got="$2" want="$3"
    if [ "$got" = "$want" ]; then
        log_pass "$desc"
    else
        log_fail "$desc" "got='$got' want='$want'"
    fi
}

assert_ne() {
    local desc="$1" got="$2" unwant="$3"
    if [ "$got" != "$unwant" ]; then
        log_pass "$desc"
    else
        log_fail "$desc" "got unexpected '$got'"
    fi
}

assert_ok() {
    local desc="$1"; shift
    if "$@" >/dev/null 2>&1; then
        log_pass "$desc"
    else
        log_fail "$desc" "command failed: $*"
    fi
}

assert_fail() {
    local desc="$1"; shift
    if "$@" >/dev/null 2>&1; then
        log_fail "$desc" "command succeeded but should have failed: $*"
    else
        log_pass "$desc"
    fi
}

# =====================================================================
echo ""
echo "======================================================================"
echo "  pNFS MDS -- Real NFS Client Integration Tests"
echo "  Mount: $MOUNT"
echo "  Date:  $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
echo "======================================================================"

# Pre-flight
if [ "$(id -u)" -ne 0 ]; then
    echo "ERROR: must run as root (for mount/chown/chmod tests)"
    exit 1
fi

if ! mountpoint -q "$MOUNT" 2>/dev/null; then
    echo "ERROR: $MOUNT is not a mountpoint"
    exit 1
fi

MOUNT_INFO=$(mount | grep " $MOUNT " | head -1)
echo "  Mount info: $MOUNT_INFO"
echo ""

cleanup

# =====================================================================
echo "--- T1: Basic file I/O ---"
# =====================================================================

# T1.1 Create a file
assert_ok "T1.1 touch creates file" touch "${MOUNT}/nfs_test_file1"
assert_ok "T1.2 file exists" test -f "${MOUNT}/nfs_test_file1"

# T1.3 Write data
echo "hello pnfs world" > "${MOUNT}/nfs_test_file1"
CONTENT=$(cat "${MOUNT}/nfs_test_file1")
assert_eq "T1.3 read back written data" "$CONTENT" "hello pnfs world"

# T1.4 Append
echo "second line" >> "${MOUNT}/nfs_test_file1"
LINES=$(wc -l < "${MOUNT}/nfs_test_file1")
assert_eq "T1.4 append adds line" "$LINES" "2"

# T1.5 Overwrite
echo "replaced" > "${MOUNT}/nfs_test_file1"
CONTENT=$(cat "${MOUNT}/nfs_test_file1")
assert_eq "T1.5 overwrite replaces content" "$CONTENT" "replaced"

# T1.6 Large-ish write (1 MiB)
dd if=/dev/urandom of="${MOUNT}/nfs_test_large" bs=1024 count=1024 2>/dev/null
SIZE=$(stat -c%s "${MOUNT}/nfs_test_large" 2>/dev/null)
assert_eq "T1.6 1MiB file size correct" "$SIZE" "1048576"

# T1.7 Data integrity
MD5_WRITE=$(md5sum "${MOUNT}/nfs_test_large" | awk '{print $1}')
# Force cache flush by dropping caches (requires root)
echo 3 > /proc/sys/vm/drop_caches 2>/dev/null
MD5_READ=$(md5sum "${MOUNT}/nfs_test_large" | awk '{print $1}')
assert_eq "T1.7 data integrity after cache drop" "$MD5_READ" "$MD5_WRITE"

# T1.8 Remove file
assert_ok "T1.8 rm succeeds" rm "${MOUNT}/nfs_test_file1"
assert_fail "T1.9 file gone after rm" test -f "${MOUNT}/nfs_test_file1"

rm -f "${MOUNT}/nfs_test_large"

# =====================================================================
echo ""
echo "--- T2: Directory operations ---"
# =====================================================================

assert_ok "T2.1 mkdir" mkdir "${MOUNT}/nfs_test_dir"
assert_ok "T2.2 is directory" test -d "${MOUNT}/nfs_test_dir"

assert_ok "T2.3 nested mkdir" mkdir "${MOUNT}/nfs_test_dir/sub1"
assert_ok "T2.4 nested mkdir 2" mkdir "${MOUNT}/nfs_test_dir/sub2"
touch "${MOUNT}/nfs_test_dir/file_a"
touch "${MOUNT}/nfs_test_dir/file_b"

# T2.5 readdir
ENTRIES=$(ls "${MOUNT}/nfs_test_dir" | wc -l)
assert_eq "T2.5 readdir returns 4 entries" "$ENTRIES" "4"

# T2.6 rmdir non-empty fails
assert_fail "T2.6 rmdir non-empty fails" rmdir "${MOUNT}/nfs_test_dir"

# T2.7 remove contents then rmdir
rm "${MOUNT}/nfs_test_dir/file_a" "${MOUNT}/nfs_test_dir/file_b"
rmdir "${MOUNT}/nfs_test_dir/sub1" "${MOUNT}/nfs_test_dir/sub2"
assert_ok "T2.7 rmdir after emptying" rmdir "${MOUNT}/nfs_test_dir"

# =====================================================================
echo ""
echo "--- T3: Rename operations ---"
# =====================================================================

echo "rename_data" > "${MOUNT}/nfs_test_src"
assert_ok "T3.1 rename file" mv "${MOUNT}/nfs_test_src" "${MOUNT}/nfs_test_dst"
assert_fail "T3.2 source gone" test -f "${MOUNT}/nfs_test_src"
CONTENT=$(cat "${MOUNT}/nfs_test_dst")
assert_eq "T3.3 data preserved after rename" "$CONTENT" "rename_data"

# T3.4 Rename with replace
echo "old" > "${MOUNT}/nfs_test_replace_target"
echo "new" > "${MOUNT}/nfs_test_replace_src"
assert_ok "T3.4 rename replaces existing" mv "${MOUNT}/nfs_test_replace_src" "${MOUNT}/nfs_test_replace_target"
CONTENT=$(cat "${MOUNT}/nfs_test_replace_target")
assert_eq "T3.5 replaced file has new content" "$CONTENT" "new"

# T3.6 Rename directory
mkdir "${MOUNT}/nfs_test_dir_old"
assert_ok "T3.6 rename directory" mv "${MOUNT}/nfs_test_dir_old" "${MOUNT}/nfs_test_dir_new"
assert_ok "T3.7 renamed dir exists" test -d "${MOUNT}/nfs_test_dir_new"
rmdir "${MOUNT}/nfs_test_dir_new"
rm -f "${MOUNT}/nfs_test_dst" "${MOUNT}/nfs_test_replace_target"

# =====================================================================
echo ""
echo "--- T4: Hard links ---"
# =====================================================================

echo "link_data" > "${MOUNT}/nfs_test_link_orig"
assert_ok "T4.1 create hard link" ln "${MOUNT}/nfs_test_link_orig" "${MOUNT}/nfs_test_link_copy"
NLINK=$(stat -c%h "${MOUNT}/nfs_test_link_orig" 2>/dev/null)
assert_eq "T4.2 nlink=2 after link" "$NLINK" "2"

CONTENT=$(cat "${MOUNT}/nfs_test_link_copy")
assert_eq "T4.3 linked file has same data" "$CONTENT" "link_data"

rm "${MOUNT}/nfs_test_link_orig"
NLINK=$(stat -c%h "${MOUNT}/nfs_test_link_copy" 2>/dev/null)
assert_eq "T4.4 nlink=1 after unlink original" "$NLINK" "1"
CONTENT=$(cat "${MOUNT}/nfs_test_link_copy")
assert_eq "T4.5 data survives original unlink" "$CONTENT" "link_data"
rm "${MOUNT}/nfs_test_link_copy"

# =====================================================================
echo ""
echo "--- T5: Permissions and ownership ---"
# =====================================================================

touch "${MOUNT}/nfs_test_perm"
assert_ok "T5.1 chmod 0444" chmod 0444 "${MOUNT}/nfs_test_perm"
MODE=$(stat -c%a "${MOUNT}/nfs_test_perm" 2>/dev/null)
assert_eq "T5.2 mode is 444" "$MODE" "444"

assert_ok "T5.3 chmod 0755" chmod 0755 "${MOUNT}/nfs_test_perm"
MODE=$(stat -c%a "${MOUNT}/nfs_test_perm" 2>/dev/null)
assert_eq "T5.4 mode is 755" "$MODE" "755"

assert_ok "T5.5 chown 1000:1000" chown 1000:1000 "${MOUNT}/nfs_test_perm"
OWNER=$(stat -c%u "${MOUNT}/nfs_test_perm" 2>/dev/null)
GROUP=$(stat -c%g "${MOUNT}/nfs_test_perm" 2>/dev/null)
assert_eq "T5.6 uid=1000" "$OWNER" "1000"
assert_eq "T5.7 gid=1000" "$GROUP" "1000"
rm "${MOUNT}/nfs_test_perm"

# =====================================================================
echo ""
echo "--- T6: File attributes (stat) ---"
# =====================================================================

echo "attrtest" > "${MOUNT}/nfs_test_attr"
FTYPE=$(stat -c%F "${MOUNT}/nfs_test_attr" 2>/dev/null)
assert_eq "T6.1 file type is regular" "$FTYPE" "regular file"

FSIZE=$(stat -c%s "${MOUNT}/nfs_test_attr" 2>/dev/null)
assert_eq "T6.2 size matches (9 bytes)" "$FSIZE" "9"

mkdir "${MOUNT}/nfs_test_attr_dir"
DTYPE=$(stat -c%F "${MOUNT}/nfs_test_attr_dir" 2>/dev/null)
assert_eq "T6.3 directory type" "$DTYPE" "directory"

FILEID=$(stat -c%i "${MOUNT}/nfs_test_attr" 2>/dev/null)
assert_ne "T6.4 fileid is non-zero" "$FILEID" "0"

rm "${MOUNT}/nfs_test_attr"
rmdir "${MOUNT}/nfs_test_attr_dir"

# =====================================================================
echo ""
echo "--- T7: Concurrent I/O ---"
# =====================================================================

# Spawn 4 writers, each writing 100 files
for w in 1 2 3 4; do
    (
        for i in $(seq 1 100); do
            echo "worker${w}_file${i}" > "${MOUNT}/nfs_test_conc_w${w}_${i}" 2>/dev/null
        done
    ) &
done
wait

COUNT=$(ls "${MOUNT}"/nfs_test_conc_* 2>/dev/null | wc -l)
assert_eq "T7.1 400 concurrent files created" "$COUNT" "400"

# Verify a sample
SAMPLE=$(cat "${MOUNT}/nfs_test_conc_w3_50" 2>/dev/null)
assert_eq "T7.2 concurrent write data correct" "$SAMPLE" "worker3_file50"

rm -f "${MOUNT}"/nfs_test_conc_* 2>/dev/null

# =====================================================================
echo ""
echo "--- T8: File locking (flock) ---"
# =====================================================================

touch "${MOUNT}/nfs_test_lockfile"

# T8.1 Acquire exclusive lock, verify second lock attempt fails
(
    flock -x -w 2 200
    echo "locked" > "${MOUNT}/nfs_test_lockfile"
    sleep 3
) 200>"${MOUNT}/nfs_test_lockfile" &
LOCK_PID=$!
sleep 1

# Try non-blocking exclusive lock -- should fail
if flock -x -n 200 200>"${MOUNT}/nfs_test_lockfile" 2>/dev/null; then
    log_fail "T8.1 exclusive lock conflict" "second lock succeeded"
else
    log_pass "T8.1 exclusive lock conflict detected"
fi

wait "$LOCK_PID" 2>/dev/null
rm -f "${MOUNT}/nfs_test_lockfile"

# =====================================================================
echo ""
echo "--- T9: Special error cases ---"
# =====================================================================

# T9.1 Open non-existent
assert_fail "T9.1 cat non-existent -> ENOENT" cat "${MOUNT}/nfs_test_no_such_file"

# T9.2 Remove non-existent
assert_fail "T9.2 rm non-existent -> error" rm "${MOUNT}/nfs_test_no_such_file"

# T9.3 Create in non-existent directory
assert_fail "T9.3 touch in non-existent dir -> error" touch "${MOUNT}/nfs_test_nodir/file"

# T9.4 rmdir on file -> ENOTDIR
touch "${MOUNT}/nfs_test_notdir"
assert_fail "T9.4 rmdir on regular file -> error" rmdir "${MOUNT}/nfs_test_notdir"
rm "${MOUNT}/nfs_test_notdir"

# T9.5 Create duplicate
mkdir "${MOUNT}/nfs_test_dupdir"
assert_fail "T9.5 mkdir duplicate -> EEXIST" mkdir "${MOUNT}/nfs_test_dupdir"
rmdir "${MOUNT}/nfs_test_dupdir"

# =====================================================================
echo ""
echo "--- T10: Readdir pagination (large directory) ---"
# =====================================================================

mkdir "${MOUNT}/nfs_test_bigdir"
for i in $(seq -w 1 500); do
    touch "${MOUNT}/nfs_test_bigdir/file_${i}" 2>/dev/null
done

COUNT=$(ls "${MOUNT}/nfs_test_bigdir" | wc -l)
assert_eq "T10.1 500 files in large dir" "$COUNT" "500"

# Verify ls returns sorted
FIRST=$(ls "${MOUNT}/nfs_test_bigdir" | head -1)
LAST=$(ls "${MOUNT}/nfs_test_bigdir" | tail -1)
assert_eq "T10.2 first entry" "$FIRST" "file_001"
assert_eq "T10.3 last entry" "$LAST" "file_500"

rm -rf "${MOUNT}/nfs_test_bigdir"

# =====================================================================
echo ""
echo "--- T11: Timestamps ---"
# =====================================================================

touch "${MOUNT}/nfs_test_times"
MTIME1=$(stat -c%Y "${MOUNT}/nfs_test_times" 2>/dev/null)
sleep 2
echo "update" >> "${MOUNT}/nfs_test_times"
MTIME2=$(stat -c%Y "${MOUNT}/nfs_test_times" 2>/dev/null)
if [ "$MTIME2" -gt "$MTIME1" ] 2>/dev/null; then
    log_pass "T11.1 mtime advances on write"
else
    log_fail "T11.1 mtime advances on write" "mtime1=$MTIME1 mtime2=$MTIME2"
fi

# touch -t sets specific time
assert_ok "T11.2 touch -t sets atime/mtime" touch -t 202501011200.00 "${MOUNT}/nfs_test_times"
MTIME3=$(stat -c%Y "${MOUNT}/nfs_test_times" 2>/dev/null)
# 2025-01-01 12:00:00 UTC = 1735732800
assert_eq "T11.3 mtime set to specific value" "$MTIME3" "1735732800"
rm "${MOUNT}/nfs_test_times"

# =====================================================================
echo ""
echo "--- T12: Symlinks (if supported) ---"
# =====================================================================

echo "symlink_target_data" > "${MOUNT}/nfs_test_sym_target"
if ln -s "${MOUNT}/nfs_test_sym_target" "${MOUNT}/nfs_test_sym_link" 2>/dev/null; then
    assert_ok "T12.1 symlink created" test -L "${MOUNT}/nfs_test_sym_link"
    CONTENT=$(cat "${MOUNT}/nfs_test_sym_link" 2>/dev/null)
    assert_eq "T12.2 read through symlink" "$CONTENT" "symlink_target_data"
    LINK_TARGET=$(readlink "${MOUNT}/nfs_test_sym_link" 2>/dev/null)
    assert_eq "T12.3 readlink returns target" "$LINK_TARGET" "${MOUNT}/nfs_test_sym_target"
    rm -f "${MOUNT}/nfs_test_sym_link"
else
    log_skip "T12.1-3 symlinks" "not supported"
fi
rm -f "${MOUNT}/nfs_test_sym_target"

# =====================================================================
echo ""
echo "--- T13: Truncate ---"
# =====================================================================

dd if=/dev/zero of="${MOUNT}/nfs_test_trunc" bs=1024 count=100 2>/dev/null
SIZE=$(stat -c%s "${MOUNT}/nfs_test_trunc")
assert_eq "T13.1 initial 100KB" "$SIZE" "102400"

truncate -s 50000 "${MOUNT}/nfs_test_trunc"
SIZE=$(stat -c%s "${MOUNT}/nfs_test_trunc")
assert_eq "T13.2 truncate down to 50000" "$SIZE" "50000"

truncate -s 200000 "${MOUNT}/nfs_test_trunc"
SIZE=$(stat -c%s "${MOUNT}/nfs_test_trunc")
assert_eq "T13.3 truncate up to 200000" "$SIZE" "200000"

rm "${MOUNT}/nfs_test_trunc"

# =====================================================================
echo ""
echo "--- T14: Rename across directories ---"
# =====================================================================

mkdir "${MOUNT}/nfs_test_dir_a"
mkdir "${MOUNT}/nfs_test_dir_b"
echo "cross_dir" > "${MOUNT}/nfs_test_dir_a/moveme"
assert_ok "T14.1 cross-dir rename" mv "${MOUNT}/nfs_test_dir_a/moveme" "${MOUNT}/nfs_test_dir_b/moveme"
assert_fail "T14.2 source gone" test -f "${MOUNT}/nfs_test_dir_a/moveme"
CONTENT=$(cat "${MOUNT}/nfs_test_dir_b/moveme")
assert_eq "T14.3 data intact after cross-dir mv" "$CONTENT" "cross_dir"
rm -rf "${MOUNT}/nfs_test_dir_a" "${MOUNT}/nfs_test_dir_b"

# =====================================================================
# Summary
# =====================================================================

cleanup

echo ""
echo "======================================================================"
printf "  TOTAL: %d  |  ${GRN}PASS: %d${RST}  |  ${RED}FAIL: %d${RST}  |  ${YEL}SKIP: %d${RST}\n" \
    "$TOTAL" "$PASS" "$FAIL" "$SKIP"
echo "======================================================================"
echo ""

exit "$FAIL"
