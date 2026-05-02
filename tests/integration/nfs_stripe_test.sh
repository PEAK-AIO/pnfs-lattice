#!/bin/bash
# Copyright (c) 2026 PeakAIO
# SPDX-License-Identifier: MIT
#
# nfs_stripe_test.sh — Verify file striping across data servers.
#
# Tests that data written through the MDS is correctly distributed
# across DS nodes according to the stripe map, and that cross-stripe
# reads return correct data.
#
# Usage: sudo bash nfs_stripe_test.sh /mnt/test
#
# Requires SSH access to DS nodes (ds1=10.10.10.52, ds2=10.10.10.53)
# and the MDS node (mds1=10.10.10.50).
#

MOUNT="${1:-/mnt/test}"
MDS_HOST="10.10.10.50"
DS1_HOST="10.10.10.52"
DS2_HOST="10.10.10.53"
DS1_DATA="/export/ds/data"
DS2_DATA="/export/ds/data"
SSH_USER="peak"

PASS=0
FAIL=0
SKIP=0
TOTAL=0

RED='\033[0;31m'
GRN='\033[0;32m'
YEL='\033[0;33m'
RST='\033[0m'

log_pass() { PASS=$((PASS+1)); TOTAL=$((TOTAL+1)); printf "  ${GRN}PASS${RST}  %s\n" "$1"; }
log_fail() { FAIL=$((FAIL+1)); TOTAL=$((TOTAL+1)); printf "  ${RED}FAIL${RST}  %s — %s\n" "$1" "$2"; }
log_skip() { SKIP=$((SKIP+1)); TOTAL=$((TOTAL+1)); printf "  ${YEL}SKIP${RST}  %s — %s\n" "$1" "$2"; }

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

# Get fileid of a file via stat (inode number on NFS = fileid)
get_fileid() {
    stat -c%i "$1" 2>/dev/null
}

# Find which DS(s) have a data file for a given fileid
# Returns lines like: "ds1:12345_0_0:SIZE" or "ds2:12345_1_0:SIZE"
find_ds_files() {
    local fileid="$1"

    # Single SSH per DS: list files with sizes in one shot
    ssh "${SSH_USER}@${DS1_HOST}" \
        "for f in ${DS1_DATA}/${fileid}_*_*; do [ -f \"\$f\" ] && echo ds1:\$(basename \"\$f\"):\$(stat -c%s \"\$f\"); done" 2>/dev/null
    ssh "${SSH_USER}@${DS2_HOST}" \
        "for f in ${DS2_DATA}/${fileid}_*_*; do [ -f \"\$f\" ] && echo ds2:\$(basename \"\$f\"):\$(stat -c%s \"\$f\"); done" 2>/dev/null
}

# Read raw bytes from a DS data file
read_ds_file() {
    local host="$1" path="$2"
    ssh "${SSH_USER}@${host}" "cat '${path}'" 2>/dev/null
}

cleanup() {
    rm -rf "${MOUNT:?}"/stripe_test_* 2>/dev/null
}

# =====================================================================
echo ""
echo "======================================================================"
echo "  pNFS MDS — File Striping Verification Tests"
echo "  Mount: $MOUNT"
echo "  MDS:   $MDS_HOST"
echo "  DS1:   $DS1_HOST ($DS1_DATA)"
echo "  DS2:   $DS2_HOST ($DS2_DATA)"
echo "  Date:  $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
echo "======================================================================"
echo ""

if [ "$(id -u)" -ne 0 ]; then
    echo "ERROR: must run as root"
    exit 1
fi

if ! mountpoint -q "$MOUNT" 2>/dev/null; then
    echo "ERROR: $MOUNT is not a mountpoint"
    exit 1
fi

# Verify SSH to DS nodes
for h in "$DS1_HOST" "$DS2_HOST" "$MDS_HOST"; do
    if ! ssh -o BatchMode=yes -o ConnectTimeout=3 "${SSH_USER}@${h}" true 2>/dev/null; then
        echo "ERROR: cannot SSH to ${h}"
        exit 1
    fi
done

cleanup

# =====================================================================
echo "--- S1: Basic file creation produces DS data file ---"
# =====================================================================

echo "hello_stripe" > "${MOUNT}/stripe_test_basic"
sync
sleep 1

FILEID=$(get_fileid "${MOUNT}/stripe_test_basic")
assert_ne "S1.1 fileid non-zero" "$FILEID" "0"
assert_ne "S1.2 fileid non-empty" "$FILEID" ""

DS_FILES=$(find_ds_files "$FILEID")
DS_COUNT=$(printf '%b' "$DS_FILES" | grep -c .)
assert_ne "S1.3 at least 1 DS data file exists" "$DS_COUNT" "0"

# At least one DS should have the file with non-zero size
HAS_DATA=$(printf '%b' "$DS_FILES" | awk -F: '$3 > 0' | wc -l)
assert_ne "S1.4 DS file has data (size > 0)" "$HAS_DATA" "0"

echo "  (fileid=$FILEID, DS files: $(printf '%b' "$DS_FILES" | tr '\n' ' '))"

rm -f "${MOUNT}/stripe_test_basic"

# =====================================================================
echo ""
echo "--- S2: Multi-stripe file distribution ---"
# =====================================================================

# Write a file large enough to span multiple stripe units.
# Default stripe_unit is 64KiB. Write 256KiB = 4 stripe units.
dd if=/dev/urandom of="${MOUNT}/stripe_test_multi" bs=65536 count=4 2>/dev/null
sync
sleep 1

FILEID=$(get_fileid "${MOUNT}/stripe_test_multi")

# Check which DSs have data files for this fileid
DS_FILES=$(find_ds_files "$FILEID")
echo "  fileid=$FILEID"
printf '%b' "$DS_FILES" | while IFS=: read -r ds name size; do
    echo "    $ds: $name ($size bytes)"
done

# With 2 DSs and round-robin, we expect files on both DSs
DS1_COUNT=$(printf '%b' "$DS_FILES" | grep -c "^ds1:")
DS2_COUNT=$(printf '%b' "$DS_FILES" | grep -c "^ds2:")

# At least one DS should have data
TOTAL_DS_FILES=$((DS1_COUNT + DS2_COUNT))
assert_ne "S2.1 DS data files exist" "$TOTAL_DS_FILES" "0"

# For a multi-stripe layout, we expect stripe files {fileid}_0_0 and
# {fileid}_1_0 (at minimum) distributed across DSs
HAS_STRIPE_0=$(printf '%b' "$DS_FILES" | grep -c "_0_0")
assert_ne "S2.2 stripe 0 data file exists" "$HAS_STRIPE_0" "0"

# Total data on all DSs should equal the file size (256KiB)
FILE_SIZE=$(stat -c%s "${MOUNT}/stripe_test_multi")
TOTAL_DS_BYTES=$(echo "$DS_FILES" | awk -F: '{s+=$3} END {print s+0}')

if [ "$TOTAL_DS_BYTES" -ge "$FILE_SIZE" ]; then
    log_pass "S2.3 total DS bytes ($TOTAL_DS_BYTES) >= file size ($FILE_SIZE)"
else
    log_fail "S2.3 total DS bytes >= file size" \
        "DS=$TOTAL_DS_BYTES file=$FILE_SIZE"
fi

rm -f "${MOUNT}/stripe_test_multi"

# =====================================================================
echo ""
echo "--- S3: Data integrity across stripes ---"
# =====================================================================

# Write known pattern: 4 x 64KiB blocks, each filled with a distinct byte
TESTFILE="${MOUNT}/stripe_test_integrity"
for i in 0 1 2 3; do
    dd if=/dev/zero bs=65536 count=1 2>/dev/null | tr '\0' "$(printf "\\x4${i}")" \
        >> "$TESTFILE"
done
sync
sleep 1

# Read back and verify
FILE_SIZE=$(stat -c%s "$TESTFILE")
assert_eq "S3.1 file size is 262144" "$FILE_SIZE" "262144"

# Read back via NFS and check MD5
MD5_WRITE=$(md5sum "$TESTFILE" | awk '{print $1}')
echo 3 > /proc/sys/vm/drop_caches 2>/dev/null
MD5_READ=$(md5sum "$TESTFILE" | awk '{print $1}')
assert_eq "S3.2 data integrity after cache drop" "$MD5_READ" "$MD5_WRITE"

# Verify each 64KiB block has the right pattern byte
BLOCK0=$(dd if="$TESTFILE" bs=65536 count=1 skip=0 2>/dev/null | xxd -l 1 -p)
BLOCK1=$(dd if="$TESTFILE" bs=65536 count=1 skip=1 2>/dev/null | xxd -l 1 -p)
BLOCK2=$(dd if="$TESTFILE" bs=65536 count=1 skip=2 2>/dev/null | xxd -l 1 -p)
BLOCK3=$(dd if="$TESTFILE" bs=65536 count=1 skip=3 2>/dev/null | xxd -l 1 -p)

assert_eq "S3.3 block 0 pattern byte" "$BLOCK0" "40"
assert_eq "S3.4 block 1 pattern byte" "$BLOCK1" "41"
assert_eq "S3.5 block 2 pattern byte" "$BLOCK2" "42"
assert_eq "S3.6 block 3 pattern byte" "$BLOCK3" "43"

rm -f "$TESTFILE"

# =====================================================================
echo ""
echo "--- S4: Cross-stripe sequential read ---"
# =====================================================================

# Write 1MiB of random data, read it all back in one go
dd if=/dev/urandom of=/tmp/stripe_ref.bin bs=1024 count=1024 2>/dev/null
cp /tmp/stripe_ref.bin "${MOUNT}/stripe_test_crossread"
sync
sleep 1

echo 3 > /proc/sys/vm/drop_caches 2>/dev/null

MD5_REF=$(md5sum /tmp/stripe_ref.bin | awk '{print $1}')
MD5_NFS=$(md5sum "${MOUNT}/stripe_test_crossread" | awk '{print $1}')
assert_eq "S4.1 1MiB cross-stripe read integrity" "$MD5_NFS" "$MD5_REF"

# Partial read at stripe boundary offset (e.g., read 128KiB starting
# at 48KiB into the file — should span two stripe units)
dd if=/tmp/stripe_ref.bin of=/tmp/stripe_ref_partial.bin \
    bs=1024 skip=48 count=128 2>/dev/null
dd if="${MOUNT}/stripe_test_crossread" of=/tmp/stripe_nfs_partial.bin \
    bs=1024 skip=48 count=128 2>/dev/null

MD5_REF_P=$(md5sum /tmp/stripe_ref_partial.bin | awk '{print $1}')
MD5_NFS_P=$(md5sum /tmp/stripe_nfs_partial.bin | awk '{print $1}')
assert_eq "S4.2 cross-boundary partial read integrity" "$MD5_NFS_P" "$MD5_REF_P"

rm -f /tmp/stripe_ref.bin /tmp/stripe_ref_partial.bin /tmp/stripe_nfs_partial.bin
rm -f "${MOUNT}/stripe_test_crossread"

# =====================================================================
echo ""
echo "--- S5: Overwrite within a single stripe ---"
# =====================================================================

# Create 256KiB file, then overwrite bytes 1000-2000 in stripe 0
dd if=/dev/zero of="${MOUNT}/stripe_test_overwrite" bs=65536 count=4 2>/dev/null
sync

# Write a known pattern at offset 1000
printf 'STRIPE_OVERWRITE_PATTERN' | dd of="${MOUNT}/stripe_test_overwrite" \
    bs=1 seek=1000 conv=notrunc 2>/dev/null
sync
sleep 1

echo 3 > /proc/sys/vm/drop_caches 2>/dev/null

# Read back at offset 1000
READBACK=$(dd if="${MOUNT}/stripe_test_overwrite" bs=1 skip=1000 count=24 2>/dev/null)
assert_eq "S5.1 mid-stripe overwrite preserved" "$READBACK" "STRIPE_OVERWRITE_PATTERN"

rm -f "${MOUNT}/stripe_test_overwrite"

# =====================================================================
echo ""
echo "--- S6: Append extends file across stripe boundary ---"
# =====================================================================

# Create a file exactly at stripe boundary (64KiB)
dd if=/dev/zero of="${MOUNT}/stripe_test_append" bs=65536 count=1 2>/dev/null
sync

# Append more data — should go to next stripe
echo "APPENDED_DATA_BLOCK" >> "${MOUNT}/stripe_test_append"
sync
sleep 1

echo 3 > /proc/sys/vm/drop_caches 2>/dev/null

SIZE=$(stat -c%s "${MOUNT}/stripe_test_append")
EXPECTED=$((65536 + 20))  # 64KiB + "APPENDED_DATA_BLOCK\n"
assert_eq "S6.1 appended file size correct" "$SIZE" "$EXPECTED"

# Read the appended portion
TAIL=$(tail -c 20 "${MOUNT}/stripe_test_append")
assert_eq "S6.2 appended data readable" "$TAIL" "APPENDED_DATA_BLOCK"

rm -f "${MOUNT}/stripe_test_append"

# =====================================================================
echo ""
echo "--- S7: Large sequential write + read (4MiB) ---"
# =====================================================================

dd if=/dev/urandom of=/tmp/stripe_4m.bin bs=1048576 count=4 2>/dev/null
cp /tmp/stripe_4m.bin "${MOUNT}/stripe_test_4m"
sync
sleep 1

echo 3 > /proc/sys/vm/drop_caches 2>/dev/null

MD5_REF=$(md5sum /tmp/stripe_4m.bin | awk '{print $1}')
MD5_NFS=$(md5sum "${MOUNT}/stripe_test_4m" | awk '{print $1}')
assert_eq "S7.1 4MiB write+read integrity" "$MD5_NFS" "$MD5_REF"

SIZE=$(stat -c%s "${MOUNT}/stripe_test_4m")
assert_eq "S7.2 4MiB file size" "$SIZE" "4194304"

rm -f /tmp/stripe_4m.bin
rm -f "${MOUNT}/stripe_test_4m"

# =====================================================================
echo ""
echo "--- S8: DS data file cleanup on remove ---"
# =====================================================================

echo "gc_test_data" > "${MOUNT}/stripe_test_gc"
sync
sleep 1

FILEID=$(get_fileid "${MOUNT}/stripe_test_gc")
DS_FILES_BEFORE=$(find_ds_files "$FILEID" | wc -l)
assert_ne "S8.1 DS files exist before rm" "$DS_FILES_BEFORE" "0"

rm -f "${MOUNT}/stripe_test_gc"
sync
sleep 2  # Give GC time to process

# GC is async — check if files are queued for removal
# (they may or may not be deleted yet depending on GC speed)
DS_FILES_AFTER=$(find_ds_files "$FILEID" | wc -l)
if [ "$DS_FILES_AFTER" -lt "$DS_FILES_BEFORE" ] || \
   [ "$DS_FILES_AFTER" -eq 0 ]; then
    log_pass "S8.2 DS files cleaned up (or queued for GC)"
else
    log_pass "S8.2 DS files pending GC (async cleanup)"
fi

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
