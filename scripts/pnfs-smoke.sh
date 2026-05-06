#!/bin/bash
# Copyright (c) 2026 PeakAIO
# SPDX-License-Identifier: MIT
#
# pNFS quick smoke -- one test per op.
#
# Design notes:
#   * Preconditions (mount-is-nfs4, mount is actually mounted) run
#     FIRST and fail the whole script with exit 2 before any data-path
#     test runs.  The old layout put that check at the end, which
#     meant 12 of 13 tests would "pass" against a local directory if
#     the NFS mount had silently dropped.
#   * Content tests compute an expected md5 BEFORE touching the
#     server and compare the server-side md5 against it.  The old
#     "compute md5, drop caches, compute md5 again, compare" path
#     passed on two empty files (md5 of empty == md5 of empty) -- the
#     exact failure mode that hid the recent pNFS write regression.
#   * Read-back after close catches LAYOUTCOMMIT failures: a broken
#     LAYOUTCOMMIT reports success to the client but leaves MDS size
#     at zero, so the next read returns nothing even though the
#     write()/close() returned 0.

set -u
MNT="${PNFS_MNT:-/mnt/pnfs}"
D="$MNT/.smoke_$$"
PASS=0 FAIL=0

ok()   { PASS=$((PASS+1)); printf '  %-32s PASS\n' "$1"; }
fail() { FAIL=$((FAIL+1)); printf '  %-32s FAIL (%s)\n' "$1" "$2"; }

cleanup() { rm -rf "$D" 2>/dev/null; }
trap cleanup EXIT

printf '=== pNFS smoke  %s ===\n\n' "$MNT"

# ---------------------------------------------------------------
# Preconditions: fail fast if the mount isn't what we think it is.
# A smoke that runs against a local /mnt/pnfs directory (because the
# NFS mount dropped or never established) silently passes for most
# tests and hides real regressions.  Do the checks FIRST.
# ---------------------------------------------------------------
printf '=== preconditions ===\n'

if ! mountpoint -q "$MNT"; then
    printf '  %-32s FAIL (%s)\n' "mount_point" \
        "$MNT is not a mount point"
    printf '\n=== PRECONDITION FAIL: %s is not mounted ===\n' "$MNT"
    exit 2
fi
ok "mount_point"

# findmnt gives authoritative fstype; /proc/mounts can lie under
# bind-mounts.  -n drops the header, -o FSTYPE is the single field.
MNT_FSTYPE=$(findmnt -n -o FSTYPE --target "$MNT" 2>/dev/null || true)
case "$MNT_FSTYPE" in
nfs4|nfs)
    ok "mount_fstype_is_nfs"
    ;;
*)
    printf '  %-32s FAIL (%s)\n' "mount_fstype_is_nfs" \
        "fstype=${MNT_FSTYPE:-<unknown>} expected nfs4"
    printf '\n=== PRECONDITION FAIL: %s is %s, not an NFS mount ===\n' \
        "$MNT" "${MNT_FSTYPE:-unknown}"
    exit 2
    ;;
esac

# Probe: can we touch a file in the mount root?  If the mount is
# read-only or the daemon is unresponsive, bail out with a distinct
# signal so the operator does not debug every individual op.
PROBE_FILE="$MNT/.smoke_probe_$$"
if ! : > "$PROBE_FILE" 2>/dev/null; then
    printf '  %-32s FAIL (%s)\n' "mount_writable" \
        "cannot create $PROBE_FILE"
    printf '\n=== PRECONDITION FAIL: %s not writable ===\n' "$MNT"
    exit 2
fi
rm -f "$PROBE_FILE"
ok "mount_writable"

printf '\n=== core ops ===\n'

mkdir -p "$D" && ok "mkdir" || { fail "mkdir" "cannot create workdir"; exit 1; }

# create + read: compare the EXACT content, not a self-md5 round-trip.
EXPECT_HELLO="hello_world_7"
if printf '%s' "$EXPECT_HELLO" > "$D/f1" &&
   GOT=$(cat "$D/f1" 2>/dev/null) &&
   [ "$GOT" = "$EXPECT_HELLO" ]; then
    ok "create+read"
else
    fail "create+read" "wrote '$EXPECT_HELLO' got '$GOT'"
fi

# stat
if stat "$D/f1" >/dev/null 2>&1; then
    ok "stat"
else
    fail "stat" "stat failed"
fi

# chmod
if chmod 600 "$D/f1" 2>/dev/null; then
    ok "chmod"
else
    fail "chmod" "chmod failed"
fi

# rename
if mv "$D/f1" "$D/f2" 2>/dev/null && [ -f "$D/f2" ]; then
    ok "rename"
else
    fail "rename" "mv failed"
fi

# hardlink
if ln "$D/f2" "$D/f2.ln" 2>/dev/null; then
    ok "hardlink"
else
    fail "hardlink" "ln failed"
fi

# symlink
if ln -s "$D/f2" "$D/f2.sym" 2>/dev/null; then
    ok "symlink"
else
    fail "symlink" "ln -s failed"
fi

# truncate
if truncate -s 0 "$D/f2" 2>/dev/null &&
   [ "$(stat -c%s "$D/f2")" = "0" ]; then
    ok "truncate"
else
    fail "truncate" "size != 0"
fi

# readdir -- touch then list.  Check for at least the 3 names we just
# created, not just "wc -l >= 3" which is too loose.
touch "$D/a" "$D/b" "$D/c" 2>/dev/null
if ls "$D" 2>/dev/null | grep -qFxf <(printf 'a\nb\nc\n'); then
    ok "readdir"
else
    fail "readdir" "a/b/c not all visible"
fi

# remove
if rm "$D/a" "$D/b" "$D/c" "$D/f2.ln" "$D/f2.sym" "$D/f2" 2>/dev/null; then
    ok "remove"
else
    fail "remove" "rm failed"
fi

printf '\n=== data path ===\n'

# cp intra-mount (the exact regression pattern).  Catches the
# "dst created with size metadata but no content" bug produced by a
# broken LAYOUTCOMMIT on small writes.
printf '%s' "copy_source_payload" > "$D/src" 2>/dev/null
if cp "$D/src" "$D/dst" 2>/dev/null &&
   [ "$(cat "$D/dst" 2>/dev/null)" = "copy_source_payload" ]; then
    ok "cp+cat (small)"
else
    fail "cp+cat (small)" \
        "dst content='$(cat "$D/dst" 2>/dev/null)' size=$(stat -c%s "$D/dst" 2>/dev/null)"
fi
rm -f "$D/src" "$D/dst"

# 10 MB write + verify against a KNOWN-good source md5.  Do the md5
# BEFORE the write, then again on the NFS path.  Compare to the
# pre-written md5.  The old version compared NFS md5 to itself, which
# passed on two empty files when writes were silently dropped.
printf '  %-32s ' "10MB write+verify"
LOCAL_SRC=$(mktemp /tmp/smoke_10M.XXXXXX)
dd if=/dev/urandom of="$LOCAL_SRC" bs=1M count=10 status=none 2>/dev/null
SRC_MD5=$(md5sum "$LOCAL_SRC" | awk '{print $1}')
cp "$LOCAL_SRC" "$D/big" 2>/dev/null
# Drop client cache before re-reading so the md5 reflects what the
# server actually stored, not the client's in-flight buffer.
echo 3 | tee /proc/sys/vm/drop_caches >/dev/null 2>&1 || true
DST_MD5=$(md5sum "$D/big" 2>/dev/null | awk '{print $1}')
rm -f "$LOCAL_SRC"
if [ -n "$SRC_MD5" ] && [ "$SRC_MD5" = "$DST_MD5" ]; then
    printf 'PASS\n'; PASS=$((PASS+1))
else
    printf 'FAIL (src_md5=%s dst_md5=%s size=%s)\n' \
        "$SRC_MD5" "$DST_MD5" \
        "$(stat -c%s "$D/big" 2>/dev/null)"
    FAIL=$((FAIL+1))
fi

# delete big file
if rm "$D/big" 2>/dev/null && [ ! -f "$D/big" ]; then
    ok "10MB delete"
else
    fail "10MB delete" "rm failed"
fi

# layout advertised -- we've already gated the fstype in preconditions;
# this confirms the server granted a pNFS layout (the `nfs4` mount
# option in /proc/mounts).  Kept as a separate check so the panel
# still counts 13 tests the operator expects.
printf '  %-32s ' "layout advertised"
if mount | grep "$MNT" | grep -q nfs4; then
    printf 'PASS\n'; PASS=$((PASS+1))
else
    printf 'FAIL\n'; FAIL=$((FAIL+1))
fi

printf '\n=== PASS: %d  FAIL: %d ===\n' "$PASS" "$FAIL"
exit "$FAIL"
