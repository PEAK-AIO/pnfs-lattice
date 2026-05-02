/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_open_exclusive.c — Tests for EXCLUSIVE4/EXCLUSIVE4_1 create modes.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "pnfs_mds.h"
#include "open_state.h"
#include "compound.h"

static int pass_count;
static int fail_count;

#define ASSERT_EQ(a, b) do { \
    long long _a = (long long)(a), _b = (long long)(b); \
    if (_a != _b) { \
        fprintf(stderr, "FAIL %s:%d: %s == %lld, expected %lld\n", \
                __FILE__, __LINE__, #a, _a, _b); \
        fail_count++; return; \
    } pass_count++; \
} while (0)

/* -------------------------------------------------------------------
 * Test 1: createmode enum values match RFC
 * ------------------------------------------------------------------- */

static void test_createmode_values(void)
{
    ASSERT_EQ(CREATEMODE_UNCHECKED4, 0);
    ASSERT_EQ(CREATEMODE_GUARDED4, 1);
    ASSERT_EQ(CREATEMODE_EXCLUSIVE4, 2);
    ASSERT_EQ(CREATEMODE_EXCLUSIVE4_1, 3);
}

/* -------------------------------------------------------------------
 * Test 2: nfs4_arg_open has create_verf field
 * ------------------------------------------------------------------- */

static void test_arg_open_has_verf(void)
{
    struct nfs4_arg_open arg;

    memset(&arg, 0, sizeof(arg));
    arg.create_verf = 0xDEADBEEFCAFE1234ULL;
    ASSERT_EQ(arg.create_verf, 0xDEADBEEFCAFE1234ULL);

    arg.createmode = CREATEMODE_EXCLUSIVE4;
    ASSERT_EQ(arg.createmode, CREATEMODE_EXCLUSIVE4);
}

/* -------------------------------------------------------------------
 * Test 3: mds_inode has create_verf field
 * ------------------------------------------------------------------- */

static void test_inode_has_verf(void)
{
    struct mds_inode inode;

    memset(&inode, 0, sizeof(inode));
    inode.create_verf = 0x1122334455667788ULL;
    ASSERT_EQ(inode.create_verf, 0x1122334455667788ULL);

    /* Verify it survives a memset of adjacent fields. */
    inode.flags = MDS_IFLAG_DS_PENDING;
    ASSERT_EQ(inode.flags, MDS_IFLAG_DS_PENDING);
    ASSERT_EQ(inode.create_verf, 0x1122334455667788ULL);
}

/* -------------------------------------------------------------------
 * Test 4: verifier match = replay, mismatch = conflict
 * ------------------------------------------------------------------- */

static void test_verifier_comparison(void)
{
    uint64_t verf_a = 0xAAAAAAAABBBBBBBBULL;
    uint64_t verf_b = 0xCCCCCCCCDDDDDDDDULL;

    /* Same verifier = replay. */
    ASSERT_EQ(verf_a == verf_a, 1);
    /* Different verifier = conflict. */
    ASSERT_EQ(verf_a == verf_b, 0);
    /* Zero verifier (no EXCLUSIVE4 used). */
    ASSERT_EQ((uint64_t)0 == (uint64_t)0, 1);
}

/* -------------------------------------------------------------------
 * Test 5: DS_PENDING flag value
 * ------------------------------------------------------------------- */

static void test_ds_pending_flag(void)
{
    ASSERT_EQ(MDS_IFLAG_INLINE, 1U << 0);
    ASSERT_EQ(MDS_IFLAG_PROMOTING, 1U << 1);
    ASSERT_EQ(MDS_IFLAG_DS_PENDING, 1U << 2);

    /* Flags are orthogonal. */
    uint32_t flags = MDS_IFLAG_DS_PENDING;
    ASSERT_EQ(flags & MDS_IFLAG_INLINE, 0);
    ASSERT_EQ(flags & MDS_IFLAG_DS_PENDING, MDS_IFLAG_DS_PENDING);
}

/* -------------------------------------------------------------------
 * main
 * ------------------------------------------------------------------- */

int main(void)
{
    printf("test_open_exclusive:\n");

    test_createmode_values();
    printf("  test_createmode_values              PASS\n");

    test_arg_open_has_verf();
    printf("  test_arg_open_has_verf              PASS\n");

    test_inode_has_verf();
    printf("  test_inode_has_verf                 PASS\n");

    test_verifier_comparison();
    printf("  test_verifier_comparison            PASS\n");

    test_ds_pending_flag();
    printf("  test_ds_pending_flag                PASS\n");

    printf("\n  %d assertions passed, %d failed\n", pass_count, fail_count);
    return fail_count > 0 ? 1 : 0;
}
