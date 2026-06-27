/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_xdr_codec.c -- Unit tests for the NFSv4.1 XDR codec.
 *
 * Each test encodes data into a buffer, then decodes from the same
 * buffer and verifies the round-trip produces identical values.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "xdr_codec.h"
#include "pnfs_mds.h"

/* -----------------------------------------------------------------------
 * Test framework
 * ----------------------------------------------------------------------- */

static int tests_run;
static int tests_passed;

#define ASSERT_EQ(a, b) do {						\
	if ((a) != (b)) {						\
		fprintf(stderr, "  FAIL %s:%d: %s != %s\n",		\
			__FILE__, __LINE__, #a, #b);			\
		return;							\
	}								\
} while (0)

#define ASSERT_TRUE(x)  ASSERT_EQ(!!(x), 1)

#define ASSERT_STR_EQ(a, b) do {					\
	if (strcmp((a), (b)) != 0) {					\
		fprintf(stderr, "  FAIL %s:%d: \"%s\" != \"%s\"\n",	\
			__FILE__, __LINE__, (a), (b));			\
		return;							\
	}								\
} while (0)

#define ASSERT_MEM_EQ(a, b, n) do {					\
	if (memcmp((a), (b), (n)) != 0) {				\
		fprintf(stderr, "  FAIL %s:%d: memcmp != 0\n",		\
			__FILE__, __LINE__);				\
		return;							\
	}								\
} while (0)

#define RUN_TEST(fn) do {						\
	tests_run++;							\
	fprintf(stdout, "  %-50s", #fn);				\
	fflush(stdout);							\
	fn();								\
	tests_passed++;							\
	fprintf(stdout, "PASS\n");					\
} while (0)

#define BUF_SIZE 8192

/* -----------------------------------------------------------------------
 * Type codec tests
 * ----------------------------------------------------------------------- */

static void test_fh_round_trip(void)
{
    char buf[BUF_SIZE];
    XDR enc, dec;
    uint64_t in_fid = 0xDEADBEEF12345678ULL;
    uint64_t out_fid = 0;

    xdrmem_ncreate(&enc, buf, sizeof(buf), XDR_ENCODE);
    ASSERT_TRUE(xdr_nfs4_fh_encode(&enc, in_fid));
    uint32_t len = xdr_getpos(&enc);
    ASSERT_TRUE(len > 0);

    xdrmem_ncreate(&dec, buf, len, XDR_DECODE);
    ASSERT_TRUE(xdr_nfs4_fh_decode(&dec, &out_fid));
    ASSERT_EQ(in_fid, out_fid);
}

static void test_fh_zero(void)
{
    char buf[BUF_SIZE];
    XDR enc, dec;
    uint64_t out_fid = 42;

    xdrmem_ncreate(&enc, buf, sizeof(buf), XDR_ENCODE);
    ASSERT_TRUE(xdr_nfs4_fh_encode(&enc, 0));

    xdrmem_ncreate(&dec, buf, xdr_getpos(&enc), XDR_DECODE);
    ASSERT_TRUE(xdr_nfs4_fh_decode(&dec, &out_fid));
    ASSERT_EQ(out_fid, (uint64_t)0);
}

static void test_stateid_round_trip(void)
{
    char buf[BUF_SIZE];
    XDR enc, dec;
    struct nfs4_stateid in_sid, out_sid;

    memset(&in_sid, 0, sizeof(in_sid));
    in_sid.seqid = 42;
    memset(in_sid.other, 0xAB, NFS4_OTHER_SIZE);

    xdrmem_ncreate(&enc, buf, sizeof(buf), XDR_ENCODE);
    ASSERT_TRUE(xdr_nfs4_stateid_encode(&enc, &in_sid));

    memset(&out_sid, 0, sizeof(out_sid));
    xdrmem_ncreate(&dec, buf, xdr_getpos(&enc), XDR_DECODE);
    ASSERT_TRUE(xdr_nfs4_stateid_decode(&dec, &out_sid));
    ASSERT_EQ(in_sid.seqid, out_sid.seqid);
    ASSERT_MEM_EQ(in_sid.other, out_sid.other, NFS4_OTHER_SIZE);
}

static void test_bitmap_round_trip(void)
{
    char buf[BUF_SIZE];
    XDR enc, dec;
    uint32_t in_bm[NFS4_BITMAP_WORDS] = {0, 0};
    uint32_t out_bm[NFS4_BITMAP_WORDS] = {0, 0};
    uint32_t words = 0;

    nfs4_bitmap_set(in_bm, FATTR4_TYPE);
    nfs4_bitmap_set(in_bm, FATTR4_SIZE);
    nfs4_bitmap_set(in_bm, FATTR4_MODE);
    nfs4_bitmap_set(in_bm, FATTR4_TIME_MODIFY);

    xdrmem_ncreate(&enc, buf, sizeof(buf), XDR_ENCODE);
    ASSERT_TRUE(xdr_nfs4_bitmap_encode(&enc, in_bm, NFS4_BITMAP_WORDS));

    xdrmem_ncreate(&dec, buf, xdr_getpos(&enc), XDR_DECODE);
    ASSERT_TRUE(xdr_nfs4_bitmap_decode(&dec, out_bm, NFS4_BITMAP_WORDS,
                                       &words));
    ASSERT_EQ(words, (uint32_t)2);
    ASSERT_EQ(in_bm[0], out_bm[0]);
    ASSERT_EQ(in_bm[1], out_bm[1]);
}

static void test_bitmap_helpers(void)
{
    uint32_t bm[NFS4_BITMAP_WORDS] = {0, 0};

    ASSERT_TRUE(!nfs4_bitmap_test(bm, FATTR4_TYPE));
    nfs4_bitmap_set(bm, FATTR4_TYPE);
    ASSERT_TRUE(nfs4_bitmap_test(bm, FATTR4_TYPE));

    /* Word 1 attribute */
    ASSERT_TRUE(!nfs4_bitmap_test(bm, FATTR4_MODE));
    nfs4_bitmap_set(bm, FATTR4_MODE);
    ASSERT_TRUE(nfs4_bitmap_test(bm, FATTR4_MODE));
}

static void test_time_round_trip(void)
{
    char buf[BUF_SIZE];
    XDR enc, dec;
    struct timespec in_ts = { .tv_sec = 1710000000, .tv_nsec = 123456789 };
    struct timespec out_ts = {0};

    xdrmem_ncreate(&enc, buf, sizeof(buf), XDR_ENCODE);
    ASSERT_TRUE(xdr_nfs4_time_encode(&enc, &in_ts));

    xdrmem_ncreate(&dec, buf, xdr_getpos(&enc), XDR_DECODE);
    ASSERT_TRUE(xdr_nfs4_time_decode(&dec, &out_ts));
    ASSERT_EQ(in_ts.tv_sec, out_ts.tv_sec);
    ASSERT_EQ(in_ts.tv_nsec, out_ts.tv_nsec);
}

/* -----------------------------------------------------------------------
 * fattr4 round-trip tests
 * ----------------------------------------------------------------------- */

static void test_fattr_encode_decode(void)
{
    char buf[BUF_SIZE];
    XDR enc, dec;
    struct mds_inode in_inode;
    struct mds_inode out_inode;
    uint32_t mask = 0;
    uint32_t requested[NFS4_BITMAP_WORDS] = {0, 0};

    memset(&in_inode, 0, sizeof(in_inode));
    in_inode.fileid = 100;
    in_inode.type = MDS_FTYPE_REG;
    in_inode.mode = 0644;
    in_inode.size = 4096;
    in_inode.nlink = 1;
    in_inode.uid = 1000;
    in_inode.gid = 1000;
    in_inode.change = 5;
    in_inode.space_used = 8192;
    in_inode.atime.tv_sec = 1710000000;
    in_inode.atime.tv_nsec = 100;
    in_inode.mtime.tv_sec = 1710000100;
    in_inode.mtime.tv_nsec = 200;
    in_inode.ctime.tv_sec = 1710000200;
    in_inode.ctime.tv_nsec = 300;

    /* Request only the attrs the decoder handles (SETATTR subset).
     * The decoder reads attrs sequentially from the opaque buffer,
     * so encoding extra attrs before SIZE would shift offsets. */
    nfs4_bitmap_set(requested, FATTR4_SIZE);
    nfs4_bitmap_set(requested, FATTR4_MODE);
    nfs4_bitmap_set(requested, FATTR4_TIME_MODIFY);

    xdrmem_ncreate(&enc, buf, sizeof(buf), XDR_ENCODE);
    ASSERT_TRUE(xdr_nfs4_fattr_encode(&enc, &in_inode, requested));
    ASSERT_TRUE(xdr_getpos(&enc) > 0);

    /* Decode reads SIZE, MODE, TIME_MODIFY back. */
    memset(&out_inode, 0, sizeof(out_inode));
    xdrmem_ncreate(&dec, buf, xdr_getpos(&enc), XDR_DECODE);
    ASSERT_TRUE(xdr_nfs4_fattr_decode(&dec, &out_inode, &mask));

    ASSERT_TRUE(mask & MDS_ATTR_SIZE);
    ASSERT_TRUE(mask & MDS_ATTR_MODE);
    ASSERT_TRUE(mask & MDS_ATTR_MTIME);
    ASSERT_EQ(out_inode.size, (uint64_t)4096);
    ASSERT_EQ(out_inode.mode, (uint32_t)0644);
    ASSERT_EQ(out_inode.mtime.tv_sec, in_inode.mtime.tv_sec);
    ASSERT_EQ(out_inode.mtime.tv_nsec, in_inode.mtime.tv_nsec);
}

/*
 * Phase 5 -- verify FATTR4_CHANGE_ATTR_TYPE (bit 79, RFC 7862 S10.2.3)
 * is advertised in supported_attrs and encoded as
 * NFS4_CHANGE_TYPE_IS_MONOTONIC_INCR when requested.  This is the
 * foundation for client-side directory-delegation caching.
 */
static void test_fattr_change_attr_type_monotonic(void)
{
    char buf[BUF_SIZE];
    char attr_buf[BUF_SIZE];
    XDR enc, dec, attr_dec;
    struct mds_inode inode;
    uint32_t requested[NFS4_BITMAP_WORDS] = {0, 0, 0};
    uint32_t actual[NFS4_BITMAP_WORDS] = {0, 0, 0};
    uint32_t supported[NFS4_BITMAP_WORDS] = {0, 0, 0};
    uint32_t sup_count = 0;
    uint32_t words = 0;
    uint32_t attr_len = 0;
    uint32_t change_type = 0xFFFFFFFF;

    memset(&inode, 0, sizeof(inode));
    inode.fileid = 7;
    inode.type = MDS_FTYPE_REG;
    inode.change = 42;

    /* Request SUPPORTED_ATTRS + CHANGE_ATTR_TYPE together so the
     * encoder stream starts with the supported-attrs bitmap (whose
     * bits we can inspect) and is followed by the 32-bit
     * change_attr_type value. */
    nfs4_bitmap_set(requested, FATTR4_SUPPORTED_ATTRS);
    nfs4_bitmap_set(requested, FATTR4_CHANGE_ATTR_TYPE);

    xdrmem_ncreate(&enc, buf, sizeof(buf), XDR_ENCODE);
    ASSERT_TRUE(xdr_nfs4_fattr_encode(&enc, &inode, requested));
    ASSERT_TRUE(xdr_getpos(&enc) > 0);

    /* Read back the outer (actual) bitmap and attr-vals opaque. */
    xdrmem_ncreate(&dec, buf, xdr_getpos(&enc), XDR_DECODE);
    ASSERT_TRUE(xdr_nfs4_bitmap_decode(&dec, actual, NFS4_BITMAP_WORDS, &words));
    ASSERT_TRUE(nfs4_bitmap_test(actual, FATTR4_SUPPORTED_ATTRS));
    ASSERT_TRUE(nfs4_bitmap_test(actual, FATTR4_CHANGE_ATTR_TYPE));
    ASSERT_TRUE(xdr_uint32_t(&dec, &attr_len));
    ASSERT_TRUE(attr_len > 0);
    ASSERT_TRUE(attr_len <= sizeof(attr_buf));
    ASSERT_TRUE(xdr_opaque_decode(&dec, attr_buf, attr_len));

    /* Inner attr_vals stream: supported_attrs bitmap, then
     * change_attr_type (u32).  Both attrs are in word 0 / word 2
     * respectively, and emission is bitmap-order. */
    xdrmem_ncreate(&attr_dec, attr_buf, attr_len, XDR_DECODE);
    ASSERT_TRUE(xdr_nfs4_bitmap_decode(&attr_dec, supported,
                                       NFS4_BITMAP_WORDS, &sup_count));
    /* The server must now claim CHANGE_ATTR_TYPE is supported. */
    ASSERT_TRUE(nfs4_bitmap_test(supported, FATTR4_CHANGE_ATTR_TYPE));
    /* And the value it emits must be MONOTONIC_INCR (= 0). */
    ASSERT_TRUE(xdr_uint32_t(&attr_dec, &change_type));
    ASSERT_EQ(change_type, (uint32_t)NFS4_CHANGE_TYPE_IS_MONOTONIC_INCR);
}

/*
 * Phase 5 -- regression test for bitmap-order correctness in
 * encode_attr_vals().
 *
 * The original CHANGE_ATTR_TYPE landing (commit e11eff4) emitted
 * bit 79 BEFORE bit 66 in the encoder, violating RFC 8881 S5.1's
 * strict bitmap-order requirement for the attr_vals stream.  When
 * a client requested both LAYOUT_BLKSIZE (66) and CHANGE_ATTR_TYPE
 * (79) in the same GETATTR, the bytes were misaligned:
 *   * Client read blksz = 0   (actually CHANGE_ATTR_TYPE's u32).
 *   * Client read change_type = 65536  (actually blksz's u32).
 * Linux kernel NFS clients rejected this with mount(2) EIO.  Fix
 * landed in commit 64658ba.
 *
 * This test is the regression gate: it requests three adjacent
 * word-2 attrs and decodes them in bitmap order, asserting that
 * each carries the value its own encoder intended.  Any future
 * edit that inserts a new attr in the wrong position, or swaps
 * two adjacent attrs, flips one of the ASSERT_EQ lines below and
 * the test fails.
 *
 * Guidance: when adding a new attribute to encode_attr_vals(),
 * extend this test to cover the new bit alongside its immediate
 * bitmap neighbours so the ordering contract stays verified.
 */
static void test_fattr_word2_bitmap_order(void)
{
    char buf[BUF_SIZE];
    char attr_buf[BUF_SIZE];
    XDR enc, dec, attr_dec;
    struct mds_inode inode;
    uint32_t requested[NFS4_BITMAP_WORDS] = {0, 0, 0};
    uint32_t actual[NFS4_BITMAP_WORDS] = {0, 0, 0};
    uint32_t words = 0;
    uint32_t attr_len = 0;
    uint32_t blksz = 0;
    uint32_t change_type = 0xFFFFFFFF;
    uint32_t xattr_support = 0xFFFFFFFF;

    memset(&inode, 0, sizeof(inode));
    inode.fileid = 11;
    inode.type = MDS_FTYPE_REG;
    inode.change = 77;

    /* Request three adjacent word-2 attrs in ascending bit order:
     *   FATTR4_LAYOUT_BLKSIZE     (bit 66)
     *   FATTR4_CHANGE_ATTR_TYPE   (bit 79)
     *   FATTR4_XATTR_SUPPORT      (bit 82)
     * The encoder MUST emit their values in that same order. */
    nfs4_bitmap_set(requested, FATTR4_LAYOUT_BLKSIZE);
    nfs4_bitmap_set(requested, FATTR4_CHANGE_ATTR_TYPE);
    nfs4_bitmap_set(requested, FATTR4_XATTR_SUPPORT);

    xdrmem_ncreate(&enc, buf, sizeof(buf), XDR_ENCODE);
    ASSERT_TRUE(xdr_nfs4_fattr_encode(&enc, &inode, requested));
    ASSERT_TRUE(xdr_getpos(&enc) > 0);

    /* Outer: actual attr-mask bitmap + len-prefixed attr_vals. */
    xdrmem_ncreate(&dec, buf, xdr_getpos(&enc), XDR_DECODE);
    ASSERT_TRUE(xdr_nfs4_bitmap_decode(&dec, actual,
                                       NFS4_BITMAP_WORDS, &words));
    ASSERT_TRUE(nfs4_bitmap_test(actual, FATTR4_LAYOUT_BLKSIZE));
    ASSERT_TRUE(nfs4_bitmap_test(actual, FATTR4_CHANGE_ATTR_TYPE));
    ASSERT_TRUE(nfs4_bitmap_test(actual, FATTR4_XATTR_SUPPORT));
    ASSERT_TRUE(xdr_uint32_t(&dec, &attr_len));
    /* Exactly three u32 values: 3 * 4 bytes = 12. */
    ASSERT_EQ(attr_len, (uint32_t)12);
    ASSERT_TRUE(attr_len <= sizeof(attr_buf));
    ASSERT_TRUE(xdr_opaque_decode(&dec, attr_buf, attr_len));

    /* Inner attr_vals stream: decode strictly in bitmap order and
     * check each u32 carries the value its encoder intended. */
    xdrmem_ncreate(&attr_dec, attr_buf, attr_len, XDR_DECODE);

    ASSERT_TRUE(xdr_uint32_t(&attr_dec, &blksz));
    /* LAYOUT_BLKSIZE is currently encoded as 65536.  This is the
     * canary: in the pre-64658ba bug the first u32 was 0 (the
     * misplaced CHANGE_ATTR_TYPE value), so this line would fail. */
    ASSERT_EQ(blksz, (uint32_t)65536);

    ASSERT_TRUE(xdr_uint32_t(&attr_dec, &change_type));
    ASSERT_EQ(change_type,
              (uint32_t)NFS4_CHANGE_TYPE_IS_MONOTONIC_INCR);

    /* XDR bool is wire-encoded as a 4-byte big-endian integer
     * (0 = false, 1 = true).  Decode as uint32_t to avoid
     * depending on xdr_bool being wired up in the test binary. */
    ASSERT_TRUE(xdr_uint32_t(&attr_dec, &xattr_support));
    ASSERT_EQ(xattr_support, (uint32_t)1);
}

/*
 * Regression: when xdr_nfs4_fattr_encode is called without an
 * FS-level space context (fs_space == NULL -- the default GETATTR
 * path when no quota subsystem is wired into the compound), the
 * encoder must advertise SPACE_AVAIL / SPACE_FREE / SPACE_TOTAL as
 * INT64_MAX ("effectively unlimited", ~9.2 EB), NOT as literal 0.
 *
 * The pre-fix behaviour emitted 0 for all three, which made the
 * client's statvfs(2) report Total=0 / Free=0 / Available=0.
 * Several user-space tools (rsync, tar with --check-links,
 * container runtimes' cp pre-flight, backup agents, GUI file
 * managers, Java/Go libs that pre-check getDiskSpace()) refuse to
 * write or abort transfers in that condition.  Linux clients show
 * `df` as `Size=0 Used=0 Avail=0`.
 *
 * Why INT64_MAX and not UINT64_MAX: Linux's NFSv4 statfs glue
 * computes f_blocks = (tbytes + blockres) >> blockbits, which
 * overflows to 0 when tbytes == UINT64_MAX.  INT64_MAX leaves
 * full 2^63 of headroom before u64 overflow.  See
 * src/mds/xdr_codec.c::encode_attr_vals for the full rationale.
 */
static void test_fattr_space_attrs_unlimited_when_no_fs_space(void)
{
    char buf[BUF_SIZE];
    char attr_buf[BUF_SIZE];
    XDR enc, dec, attr_dec;
    struct mds_inode inode;
    uint32_t requested[NFS4_BITMAP_WORDS] = {0, 0, 0};
    uint32_t actual[NFS4_BITMAP_WORDS] = {0, 0, 0};
    uint32_t words = 0;
    uint32_t attr_len = 0;
    uint64_t avail = 0;
    uint64_t sfree = 0;
    uint64_t total = 0;

    memset(&inode, 0, sizeof(inode));
    inode.fileid = 42;
    inode.type = MDS_FTYPE_REG;

    nfs4_bitmap_set(requested, FATTR4_SPACE_AVAIL);
    nfs4_bitmap_set(requested, FATTR4_SPACE_FREE);
    nfs4_bitmap_set(requested, FATTR4_SPACE_TOTAL);

    xdrmem_ncreate(&enc, buf, sizeof(buf), XDR_ENCODE);
    /* xdr_nfs4_fattr_encode internally passes fs_space=NULL.  This
     * is the exact code path that GETATTR responses take when no
     * quota context is plumbed through to encode_res_getattr. */
    ASSERT_TRUE(xdr_nfs4_fattr_encode(&enc, &inode, requested));
    ASSERT_TRUE(xdr_getpos(&enc) > 0);

    xdrmem_ncreate(&dec, buf, xdr_getpos(&enc), XDR_DECODE);
    ASSERT_TRUE(xdr_nfs4_bitmap_decode(&dec, actual,
                                       NFS4_BITMAP_WORDS, &words));
    ASSERT_TRUE(nfs4_bitmap_test(actual, FATTR4_SPACE_AVAIL));
    ASSERT_TRUE(nfs4_bitmap_test(actual, FATTR4_SPACE_FREE));
    ASSERT_TRUE(nfs4_bitmap_test(actual, FATTR4_SPACE_TOTAL));
    ASSERT_TRUE(xdr_uint32_t(&dec, &attr_len));
    ASSERT_EQ(attr_len, (uint32_t)24);
    ASSERT_TRUE(attr_len <= sizeof(attr_buf));
    ASSERT_TRUE(xdr_opaque_decode(&dec, attr_buf, attr_len));

    xdrmem_ncreate(&attr_dec, attr_buf, attr_len, XDR_DECODE);
    ASSERT_TRUE(xdr_uint64_t(&attr_dec, &avail));
    ASSERT_EQ(avail, (uint64_t)INT64_MAX);
    ASSERT_TRUE(xdr_uint64_t(&attr_dec, &sfree));
    ASSERT_EQ(sfree, (uint64_t)INT64_MAX);
    ASSERT_TRUE(xdr_uint64_t(&attr_dec, &total));
    ASSERT_EQ(total, (uint64_t)INT64_MAX);
}

/*
 * Regression: when xdr_nfs4_fattr_encode_ex receives FS-level space
 * values via fs_space (e.g. mds_quota_space_avail() filled them in
 * for the no-rules-installed fast path), the encoder must clamp any
 * value > INT64_MAX down to INT64_MAX before serialising.
 *
 * Why it matters: Linux's NFSv4 statfs glue computes
 *   f_blocks = (tbytes + (sb->s_blocksize - 1)) >> blockbits;
 * (fs/nfs/super.c::nfs_statfs).  With tbytes = UINT64_MAX, the
 * addition wraps to a tiny number and the shift yields 0, so `df`
 * reports `Size=0 Used=0 Avail=0` even though the wire value was
 * "unlimited".  The encoder must therefore normalise the wire
 * output regardless of which producer wrote fs_space.
 */
static void test_fattr_space_attrs_clamped_when_fs_space_unlimited(void)
{
    char buf[BUF_SIZE];
    char attr_buf[BUF_SIZE];
    XDR enc, dec, attr_dec;
    struct mds_inode inode;
    struct xdr_fattr_fs_space fs_space = {
        .space_avail = UINT64_MAX,
        .space_free  = UINT64_MAX,
        .space_total = UINT64_MAX,
    };
    uint32_t requested[NFS4_BITMAP_WORDS] = {0, 0, 0};
    uint32_t actual[NFS4_BITMAP_WORDS] = {0, 0, 0};
    uint32_t words = 0;
    uint32_t attr_len = 0;
    uint64_t avail = 0;
    uint64_t sfree = 0;
    uint64_t total = 0;

    memset(&inode, 0, sizeof(inode));
    inode.fileid = 43;
    inode.type = MDS_FTYPE_REG;

    nfs4_bitmap_set(requested, FATTR4_SPACE_AVAIL);
    nfs4_bitmap_set(requested, FATTR4_SPACE_FREE);
    nfs4_bitmap_set(requested, FATTR4_SPACE_TOTAL);

    xdrmem_ncreate(&enc, buf, sizeof(buf), XDR_ENCODE);
    ASSERT_TRUE(xdr_nfs4_fattr_encode_ex(&enc, &inode, requested,
                                         &fs_space,
                                         0, 0,
                                         NULL, NULL, NULL));
    ASSERT_TRUE(xdr_getpos(&enc) > 0);

    xdrmem_ncreate(&dec, buf, xdr_getpos(&enc), XDR_DECODE);
    ASSERT_TRUE(xdr_nfs4_bitmap_decode(&dec, actual,
                                       NFS4_BITMAP_WORDS, &words));
    ASSERT_TRUE(nfs4_bitmap_test(actual, FATTR4_SPACE_AVAIL));
    ASSERT_TRUE(nfs4_bitmap_test(actual, FATTR4_SPACE_FREE));
    ASSERT_TRUE(nfs4_bitmap_test(actual, FATTR4_SPACE_TOTAL));
    ASSERT_TRUE(xdr_uint32_t(&dec, &attr_len));
    ASSERT_EQ(attr_len, (uint32_t)24);
    ASSERT_TRUE(attr_len <= sizeof(attr_buf));
    ASSERT_TRUE(xdr_opaque_decode(&dec, attr_buf, attr_len));

    xdrmem_ncreate(&attr_dec, attr_buf, attr_len, XDR_DECODE);
    ASSERT_TRUE(xdr_uint64_t(&attr_dec, &avail));
    ASSERT_EQ(avail, (uint64_t)INT64_MAX);
    ASSERT_TRUE(xdr_uint64_t(&attr_dec, &sfree));
    ASSERT_EQ(sfree, (uint64_t)INT64_MAX);
    ASSERT_TRUE(xdr_uint64_t(&attr_dec, &total));
    ASSERT_EQ(total, (uint64_t)INT64_MAX);
}

static void test_fattr_empty_request(void)
{
    char buf[BUF_SIZE];
    XDR enc, dec;
    struct mds_inode inode;
    uint32_t mask = 0;
    uint32_t requested[NFS4_BITMAP_WORDS] = {0, 0};

    memset(&inode, 0, sizeof(inode));

    xdrmem_ncreate(&enc, buf, sizeof(buf), XDR_ENCODE);
    ASSERT_TRUE(xdr_nfs4_fattr_encode(&enc, &inode, requested));

    /* Should produce bitmap (2 words of 0) + length (0). */
    xdrmem_ncreate(&dec, buf, xdr_getpos(&enc), XDR_DECODE);
    ASSERT_TRUE(xdr_nfs4_fattr_decode(&dec, &inode, &mask));
    ASSERT_EQ(mask, (uint32_t)0);
}
static void encode_test_hpc_hint(uint8_t body[HPC_HINT_BODY_SIZE],
                                 uint64_t expected_file_size,
                                 uint32_t expected_client_count,
                                 uint32_t flags)
{
    body[0] = (uint8_t)(expected_file_size >> 56);
    body[1] = (uint8_t)(expected_file_size >> 48);
    body[2] = (uint8_t)(expected_file_size >> 40);
    body[3] = (uint8_t)(expected_file_size >> 32);
    body[4] = (uint8_t)(expected_file_size >> 24);
    body[5] = (uint8_t)(expected_file_size >> 16);
    body[6] = (uint8_t)(expected_file_size >> 8);
    body[7] = (uint8_t)expected_file_size;
    body[8] = (uint8_t)(expected_client_count >> 24);
    body[9] = (uint8_t)(expected_client_count >> 16);
    body[10] = (uint8_t)(expected_client_count >> 8);
    body[11] = (uint8_t)expected_client_count;
    body[12] = (uint8_t)(flags >> 24);
    body[13] = (uint8_t)(flags >> 16);
    body[14] = (uint8_t)(flags >> 8);
    body[15] = (uint8_t)flags;
}

static uint32_t encode_test_fattr_mode_layout_hint(
    char *buf, size_t buflen,
    uint32_t mode, uint32_t layout_type,
    uint64_t expected_file_size,
    uint32_t expected_client_count,
    uint32_t flags)
{
    char attr_buf[BUF_SIZE];
    uint8_t hint_body[HPC_HINT_BODY_SIZE];
    uint32_t bm[NFS4_BITMAP_WORDS] = {0, 0, 0};
    uint32_t body_len = HPC_HINT_BODY_SIZE;
    uint32_t attr_len;
    XDR enc;
    XDR attr_enc;

    encode_test_hpc_hint(hint_body, expected_file_size,
                         expected_client_count, flags);
    nfs4_bitmap_set(bm, FATTR4_MODE);
    nfs4_bitmap_set(bm, FATTR4_LAYOUT_HINT);

    xdrmem_ncreate(&attr_enc, attr_buf, sizeof(attr_buf), XDR_ENCODE);
    assert(xdr_uint32_t(&attr_enc, &mode));
    assert(xdr_uint32_t(&attr_enc, &layout_type));
    assert(xdr_uint32_t(&attr_enc, &body_len));
    assert(xdr_opaque_encode(&attr_enc, (const char *)hint_body, body_len));
    attr_len = xdr_getpos(&attr_enc);

    xdrmem_ncreate(&enc, buf, (u_int)buflen, XDR_ENCODE);
    assert(xdr_nfs4_bitmap_encode(&enc, bm, NFS4_BITMAP_WORDS));
    assert(xdr_uint32_t(&enc, &attr_len));
    assert(xdr_opaque_encode(&enc, attr_buf, attr_len));
    return xdr_getpos(&enc);
}

static void test_fattr_decode_layout_hint_after_mode(void)
{
    char buf[BUF_SIZE];
    XDR dec;
    struct mds_inode attrs;
    struct nfs4_layout_hint hint;
    uint32_t mask = 0;
    uint32_t total;

    total = encode_test_fattr_mode_layout_hint(
        buf, sizeof(buf), 0600, LAYOUT4_FLEX_FILES,
        1ULL << 40, 2048, 0xA5A5A5A5);

    memset(&attrs, 0, sizeof(attrs));
    memset(&hint, 0, sizeof(hint));
    xdrmem_ncreate(&dec, buf, total, XDR_DECODE);
    ASSERT_TRUE(xdr_nfs4_fattr_decode_ex(&dec, &attrs, &mask, &hint));

    ASSERT_TRUE(mask & MDS_ATTR_MODE);
    ASSERT_EQ(attrs.mode, (uint32_t)0600);
    ASSERT_TRUE(hint.present);
    ASSERT_EQ(hint.layout_type, (uint32_t)LAYOUT4_FLEX_FILES);
    ASSERT_EQ(hint.hpc.expected_file_size, (uint64_t)(1ULL << 40));
    ASSERT_EQ(hint.hpc.expected_client_count, (uint32_t)2048);
    ASSERT_EQ(hint.hpc.flags, (uint32_t)0xA5A5A5A5);
}

static void test_fattr_decode_unsupported_layout_hint_absent(void)
{
    char buf[BUF_SIZE];
    XDR dec;
    struct mds_inode attrs;
    struct nfs4_layout_hint hint;
    uint32_t mask = 0;
    uint32_t total;

    total = encode_test_fattr_mode_layout_hint(
        buf, sizeof(buf), 0640, LAYOUT4_NFSV4_1_FILES,
        64ULL << 30, 16, 0);

    memset(&attrs, 0, sizeof(attrs));
    memset(&hint, 0xFF, sizeof(hint));
    xdrmem_ncreate(&dec, buf, total, XDR_DECODE);
    ASSERT_TRUE(xdr_nfs4_fattr_decode_ex(&dec, &attrs, &mask, &hint));

    ASSERT_TRUE(mask & MDS_ATTR_MODE);
    ASSERT_EQ(attrs.mode, (uint32_t)0640);
    ASSERT_TRUE(!hint.present);
}

bool decode_op_setattr(XDR *xdrs, struct nfs4_op *op);
bool decode_op_open(XDR *xdrs, struct nfs4_op *op);

static void test_setattr_decode_layout_hint(void)
{
    char fattr_buf[BUF_SIZE];
    char buf[BUF_SIZE];
    XDR enc;
    XDR dec;
    struct nfs4_op op;
    struct nfs4_stateid sid;
    uint32_t fattr_len;

    fattr_len = encode_test_fattr_mode_layout_hint(
        fattr_buf, sizeof(fattr_buf), 0604, LAYOUT4_FLEX_FILES,
        2ULL << 40, 4096, 0x01020304);

    memset(&sid, 0, sizeof(sid));
    xdrmem_ncreate(&enc, buf, sizeof(buf), XDR_ENCODE);
    ASSERT_TRUE(xdr_nfs4_stateid_encode(&enc, &sid));
    ASSERT_TRUE(xdr_opaque_encode(&enc, fattr_buf, fattr_len));

    memset(&op, 0, sizeof(op));
    op.opnum = OP_SETATTR;
    xdrmem_ncreate(&dec, buf, xdr_getpos(&enc), XDR_DECODE);
    ASSERT_TRUE(decode_op_setattr(&dec, &op));

    ASSERT_TRUE(op.arg.setattr.mask & MDS_ATTR_MODE);
    ASSERT_EQ(op.arg.setattr.attrs.mode, (uint32_t)0604);
    ASSERT_TRUE(op.arg.setattr.layout_hint.present);
    ASSERT_EQ(op.arg.setattr.layout_hint.hpc.expected_client_count,
              (uint32_t)4096);
    ASSERT_EQ(op.arg.setattr.layout_hint.hpc.flags, (uint32_t)0x01020304);
}

static void test_open_create_decode_layout_hint(void)
{
    char fattr_buf[BUF_SIZE];
    char buf[BUF_SIZE];
    XDR enc;
    XDR dec;
    struct nfs4_op op;
    uint32_t fattr_len;
    uint32_t seqid = 0;
    uint32_t share_access = 3;
    uint32_t share_deny = 0;
    uint64_t clientid = 0x1234;
    uint32_t owner_len = 4;
    char owner[4] = {'h', 'p', 'c', '1'};
    uint32_t opentype = 1;
    uint32_t createmode = CREATEMODE_UNCHECKED4;
    uint32_t claim = CLAIM_NULL;
    uint32_t name_len = 9;
    char name[9] = {'h', 'i', 'n', 't', 'e', 'd', '.', 'd', 'a'};

    fattr_len = encode_test_fattr_mode_layout_hint(
        fattr_buf, sizeof(fattr_buf), 0644, LAYOUT4_FLEX_FILES,
        128ULL << 30, 256, 0);

    xdrmem_ncreate(&enc, buf, sizeof(buf), XDR_ENCODE);
    ASSERT_TRUE(xdr_uint32_t(&enc, &seqid));
    ASSERT_TRUE(xdr_uint32_t(&enc, &share_access));
    ASSERT_TRUE(xdr_uint32_t(&enc, &share_deny));
    ASSERT_TRUE(xdr_uint64_t(&enc, &clientid));
    ASSERT_TRUE(xdr_uint32_t(&enc, &owner_len));
    ASSERT_TRUE(xdr_opaque_encode(&enc, owner, owner_len));
    ASSERT_TRUE(xdr_uint32_t(&enc, &opentype));
    ASSERT_TRUE(xdr_uint32_t(&enc, &createmode));
    ASSERT_TRUE(xdr_opaque_encode(&enc, fattr_buf, fattr_len));
    ASSERT_TRUE(xdr_uint32_t(&enc, &claim));
    ASSERT_TRUE(xdr_uint32_t(&enc, &name_len));
    ASSERT_TRUE(xdr_opaque_encode(&enc, name, name_len));

    memset(&op, 0, sizeof(op));
    op.opnum = OP_OPEN;
    xdrmem_ncreate(&dec, buf, xdr_getpos(&enc), XDR_DECODE);
    ASSERT_TRUE(decode_op_open(&dec, &op));

    ASSERT_TRUE(op.arg.open.create);
    ASSERT_EQ(op.arg.open.createmode, CREATEMODE_UNCHECKED4);
    ASSERT_EQ(op.arg.open.mode, (uint32_t)0644);
    ASSERT_STR_EQ(op.arg.open.name, "hinted.da");
    ASSERT_TRUE(op.arg.open.layout_hint.present);
    ASSERT_EQ(op.arg.open.layout_hint.hpc.expected_file_size,
              (uint64_t)(128ULL << 30));
    ASSERT_EQ(op.arg.open.layout_hint.hpc.expected_client_count,
              (uint32_t)256);
}

static bool decode_test_counted_string(XDR *xdrs, char *out, size_t out_sz)
{
    uint32_t len = 0;

    if (!xdr_uint32_t(xdrs, &len)) {
        return false;
    }
    if (len >= out_sz) {
        return false;
    }
    if (len > 0 && !xdr_opaque_decode(xdrs, out, len)) {
        return false;
    }
    out[len] = '\0';
    return true;
}

static bool decode_test_pathname4(XDR *xdrs, char *out, size_t out_sz)
{
    uint32_t count = 0;
    size_t pos = 0;

    if (!xdr_uint32_t(xdrs, &count)) {
        return false;
    }
    if (out_sz == 0) {
        return false;
    }
    if (count == 0) {
        if (out_sz < 2) {
            return false;
        }
        out[0] = '/';
        out[1] = '\0';
        return true;
    }

    out[0] = '\0';
    for (uint32_t i = 0; i < count; i++) {
        char component[256];
        size_t len;

        if (!decode_test_counted_string(xdrs, component, sizeof(component))) {
            return false;
        }
        len = strlen(component);
        if (pos + 1 + len >= out_sz) {
            return false;
        }
        out[pos++] = '/';
        memcpy(out + pos, component, len);
        pos += len;
        out[pos] = '\0';
    }
    return true;
}

static void test_fattr_encode_referral_fs_locations(void)
{
    char buf[BUF_SIZE];
    char attr_buf[BUF_SIZE];
    char server[256];
    char fs_root[256];
    char rootpath[256];
    XDR enc, dec, attr_dec;
    struct mds_inode inode;
    uint32_t requested[NFS4_BITMAP_WORDS] = {0, 0, 0};
    uint32_t actual[NFS4_BITMAP_WORDS] = {0, 0, 0};
    uint32_t words = 0;
    uint32_t attr_len = 0;
    uint32_t loc_count = 0;
    uint32_t srv_count = 0;
    uint32_t mode = 0;
    uint64_t fsid_major = 0;
    uint64_t fsid_minor = 0;
    uint64_t fileid = 0;

    memset(&inode, 0, sizeof(inode));
    inode.fileid = 1234;
    inode.type = MDS_FTYPE_DIR;
    inode.mode = 01777;
    inode.nlink = 2;
    inode.change = 1;

    nfs4_bitmap_set(requested, FATTR4_FSID);
    nfs4_bitmap_set(requested, FATTR4_FILEID);
    nfs4_bitmap_set(requested, FATTR4_FS_LOCATIONS);
    nfs4_bitmap_set(requested, FATTR4_MODE);

    xdrmem_ncreate(&enc, buf, sizeof(buf), XDR_ENCODE);
    ASSERT_TRUE(xdr_nfs4_fattr_encode_ex(&enc, &inode, requested, NULL,
                                         3, 0,
                                         "10.10.10.51", "/", "/foreign"));

    xdrmem_ncreate(&dec, buf, xdr_getpos(&enc), XDR_DECODE);
    ASSERT_TRUE(xdr_nfs4_bitmap_decode(&dec, actual, NFS4_BITMAP_WORDS, &words));
    ASSERT_EQ(words, (uint32_t)NFS4_BITMAP_WORDS);
    ASSERT_TRUE(nfs4_bitmap_test(actual, FATTR4_FSID));
    ASSERT_TRUE(nfs4_bitmap_test(actual, FATTR4_FILEID));
    ASSERT_TRUE(nfs4_bitmap_test(actual, FATTR4_FS_LOCATIONS));
    ASSERT_TRUE(nfs4_bitmap_test(actual, FATTR4_MODE));
    ASSERT_TRUE(xdr_uint32_t(&dec, &attr_len));
    ASSERT_TRUE(attr_len > 0);
    ASSERT_TRUE(attr_len <= sizeof(attr_buf));
    ASSERT_TRUE(xdr_opaque_decode(&dec, attr_buf, attr_len));

    xdrmem_ncreate(&attr_dec, attr_buf, attr_len, XDR_DECODE);
    ASSERT_TRUE(xdr_uint64_t(&attr_dec, &fsid_major));
    ASSERT_TRUE(xdr_uint64_t(&attr_dec, &fsid_minor));
    ASSERT_EQ(fsid_major, (uint64_t)3);
    ASSERT_EQ(fsid_minor, (uint64_t)0);
    ASSERT_TRUE(xdr_uint64_t(&attr_dec, &fileid));
    ASSERT_EQ(fileid, inode.fileid);
    ASSERT_TRUE(decode_test_pathname4(&attr_dec, fs_root, sizeof(fs_root)));
    ASSERT_STR_EQ(fs_root, "/foreign");
    ASSERT_TRUE(xdr_uint32_t(&attr_dec, &loc_count));
    ASSERT_EQ(loc_count, (uint32_t)1);
    ASSERT_TRUE(xdr_uint32_t(&attr_dec, &srv_count));
    ASSERT_EQ(srv_count, (uint32_t)1);
    ASSERT_TRUE(decode_test_counted_string(&attr_dec, server, sizeof(server)));
    ASSERT_STR_EQ(server, "10.10.10.51");
    ASSERT_TRUE(decode_test_pathname4(&attr_dec, rootpath, sizeof(rootpath)));
    ASSERT_STR_EQ(rootpath, "/");
    ASSERT_TRUE(xdr_uint32_t(&attr_dec, &mode));
    ASSERT_EQ(mode, (uint32_t)01777);
}

/* -----------------------------------------------------------------------
 * Per-op encode/decode round-trip via compound codec
 *
 * Build a compound with specific ops, encode it, decode it back,
 * verify the args match.
 * ----------------------------------------------------------------------- */

/** Helper: build an RPC call header + compound args into buf. */
static uint32_t build_rpc_compound(char *buf, size_t buflen,
                                   uint32_t xid,
                                   const struct nfs4_op *ops,
                                   uint32_t op_count)
{
    XDR enc;

    xdrmem_ncreate(&enc, buf, (u_int)buflen, XDR_ENCODE);

    /* RPC call header */
    {
        uint32_t msg_type = 0;   /* CALL */
        uint32_t rpcvers = 2;
        uint32_t prog = NFS_PROGRAM;
        uint32_t vers = NFS_V4;
        uint32_t proc = NFSPROC4_COMPOUND;
        uint32_t auth_none = 0;
        uint32_t auth_len = 0;

        assert(xdr_uint32_t(&enc, &xid));
        assert(xdr_uint32_t(&enc, &msg_type));
        assert(xdr_uint32_t(&enc, &rpcvers));
        assert(xdr_uint32_t(&enc, &prog));
        assert(xdr_uint32_t(&enc, &vers));
        assert(xdr_uint32_t(&enc, &proc));
        /* credential: AUTH_NONE */
        assert(xdr_uint32_t(&enc, &auth_none));
        assert(xdr_uint32_t(&enc, &auth_len));
        /* verifier: AUTH_NONE */
        assert(xdr_uint32_t(&enc, &auth_none));
        assert(xdr_uint32_t(&enc, &auth_len));
    }

    /* COMPOUND4args: tag + minorversion + ops */
    {
        uint32_t tag_len = 4;
        char tag[] = "test";
        uint32_t minorver = 1;
        uint32_t count = op_count;

        assert(xdr_uint32_t(&enc, &tag_len));
        assert(xdr_opaque_encode(&enc, tag, tag_len));
        assert(xdr_uint32_t(&enc, &minorver));
        assert(xdr_uint32_t(&enc, &count));
    }

    /* Encode each op's args. */
    for (uint32_t i = 0; i < op_count; i++) {
        uint32_t opnum = (uint32_t)ops[i].opnum;

        assert(xdr_uint32_t(&enc, &opnum));
        switch (ops[i].opnum) {
        case OP_SEQUENCE: {
            const struct nfs4_arg_sequence *a = &ops[i].arg.sequence;
            uint32_t cache = a->cache_this ? 1 : 0;

            assert(xdr_opaque_encode(&enc, (const char *)a->session_id,
                                     SESSION_ID_SIZE));
            assert(xdr_uint32_t(&enc, (uint32_t *)&a->slot_id));
            assert(xdr_uint32_t(&enc, (uint32_t *)&a->seq_id));
            assert(xdr_uint32_t(&enc, (uint32_t *)&a->highest_slot_id));
            assert(xdr_uint32_t(&enc, &cache));
            break;
        }
        case OP_PUTROOTFH:
            break; /* no args */
        case OP_PUTFH: {
            assert(xdr_nfs4_fh_encode(&enc, ops[i].arg.putfh.fh.fileid));
            break;
        }
        case OP_GETFH:
        case OP_SAVEFH:
        case OP_RESTOREFH:
            break;
        case OP_LOOKUP: {
            uint32_t nlen = (uint32_t)strlen(ops[i].arg.lookup.name);

            assert(xdr_uint32_t(&enc, &nlen));
            assert(xdr_opaque_encode(&enc, ops[i].arg.lookup.name, nlen));
            break;
        }
        case OP_GETATTR: {
            /* bitmap: 2 words of 0xFF (all bits). */
            uint32_t words = NFS4_BITMAP_WORDS;
            uint32_t w = 0xFFFFFFFF;

            assert(xdr_uint32_t(&enc, &words));
            assert(xdr_uint32_t(&enc, &w));
            assert(xdr_uint32_t(&enc, &w));
            break;
        }
        case OP_REMOVE: {
            uint32_t nlen = (uint32_t)strlen(ops[i].arg.remove.name);

            assert(xdr_uint32_t(&enc, &nlen));
            assert(xdr_opaque_encode(&enc, ops[i].arg.remove.name, nlen));
            break;
        }
        case OP_READDIR: {
            const struct nfs4_arg_readdir *a = &ops[i].arg.readdir;
            char cookieverf[8] = {0};
            uint64_t cookie = a->cookie;
            uint32_t dircount = a->dircount;
            uint32_t maxcount = a->maxcount;

            assert(xdr_uint64_t(&enc, &cookie));
            assert(xdr_opaque_encode(&enc, cookieverf, sizeof(cookieverf)));
            assert(xdr_uint32_t(&enc, &dircount));
            assert(xdr_uint32_t(&enc, &maxcount));
            assert(xdr_nfs4_bitmap_encode(&enc, a->requested,
                                          NFS4_BITMAP_WORDS));
            break;
        }
        case OP_CLOSE: {
            uint32_t seqid_unused = 0;
            const struct nfs4_arg_close *a = &ops[i].arg.close;

            assert(xdr_uint32_t(&enc, &seqid_unused));
            assert(xdr_nfs4_stateid_encode(&enc, &a->stateid));
            break;
        }
        case OP_OPEN: {
            const struct nfs4_arg_open *a = &ops[i].arg.open;
            uint32_t seqid_unused = 0;
            uint64_t clientid_wire = 0x1111;
            uint32_t owner_len = a->open_owner_len;
            uint32_t opentype = a->create ? 1 : 0;
            uint32_t claim = (uint32_t)a->claim;

            assert(xdr_uint32_t(&enc, &seqid_unused));
            assert(xdr_uint32_t(&enc, (uint32_t *)&a->share_access));
            assert(xdr_uint32_t(&enc, (uint32_t *)&a->share_deny));
            /* open_owner4 */
            assert(xdr_uint64_t(&enc, &clientid_wire));
            assert(xdr_uint32_t(&enc, &owner_len));
            if (owner_len > 0)
                assert(xdr_opaque_encode(&enc,
                    (const char *)a->open_owner, owner_len));
            /* openflag4 */
            assert(xdr_uint32_t(&enc, &opentype));
            if (a->create) {
                uint32_t cmode = 0; /* UNCHECKED4 */
                uint32_t bm_words = NFS4_BITMAP_WORDS;
                uint32_t bm0 = 0, bm1 = 0;
                uint32_t attr_len = 0;

                assert(xdr_uint32_t(&enc, &cmode));
                /* empty fattr4 */
                assert(xdr_uint32_t(&enc, &bm_words));
                assert(xdr_uint32_t(&enc, &bm0));
                assert(xdr_uint32_t(&enc, &bm1));
                assert(xdr_uint32_t(&enc, &attr_len));
            }
            /* open_claim4 */
            assert(xdr_uint32_t(&enc, &claim));
            if (a->claim == CLAIM_NULL) {
                uint32_t nlen =
                    (uint32_t)strlen(a->name);
                assert(xdr_uint32_t(&enc, &nlen));
                assert(xdr_opaque_encode(&enc,
                    a->name, nlen));
            }
            break;
        }
        case OP_RECLAIM_COMPLETE: {
            uint32_t rca = ops[i].arg.reclaim_complete.rca_one_fs ? 1 : 0;
            assert(xdr_uint32_t(&enc, &rca));
            break;
        }
        default:
            fprintf(stderr, "Unhandled op %d in test encoder\n",
                    ops[i].opnum);
            assert(0);
        }
    }
    return xdr_getpos(&enc);
}

static void test_compound_sequence_putrootfh_getfh(void)
{
    char buf[BUF_SIZE];
    struct nfs4_op ops[3];
    struct nfs4_op decoded[NFS4_MAX_OPS];
    char tag[NFS4_TAG_MAXLEN];
    uint32_t minorver = 0;
    uint32_t op_count = 0;
    uint32_t xid = 0;
    uint32_t prog, vers, proc;

    /* Build ops. */
    memset(ops, 0, sizeof(ops));
    ops[0].opnum = OP_SEQUENCE;
    memset(ops[0].arg.sequence.session_id, 0x11, SESSION_ID_SIZE);
    ops[0].arg.sequence.slot_id = 0;
    ops[0].arg.sequence.seq_id = 1;
    ops[0].arg.sequence.highest_slot_id = 31;
    ops[0].arg.sequence.cache_this = false;

    ops[1].opnum = OP_PUTROOTFH;
    ops[2].opnum = OP_GETFH;

    uint32_t total = build_rpc_compound(buf, sizeof(buf), 0x1234, ops, 3);

    /* Decode RPC header. */
    XDR dec;

    xdrmem_ncreate(&dec, buf, total, XDR_DECODE);
    ASSERT_EQ(rpc_decode_call_header(&dec, &xid, &prog, &vers, &proc, NULL, NULL, NULL, NULL, NULL, NULL), 0);
    ASSERT_EQ(xid, (uint32_t)0x1234);
    ASSERT_EQ(prog, (uint32_t)NFS_PROGRAM);
    ASSERT_EQ(vers, (uint32_t)NFS_V4);
    ASSERT_EQ(proc, (uint32_t)NFSPROC4_COMPOUND);

    /* Decode compound args. */
    memset(decoded, 0, sizeof(decoded));
    ASSERT_EQ(nfs4_decode_compound_args(&dec, tag, sizeof(tag),
                                        &minorver, decoded,
                                        NFS4_MAX_OPS, &op_count), 0);
    ASSERT_EQ(op_count, (uint32_t)3);
    ASSERT_STR_EQ(tag, "test");
    ASSERT_EQ(minorver, (uint32_t)1);

    /* Verify SEQUENCE args. */
    ASSERT_EQ(decoded[0].opnum, OP_SEQUENCE);
    ASSERT_EQ(decoded[0].arg.sequence.slot_id, (uint32_t)0);
    ASSERT_EQ(decoded[0].arg.sequence.seq_id, (uint32_t)1);
    ASSERT_EQ(decoded[0].arg.sequence.highest_slot_id, (uint32_t)31);
    {
        uint8_t expected[SESSION_ID_SIZE];

        memset(expected, 0x11, SESSION_ID_SIZE);
        ASSERT_MEM_EQ(decoded[0].arg.sequence.session_id, expected,
                      SESSION_ID_SIZE);
    }

    /* Verify PUTROOTFH + GETFH. */
    ASSERT_EQ(decoded[1].opnum, OP_PUTROOTFH);
    ASSERT_EQ(decoded[2].opnum, OP_GETFH);
}

static void test_compound_putfh_lookup(void)
{
    char buf[BUF_SIZE];
    struct nfs4_op ops[3];
    struct nfs4_op decoded[NFS4_MAX_OPS];
    char tag[NFS4_TAG_MAXLEN];
    uint32_t minorver, op_count;
    uint32_t xid, prog, vers, proc;
    XDR dec;

    memset(ops, 0, sizeof(ops));
    ops[0].opnum = OP_PUTFH;
    ops[0].arg.putfh.fh.fileid = MDS_FILEID_ROOT;
    ops[1].opnum = OP_LOOKUP;
    snprintf(ops[1].arg.lookup.name, sizeof(ops[1].arg.lookup.name),
             "testfile.txt");
    ops[2].opnum = OP_GETATTR;

    uint32_t total = build_rpc_compound(buf, sizeof(buf), 0x5678, ops, 3);

    xdrmem_ncreate(&dec, buf, total, XDR_DECODE);
    ASSERT_EQ(rpc_decode_call_header(&dec, &xid, &prog, &vers, &proc, NULL, NULL, NULL, NULL, NULL, NULL), 0);
    ASSERT_EQ(xid, (uint32_t)0x5678);

    memset(decoded, 0, sizeof(decoded));
    ASSERT_EQ(nfs4_decode_compound_args(&dec, tag, sizeof(tag),
                                        &minorver, decoded,
                                        NFS4_MAX_OPS, &op_count), 0);
    ASSERT_EQ(op_count, (uint32_t)3);

    ASSERT_EQ(decoded[0].opnum, OP_PUTFH);
    ASSERT_EQ(decoded[0].arg.putfh.fh.fileid, (uint64_t)MDS_FILEID_ROOT);

    ASSERT_EQ(decoded[1].opnum, OP_LOOKUP);
    ASSERT_STR_EQ(decoded[1].arg.lookup.name, "testfile.txt");

    ASSERT_EQ(decoded[2].opnum, OP_GETATTR);
}

static void test_compound_remove(void)
{
    char buf[BUF_SIZE];
    struct nfs4_op ops[2];
    struct nfs4_op decoded[NFS4_MAX_OPS];
    char tag[NFS4_TAG_MAXLEN];
    uint32_t minorver, op_count;
    uint32_t xid, prog, vers, proc;
    XDR dec;

    memset(ops, 0, sizeof(ops));
    ops[0].opnum = OP_PUTFH;
    ops[0].arg.putfh.fh.fileid = 100;
    ops[1].opnum = OP_REMOVE;
    snprintf(ops[1].arg.remove.name, sizeof(ops[1].arg.remove.name),
             "deadfile");

    uint32_t total = build_rpc_compound(buf, sizeof(buf), 1, ops, 2);

    xdrmem_ncreate(&dec, buf, total, XDR_DECODE);
    ASSERT_EQ(rpc_decode_call_header(&dec, &xid, &prog, &vers, &proc, NULL, NULL, NULL, NULL, NULL, NULL), 0);

    memset(decoded, 0, sizeof(decoded));
    ASSERT_EQ(nfs4_decode_compound_args(&dec, tag, sizeof(tag),
                                        &minorver, decoded,
                                        NFS4_MAX_OPS, &op_count), 0);
    ASSERT_EQ(op_count, (uint32_t)2);
    ASSERT_EQ(decoded[1].opnum, OP_REMOVE);
    ASSERT_STR_EQ(decoded[1].arg.remove.name, "deadfile");
}

static void test_compound_readdir_decode(void)
{
    char buf[BUF_SIZE];
    struct nfs4_op ops[1];
    struct nfs4_op decoded[NFS4_MAX_OPS];
    char tag[NFS4_TAG_MAXLEN];
    uint32_t minorver, op_count;
    uint32_t xid, prog, vers, proc;
    uint32_t total;
    XDR dec;

    memset(ops, 0, sizeof(ops));
    ops[0].opnum = OP_READDIR;
    ops[0].arg.readdir.cookie = 99;
    ops[0].arg.readdir.dircount = 4096;
    ops[0].arg.readdir.maxcount = 32768;
    nfs4_bitmap_set(ops[0].arg.readdir.requested, FATTR4_FILEID);
    nfs4_bitmap_set(ops[0].arg.readdir.requested, FATTR4_MODE);
    nfs4_bitmap_set(ops[0].arg.readdir.requested, FATTR4_MOUNTED_ON_FILEID);

    total = build_rpc_compound(buf, sizeof(buf), 2, ops, 1);

    xdrmem_ncreate(&dec, buf, total, XDR_DECODE);
    ASSERT_EQ(rpc_decode_call_header(&dec, &xid, &prog, &vers, &proc,
                                     NULL, NULL, NULL, NULL, NULL, NULL), 0);

    memset(decoded, 0, sizeof(decoded));
    ASSERT_EQ(nfs4_decode_compound_args(&dec, tag, sizeof(tag),
                                        &minorver, decoded,
                                        NFS4_MAX_OPS, &op_count), 0);
    ASSERT_EQ(op_count, (uint32_t)1);
    ASSERT_EQ(decoded[0].opnum, OP_READDIR);
    ASSERT_EQ(decoded[0].arg.readdir.cookie, (uint64_t)99);
    ASSERT_EQ(decoded[0].arg.readdir.dircount, (uint32_t)4096);
    ASSERT_EQ(decoded[0].arg.readdir.maxcount, (uint32_t)32768);
    ASSERT_TRUE(nfs4_bitmap_test(decoded[0].arg.readdir.requested,
                                 FATTR4_FILEID));
    ASSERT_TRUE(nfs4_bitmap_test(decoded[0].arg.readdir.requested,
                                 FATTR4_MODE));
    ASSERT_TRUE(nfs4_bitmap_test(decoded[0].arg.readdir.requested,
                                 FATTR4_MOUNTED_ON_FILEID));
}

static void test_compound_close(void)
{
    char buf[BUF_SIZE];
    struct nfs4_op ops[1];
    struct nfs4_op decoded[NFS4_MAX_OPS];
    char tag[NFS4_TAG_MAXLEN];
    uint32_t minorver, op_count;
    uint32_t xid, prog, vers, proc;
    XDR dec;

    memset(ops, 0, sizeof(ops));
    ops[0].opnum = OP_CLOSE;
    ops[0].arg.close.stateid.seqid = 7;
    memset(ops[0].arg.close.stateid.other, 0xCC, NFS4_OTHER_SIZE);

    uint32_t total = build_rpc_compound(buf, sizeof(buf), 99, ops, 1);

    xdrmem_ncreate(&dec, buf, total, XDR_DECODE);
    ASSERT_EQ(rpc_decode_call_header(&dec, &xid, &prog, &vers, &proc, NULL, NULL, NULL, NULL, NULL, NULL), 0);

    memset(decoded, 0, sizeof(decoded));
    ASSERT_EQ(nfs4_decode_compound_args(&dec, tag, sizeof(tag),
                                        &minorver, decoded,
                                        NFS4_MAX_OPS, &op_count), 0);
    ASSERT_EQ(op_count, (uint32_t)1);
    ASSERT_EQ(decoded[0].opnum, OP_CLOSE);
    ASSERT_EQ(decoded[0].arg.close.stateid.seqid, (uint32_t)7);
    ASSERT_MEM_EQ(decoded[0].arg.close.stateid.other,
                  ops[0].arg.close.stateid.other, NFS4_OTHER_SIZE);
}

/* -----------------------------------------------------------------------
 * Compound result encode/decode round-trip
 * ----------------------------------------------------------------------- */

static void test_compound_result_encode(void)
{
    char buf[BUF_SIZE];
    XDR enc, dec;
    struct nfs4_result results[3];

    memset(results, 0, sizeof(results));

    /* SEQUENCE result */
    results[0].opnum = OP_SEQUENCE;
    results[0].status = NFS4_OK;
    memset(results[0].res.sequence.session_id, 0x22, SESSION_ID_SIZE);
    results[0].res.sequence.slot_id = 0;
    results[0].res.sequence.seq_id = 1;
    results[0].res.sequence.highest_slot_id = 15;
    results[0].res.sequence.target_highest_slot_id = 15;
    results[0].res.sequence.status_flags = 0;

    /* PUTROOTFH result */
    results[1].opnum = OP_PUTROOTFH;
    results[1].status = NFS4_OK;

    /* GETFH result */
    results[2].opnum = OP_GETFH;
    results[2].status = NFS4_OK;
    results[2].res.getfh.fh.fileid = MDS_FILEID_ROOT;

    xdrmem_ncreate(&enc, buf, sizeof(buf), XDR_ENCODE);
    ASSERT_EQ(nfs4_encode_compound_res(&enc, NFS4_OK, "test",
                                       results, 3), 0);
    uint32_t total = xdr_getpos(&enc);
    ASSERT_TRUE(total > 0);

    /* Verify by manually decoding the compound response. */
    xdrmem_ncreate(&dec, buf, total, XDR_DECODE);

    uint32_t status, tag_len;

    ASSERT_TRUE(xdr_uint32_t(&dec, &status));
    ASSERT_EQ(status, (uint32_t)NFS4_OK);

    ASSERT_TRUE(xdr_uint32_t(&dec, &tag_len));
    ASSERT_EQ(tag_len, (uint32_t)4);
    {
        char tag_buf[64];

        ASSERT_TRUE(xdr_opaque_decode(&dec, tag_buf, tag_len));
        tag_buf[tag_len] = '\0';
        ASSERT_STR_EQ(tag_buf, "test");
    }

    uint32_t res_count;

    ASSERT_TRUE(xdr_uint32_t(&dec, &res_count));
    ASSERT_EQ(res_count, (uint32_t)3);

    /* SEQUENCE result: opnum + status + session_id + 5 uint32s */
    {
        uint32_t op, st;

        ASSERT_TRUE(xdr_uint32_t(&dec, &op));
        ASSERT_EQ(op, (uint32_t)OP_SEQUENCE);
        ASSERT_TRUE(xdr_uint32_t(&dec, &st));
        ASSERT_EQ(st, (uint32_t)NFS4_OK);

        uint8_t sid[SESSION_ID_SIZE];

        ASSERT_TRUE(xdr_opaque_decode(&dec, (char *)sid, SESSION_ID_SIZE));
        {
            uint8_t expected[SESSION_ID_SIZE];

            memset(expected, 0x22, SESSION_ID_SIZE);
            ASSERT_MEM_EQ(sid, expected, SESSION_ID_SIZE);
        }

        uint32_t v;

        ASSERT_TRUE(xdr_uint32_t(&dec, &v)); /* slot_id */
        ASSERT_EQ(v, (uint32_t)0);
        ASSERT_TRUE(xdr_uint32_t(&dec, &v)); /* seq_id */
        ASSERT_EQ(v, (uint32_t)1);
        ASSERT_TRUE(xdr_uint32_t(&dec, &v)); /* highest_slot_id */
        ASSERT_EQ(v, (uint32_t)15);
        ASSERT_TRUE(xdr_uint32_t(&dec, &v)); /* target_highest_slot_id */
        ASSERT_EQ(v, (uint32_t)15);
        ASSERT_TRUE(xdr_uint32_t(&dec, &v)); /* status_flags */
        ASSERT_EQ(v, (uint32_t)0);
    }

    /* PUTROOTFH result: opnum + status only */
    {
        uint32_t op, st;

        ASSERT_TRUE(xdr_uint32_t(&dec, &op));
        ASSERT_EQ(op, (uint32_t)OP_PUTROOTFH);
        ASSERT_TRUE(xdr_uint32_t(&dec, &st));
        ASSERT_EQ(st, (uint32_t)NFS4_OK);
    }

    /* GETFH result: opnum + status + fh */
    {
        uint32_t op, st;
        uint64_t fid = 0;

        ASSERT_TRUE(xdr_uint32_t(&dec, &op));
        ASSERT_EQ(op, (uint32_t)OP_GETFH);
        ASSERT_TRUE(xdr_uint32_t(&dec, &st));
        ASSERT_EQ(st, (uint32_t)NFS4_OK);
        ASSERT_TRUE(xdr_nfs4_fh_decode(&dec, &fid));
        ASSERT_EQ(fid, (uint64_t)MDS_FILEID_ROOT);
    }
}

static void test_compound_result_error_stops(void)
{
    char buf[BUF_SIZE];
    XDR enc, dec;
    struct nfs4_result results[2];

    memset(results, 0, sizeof(results));

    /* First op fails. */
    results[0].opnum = OP_PUTFH;
    results[0].status = NFS4ERR_STALE;

    /* Second op should still encode (status only, no body). */
    results[1].opnum = OP_GETATTR;
    results[1].status = NFS4ERR_NOFILEHANDLE;

    xdrmem_ncreate(&enc, buf, sizeof(buf), XDR_ENCODE);
    ASSERT_EQ(nfs4_encode_compound_res(&enc, NFS4ERR_STALE, "",
                                       results, 2), 0);

    /* Verify we can decode the error result. */
    xdrmem_ncreate(&dec, buf, xdr_getpos(&enc), XDR_DECODE);

    uint32_t status, tag_len, res_count;

    ASSERT_TRUE(xdr_uint32_t(&dec, &status));
    ASSERT_EQ(status, (uint32_t)NFS4ERR_STALE);
    ASSERT_TRUE(xdr_uint32_t(&dec, &tag_len));
    ASSERT_EQ(tag_len, (uint32_t)0);
    ASSERT_TRUE(xdr_uint32_t(&dec, &res_count));
    ASSERT_EQ(res_count, (uint32_t)2);

    /* PUTFH: opnum + status (error, no body) */
    {
        uint32_t op, st;

        ASSERT_TRUE(xdr_uint32_t(&dec, &op));
        ASSERT_EQ(op, (uint32_t)OP_PUTFH);
        ASSERT_TRUE(xdr_uint32_t(&dec, &st));
        ASSERT_EQ(st, (uint32_t)NFS4ERR_STALE);
    }
}

/* -----------------------------------------------------------------------
 * RPC header tests
 * ----------------------------------------------------------------------- */

static void test_rpc_header_round_trip(void)
{
    char buf[BUF_SIZE];
    XDR enc, dec;
    uint32_t xid_out, prog_out, vers_out, proc_out;

    xdrmem_ncreate(&enc, buf, sizeof(buf), XDR_ENCODE);

    /* Build a minimal RPC call header. */
    {
        uint32_t xid = 0xAAAA;
        uint32_t msg_type = 0;
        uint32_t rpcvers = 2;
        uint32_t prog = NFS_PROGRAM;
        uint32_t vers = NFS_V4;
        uint32_t proc = NFSPROC4_COMPOUND;
        uint32_t auth_flavor = 0;
        uint32_t auth_len = 0;

        ASSERT_TRUE(xdr_uint32_t(&enc, &xid));
        ASSERT_TRUE(xdr_uint32_t(&enc, &msg_type));
        ASSERT_TRUE(xdr_uint32_t(&enc, &rpcvers));
        ASSERT_TRUE(xdr_uint32_t(&enc, &prog));
        ASSERT_TRUE(xdr_uint32_t(&enc, &vers));
        ASSERT_TRUE(xdr_uint32_t(&enc, &proc));
        ASSERT_TRUE(xdr_uint32_t(&enc, &auth_flavor));
        ASSERT_TRUE(xdr_uint32_t(&enc, &auth_len));
        ASSERT_TRUE(xdr_uint32_t(&enc, &auth_flavor));
        ASSERT_TRUE(xdr_uint32_t(&enc, &auth_len));
    }

    xdrmem_ncreate(&dec, buf, xdr_getpos(&enc), XDR_DECODE);
    ASSERT_EQ(rpc_decode_call_header(&dec, &xid_out, &prog_out,
                                     &vers_out, &proc_out, NULL, NULL, NULL, NULL, NULL, NULL), 0);
    ASSERT_EQ(xid_out, (uint32_t)0xAAAA);
    ASSERT_EQ(prog_out, (uint32_t)NFS_PROGRAM);
    ASSERT_EQ(vers_out, (uint32_t)NFS_V4);
    ASSERT_EQ(proc_out, (uint32_t)NFSPROC4_COMPOUND);
}

static void test_rpc_accepted_reply(void)
{
    char buf[BUF_SIZE];
    XDR enc, dec;

    xdrmem_ncreate(&enc, buf, sizeof(buf), XDR_ENCODE);
    ASSERT_EQ(rpc_encode_accepted_reply(&enc, 0xBBBB), 0);

    xdrmem_ncreate(&dec, buf, xdr_getpos(&enc), XDR_DECODE);

    uint32_t xid, msg_type, reply_stat, verf_flavor, verf_len, accept_stat;

    ASSERT_TRUE(xdr_uint32_t(&dec, &xid));
    ASSERT_EQ(xid, (uint32_t)0xBBBB);
    ASSERT_TRUE(xdr_uint32_t(&dec, &msg_type));
    ASSERT_EQ(msg_type, (uint32_t)1); /* REPLY */
    ASSERT_TRUE(xdr_uint32_t(&dec, &reply_stat));
    ASSERT_EQ(reply_stat, (uint32_t)0); /* MSG_ACCEPTED */
    ASSERT_TRUE(xdr_uint32_t(&dec, &verf_flavor));
    ASSERT_EQ(verf_flavor, (uint32_t)0); /* AUTH_NONE */
    ASSERT_TRUE(xdr_uint32_t(&dec, &verf_len));
    ASSERT_EQ(verf_len, (uint32_t)0);
    ASSERT_TRUE(xdr_uint32_t(&dec, &accept_stat));
    ASSERT_EQ(accept_stat, (uint32_t)0); /* SUCCESS */
}

static void test_rpc_error_reply(void)
{
    char buf[BUF_SIZE];
    XDR enc, dec;

    xdrmem_ncreate(&enc, buf, sizeof(buf), XDR_ENCODE);
    ASSERT_EQ(rpc_encode_error_reply(&enc, 0xCCCC, 2 /* PROG_UNAVAIL */), 0);

    xdrmem_ncreate(&dec, buf, xdr_getpos(&enc), XDR_DECODE);

    uint32_t xid, msg_type, reply_stat, verf_flavor, verf_len, accept_stat;

    ASSERT_TRUE(xdr_uint32_t(&dec, &xid));
    ASSERT_EQ(xid, (uint32_t)0xCCCC);
    ASSERT_TRUE(xdr_uint32_t(&dec, &msg_type));
    ASSERT_EQ(msg_type, (uint32_t)1);
    ASSERT_TRUE(xdr_uint32_t(&dec, &reply_stat));
    ASSERT_EQ(reply_stat, (uint32_t)0);
    ASSERT_TRUE(xdr_uint32_t(&dec, &verf_flavor));
    ASSERT_TRUE(xdr_uint32_t(&dec, &verf_len));
    ASSERT_TRUE(xdr_uint32_t(&dec, &accept_stat));
    ASSERT_EQ(accept_stat, (uint32_t)2);
}

/* -----------------------------------------------------------------------
 * Readdir result encode test
 * ----------------------------------------------------------------------- */

static void test_readdir_result_encode(void)
{
    char buf[BUF_SIZE];
    char tag[8];
    char name[MDS_MAX_NAME + 1];
    char owner[64];
    char owner_group[64];
    char attr_buf[BUF_SIZE];
    char cookieverf[8];
    XDR enc, dec, attr_dec;
    struct nfs4_result result;
    uint32_t compound_status = 0;
    uint32_t tag_len = 0;
    uint32_t rescount = 0;
    uint32_t opnum = 0;
    uint32_t op_status = 0;
    uint32_t words = 0;
    uint32_t actual[NFS4_BITMAP_WORDS] = {0, 0, 0};
    uint32_t attr_len = 0;
    uint32_t nfs4_type = 0;
    uint32_t mode = 0;
    uint64_t cookie = 0;
    uint64_t fileid = 0;
    uint64_t mounted_on_fileid = 0;
    int32_t value_follows = 0;
    int32_t eof = 0;

    memset(&result, 0, sizeof(result));
    result.opnum = OP_READDIR;
    result.status = NFS4_OK;
    result.res.readdir.count = 1;
    result.res.readdir.eof = true;
    result.res.readdir.dir_change = 7;
    snprintf(result.res.readdir.entries[0].name,
             sizeof(result.res.readdir.entries[0].name), "file1");
    result.res.readdir.entries[0].fileid = 10;
    result.res.readdir.entries[0].type = MDS_FTYPE_REG;
    result.res.readdir.entry_attrs_valid[0] = true;
    result.res.readdir.entry_attrs[0].fileid = 10;
    result.res.readdir.entry_attrs[0].type = MDS_FTYPE_REG;
    result.res.readdir.entry_attrs[0].mode = 0644;
    result.res.readdir.entry_attrs[0].uid = 1000;
    result.res.readdir.entry_attrs[0].gid = 1001;
    nfs4_bitmap_set(result.res.readdir.requested, FATTR4_TYPE);
    nfs4_bitmap_set(result.res.readdir.requested, FATTR4_FILEID);
    nfs4_bitmap_set(result.res.readdir.requested, FATTR4_MODE);
    nfs4_bitmap_set(result.res.readdir.requested, FATTR4_OWNER);
    nfs4_bitmap_set(result.res.readdir.requested, FATTR4_OWNER_GROUP);
    nfs4_bitmap_set(result.res.readdir.requested, FATTR4_MOUNTED_ON_FILEID);

    xdrmem_ncreate(&enc, buf, sizeof(buf), XDR_ENCODE);
    ASSERT_EQ(nfs4_encode_compound_res(&enc, NFS4_OK, "rd",
                                       &result, 1), 0);

    xdrmem_ncreate(&dec, buf, xdr_getpos(&enc), XDR_DECODE);
    ASSERT_TRUE(xdr_uint32_t(&dec, &compound_status));
    ASSERT_EQ(compound_status, (uint32_t)NFS4_OK);
    ASSERT_TRUE(xdr_uint32_t(&dec, &tag_len));
    ASSERT_EQ(tag_len, (uint32_t)2);
    ASSERT_TRUE(xdr_opaque_decode(&dec, tag, tag_len));
    tag[tag_len] = '\0';
    ASSERT_STR_EQ(tag, "rd");
    ASSERT_TRUE(xdr_uint32_t(&dec, &rescount));
    ASSERT_EQ(rescount, (uint32_t)1);
    ASSERT_TRUE(xdr_uint32_t(&dec, &opnum));
    ASSERT_EQ(opnum, (uint32_t)OP_READDIR);
    ASSERT_TRUE(xdr_uint32_t(&dec, &op_status));
    ASSERT_EQ(op_status, (uint32_t)NFS4_OK);
    ASSERT_TRUE(xdr_opaque_decode(&dec, cookieverf, sizeof(cookieverf)));
    ASSERT_EQ((unsigned char)cookieverf[0], (unsigned char)0);
    ASSERT_EQ((unsigned char)cookieverf[7], (unsigned char)7);
    ASSERT_TRUE(xdr_getbool(&dec, &value_follows));
    ASSERT_EQ(value_follows, 1);
    ASSERT_TRUE(xdr_uint64_t(&dec, &cookie));
    ASSERT_EQ(cookie, (uint64_t)10);
    ASSERT_TRUE(decode_test_counted_string(&dec, name, sizeof(name)));
    ASSERT_STR_EQ(name, "file1");
    ASSERT_TRUE(xdr_nfs4_bitmap_decode(&dec, actual, NFS4_BITMAP_WORDS, &words));
    ASSERT_EQ(words, (uint32_t)2);
    ASSERT_TRUE(nfs4_bitmap_test(actual, FATTR4_TYPE));
    ASSERT_TRUE(nfs4_bitmap_test(actual, FATTR4_FILEID));
    ASSERT_TRUE(nfs4_bitmap_test(actual, FATTR4_MODE));
    ASSERT_TRUE(nfs4_bitmap_test(actual, FATTR4_OWNER));
    ASSERT_TRUE(nfs4_bitmap_test(actual, FATTR4_OWNER_GROUP));
    ASSERT_TRUE(nfs4_bitmap_test(actual, FATTR4_MOUNTED_ON_FILEID));
    ASSERT_TRUE(xdr_uint32_t(&dec, &attr_len));
    ASSERT_TRUE(attr_len > 0);
    ASSERT_TRUE(attr_len <= sizeof(attr_buf));
    ASSERT_TRUE(xdr_opaque_decode(&dec, attr_buf, attr_len));

    xdrmem_ncreate(&attr_dec, attr_buf, attr_len, XDR_DECODE);
    ASSERT_TRUE(xdr_uint32_t(&attr_dec, &nfs4_type));
    ASSERT_EQ(nfs4_type, (uint32_t)1);
    ASSERT_TRUE(xdr_uint64_t(&attr_dec, &fileid));
    ASSERT_EQ(fileid, (uint64_t)10);
    ASSERT_TRUE(xdr_uint32_t(&attr_dec, &mode));
    ASSERT_EQ(mode, (uint32_t)0644);
    ASSERT_TRUE(decode_test_counted_string(&attr_dec, owner, sizeof(owner)));
    ASSERT_STR_EQ(owner, "1000");
    ASSERT_TRUE(decode_test_counted_string(&attr_dec, owner_group,
                                           sizeof(owner_group)));
    ASSERT_STR_EQ(owner_group, "1001");
    ASSERT_TRUE(xdr_uint64_t(&attr_dec, &mounted_on_fileid));
    ASSERT_EQ(mounted_on_fileid, (uint64_t)10);
    ASSERT_TRUE(xdr_getbool(&dec, &value_follows));
    ASSERT_EQ(value_follows, 0);
    ASSERT_TRUE(xdr_getbool(&dec, &eof));
    ASSERT_EQ(eof, 1);
}

/* -----------------------------------------------------------------------
 * Open result encode test
 * ----------------------------------------------------------------------- */

static void test_open_result_encode(void)
{
    char buf[BUF_SIZE];
    XDR enc;
    struct nfs4_result result;

    memset(&result, 0, sizeof(result));
    result.opnum = OP_OPEN;
    result.status = NFS4_OK;
    result.res.open.stateid.seqid = 1;
    memset(result.res.open.stateid.other, 0xDD, NFS4_OTHER_SIZE);
    result.res.open.inode.change = 5;

    xdrmem_ncreate(&enc, buf, sizeof(buf), XDR_ENCODE);
    ASSERT_EQ(nfs4_encode_compound_res(&enc, NFS4_OK, "",
                                       &result, 1), 0);
    ASSERT_TRUE(xdr_getpos(&enc) > 0);
}

/* -----------------------------------------------------------------------
 * Exchange ID result encode test
 * ----------------------------------------------------------------------- */

static void test_exchange_id_result_encode(void)
{
    char buf[BUF_SIZE];
    XDR enc;
    struct nfs4_result result;

    memset(&result, 0, sizeof(result));
    result.opnum = OP_EXCHANGE_ID;
    result.status = NFS4_OK;
    result.res.exchange_id.clientid = 0x0001000000000001ULL;
    result.res.exchange_id.seqid = 1;
    result.res.exchange_id.eir_flags =
        EXCHGID4_FLAG_USE_PNFS_MDS | EXCHGID4_FLAG_USE_NON_PNFS;

    xdrmem_ncreate(&enc, buf, sizeof(buf), XDR_ENCODE);
    ASSERT_EQ(nfs4_encode_compound_res(&enc, NFS4_OK, "eid",
                                       &result, 1), 0);
    ASSERT_TRUE(xdr_getpos(&enc) > 0);
}

/* -----------------------------------------------------------------------
 * Create session result encode test
 * ----------------------------------------------------------------------- */

static void test_create_session_result_encode(void)
{
    char buf[BUF_SIZE];
    XDR enc;
    struct nfs4_result result;

    memset(&result, 0, sizeof(result));
    result.opnum = OP_CREATE_SESSION;
    result.status = NFS4_OK;
    memset(result.res.create_session.session_id, 0x33, SESSION_ID_SIZE);
    result.res.create_session.csr_flags = 0;
    result.res.create_session.fore_slots = 16;
    result.res.create_session.back_slots = 2;

    xdrmem_ncreate(&enc, buf, sizeof(buf), XDR_ENCODE);
    ASSERT_EQ(nfs4_encode_compound_res(&enc, NFS4_OK, "cs",
                                       &result, 1), 0);
    ASSERT_TRUE(xdr_getpos(&enc) > 0);
}

/* -----------------------------------------------------------------------
 * Open-owner XDR decode test
 * ----------------------------------------------------------------------- */

static void test_compound_open_owner_decode(void)
{
    char buf[BUF_SIZE];
    struct nfs4_op ops[1];
    struct nfs4_op decoded[NFS4_MAX_OPS];
    char tag[NFS4_TAG_MAXLEN];
    uint32_t minorver, op_count;
    uint32_t xid, prog, vers, proc;
    XDR dec;

    /* Known open_owner: 8 bytes 0xA1..0xA8. */
    uint8_t owner_bytes[8] = {0xA1, 0xA2, 0xA3, 0xA4,
                              0xA5, 0xA6, 0xA7, 0xA8};

    memset(ops, 0, sizeof(ops));
    ops[0].opnum = OP_OPEN;
    ops[0].arg.open.share_access = 1; /* OPEN4_SHARE_ACCESS_READ */
    ops[0].arg.open.share_deny = 0;
    memcpy(ops[0].arg.open.open_owner, owner_bytes, sizeof(owner_bytes));
    ops[0].arg.open.open_owner_len = sizeof(owner_bytes);
    ops[0].arg.open.create = false;
    ops[0].arg.open.claim = CLAIM_FH;

    uint32_t total = build_rpc_compound(buf, sizeof(buf), 0xBBBB, ops, 1);

    xdrmem_ncreate(&dec, buf, total, XDR_DECODE);
    ASSERT_EQ(rpc_decode_call_header(&dec, &xid, &prog, &vers, &proc, NULL, NULL, NULL, NULL, NULL, NULL), 0);

    memset(decoded, 0, sizeof(decoded));
    ASSERT_EQ(nfs4_decode_compound_args(&dec, tag, sizeof(tag),
                                        &minorver, decoded,
                                        NFS4_MAX_OPS, &op_count), 0);
    ASSERT_EQ(op_count, (uint32_t)1);
    ASSERT_EQ(decoded[0].opnum, OP_OPEN);
    ASSERT_EQ(decoded[0].arg.open.share_access, (uint32_t)1);
    ASSERT_EQ(decoded[0].arg.open.share_deny, (uint32_t)0);
    ASSERT_EQ(decoded[0].arg.open.open_owner_len, (uint32_t)8);
    ASSERT_MEM_EQ(decoded[0].arg.open.open_owner, owner_bytes, 8);
    ASSERT_EQ(decoded[0].arg.open.claim, CLAIM_FH);
    ASSERT_TRUE(!decoded[0].arg.open.create);
}

/* -----------------------------------------------------------------------
 * Edge case: decode truncated/bad data
 * ----------------------------------------------------------------------- */

static void test_decode_truncated_fails(void)
{
    char buf[4] = {0};
    XDR dec;
    struct nfs4_op ops[1];
    char tag[NFS4_TAG_MAXLEN];
    uint32_t minorver, op_count;

    /* Buffer too small to hold even a tag length. */
    xdrmem_ncreate(&dec, buf, 2, XDR_DECODE);
    ASSERT_EQ(nfs4_decode_compound_args(&dec, tag, sizeof(tag),
                                        &minorver, ops, 1, &op_count), -1);
}

static void test_rpc_bad_msg_type_fails(void)
{
    char buf[BUF_SIZE];
    XDR enc, dec;

    xdrmem_ncreate(&enc, buf, sizeof(buf), XDR_ENCODE);
    {
        uint32_t xid = 1;
        uint32_t msg_type = 1; /* REPLY, not CALL -- should fail */
        uint32_t rpcvers = 2;

        xdr_uint32_t(&enc, &xid);
        xdr_uint32_t(&enc, &msg_type);
        xdr_uint32_t(&enc, &rpcvers);
    }

    uint32_t xid, prog, vers, proc;

    xdrmem_ncreate(&dec, buf, xdr_getpos(&enc), XDR_DECODE);
    ASSERT_EQ(rpc_decode_call_header(&dec, &xid, &prog, &vers, &proc, NULL, NULL, NULL, NULL, NULL, NULL), -1);
}

/* -----------------------------------------------------------------------
 * RECLAIM_COMPLETE XDR round-trip
 * ----------------------------------------------------------------------- */

static void test_reclaim_complete_round_trip(void)
{
    char buf[BUF_SIZE];
    struct nfs4_op ops[2];
    struct nfs4_op decoded[NFS4_MAX_OPS];
    char tag[NFS4_TAG_MAXLEN];
    uint32_t minorver = 0;
    uint32_t op_count = 0;
    uint32_t xid = 0;
    uint32_t prog, vers, proc;

    /* Build: SEQUENCE + RECLAIM_COMPLETE(rca_one_fs=false). */
    memset(ops, 0, sizeof(ops));
    ops[0].opnum = OP_SEQUENCE;
    memset(ops[0].arg.sequence.session_id, 0x22, SESSION_ID_SIZE);
    ops[0].arg.sequence.slot_id = 0;
    ops[0].arg.sequence.seq_id = 1;
    ops[0].arg.sequence.highest_slot_id = 7;
    ops[0].arg.sequence.cache_this = false;

    ops[1].opnum = OP_RECLAIM_COMPLETE;
    ops[1].arg.reclaim_complete.rca_one_fs = false;

    uint32_t total = build_rpc_compound(buf, sizeof(buf), 0xABCD, ops, 2);

    /* Decode. */
    XDR dec;
    xdrmem_ncreate(&dec, buf, total, XDR_DECODE);
    ASSERT_EQ(rpc_decode_call_header(&dec, &xid, &prog, &vers, &proc, NULL, NULL, NULL, NULL, NULL, NULL), 0);
    ASSERT_EQ(xid, (uint32_t)0xABCD);

    memset(decoded, 0, sizeof(decoded));
    ASSERT_EQ(nfs4_decode_compound_args(&dec, tag, sizeof(tag),
                                        &minorver, decoded,
                                        NFS4_MAX_OPS, &op_count), 0);
    ASSERT_EQ(op_count, (uint32_t)2);
    ASSERT_EQ(decoded[0].opnum, OP_SEQUENCE);
    ASSERT_EQ(decoded[1].opnum, OP_RECLAIM_COMPLETE);
    ASSERT_EQ(decoded[1].arg.reclaim_complete.rca_one_fs, false);

    /* Now test with rca_one_fs=true. */
    memset(ops, 0, sizeof(ops));
    ops[0].opnum = OP_SEQUENCE;
    memset(ops[0].arg.sequence.session_id, 0x33, SESSION_ID_SIZE);
    ops[0].arg.sequence.slot_id = 0;
    ops[0].arg.sequence.seq_id = 1;
    ops[0].arg.sequence.highest_slot_id = 7;
    ops[0].arg.sequence.cache_this = false;

    ops[1].opnum = OP_RECLAIM_COMPLETE;
    ops[1].arg.reclaim_complete.rca_one_fs = true;

    total = build_rpc_compound(buf, sizeof(buf), 0xABCE, ops, 2);

    xdrmem_ncreate(&dec, buf, total, XDR_DECODE);
    ASSERT_EQ(rpc_decode_call_header(&dec, &xid, &prog, &vers, &proc, NULL, NULL, NULL, NULL, NULL, NULL), 0);

    memset(decoded, 0, sizeof(decoded));
    ASSERT_EQ(nfs4_decode_compound_args(&dec, tag, sizeof(tag),
                                        &minorver, decoded,
                                        NFS4_MAX_OPS, &op_count), 0);
    ASSERT_EQ(op_count, (uint32_t)2);
    ASSERT_EQ(decoded[1].opnum, OP_RECLAIM_COMPLETE);
    ASSERT_EQ(decoded[1].arg.reclaim_complete.rca_one_fs, true);

    /* Encode a success result and verify it encodes (status-only). */
    {
        struct nfs4_result res[2];
        char resbuf[BUF_SIZE];
        XDR renc;

        memset(res, 0, sizeof(res));
        res[0].opnum = OP_SEQUENCE;
        res[0].status = NFS4_OK;
        memset(res[0].res.sequence.session_id, 0x22, SESSION_ID_SIZE);
        res[0].res.sequence.slot_id = 0;
        res[0].res.sequence.seq_id = 1;
        res[0].res.sequence.highest_slot_id = 7;
        res[0].res.sequence.target_highest_slot_id = 7;
        res[0].res.sequence.status_flags = 0;

        res[1].opnum = OP_RECLAIM_COMPLETE;
        res[1].status = NFS4_OK;

        xdrmem_ncreate(&renc, resbuf, sizeof(resbuf), XDR_ENCODE);
        ASSERT_EQ(nfs4_encode_compound_res(&renc, NFS4_OK, "test",
                                           res, 2), 0);
    }
}


/* -----------------------------------------------------------------------
 * Unknown opnum regression test
 *
 * An unrecognized opnum in a COMPOUND must decode successfully (the
 * decoder preserves the raw opnum) so that dispatch_op() can return
 * NFS4ERR_OP_ILLEGAL.  Encoding the result for an unknown op must
 * fail (return -1) since we have no encoder for it.
 * ----------------------------------------------------------------------- */

static void test_unknown_opnum_decode(void)
{
    char buf[BUF_SIZE];
    struct nfs4_op ops[2];
    struct nfs4_op decoded[NFS4_MAX_OPS];
    char tag[NFS4_TAG_MAXLEN];
    uint32_t minorver = 0;
    uint32_t op_count = 0;
    XDR enc, dec;

    /* Build a compound: SEQUENCE + unknown opnum 9999. */
    memset(ops, 0, sizeof(ops));
    ops[0].opnum = OP_SEQUENCE;
    memset(ops[0].arg.sequence.session_id, 0xAA, SESSION_ID_SIZE);
    ops[0].arg.sequence.slot_id = 0;
    ops[0].arg.sequence.seq_id = 1;
    ops[0].arg.sequence.highest_slot_id = 7;

    /* Manually encode to inject the unknown opnum. */
    xdrmem_ncreate(&enc, buf, sizeof(buf), XDR_ENCODE);
    {
        /* Compound header: tag, minorversion, op_count */
        uint32_t tlen = 4;
        char ttag[4] = {'t', 'e', 's', 't'};
        uint32_t mv = 1;
        uint32_t cnt = 2;

        xdr_uint32_t(&enc, &tlen);
        xdr_opaque_encode(&enc, ttag, 4);
        xdr_uint32_t(&enc, &mv);
        xdr_uint32_t(&enc, &cnt);

        /* Op 1: SEQUENCE */
        uint32_t opnum = (uint32_t)OP_SEQUENCE;
        xdr_uint32_t(&enc, &opnum);
        xdr_opaque_encode(&enc, (char *)ops[0].arg.sequence.session_id,
                          SESSION_ID_SIZE);
        uint32_t slot = 0, seq = 1, highest = 7;
        int32_t cache = 0;
        xdr_uint32_t(&enc, &slot);
        xdr_uint32_t(&enc, &seq);
        xdr_uint32_t(&enc, &highest);
        xdr_putbool(&enc, cache);

        /* Op 2: unknown opnum 9999 (no additional args) */
        uint32_t unk = 9999;
        xdr_uint32_t(&enc, &unk);
    }

    /* Decode: should succeed, preserving the unknown opnum. */
    xdrmem_ncreate(&dec, buf, xdr_getpos(&enc), XDR_DECODE);
    ASSERT_EQ(nfs4_decode_compound_args(&dec, tag, sizeof(tag),
                                        &minorver, decoded,
                                        NFS4_MAX_OPS, &op_count), 0);
    ASSERT_EQ(op_count, (uint32_t)2);
    ASSERT_EQ(decoded[0].opnum, OP_SEQUENCE);
    ASSERT_EQ((uint32_t)decoded[1].opnum, (uint32_t)9999);

    /* Encode result: the unknown op result should cause failure. */
    {
        struct nfs4_result res[2];
        char resbuf[BUF_SIZE];
        XDR renc;

        memset(res, 0, sizeof(res));
        res[0].opnum = OP_SEQUENCE;
        res[0].status = NFS4_OK;
        memset(res[0].res.sequence.session_id, 0xAA, SESSION_ID_SIZE);
        res[0].res.sequence.slot_id = 0;
        res[0].res.sequence.seq_id = 1;
        res[0].res.sequence.highest_slot_id = 7;
        res[0].res.sequence.target_highest_slot_id = 7;

        res[1].opnum = (enum nfs_opnum4)9999;
        res[1].status = NFS4ERR_OP_ILLEGAL;

        /*
         * Even with an error status the encoder should succeed,
         * because error results are status-only (no body).
         */
        xdrmem_ncreate(&renc, resbuf, sizeof(resbuf), XDR_ENCODE);
        ASSERT_EQ(nfs4_encode_compound_res(&renc, NFS4ERR_OP_ILLEGAL,
                                           "test", res, 2), 0);
    }
}

/* -----------------------------------------------------------------------
 * Phase 8a -- GET_DIR_DELEGATION (RFC 8881 S18.39) wire round-trips
 * ----------------------------------------------------------------------- */

/*
 * decode_op_get_dir_delegation lives in xdr_ops_dir_deleg.c and is
 * exported through the private xdr_internal.h header.  The test
 * target deliberately excludes src/mds from its include path, so
 * forward-declare the symbol here (same pattern as op_io_advise in
 * test_nfsv42.c).
 */
bool decode_op_get_dir_delegation(XDR *xdrs, struct nfs4_op *op);

/**
 * Encode GDD args onto a wire buffer manually, then decode them via
 * the new decoder and verify every field round-trips.  Also checks
 * that the decoder consumes exactly the bytes that were written so a
 * following op in the compound can decode cleanly.
 */
static void test_get_dir_delegation_decode_round_trip(void)
{
    char buf[BUF_SIZE];
    XDR enc, dec;
    struct nfs4_op op;
    uint32_t bytes_in;

    /* Build GDD args: signal=1, notify-types bitmap [0x1F],
     * child_delay={10,0}, dir_delay={20,500}, child_attrs [0xAA,0x01],
     * dir_attrs [0x55]. */
    xdrmem_ncreate(&enc, buf, sizeof(buf), XDR_ENCODE);
    {
        uint32_t signal_val = 1;
        uint32_t bm_count_1 = 1;
        uint32_t bm_w0 = 0x1F;
        int64_t cd_sec = 10;
        uint32_t cd_nsec = 0;
        int64_t dd_sec = 20;
        uint32_t dd_nsec = 500;
        uint32_t bm_count_2 = 2;
        uint32_t ca_w0 = 0xAA;
        uint32_t ca_w1 = 0x01;
        uint32_t bm_count_3 = 1;
        uint32_t da_w0 = 0x55;

        ASSERT_TRUE(xdr_uint32_t(&enc, &signal_val));
        ASSERT_TRUE(xdr_uint32_t(&enc, &bm_count_1));
        ASSERT_TRUE(xdr_uint32_t(&enc, &bm_w0));
        ASSERT_TRUE(xdr_int64_t(&enc, &cd_sec));
        ASSERT_TRUE(xdr_uint32_t(&enc, &cd_nsec));
        ASSERT_TRUE(xdr_int64_t(&enc, &dd_sec));
        ASSERT_TRUE(xdr_uint32_t(&enc, &dd_nsec));
        ASSERT_TRUE(xdr_uint32_t(&enc, &bm_count_2));
        ASSERT_TRUE(xdr_uint32_t(&enc, &ca_w0));
        ASSERT_TRUE(xdr_uint32_t(&enc, &ca_w1));
        ASSERT_TRUE(xdr_uint32_t(&enc, &bm_count_3));
        ASSERT_TRUE(xdr_uint32_t(&enc, &da_w0));
    }
    bytes_in = xdr_getpos(&enc);

    memset(&op, 0, sizeof(op));
    op.opnum = OP_GET_DIR_DELEGATION;
    xdrmem_ncreate(&dec, buf, bytes_in, XDR_DECODE);
    ASSERT_TRUE(decode_op_get_dir_delegation(&dec, &op));
    /* Decoder must consume every byte written. */
    ASSERT_EQ(xdr_getpos(&dec), bytes_in);

    ASSERT_EQ(op.arg.get_dir_delegation.signal_deleg_avail, true);
    ASSERT_EQ(op.arg.get_dir_delegation.notification_types_words, (uint32_t)1);
    ASSERT_EQ(op.arg.get_dir_delegation.notification_types[0], (uint32_t)0x1F);
    ASSERT_EQ(op.arg.get_dir_delegation.child_attr_delay.tv_sec, (time_t)10);
    ASSERT_EQ(op.arg.get_dir_delegation.child_attr_delay.tv_nsec, (long)0);
    ASSERT_EQ(op.arg.get_dir_delegation.dir_attr_delay.tv_sec, (time_t)20);
    ASSERT_EQ(op.arg.get_dir_delegation.dir_attr_delay.tv_nsec, (long)500);
    ASSERT_EQ(op.arg.get_dir_delegation.child_attributes_words, (uint32_t)2);
    ASSERT_EQ(op.arg.get_dir_delegation.child_attributes[0], (uint32_t)0xAA);
    ASSERT_EQ(op.arg.get_dir_delegation.child_attributes[1], (uint32_t)0x01);
    ASSERT_EQ(op.arg.get_dir_delegation.dir_attributes_words, (uint32_t)1);
    ASSERT_EQ(op.arg.get_dir_delegation.dir_attributes[0], (uint32_t)0x55);
}

/**
 * Encode an NFS4ERR_DIRDELEG_UNAVAIL GDD result through
 * nfs4_encode_compound_res and verify the wire shape: after the
 * compound status + tag + resarray-count + opnum + per-op-status,
 * the non-fatal body contains a single bool (will_signal=false).
 */
static void test_get_dir_delegation_unavail_body(void)
{
    char buf[BUF_SIZE];
    XDR enc, dec;
    struct nfs4_result results[1];
    uint32_t compound_status, tag_len;
    uint32_t res_count, op_val, op_status, will_signal;
    char tag_out[64];

    memset(results, 0, sizeof(results));
    results[0].opnum = OP_GET_DIR_DELEGATION;
    results[0].status = NFS4ERR_DIRDELEG_UNAVAIL;
    results[0].res.get_dir_delegation.will_signal_deleg_avail = false;

    xdrmem_ncreate(&enc, buf, sizeof(buf), XDR_ENCODE);
    ASSERT_EQ(nfs4_encode_compound_res(&enc, NFS4ERR_DIRDELEG_UNAVAIL,
                                       "gdd", results, 1), 0);

    xdrmem_ncreate(&dec, buf, xdr_getpos(&enc), XDR_DECODE);
    ASSERT_TRUE(xdr_uint32_t(&dec, &compound_status));
    ASSERT_EQ(compound_status, (uint32_t)NFS4ERR_DIRDELEG_UNAVAIL);
    ASSERT_TRUE(xdr_uint32_t(&dec, &tag_len));
    ASSERT_EQ(tag_len, (uint32_t)3);
    ASSERT_TRUE(xdr_opaque_decode(&dec, tag_out, tag_len));
    tag_out[tag_len] = '\0';
    ASSERT_STR_EQ(tag_out, "gdd");

    ASSERT_TRUE(xdr_uint32_t(&dec, &res_count));
    ASSERT_EQ(res_count, (uint32_t)1);
    ASSERT_TRUE(xdr_uint32_t(&dec, &op_val));
    ASSERT_EQ(op_val, (uint32_t)OP_GET_DIR_DELEGATION);
    ASSERT_TRUE(xdr_uint32_t(&dec, &op_status));
    ASSERT_EQ(op_status, (uint32_t)NFS4ERR_DIRDELEG_UNAVAIL);
    /* Non-fatal body: a single bool (will_signal_deleg_avail=0). */
    ASSERT_TRUE(xdr_uint32_t(&dec, &will_signal));
    ASSERT_EQ(will_signal, (uint32_t)0);
}

/* -----------------------------------------------------------------------
 * Main
 * ----------------------------------------------------------------------- */

static void test_rpc_cred_flavor_extraction(void)
{
	fprintf(stdout, "  test_rpc_cred_flavor_extraction: ");

	char buf[512];
	XDR enc, dec;
	uint32_t xid = 0xBBBB;
	uint32_t msg_type = 0;  /* CALL */
	uint32_t rpcvers = 2;
	uint32_t prog = NFS_PROGRAM;
	uint32_t vers = NFS_V4;
	uint32_t proc = NFSPROC4_COMPOUND;

	xdrmem_ncreate(&enc, buf, sizeof(buf), XDR_ENCODE);
	(void)xdr_uint32_t(&enc, &xid);
	(void)xdr_uint32_t(&enc, &msg_type);
	(void)xdr_uint32_t(&enc, &rpcvers);
	(void)xdr_uint32_t(&enc, &prog);
	(void)xdr_uint32_t(&enc, &vers);
	(void)xdr_uint32_t(&enc, &proc);

	/* AUTH_SYS credential: flavor=1, body_len=0 */
	uint32_t cred_flavor = 1;
	uint32_t cred_len = 0;
	(void)xdr_uint32_t(&enc, &cred_flavor);
	(void)xdr_uint32_t(&enc, &cred_len);

	/* NULL verifier: flavor=0, body_len=0 */
	uint32_t verf_flavor = 0;
	uint32_t verf_len = 0;
	(void)xdr_uint32_t(&enc, &verf_flavor);
	(void)xdr_uint32_t(&enc, &verf_len);

	xdrmem_ncreate(&dec, buf, xdr_getpos(&enc), XDR_DECODE);

	uint32_t out_xid, out_prog, out_vers, out_proc;
	uint32_t out_flavor = 0;
	ASSERT_EQ(rpc_decode_call_header(&dec, &out_xid, &out_prog,
		&out_vers, &out_proc, &out_flavor, NULL, NULL, NULL, NULL, NULL), 0);
	ASSERT_EQ(out_xid, (uint32_t)0xBBBB);
	ASSERT_EQ(out_flavor, (uint32_t)1);  /* AUTH_SYS */
}


int main(void)
{
    fprintf(stdout, "test_xdr_codec:\n");

    /* Type codecs */
    RUN_TEST(test_fh_round_trip);
    RUN_TEST(test_fh_zero);
    RUN_TEST(test_stateid_round_trip);
    RUN_TEST(test_bitmap_round_trip);
    RUN_TEST(test_bitmap_helpers);
    RUN_TEST(test_time_round_trip);

    /* fattr4 */
    RUN_TEST(test_fattr_encode_decode);
    RUN_TEST(test_fattr_change_attr_type_monotonic);
    RUN_TEST(test_fattr_word2_bitmap_order);
    RUN_TEST(test_fattr_space_attrs_unlimited_when_no_fs_space);
    RUN_TEST(test_fattr_space_attrs_clamped_when_fs_space_unlimited);
    RUN_TEST(test_fattr_empty_request);
    RUN_TEST(test_fattr_decode_layout_hint_after_mode);
    RUN_TEST(test_fattr_decode_unsupported_layout_hint_absent);
    RUN_TEST(test_fattr_encode_referral_fs_locations);

    /* Compound args decode */
    RUN_TEST(test_compound_sequence_putrootfh_getfh);
    RUN_TEST(test_compound_putfh_lookup);
    RUN_TEST(test_compound_remove);
    RUN_TEST(test_compound_readdir_decode);
    RUN_TEST(test_compound_close);
    RUN_TEST(test_compound_open_owner_decode);
    RUN_TEST(test_setattr_decode_layout_hint);
    RUN_TEST(test_open_create_decode_layout_hint);

    /* Compound result encode */
    RUN_TEST(test_compound_result_encode);
    RUN_TEST(test_compound_result_error_stops);
    RUN_TEST(test_readdir_result_encode);
    RUN_TEST(test_open_result_encode);
    RUN_TEST(test_exchange_id_result_encode);
    RUN_TEST(test_create_session_result_encode);

    /* RPC headers */
    RUN_TEST(test_rpc_header_round_trip);
    RUN_TEST(test_rpc_accepted_reply);
    RUN_TEST(test_rpc_error_reply);

    /* RECLAIM_COMPLETE */
    RUN_TEST(test_reclaim_complete_round_trip);

    /* Edge cases */
    RUN_TEST(test_decode_truncated_fails);
    RUN_TEST(test_rpc_bad_msg_type_fails);

    /* Unknown opnum */
    RUN_TEST(test_unknown_opnum_decode);
    RUN_TEST(test_rpc_cred_flavor_extraction);

    /* Phase 8a -- GET_DIR_DELEGATION wire format */
    RUN_TEST(test_get_dir_delegation_decode_round_trip);
    RUN_TEST(test_get_dir_delegation_unavail_body);

    fprintf(stdout, "\n  %d/%d tests passed.\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
