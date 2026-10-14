/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_fdb_codec.c -- FoundationDB value codec (fdb_codec.[ch]) and key
 * registry (fdb_keys.h) tests.
 *
 * Pure encoding tests; no cluster and no fdb_c client needed.  Attacks
 * the codec's invariants rather than the happy path: every decoder
 * rejects a short, padded, wrong-version or out-of-bound value; the
 * directory split leaves the blob's counter copies dead and compose
 * restores them from the side values with the documented clamp; keys
 * built from big-endian ids sort numerically, names sort bytewise, and
 * the range helpers cover exactly one prefix.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "pnfs_mds.h"
#include "fdb_codec.h"
#include "fdb_keys.h"

static int tests_run;
static int tests_passed;
static int test_failed;

#define ASSERT_TRUE(cond) do { \
    if (!(cond)) { \
        fprintf(stderr, "  FAIL %s:%d: !(%s)\n", __FILE__, __LINE__, #cond); \
        test_failed = 1; \
        return; \
    } \
} while (0)

#define ASSERT_EQ(a, b) do { \
    long long _a = (long long)(a); \
    long long _b = (long long)(b); \
    if (_a != _b) { \
        fprintf(stderr, "  FAIL %s:%d: %s (%lld) != %s (%lld)\n", \
                __FILE__, __LINE__, #a, _a, #b, _b); \
        test_failed = 1; \
        return; \
    } \
} while (0)

#define RUN_TEST(fn) do { \
    fprintf(stdout, "  %-48s", #fn); \
    fflush(stdout); \
    tests_run++; \
    test_failed = 0; \
    fn(); \
    if (test_failed == 0) { \
        tests_passed++; \
        fprintf(stdout, "ok\n"); \
    } else { \
        fprintf(stdout, "FAILED\n"); \
    } \
} while (0)

/* ----------------------------------------------------------------------- */

static void fill_inode(struct mds_inode *i)
{
    uint32_t k;

    memset(i, 0, sizeof(*i));
    i->fileid = 0x0102030405060708ULL;
    i->type = MDS_FTYPE_REG;
    i->mode = 0640;
    i->nlink = 3;
    i->uid = 1000;
    i->gid = 0xFFFFFFFF12345678ULL;
    i->size = (1ULL << 40) + 17;
    i->space_used = 4096;
    i->atime.tv_sec = 1700000000;
    i->atime.tv_nsec = 999999999;
    i->mtime.tv_sec = -5;
    i->mtime.tv_nsec = 1;
    i->ctime.tv_sec = 1;
    i->ctime.tv_nsec = 0;
    i->change = 77;
    i->generation = 5;
    i->flags = MDS_IFLAG_INLINE_STRIPE | MDS_IFLAG_HPC_SHARED;
    i->create_verf = 0xDEADBEEFCAFEF00DULL;
    i->parent_fileid = 2;
    i->synth_suid = 4242;
    i->synth_sgid = 4343;
    i->stripe_count = 1;
    i->stripe_unit = 65536;
    i->mirror_count = 1;
    i->inline_ds_id = 9;
    i->inline_fh_len = 37;
    for (k = 0; k < i->inline_fh_len; k++) {
        i->inline_fh[k] = (uint8_t)(k * 7U + 1U);
    }
    i->ds_map = (struct mds_ds_map_entry *)(uintptr_t)0x1; /* must not be serialised */
}

static bool inode_equal(const struct mds_inode *a, const struct mds_inode *b)
{
    return a->fileid == b->fileid && a->type == b->type && a->mode == b->mode &&
           a->nlink == b->nlink && a->uid == b->uid && a->gid == b->gid &&
           a->size == b->size && a->space_used == b->space_used &&
           a->atime.tv_sec == b->atime.tv_sec && a->atime.tv_nsec == b->atime.tv_nsec &&
           a->mtime.tv_sec == b->mtime.tv_sec && a->mtime.tv_nsec == b->mtime.tv_nsec &&
           a->ctime.tv_sec == b->ctime.tv_sec && a->ctime.tv_nsec == b->ctime.tv_nsec &&
           a->change == b->change && a->generation == b->generation &&
           a->flags == b->flags && a->create_verf == b->create_verf &&
           a->parent_fileid == b->parent_fileid && a->synth_suid == b->synth_suid &&
           a->synth_sgid == b->synth_sgid && a->stripe_count == b->stripe_count &&
           a->stripe_unit == b->stripe_unit && a->mirror_count == b->mirror_count &&
           a->inline_ds_id == b->inline_ds_id && a->inline_fh_len == b->inline_fh_len &&
           memcmp(a->inline_fh, b->inline_fh, a->inline_fh_len) == 0;
}

/* -----------------------------------------------------------------------
 * Little-endian helpers and timestamps
 * ----------------------------------------------------------------------- */

static void test_le_helpers(void)
{
    uint8_t b[8];
    uint64_t v = 0;

    fdb_le64_put(b, 0x0102030405060708ULL);
    ASSERT_EQ(b[0], 0x08);
    ASSERT_EQ(b[7], 0x01);
    ASSERT_EQ(fdb_le64_get(b), 0x0102030405060708ULL);
    fdb_le32_put(b, 0xAABBCCDDU);
    ASSERT_EQ(b[0], 0xDD);
    ASSERT_EQ(fdb_le32_get(b), 0xAABBCCDDU);
    fdb_le64_put(b, 42);
    ASSERT_TRUE(fdb_le64_decode(b, 8, &v));
    ASSERT_EQ(v, 42);
    ASSERT_TRUE(!fdb_le64_decode(b, 7, &v));
    ASSERT_TRUE(!fdb_le64_decode(b, 9, &v));
    ASSERT_TRUE(!fdb_le64_decode(NULL, 8, &v));
}

static void test_timespec_ns(void)
{
    struct timespec ts;
    struct timespec back;

    ts.tv_sec = 1700000000;
    ts.tv_nsec = 123456789;
    ASSERT_EQ(fdb_ts_to_ns(ts), 1700000000123456789LL);
    back = fdb_ns_to_ts(fdb_ts_to_ns(ts));
    ASSERT_EQ(back.tv_sec, ts.tv_sec);
    ASSERT_EQ(back.tv_nsec, ts.tv_nsec);

    /* Before the epoch: tv_nsec stays normalised. */
    ts.tv_sec = -1;
    ts.tv_nsec = 500000000;
    ASSERT_EQ(fdb_ts_to_ns(ts), -500000000LL);
    back = fdb_ns_to_ts(-500000000LL);
    ASSERT_EQ(back.tv_sec, -1);
    ASSERT_EQ(back.tv_nsec, 500000000);

    /* Saturation instead of overflow. */
    ts.tv_sec = INT64_MAX;
    ts.tv_nsec = 0;
    ASSERT_EQ(fdb_ts_to_ns(ts), INT64_MAX);
}

/* -----------------------------------------------------------------------
 * Inode
 * ----------------------------------------------------------------------- */

static void test_inode_round_trip(void)
{
    struct mds_inode in;
    struct mds_inode out;
    uint8_t buf[FDB_INODE_ENC_MAX];
    size_t len = 0;

    fill_inode(&in);
    ASSERT_TRUE(fdb_inode_encode(&in, buf, sizeof(buf), &len));
    ASSERT_EQ(len, FDB_INODE_ENC_FIXED + 37);
    ASSERT_EQ(buf[0], FDB_INODE_VERSION);
    memset(&out, 0xAA, sizeof(out));
    ASSERT_TRUE(fdb_inode_decode(buf, len, &out));
    ASSERT_TRUE(inode_equal(&in, &out));
    ASSERT_TRUE(out.ds_map == NULL);

    /* No inline handle: exactly the fixed size. */
    in.inline_fh_len = 0;
    in.flags = 0;
    ASSERT_TRUE(fdb_inode_encode(&in, buf, sizeof(buf), &len));
    ASSERT_EQ(len, FDB_INODE_ENC_FIXED);
    ASSERT_TRUE(fdb_inode_decode(buf, len, &out));
    ASSERT_TRUE(inode_equal(&in, &out));

    /* Every file type round-trips. */
    for (int t = MDS_FTYPE_REG; t <= MDS_FTYPE_SOCK; t++) {
        in.type = (enum mds_file_type)t;
        ASSERT_TRUE(fdb_inode_encode(&in, buf, sizeof(buf), &len));
        ASSERT_TRUE(fdb_inode_decode(buf, len, &out));
        ASSERT_EQ(out.type, t);
    }
}

static void test_inode_encode_rejects(void)
{
    struct mds_inode in;
    uint8_t buf[FDB_INODE_ENC_MAX];
    size_t len = 0;

    fill_inode(&in);
    in.inline_fh_len = MDS_NFS_FH_MAX + 1;
    ASSERT_TRUE(!fdb_inode_encode(&in, buf, sizeof(buf), &len));
    fill_inode(&in);
    in.type = (enum mds_file_type)0;
    ASSERT_TRUE(!fdb_inode_encode(&in, buf, sizeof(buf), &len));
    fill_inode(&in);
    in.type = (enum mds_file_type)8;
    ASSERT_TRUE(!fdb_inode_encode(&in, buf, sizeof(buf), &len));
    fill_inode(&in);
    in.mtime.tv_nsec = 1000000000L;
    ASSERT_TRUE(!fdb_inode_encode(&in, buf, sizeof(buf), &len));
    fill_inode(&in);
    in.ctime.tv_nsec = -1;
    ASSERT_TRUE(!fdb_inode_encode(&in, buf, sizeof(buf), &len));
    /* Buffer too small: nothing written, refused. */
    fill_inode(&in);
    ASSERT_TRUE(!fdb_inode_encode(&in, buf, FDB_INODE_ENC_FIXED + 10, &len));
    ASSERT_TRUE(!fdb_inode_encode(&in, buf, 0, &len));
    ASSERT_TRUE(!fdb_inode_encode(NULL, buf, sizeof(buf), &len));
    ASSERT_TRUE(!fdb_inode_encode(&in, NULL, sizeof(buf), &len));
    ASSERT_TRUE(!fdb_inode_encode(&in, buf, sizeof(buf), NULL));
}

static void test_inode_decode_rejects(void)
{
    struct mds_inode in;
    struct mds_inode out;
    uint8_t buf[FDB_INODE_ENC_MAX + 1];
    size_t len = 0;

    fill_inode(&in);
    ASSERT_TRUE(fdb_inode_encode(&in, buf, sizeof(buf), &len));

    /* Short by one, padded by one, empty. */
    ASSERT_TRUE(!fdb_inode_decode(buf, len - 1, &out));
    buf[len] = 0;
    ASSERT_TRUE(!fdb_inode_decode(buf, len + 1, &out));
    ASSERT_TRUE(!fdb_inode_decode(buf, 0, &out));
    ASSERT_TRUE(!fdb_inode_decode(buf, FDB_INODE_ENC_FIXED - 1, &out));
    ASSERT_TRUE(!fdb_inode_decode(NULL, len, &out));
    ASSERT_TRUE(!fdb_inode_decode(buf, len, NULL));

    /* Wrong version. */
    buf[0] = (uint8_t)(FDB_INODE_VERSION + 1);
    ASSERT_TRUE(!fdb_inode_decode(buf, len, &out));
    buf[0] = (uint8_t)FDB_INODE_VERSION;
    ASSERT_TRUE(fdb_inode_decode(buf, len, &out));

    /* Type outside the enum. */
    buf[1] = 0;
    ASSERT_TRUE(!fdb_inode_decode(buf, len, &out));
    buf[1] = 9;
    ASSERT_TRUE(!fdb_inode_decode(buf, len, &out));
    buf[1] = (uint8_t)MDS_FTYPE_REG;

    /* inline_fh_len field beyond MDS_NFS_FH_MAX (offset 148..151). */
    fdb_le32_put(buf + 148, MDS_NFS_FH_MAX + 1);
    ASSERT_TRUE(!fdb_inode_decode(buf, len, &out));
    /* inline_fh_len that disagrees with the buffer length. */
    fdb_le32_put(buf + 148, 36);
    ASSERT_TRUE(!fdb_inode_decode(buf, len, &out));
    fdb_le32_put(buf + 148, 37);
    ASSERT_TRUE(fdb_inode_decode(buf, len, &out));

    /* tv_nsec at 1e9 (atime nsec at offset 56+8 = 64). */
    fdb_le32_put(buf + 64, 1000000000U);
    ASSERT_TRUE(!fdb_inode_decode(buf, len, &out));
}

static void test_inode_split_compose(void)
{
    struct mds_inode dir;
    struct mds_inode blob;
    struct mds_inode file;
    struct fdb_dir_counters ctr;

    fill_inode(&dir);
    dir.type = MDS_FTYPE_DIR;
    dir.nlink = 7;
    dir.change = 99;
    fdb_inode_split(&dir, &blob, &ctr);
    ASSERT_EQ(ctr.nlink, 7);
    ASSERT_EQ(ctr.change, 99);
    ASSERT_EQ((int64_t)ctr.mtime_ns, fdb_ts_to_ns(dir.mtime));
    ASSERT_EQ((int64_t)ctr.ctime_ns, fdb_ts_to_ns(dir.ctime));
    /* Dead copies are zero, everything else intact. */
    ASSERT_EQ(blob.nlink, 0);
    ASSERT_EQ(blob.change, 0);
    ASSERT_EQ(blob.mtime.tv_sec, 0);
    ASSERT_EQ(blob.ctime.tv_nsec, 0);
    ASSERT_EQ(blob.mode, dir.mode);
    ASSERT_EQ(blob.fileid, dir.fileid);

    /* Compose restores the four fields. */
    fdb_inode_compose(&blob, &ctr);
    ASSERT_TRUE(inode_equal(&blob, &dir));

    /* Clamp: a counter driven below zero reads 0; above UINT32_MAX saturates. */
    ctr.nlink = (uint64_t)-1;
    fdb_inode_compose(&blob, &ctr);
    ASSERT_EQ(blob.nlink, 0);
    ctr.nlink = (uint64_t)UINT32_MAX + 5U;
    fdb_inode_compose(&blob, &ctr);
    ASSERT_EQ(blob.nlink, UINT32_MAX);

    /* Files: split copies verbatim, compose is a no-op. */
    fill_inode(&file);
    fdb_inode_split(&file, &blob, &ctr);
    ASSERT_EQ(ctr.nlink, 0);
    ASSERT_TRUE(inode_equal(&blob, &file));
    ctr.nlink = 12345;
    fdb_inode_compose(&blob, &ctr);
    ASSERT_EQ(blob.nlink, file.nlink);
}

/* -----------------------------------------------------------------------
 * Dirent and dirent-seq rows
 * ----------------------------------------------------------------------- */

static void test_dirent_codec(void)
{
    struct fdb_dirent_val in = { 0x1122334455667788ULL, 0x99AABBCCDDEEFF00ULL,
                                 (uint8_t)MDS_FTYPE_DIR };
    struct fdb_dirent_val out;
    uint8_t buf[FDB_DIRENT_ENC_SIZE + 1];
    size_t len = 0;

    ASSERT_TRUE(fdb_dirent_encode(&in, buf, sizeof(buf), &len));
    ASSERT_EQ(len, FDB_DIRENT_ENC_SIZE);
    ASSERT_TRUE(fdb_dirent_decode(buf, len, &out));
    ASSERT_EQ(out.child_fileid, in.child_fileid);
    ASSERT_EQ(out.seq, in.seq);
    ASSERT_EQ(out.type, in.type);
    ASSERT_TRUE(!fdb_dirent_decode(buf, len - 1, &out));
    ASSERT_TRUE(!fdb_dirent_decode(buf, len + 1, &out));
    buf[0] = 2;
    ASSERT_TRUE(!fdb_dirent_decode(buf, len, &out));
    buf[0] = (uint8_t)FDB_DIRENT_VERSION;
    buf[9] = 0; /* type */
    ASSERT_TRUE(!fdb_dirent_decode(buf, len, &out));
    in.type = 0;
    ASSERT_TRUE(!fdb_dirent_encode(&in, buf, sizeof(buf), &len));
    in.type = (uint8_t)MDS_FTYPE_REG;
    ASSERT_TRUE(!fdb_dirent_encode(&in, buf, FDB_DIRENT_ENC_SIZE - 1, &len));
}

static void test_dirent_seq_codec(void)
{
    struct fdb_dirent_seq_val in;
    struct fdb_dirent_seq_val out;
    uint8_t buf[FDB_DIRENT_SEQ_ENC_MAX + 1];
    size_t len = 0;

    memset(&in, 0, sizeof(in));
    in.child_fileid = 4242;
    in.type = (uint8_t)MDS_FTYPE_SYMLINK;
    memset(in.name, 'n', MDS_MAX_NAME);
    in.name[MDS_MAX_NAME] = '\0';
    ASSERT_TRUE(fdb_dirent_seq_encode(&in, buf, sizeof(buf), &len));
    ASSERT_EQ(len, FDB_DIRENT_SEQ_ENC_FIXED + MDS_MAX_NAME);
    ASSERT_TRUE(fdb_dirent_seq_decode(buf, len, &out));
    ASSERT_EQ(out.child_fileid, 4242);
    ASSERT_EQ(out.type, MDS_FTYPE_SYMLINK);
    ASSERT_TRUE(strcmp(out.name, in.name) == 0);

    /* Empty name (fixed part only), over-long buffer, embedded NUL. */
    ASSERT_TRUE(!fdb_dirent_seq_decode(buf, FDB_DIRENT_SEQ_ENC_FIXED, &out));
    buf[len] = 'x';
    ASSERT_TRUE(!fdb_dirent_seq_decode(buf, len + 1, &out));
    buf[FDB_DIRENT_SEQ_ENC_FIXED + 3] = '\0';
    ASSERT_TRUE(!fdb_dirent_seq_decode(buf, len, &out));

    in.name[0] = '\0';
    ASSERT_TRUE(!fdb_dirent_seq_encode(&in, buf, sizeof(buf), &len));
    (void)snprintf(in.name, sizeof(in.name), "ok");
    ASSERT_TRUE(fdb_dirent_seq_encode(&in, buf, sizeof(buf), &len));
    ASSERT_EQ(len, FDB_DIRENT_SEQ_ENC_FIXED + 2);
}

/* -----------------------------------------------------------------------
 * Stripe rows and GC rows
 * ----------------------------------------------------------------------- */

static void test_stripe_codec(void)
{
    struct fdb_stripe_hdr_val hdr = { MDS_MAX_STRIPES, 65536, MDS_MAX_MIRRORS };
    struct fdb_stripe_hdr_val hout;
    struct mds_ds_map_entry e;
    struct mds_ds_map_entry eout;
    uint8_t buf[FDB_STRIPE_ENT_ENC_MAX + 1];
    size_t len = 0;

    ASSERT_TRUE(fdb_stripe_hdr_encode(&hdr, buf, sizeof(buf), &len));
    ASSERT_EQ(len, FDB_STRIPE_HDR_ENC_SIZE);
    ASSERT_TRUE(fdb_stripe_hdr_decode(buf, len, &hout));
    ASSERT_EQ(hout.stripe_count, MDS_MAX_STRIPES);
    ASSERT_EQ(hout.mirror_count, MDS_MAX_MIRRORS);
    ASSERT_TRUE(!fdb_stripe_hdr_decode(buf, len - 1, &hout));
    ASSERT_TRUE(!fdb_stripe_hdr_decode(buf, len + 1, &hout));
    hdr.stripe_count = 0;
    ASSERT_TRUE(!fdb_stripe_hdr_encode(&hdr, buf, sizeof(buf), &len));
    hdr.stripe_count = MDS_MAX_STRIPES + 1;
    ASSERT_TRUE(!fdb_stripe_hdr_encode(&hdr, buf, sizeof(buf), &len));
    hdr.stripe_count = 1;
    hdr.mirror_count = MDS_MAX_MIRRORS + 1;
    ASSERT_TRUE(!fdb_stripe_hdr_encode(&hdr, buf, sizeof(buf), &len));
    hdr.mirror_count = 1;
    hdr.stripe_unit = 0;
    ASSERT_TRUE(!fdb_stripe_hdr_encode(&hdr, buf, sizeof(buf), &len));
    /* A decoded header is bound-checked too. */
    hdr.stripe_unit = 65536;
    ASSERT_TRUE(fdb_stripe_hdr_encode(&hdr, buf, sizeof(buf), &len));
    fdb_le32_put(buf + 1, MDS_MAX_STRIPES + 1);
    ASSERT_TRUE(!fdb_stripe_hdr_decode(buf, len, &hout));

    memset(&e, 0, sizeof(e));
    e.ds_id = 7;
    e.synth_suid = 1;
    e.synth_sgid = 2;
    e.nfs_fh_len = MDS_NFS_FH_MAX;
    memset(e.nfs_fh, 0x5A, sizeof(e.nfs_fh));
    ASSERT_TRUE(fdb_stripe_ent_encode(&e, buf, sizeof(buf), &len));
    ASSERT_EQ(len, FDB_STRIPE_ENT_ENC_MAX);
    ASSERT_TRUE(fdb_stripe_ent_decode(buf, len, &eout));
    ASSERT_EQ(eout.ds_id, 7);
    ASSERT_EQ(eout.nfs_fh_len, MDS_NFS_FH_MAX);
    ASSERT_TRUE(memcmp(eout.nfs_fh, e.nfs_fh, MDS_NFS_FH_MAX) == 0);
    ASSERT_TRUE(!fdb_stripe_ent_decode(buf, len - 1, &eout));
    buf[len] = 0;
    ASSERT_TRUE(!fdb_stripe_ent_decode(buf, len + 1, &eout));
    e.nfs_fh_len = MDS_NFS_FH_MAX + 1;
    ASSERT_TRUE(!fdb_stripe_ent_encode(&e, buf, sizeof(buf), &len));
    e.nfs_fh_len = 0;
    ASSERT_TRUE(fdb_stripe_ent_encode(&e, buf, sizeof(buf), &len));
    ASSERT_EQ(len, FDB_STRIPE_ENT_ENC_FIXED);
    ASSERT_TRUE(fdb_stripe_ent_decode(buf, len, &eout));
    ASSERT_EQ(eout.nfs_fh_len, 0);
}

static void test_gc_codec(void)
{
    struct mds_gc_entry e;
    struct mds_gc_entry out;
    uint8_t buf[FDB_GC_ENC_MAX + 1];
    size_t len = 0;

    memset(&e, 0, sizeof(e));
    e.gc_seq = 99; /* key, not value */
    e.fileid = 0xABCDEF;
    e.ds_id = 3;
    e.owner_mds_id = 12;
    e.sweep_hint = MDS_GC_SWEEP_GEOM(4, 2);
    e.nfs_fh_len = 5;
    memcpy(e.nfs_fh, "hello", 5);
    ASSERT_TRUE(fdb_gc_encode(&e, buf, sizeof(buf), &len));
    ASSERT_EQ(len, FDB_GC_ENC_FIXED + 5);
    memset(&out, 0xFF, sizeof(out));
    ASSERT_TRUE(fdb_gc_decode(buf, len, &out));
    ASSERT_EQ(out.fileid, 0xABCDEF);
    ASSERT_EQ(out.ds_id, 3);
    ASSERT_EQ(out.owner_mds_id, 12);
    ASSERT_EQ(out.sweep_hint, MDS_GC_SWEEP_GEOM(4, 2));
    ASSERT_EQ(out.nfs_fh_len, 5);
    ASSERT_TRUE(memcmp(out.nfs_fh, "hello", 5) == 0);
    ASSERT_EQ(out.gc_seq, 0); /* the caller fills it from the key */
    ASSERT_TRUE(!fdb_gc_decode(buf, len - 1, &out));
    buf[len] = 0;
    ASSERT_TRUE(!fdb_gc_decode(buf, len + 1, &out));
    buf[0] = 7;
    ASSERT_TRUE(!fdb_gc_decode(buf, len, &out));
}

/* -----------------------------------------------------------------------
 * Node registry row
 * ----------------------------------------------------------------------- */

/* The row carries the registering process's witness epoch, the bound
 * of the open-time witness sweep (catalogue_fdb.c), after the heartbeat
 * stamp; a row of the previous layout (version 1, no such field) is
 * rejected by its version byte, never read with shifted fields. */
static void test_node_codec(void)
{
    struct fdb_node_val in;
    struct fdb_node_val out;
    uint8_t buf[FDB_NODE_ENC_MAX + 1];
    size_t len = 0;

    memset(&in, 0, sizeof(in));
    in.boot_epoch = 0x1122334455667788ULL;
    in.last_heartbeat_ns = 0x99AABBCCDDEEFF00ULL;
    in.witness_epoch = 0x0102030405060708ULL;
    in.nfs_port = 2049;
    in.grpc_port = 9401;
    in.state = 2;
    (void)snprintf(in.sw_version, sizeof(in.sw_version), "1.2.3");
    (void)snprintf(in.hostname, sizeof(in.hostname), "mds-a");
    ASSERT_TRUE(fdb_node_encode(&in, buf, sizeof(buf), &len));
    ASSERT_EQ(len, FDB_NODE_ENC_FIXED + 5 + 5);
    ASSERT_EQ(buf[0], FDB_NODE_VERSION);
    /* version, boot_epoch, two ports, state, heartbeat: offset 22. */
    ASSERT_EQ(fdb_le64_get(buf + 22), in.witness_epoch);
    memset(&out, 0xAA, sizeof(out));
    ASSERT_TRUE(fdb_node_decode(buf, len, &out));
    ASSERT_EQ(out.boot_epoch, in.boot_epoch);
    ASSERT_EQ(out.last_heartbeat_ns, in.last_heartbeat_ns);
    ASSERT_EQ(out.witness_epoch, in.witness_epoch);
    ASSERT_EQ(out.nfs_port, 2049);
    ASSERT_EQ(out.grpc_port, 9401);
    ASSERT_EQ(out.state, 2);
    ASSERT_TRUE(strcmp(out.sw_version, "1.2.3") == 0);
    ASSERT_TRUE(strcmp(out.hostname, "mds-a") == 0);

    /* An unknown epoch (0) round-trips as such. */
    in.witness_epoch = 0;
    ASSERT_TRUE(fdb_node_encode(&in, buf, sizeof(buf), &len));
    ASSERT_TRUE(fdb_node_decode(buf, len, &out));
    ASSERT_EQ(out.witness_epoch, 0);

    /* Truncated by one, padded by one, shorter than the fixed part. */
    ASSERT_TRUE(!fdb_node_decode(buf, len - 1, &out));
    buf[len] = 0;
    ASSERT_TRUE(!fdb_node_decode(buf, len + 1, &out));
    ASSERT_TRUE(!fdb_node_decode(buf, FDB_NODE_ENC_FIXED - 1, &out));
    /* The previous layout's version byte. */
    buf[0] = 1;
    ASSERT_TRUE(!fdb_node_decode(buf, len, &out));
    buf[0] = (uint8_t)FDB_NODE_VERSION;
    ASSERT_TRUE(fdb_node_decode(buf, len, &out));

    /* Encoder bounds: a buffer one byte short, an empty hostname. */
    ASSERT_TRUE(!fdb_node_encode(&in, buf, len - 1, &len));
    in.hostname[0] = '\0';
    ASSERT_TRUE(!fdb_node_encode(&in, buf, sizeof(buf), &len));
}

/* -----------------------------------------------------------------------
 * Keys
 * ----------------------------------------------------------------------- */

static int key_cmp(const struct fdb_key *a, const struct fdb_key *b)
{
    uint32_t n = a->len < b->len ? a->len : b->len;
    int c = memcmp(a->buf, b->buf, n);

    if (c != 0) {
        return c;
    }
    return (a->len < b->len) ? -1 : (a->len > b->len ? 1 : 0);
}

static void test_key_layout(void)
{
    struct fdb_key_prefix p;
    struct fdb_key k;

    memset(&p, 0, sizeof(p));
    memcpy(p.bytes, "px", 2);
    p.len = 2;

    fdb_key_witness(&k, &p, 0x01020304U, 0x0A0B0C0D0E0F1011ULL, 5);
    ASSERT_TRUE(fdb_key_ok(&k));
    ASSERT_EQ(k.len, 2 + 1 + 4 + 8 + 4);
    ASSERT_EQ(k.buf[0], 'p');
    ASSERT_EQ(k.buf[2], FDB_KT_WITNESS);
    ASSERT_EQ(k.buf[3], 0x01);       /* be32 mds_id */
    ASSERT_EQ(k.buf[6], 0x04);
    ASSERT_EQ(k.buf[7], 0x0A);       /* be64 epoch */
    ASSERT_EQ(k.buf[14], 0x11);
    ASSERT_EQ(k.buf[18], 5);         /* be32 slot */

    fdb_key_inode(&k, &p, 2, FDB_INODE_PART_CHANGE);
    ASSERT_EQ(k.len, 2 + 1 + 8 + 1);
    ASSERT_EQ(k.buf[2], FDB_KT_INODE);
    ASSERT_EQ(k.buf[10], 2);
    ASSERT_EQ(k.buf[11], FDB_INODE_PART_CHANGE);

    fdb_key_dirent(&k, &p, 2, "abc");
    ASSERT_TRUE(fdb_key_ok(&k));
    ASSERT_EQ(k.len, 2 + 1 + 8 + 3);
    ASSERT_TRUE(memcmp(k.buf + 11, "abc", 3) == 0);

    /* No prefix at all. */
    fdb_key_meta(&k, NULL, FDB_META_FILEID);
    ASSERT_EQ(k.len, 2);
    ASSERT_EQ(k.buf[0], FDB_KT_META);
    ASSERT_EQ(k.buf[1], FDB_META_FILEID);
}

static void test_key_name_bounds(void)
{
    struct fdb_key k;
    char name[MDS_MAX_NAME + 2];

    memset(name, 'a', sizeof(name));
    name[MDS_MAX_NAME] = '\0';
    fdb_key_dirent(&k, NULL, 1, name);
    ASSERT_TRUE(fdb_key_ok(&k));
    ASSERT_EQ(k.len, 1 + 8 + MDS_MAX_NAME);

    name[MDS_MAX_NAME] = 'a';
    name[MDS_MAX_NAME + 1] = '\0';
    fdb_key_dirent(&k, NULL, 1, name);
    ASSERT_TRUE(!fdb_key_ok(&k));

    fdb_key_dirent(&k, NULL, 1, "");
    ASSERT_TRUE(!fdb_key_ok(&k));
    fdb_key_dirent(&k, NULL, 1, NULL);
    ASSERT_TRUE(!fdb_key_ok(&k));

    /* A mid-key name is NUL-terminated. */
    fdb_key_init(&k, NULL, FDB_KT_EXT_DIRENT);
    fdb_key_name(&k, "ab", true);
    fdb_key_be32(&k, 1);
    ASSERT_TRUE(fdb_key_ok(&k));
    ASSERT_EQ(k.len, 1 + 2 + 1 + 4);
    ASSERT_EQ(k.buf[3], 0);

    /* Appending past FDB_KEY_MAX is refused, not truncated. */
    fdb_key_init(&k, NULL, FDB_KT_XATTR);
    while (fdb_key_ok(&k)) {
        fdb_key_be64(&k, 0);
    }
    ASSERT_TRUE(k.len <= FDB_KEY_MAX);
}

static void test_key_ordering(void)
{
    struct fdb_key a;
    struct fdb_key b;
    const uint64_t ids[] = { 0, 1, 2, 255, 256, 65535, 65536, 1ULL << 32, (1ULL << 40) + 3,
                             UINT64_MAX - 1, UINT64_MAX };
    size_t i;

    /* Big-endian fileids sort numerically under bytewise comparison. */
    for (i = 1; i < sizeof(ids) / sizeof(ids[0]); i++) {
        fdb_key_inode_prefix(&a, NULL, ids[i - 1]);
        fdb_key_inode_prefix(&b, NULL, ids[i]);
        ASSERT_TRUE(key_cmp(&a, &b) < 0);
    }
    /* All parts of one inode sort inside its prefix range, before the
     * next fileid. */
    fdb_key_inode(&a, NULL, 5, FDB_INODE_PART_CTIME);
    fdb_key_inode_prefix(&b, NULL, 6);
    ASSERT_TRUE(key_cmp(&a, &b) < 0);
    /* Cookies sort numerically within a parent. */
    fdb_key_dirent_seq(&a, NULL, 9, 255);
    fdb_key_dirent_seq(&b, NULL, 9, 256);
    ASSERT_TRUE(key_cmp(&a, &b) < 0);
    /* Names sort bytewise; a prefix sorts before its extension. */
    fdb_key_dirent(&a, NULL, 9, "ab");
    fdb_key_dirent(&b, NULL, 9, "abc");
    ASSERT_TRUE(key_cmp(&a, &b) < 0);
    fdb_key_dirent(&b, NULL, 9, "b");
    ASSERT_TRUE(key_cmp(&a, &b) < 0);
    /* Tables never interleave: every INODE key precedes every DIRENT key. */
    fdb_key_inode(&a, NULL, UINT64_MAX, FDB_INODE_PART_CTIME);
    fdb_key_dirent(&b, NULL, 0, "a");
    ASSERT_TRUE(key_cmp(&a, &b) < 0);
}

static void test_key_ranges(void)
{
    struct fdb_key base;
    struct fdb_key k;
    struct fdb_key_range r;

    /* Prefix range of one directory: every name inside, the next parent
     * (and any other table) outside. */
    fdb_key_dirent_prefix(&base, NULL, 7);
    ASSERT_TRUE(fdb_key_range_prefix(&r, &base));
    fdb_key_dirent(&k, NULL, 7, "a");
    ASSERT_TRUE(key_cmp(&r.begin, &k) <= 0 && key_cmp(&k, &r.end) < 0);
    memset(k.buf, 0, sizeof(k.buf));
    fdb_key_dirent_prefix(&k, NULL, 7);
    fdb_key_bytes(&k, "\xff\xff\xff", 3);
    ASSERT_TRUE(key_cmp(&k, &r.end) < 0);
    fdb_key_dirent_prefix(&k, NULL, 8);
    ASSERT_TRUE(key_cmp(&k, &r.end) >= 0);
    fdb_key_dirent_prefix(&k, NULL, 6);
    ASSERT_TRUE(key_cmp(&k, &r.begin) < 0);

    /* strinc over trailing 0xFF bytes. */
    fdb_key_dirent_prefix(&base, NULL, UINT64_MAX);
    ASSERT_TRUE(fdb_key_range_prefix(&r, &base));
    ASSERT_EQ(r.end.len, 1);
    ASSERT_EQ(r.end.buf[0], FDB_KT_DIRENT + 1);
    /* A key of only 0xFF bytes has no strinc. */
    k.len = 3;
    k.overflow = false;
    memset(k.buf, 0xFF, 3);
    ASSERT_TRUE(!fdb_key_strinc(&k));
    ASSERT_EQ(k.len, 3);

    /* Single-key range: exactly the key. */
    fdb_key_dirent(&k, NULL, 7, "x");
    fdb_key_range_single(&r, &k);
    ASSERT_TRUE(key_cmp(&r.begin, &k) == 0);
    ASSERT_EQ(r.end.len, k.len + 1);
    ASSERT_EQ(r.end.buf[r.end.len - 1], 0);
    fdb_key_dirent(&k, NULL, 7, "xa");
    ASSERT_TRUE(key_cmp(&k, &r.end) > 0);

    /* An overflowed key yields no range. */
    fdb_key_dirent(&base, NULL, 7, "");
    ASSERT_TRUE(!fdb_key_range_prefix(&r, &base));
}

/* ----------------------------------------------------------------------- */

int main(void)
{
    printf("test_fdb_codec:\n");
    RUN_TEST(test_le_helpers);
    RUN_TEST(test_timespec_ns);
    RUN_TEST(test_inode_round_trip);
    RUN_TEST(test_inode_encode_rejects);
    RUN_TEST(test_inode_decode_rejects);
    RUN_TEST(test_inode_split_compose);
    RUN_TEST(test_dirent_codec);
    RUN_TEST(test_dirent_seq_codec);
    RUN_TEST(test_stripe_codec);
    RUN_TEST(test_gc_codec);
    RUN_TEST(test_node_codec);
    RUN_TEST(test_key_layout);
    RUN_TEST(test_key_name_bounds);
    RUN_TEST(test_key_ordering);
    RUN_TEST(test_key_ranges);
    printf("\n%d/%d tests passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
