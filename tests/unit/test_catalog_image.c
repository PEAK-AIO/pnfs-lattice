/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_catalog_image.c — Unit tests for the materialized catalog image.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "pnfs_mds.h"
#include "catalog_image.h"
#include "catalog_delta.h"
#include "endian_helpers.h"

#define TEST_INODE_UPSERT_PAYLOAD_SIZE 133U

static int passed;
static int failed;

#define ASSERT_EQ(a, b) do { \
    if ((a) != (b)) { \
        fprintf(stderr, "  FAIL %s:%d: %s (%d) != %s (%d)\n", \
                __FILE__, __LINE__, #a, (int)(a), #b, (int)(b)); \
        failed++; return; \
    } \
} while (0)

#define ASSERT_EQ_U64(a, b) do { \
    if ((a) != (b)) { \
        fprintf(stderr, "  FAIL %s:%d: %s (%llu) != %s (%llu)\n", \
                __FILE__, __LINE__, #a, (unsigned long long)(a), \
                #b, (unsigned long long)(b)); \
        failed++; return; \
    } \
} while (0)

#define ASSERT_TRUE(c) do { \
    if (!(c)) { \
        fprintf(stderr, "  FAIL %s:%d: !(%s)\n", \
                __FILE__, __LINE__, #c); \
        failed++; return; \
    } \
} while (0)

#define PASS() do { passed++; fprintf(stdout, "  PASS\n"); } while (0)

static uint32_t encode_test_inode_payload(const struct mds_inode *inode,
                                          uint8_t *buf, uint32_t cap)
{
    uint32_t off = 0;

    if (inode == NULL || buf == NULL ||
        cap < TEST_INODE_UPSERT_PAYLOAD_SIZE) {
        return 0;
    }

    fdb_put_u64(buf + off, inode->fileid);
    off += 8;
    buf[off++] = (uint8_t)inode->type;
    fdb_put_u32(buf + off, inode->mode);
    off += 4;
    fdb_put_u32(buf + off, inode->nlink);
    off += 4;
    fdb_put_u64(buf + off, inode->uid);
    off += 8;
    fdb_put_u64(buf + off, inode->gid);
    off += 8;
    fdb_put_u64(buf + off, inode->size);
    off += 8;
    fdb_put_u64(buf + off, inode->space_used);
    off += 8;
    fdb_put_u64(buf + off, (uint64_t)(int64_t)inode->atime.tv_sec);
    off += 8;
    fdb_put_u32(buf + off, (uint32_t)inode->atime.tv_nsec);
    off += 4;
    fdb_put_u64(buf + off, (uint64_t)(int64_t)inode->mtime.tv_sec);
    off += 8;
    fdb_put_u32(buf + off, (uint32_t)inode->mtime.tv_nsec);
    off += 4;
    fdb_put_u64(buf + off, (uint64_t)(int64_t)inode->ctime.tv_sec);
    off += 8;
    fdb_put_u32(buf + off, (uint32_t)inode->ctime.tv_nsec);
    off += 4;
    fdb_put_u64(buf + off, inode->change);
    off += 8;
    fdb_put_u64(buf + off, inode->generation);
    off += 8;
    fdb_put_u32(buf + off, inode->flags);
    off += 4;
    fdb_put_u64(buf + off, inode->create_verf);
    off += 8;
    fdb_put_u64(buf + off, inode->parent_fileid);
    off += 8;
    fdb_put_u32(buf + off, inode->stripe_count);
    off += 4;
    fdb_put_u32(buf + off, inode->stripe_unit);
    off += 4;
    fdb_put_u32(buf + off, inode->mirror_count);
    off += 4;

    return off;
}

/* -- Inode tests ------------------------------------------------------- */

static void test_inode_put_get_del(void)
{
    fprintf(stdout, "  test_inode_put_get_del:         ");
    fflush(stdout);

    struct catalog_image *img = NULL;
    ASSERT_EQ(catalog_image_create(&img), 0);

    struct mds_inode ino;
    memset(&ino, 0, sizeof(ino));
    ino.fileid = 100;
    ino.type = MDS_FTYPE_REG;
    ino.mode = 0644;
    ino.nlink = 1;

    ASSERT_EQ(catalog_image_inode_put(img, &ino), 0);

    struct mds_inode got;
    ASSERT_EQ(catalog_image_inode_get(img, 100, &got), 0);
    ASSERT_EQ_U64(got.fileid, 100);
    ASSERT_EQ((int)got.type, MDS_FTYPE_REG);
    ASSERT_EQ(got.mode, 0644);

    /* Not found. */
    ASSERT_EQ(catalog_image_inode_get(img, 999, &got), 1);

    /* Delete. */
    ASSERT_EQ(catalog_image_inode_del(img, 100), 0);
    ASSERT_EQ(catalog_image_inode_get(img, 100, &got), 1);

    catalog_image_destroy(img);
    PASS();
}

static void test_replay_initial_nonzero_seq_marks_gap(void)
{
    fprintf(stdout, "  test_replay_initial_nonzero:   ");
    fflush(stdout);

    struct catalog_image *img = NULL;
    struct mds_inode ino;
    uint8_t payload[TEST_INODE_UPSERT_PAYLOAD_SIZE];
    struct catalog_delta_record rec;

    ASSERT_EQ(catalog_image_create(&img), 0);
    memset(&ino, 0, sizeof(ino));
    ino.fileid = 201;
    ino.type = MDS_FTYPE_REG;
    ino.nlink = 1;
    ASSERT_EQ(encode_test_inode_payload(&ino, payload, sizeof(payload)),
              TEST_INODE_UPSERT_PAYLOAD_SIZE);

    memset(&rec, 0, sizeof(rec));
    rec.type = CAT_DELTA_INODE_UPSERT;
    rec.stream_id = 0;
    rec.seqno = 5;
    rec.timestamp_ns = 100;
    rec.payload_len = TEST_INODE_UPSERT_PAYLOAD_SIZE;
    rec.payload = payload;

    ASSERT_EQ(catalog_image_apply(img, &rec), 0);
    ASSERT_EQ_U64(catalog_image_applied_high_water(img, 0), 5);
    ASSERT_TRUE(!catalog_image_is_complete(img, 0, 5));

    catalog_image_destroy(img);
    PASS();
}

static void test_inode_overwrite(void)
{
    fprintf(stdout, "  test_inode_overwrite:           ");
    fflush(stdout);

    struct catalog_image *img = NULL;
    ASSERT_EQ(catalog_image_create(&img), 0);

    struct mds_inode ino;
    memset(&ino, 0, sizeof(ino));
    ino.fileid = 50;
    ino.nlink = 1;
    ASSERT_EQ(catalog_image_inode_put(img, &ino), 0);

    ino.nlink = 5;
    ASSERT_EQ(catalog_image_inode_put(img, &ino), 0);

    struct mds_inode got;
    ASSERT_EQ(catalog_image_inode_get(img, 50, &got), 0);
    ASSERT_EQ(got.nlink, 5);

    catalog_image_destroy(img);
    PASS();
}

/* -- Dirent tests ------------------------------------------------------ */

static void test_dirent_put_get_del(void)
{
    fprintf(stdout, "  test_dirent_put_get_del:       ");
    fflush(stdout);

    struct catalog_image *img = NULL;
    ASSERT_EQ(catalog_image_create(&img), 0);

    ASSERT_EQ(catalog_image_dirent_put(img, 2, "hello.txt", 100, 1), 0);

    uint64_t fid = 0;
    uint8_t type = 0;
    ASSERT_EQ(catalog_image_dirent_get(img, 2, "hello.txt", &fid, &type), 0);
    ASSERT_EQ_U64(fid, 100);
    ASSERT_EQ(type, 1);

    /* Not found. */
    ASSERT_EQ(catalog_image_dirent_get(img, 2, "nope.txt", &fid, &type), 1);

    /* Delete. */
    ASSERT_EQ(catalog_image_dirent_del(img, 2, "hello.txt"), 0);
    ASSERT_EQ(catalog_image_dirent_get(img, 2, "hello.txt", &fid, &type), 1);

    catalog_image_destroy(img);
    PASS();
}

/* -- READDIR ----------------------------------------------------------- */

struct rd_ctx {
    char names[64][MDS_MAX_NAME + 1];
    int count;
};

static int rd_cb(uint64_t fileid, uint8_t type,
                 const char *name, void *arg)
{
    struct rd_ctx *ctx = arg;
    (void)fileid;
    (void)type;
    if (ctx->count < 64) {
        snprintf(ctx->names[ctx->count], MDS_MAX_NAME + 1, "%s", name);
    }
    ctx->count++;
    return 0;
}

static void test_readdir_sorted(void)
{
    fprintf(stdout, "  test_readdir_sorted:           ");
    fflush(stdout);

    struct catalog_image *img = NULL;
    ASSERT_EQ(catalog_image_create(&img), 0);

    /* Insert in reverse order. */
    ASSERT_EQ(catalog_image_dirent_put(img, 2, "delta", 14, 1), 0);
    ASSERT_EQ(catalog_image_dirent_put(img, 2, "bravo", 12, 1), 0);
    ASSERT_EQ(catalog_image_dirent_put(img, 2, "charlie", 13, 1), 0);
    ASSERT_EQ(catalog_image_dirent_put(img, 2, "alpha", 11, 1), 0);

    struct rd_ctx ctx;
    memset(&ctx, 0, sizeof(ctx));
    ASSERT_EQ(catalog_image_readdir(img, 2, NULL, rd_cb, &ctx), 0);
    ASSERT_EQ(ctx.count, 4);

    /* Must be sorted. */
    ASSERT_TRUE(strcmp(ctx.names[0], "alpha") == 0);
    ASSERT_TRUE(strcmp(ctx.names[1], "bravo") == 0);
    ASSERT_TRUE(strcmp(ctx.names[2], "charlie") == 0);
    ASSERT_TRUE(strcmp(ctx.names[3], "delta") == 0);

    catalog_image_destroy(img);
    PASS();
}

static void test_readdir_start_after(void)
{
    fprintf(stdout, "  test_readdir_start_after:      ");
    fflush(stdout);

    struct catalog_image *img = NULL;
    ASSERT_EQ(catalog_image_create(&img), 0);

    ASSERT_EQ(catalog_image_dirent_put(img, 2, "a", 1, 1), 0);
    ASSERT_EQ(catalog_image_dirent_put(img, 2, "b", 2, 1), 0);
    ASSERT_EQ(catalog_image_dirent_put(img, 2, "c", 3, 1), 0);
    ASSERT_EQ(catalog_image_dirent_put(img, 2, "d", 4, 1), 0);

    struct rd_ctx ctx;
    memset(&ctx, 0, sizeof(ctx));
    ASSERT_EQ(catalog_image_readdir(img, 2, "b", rd_cb, &ctx), 0);
    ASSERT_EQ(ctx.count, 2);
    ASSERT_TRUE(strcmp(ctx.names[0], "c") == 0);
    ASSERT_TRUE(strcmp(ctx.names[1], "d") == 0);

    catalog_image_destroy(img);
    PASS();
}

static void test_readdir_empty_dir(void)
{
    fprintf(stdout, "  test_readdir_empty_dir:        ");
    fflush(stdout);

    struct catalog_image *img = NULL;
    ASSERT_EQ(catalog_image_create(&img), 0);

    struct rd_ctx ctx;
    memset(&ctx, 0, sizeof(ctx));
    ASSERT_EQ(catalog_image_readdir(img, 99, NULL, rd_cb, &ctx), 0);
    ASSERT_EQ(ctx.count, 0);

    catalog_image_destroy(img);
    PASS();
}

/* -- Replay ------------------------------------------------------------ */

static void test_replay_inode(void)
{
    fprintf(stdout, "  test_replay_inode:             ");
    fflush(stdout);

    struct catalog_image *img = NULL;
    struct mds_inode ino;
    struct mds_inode got;
    uint8_t payload[TEST_INODE_UPSERT_PAYLOAD_SIZE];
    ASSERT_EQ(catalog_image_create(&img), 0);
    memset(&ino, 0, sizeof(ino));
    ino.fileid = 42;
    ino.type = MDS_FTYPE_REG;
    ino.mode = 0640;
    ino.nlink = 2;
    ino.uid = 1001;
    ino.gid = 1002;
    ino.size = 4096;
    ino.space_used = 4096;
    ino.atime.tv_sec = 10;
    ino.atime.tv_nsec = 11;
    ino.mtime.tv_sec = 12;
    ino.mtime.tv_nsec = 13;
    ino.ctime.tv_sec = 14;
    ino.ctime.tv_nsec = 15;
    ino.change = 16;
    ino.generation = 17;
    ino.flags = MDS_IFLAG_DS_PENDING;
    ino.create_verf = 18;
    ino.parent_fileid = MDS_FILEID_ROOT;
    ino.stripe_count = 1;
    ino.stripe_unit = 65536;
    ino.mirror_count = 1;
    ASSERT_EQ(encode_test_inode_payload(&ino, payload, sizeof(payload)),
              TEST_INODE_UPSERT_PAYLOAD_SIZE);

    struct catalog_delta_record rec = {
        .type = CAT_DELTA_INODE_UPSERT,
        .stream_id = 0,
        .seqno = 1,
        .timestamp_ns = 100,
        .payload_len = TEST_INODE_UPSERT_PAYLOAD_SIZE,
        .payload = payload,
    };
    ASSERT_EQ(catalog_image_apply(img, &rec), 0);
    ASSERT_EQ(catalog_image_inode_get(img, 42, &got), 0);
    ASSERT_EQ_U64(got.fileid, 42);
    ASSERT_EQ((int)got.type, MDS_FTYPE_REG);
    ASSERT_EQ(got.mode, 0640);
    ASSERT_EQ(got.nlink, 2);
    ASSERT_EQ_U64(got.uid, 1001);
    ASSERT_EQ_U64(got.gid, 1002);
    ASSERT_EQ_U64(got.size, 4096);
    ASSERT_EQ_U64(got.space_used, 4096);
    ASSERT_EQ_U64(got.change, 16);
    ASSERT_EQ_U64(got.parent_fileid, MDS_FILEID_ROOT);
    ASSERT_EQ(got.flags, MDS_IFLAG_DS_PENDING);
    ASSERT_EQ(got.stripe_count, 1);
    ASSERT_EQ(got.stripe_unit, 65536);
    ASSERT_EQ(got.mirror_count, 1);

    /* Delete. */
    rec.type = CAT_DELTA_INODE_DELETE;
    rec.seqno = 2;
    rec.payload_len = 8;
    ASSERT_EQ(catalog_image_apply(img, &rec), 0);
    ASSERT_EQ(catalog_image_inode_get(img, 42, &got), 1);

    catalog_image_destroy(img);
    PASS();
}

static void test_replay_high_water(void)
{
    fprintf(stdout, "  test_replay_high_water:        ");
    fflush(stdout);

    struct catalog_image *img = NULL;
    ASSERT_EQ(catalog_image_create(&img), 0);

    ASSERT_EQ_U64(catalog_image_applied_high_water(img, 0), 0);
    ASSERT_TRUE(!catalog_image_is_complete(img, 0, 3));
    struct mds_inode ino;
    uint8_t payload[TEST_INODE_UPSERT_PAYLOAD_SIZE];
    memset(&ino, 0, sizeof(ino));
    ino.fileid = 100;
    ino.type = MDS_FTYPE_REG;
    ino.nlink = 1;
    ino.parent_fileid = MDS_FILEID_ROOT;
    ASSERT_EQ(encode_test_inode_payload(&ino, payload, sizeof(payload)),
              TEST_INODE_UPSERT_PAYLOAD_SIZE);

    /* Apply consecutive seqnos 1, 2, 3. */
    for (uint64_t s = 1; s <= 3; s++) {
        struct catalog_delta_record rec = {
            .type = CAT_DELTA_INODE_UPSERT,
            .stream_id = 0,
            .seqno = s,
            .timestamp_ns = 100,
            .payload_len = TEST_INODE_UPSERT_PAYLOAD_SIZE,
            .payload = payload,
        };
        ASSERT_EQ(catalog_image_apply(img, &rec), 0);
    }
    ASSERT_EQ_U64(catalog_image_applied_high_water(img, 0), 3);
    ASSERT_TRUE(catalog_image_is_complete(img, 0, 3));
    ASSERT_TRUE(!catalog_image_is_complete(img, 0, 4));

    catalog_image_destroy(img);
    PASS();
}

static void test_replay_gap_detection(void)
{
    fprintf(stdout, "  test_replay_gap_detection:     ");
    fflush(stdout);

    struct catalog_image *img = NULL;
    ASSERT_EQ(catalog_image_create(&img), 0);
    struct mds_inode ino;
    uint8_t payload[TEST_INODE_UPSERT_PAYLOAD_SIZE];
    memset(&ino, 0, sizeof(ino));
    ino.fileid = 200;
    ino.type = MDS_FTYPE_REG;
    ino.nlink = 1;
    ASSERT_EQ(encode_test_inode_payload(&ino, payload, sizeof(payload)),
              TEST_INODE_UPSERT_PAYLOAD_SIZE);

    /* Apply seqno 1, then skip 2, apply 3. */
    struct catalog_delta_record rec = {
        .type = CAT_DELTA_INODE_UPSERT,
        .stream_id = 0,
        .seqno = 1,
        .timestamp_ns = 100,
        .payload_len = TEST_INODE_UPSERT_PAYLOAD_SIZE,
        .payload = payload,
    };
    ASSERT_EQ(catalog_image_apply(img, &rec), 0);
    ASSERT_TRUE(catalog_image_is_complete(img, 0, 1));

    rec.seqno = 3; /* skip seqno 2 */
    ASSERT_EQ(catalog_image_apply(img, &rec), 0);
    ASSERT_EQ_U64(catalog_image_applied_high_water(img, 0), 3);
    /* Gap detected — not complete even though hw >= 3. */
    ASSERT_TRUE(!catalog_image_is_complete(img, 0, 3));

    catalog_image_destroy(img);
    PASS();
}

static void test_replay_dirent_via_delta(void)
{
    fprintf(stdout, "  test_replay_dirent_via_delta:  ");
    fflush(stdout);

    struct catalog_image *img = NULL;
    ASSERT_EQ(catalog_image_create(&img), 0);

    /* Uniform dirent payload: parent(8)+child(8)+type(1)+name. */
    uint8_t payload[64];
    fdb_put_u64(payload, 2);       /* parent */
    fdb_put_u64(payload + 8, 42);  /* child fileid */
    payload[16] = MDS_FTYPE_REG;   /* type */
    memcpy(payload + 17, "test.txt", 8);

    struct catalog_delta_record rec = {
        .type = CAT_DELTA_DIRENT_PUT,
        .stream_id = 0,
        .seqno = 1,
        .timestamp_ns = 100,
        .payload_len = 25,  /* 17 + 8 */
        .payload = payload,
    };
    ASSERT_EQ(catalog_image_apply(img, &rec), 0);

    uint64_t fid = 0;
    uint8_t type = 0;
    ASSERT_EQ(catalog_image_dirent_get(img, 2, "test.txt", &fid, &type), 0);
    ASSERT_EQ_U64(fid, 42);
    ASSERT_EQ(type, MDS_FTYPE_REG);

    /* Delete via delta. */
    rec.type = CAT_DELTA_DIRENT_DELETE;
    rec.seqno = 2;
    ASSERT_EQ(catalog_image_apply(img, &rec), 0);
    ASSERT_EQ(catalog_image_dirent_get(img, 2, "test.txt", &fid, &type), 1);

    catalog_image_destroy(img);
    PASS();
}

/* -- main -------------------------------------------------------------- */

int main(void)
{
    fprintf(stdout, "test_catalog_image:\n");

    test_inode_put_get_del();
    test_inode_overwrite();
    test_dirent_put_get_del();
    test_readdir_sorted();
    test_readdir_start_after();
    test_readdir_empty_dir();
    test_replay_inode();
    test_replay_high_water();
    test_replay_gap_detection();
    test_replay_initial_nonzero_seq_marks_gap();
    test_replay_dirent_via_delta();

    fprintf(stdout, "\n  %d passed, %d failed\n", passed, failed);
    return failed > 0 ? 1 : 0;
}
