/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_catalog_delta.c -- Unit tests for catalog delta builder and
 * serialization/deserialization.
 *
 * RonDB-native: no raw catalogue access.  Tests the backend-neutral
 * delta builder API (create, append, serialize, deserialize, reset).
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <time.h>

#include "pnfs_mds.h"
#include "mds_catalogue.h"
#include "catalog_delta.h"

/* -------------------------------------------------------------------
 * Test helpers
 * ------------------------------------------------------------------- */

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

#define ASSERT_NE(a, b) do { \
    long long _a = (long long)(a), _b = (long long)(b); \
    if (_a == _b) { \
        fprintf(stderr, "FAIL %s:%d: %s == %lld, expected != %lld\n", \
                __FILE__, __LINE__, #a, _a, _b); \
        fail_count++; return; \
    } pass_count++; \
} while (0)

#define ASSERT_TRUE(x)  ASSERT_NE((x), 0)

/* -------------------------------------------------------------------
 * Test 1: Builder create/destroy lifecycle
 * ------------------------------------------------------------------- */

static void test_builder_create_destroy(void)
{
    struct catalog_delta_builder *b = NULL;

    ASSERT_EQ(catalog_delta_builder_create(&b), 0);
    ASSERT_NE((long long)(uintptr_t)b, 0);
    ASSERT_EQ(catalog_delta_builder_count(b), (uint32_t)0);

    catalog_delta_builder_destroy(b);
    catalog_delta_builder_destroy(NULL);

    printf("  PASS: test_builder_create_destroy\n");
}

/* -------------------------------------------------------------------
 * Test 2: Builder append + serialize + deserialize round-trip
 * ------------------------------------------------------------------- */

struct roundtrip_ctx {
    uint32_t count;
    uint8_t  first_type;
    uint64_t first_seqno;
};

static int roundtrip_cb(const struct catalog_delta_record *rec, void *arg)
{
    struct roundtrip_ctx *ctx = arg;

    if (ctx->count == 0) {
        ctx->first_type = rec->type;
        ctx->first_seqno = rec->seqno;
    }
    ctx->count++;
    return 0;
}

static void test_builder_roundtrip(void)
{
    struct catalog_delta_builder *b = NULL;
    struct catalog_delta_record rec;
    const void *buf = NULL;
    size_t len = 0;

    ASSERT_EQ(catalog_delta_builder_create(&b), 0);

    memset(&rec, 0, sizeof(rec));
    rec.type = CAT_DELTA_INODE_UPSERT;
    rec.stream_id = 0;
    rec.seqno = 1;
    rec.timestamp_ns = 1000000;
    uint64_t payload1 = 0xDEADBEEF;
    rec.payload = &payload1;
    rec.payload_len = sizeof(payload1);
    ASSERT_EQ(catalog_delta_builder_append(b, &rec), 0);

    rec.type = CAT_DELTA_DIRENT_PUT;
    rec.seqno = 2;
    rec.timestamp_ns = 2000000;
    uint64_t payload2 = 0xCAFEBABE;
    rec.payload = &payload2;
    rec.payload_len = sizeof(payload2);
    ASSERT_EQ(catalog_delta_builder_append(b, &rec), 0);

    ASSERT_EQ(catalog_delta_builder_count(b), (uint32_t)2);

    ASSERT_EQ(catalog_delta_builder_serialize(b, &buf, &len), 0);
    ASSERT_TRUE(buf != NULL);
    ASSERT_TRUE(len > 0);

    struct roundtrip_ctx ctx;
    memset(&ctx, 0, sizeof(ctx));
    ASSERT_EQ(catalog_delta_deserialize(buf, len, roundtrip_cb, &ctx), 0);
    ASSERT_EQ(ctx.count, (uint32_t)2);
    ASSERT_EQ(ctx.first_type, (uint8_t)CAT_DELTA_INODE_UPSERT);
    ASSERT_EQ(ctx.first_seqno, (uint64_t)1);

    catalog_delta_builder_destroy(b);
    printf("  PASS: test_builder_roundtrip\n");
}

/* -------------------------------------------------------------------
 * Test 3: Builder reset clears accumulated records
 * ------------------------------------------------------------------- */

static void test_builder_reset(void)
{
    struct catalog_delta_builder *b = NULL;
    struct catalog_delta_record rec;

    ASSERT_EQ(catalog_delta_builder_create(&b), 0);

    memset(&rec, 0, sizeof(rec));
    rec.type = CAT_DELTA_INODE_UPSERT;
    rec.seqno = 42;
    ASSERT_EQ(catalog_delta_builder_append(b, &rec), 0);
    ASSERT_EQ(catalog_delta_builder_count(b), (uint32_t)1);

    catalog_delta_builder_reset(b);
    ASSERT_EQ(catalog_delta_builder_count(b), (uint32_t)0);

    catalog_delta_builder_destroy(b);
    printf("  PASS: test_builder_reset\n");
}

/* -------------------------------------------------------------------
 * Test 4: Deserialize rejects truncated data
 * ------------------------------------------------------------------- */

static void test_deserialize_truncated_rejected(void)
{
    struct roundtrip_ctx ctx;

    uint8_t tiny[4] = {0};
    memset(&ctx, 0, sizeof(ctx));
    int rc = catalog_delta_deserialize(tiny, sizeof(tiny),
                                       roundtrip_cb, &ctx);
    ASSERT_NE(rc, 0);

    printf("  PASS: test_deserialize_truncated_rejected\n");
}

/* -------------------------------------------------------------------
 * Test 5: All delta type enum values are distinct
 * ------------------------------------------------------------------- */

static void test_delta_type_values_distinct(void)
{
    ASSERT_NE(CAT_DELTA_INODE_UPSERT, CAT_DELTA_DIRENT_PUT);
    ASSERT_NE(CAT_DELTA_STRIPE_MAP_PUT, CAT_DELTA_XATTR_PUT);
    ASSERT_NE(CAT_DELTA_LAYOUT_GRANT, CAT_DELTA_LAYOUT_RETURN);
    ASSERT_NE(CAT_DELTA_GC_ENQUEUE, CAT_DELTA_GC_DEQUEUE);
    ASSERT_EQ(CATALOG_DELTA_HDR_SIZE, 25);

    printf("  PASS: test_delta_type_values_distinct\n");
}

/* -------------------------------------------------------------------
 * Main
 * ------------------------------------------------------------------- */

int main(void)
{
    printf("test_catalog_delta (RonDB-native)\n");

    test_builder_create_destroy();
    test_builder_roundtrip();
    test_builder_reset();
    test_deserialize_truncated_rejected();
    test_delta_type_values_distinct();

    printf("\n%d passed, %d failed\n", pass_count, fail_count);
    return fail_count > 0 ? 1 : 0;
}
