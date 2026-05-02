/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_mds_shard.c — Unit tests for shard map routing.
 *
 * RonDB-native: no catalogue handles.  Tests the shard map API
 * (create, add, lookup, remove, for_each, format_local_path).
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "pnfs_mds.h"
#include "mds_shard.h"

static int tests_run, tests_passed;

#define ASSERT_EQ(a, b) do { \
    if ((a) != (b)) { \
        fprintf(stderr, "  FAIL %s:%d: %s (%d) != %s (%d)\n", \
                __FILE__, __LINE__, #a, (int)(a), #b, (int)(b)); \
        return; \
    } \
} while (0)

#define ASSERT_NE(a, b) do { \
    if ((a) == (b)) { \
        fprintf(stderr, "  FAIL %s:%d: %s == %s\n", \
                __FILE__, __LINE__, #a, #b); \
        return; \
    } \
} while (0)

#define ASSERT_TRUE(x) ASSERT_NE((x), 0)

#define RUN_TEST(fn) do { \
    tests_run++; \
    fprintf(stdout, "  %-50s", #fn); fflush(stdout); \
    fn(); tests_passed++; \
    fprintf(stdout, "PASS\n"); \
} while (0)

static void test_shard_map_create_destroy(void)
{
    struct mds_shard_map *map = NULL;
    ASSERT_EQ(mds_shard_map_create(&map), 0);
    ASSERT_NE(map, NULL);
    ASSERT_EQ(mds_shard_map_count(map), (uint32_t)0);
    mds_shard_map_destroy(map);
    mds_shard_map_destroy(NULL);  /* NULL-safe */
}

static void test_shard_map_add_lookup(void)
{
    struct mds_shard_map *map = NULL;
    ASSERT_EQ(mds_shard_map_create(&map), 0);
    ASSERT_EQ(mds_shard_map_add(map, "/", NULL), 0);
    ASSERT_EQ(mds_shard_map_count(map), (uint32_t)1);

    const struct mds_shard *s = mds_shard_map_lookup(map, "/data/foo");
    ASSERT_NE(s, NULL);

    mds_shard_map_destroy(map);
}

static void test_shard_map_longest_prefix(void)
{
    struct mds_shard_map *map = NULL;
    ASSERT_EQ(mds_shard_map_create(&map), 0);
    ASSERT_EQ(mds_shard_map_add(map, "/", NULL), 0);
    ASSERT_EQ(mds_shard_map_add(map, "/data", NULL), 0);
    ASSERT_EQ(mds_shard_map_count(map), (uint32_t)2);

    const struct mds_shard *s1 = mds_shard_map_lookup(map, "/data/foo");
    ASSERT_NE(s1, NULL);
    ASSERT_EQ(strcmp(s1->subtree_path, "/data"), 0);

    const struct mds_shard *s2 = mds_shard_map_lookup(map, "/home/user");
    ASSERT_NE(s2, NULL);
    ASSERT_EQ(strcmp(s2->subtree_path, "/"), 0);

    mds_shard_map_destroy(map);
}

static void test_shard_map_remove(void)
{
    struct mds_shard_map *map = NULL;
    ASSERT_EQ(mds_shard_map_create(&map), 0);
    ASSERT_EQ(mds_shard_map_add(map, "/data", NULL), 0);
    ASSERT_EQ(mds_shard_map_count(map), (uint32_t)1);

    ASSERT_EQ(mds_shard_map_remove(map, "/data"), 0);
    ASSERT_EQ(mds_shard_map_count(map), (uint32_t)0);

    /* Remove non-existent — should return -1. */
    ASSERT_EQ(mds_shard_map_remove(map, "/data"), -1);

    mds_shard_map_destroy(map);
}

static void test_shard_map_get_default(void)
{
    struct mds_shard_map *map = NULL;
    ASSERT_EQ(mds_shard_map_create(&map), 0);

    /* No root shard → NULL. */
    ASSERT_EQ(mds_shard_map_get_default(map) == NULL, 1);

    ASSERT_EQ(mds_shard_map_add(map, "/", NULL), 0);
    const struct mds_shard *root = mds_shard_map_get_default(map);
    ASSERT_NE(root, NULL);
    ASSERT_EQ(strcmp(root->subtree_path, "/"), 0);

    mds_shard_map_destroy(map);
}

static void test_shard_map_null_safety(void)
{
    ASSERT_EQ(mds_shard_map_lookup(NULL, "/") == NULL, 1);
    ASSERT_EQ(mds_shard_map_get_default(NULL) == NULL, 1);
    ASSERT_EQ(mds_shard_map_count(NULL), (uint32_t)0);
    ASSERT_EQ(mds_shard_map_add(NULL, "/", NULL), -1);
}

static void test_shard_format_local_path(void)
{
    char buf[256];
    ASSERT_EQ(mds_shard_format_local_path("/shards", 1, buf, sizeof(buf)), 0);
    ASSERT_TRUE(strstr(buf, "shard-1") != NULL);

    /* Invalid inputs. */
    ASSERT_EQ(mds_shard_format_local_path(NULL, 1, buf, sizeof(buf)), -1);
    ASSERT_EQ(mds_shard_format_local_path("/shards", 0, buf, sizeof(buf)), -1);
}

struct foreach_ctx { uint32_t count; };
static int foreach_cb(const struct mds_shard *s, void *arg)
{
    (void)s;
    struct foreach_ctx *ctx = arg;
    ctx->count++;
    return 0;
}

static void test_shard_for_each(void)
{
    struct mds_shard_map *map = NULL;
    struct foreach_ctx ctx = {0};

    ASSERT_EQ(mds_shard_map_create(&map), 0);
    ASSERT_EQ(mds_shard_map_add(map, "/", NULL), 0);
    ASSERT_EQ(mds_shard_map_add(map, "/data", NULL), 0);

    mds_shard_map_for_each(map, foreach_cb, &ctx);
    ASSERT_EQ(ctx.count, (uint32_t)2);

    mds_shard_map_destroy(map);
}

int main(void)
{
    fprintf(stdout, "test_mds_shard (RonDB-native)\n");

    RUN_TEST(test_shard_map_create_destroy);
    RUN_TEST(test_shard_map_add_lookup);
    RUN_TEST(test_shard_map_longest_prefix);
    RUN_TEST(test_shard_map_remove);
    RUN_TEST(test_shard_map_get_default);
    RUN_TEST(test_shard_map_null_safety);
    RUN_TEST(test_shard_format_local_path);
    RUN_TEST(test_shard_for_each);

    fprintf(stdout, "\n  %d/%d tests passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
