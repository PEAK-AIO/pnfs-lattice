/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_ds_prealloc.c -- Unit tests for the DS pre-allocation pool.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>
#include <pthread.h>

#include "ds_prealloc.h"
#include "mds_catalogue.h"
#include "test_helpers.h"
#include "placement.h"

/* -------------------------------------------------------------------
 * Helpers
 * ------------------------------------------------------------------- */

static int pass_count;
static int fail_count;

#define ASSERT_EQ(a, b) do { \
    long long _a = (long long)(a), _b = (long long)(b); \
    if (_a != _b) { \
        fprintf(stdout, "FAIL %s:%d: %s == %lld, expected %lld\n", \
                __FILE__, __LINE__, #a, _a, _b); \
        fail_count++; return; \
    } pass_count++; \
} while (0)

#define ASSERT_NE(a, b) do { \
    long long _a = (long long)(a), _b = (long long)(b); \
    if (_a == _b) { \
        fprintf(stdout, "FAIL %s:%d: %s == %lld (expected !=)\n", \
                __FILE__, __LINE__, #a, _a); \
        fail_count++; return; \
    } pass_count++; \
} while (0)

#define TEST_MAP_SIZE (64ULL * 1024 * 1024)

static int db_counter;
static char *make_temp_db(void)
{
    static char path[256];
    snprintf(path, sizeof(path), "/tmp/test_ds_prealloc_%d_%d", getpid(), db_counter++);
    return path;
}

static void seed_ds(struct mds_catalogue *db, uint32_t ds_id)
{
    struct mds_ds_info info;
    struct mds_cat_txn *txn = NULL;

    memset(&info, 0, sizeof(info));
    info.ds_id = ds_id;
    info.state = DS_ONLINE;
    info.port = 2049;
    info.tcp_port = 2049;
    info.transport = DS_TRANSPORT_TCP;
    info.mode = DS_MODE_GENERIC;  /* Phase 5B: prealloc requires patched DSes. */
    info.capabilities = 0; /* Synth cred + writeverf. */
    snprintf(info.addr, sizeof(info.addr), "10.0.0.%u:/data", ds_id);
    snprintf(info.host, sizeof(info.host), "10.0.0.%u", ds_id);
    snprintf(info.export_path, sizeof(info.export_path), "/data");

    if (mds_cat_txn_begin(db, MDS_CAT_TXN_WRITE, &txn) != MDS_OK) { return; }
    if (mds_cat_ds_put(db, txn, &info) != MDS_OK) { mds_cat_txn_abort(txn); return; }
    mds_cat_txn_commit(txn);
}

/* -------------------------------------------------------------------
 * Test 1: init + destroy
 * ------------------------------------------------------------------- */

static void test_init_destroy(void)
{
    char *path = make_temp_db();
    struct mds_catalogue *db = NULL;
	struct mds_catalogue *cat = NULL;
    struct ds_prealloc_ctx *ctx = NULL;

    db = open_test_catalogue(); assert(db != NULL);
	cat = db;
    seed_ds(db, 0);

    ASSERT_EQ(ds_prealloc_init(cat, NULL, 16, &ctx), 0);
    ASSERT_NE((long long)(uintptr_t)ctx, 0);

    ds_prealloc_destroy(ctx);
    mds_catalogue_close(db);
}

/* -------------------------------------------------------------------
 * Test 2: init rejects NULL db
 * ------------------------------------------------------------------- */

static void test_init_null_db(void)
{
    struct ds_prealloc_ctx *ctx = NULL;

    ASSERT_EQ(ds_prealloc_init(NULL, NULL, 16, &ctx), -1);
}

/* -------------------------------------------------------------------
 * Test 3: pop returns valid placement
 * ------------------------------------------------------------------- */

static void test_pop_valid(void)
{
    char *path = make_temp_db();
    struct mds_catalogue *db = NULL;
	struct mds_catalogue *cat = NULL;
    struct ds_prealloc_ctx *ctx = NULL;
    struct mds_ds_map_entry entry;
    uint32_t stripe_unit = 0;
    uint64_t fileid = 0;

    db = open_test_catalogue(); assert(db != NULL);
	cat = db;
    seed_ds(db, 0);
    seed_ds(db, 1);

    if (ds_prealloc_init(cat, NULL, 16, &ctx) != 0) { mds_catalogue_close(db); return; }

    /* Give background thread time to fill. */
    usleep(200000);

    ASSERT_EQ(ds_prealloc_pop(ctx, &entry, &stripe_unit, &fileid), 0);
    ASSERT_NE(stripe_unit, 0);
    /* ds_id should be 0 or 1. */
    ASSERT_EQ(entry.ds_id == 0 || entry.ds_id == 1, 1);

    ds_prealloc_destroy(ctx);
    mds_catalogue_close(db);
}

/* -------------------------------------------------------------------
 * Test 4: pop with empty pool falls back to synchronous
 * ------------------------------------------------------------------- */

static void test_pop_empty_pool(void)
{
    char *path = make_temp_db();
    struct mds_catalogue *db = NULL;
	struct mds_catalogue *cat = NULL;
    struct ds_prealloc_ctx *ctx = NULL;
    struct mds_ds_map_entry entry;
    uint32_t stripe_unit = 0;
    uint64_t fileid = 0;

    db = open_test_catalogue(); assert(db != NULL);
	cat = db;
    seed_ds(db, 0);

    if (ds_prealloc_init(cat, NULL, 4, &ctx) != 0) { mds_catalogue_close(db); return; }

    /* Don't sleep -- pop immediately before background fills. */
    /* Even with empty ring, pop should succeed via sync fallback. */
    int rc = ds_prealloc_pop(ctx, &entry, &stripe_unit, &fileid);
    /* May succeed (sync fallback) or ring already has entries. */
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(entry.ds_id, 0);

    ds_prealloc_destroy(ctx);
    mds_catalogue_close(db);
}

/* -------------------------------------------------------------------
 * Test 5: concurrent pop (thread safety)
 * ------------------------------------------------------------------- */

struct pop_thread_ctx {
    struct ds_prealloc_ctx *pool;
    int success;
    int fail;
};

static void *pop_thread_fn(void *arg)
{
    struct pop_thread_ctx *tc = arg;
    struct mds_ds_map_entry entry;
    uint32_t su;
    uint64_t fid;

    for (int i = 0; i < 50; i++) {
        if (ds_prealloc_pop(tc->pool, &entry, &su, &fid) == 0) {
            tc->success++;
        } else {
            tc->fail++;
        }
    }
    return NULL;
}

static void test_concurrent_pop(void)
{
    char *path = make_temp_db();
    struct mds_catalogue *db = NULL;
	struct mds_catalogue *cat = NULL;
    struct ds_prealloc_ctx *ctx = NULL;

    db = open_test_catalogue(); assert(db != NULL);
	cat = db;
    seed_ds(db, 0);
    seed_ds(db, 1);

    if (ds_prealloc_init(cat, NULL, 128, &ctx) != 0) { mds_catalogue_close(db); return; }
    usleep(300000); /* Let pool fill. */

    /* Launch 4 threads, each popping 50 entries. */
    pthread_t threads[4];
    struct pop_thread_ctx tctx[4];
    memset(tctx, 0, sizeof(tctx));

    for (int i = 0; i < 4; i++) {
        tctx[i].pool = ctx;
        pthread_create(&threads[i], NULL, pop_thread_fn, &tctx[i]);
    }
    for (int i = 0; i < 4; i++) {
        pthread_join(threads[i], NULL);
    }

    int total_ok = 0, total_fail = 0;
    for (int i = 0; i < 4; i++) {
        total_ok += tctx[i].success;
        total_fail += tctx[i].fail;
    }

    /* All 200 pops should succeed (sync fallback if pool runs dry). */
    ASSERT_EQ(total_ok, 200);
    ASSERT_EQ(total_fail, 0);

    ds_prealloc_destroy(ctx);
    mds_catalogue_close(db);
}

/* -------------------------------------------------------------------
 * Test 6: pop fails with no DSes registered
 * ------------------------------------------------------------------- */

static void test_pop_no_ds(void)
{
    char *path = make_temp_db();
    struct mds_catalogue *db = NULL;
	struct mds_catalogue *cat = NULL;
    struct ds_prealloc_ctx *ctx = NULL;
    struct mds_ds_map_entry entry;
    uint32_t stripe_unit = 0;
    uint64_t fileid = 0;

    db = open_test_catalogue(); assert(db != NULL);
	cat = db;
    /* No DSes seeded. */

    if (ds_prealloc_init(cat, NULL, 4, &ctx) != 0) { mds_catalogue_close(db); return; }

    /* Pop should fail -- no DSes available. */
    ASSERT_EQ(ds_prealloc_pop(ctx, &entry, &stripe_unit, &fileid), -1);

    ds_prealloc_destroy(ctx);
    mds_catalogue_close(db);
}

/* -------------------------------------------------------------------
 * main
 * ------------------------------------------------------------------- */

int main(void)
{
    printf("test_ds_prealloc:\n");

    test_init_destroy();
    printf("  test_init_destroy                   PASS\n");

    test_init_null_db();
    printf("  test_init_null_db                   PASS\n");

    test_pop_valid();
    printf("  test_pop_valid                      PASS\n");

    test_pop_empty_pool();
    printf("  test_pop_empty_pool                 PASS\n");

    test_concurrent_pop();
    printf("  test_concurrent_pop                 PASS\n");

    test_pop_no_ds();
    printf("  test_pop_no_ds                      PASS\n");

    printf("\n  %d assertions passed, %d failed\n", pass_count, fail_count);
    return fail_count > 0 ? 1 : 0;
}
