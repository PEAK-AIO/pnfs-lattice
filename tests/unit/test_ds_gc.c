/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_ds_gc.c -- Unit tests for the parallel DS GC drainer.
 *
 * The drainer is exercised against the in-memory memdb catalogue
 * (no RonDB dependency; the dispatcher's single-peek fallback fills
 * in for the missing gc_peek_batch slot).  A real mds_proxy_ctx is
 * created with a temp directory as a fake DS mount so that
 * mds_proxy_unlink_ds_file() returns MDS_OK + existed=false on every
 * absent path, which lets process_one_entry() exit on the first
 * stripe row's first mirror with had_any_existed=false -- i.e. the
 * fast path that keeps the test deterministic without simulating the
 * (s, m) layout.
 */

#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "pnfs_mds.h"
#include "test_helpers.h"
#include "mds_catalogue.h"
#include "ds_gc.h"
#include "proxy_io.h"

/* -----------------------------------------------------------------------
 * Minimal test framework (mirrors test_proxy_io.c).
 * ----------------------------------------------------------------------- */

static int tests_run    = 0;
static int tests_passed = 0;

#define RUN_TEST(fn) do {                                              \
    tests_run++;                                                        \
    fprintf(stdout, "  %-40s ", #fn);                                   \
    fn();                                                               \
    tests_passed++;                                                     \
    fprintf(stdout, "PASS\n");                                          \
} while (0)

#define ASSERT_EQ(a, b) do {                                           \
    long long _av = (long long)(a);                                     \
    long long _bv = (long long)(b);                                     \
    if (_av != _bv) {                                                   \
        fprintf(stderr, "FAIL at %s:%d: %s (=%lld) != %s (=%lld)\n",    \
                __FILE__, __LINE__, #a, _av, #b, _bv);                  \
        exit(1);                                                        \
    }                                                                   \
} while (0)

#define ASSERT_TRUE(cond) do {                                         \
    if (!(cond)) {                                                      \
        fprintf(stderr, "FAIL at %s:%d: !(%s)\n",                       \
                __FILE__, __LINE__, #cond);                             \
        exit(1);                                                        \
    }                                                                   \
} while (0)

/* -----------------------------------------------------------------------
 * Fixture helpers
 * ----------------------------------------------------------------------- */

static char *make_ds_dir(void)
{
    char *tpl = strdup("/tmp/test_ds_gc_XXXXXX");
    ASSERT_TRUE(tpl != NULL);
    ASSERT_TRUE(mkdtemp(tpl) != NULL);
    return tpl;
}

static void rm_ds_dir(char *path)
{
    char cmd[4200];
    (void)snprintf(cmd, sizeof(cmd), "rm -rf '%s'", path);
    (void)system(cmd);
    free(path);
}

/** Push @count entries into the catalogue's GC queue. */
static void enqueue_n(struct mds_catalogue *cat, uint32_t ds_id, uint32_t count)
{
    for (uint32_t i = 0; i < count; i++) {
        uint8_t fh[4] = { 'g', 'c', 0, 0 };
        ASSERT_EQ(mds_cat_gc_enqueue(cat, NULL,
                                     /* fileid */ (uint64_t)(1000U + i),
                                     ds_id, fh, sizeof(fh)),
                  MDS_OK);
    }
}

/**
 * Spin until mds_cat_gc_count drops to @target_count or the timeout
 * (in milliseconds) elapses.  Returns the last observed count so the
 * caller can ASSERT_EQ on it.
 */
static uint32_t wait_for_count(struct mds_catalogue *cat,
                               uint32_t target_count,
                               uint32_t timeout_ms)
{
    const uint32_t step_ms = 10;
    uint32_t elapsed = 0;
    uint32_t count = UINT32_MAX;

    while (elapsed < timeout_ms) {
        if (mds_cat_gc_count(cat, &count) == MDS_OK &&
            count <= target_count) {
            return count;
        }
        usleep(step_ms * 1000U);
        elapsed += step_ms;
    }
    (void)mds_cat_gc_count(cat, &count);
    return count;
}

/* -----------------------------------------------------------------------
 * Tests
 * ----------------------------------------------------------------------- */

/**
 * Empty queue: start, idle for a tick, stop cleanly.  The primary
 * assertion is that ds_gc_stop() returns within the test's lifetime
 * (no deadlock between coordinator and workers).
 */
static void test_empty_queue_clean_stop(void)
{
    struct mds_catalogue *cat = open_test_catalogue();
    struct mds_proxy_ctx *proxy = NULL;
    struct ds_gc *gc = NULL;
    char *ds_dir = make_ds_dir();
    uint32_t count_before = 0;
    uint32_t count_after = 0;

    ASSERT_EQ(mds_proxy_ctx_create(&proxy), MDS_OK);
    ASSERT_EQ(mds_proxy_mount_set(proxy, /*ds_id*/ 1, ds_dir), MDS_OK);
    ASSERT_EQ(mds_cat_gc_count(cat, &count_before), MDS_OK);
    ASSERT_EQ(count_before, 0);

    ASSERT_EQ(ds_gc_start_ex(cat, proxy,
                             /*poll_ms*/ 100U,
                             /*workers*/ 4U,
                             /*batch_size*/ 64U,
                             &gc), 0);
    ASSERT_TRUE(gc != NULL);

    /* Let the coordinator fire at least one peek tick. */
    usleep(150 * 1000U);

    ds_gc_stop(gc);
    ASSERT_EQ(mds_cat_gc_count(cat, &count_after), MDS_OK);
    ASSERT_EQ(count_after, 0);

    mds_proxy_ctx_destroy(proxy);
    mds_catalogue_close(cat);
    rm_ds_dir(ds_dir);
}

/**
 * Backlog drain: 200 entries with workers=4, batch=64.  Verifies
 * that the parallel drainer empties the queue within the timeout
 * and stops cleanly.  The backlog is sized below the memdb backend's
 * MEMDB_MAX_GC=256 cap so this test does not depend on backend tuning.
 */
static void test_backlog_drains_with_workers(void)
{
    struct mds_catalogue *cat = open_test_catalogue();
    struct mds_proxy_ctx *proxy = NULL;
    struct ds_gc *gc = NULL;
    char *ds_dir = make_ds_dir();
    uint32_t observed = 0;

    ASSERT_EQ(mds_proxy_ctx_create(&proxy), MDS_OK);
    ASSERT_EQ(mds_proxy_mount_set(proxy, /*ds_id*/ 1, ds_dir), MDS_OK);
    enqueue_n(cat, /*ds_id*/ 1, 200U);
    ASSERT_EQ(mds_cat_gc_count(cat, &observed), MDS_OK);
    ASSERT_EQ(observed, 200U);

    ASSERT_EQ(ds_gc_start_ex(cat, proxy,
                             /*poll_ms*/ 100U,
                             /*workers*/ 4U,
                             /*batch_size*/ 64U,
                             &gc), 0);
    ASSERT_TRUE(gc != NULL);

    /* 60-second budget is generous on every CI worker class. */
    observed = wait_for_count(cat, /*target*/ 0U, /*timeout_ms*/ 60000U);
    ASSERT_EQ(observed, 0U);

    ds_gc_stop(gc);
    mds_proxy_ctx_destroy(proxy);
    mds_catalogue_close(cat);
    rm_ds_dir(ds_dir);
}

/**
 * Parity: workers=1 reproduces today's serial drain rate using the
 * batched-peek path.  Smaller backlog so the test stays fast.
 */
static void test_workers_one_parity(void)
{
    struct mds_catalogue *cat = open_test_catalogue();
    struct mds_proxy_ctx *proxy = NULL;
    struct ds_gc *gc = NULL;
    char *ds_dir = make_ds_dir();
    uint32_t observed = 0;

    ASSERT_EQ(mds_proxy_ctx_create(&proxy), MDS_OK);
    ASSERT_EQ(mds_proxy_mount_set(proxy, /*ds_id*/ 1, ds_dir), MDS_OK);
    enqueue_n(cat, /*ds_id*/ 1, 200U);

    ASSERT_EQ(ds_gc_start_ex(cat, proxy,
                             /*poll_ms*/ 100U,
                             /*workers*/ 1U,
                             /*batch_size*/ 64U,
                             &gc), 0);
    ASSERT_TRUE(gc != NULL);

    observed = wait_for_count(cat, /*target*/ 0U, /*timeout_ms*/ 60000U);
    ASSERT_EQ(observed, 0U);

    ds_gc_stop(gc);
    mds_proxy_ctx_destroy(proxy);
    mds_catalogue_close(cat);
    rm_ds_dir(ds_dir);
}

/**
 * Legacy entry point still works.  ds_gc_start() is the
 * three-argument shim used by callers that haven't been migrated.
 */
static void test_legacy_start_wrapper(void)
{
    struct mds_catalogue *cat = open_test_catalogue();
    struct mds_proxy_ctx *proxy = NULL;
    struct ds_gc *gc = NULL;
    char *ds_dir = make_ds_dir();
    uint32_t observed = 0;

    ASSERT_EQ(mds_proxy_ctx_create(&proxy), MDS_OK);
    ASSERT_EQ(mds_proxy_mount_set(proxy, /*ds_id*/ 1, ds_dir), MDS_OK);
    enqueue_n(cat, /*ds_id*/ 1, 64U);

    ASSERT_EQ(ds_gc_start(cat, proxy, /*poll_ms*/ 100U, &gc), 0);
    ASSERT_TRUE(gc != NULL);

    observed = wait_for_count(cat, /*target*/ 0U, /*timeout_ms*/ 60000U);
    ASSERT_EQ(observed, 0U);

    ds_gc_stop(gc);
    mds_proxy_ctx_destroy(proxy);
    mds_catalogue_close(cat);
    rm_ds_dir(ds_dir);
}

/**
 * Defensive paths: NULL out is rejected; poll_ms=0 disables the
 * drainer (returns 0 with *out == NULL).
 */
static void test_disabled_inputs(void)
{
    struct mds_catalogue *cat = open_test_catalogue();
    struct mds_proxy_ctx *proxy = NULL;
    struct ds_gc *gc = (struct ds_gc *)(void *)0xDEADBEEF;
    char *ds_dir = make_ds_dir();

    ASSERT_EQ(mds_proxy_ctx_create(&proxy), MDS_OK);
    ASSERT_EQ(mds_proxy_mount_set(proxy, /*ds_id*/ 1, ds_dir), MDS_OK);

    /* NULL out -> -1, no allocation. */
    ASSERT_EQ(ds_gc_start_ex(cat, proxy, 100U, 4U, 64U, NULL), -1);

    /* poll_ms=0 -> disabled, *out=NULL, no thread. */
    gc = (struct ds_gc *)(void *)0xDEADBEEF;
    ASSERT_EQ(ds_gc_start_ex(cat, proxy, 0U, 4U, 64U, &gc), 0);
    ASSERT_TRUE(gc == NULL);

    /* NULL cat -> disabled. */
    gc = (struct ds_gc *)(void *)0xDEADBEEF;
    ASSERT_EQ(ds_gc_start_ex(NULL, proxy, 100U, 4U, 64U, &gc), 0);
    ASSERT_TRUE(gc == NULL);

    mds_proxy_ctx_destroy(proxy);
    mds_catalogue_close(cat);
    rm_ds_dir(ds_dir);
}

/* -----------------------------------------------------------------------
 * Entry point
 * ----------------------------------------------------------------------- */

int main(void)
{
    fprintf(stdout, "ds_gc parallel drainer tests\n");

    RUN_TEST(test_empty_queue_clean_stop);
    RUN_TEST(test_disabled_inputs);
    RUN_TEST(test_legacy_start_wrapper);
    RUN_TEST(test_workers_one_parity);
    RUN_TEST(test_backlog_drains_with_workers);

    fprintf(stdout, "%d/%d tests passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
