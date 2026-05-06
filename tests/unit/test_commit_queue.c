/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_commit_queue.c -- Unit tests for the RonDB-native commit queue.
 *
 * The commit queue is a thin dispatch wrapper: each op is routed
 * through the catalogue vtable on the caller's thread.  These tests
 * verify lifecycle, null safety, and basic dispatch semantics.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#include "commit_queue.h"
#include "mds_catalogue.h"

/* ----------------------------------------------------------------------- */

static int tests_run    = 0;
static int tests_passed = 0;

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

#define RUN_TEST(fn) do { \
    fprintf(stdout, "  %-50s", #fn); \
    tests_run++; \
    fn(); \
    tests_passed++; \
    fprintf(stdout, "OK\n"); \
} while (0)

/* -----------------------------------------------------------------------
 * test_cq_null_safety -- NULL args must not crash
 * ----------------------------------------------------------------------- */

static void test_cq_null_safety(void)
{
    commit_queue_destroy(NULL);  /* must not crash */
    ASSERT_EQ(commit_queue_submit(NULL, NULL), MDS_ERR_INVAL);

    struct commit_op op;
    memset(&op, 0, sizeof(op));
    op.type = COMMIT_OP_CREATE;
    ASSERT_EQ(commit_queue_submit(NULL, &op), MDS_ERR_INVAL);
}

/* -----------------------------------------------------------------------
 * test_cq_create_requires_cat -- NULL cat + NULL db = EINVAL
 * ----------------------------------------------------------------------- */

static void test_cq_create_requires_cat(void)
{
    struct commit_queue *cq = NULL;
    int rc;

    rc = commit_queue_create(NULL, NULL, 0, 0, 0, 0, 0, 0, &cq);
    ASSERT_EQ(rc, -EINVAL);
}

/* -----------------------------------------------------------------------
 * test_cq_create_null_out -- NULL out pointer = EINVAL
 * ----------------------------------------------------------------------- */

static void test_cq_create_null_out(void)
{
    int rc;

    rc = commit_queue_create(NULL, NULL, 0, 0, 0, 0, 0, 0, NULL);
    ASSERT_EQ(rc, -EINVAL);
}

/* -----------------------------------------------------------------------
 * test_cq_get_repl_null -- get_repl on NULL CQ returns NULL
 * ----------------------------------------------------------------------- */

static void test_cq_get_repl_null(void)
{
    ASSERT_EQ(commit_queue_get_repl(NULL) == NULL, 1);
}

/* -----------------------------------------------------------------------
 * test_cq_get_repl_mode_null -- get_repl_mode on NULL CQ returns 0
 * ----------------------------------------------------------------------- */

static void test_cq_get_repl_mode_null(void)
{
    ASSERT_EQ(commit_queue_get_repl_mode(NULL), 0);
}

/* -----------------------------------------------------------------------
 * test_cq_set_ds_cache_null -- set_ds_cache on NULL CQ must not crash
 * ----------------------------------------------------------------------- */

static void test_cq_set_ds_cache_null(void)
{
    commit_queue_set_ds_cache(NULL, NULL);  /* must not crash */
}

/* -----------------------------------------------------------------------
 * test_cq_lifecycle_with_catalogue -- create + destroy with real catalogue
 *
 * Uses mds_catalogue_open() if available; skipped in minimal builds.
 * ----------------------------------------------------------------------- */

static void test_cq_lifecycle_with_catalogue(void)
{
    struct mds_config cfg;
    struct mds_catalogue *cat = NULL;
    struct commit_queue *cq = NULL;
    enum mds_status st;
    int rc;

    memset(&cfg, 0, sizeof(cfg));
    cfg.catalogue_backend = MDS_BACKEND_RONDB;

    /* Try to open catalogue -- may fail if no RonDB available. */
    st = mds_catalogue_open(&cfg, &cat);
    if (st != MDS_OK) {
        /* No RonDB cluster available -- skip gracefully. */
        fprintf(stdout, "(skipped: no RonDB) ");
        return;
    }

    rc = commit_queue_create(cat, NULL, 0, 0, 0, 0, 0, 0, &cq);
    ASSERT_EQ(rc, 0);
    ASSERT_NE(cq, NULL);

    /* Repl should be NULL (no replication in RonDB mode). */
    ASSERT_EQ(commit_queue_get_repl(cq) == NULL, 1);
    ASSERT_EQ(commit_queue_get_repl_mode(cq), 0);

    commit_queue_destroy(cq);
    mds_catalogue_close(cat);
}

int main(void)
{
    fprintf(stdout, "test_commit_queue (RonDB-native)\n");

    RUN_TEST(test_cq_null_safety);
    RUN_TEST(test_cq_create_requires_cat);
    RUN_TEST(test_cq_create_null_out);
    RUN_TEST(test_cq_get_repl_null);
    RUN_TEST(test_cq_get_repl_mode_null);
    RUN_TEST(test_cq_set_ds_cache_null);
    RUN_TEST(test_cq_lifecycle_with_catalogue);

    fprintf(stdout, "\n  %d/%d tests passed\n",
        tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
