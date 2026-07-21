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
#include <assert.h>
#include <errno.h>

#include "commit_queue.h"
#include "layout_types.h"
#include "mds_catalogue.h"
#include "mds_coordination.h"
#include "test_helpers.h"

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
struct layout_index_count {
    uint32_t count;
};

static int count_layout_index(uint64_t clientid, uint64_t fileid, void *ctx)
{
    struct layout_index_count *count = ctx;

    (void)clientid;
    (void)fileid;
    count->count++;
    return 0;
}

/*
 * A CREATE backend owns the prealloc pop.  The CQ must not persist the
 * caller's preview as a layout DS ID when the resulting inode has no
 * stripe map; the following LAYOUTGET must take its ordinary path.
 */
static void test_cq_create_rejects_speculative_layout_placement(void)
{
    struct mds_catalogue *cat;
    struct commit_queue *cq = NULL;
    struct commit_op op;
    struct mds_inode inode;
    struct layout_index_count index_count;
    uint32_t preview_ds_id = 99;

    cat = open_test_catalogue();
    assert(cat != NULL);
    assert(commit_queue_create(cat, NULL, 0, 0, 0, 0, 0, 0, &cq) == 0);
    assert(cq != NULL);

    memset(&op, 0, sizeof(op));
    op.type = COMMIT_OP_CREATE;
    op.args.create.parent_fileid = MDS_FILEID_ROOT;
    (void)snprintf(op.args.create.name, sizeof(op.args.create.name),
                   "%s", "no-placement");
    op.args.create.type = MDS_FTYPE_REG;
    op.args.create.mode = 0644;
    op.args.create.prealloc =
        (struct ds_prealloc_ctx *)(void *)&op;
    op.args.create.out = &inode;
    op.args.create.layout_pregrant = true;
    op.args.create.layout_clientid = 123;
    op.args.create.layout_iomode = LAYOUTIOMODE4_RW;
    op.args.create.layout_length = 65536;
    op.args.create.layout_stateid.seqid = 1;
    op.args.create.layout_ds_ids = &preview_ds_id;
    op.args.create.layout_ds_count = 1;

    assert(commit_queue_submit(cq, &op) == MDS_OK);
    assert(!op.args.create.layout_pregrant_ok);

    memset(&index_count, 0, sizeof(index_count));
    assert(mds_coord_ds_layout_idx_scan(cat, preview_ds_id,
                                        count_layout_index,
                                        &index_count) == MDS_OK);
    assert(index_count.count == 0);

    commit_queue_destroy(cq);
    mds_catalogue_close(cat);
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
    RUN_TEST(test_cq_create_rejects_speculative_layout_placement);
    RUN_TEST(test_cq_lifecycle_with_catalogue);

    fprintf(stdout, "\n  %d/%d tests passed\n",
        tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
