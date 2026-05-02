/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_ds_layout_idx.c — Tests for DS layout reverse index and
 * replicated layout operations via commit queue.
 *
 * Covers:
 *   1. ds_layout_idx put/scan/del round-trip.
 *   2. Multi-DS scan with distinct ds_ids.
 *   3. Empty scan returns MDS_OK with no callbacks.
 *   4. COMMIT_OP_LAYOUT_STATE_PUT through CQ — layout_state + index.
 *   5. COMMIT_OP_LAYOUT_STATE_DEL through CQ — layout_state + index cleanup.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>

#include "pnfs_mds.h"
#include "mds_catalogue.h"
#include "test_helpers.h"
#include "mds_coordination.h"
#include "commit_queue.h"
#include "open_state.h"

/* -----------------------------------------------------------------------
 * Test helpers
 * ----------------------------------------------------------------------- */

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

#define TEST_MAP_SIZE (64ULL * 1024 * 1024)

static char *make_temp_db_path(void)
{
    static char path[256];
    snprintf(path, sizeof(path), "/tmp/test_ds_layout_idx_%d", getpid());
    return path;
}

/* Scan callback context for collecting results. */
struct scan_result {
    uint64_t clientids[64];
    uint64_t fileids[64];
    uint32_t count;
};

static int collect_cb(uint64_t clientid, uint64_t fileid, void *ctx)
{
    struct scan_result *sr = ctx;

    if (sr->count < 64) {
        sr->clientids[sr->count] = clientid;
        sr->fileids[sr->count] = fileid;
        sr->count++;
    }
    return 0;
}

/* -----------------------------------------------------------------------
 * Test 1: ds_layout_idx put/scan/del round-trip
 * ----------------------------------------------------------------------- */

static void test_idx_put_scan_del(void)
{
    struct mds_catalogue *db = NULL;
    struct mds_cat_txn *txn = NULL;
    struct scan_result sr;
    enum mds_status st;
    struct nfs4_stateid sid;
    uint32_t ds_ids[1] = {1};

    db = open_test_catalogue();
    if (db == NULL) { printf("  SKIP (no RonDB)\n"); return; }

    /* Grant a layout on ds_id=1 for clientid=100, fileid=200.
     * mds_coord_layout_grant creates both layout_state and ds_layout_idx. */
    memset(&sid, 0, sizeof(sid));
    sid.seqid = 1;
    st = mds_cat_txn_begin(db, MDS_CAT_TXN_WRITE, &txn);
    ASSERT_EQ(st, MDS_OK);
    st = mds_coord_layout_grant(db, txn, 100, 200, 1, 0,
                                0xFFFFFFFFFFFFFFFFULL, &sid,
                                ds_ids, 1);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(mds_cat_txn_commit(txn), MDS_OK);

    /* Scan for ds_id=1 — should find one entry. */
    memset(&sr, 0, sizeof(sr));
    st = mds_coord_ds_layout_idx_scan(db, 1, collect_cb, &sr);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(sr.count, 1);
    ASSERT_EQ(sr.clientids[0], 100);
    ASSERT_EQ(sr.fileids[0], 200);

    /* Return the layout (deletes layout_state + ds_layout_idx). */
    st = mds_coord_layout_return(db, NULL, sid.other, 100, 200,
                                 ds_ids, 1);
    ASSERT_EQ(st, MDS_OK);

    /* Scan again — should be empty. */
    memset(&sr, 0, sizeof(sr));
    st = mds_coord_ds_layout_idx_scan(db, 1, collect_cb, &sr);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(sr.count, 0);

    mds_catalogue_close(db);
    printf("  PASS: test_idx_put_scan_del\n");
}

/* -----------------------------------------------------------------------
 * Test 2: Multi-DS scan — entries for different ds_ids
 * ----------------------------------------------------------------------- */

static void test_idx_multi_ds_scan(void)
{
    struct mds_catalogue *db = NULL;
    struct mds_cat_txn *txn = NULL;
    struct scan_result sr;
    enum mds_status st;
    struct nfs4_stateid sid1, sid2, sid3;
    uint32_t ds1[] = {1};
    uint32_t ds2[] = {2};

    db = open_test_catalogue();
    if (db == NULL) { printf("  SKIP (no RonDB)\n"); return; }

    /* Grant 3 layouts: 2 on ds_id=1, 1 on ds_id=2. */
    memset(&sid1, 0, sizeof(sid1)); sid1.seqid = 1;
    memset(&sid2, 0, sizeof(sid2)); sid2.seqid = 2;
    memset(&sid3, 0, sizeof(sid3)); sid3.seqid = 3;

    st = mds_cat_txn_begin(db, MDS_CAT_TXN_WRITE, &txn);
    ASSERT_EQ(st, MDS_OK);
    st = mds_coord_layout_grant(db, txn, 10, 100, 1, 0,
                                0xFFFFFFFFFFFFFFFFULL, &sid1, ds1, 1);
    ASSERT_EQ(st, MDS_OK);
    st = mds_coord_layout_grant(db, txn, 10, 200, 1, 0,
                                0xFFFFFFFFFFFFFFFFULL, &sid2, ds1, 1);
    ASSERT_EQ(st, MDS_OK);
    st = mds_coord_layout_grant(db, txn, 20, 300, 1, 0,
                                0xFFFFFFFFFFFFFFFFULL, &sid3, ds2, 1);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(mds_cat_txn_commit(txn), MDS_OK);

    /* Scan ds_id=1 — should find 2 entries. */
    memset(&sr, 0, sizeof(sr));
    st = mds_coord_ds_layout_idx_scan(db, 1, collect_cb, &sr);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(sr.count, 2);

    /* Scan ds_id=2 — should find 1 entry. */
    memset(&sr, 0, sizeof(sr));
    st = mds_coord_ds_layout_idx_scan(db, 2, collect_cb, &sr);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(sr.count, 1);
    ASSERT_EQ(sr.clientids[0], 20);
    ASSERT_EQ(sr.fileids[0], 300);

    /* Scan ds_id=99 — should find nothing. */
    memset(&sr, 0, sizeof(sr));
    st = mds_coord_ds_layout_idx_scan(db, 99, collect_cb, &sr);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(sr.count, 0);

    mds_catalogue_close(db);
    printf("  PASS: test_idx_multi_ds_scan\n");
}

/* -----------------------------------------------------------------------
 * Test 3: Empty scan returns MDS_OK
 * ----------------------------------------------------------------------- */

static void test_idx_empty_scan(void)
{
    struct mds_catalogue *db = NULL;
    struct scan_result sr;
    enum mds_status st;

    db = open_test_catalogue();
    if (db == NULL) { printf("  SKIP (no RonDB)\n"); return; }

    memset(&sr, 0, sizeof(sr));
    st = mds_coord_ds_layout_idx_scan(db, 42, collect_cb, &sr);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(sr.count, 0);

    mds_catalogue_close(db);
    printf("  PASS: test_idx_empty_scan\n");
}

/* -----------------------------------------------------------------------
 * Test 4: COMMIT_OP_LAYOUT_STATE_PUT via CQ
 * ----------------------------------------------------------------------- */

static void test_cq_layout_put(void)
{
    struct mds_catalogue *db = NULL;
    struct commit_queue *cq = NULL;
    char *path = make_temp_db_path();
    enum mds_status st;
    struct commit_op cop;
    struct commit_op_layout_put *lp;
    uint32_t ds_ids[2] = {10, 20};
    struct scan_result sr;

    st = ((db = open_test_catalogue()) != NULL ? MDS_OK : MDS_ERR_IO);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(commit_queue_create(db, NULL, 0, 0, 0, 0, 0, 0, &cq), 0);

    /* Submit a layout put with 2 DS entries. */
    memset(&cop, 0, sizeof(cop));
    cop.type = COMMIT_OP_LAYOUT_STATE_PUT;
    lp = &cop.args.layout_put;
    lp->clientid = 500;
    lp->fileid = 600;
    lp->iomode = 1; /* LAYOUTIOMODE4_READ */
    lp->offset = 0;
    lp->length = 0xFFFFFFFFFFFFFFFFULL;
    lp->stateid.seqid = 1;
    lp->ds_ids = ds_ids;
    lp->ds_count = 2;

    st = commit_queue_submit(cq, &cop);
    ASSERT_EQ(st, MDS_OK);

    /* Verify layout_state was persisted via stateid lookup. */
    {
        uint64_t got_cid, got_fid;
        uint32_t got_iomode;
        st = mds_coord_layout_get_by_stateid(db, lp->stateid.other,
                                        &got_cid, &got_fid,
                                        &got_iomode, NULL, NULL, NULL);
        ASSERT_EQ(st, MDS_OK);
        ASSERT_EQ(got_cid, 500);
        ASSERT_EQ(got_fid, 600);
        ASSERT_EQ(got_iomode, 1);
    }

    /* Verify reverse index entries for both DSs. */
    memset(&sr, 0, sizeof(sr));
    st = mds_coord_ds_layout_idx_scan(db, 10, collect_cb, &sr);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(sr.count, 1);
    ASSERT_EQ(sr.clientids[0], 500);
    ASSERT_EQ(sr.fileids[0], 600);

    memset(&sr, 0, sizeof(sr));
    st = mds_coord_ds_layout_idx_scan(db, 20, collect_cb, &sr);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(sr.count, 1);

    commit_queue_destroy(cq);
    mds_catalogue_close(db);
    unlink(path);
    {
        char lockpath[300];
        snprintf(lockpath, sizeof(lockpath), "%s-lock", path);
        unlink(lockpath);
    }
    printf("  PASS: test_cq_layout_put\n");
}

/* -----------------------------------------------------------------------
 * Test 5: COMMIT_OP_LAYOUT_STATE_DEL via CQ
 * ----------------------------------------------------------------------- */

static void test_cq_layout_del(void)
{
    struct mds_catalogue *db = NULL;
    struct commit_queue *cq = NULL;
    char *path = make_temp_db_path();
    enum mds_status st;
    struct commit_op cop;
    struct commit_op_layout_put *lp;
    struct commit_op_layout_del *ld;
    struct scan_result sr;
    uint32_t ds_ids[1] = {30};

    st = ((db = open_test_catalogue()) != NULL ? MDS_OK : MDS_ERR_IO);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(commit_queue_create(db, NULL, 0, 0, 0, 0, 0, 0, &cq), 0);

    /* First, put a layout. */
    memset(&cop, 0, sizeof(cop));
    cop.type = COMMIT_OP_LAYOUT_STATE_PUT;
    lp = &cop.args.layout_put;
    lp->clientid = 700;
    lp->fileid = 800;
    lp->iomode = 2; /* LAYOUTIOMODE4_RW */
    lp->offset = 0;
    lp->length = 4096;
    lp->stateid.seqid = 1;
    lp->ds_ids = ds_ids;
    lp->ds_count = 1;

    st = commit_queue_submit(cq, &cop);
    ASSERT_EQ(st, MDS_OK);

    /* Verify it exists via scan. */
    {
        bool has_layout = false;
        st = mds_coord_layout_scan_for_file(db, 800, &has_layout);
        ASSERT_EQ(st, MDS_OK);
        ASSERT_EQ(has_layout, 1);
    }

    /* Now delete it. */
    memset(&cop, 0, sizeof(cop));
    cop.type = COMMIT_OP_LAYOUT_STATE_DEL;
    ld = &cop.args.layout_del;
    ld->clientid = 700;
    ld->fileid = 800;
    ld->ds_ids = ds_ids;
    ld->ds_count = 1;

    st = commit_queue_submit(cq, &cop);
    ASSERT_EQ(st, MDS_OK);

    /* Verify layout_state is gone. */
    {
        bool has_layout = false;
        st = mds_coord_layout_scan_for_file(db, 800, &has_layout);
        ASSERT_EQ(st, MDS_OK);
        ASSERT_EQ(has_layout, 0);
    }

    /* Verify index entry is gone. */
    memset(&sr, 0, sizeof(sr));
    st = mds_coord_ds_layout_idx_scan(db, 30, collect_cb, &sr);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(sr.count, 0);

    commit_queue_destroy(cq);
    mds_catalogue_close(db);
    unlink(path);
    {
        char lockpath[300];
        snprintf(lockpath, sizeof(lockpath), "%s-lock", path);
        unlink(lockpath);
    }
    printf("  PASS: test_cq_layout_del\n");
}

/* -----------------------------------------------------------------------
 * Main
 * ----------------------------------------------------------------------- */

int main(void)
{
    printf("test_ds_layout_idx\n");

    test_idx_put_scan_del();
    test_idx_multi_ds_scan();
    test_idx_empty_scan();
    test_cq_layout_put();
    test_cq_layout_del();

    printf("\n%d passed, %d failed\n", pass_count, fail_count);
    return fail_count > 0 ? 1 : 0;
}
