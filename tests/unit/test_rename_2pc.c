/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_rename_2pc.c -- Unit tests for cross-subtree rename 2PC state
 *                      machine with in-process loopback transport.
 *
 * RonDB-native: uses coordination journal API for verification
 * instead of raw catalogue reads.
 */

#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <endian.h>
#include <sys/stat.h>

#include "pnfs_mds.h"
#include "mds_catalogue.h"
#include "test_helpers.h"
#include "harness.h"        /* conformance_open_checked */
#include "mds_coordination.h"
#include "rename_2pc.h"
#include "cluster_transport.h"

/* -------------------------------------------------------------------
 * Helpers
 * ------------------------------------------------------------------- */

static int passed;
static int failed;

#define ASSERT_EQ(a, b) do { \
    if ((a) != (b)) { \
        fprintf(stderr, "  FAIL %s:%d: %s (%d) != %s (%d)\n", \
                __FILE__, __LINE__, #a, (int)(a), #b, (int)(b)); \
        failed++; \
        return; \
    } \
} while (0)

#define ASSERT_TRUE(cond) do { \
    if (!(cond)) { \
        fprintf(stderr, "  FAIL %s:%d: !(%s)\n", \
                __FILE__, __LINE__, #cond); \
        failed++; \
        return; \
    } \
} while (0)

#define PASS() do { \
    passed++; \
    fprintf(stdout, "  PASS\n"); \
} while (0)

#define SKIP(msg) do { \
    fprintf(stdout, "  SKIP (%s)\n", (msg)); \
    passed++; \
    return; \
} while (0)

/* -------------------------------------------------------------------
 * Loopback transport
 * ------------------------------------------------------------------- */

struct loopback_ctx {
    struct mds_catalogue *cat;
    uint32_t              coordinator_id;
    uint64_t              last_txn_id;
    int                   prepare_vote;  /* -1=real, 0=force abort, 1=force commit */
};

static int loopback_prepare(uint32_t remote_mds_id, uint64_t txn_id,
                            uint64_t dst_parent, const char *dst_name,
                            const void *inode_data, size_t data_len,
                            void *user_ctx)
{
    struct loopback_ctx *lc = user_ctx;
    (void)remote_mds_id;

    lc->last_txn_id = txn_id;

    int vote = rename_2pc_on_prepare(lc->cat, txn_id,
                                     dst_parent, dst_name,
                                     inode_data, data_len,
                                     lc->coordinator_id);

    if (lc->prepare_vote >= 0) {
        vote = lc->prepare_vote;
    }
    return vote;
}

static int loopback_commit(uint32_t remote_mds_id, uint64_t txn_id,
                           void *user_ctx)
{
    struct loopback_ctx *lc = user_ctx;
    (void)remote_mds_id;
    enum mds_status st = rename_2pc_on_commit(lc->cat, txn_id);
    return (st == MDS_OK) ? 0 : -1;
}

/** Commit transport that always fails (simulates lost RPC). */
static int loopback_commit_fail(uint32_t remote_mds_id, uint64_t txn_id,
                                void *user_ctx)
{
    (void)remote_mds_id;
    (void)txn_id;
    (void)user_ctx;
    return -1;  /* Simulate network failure. */
}

static int loopback_abort(uint32_t remote_mds_id, uint64_t txn_id,
                          void *user_ctx)
{
    struct loopback_ctx *lc = user_ctx;
    (void)remote_mds_id;
    enum mds_status st = rename_2pc_on_abort(lc->cat, txn_id);
    return (st == MDS_OK) ? 0 : -1;
}

/* -------------------------------------------------------------------
 * Catalogue setup / teardown
 * ------------------------------------------------------------------- */

/* Backend selected by CATALOGUE_TEST_BACKEND (memdb by default); an
 * unavailable backend exits 77 before the first test.  Each test opens
 * its own handle and the loopback transport shares that one handle, so
 * coordinator and participant see ONE store.
 *
 * Persistent-store isolation: every test works inside its own
 * conformance scratch directory (g_dir) instead of the export root,
 * journal txn_ids are derived from this process's pid so two runs never
 * share a row, and each test deletes the journal rows it leaves behind
 * by design (the "must survive recovery" rows) so the next run's
 * rename_2pc_recover() starts from an empty journal, as the first run
 * did.  No assertion is weakened. */
static uint64_t g_dir;

static struct mds_catalogue *open_test_db(void)
{
    struct mds_catalogue *cat = conformance_open_checked();

    g_dir = 0;
    if (conformance_scratch_dir(cat, &g_dir) != MDS_OK) {
        fprintf(stderr, "cannot create the scratch directory\n");
        mds_catalogue_close(cat);
        return NULL;
    }
    return cat;
}

static void close_test_db(struct mds_catalogue *cat)
{
    if (g_dir != 0) {
        conformance_scratch_cleanup(cat, g_dir);
        g_dir = 0;
    }
    mds_catalogue_close(cat);
}

/* Journal txn_id private to this run: the process id in the high bits,
 * a small per-test tag in the low bits. */
static uint64_t run_txn_id(uint32_t tag)
{
    return ((uint64_t)(uint32_t)getpid() << 20) | (uint64_t)tag;
}

static uint64_t create_test_reg(struct mds_catalogue *db, const char *name)
{
    struct mds_inode out;
    enum mds_status st = test_create_file(db, g_dir, name, 0644, &out);
    return (st == MDS_OK) ? out.fileid : 0;
}

static uint64_t create_test_dir(struct mds_catalogue *db, const char *name)
{
    struct mds_inode out;
    enum mds_status st = mds_cat_ns_create(db, NULL, g_dir, name,
                                        MDS_FTYPE_DIR, 0755, 0, 0,
                                        NULL, &out);
    return (st == MDS_OK) ? out.fileid : 0;
}

/* -------------------------------------------------------------------
 * Journal scan helper -- counts entries matching a txn_id and picks
 * out the COORDINATOR's record explicitly.  The loopback transport
 * makes coordinator and participant share one store, so a txn_id has
 * two journal rows; a scan's delivery order is backend-specific
 * (arbitrary on RonDB), so the assertion must never depend on which
 * row arrives last.
 * ------------------------------------------------------------------- */

#define JOURNAL_ROLE_COORDINATOR 0  /* R2PC_COORDINATOR (rename_2pc.c) */

struct journal_scan_ctx {
    uint64_t target_txn_id;
    uint32_t count;            /* rows with target_txn_id, any role */
    bool     coord_found;      /* coordinator row seen */
    uint8_t  coord_state;      /* its state, valid when coord_found */
};

static int journal_count_cb(const struct mds_coord_journal_record *rec,
                            void *arg)
{
    struct journal_scan_ctx *ctx = arg;

    if (rec->txn_id == ctx->target_txn_id) {
        ctx->count++;
        if (rec->role == JOURNAL_ROLE_COORDINATOR) {
            ctx->coord_found = true;
            ctx->coord_state = rec->state;
        }
    }
    return 0;
}

/* -------------------------------------------------------------------
 * test_2pc_commit_happy_path
 * ------------------------------------------------------------------- */

static void test_2pc_commit_happy_path(void)
{
    fprintf(stdout, "  test_2pc_commit_happy_path: ");

    struct mds_catalogue *cat = open_test_db();
    if (cat == NULL) SKIP("no RonDB");

    uint64_t src_fid = create_test_reg(cat, "src_file");
    ASSERT_TRUE(src_fid > 0);

    struct loopback_ctx lc = {
        .cat = cat, .coordinator_id = 0,
        .prepare_vote = -1,
    };
    struct rename_2pc_transport transport = {
        .prepare = loopback_prepare, .commit = loopback_commit,
        .abort_rename = loopback_abort, .user_ctx = &lc,
    };

    enum mds_status st = rename_2pc_initiate(
        cat, &transport,
        g_dir, "src_file",
        g_dir, "dst_file", 1);
    ASSERT_EQ(st, MDS_OK);

    /* src_file should be gone. */
    uint64_t child; uint8_t type;
    st = mds_cat_dirent_get(cat, g_dir, "src_file", &child, &type);
    ASSERT_EQ(st, MDS_ERR_NOTFOUND);

    /* dst_file should exist. */
    st = mds_cat_dirent_get(cat, g_dir, "dst_file", &child, &type);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(type, (uint8_t)MDS_FTYPE_REG);

    close_test_db(cat);
    PASS();
}

/* -------------------------------------------------------------------
 * test_2pc_abort_on_conflict
 * ------------------------------------------------------------------- */

static void test_2pc_abort_on_conflict(void)
{
    fprintf(stdout, "  test_2pc_abort_on_conflict: ");

    struct mds_catalogue *cat = open_test_db();
    if (cat == NULL) SKIP("no RonDB");

    uint64_t src_fid = create_test_reg(cat, "src_file");
    ASSERT_TRUE(src_fid > 0);
    uint64_t dst_fid = create_test_reg(cat, "dst_file");
    ASSERT_TRUE(dst_fid > 0);

    struct loopback_ctx lc = {
        .cat = cat, .coordinator_id = 0,
        .prepare_vote = -1,
    };
    struct rename_2pc_transport transport = {
        .prepare = loopback_prepare, .commit = loopback_commit,
        .abort_rename = loopback_abort, .user_ctx = &lc,
    };

    enum mds_status st = rename_2pc_initiate(
        cat, &transport,
        g_dir, "src_file",
        g_dir, "dst_file", 1);
    ASSERT_EQ(st, MDS_ERR_XDEV);

    /* Source should still exist. */
    uint64_t child; uint8_t type;
    st = mds_cat_dirent_get(cat, g_dir, "src_file", &child, &type);
    ASSERT_EQ(st, MDS_OK);

    close_test_db(cat);
    PASS();
}

/* -------------------------------------------------------------------
 * test_2pc_dir_rejected
 * ------------------------------------------------------------------- */

static void test_2pc_dir_rejected(void)
{
    fprintf(stdout, "  test_2pc_dir_rejected: ");

    struct mds_catalogue *cat = open_test_db();
    if (cat == NULL) SKIP("no RonDB");

    uint64_t dir_fid = create_test_dir(cat, "mydir");
    ASSERT_TRUE(dir_fid > 0);

    struct loopback_ctx lc = {
        .cat = cat, .coordinator_id = 0,
        .prepare_vote = -1,
    };
    struct rename_2pc_transport transport = {
        .prepare = loopback_prepare, .commit = loopback_commit,
        .abort_rename = loopback_abort, .user_ctx = &lc,
    };

    enum mds_status st = rename_2pc_initiate(
        cat, &transport,
        g_dir, "mydir",
        g_dir, "newdir", 1);
    ASSERT_EQ(st, MDS_ERR_XDEV);

    uint64_t child; uint8_t type;
    st = mds_cat_dirent_get(cat, g_dir, "mydir", &child, &type);
    ASSERT_EQ(st, MDS_OK);

    close_test_db(cat);
    PASS();
}

/* -------------------------------------------------------------------
 * test_2pc_recover
 *
 * Write a PREPARED+COORDINATOR journal entry via coordination API,
 * then call recover.  Recovery should clean it up.
 * ------------------------------------------------------------------- */

static void test_2pc_recover(void)
{
    fprintf(stdout, "  test_2pc_recover: ");

    struct mds_catalogue *cat = open_test_db();
    if (cat == NULL) SKIP("no RonDB");

    /* Write a PREPARED journal entry via coordination API. */
    const uint64_t txn_id = run_txn_id(0x9999);
    struct mds_coord_journal_record rec;
    memset(&rec, 0, sizeof(rec));
    rec.txn_id = txn_id;
    rec.state = 1;  /* R2PC_PREPARED */
    rec.role = 0;   /* R2PC_COORDINATOR */
    rec.remote_mds_id = 1;
    rec.src_parent_fileid = g_dir;
    rec.dst_parent_fileid = g_dir;
    snprintf(rec.src_name, sizeof(rec.src_name), "src");
    snprintf(rec.dst_name, sizeof(rec.dst_name), "dst");

    struct mds_cat_txn *txn = NULL;
    ASSERT_EQ(mds_cat_txn_begin(cat, MDS_CAT_TXN_WRITE, &txn), MDS_OK);
    ASSERT_EQ(mds_coord_journal_put(cat, txn, &rec), MDS_OK);
    ASSERT_EQ(mds_cat_txn_commit(txn), MDS_OK);

    /* Recover should clean it up. */
    enum mds_status st = rename_2pc_recover(cat, NULL, NULL);
    ASSERT_EQ(st, MDS_OK);

    /* Entry should be gone -- verify via coordination API. */
    struct mds_coord_journal_record got;
    st = mds_coord_journal_get(cat, NULL, txn_id, 0, &got);
    ASSERT_EQ(st, MDS_ERR_NOTFOUND);

    close_test_db(cat);
    PASS();
}

/* -------------------------------------------------------------------
 * test_2pc_transport_error
 * ------------------------------------------------------------------- */

static void test_2pc_transport_error(void)
{
    fprintf(stdout, "  test_2pc_transport_error: ");

    struct mds_catalogue *cat = open_test_db();
    if (cat == NULL) SKIP("no RonDB");

    uint64_t src_fid = create_test_reg(cat, "errfile");
    ASSERT_TRUE(src_fid > 0);

    struct loopback_ctx lc = {
        .cat = cat, .coordinator_id = 0,
        .prepare_vote = 0,  /* Force abort. */
    };
    struct rename_2pc_transport transport = {
        .prepare = loopback_prepare, .commit = loopback_commit,
        .abort_rename = loopback_abort, .user_ctx = &lc,
    };

    enum mds_status st = rename_2pc_initiate(
        cat, &transport,
        g_dir, "errfile",
        g_dir, "errfile_dst", 1);
    ASSERT_EQ(st, MDS_ERR_XDEV);

    uint64_t child; uint8_t type;
    st = mds_cat_dirent_get(cat, g_dir, "errfile", &child, &type);
    ASSERT_EQ(st, MDS_OK);

    close_test_db(cat);
    PASS();
}

/* -------------------------------------------------------------------
 * test_2pc_commit_delivery_failure
 *
 * Prepare succeeds, coordinator commits locally, but commit RPC
 * fails.  Verifies COMMITTED journal entry is preserved.
 * ------------------------------------------------------------------- */

static void test_2pc_commit_delivery_failure(void)
{
    fprintf(stdout, "  test_2pc_commit_delivery_failure: ");

    struct mds_catalogue *cat = open_test_db();
    if (cat == NULL) SKIP("no RonDB");

    uint64_t src_fid = create_test_reg(cat, "lostfile");
    ASSERT_TRUE(src_fid > 0);

    struct loopback_ctx lc = {
        .cat = cat, .coordinator_id = 0,
        .prepare_vote = -1,
    };
    struct rename_2pc_transport transport = {
        .prepare = loopback_prepare,
        .commit = loopback_commit_fail,
        .abort_rename = loopback_abort,
        .user_ctx = &lc,
    };

    enum mds_status st = rename_2pc_initiate(
        cat, &transport,
        g_dir, "lostfile",
        g_dir, "lostfile_dst", 1);

    /* Must NOT return MDS_OK -- commit delivery failed. */
    ASSERT_EQ(st, MDS_ERR_IO);

    /* RonDB mode: source is kept until commit delivery succeeds.
     * The coordinator only removes the source dirent AFTER the
     * commit RPC is acknowledged, so a delivery failure leaves
     * the source intact. */
    uint64_t child; uint8_t type;
    st = mds_cat_dirent_get(cat, g_dir, "lostfile", &child, &type);
    ASSERT_EQ(st, MDS_OK);

    /* The coordinator's COMMITTED journal entry MUST still exist --
     * scan via coord API and select it by (txn_id, role). */
    struct journal_scan_ctx sc;
    memset(&sc, 0, sizeof(sc));
    sc.target_txn_id = lc.last_txn_id;
    st = mds_coord_journal_scan(cat, journal_count_cb, &sc);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_TRUE(sc.count > 0);
    ASSERT_TRUE(sc.coord_found);
    ASSERT_EQ(sc.coord_state, 2);  /* R2PC_COMMITTED */

    /* Recovery must keep it (returns MDS_ERR_DELAY). */
    st = rename_2pc_recover(cat, NULL, NULL);
    ASSERT_EQ(st, MDS_ERR_DELAY);

    /* The rows this test leaves by design (coordinator COMMITTED,
     * participant PREPARED) would make the next run's recover() keep
     * finding work; remove them now that they have been verified. */
    ASSERT_EQ(mds_coord_journal_del(cat, NULL, lc.last_txn_id, 0), MDS_OK);
    ASSERT_EQ(mds_coord_journal_del(cat, NULL, lc.last_txn_id, 1), MDS_OK);

    close_test_db(cat);
    PASS();
}

/* -------------------------------------------------------------------
 * test_2pc_recover_committed_kept
 *
 * A COMMITTED+COORDINATOR entry must survive recovery.
 * ------------------------------------------------------------------- */

static void test_2pc_recover_committed_kept(void)
{
    fprintf(stdout, "  test_2pc_recover_committed_kept: ");

    struct mds_catalogue *cat = open_test_db();
    if (cat == NULL) SKIP("no RonDB");

    /* Insert a COMMITTED+COORDINATOR journal entry. */
    const uint64_t txn_id = run_txn_id(0x8888);
    struct mds_coord_journal_record rec;
    memset(&rec, 0, sizeof(rec));
    rec.txn_id = txn_id;
    rec.state = 2;  /* R2PC_COMMITTED */
    rec.role = 0;   /* R2PC_COORDINATOR */
    rec.remote_mds_id = 1;
    rec.src_parent_fileid = g_dir;
    rec.dst_parent_fileid = g_dir;

    struct mds_cat_txn *txn = NULL;
    ASSERT_EQ(mds_cat_txn_begin(cat, MDS_CAT_TXN_WRITE, &txn), MDS_OK);
    ASSERT_EQ(mds_coord_journal_put(cat, txn, &rec), MDS_OK);
    ASSERT_EQ(mds_cat_txn_commit(txn), MDS_OK);

    /* Recovery must keep it. */
    enum mds_status st = rename_2pc_recover(cat, NULL, NULL);
    ASSERT_EQ(st, MDS_ERR_DELAY);

    /* Entry must still be present. */
    struct mds_coord_journal_record got;
    st = mds_coord_journal_get(cat, NULL, txn_id, 0, &got);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(got.state, (uint8_t)2);

    /* Verified; do not leave it for the next run's recover(). */
    ASSERT_EQ(mds_coord_journal_del(cat, NULL, txn_id, 0), MDS_OK);

    close_test_db(cat);
    PASS();
}

/* -------------------------------------------------------------------
 * test_2pc_recover_prepared_participant_kept
 *
 * A PREPARED+PARTICIPANT entry must survive recovery.
 * ------------------------------------------------------------------- */

static void test_2pc_recover_prepared_participant_kept(void)
{
    fprintf(stdout, "  test_2pc_recover_prepared_participant_kept: ");

    struct mds_catalogue *cat = open_test_db();
    if (cat == NULL) SKIP("no RonDB");

    /* Insert a PREPARED+PARTICIPANT journal entry. */
    const uint64_t txn_id = run_txn_id(0x7777);
    struct mds_coord_journal_record rec;
    memset(&rec, 0, sizeof(rec));
    rec.txn_id = txn_id;
    rec.state = 1;  /* R2PC_PREPARED */
    rec.role = 1;   /* R2PC_PARTICIPANT */
    rec.remote_mds_id = 2;
    rec.src_parent_fileid = g_dir;
    rec.dst_parent_fileid = g_dir;

    struct mds_cat_txn *txn = NULL;
    ASSERT_EQ(mds_cat_txn_begin(cat, MDS_CAT_TXN_WRITE, &txn), MDS_OK);
    ASSERT_EQ(mds_coord_journal_put(cat, txn, &rec), MDS_OK);
    ASSERT_EQ(mds_cat_txn_commit(txn), MDS_OK);

    /* Recovery must keep it. */
    enum mds_status st = rename_2pc_recover(cat, NULL, NULL);
    ASSERT_EQ(st, MDS_ERR_DELAY);

    /* Entry must still be present. */
    struct mds_coord_journal_record got;
    st = mds_coord_journal_get(cat, NULL, txn_id, 1, &got);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(got.state, (uint8_t)1);
    ASSERT_EQ(got.role, (uint8_t)1);

    /* Verified; do not leave it for the next run's recover(). */
    ASSERT_EQ(mds_coord_journal_del(cat, NULL, txn_id, 1), MDS_OK);

    close_test_db(cat);
    PASS();
}

/* -------------------------------------------------------------------
 * main
 * ------------------------------------------------------------------- */

int main(void)
{
    fprintf(stdout, "test_rename_2pc (RonDB-native):\n");

    test_2pc_commit_happy_path();
    test_2pc_abort_on_conflict();
    test_2pc_dir_rejected();
    test_2pc_recover();
    test_2pc_transport_error();
    test_2pc_commit_delivery_failure();
    test_2pc_recover_committed_kept();
    test_2pc_recover_prepared_participant_kept();

    fprintf(stdout, "\n  %d passed, %d failed\n", passed, failed);
    conformance_shutdown();
    return failed ? 1 : 0;
}
