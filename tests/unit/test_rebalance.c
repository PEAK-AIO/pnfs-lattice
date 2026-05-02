/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_rebalance.c — Unit tests for admin-triggered mirror relocation.
 */

#include <assert.h>

/* Like assert() but not elided by NDEBUG. */
#define VERIFY(expr) do { if (!(expr)) { \
	fprintf(stderr, "VERIFY FAILED: %s (%s:%d)\n", \
		#expr, __FILE__, __LINE__); abort(); } } while (0)
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <errno.h>

#include "pnfs_mds.h"
#include "test_helpers.h"
#include "mds_catalogue.h"
#include "mds_coordination.h"
#include "commit_queue.h"
#include "proxy_io.h"
#include "placement.h"
#include "rebalance.h"
#include "open_state.h"

/* -----------------------------------------------------------------------
 * Auto-txn helper for layout_state_put
 * ----------------------------------------------------------------------- */
static enum mds_status layout_put_auto(
    struct mds_catalogue *db, uint64_t clientid, uint64_t fileid,
    uint32_t iomode, uint64_t offset, uint64_t length,
    const struct nfs4_stateid *stateid)
{
    struct mds_cat_txn *txn = NULL;
    enum mds_status st;
    st = mds_cat_txn_begin(db, MDS_CAT_TXN_WRITE, &txn);
    if (st != MDS_OK) return st;
    st = mds_coord_layout_grant(db, txn,
                                    clientid, fileid, iomode,
                                    offset, length, stateid,
                                    NULL, 0);
    if (st != MDS_OK) {
        mds_cat_txn_abort(txn);
        return st;
    }
    return (mds_cat_txn_commit(txn) == 0) ? MDS_OK : MDS_ERR_IO;
}

/* -----------------------------------------------------------------------
 * Test helpers
 * ----------------------------------------------------------------------- */

static char g_tmpdir[256];

static void make_tmpdir(void)
{
    snprintf(g_tmpdir, sizeof(g_tmpdir), "/tmp/test_rebalance_XXXXXX");
    if (mkdtemp(g_tmpdir) == NULL) { abort(); };
}

static struct mds_catalogue *open_test_db(void)
{
    char path[512];
    snprintf(path, sizeof(path), "%s/test.db", g_tmpdir);
    struct mds_catalogue *db = NULL;
    enum mds_status st = ((db = open_test_catalogue()) != NULL ? MDS_OK : MDS_ERR_IO);
    VERIFY(st == MDS_OK);
    return db;
}

static void register_ds(struct mds_catalogue *db, uint32_t ds_id, uint32_t state)
{
    struct mds_ds_info info;
    struct mds_cat_txn *txn = NULL;
    memset(&info, 0, sizeof(info));
    info.ds_id = ds_id;
    info.state = state;
    info.port = 2049;
    snprintf(info.addr, sizeof(info.addr), "ds%u:/export", ds_id);
    VERIFY(mds_cat_txn_begin(db, MDS_CAT_TXN_WRITE, &txn) == MDS_OK);
    VERIFY(mds_cat_ds_put(db, txn, &info) == MDS_OK);
    VERIFY(mds_cat_txn_commit(txn) == 0);
}

static void create_test_inode(struct mds_catalogue *db, uint64_t fileid,
                              uint64_t size)
{
    struct mds_inode inode;
    struct mds_cat_txn *txn = NULL;
    memset(&inode, 0, sizeof(inode));
    inode.fileid = fileid;
    inode.type = MDS_FTYPE_REG;
    inode.mode = 0644;
    inode.nlink = 1;
    inode.size = size;
    VERIFY(mds_cat_txn_begin(db, MDS_CAT_TXN_WRITE, &txn) == MDS_OK);
    VERIFY(mds_cat_inode_put(db, txn, &inode) == MDS_OK);
    VERIFY(mds_cat_txn_commit(txn) == 0);
}

static void create_stripe_map(struct mds_catalogue *db, uint64_t fileid,
                               uint32_t mirror_count,
                               const struct mds_ds_map_entry *entries)
{
    struct mds_cat_txn *txn = NULL;
    VERIFY(mds_cat_txn_begin(db, MDS_CAT_TXN_WRITE, &txn) == MDS_OK);
    VERIFY(mds_cat_stripe_map_put(db, txn,
           fileid, 1, 65536, mirror_count, entries) == MDS_OK);
    VERIFY(mds_cat_txn_commit(txn) == 0);
}

static struct mds_proxy_ctx *setup_proxy(void)
{
    struct mds_proxy_ctx *ctx = NULL;
    VERIFY(mds_proxy_ctx_create(&ctx) == MDS_OK);
    return ctx;
}

static void mount_ds(struct mds_proxy_ctx *ctx, uint32_t ds_id)
{
    char path[384];
    snprintf(path, sizeof(path), "%s/ds%u", g_tmpdir, ds_id);
    mkdir(path, 0755);
    char dpath[512];
    snprintf(dpath, sizeof(dpath), "%s/data", path);
    mkdir(dpath, 0755);
    VERIFY(mds_proxy_mount_set(ctx, ds_id, path) == MDS_OK);
}

static void write_ds_data(uint32_t ds_id, uint64_t fileid,
                           uint32_t stripe, uint32_t mirror,
                           const void *data, size_t len)
{
    char path[512];
    snprintf(path, sizeof(path), "%s/ds%u/data/%lu_%u_%u",
             g_tmpdir, ds_id, (unsigned long)fileid, stripe, mirror);
    FILE *f = fopen(path, "w");
    VERIFY(f != NULL);
    VERIFY(fwrite(data, 1, len, f) == len);
    fclose(f);
}

static size_t read_ds_data(uint32_t ds_id, uint64_t fileid,
                            uint32_t stripe, uint32_t mirror,
                            void *buf, size_t buf_len)
{
    char path[512];
    snprintf(path, sizeof(path), "%s/ds%u/data/%lu_%u_%u",
             g_tmpdir, ds_id, (unsigned long)fileid, stripe, mirror);
    FILE *f = fopen(path, "r");
    if (f == NULL) return 0;
    size_t nr = fread(buf, 1, buf_len, f);
    fclose(f);
    return nr;
}

static void cleanup(void)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", g_tmpdir);
    (void)system(cmd);
}


/* Wait for rebalance to complete (DONE, IDLE, or ERROR). */
static void wait_rebalance_done(struct rebalance_worker *rw)
{
    usleep(100000); /* let thread initialise */
    for (int i = 0; i < 100; i++) {
        struct rebalance_status_info info;
        rebalance_status_ext(rw, &info);
        if (info.state == REBALANCE_DONE || info.state == REBALANCE_IDLE
            || info.state == REBALANCE_ERROR) {
            break;
        }
        usleep(50000);
    }
}

/* -----------------------------------------------------------------------
 * Tests
 * ----------------------------------------------------------------------- */

static void test_moves_file(void)
{
    make_tmpdir();
    struct mds_catalogue *db = open_test_db();
	struct mds_catalogue *cat = NULL;
	cat = db;
    struct mds_proxy_ctx *proxy = setup_proxy();
    struct open_state_table *ot = NULL;
    open_state_table_init(1, &ot);

    register_ds(db, 1, DS_ONLINE);
    register_ds(db, 2, DS_ONLINE);
    register_ds(db, 3, DS_ONLINE);

    mount_ds(proxy, 1);
    mount_ds(proxy, 2);
    mount_ds(proxy, 3);

    /* File 100: 2 mirrors on DS1 + DS2, 16 bytes. */
    struct mds_ds_map_entry ent[2];
    memset(ent, 0, sizeof(ent));
    ent[0].ds_id = 1; ent[0].nfs_fh_len = 5; memcpy(ent[0].nfs_fh, "100_0", 5);
    ent[1].ds_id = 2; ent[1].nfs_fh_len = 5; memcpy(ent[1].nfs_fh, "100_1", 5);
    create_stripe_map(db, 100, 2, ent);
    create_test_inode(db, 100, 16);
    write_ds_data(1, 100, 0, 0, "abcdefghijklmnop", 16);
    write_ds_data(2, 100, 0, 1, "abcdefghijklmnop", 16);

    /* Start CQ so COMMIT_OP_REBALANCE_MOVE can execute. */
    struct commit_queue *cq = NULL;
    VERIFY(commit_queue_create(db, NULL, 0, 0, 0, 0, 0, 0, &cq) == 0);

    struct rebalance_worker *rw = NULL;
    VERIFY(rebalance_init(cat, cq, proxy, ot, &rw) == 0);
    rebalance_set_retry_delay(rw, 0);

    /* Move DS1 → DS3. */
    VERIFY(rebalance_start(rw, 1, 3) == 0);
    wait_rebalance_done(rw);

    struct rebalance_status_info info;
    rebalance_status_ext(rw, &info);
    VERIFY(info.files_moved >= 1);

    /* Verify: stripe map now has DS3 replacing DS1. */
    uint32_t sc, su, mc;
    struct mds_ds_map_entry *new_ent = NULL;
    VERIFY(mds_cat_stripe_map_get(db, 100, &sc, &su, &mc, &new_ent) == MDS_OK);
    VERIFY(sc == 1 && mc == 2);
    VERIFY(new_ent[0].ds_id == 3);
    VERIFY(new_ent[1].ds_id == 2);

    /* Verify: data was copied to DS3. */
    char buf[32];
    size_t nr = read_ds_data(3, 100, 0, 0, buf, sizeof(buf));
    VERIFY(nr == 16);
    VERIFY(memcmp(buf, "abcdefghijklmnop", 16) == 0);

    /* Verify: GC entry exists for old DS1 copy. */
    struct mds_gc_entry gc;
    VERIFY(mds_cat_gc_peek(db, &gc) == MDS_OK);
    VERIFY(gc.fileid == 100);
    VERIFY(gc.ds_id == 1);
    /* FH should be the original stored FH: "100_0" (5 bytes). */
    VERIFY(gc.nfs_fh_len == 5);
    VERIFY(memcmp(gc.nfs_fh, "100_0", 5) == 0);

    free(new_ent);
    rebalance_destroy(rw);
    commit_queue_destroy(cq);
    open_state_table_destroy(ot);
    mds_proxy_ctx_destroy(proxy);
    mds_catalogue_close(db);
    cleanup();
    printf("  PASS: test_moves_file\n");
}

static void test_skips_active_layout(void)
{
    make_tmpdir();
    struct mds_catalogue *db = open_test_db();
	struct mds_catalogue *cat = NULL;
	cat = db;
    struct mds_proxy_ctx *proxy = setup_proxy();
    struct open_state_table *ot = NULL;
    open_state_table_init(1, &ot);

    register_ds(db, 1, DS_ONLINE);
    register_ds(db, 2, DS_ONLINE);
    register_ds(db, 3, DS_ONLINE);
    mount_ds(proxy, 1);
    mount_ds(proxy, 2);
    mount_ds(proxy, 3);

    struct mds_ds_map_entry ent[2];
    memset(ent, 0, sizeof(ent));
    ent[0].ds_id = 1; ent[0].nfs_fh_len = 3; memcpy(ent[0].nfs_fh, "fh1", 3);
    ent[1].ds_id = 2; ent[1].nfs_fh_len = 3; memcpy(ent[1].nfs_fh, "fh2", 3);
    create_stripe_map(db, 200, 2, ent);
    create_test_inode(db, 200, 8);
    write_ds_data(1, 200, 0, 0, "12345678", 8);

    /* Place active layout on the file. */
    struct nfs4_stateid sid;
    memset(&sid, 0, sizeof(sid));
    VERIFY(layout_put_auto(db, 42, 200, 1, 0, UINT64_MAX, &sid) == MDS_OK);

    struct commit_queue *cq = NULL;
    VERIFY(commit_queue_create(db, NULL, 0, 0, 0, 0, 0, 0, &cq) == 0);

    struct rebalance_worker *rw = NULL;
    VERIFY(rebalance_init(cat, cq, proxy, ot, &rw) == 0);
    rebalance_set_retry_delay(rw, 0);
    VERIFY(rebalance_start(rw, 1, 3) == 0);

    wait_rebalance_done(rw);

    struct rebalance_status_info info;
    rebalance_status_ext(rw, &info);
    VERIFY(info.files_moved == 0);
    VERIFY(info.skipped_active_layout >= 1);

    /* Stripe map unchanged. */
    uint32_t sc, su, mc;
    struct mds_ds_map_entry *got = NULL;
    VERIFY(mds_cat_stripe_map_get(db, 200, &sc, &su, &mc, &got) == MDS_OK);
    VERIFY(got[0].ds_id == 1);
    free(got);

    rebalance_destroy(rw);
    commit_queue_destroy(cq);
    open_state_table_destroy(ot);
    mds_proxy_ctx_destroy(proxy);
    mds_catalogue_close(db);
    cleanup();
    printf("  PASS: test_skips_active_layout\n");
}

static void test_skips_active_writer(void)
{
    make_tmpdir();
    struct mds_catalogue *db = open_test_db();
	struct mds_catalogue *cat = NULL;
	cat = db;
    struct mds_proxy_ctx *proxy = setup_proxy();
    struct open_state_table *ot = NULL;
    open_state_table_init(1, &ot);

    register_ds(db, 1, DS_ONLINE);
    register_ds(db, 2, DS_ONLINE);
    register_ds(db, 3, DS_ONLINE);
    mount_ds(proxy, 1);
    mount_ds(proxy, 2);
    mount_ds(proxy, 3);

    struct mds_ds_map_entry ent[2];
    memset(ent, 0, sizeof(ent));
    ent[0].ds_id = 1; ent[0].nfs_fh_len = 3; memcpy(ent[0].nfs_fh, "fh1", 3);
    ent[1].ds_id = 2; ent[1].nfs_fh_len = 3; memcpy(ent[1].nfs_fh, "fh2", 3);
    create_stripe_map(db, 300, 2, ent);
    create_test_inode(db, 300, 8);
    write_ds_data(1, 300, 0, 0, "12345678", 8);

    /* Open file for writing — triggers active-writer skip. */
    struct nfs4_stateid osid;
    uint8_t owner[] = "test_owner";
    VERIFY(open_state_open(ot, 42, owner, sizeof(owner) - 1,
                           300, OPEN4_SHARE_ACCESS_WRITE, 0, &osid) == 0);

    struct commit_queue *cq = NULL;
    VERIFY(commit_queue_create(db, NULL, 0, 0, 0, 0, 0, 0, &cq) == 0);

    struct rebalance_worker *rw = NULL;
    VERIFY(rebalance_init(cat, cq, proxy, ot, &rw) == 0);
    rebalance_set_retry_delay(rw, 0);
    VERIFY(rebalance_start(rw, 1, 3) == 0);

    wait_rebalance_done(rw);

    struct rebalance_status_info info;
    rebalance_status_ext(rw, &info);
    VERIFY(info.files_moved == 0);
    VERIFY(info.skipped_active_writer >= 1);

    rebalance_destroy(rw);
    commit_queue_destroy(cq);
    open_state_table_destroy(ot);
    mds_proxy_ctx_destroy(proxy);
    mds_catalogue_close(db);
    cleanup();
    printf("  PASS: test_skips_active_writer\n");
}

static void test_no_candidates(void)
{
    make_tmpdir();
    struct mds_catalogue *db = open_test_db();
	struct mds_catalogue *cat = NULL;
	cat = db;
    struct mds_proxy_ctx *proxy = setup_proxy();

    register_ds(db, 1, DS_ONLINE);
    register_ds(db, 2, DS_ONLINE);
    register_ds(db, 3, DS_ONLINE);
    mount_ds(proxy, 1);
    mount_ds(proxy, 2);
    mount_ds(proxy, 3);

    /* No stripe maps → no candidates for any source DS. */

    struct commit_queue *cq = NULL;
    VERIFY(commit_queue_create(db, NULL, 0, 0, 0, 0, 0, 0, &cq) == 0);

    struct rebalance_worker *rw = NULL;
    VERIFY(rebalance_init(cat, cq, proxy, NULL, &rw) == 0);
    rebalance_set_retry_delay(rw, 0);
    VERIFY(rebalance_start(rw, 1, 3) == 0);

    wait_rebalance_done(rw);

    struct rebalance_status_info info;
    rebalance_status_ext(rw, &info);
    VERIFY(info.files_moved == 0);

    rebalance_destroy(rw);
    commit_queue_destroy(cq);
    mds_proxy_ctx_destroy(proxy);
    mds_catalogue_close(db);
    cleanup();
    printf("  PASS: test_no_candidates\n");
}

static void test_graceful_stop(void)
{
    make_tmpdir();
    struct mds_catalogue *db = open_test_db();
	struct mds_catalogue *cat = NULL;
	cat = db;
    struct mds_proxy_ctx *proxy = setup_proxy();

    register_ds(db, 1, DS_ONLINE);
    register_ds(db, 2, DS_ONLINE);
    register_ds(db, 3, DS_ONLINE);
    mount_ds(proxy, 1);
    mount_ds(proxy, 2);
    mount_ds(proxy, 3);

    struct commit_queue *cq = NULL;
    VERIFY(commit_queue_create(db, NULL, 0, 0, 0, 0, 0, 0, &cq) == 0);

    struct rebalance_worker *rw = NULL;
    VERIFY(rebalance_init(cat, cq, proxy, NULL, &rw) == 0);
    rebalance_set_retry_delay(rw, 0);
    VERIFY(rebalance_start(rw, 1, 3) == 0);

    /* Immediately stop. */
    rebalance_stop(rw);

    /* Should not crash and should be stoppable. */
    rebalance_destroy(rw);
    commit_queue_destroy(cq);
    mds_proxy_ctx_destroy(proxy);
    mds_catalogue_close(db);
    cleanup();
    printf("  PASS: test_graceful_stop\n");
}

static void test_status_fields(void)
{
    make_tmpdir();
    struct mds_catalogue *db = open_test_db();
	struct mds_catalogue *cat = NULL;
	cat = db;
    struct mds_proxy_ctx *proxy = setup_proxy();

    register_ds(db, 1, DS_ONLINE);
    register_ds(db, 2, DS_ONLINE);
    register_ds(db, 3, DS_ONLINE);
    mount_ds(proxy, 1);
    mount_ds(proxy, 2);
    mount_ds(proxy, 3);

    struct rebalance_worker *rw = NULL;
    VERIFY(rebalance_init(cat, NULL, proxy, NULL, &rw) == 0);

    /* Before start: idle, zeroed. */
    struct rebalance_status_info info;
    rebalance_status_ext(rw, &info);
    VERIFY(info.state == REBALANCE_IDLE);
    VERIFY(info.files_moved == 0);
    VERIFY(info.bytes_copied == 0);

    /* NULL worker: zeroed. */
    rebalance_status_ext(NULL, &info);
    VERIFY(info.state == 0);

    rebalance_destroy(rw);
    mds_proxy_ctx_destroy(proxy);
    mds_catalogue_close(db);
    cleanup();
    printf("  PASS: test_status_fields\n");
}

static void test_rejects_source_eq_target(void)
{
    make_tmpdir();
    struct mds_catalogue *db = open_test_db();
	struct mds_catalogue *cat = NULL;
	cat = db;
    struct mds_proxy_ctx *proxy = setup_proxy();

    register_ds(db, 1, DS_ONLINE);
    mount_ds(proxy, 1);

    struct rebalance_worker *rw = NULL;
    VERIFY(rebalance_init(cat, NULL, proxy, NULL, &rw) == 0);

    VERIFY(rebalance_start(rw, 1, 1) == MDS_ERR_INVAL);

    rebalance_destroy(rw);
    mds_proxy_ctx_destroy(proxy);
    mds_catalogue_close(db);
    cleanup();
    printf("  PASS: test_rejects_source_eq_target\n");
}

static void test_rejects_target_offline(void)
{
    make_tmpdir();
    struct mds_catalogue *db = open_test_db();
	struct mds_catalogue *cat = NULL;
	cat = db;
    struct mds_proxy_ctx *proxy = setup_proxy();

    register_ds(db, 1, DS_ONLINE);
    register_ds(db, 3, DS_OFFLINE);
    mount_ds(proxy, 1);
    mount_ds(proxy, 3);

    struct rebalance_worker *rw = NULL;
    VERIFY(rebalance_init(cat, NULL, proxy, NULL, &rw) == 0);

    VERIFY(rebalance_start(rw, 1, 3) == MDS_ERR_IO);

    rebalance_destroy(rw);
    mds_proxy_ctx_destroy(proxy);
    mds_catalogue_close(db);
    cleanup();
    printf("  PASS: test_rejects_target_offline\n");
}

static void test_rejects_target_not_mounted(void)
{
    make_tmpdir();
    struct mds_catalogue *db = open_test_db();
	struct mds_catalogue *cat = NULL;
	cat = db;
    struct mds_proxy_ctx *proxy = setup_proxy();

    register_ds(db, 1, DS_ONLINE);
    register_ds(db, 3, DS_ONLINE);
    mount_ds(proxy, 1);
    /* DS3 registered ONLINE but NOT mounted. */

    struct rebalance_worker *rw = NULL;
    VERIFY(rebalance_init(cat, NULL, proxy, NULL, &rw) == 0);

    VERIFY(rebalance_start(rw, 1, 3) == MDS_ERR_IO);

    rebalance_destroy(rw);
    mds_proxy_ctx_destroy(proxy);
    mds_catalogue_close(db);
    cleanup();
    printf("  PASS: test_rejects_target_not_mounted\n");
}

static void test_rejects_target_already_mirror(void)
{
    make_tmpdir();
    struct mds_catalogue *db = open_test_db();
	struct mds_catalogue *cat = NULL;
	cat = db;
    struct mds_proxy_ctx *proxy = setup_proxy();

    register_ds(db, 1, DS_ONLINE);
    register_ds(db, 2, DS_ONLINE);
    mount_ds(proxy, 1);
    mount_ds(proxy, 2);

    /* File 400: mirrors on DS1 + DS2. Rebalance DS1 → DS2 should skip
     * because target is already a mirror. */
    struct mds_ds_map_entry ent[2];
    memset(ent, 0, sizeof(ent));
    ent[0].ds_id = 1; ent[0].nfs_fh_len = 3; memcpy(ent[0].nfs_fh, "fh1", 3);
    ent[1].ds_id = 2; ent[1].nfs_fh_len = 3; memcpy(ent[1].nfs_fh, "fh2", 3);
    create_stripe_map(db, 400, 2, ent);
    create_test_inode(db, 400, 8);
    write_ds_data(1, 400, 0, 0, "12345678", 8);

    struct commit_queue *cq = NULL;
    VERIFY(commit_queue_create(db, NULL, 0, 0, 0, 0, 0, 0, &cq) == 0);

    struct rebalance_worker *rw = NULL;
    VERIFY(rebalance_init(cat, cq, proxy, NULL, &rw) == 0);
    rebalance_set_retry_delay(rw, 0);
    VERIFY(rebalance_start(rw, 1, 2) == 0);

    wait_rebalance_done(rw);

    struct rebalance_status_info info;
    rebalance_status_ext(rw, &info);
    VERIFY(info.files_moved == 0);

    rebalance_destroy(rw);
    commit_queue_destroy(cq);
    mds_proxy_ctx_destroy(proxy);
    mds_catalogue_close(db);
    cleanup();
    printf("  PASS: test_rejects_target_already_mirror\n");
}

static void test_rebalance_move_cq_op(void)
{
    /* Test the COMMIT_OP_REBALANCE_MOVE does stripe_map_put + gc_enqueue. */
    make_tmpdir();
    struct mds_catalogue *db = open_test_db();

    register_ds(db, 1, DS_ONLINE);
    register_ds(db, 2, DS_ONLINE);
    register_ds(db, 3, DS_ONLINE);

    /* Set up initial stripe map: file 500, mirrors DS1+DS2. */
    struct mds_ds_map_entry ent[2];
    memset(ent, 0, sizeof(ent));
    ent[0].ds_id = 1; ent[0].nfs_fh_len = 5; memcpy(ent[0].nfs_fh, "500_0", 5);
    ent[1].ds_id = 2; ent[1].nfs_fh_len = 5; memcpy(ent[1].nfs_fh, "500_1", 5);
    create_stripe_map(db, 500, 2, ent);

    struct commit_queue *cq = NULL;
    VERIFY(commit_queue_create(db, NULL, 0, 0, 0, 0, 0, 0, &cq) == 0);

    /* Submit REBALANCE_MOVE: replace mirror 0 (DS1) with DS3, GC old DS1 copy. */
    struct mds_ds_map_entry new_ent[2];
    memcpy(new_ent, ent, sizeof(new_ent));
    new_ent[0].ds_id = 3;
    memcpy(new_ent[0].nfs_fh, "new_0", 5);
    new_ent[0].nfs_fh_len = 5;

    struct commit_op op;
    memset(&op, 0, sizeof(op));
    op.type = COMMIT_OP_REBALANCE_MOVE;
    op.args.rebalance_move.fileid = 500;
    op.args.rebalance_move.stripe_count = 1;
    op.args.rebalance_move.stripe_unit = 65536;
    op.args.rebalance_move.mirror_count = 2;
    op.args.rebalance_move.entries = new_ent;
    op.args.rebalance_move.gc_fileid = 500;
    op.args.rebalance_move.gc_ds_id = 1;
    memcpy(op.args.rebalance_move.gc_nfs_fh, "500_0", 5);
    op.args.rebalance_move.gc_fh_len = 5;

    VERIFY(commit_queue_submit(cq, &op) == MDS_OK);

    /* Verify stripe map updated. */
    uint32_t sc, su, mc;
    struct mds_ds_map_entry *got = NULL;
    VERIFY(mds_cat_stripe_map_get(db, 500, &sc, &su, &mc, &got) == MDS_OK);
    VERIFY(sc == 1 && mc == 2);
    VERIFY(got[0].ds_id == 3);
    VERIFY(got[1].ds_id == 2);
    free(got);

    /* Verify GC entry. */
    struct mds_gc_entry gc;
    VERIFY(mds_cat_gc_peek(db, &gc) == MDS_OK);
    VERIFY(gc.fileid == 500);
    VERIFY(gc.ds_id == 1);
    VERIFY(gc.nfs_fh_len == 5);
    VERIFY(memcmp(gc.nfs_fh, "500_0", 5) == 0);

    commit_queue_destroy(cq);
    mds_catalogue_close(db);
    cleanup();
    printf("  PASS: test_rebalance_move_cq_op\n");
}

int main(void)
{
    printf("test_rebalance:\n");
    test_rejects_source_eq_target();
    test_rejects_target_offline();
    test_rejects_target_not_mounted();
    test_rejects_target_already_mirror();
    test_status_fields();
    test_no_candidates();
    test_graceful_stop();
    test_rebalance_move_cq_op();
    test_moves_file();
    test_skips_active_layout();
    test_skips_active_writer();
    printf("All rebalance tests passed.\n");
    return 0;
}
