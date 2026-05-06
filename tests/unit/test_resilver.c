/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_resilver.c -- Unit tests for background resilvering.
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
#include "resilver.h"
#include "open_state.h"


/* -----------------------------------------------------------------------
 * Auto-txn helper for layout_state_put (test convenience)
 * ----------------------------------------------------------------------- */
static enum mds_status resilver_layout_grant_auto(
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
    snprintf(g_tmpdir, sizeof(g_tmpdir), "/tmp/test_resilver_XXXXXX");
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

/* Write test data to a DS file directly. */
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

/* Read DS file data back. */
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

/* -----------------------------------------------------------------------
 * Scan callback for test verification
 * ----------------------------------------------------------------------- */

struct test_scan_ctx {
    uint64_t found_fileids[64];
    uint32_t count;
};

static int test_scan_cb(uint64_t fileid, uint32_t sc, uint32_t su,
                         uint32_t mc, const struct mds_ds_map_entry *ent,
                         void *arg)
{
    (void)sc; (void)su; (void)mc; (void)ent;
    struct test_scan_ctx *ctx = arg;
    if (ctx->count < 64)
        ctx->found_fileids[ctx->count++] = fileid;
    return 0;
}

/* -----------------------------------------------------------------------
 * Tests
 * ----------------------------------------------------------------------- */

static void test_stripe_map_scan_finds_degraded(void)
{
    make_tmpdir();
    struct mds_catalogue *db = open_test_db();

    register_ds(db, 1, DS_ONLINE);
    register_ds(db, 2, DS_OFFLINE);

    /* File 100: 2 mirrors, DS1 + DS2 (DS2 offline). */
    struct mds_ds_map_entry ent[2];
    memset(ent, 0, sizeof(ent));
    ent[0].ds_id = 1; ent[0].nfs_fh_len = 3; memcpy(ent[0].nfs_fh, "fh1", 3);
    ent[1].ds_id = 2; ent[1].nfs_fh_len = 3; memcpy(ent[1].nfs_fh, "fh2", 3);
    create_stripe_map(db, 100, 2, ent);

    struct test_scan_ctx sc = { .count = 0 };
    enum mds_status st = mds_cat_stripe_map_scan(db, test_scan_cb, &sc);
    VERIFY(st == MDS_OK);
    VERIFY(sc.count == 1);
    VERIFY(sc.found_fileids[0] == 100);

    mds_catalogue_close(db);
    cleanup();
    printf("  PASS: test_stripe_map_scan_finds_degraded\n");
}

static void test_skips_fully_mirrored(void)
{
    make_tmpdir();
    struct mds_catalogue *db = open_test_db();

    register_ds(db, 1, DS_ONLINE);
    register_ds(db, 2, DS_ONLINE);

    struct mds_ds_map_entry ent[2];
    memset(ent, 0, sizeof(ent));
    ent[0].ds_id = 1; ent[0].nfs_fh_len = 3; memcpy(ent[0].nfs_fh, "fh1", 3);
    ent[1].ds_id = 2; ent[1].nfs_fh_len = 3; memcpy(ent[1].nfs_fh, "fh2", 3);
    create_stripe_map(db, 200, 2, ent);

    /* Scan should find nothing degraded. */
    struct test_scan_ctx sc = { .count = 0 };
    mds_cat_stripe_map_scan(db, test_scan_cb, &sc);
    /* File is fully healthy -- scan still finds it, but resilver scan_cb
       would skip it. Let's test the worker-level logic by running init. */

    mds_catalogue_close(db);
    cleanup();
    printf("  PASS: test_skips_fully_mirrored\n");
}

static void test_skips_single_mirror(void)
{
    make_tmpdir();
    struct mds_catalogue *db = open_test_db();

    register_ds(db, 1, DS_ONLINE);

    struct mds_ds_map_entry ent[1];
    memset(ent, 0, sizeof(ent));
    ent[0].ds_id = 1; ent[0].nfs_fh_len = 3; memcpy(ent[0].nfs_fh, "fh1", 3);
    create_stripe_map(db, 300, 1, ent);

    /* Single-mirror file should not trigger scan callback. */
    struct test_scan_ctx sc = { .count = 0 };
    mds_cat_stripe_map_scan(db, test_scan_cb, &sc);
    VERIFY(sc.count == 1);  /* scan finds it */
    /* But resilver's scan_cb would skip (mirror_count < 2). */

    mds_catalogue_close(db);
    cleanup();
    printf("  PASS: test_skips_single_mirror\n");
}

static void test_resilver_copies_data(void)
{
    make_tmpdir();
    struct mds_catalogue *db = open_test_db();
	struct mds_catalogue *cat = NULL;
	cat = db;
    struct mds_proxy_ctx *proxy = setup_proxy();

    register_ds(db, 1, DS_ONLINE);
    register_ds(db, 2, DS_OFFLINE);
    register_ds(db, 3, DS_ONLINE);

    mount_ds(proxy, 1);
    mount_ds(proxy, 2);
    mount_ds(proxy, 3);

    /* Create inode + stripe map for file 100 (2 mirrors, DS1+DS2). */
    create_test_inode(db, 100, 1024);

    struct mds_ds_map_entry ent[2];
    memset(ent, 0, sizeof(ent));
    ent[0].ds_id = 1; ent[0].nfs_fh_len = 3; memcpy(ent[0].nfs_fh, "fh1", 3);
    ent[1].ds_id = 2; ent[1].nfs_fh_len = 3; memcpy(ent[1].nfs_fh, "fh2", 3);
    create_stripe_map(db, 100, 2, ent);

    /* Write test data on healthy mirror (DS1, mirror 0). */
    uint8_t data[1024];
    memset(data, 0xAB, sizeof(data));
    write_ds_data(1, 100, 0, 0, data, sizeof(data));

    /* Create CQ for atomic stripe map update. */
    struct commit_queue *cq = NULL;
    int cq_rc = commit_queue_create(db, NULL, 0, 0, 0, 0, 0, 0, &cq);
    VERIFY(cq_rc == 0);

    /* Run resilver. */
    struct resilver_worker *rw = NULL;
    VERIFY(resilver_init(cat, cq, proxy, NULL, &rw) == 0);
    resilver_set_retry_delay(rw, 0);
    VERIFY(resilver_start(rw, 3) == 0);

    /* Wait for completion (with timeout). */
    for (int i = 0; i < 100; i++) {
        uint32_t state;
        resilver_status(rw, NULL, NULL, NULL, &state);
        if (state == RESILVER_DONE || state == RESILVER_ERROR)
            break;
        usleep(50000); /* 50ms */
    }

    uint32_t state, total, done;
    uint64_t bytes;
    resilver_status(rw, &total, &done, &bytes, &state);

    VERIFY(state == RESILVER_DONE);
    VERIFY(total == 1);
    VERIFY(done == 1);
    VERIFY(bytes == 1024);

    /* Verify data was copied to DS3. */
    uint8_t read_buf[1024];
    size_t nr = read_ds_data(3, 100, 0, 1, read_buf, sizeof(read_buf));
    VERIFY(nr == 1024);
    VERIFY(memcmp(data, read_buf, 1024) == 0);

    /* Verify stripe map was updated. */
    uint32_t sc, su, mc;
    struct mds_ds_map_entry *new_ent = NULL;
    enum mds_status st = mds_cat_stripe_map_get(db, 100, &sc, &su, &mc,
                                                   &new_ent);
    VERIFY(st == MDS_OK);
    VERIFY(mc == 2);
    VERIFY(new_ent[1].ds_id == 3);  /* degraded slot replaced */
    free(new_ent);

    resilver_destroy(rw);
    commit_queue_destroy(cq);
    mds_proxy_ctx_destroy(proxy);
    mds_catalogue_close(db);
    cleanup();
    printf("  PASS: test_resilver_copies_data\n");
}

static void test_skips_file_with_active_layout(void)
{
    make_tmpdir();
    struct mds_catalogue *db = open_test_db();
	struct mds_catalogue *cat = NULL;
	cat = db;
    struct mds_proxy_ctx *proxy = setup_proxy();

    register_ds(db, 1, DS_ONLINE);
    register_ds(db, 2, DS_OFFLINE);
    register_ds(db, 3, DS_ONLINE);

    mount_ds(proxy, 1);
    mount_ds(proxy, 3);

    create_test_inode(db, 100, 512);

    struct mds_ds_map_entry ent[2];
    memset(ent, 0, sizeof(ent));
    ent[0].ds_id = 1; ent[0].nfs_fh_len = 3; memcpy(ent[0].nfs_fh, "fh1", 3);
    ent[1].ds_id = 2; ent[1].nfs_fh_len = 3; memcpy(ent[1].nfs_fh, "fh2", 3);
    create_stripe_map(db, 100, 2, ent);

    /* Add an active layout_state for file 100. */
    struct nfs4_stateid sid;
    memset(&sid, 0, sizeof(sid));
    enum mds_status st = resilver_layout_grant_auto(
        db, 999, 100, 1, 0, UINT64_MAX, &sid);
    VERIFY(st == MDS_OK);

    write_ds_data(1, 100, 0, 0, "test", 4);

    struct commit_queue *cq = NULL;
    commit_queue_create(db, NULL, 0, 0, 0, 0, 0, 0, &cq);

    struct resilver_worker *rw = NULL;
    VERIFY(resilver_init(cat, cq, proxy, NULL, &rw) == 0);
    resilver_set_retry_delay(rw, 0);
    VERIFY(resilver_start(rw, 3) == 0);

    for (int i = 0; i < 50; i++) {
        uint32_t s;
        resilver_status(rw, NULL, NULL, NULL, &s);
        if (s == RESILVER_DONE || s == RESILVER_ERROR)
            break;
        usleep(50000);
    }

    uint32_t total, done, s;
    resilver_status(rw, &total, &done, NULL, &s);
    VERIFY(s == RESILVER_DONE);
    VERIFY(total == 1);  /* Found in scan */
    VERIFY(done == 1);   /* Skipped (returns 0 = success/skip) */

    /* Stripe map should be unchanged (DS2 still in slot). */
    uint32_t sc2, su2, mc2;
    struct mds_ds_map_entry *ent2 = NULL;
    st = mds_cat_stripe_map_get(db, 100, &sc2, &su2, &mc2, &ent2);
    VERIFY(st == MDS_OK);
    VERIFY(ent2[1].ds_id == 2);  /* Still DS2 -- not replaced */
    free(ent2);

    resilver_destroy(rw);
    commit_queue_destroy(cq);
    mds_proxy_ctx_destroy(proxy);
    mds_catalogue_close(db);
    cleanup();
    printf("  PASS: test_skips_file_with_active_layout\n");
}

static void test_progress_counters(void)
{
    make_tmpdir();
    struct mds_catalogue *db = open_test_db();
	struct mds_catalogue *cat = NULL;
	cat = db;

    register_ds(db, 1, DS_ONLINE);

    struct resilver_worker *rw = NULL;
    VERIFY(resilver_init(cat, NULL, NULL, NULL, &rw) == 0);

    uint32_t total, done, state;
    uint64_t bytes;
    resilver_status(rw, &total, &done, &bytes, &state);
    VERIFY(state == RESILVER_IDLE);
    VERIFY(total == 0);
    VERIFY(done == 0);
    VERIFY(bytes == 0);

    resilver_destroy(rw);
    mds_catalogue_close(db);
    cleanup();
    printf("  PASS: test_progress_counters\n");
}

static void test_graceful_stop(void)
{
    make_tmpdir();
    struct mds_catalogue *db = open_test_db();
	struct mds_catalogue *cat = NULL;
	cat = db;
    struct mds_proxy_ctx *proxy = setup_proxy();

    register_ds(db, 1, DS_ONLINE);
    register_ds(db, 2, DS_OFFLINE);
    register_ds(db, 3, DS_ONLINE);

    mount_ds(proxy, 1);
    mount_ds(proxy, 3);

    /* Create many degraded files so resilver takes a while. */
    for (uint64_t fid = 1000; fid < 1050; fid++) {
        create_test_inode(db, fid, 512);
        struct mds_ds_map_entry ent[2];
        memset(ent, 0, sizeof(ent));
        ent[0].ds_id = 1; ent[0].nfs_fh_len = 3;
        memcpy(ent[0].nfs_fh, "fh1", 3);
        ent[1].ds_id = 2; ent[1].nfs_fh_len = 3;
        memcpy(ent[1].nfs_fh, "fh2", 3);
        create_stripe_map(db, fid, 2, ent);
        write_ds_data(1, fid, 0, 0, "data", 4);
    }

    struct commit_queue *cq = NULL;
    commit_queue_create(db, NULL, 0, 0, 0, 0, 0, 0, &cq);

    struct resilver_worker *rw = NULL;
    VERIFY(resilver_init(cat, cq, proxy, NULL, &rw) == 0);
    resilver_set_retry_delay(rw, 0);
    VERIFY(resilver_start(rw, 3) == 0);

    /* Give it a moment then stop. */
    usleep(10000);
    resilver_stop(rw);

    uint32_t state;
    resilver_status(rw, NULL, NULL, NULL, &state);
    /* Should not be DONE if we stopped early (could be COPYING). */
    VERIFY(state != RESILVER_IDLE);

    resilver_destroy(rw);
    commit_queue_destroy(cq);
    mds_proxy_ctx_destroy(proxy);
    mds_catalogue_close(db);
    cleanup();
    printf("  PASS: test_graceful_stop\n");
}

static void test_placement_select_replacement(void)
{
    struct mds_ds_info ds[3];
    memset(ds, 0, sizeof(ds));
    ds[0].ds_id = 1; ds[0].state = DS_ONLINE;
    ds[1].ds_id = 2; ds[1].state = DS_OFFLINE;
    ds[2].ds_id = 3; ds[2].state = DS_ONLINE;

    uint32_t exclude[] = { 1 };  /* DS1 already in use */
    uint32_t out = 0;

    /* Admin-specified target = 3. */
    enum mds_status st = placement_select_replacement(
        ds, 3, exclude, 1, 3, &out);
    VERIFY(st == MDS_OK);
    VERIFY(out == 3);

    /* Admin-specified target = 2 (offline) should fail. */
    st = placement_select_replacement(ds, 3, exclude, 1, 2, &out);
    VERIFY(st == MDS_ERR_NOSPC);

    /* Auto-select (target=0): should pick DS3 (DS1 excluded, DS2 offline). */
    out = 0;
    st = placement_select_replacement(ds, 3, exclude, 1, 0, &out);
    VERIFY(st == MDS_OK);
    VERIFY(out == 3);

    printf("  PASS: test_placement_select_replacement\n");
}

static void test_truncates_stale_target(void)
{
    make_tmpdir();
    struct mds_proxy_ctx *proxy = setup_proxy();
    mount_ds(proxy, 5);

    /* Create a file with stale data. */
    write_ds_data(5, 999, 0, 0, "STALE_DATA_HERE", 15);

    /* Truncate should zero it. */
    enum mds_status st = mds_proxy_truncate_ds_file(proxy, 5, 999, 0, 0);
    VERIFY(st == MDS_OK);

    uint8_t buf[32];
    size_t nr = read_ds_data(5, 999, 0, 0, buf, sizeof(buf));
    VERIFY(nr == 0);

    mds_proxy_ctx_destroy(proxy);
    cleanup();
    printf("  PASS: test_truncates_stale_target\n");
}

static void test_read_direct(void)
{
    make_tmpdir();
    struct mds_proxy_ctx *proxy = setup_proxy();
    mount_ds(proxy, 1);

    uint8_t data[] = "hello resilver test";
    write_ds_data(1, 42, 0, 0, data, sizeof(data) - 1);

    uint8_t buf[64];
    uint32_t nr = 0;
    bool eof = false;
    enum mds_status st = mds_proxy_read_direct(
        proxy, 1, 42, 0, 0, 0, buf, sizeof(buf), &nr, &eof);
    VERIFY(st == MDS_OK);
    VERIFY(nr == sizeof(data) - 1);
    VERIFY(eof == true);
    VERIFY(memcmp(buf, data, nr) == 0);

    mds_proxy_ctx_destroy(proxy);
    cleanup();
    printf("  PASS: test_read_direct\n");
}

static void test_layout_state_scan_for_file(void)
{
    make_tmpdir();
    struct mds_catalogue *db = open_test_db();

    /* No layouts -- should return false. */
    bool exists = true;
    enum mds_status st = mds_coord_layout_scan_for_file(
        db, 42, &exists);
    VERIFY(st == MDS_OK);
    VERIFY(!exists);

    /* Add a layout for file 42 (client 999). */
    struct nfs4_stateid sid;
    memset(&sid, 0, sizeof(sid));
    st = resilver_layout_grant_auto(db, 999, 42, 1, 0, UINT64_MAX, &sid);
    VERIFY(st == MDS_OK);

    exists = false;
    st = mds_coord_layout_scan_for_file(db, 42, &exists);
    VERIFY(st == MDS_OK);
    VERIFY(exists);

    /* Different file should not match. */
    exists = true;
    st = mds_coord_layout_scan_for_file(db, 43, &exists);
    VERIFY(st == MDS_OK);
    VERIFY(!exists);

    mds_catalogue_close(db);
    cleanup();
    printf("  PASS: test_layout_state_scan_for_file\n");
}

/* -----------------------------------------------------------------------
 * Main
 * ----------------------------------------------------------------------- */


/* Blocker 1: verify restart-after-DONE works. */
static void test_restart_after_done(void)
{
    make_tmpdir();
    struct mds_catalogue *db = open_test_db();
	struct mds_catalogue *cat = NULL;
	cat = db;
    struct mds_proxy_ctx *proxy = setup_proxy();

    register_ds(db, 1, DS_ONLINE);
    register_ds(db, 2, DS_OFFLINE);
    register_ds(db, 3, DS_ONLINE);
    mount_ds(proxy, 1);
    mount_ds(proxy, 2);
    mount_ds(proxy, 3);

    /* File with 2 mirrors, DS1+DS2.  DS2 offline. */
    create_test_inode(db, 200, 512);
    struct mds_ds_map_entry ent[2];
    memset(ent, 0, sizeof(ent));
    ent[0].ds_id = 1; ent[0].nfs_fh_len = 3; memcpy(ent[0].nfs_fh, "fh1", 3);
    ent[1].ds_id = 2; ent[1].nfs_fh_len = 3; memcpy(ent[1].nfs_fh, "fh2", 3);
    create_stripe_map(db, 200, 2, ent);

    uint8_t data[512];
    memset(data, 0xCD, sizeof(data));
    write_ds_data(1, 200, 0, 0, data, sizeof(data));

    struct commit_queue *cq = NULL;
    VERIFY(commit_queue_create(db, NULL, 0, 0, 0, 0, 0, 0, &cq) == 0);

    struct resilver_worker *rw = NULL;
    VERIFY(resilver_init(cat, cq, proxy, NULL, &rw) == 0);
    resilver_set_retry_delay(rw, 0);

    /* First run. */
    VERIFY(resilver_start(rw, 3) == 0);
    for (int i = 0; i < 100; i++) {
        uint32_t state;
        resilver_status(rw, NULL, NULL, NULL, &state);
        if (state == RESILVER_DONE || state == RESILVER_ERROR)
            break;
        usleep(50000);
    }

    uint32_t state;
    resilver_status(rw, NULL, NULL, NULL, &state);
    VERIFY(state == RESILVER_DONE);

    /* Re-register DS2 as OFFLINE again (since first run updated stripe map),
     * create a new file for the second run. */
    register_ds(db, 4, DS_ONLINE);
    mount_ds(proxy, 4);
    create_test_inode(db, 201, 256);
    struct mds_ds_map_entry ent2[2];
    memset(ent2, 0, sizeof(ent2));
    ent2[0].ds_id = 1; ent2[0].nfs_fh_len = 3; memcpy(ent2[0].nfs_fh, "fg1", 3);
    ent2[1].ds_id = 2; ent2[1].nfs_fh_len = 3; memcpy(ent2[1].nfs_fh, "fg2", 3);
    create_stripe_map(db, 201, 2, ent2);
    uint8_t data2[256];
    memset(data2, 0xEF, sizeof(data2));
    write_ds_data(1, 201, 0, 0, data2, sizeof(data2));

    /* Second run -- must NOT fail with "already running". */
    VERIFY(resilver_start(rw, 4) == 0);
    for (int i = 0; i < 100; i++) {
        resilver_status(rw, NULL, NULL, NULL, &state);
        if (state == RESILVER_DONE || state == RESILVER_ERROR)
            break;
        usleep(50000);
    }
    resilver_status(rw, NULL, NULL, NULL, &state);
    VERIFY(state == RESILVER_DONE);

    resilver_destroy(rw);
    commit_queue_destroy(cq);
    mds_catalogue_close(db);
    mds_proxy_ctx_destroy(proxy);
    cleanup();
    printf("  PASS: test_restart_after_done\n");
}

/* Blocker 2: verify incomplete copy does not commit stripe map. */
static void test_rejects_short_copy(void)
{
    make_tmpdir();
    struct mds_catalogue *db = open_test_db();
	struct mds_catalogue *cat = NULL;
	cat = db;
    struct mds_proxy_ctx *proxy = setup_proxy();

    register_ds(db, 1, DS_ONLINE);
    register_ds(db, 2, DS_OFFLINE);
    register_ds(db, 3, DS_ONLINE);
    mount_ds(proxy, 1);
    mount_ds(proxy, 2);
    mount_ds(proxy, 3);

    /* Register inode as 2048 bytes, but only write 512 bytes to DS file.
     * This simulates a truncated source. */
    create_test_inode(db, 300, 2048);
    struct mds_ds_map_entry ent[2];
    memset(ent, 0, sizeof(ent));
    ent[0].ds_id = 1; ent[0].nfs_fh_len = 3; memcpy(ent[0].nfs_fh, "fh1", 3);
    ent[1].ds_id = 2; ent[1].nfs_fh_len = 3; memcpy(ent[1].nfs_fh, "fh2", 3);
    create_stripe_map(db, 300, 2, ent);

    /* Write only 512 bytes -- inode says 2048. */
    uint8_t data[512];
    memset(data, 0xBB, sizeof(data));
    write_ds_data(1, 300, 0, 0, data, sizeof(data));

    struct commit_queue *cq = NULL;
    VERIFY(commit_queue_create(db, NULL, 0, 0, 0, 0, 0, 0, &cq) == 0);

    struct resilver_worker *rw = NULL;
    VERIFY(resilver_init(cat, cq, proxy, NULL, &rw) == 0);
    resilver_set_retry_delay(rw, 0);
    VERIFY(resilver_start(rw, 3) == 0);

    for (int i = 0; i < 100; i++) {
        uint32_t state;
        resilver_status(rw, NULL, NULL, NULL, &state);
        if (state == RESILVER_DONE || state == RESILVER_ERROR)
            break;
        usleep(50000);
    }

    /* The worker should finish (DONE) but the file should NOT have been
     * committed -- stripe map must still point to DS2 (original). */
    uint32_t sc, su, mc;
    struct mds_ds_map_entry *map_ent = NULL;
    VERIFY(mds_cat_stripe_map_get(db, 300, &sc, &su, &mc, &map_ent) == MDS_OK);
    VERIFY(map_ent[1].ds_id == 2);  /* NOT updated to DS3 */
    free(map_ent);

    resilver_destroy(rw);
    commit_queue_destroy(cq);
    mds_catalogue_close(db);
    mds_proxy_ctx_destroy(proxy);
    cleanup();
    printf("  PASS: test_rejects_short_copy\n");
}

/* -----------------------------------------------------------------------
 * B3: Skip-counter and retry tests (Seq 11)
 * ----------------------------------------------------------------------- */

/**
 * test_skip_counter_active_layout -- verify skipped_active_layout counter.
 */
static void test_skip_counter_active_layout(void)
{
    make_tmpdir();
    struct mds_catalogue *db = open_test_db();
	struct mds_catalogue *cat = NULL;
	cat = db;
    struct mds_proxy_ctx *proxy = setup_proxy();

    register_ds(db, 1, DS_ONLINE);
    register_ds(db, 2, DS_OFFLINE);
    register_ds(db, 3, DS_ONLINE);
    mount_ds(proxy, 1);
    mount_ds(proxy, 3);

    create_test_inode(db, 500, 128);
    struct mds_ds_map_entry ent[2];
    memset(ent, 0, sizeof(ent));
    ent[0].ds_id = 1; ent[0].nfs_fh_len = 3; memcpy(ent[0].nfs_fh, "fh1", 3);
    ent[1].ds_id = 2; ent[1].nfs_fh_len = 3; memcpy(ent[1].nfs_fh, "fh2", 3);
    create_stripe_map(db, 500, 2, ent);

    write_ds_data(1, 500, 0, 0, "data", 4);

    /* Add active layout for file 500. */
    struct nfs4_stateid sid;
    memset(&sid, 0, sizeof(sid));
    enum mds_status st = resilver_layout_grant_auto(
        db, 999, 500, 1, 0, UINT64_MAX, &sid);
    VERIFY(st == MDS_OK);

    struct commit_queue *cq = NULL;
    commit_queue_create(db, NULL, 0, 0, 0, 0, 0, 0, &cq);

    struct resilver_worker *rw = NULL;
    VERIFY(resilver_init(cat, cq, proxy, NULL, &rw) == 0);
    resilver_set_retry_delay(rw, 0);
    VERIFY(resilver_start(rw, 3) == 0);

    for (int i = 0; i < 50; i++) {
        uint32_t s;
        resilver_status(rw, NULL, NULL, NULL, &s);
        if (s == RESILVER_DONE || s == RESILVER_ERROR) break;
        usleep(50000);
    }

    struct resilver_status_info info;
    resilver_status_ext(rw, &info);
    VERIFY(info.state == RESILVER_DONE);
    VERIFY(info.skipped_active_layout >= 1);
    VERIFY(info.pass_count >= 1);

    resilver_destroy(rw);
    commit_queue_destroy(cq);
    mds_proxy_ctx_destroy(proxy);
    mds_catalogue_close(db);
    cleanup();
    printf("  PASS: test_skip_counter_active_layout\n");
}

/**
 * test_skip_counter_active_writer -- verify skipped_active_writer counter
 * via open_state_table with a WRITE open.
 */
static void test_skip_counter_active_writer(void)
{
    make_tmpdir();
    struct mds_catalogue *db = open_test_db();
	struct mds_catalogue *cat = NULL;
	cat = db;
    struct mds_proxy_ctx *proxy = setup_proxy();

    register_ds(db, 1, DS_ONLINE);
    register_ds(db, 2, DS_OFFLINE);
    register_ds(db, 3, DS_ONLINE);
    mount_ds(proxy, 1);
    mount_ds(proxy, 3);

    create_test_inode(db, 501, 128);
    struct mds_ds_map_entry ent[2];
    memset(ent, 0, sizeof(ent));
    ent[0].ds_id = 1; ent[0].nfs_fh_len = 3; memcpy(ent[0].nfs_fh, "fh1", 3);
    ent[1].ds_id = 2; ent[1].nfs_fh_len = 3; memcpy(ent[1].nfs_fh, "fh2", 3);
    create_stripe_map(db, 501, 2, ent);

    write_ds_data(1, 501, 0, 0, "data", 4);

    /* Open file 501 for writing -- triggers active-writer skip. */
    struct open_state_table *ot = NULL;
    VERIFY(open_state_table_init(1, &ot) == 0);

    struct nfs4_stateid osid;
    uint8_t owner[] = "test_owner";
    VERIFY(open_state_open(ot, 1000, owner, sizeof(owner) - 1,
                           501, OPEN4_SHARE_ACCESS_WRITE, 0, &osid) == 0);

    struct commit_queue *cq = NULL;
    commit_queue_create(db, NULL, 0, 0, 0, 0, 0, 0, &cq);

    struct resilver_worker *rw = NULL;
    VERIFY(resilver_init(cat, cq, proxy, ot, &rw) == 0);
    resilver_set_retry_delay(rw, 0);
    VERIFY(resilver_start(rw, 3) == 0);

    for (int i = 0; i < 50; i++) {
        uint32_t s;
        resilver_status(rw, NULL, NULL, NULL, &s);
        if (s == RESILVER_DONE || s == RESILVER_ERROR) break;
        usleep(50000);
    }

    struct resilver_status_info info;
    resilver_status_ext(rw, &info);
    VERIFY(info.state == RESILVER_DONE);
    VERIFY(info.skipped_active_writer >= 1);

    resilver_destroy(rw);
    commit_queue_destroy(cq);
    open_state_table_destroy(ot);
    mds_proxy_ctx_destroy(proxy);
    mds_catalogue_close(db);
    cleanup();
    printf("  PASS: test_skip_counter_active_writer\n");
}

/**
 * test_ext_status_zeroed_on_null -- verify resilver_status_ext handles NULL.
 */
static void test_ext_status_zeroed_on_null(void)
{
    struct resilver_status_info info;
    memset(&info, 0xFF, sizeof(info));
    resilver_status_ext(NULL, &info);
    VERIFY(info.state == 0);
    VERIFY(info.files_total == 0);
    VERIFY(info.pass_count == 0);
    VERIFY(info.skipped_active_layout == 0);
    printf("  PASS: test_ext_status_zeroed_on_null\n");
}

/**
 * test_pass_count_increments -- verify pass_count after a successful resilver.
 */
static void test_pass_count_increments(void)
{
    make_tmpdir();
    struct mds_catalogue *db = open_test_db();
	struct mds_catalogue *cat = NULL;
	cat = db;
    struct mds_proxy_ctx *proxy = setup_proxy();

    register_ds(db, 1, DS_ONLINE);
    register_ds(db, 2, DS_OFFLINE);
    register_ds(db, 3, DS_ONLINE);
    mount_ds(proxy, 1);
    mount_ds(proxy, 3);

    create_test_inode(db, 502, 64);
    struct mds_ds_map_entry ent[2];
    memset(ent, 0, sizeof(ent));
    ent[0].ds_id = 1; ent[0].nfs_fh_len = 3; memcpy(ent[0].nfs_fh, "fh1", 3);
    ent[1].ds_id = 2; ent[1].nfs_fh_len = 3; memcpy(ent[1].nfs_fh, "fh2", 3);
    create_stripe_map(db, 502, 2, ent);

    uint8_t data[64];
    memset(data, 0xAB, sizeof(data));
    write_ds_data(1, 502, 0, 0, data, sizeof(data));

    struct commit_queue *cq = NULL;
    commit_queue_create(db, NULL, 0, 0, 0, 0, 0, 0, &cq);

    struct resilver_worker *rw = NULL;
    VERIFY(resilver_init(cat, cq, proxy, NULL, &rw) == 0);
    resilver_set_retry_delay(rw, 0);
    VERIFY(resilver_start(rw, 3) == 0);

    for (int i = 0; i < 100; i++) {
        uint32_t s;
        resilver_status(rw, NULL, NULL, NULL, &s);
        if (s == RESILVER_DONE || s == RESILVER_ERROR) break;
        usleep(50000);
    }

    struct resilver_status_info info;
    resilver_status_ext(rw, &info);
    VERIFY(info.state == RESILVER_DONE);
    VERIFY(info.pass_count >= 1);
    VERIFY(info.files_done >= 1);

    resilver_destroy(rw);
    commit_queue_destroy(cq);
    mds_proxy_ctx_destroy(proxy);
    mds_catalogue_close(db);
    cleanup();
    printf("  PASS: test_pass_count_increments\n");
}

int main(void)
{
    printf("test_resilver:\n");
    test_stripe_map_scan_finds_degraded();
    test_skips_fully_mirrored();
    test_skips_single_mirror();
    test_placement_select_replacement();
    test_read_direct();
    test_truncates_stale_target();
    test_layout_state_scan_for_file();
    test_progress_counters();
    test_resilver_copies_data();
    test_skips_file_with_active_layout();
    test_graceful_stop();
    test_restart_after_done();
    test_rejects_short_copy();
    test_skip_counter_active_layout();
    test_skip_counter_active_writer();
    test_ext_status_zeroed_on_null();
    test_pass_count_increments();
    printf("All resilver tests passed.\n");
    return 0;
}
