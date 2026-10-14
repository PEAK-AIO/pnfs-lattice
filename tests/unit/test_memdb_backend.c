/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_memdb_backend.c -- Contract tests for the in-memory catalogue
 * backend (src/catalogue/catalogue_memdb.c).
 *
 * The backend is the reference implementation of the slot contract
 * (catalogue_internal.h, C1-C7), so these tests attack the properties
 * that contract promises rather than the happy path: concurrent
 * mutators see exactly-once semantics, callbacks may re-enter the same
 * handle, two instances share nothing, a full table changes nothing,
 * READDIR cookies are per dirent (hard-link safe), a layout renewal
 * never shrinks the persisted range, LOCKT sees foreign rows only, the
 * cluster slots implement the Phase 1b registry contract, and the link
 * count saturates at both ends of uint32_t for any int32_t delta.
 */

#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "pnfs_mds.h"
#include "mds_catalogue.h"
#include "mds_coordination.h"
#include "mds_cluster.h"
#include "catalogue_internal.h"
#include "catalogue_memdb.h"
#include "open_state.h"
#include "test_helpers.h"

/* ----------------------------------------------------------------------- */

static int tests_run;
static int tests_passed;
static int test_failed;

#define ASSERT_EQ(a, b) do { \
    long long _a = (long long)(a); \
    long long _b = (long long)(b); \
    if (_a != _b) { \
        fprintf(stderr, "  FAIL %s:%d: %s (%lld) != %s (%lld)\n", \
                __FILE__, __LINE__, #a, _a, #b, _b); \
        test_failed = 1; \
        return; \
    } \
} while (0)

#define ASSERT_TRUE(cond) do { \
    if (!(cond)) { \
        fprintf(stderr, "  FAIL %s:%d: !(%s)\n", __FILE__, __LINE__, #cond); \
        test_failed = 1; \
        return; \
    } \
} while (0)

#define RUN_TEST(fn) do { \
    fprintf(stdout, "  %-52s", #fn); \
    fflush(stdout); \
    tests_run++; \
    test_failed = 0; \
    fn(); \
    if (test_failed == 0) { \
        tests_passed++; \
        fprintf(stdout, "OK\n"); \
    } else { \
        fprintf(stdout, "FAILED\n"); \
    } \
} while (0)

/* Smallest legal READDIR cookie (0, 1, 2 are reserved). */
#define COOKIE_MIN 3U

/* RFC 8881 lock types (lock_state.h drags in the RPC headers). */
#define LT_READ   1U
#define LT_WRITE  2U
#define LT_READW  3U

/* LAYOUTIOMODE4_READ / _RW. */
#define IOMODE_READ 1U
#define IOMODE_RW   2U

/* ----------------------------------------------------------------------- */

static struct nfs4_stateid mk_sid(uint32_t seqid, uint8_t seed)
{
    struct nfs4_stateid sid;

    memset(&sid, 0, sizeof(sid));
    sid.seqid = seqid;
    for (uint32_t i = 0; i < NFS4_OTHER_SIZE; i++) {
        sid.other[i] = (uint8_t)(seed + i);
    }
    return sid;
}

struct count_ctx {
    uint32_t count;
    uint32_t stop_after;   /* 0 = never stop */
};

static int count_readdir_cb(const struct mds_cat_dirent *entry, void *arg)
{
    struct count_ctx *c = arg;

    (void)entry;
    c->count++;
    return (c->stop_after != 0 && c->count >= c->stop_after) ? 1 : 0;
}

static uint32_t dir_count(struct mds_catalogue *cat, uint64_t dir)
{
    struct count_ctx c = { 0, 0 };

    if (mds_cat_ns_readdir(cat, dir, NULL, 0, NULL, count_readdir_cb, &c) != MDS_OK) {
        return UINT32_MAX;
    }
    return c.count;
}

/* -----------------------------------------------------------------------
 * 1. Lifecycle: factory-shaped open, identity, caps, repeated cycles (C7)
 * ----------------------------------------------------------------------- */

static void test_open_cfg_and_identity(void)
{
    struct mds_config cfg;
    struct mds_catalogue *cat = NULL;
    struct mds_catalogue *other = NULL;
    uint64_t fid_a = 0;
    uint64_t fid_b = 0;

    memset(&cfg, 0, sizeof(cfg));
    cfg.catalogue_backend = MDS_BACKEND_MEMDB;

    ASSERT_EQ(catalogue_memdb_open_cfg(NULL, &cat), MDS_ERR_INVAL);
    ASSERT_TRUE(cat == NULL);
    ASSERT_EQ(catalogue_memdb_open_cfg(&cfg, NULL), MDS_ERR_INVAL);
    ASSERT_EQ(catalogue_memdb_open_cfg(&cfg, &cat), MDS_OK);
    ASSERT_TRUE(cat != NULL);

    ASSERT_EQ(mds_catalogue_backend_type(cat), MDS_BACKEND_MEMDB);
    ASSERT_EQ(cat->caps, (uint32_t)MDS_CAT_CAP_SHARED_AUTHORITY);
    ASSERT_TRUE((cat->caps & MDS_CAT_CAP_MULTI_PROCESS) == 0);
    ASSERT_TRUE(mds_catalogue_shared_authority(cat));
    ASSERT_TRUE(cat->cluster_ops != NULL);
    ASSERT_EQ(mds_cluster_supported(cat), false);
    ASSERT_EQ(mds_catalogue_bootstrap_supported(cat), false);
    ASSERT_EQ(mds_catalogue_bootstrap(cat), MDS_ERR_NOSUPPORT);
    ASSERT_TRUE(mds_catalogue_backend_handle(cat) == NULL);
    ASSERT_EQ(mds_catalogue_probe(cat), MDS_OK);
    ASSERT_TRUE(mds_coord_shared_state_supported(cat));

    /* Coordination slots RonDB populates are all present here too,
     * plus the two RonDB leaves NULL. */
    ASSERT_TRUE(cat->coord_ops->layout_grant_union != NULL);
    ASSERT_TRUE(cat->coord_ops->lock_test != NULL);
    ASSERT_TRUE(cat->coord_ops->lock_scan_owner != NULL);
    ASSERT_TRUE(cat->coord_ops->layoutget_fused == NULL);

    /* alloc_fileid is monotonic per instance and independent across
     * instances. */
    other = catalogue_memdb_open();
    ASSERT_TRUE(other != NULL);
    ASSERT_EQ(mds_cat_alloc_fileid(cat, NULL, &fid_a), MDS_OK);
    ASSERT_EQ(mds_cat_alloc_fileid(cat, NULL, &fid_b), MDS_OK);
    ASSERT_EQ(fid_b, fid_a + 1);
    ASSERT_EQ(mds_cat_alloc_fileid(other, NULL, &fid_b), MDS_OK);
    ASSERT_EQ(fid_b, fid_a);
    mds_catalogue_close(other);
    mds_catalogue_close(cat);

    /* Repeated open/close cycles release everything (checked under
     * valgrind by the QA gate). */
    for (int i = 0; i < 16; i++) {
        cat = catalogue_memdb_open();
        ASSERT_TRUE(cat != NULL);
        mds_catalogue_close(cat);
    }
    mds_catalogue_close(NULL);
}

/* -----------------------------------------------------------------------
 * 2. Two instances in one process share no state
 * ----------------------------------------------------------------------- */

static int node_seen_cb(uint32_t mds_id, uint64_t boot_epoch, const char *hostname,
                        uint16_t nfs_port, uint16_t grpc_port,
                        uint64_t last_heartbeat_ns, void *ctx)
{
    uint32_t *n = ctx;

    (void)mds_id; (void)boot_epoch; (void)hostname; (void)nfs_port;
    (void)grpc_port; (void)last_heartbeat_ns;
    (*n)++;
    return 0;
}

static int partition_seen_cb(uint32_t partition_id, uint32_t owner_mds_id,
                             uint8_t state, const char *subtree_path, void *ctx)
{
    uint32_t *n = ctx;

    (void)partition_id; (void)owner_mds_id; (void)state; (void)subtree_path;
    (*n)++;
    return 0;
}

static void test_two_instances_isolated(void)
{
    struct mds_catalogue *a = catalogue_memdb_open();
    struct mds_catalogue *b = catalogue_memdb_open();
    struct mds_inode child;
    struct mds_inode got;
    uint32_t shard = 0;
    uint32_t owner = 0;
    uint64_t target = 0;
    uint32_t n = 0;

    ASSERT_TRUE(a != NULL && b != NULL);

    ASSERT_EQ(mds_cat_ns_create(a, NULL, MDS_FILEID_ROOT, "only-in-a", MDS_FTYPE_REG,
                                0644, 0, 0, NULL, &child), MDS_OK);
    ASSERT_EQ(mds_cat_ns_lookup(b, MDS_FILEID_ROOT, "only-in-a", &got), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_ns_getattr(b, child.fileid, &got), MDS_ERR_NOTFOUND);
    ASSERT_EQ(dir_count(a, MDS_FILEID_ROOT), 1U);
    ASSERT_EQ(dir_count(b, MDS_FILEID_ROOT), 0U);

    /* The former process-global tables are per instance now. */
    ASSERT_EQ(mds_cat_shard_fileid_put(a, NULL, child.fileid, 7), MDS_OK);
    ASSERT_EQ(mds_cat_shard_fileid_get(b, child.fileid, &shard), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_shard_fileid_get(a, child.fileid, &shard), MDS_OK);
    ASSERT_EQ(shard, 7U);
    ASSERT_EQ(mds_cat_ext_dirent_put(a, NULL, MDS_FILEID_ROOT, "ext", 1, child.fileid,
                                     (uint8_t)MDS_FTYPE_REG, 99), MDS_OK);
    ASSERT_EQ(mds_cat_ext_dirent_get(b, MDS_FILEID_ROOT, "ext", &owner, &target, NULL,
                                     NULL), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_ext_dirent_get(a, MDS_FILEID_ROOT, "ext", &owner, &target, NULL,
                                     NULL), MDS_OK);
    ASSERT_EQ(target, child.fileid);

    /* Cluster tables too. */
    ASSERT_EQ(mds_cluster_node_register(a, 1, 10, "a.local", 2049, 9401), MDS_OK);
    ASSERT_EQ(mds_cluster_partition_put(a, 0, 1, MDS_PARTITION_STATE_ACTIVE, "/", true),
              MDS_OK);
    n = 0;
    ASSERT_EQ(mds_cluster_node_list(b, node_seen_cb, &n), MDS_OK);
    ASSERT_EQ(n, 0U);
    ASSERT_EQ(mds_cluster_partition_list(b, partition_seen_cb, &n), MDS_OK);
    ASSERT_EQ(n, 0U);
    ASSERT_EQ(mds_cluster_node_heartbeat(b, 1, 10), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cluster_node_heartbeat(a, 1, 10), MDS_OK);

    /* Closing one instance leaves the other intact. */
    mds_catalogue_close(b);
    ASSERT_EQ(mds_cat_ns_lookup(a, MDS_FILEID_ROOT, "only-in-a", &got), MDS_OK);
    mds_catalogue_close(a);
}

/* -----------------------------------------------------------------------
 * 3. Concurrency: distinct names, the same name, setattr without lost
 *    updates.  Every worker records its outcomes; the test thread does
 *    the asserting.
 * ----------------------------------------------------------------------- */

#define CONC_THREADS 8
#define CONC_OPS     64

struct conc_worker {
    struct mds_catalogue *cat;
    pthread_barrier_t    *barrier;
    uint32_t              id;
    uint64_t              fileid;      /* setattr target */
    uint32_t              ok;
    uint32_t              exists;
    uint32_t              other;
};

static void *create_distinct_worker(void *arg)
{
    struct conc_worker *w = arg;
    char name[64];

    (void)pthread_barrier_wait(w->barrier);
    for (uint32_t i = 0; i < CONC_OPS; i++) {
        struct mds_inode out;
        enum mds_status st;

        (void)snprintf(name, sizeof(name), "t%u-f%u", w->id, i);
        st = mds_cat_ns_create(w->cat, NULL, MDS_FILEID_ROOT, name, MDS_FTYPE_REG,
                               0644, 0, 0, NULL, &out);
        if (st == MDS_OK) {
            w->ok++;
        } else if (st == MDS_ERR_EXISTS) {
            w->exists++;
        } else {
            w->other++;
        }
    }
    return NULL;
}

static void *create_same_worker(void *arg)
{
    struct conc_worker *w = arg;
    struct mds_inode out;
    enum mds_status st;

    (void)pthread_barrier_wait(w->barrier);
    st = mds_cat_ns_create(w->cat, NULL, MDS_FILEID_ROOT, "same-name", MDS_FTYPE_REG,
                           0644, 0, 0, NULL, &out);
    if (st == MDS_OK) {
        w->ok++;
    } else if (st == MDS_ERR_EXISTS) {
        w->exists++;
    } else {
        w->other++;
    }
    return NULL;
}

static void *setattr_worker(void *arg)
{
    struct conc_worker *w = arg;
    struct mds_inode attrs;

    memset(&attrs, 0, sizeof(attrs));
    (void)pthread_barrier_wait(w->barrier);
    for (uint32_t i = 0; i < CONC_OPS; i++) {
        attrs.mode = 0600U + (w->id & 7U);
        if (mds_cat_ns_setattr(w->cat, NULL, w->fileid, &attrs, MDS_ATTR_MODE) == MDS_OK &&
            mds_cat_ns_nlink_adjust(w->cat, w->fileid, 1) == MDS_OK) {
            w->ok++;
        } else {
            w->other++;
        }
    }
    return NULL;
}

static int run_workers(struct mds_catalogue *cat, void *(*fn)(void *),
                       uint64_t fileid, struct conc_worker *ws)
{
    pthread_t th[CONC_THREADS];
    pthread_barrier_t barrier;
    int rc = 0;

    if (pthread_barrier_init(&barrier, NULL, CONC_THREADS) != 0) {
        return -1;
    }
    for (uint32_t i = 0; i < CONC_THREADS; i++) {
        memset(&ws[i], 0, sizeof(ws[i]));
        ws[i].cat = cat;
        ws[i].barrier = &barrier;
        ws[i].id = i;
        ws[i].fileid = fileid;
        if (pthread_create(&th[i], NULL, fn, &ws[i]) != 0) {
            rc = -1;
            /* Release the workers already started so they can exit. */
            for (uint32_t j = i; j < CONC_THREADS; j++) {
                (void)pthread_barrier_wait(&barrier);
            }
            for (uint32_t j = 0; j < i; j++) {
                (void)pthread_join(th[j], NULL);
            }
            (void)pthread_barrier_destroy(&barrier);
            return rc;
        }
    }
    for (uint32_t i = 0; i < CONC_THREADS; i++) {
        (void)pthread_join(th[i], NULL);
    }
    (void)pthread_barrier_destroy(&barrier);
    return rc;
}

static void test_concurrent_creates_distinct_names(void)
{
    struct mds_catalogue *cat = catalogue_memdb_open();
    struct conc_worker ws[CONC_THREADS];
    struct mds_inode root_before;
    struct mds_inode root_after;
    uint32_t ok = 0;

    ASSERT_TRUE(cat != NULL);
    ASSERT_EQ(mds_cat_ns_getattr(cat, MDS_FILEID_ROOT, &root_before), MDS_OK);
    ASSERT_EQ(run_workers(cat, create_distinct_worker, 0, ws), 0);
    for (uint32_t i = 0; i < CONC_THREADS; i++) {
        ok += ws[i].ok;
        ASSERT_EQ(ws[i].exists, 0U);
        ASSERT_EQ(ws[i].other, 0U);
    }
    ASSERT_EQ(ok, (uint32_t)CONC_THREADS * CONC_OPS);
    ASSERT_EQ(dir_count(cat, MDS_FILEID_ROOT), (uint32_t)CONC_THREADS * CONC_OPS);
    /* One parent bump per committed create, none lost. */
    ASSERT_EQ(mds_cat_ns_getattr(cat, MDS_FILEID_ROOT, &root_after), MDS_OK);
    ASSERT_EQ(root_after.change, root_before.change + CONC_THREADS * CONC_OPS);
    mds_catalogue_close(cat);
}

static void test_concurrent_creates_same_name(void)
{
    struct mds_catalogue *cat = catalogue_memdb_open();
    struct conc_worker ws[CONC_THREADS];
    struct mds_inode root_before;
    struct mds_inode root_after;
    struct mds_inode got;
    uint32_t ok = 0;
    uint32_t exists = 0;

    ASSERT_TRUE(cat != NULL);
    ASSERT_EQ(mds_cat_ns_getattr(cat, MDS_FILEID_ROOT, &root_before), MDS_OK);
    ASSERT_EQ(run_workers(cat, create_same_worker, 0, ws), 0);
    for (uint32_t i = 0; i < CONC_THREADS; i++) {
        ok += ws[i].ok;
        exists += ws[i].exists;
        ASSERT_EQ(ws[i].other, 0U);
    }
    ASSERT_EQ(ok, 1U);
    ASSERT_EQ(exists, (uint32_t)CONC_THREADS - 1U);
    ASSERT_EQ(dir_count(cat, MDS_FILEID_ROOT), 1U);
    ASSERT_EQ(mds_cat_ns_lookup(cat, MDS_FILEID_ROOT, "same-name", &got), MDS_OK);
    ASSERT_EQ(mds_cat_ns_getattr(cat, MDS_FILEID_ROOT, &root_after), MDS_OK);
    ASSERT_EQ(root_after.change, root_before.change + 1);
    ASSERT_EQ(root_after.nlink, root_before.nlink); /* a file, not a dir */
    mds_catalogue_close(cat);
}

static void test_concurrent_setattr_no_lost_update(void)
{
    struct mds_catalogue *cat = catalogue_memdb_open();
    struct conc_worker ws[CONC_THREADS];
    struct mds_inode f;
    struct mds_inode got;
    uint32_t ok = 0;

    ASSERT_TRUE(cat != NULL);
    ASSERT_EQ(mds_cat_ns_create(cat, NULL, MDS_FILEID_ROOT, "attr", MDS_FTYPE_REG,
                                0644, 0, 0, NULL, &f), MDS_OK);
    ASSERT_EQ(run_workers(cat, setattr_worker, f.fileid, ws), 0);
    for (uint32_t i = 0; i < CONC_THREADS; i++) {
        ok += ws[i].ok;
        ASSERT_EQ(ws[i].other, 0U);
    }
    ASSERT_EQ(ok, (uint32_t)CONC_THREADS * CONC_OPS);
    ASSERT_EQ(mds_cat_ns_getattr(cat, f.fileid, &got), MDS_OK);
    /* Every setattr bumped change exactly once and every nlink_adjust
     * landed: a lost update would show a smaller count. */
    ASSERT_EQ(got.change, f.change + CONC_THREADS * CONC_OPS);
    ASSERT_EQ(got.nlink, f.nlink + CONC_THREADS * CONC_OPS);
    mds_catalogue_close(cat);
}

/* -----------------------------------------------------------------------
 * 4. Re-entrancy (C1): callbacks call back into the same handle
 * ----------------------------------------------------------------------- */

struct reenter_layout_ctx {
    struct mds_catalogue *cat;
    uint32_t hits;
    uint32_t lookups_ok;
    uint64_t seen_offset;
};

/* The layout_recall byte-range collector shape: iter_file callback
 * looks the row up by stateid on the same handle. */
static int reenter_layout_cb(uint64_t clientid, const struct nfs4_stateid *stateid,
                             uint32_t iomode, void *arg)
{
    struct reenter_layout_ctx *c = arg;
    uint64_t cid = 0;
    uint64_t fid = 0;
    uint32_t mode = 0;
    uint64_t off = 0;
    uint64_t len = 0;
    uint32_t seq = 0;

    (void)iomode;
    c->hits++;
    if (mds_coord_layout_get_by_stateid(c->cat, stateid->other, &cid, &fid, &mode,
                                        &off, &len, &seq) == MDS_OK && cid == clientid) {
        c->lookups_ok++;
        c->seen_offset = off;
    }
    return 0;
}

struct reenter_readdir_ctx {
    struct mds_catalogue *cat;
    uint64_t dir;
    uint32_t entries;
    uint32_t getattr_ok;
    bool created;
};

/* readdir callback that reads AND mutates the directory being listed. */
static int reenter_readdir_cb(const struct mds_cat_dirent *entry, void *arg)
{
    struct reenter_readdir_ctx *c = arg;
    struct mds_inode ino;

    c->entries++;
    if (mds_cat_ns_getattr(c->cat, entry->fileid, &ino) == MDS_OK) {
        c->getattr_ok++;
    }
    if (!c->created) {
        struct mds_inode out;

        c->created = true;
        (void)mds_cat_ns_create(c->cat, NULL, c->dir, "made-during-scan", MDS_FTYPE_REG,
                                0644, 0, 0, NULL, &out);
    }
    return 0;
}

static int reenter_plus_cb(const struct mds_cat_dirent *entry, const struct mds_inode *inode,
                           bool inode_valid, void *arg)
{
    uint32_t *valid = arg;

    (void)entry;
    if (inode_valid && inode != NULL) {
        (*valid)++;
    }
    return 0;
}

struct reenter_node_ctx {
    struct mds_catalogue *cat;
    enum mds_status hb_st;
};

static int reenter_node_cb(uint32_t mds_id, uint64_t boot_epoch, const char *hostname,
                           uint16_t nfs_port, uint16_t grpc_port,
                           uint64_t last_heartbeat_ns, void *arg)
{
    struct reenter_node_ctx *c = arg;

    (void)hostname; (void)nfs_port; (void)grpc_port; (void)last_heartbeat_ns;
    c->hb_st = mds_cluster_node_heartbeat(c->cat, mds_id, boot_epoch);
    return 1; /* stop after the first row */
}

struct reenter_xattr_ctx {
    struct mds_catalogue *cat;
    uint64_t fileid;
    uint32_t names;
    uint32_t values_ok;
};

static int reenter_xattr_cb(const char *name, size_t name_len, void *arg)
{
    struct reenter_xattr_ctx *c = arg;
    void *val = NULL;
    uint32_t vallen = 0;

    (void)name_len;
    c->names++;
    if (mds_cat_xattr_get(c->cat, c->fileid, name, &val, &vallen) == MDS_OK) {
        c->values_ok++;
        free(val);
    }
    return 0;
}

static void test_callbacks_reenter_same_handle(void)
{
    struct mds_catalogue *cat = catalogue_memdb_open();
    struct reenter_layout_ctx lc;
    struct reenter_readdir_ctx rc;
    struct reenter_node_ctx nc;
    struct reenter_xattr_ctx xc;
    struct nfs4_stateid sid = mk_sid(1, 0x40);
    struct mds_inode dir;
    struct mds_inode f;
    struct count_ctx cc;
    uint32_t ds_ids[1] = { 3 };
    uint32_t valid = 0;

    ASSERT_TRUE(cat != NULL);

    /* layout_iter_file -> layout_get_by_stateid (layout_recall.c). */
    ASSERT_EQ(mds_coord_layout_grant(cat, NULL, 77, 900, IOMODE_RW, 4096, 8192, &sid,
                                     ds_ids, 1), MDS_OK);
    memset(&lc, 0, sizeof(lc));
    lc.cat = cat;
    ASSERT_EQ(mds_coord_layout_iter_file(cat, 900, reenter_layout_cb, &lc), MDS_OK);
    ASSERT_EQ(lc.hits, 1U);
    ASSERT_EQ(lc.lookups_ok, 1U);
    ASSERT_EQ(lc.seen_offset, 4096U);

    /* ns_readdir -> ns_getattr + ns_create on the directory being read. */
    ASSERT_EQ(mds_cat_ns_create(cat, NULL, MDS_FILEID_ROOT, "d", MDS_FTYPE_DIR, 0755,
                                0, 0, NULL, &dir), MDS_OK);
    for (uint32_t i = 0; i < 3; i++) {
        char name[16];

        (void)snprintf(name, sizeof(name), "e%u", i);
        ASSERT_EQ(mds_cat_ns_create(cat, NULL, dir.fileid, name, MDS_FTYPE_REG, 0644,
                                    0, 0, NULL, &f), MDS_OK);
    }
    memset(&rc, 0, sizeof(rc));
    rc.cat = cat;
    rc.dir = dir.fileid;
    ASSERT_EQ(mds_cat_ns_readdir(cat, dir.fileid, NULL, 0, NULL, reenter_readdir_cb, &rc),
              MDS_OK);
    ASSERT_TRUE(rc.entries >= 3);
    ASSERT_EQ(rc.getattr_ok, rc.entries);
    ASSERT_EQ(dir_count(cat, dir.fileid), 4U);

    /* The dispatcher's readdir_plus fallback calls ns_getattr from inside
     * the ns_readdir callback. */
    ASSERT_EQ(mds_cat_ns_readdir_plus(cat, dir.fileid, NULL, 0, NULL, reenter_plus_cb,
                                      &valid), MDS_OK);
    ASSERT_EQ(valid, 4U);

    /* Early stop: a non-zero return ends delivery. */
    cc.count = 0;
    cc.stop_after = 2;
    ASSERT_EQ(mds_cat_ns_readdir(cat, dir.fileid, NULL, 0, NULL, count_readdir_cb, &cc),
              MDS_OK);
    ASSERT_EQ(cc.count, 2U);

    /* node_list -> node_heartbeat. */
    ASSERT_EQ(mds_cluster_node_register(cat, 5, 1, "n5", 2049, 9401), MDS_OK);
    nc.cat = cat;
    nc.hb_st = MDS_ERR_IO;
    ASSERT_EQ(mds_cluster_node_list(cat, reenter_node_cb, &nc), MDS_OK);
    ASSERT_EQ(nc.hb_st, MDS_OK);

    /* xattr_list -> xattr_get. */
    ASSERT_EQ(mds_cat_xattr_put(cat, NULL, f.fileid, "user.a", "1", 1), MDS_OK);
    ASSERT_EQ(mds_cat_xattr_put(cat, NULL, f.fileid, "user.b", "22", 2), MDS_OK);
    memset(&xc, 0, sizeof(xc));
    xc.cat = cat;
    xc.fileid = f.fileid;
    ASSERT_EQ(mds_cat_xattr_list(cat, f.fileid, reenter_xattr_cb, &xc), MDS_OK);
    ASSERT_EQ(xc.names, 2U);
    ASSERT_EQ(xc.values_ok, 2U);

    mds_catalogue_close(cat);
}

/* -----------------------------------------------------------------------
 * 5. Capacity: a full table changes nothing (atomic failure)
 * ----------------------------------------------------------------------- */

#define FILL_LIMIT 100000U

static void test_capacity_exhaustion_is_atomic(void)
{
    struct mds_catalogue *cat = catalogue_memdb_open();
    struct mds_inode probe;
    struct mds_inode root_before;
    struct mds_inode root_after;
    struct mds_inode out;
    struct mds_inode target;
    uint64_t fid_before = 0;
    uint64_t fid_after = 0;
    uint32_t dirents_before;
    uint32_t i;
    enum mds_status st = MDS_OK;

    ASSERT_TRUE(cat != NULL);
    ASSERT_EQ(mds_cat_ns_create(cat, NULL, MDS_FILEID_ROOT, "victim", MDS_FTYPE_REG,
                                0644, 0, 0, NULL, &target), MDS_OK);

    /* Fill the inode table with raw rows until it refuses. */
    memset(&probe, 0, sizeof(probe));
    probe.type = MDS_FTYPE_REG;
    probe.nlink = 1;
    for (i = 0; i < FILL_LIMIT; i++) {
        probe.fileid = 1000000ULL + i;
        st = mds_cat_inode_put(cat, NULL, &probe);
        if (st != MDS_OK) {
            break;
        }
    }
    ASSERT_EQ(st, MDS_ERR_NOSPC);
    ASSERT_TRUE(i < FILL_LIMIT);

    ASSERT_EQ(mds_cat_ns_getattr(cat, MDS_FILEID_ROOT, &root_before), MDS_OK);
    dirents_before = dir_count(cat, MDS_FILEID_ROOT);
    ASSERT_EQ(mds_cat_alloc_fileid(cat, NULL, &fid_before), MDS_OK);

    /* CREATE needs an inode row: refused, and nothing else moved. */
    ASSERT_EQ(mds_cat_ns_create(cat, NULL, MDS_FILEID_ROOT, "wontfit", MDS_FTYPE_DIR,
                                0755, 0, 0, NULL, &out), MDS_ERR_NOSPC);
    ASSERT_EQ(mds_cat_ns_getattr(cat, MDS_FILEID_ROOT, &root_after), MDS_OK);
    ASSERT_EQ(root_after.nlink, root_before.nlink);
    ASSERT_EQ(root_after.change, root_before.change);
    ASSERT_EQ(root_after.mtime.tv_sec, root_before.mtime.tv_sec);
    ASSERT_EQ(root_after.mtime.tv_nsec, root_before.mtime.tv_nsec);
    ASSERT_EQ(root_after.ctime.tv_nsec, root_before.ctime.tv_nsec);
    ASSERT_EQ(mds_cat_ns_lookup(cat, MDS_FILEID_ROOT, "wontfit", &out), MDS_ERR_NOTFOUND);
    ASSERT_EQ(dir_count(cat, MDS_FILEID_ROOT), dirents_before);
    /* The fileid sequence did not burn an id on the refused create. */
    ASSERT_EQ(mds_cat_alloc_fileid(cat, NULL, &fid_after), MDS_OK);
    ASSERT_EQ(fid_after, fid_before + 1);

    /* Same for the wide create: no inode row, no dirent, no stripe map. */
    {
        struct mds_inode wide;
        struct mds_ds_map_entry ent;
        bool discard = false;

        memset(&wide, 0, sizeof(wide));
        wide.fileid = fid_after + 1;
        wide.parent_fileid = MDS_FILEID_ROOT;
        wide.type = MDS_FTYPE_REG;
        wide.nlink = 1;
        memset(&ent, 0, sizeof(ent));
        ent.ds_id = 1;
        ent.nfs_fh_len = 1;
        ASSERT_EQ(mds_cat_ns_create_wide(cat, MDS_FILEID_ROOT, "wide", &wide, 1, 65536, 1,
                                         &ent, &discard), MDS_ERR_NOSPC);
        ASSERT_EQ(discard, true);
        ASSERT_EQ(mds_cat_ns_lookup(cat, MDS_FILEID_ROOT, "wide", &out), MDS_ERR_NOTFOUND);
        ASSERT_EQ(mds_cat_stripe_map_get(cat, wide.fileid, NULL, NULL, NULL, NULL),
                  MDS_ERR_NOTFOUND);
    }

    /* Free one inode row: the same create now succeeds (the table was
     * the only thing in the way). */
    ASSERT_EQ(mds_cat_inode_del(cat, NULL, 1000000ULL), MDS_OK);
    ASSERT_EQ(mds_cat_ns_create(cat, NULL, MDS_FILEID_ROOT, "fits", MDS_FTYPE_REG, 0644,
                                0, 0, NULL, &out), MDS_OK);
    mds_catalogue_close(cat);

    /* Dirent table: LINK refused with the target's nlink untouched. */
    cat = catalogue_memdb_open();
    ASSERT_TRUE(cat != NULL);
    ASSERT_EQ(mds_cat_ns_create(cat, NULL, MDS_FILEID_ROOT, "victim", MDS_FTYPE_REG,
                                0644, 0, 0, NULL, &target), MDS_OK);
    for (i = 0; i < FILL_LIMIT; i++) {
        char name[32];

        (void)snprintf(name, sizeof(name), "pad%u", i);
        st = mds_cat_dirent_put(cat, NULL, MDS_FILEID_ROOT, name, target.fileid,
                                (uint8_t)MDS_FTYPE_REG);
        if (st != MDS_OK) {
            break;
        }
    }
    ASSERT_EQ(st, MDS_ERR_NOSPC);
    ASSERT_EQ(mds_cat_ns_getattr(cat, MDS_FILEID_ROOT, &root_before), MDS_OK);
    ASSERT_EQ(mds_cat_ns_link(cat, NULL, MDS_FILEID_ROOT, "link", target.fileid),
              MDS_ERR_NOSPC);
    ASSERT_EQ(mds_cat_ns_getattr(cat, target.fileid, &out), MDS_OK);
    ASSERT_EQ(out.nlink, 1U);
    ASSERT_EQ(out.change, target.change);
    ASSERT_EQ(mds_cat_ns_getattr(cat, MDS_FILEID_ROOT, &root_after), MDS_OK);
    ASSERT_EQ(root_after.change, root_before.change);
    ASSERT_EQ(mds_cat_ns_lookup(cat, MDS_FILEID_ROOT, "link", &out), MDS_ERR_NOTFOUND);
    mds_catalogue_close(cat);
}

/* -----------------------------------------------------------------------
 * 6. READDIR cookies: per dirent, hard-link safe, deleted-cursor safe
 * ----------------------------------------------------------------------- */

struct page_ctx {
    uint32_t count;
    uint64_t cookie[8];
    uint64_t fileid[8];
    char     name[8][MDS_MAX_NAME + 1];
};

static int page_cb(const struct mds_cat_dirent *entry, const struct mds_inode *inode,
                   bool inode_valid, void *arg)
{
    struct page_ctx *p = arg;

    (void)inode;
    (void)inode_valid;
    if (p->count < 8) {
        p->cookie[p->count] = entry->cookie;
        p->fileid[p->count] = entry->fileid;
        (void)snprintf(p->name[p->count], sizeof(p->name[p->count]), "%s", entry->name);
    }
    p->count++;
    return 0;
}

static int page_plain_cb(const struct mds_cat_dirent *entry, void *arg)
{
    return page_cb(entry, NULL, false, arg);
}

static void test_cookies_hard_link_paging(void)
{
    struct mds_catalogue *cat = catalogue_memdb_open();
    struct mds_inode dir;
    struct mds_inode f;
    struct page_ctx p1;
    struct page_ctx p2;
    struct page_ctx p3;
    struct page_ctx all;

    ASSERT_TRUE(cat != NULL);
    ASSERT_EQ(mds_cat_ns_create(cat, NULL, MDS_FILEID_ROOT, "hl", MDS_FTYPE_DIR, 0755,
                                0, 0, NULL, &dir), MDS_OK);
    ASSERT_EQ(mds_cat_ns_create(cat, NULL, dir.fileid, "a", MDS_FTYPE_REG, 0644, 0, 0,
                                NULL, &f), MDS_OK);
    ASSERT_EQ(mds_cat_ns_link(cat, NULL, dir.fileid, "b", f.fileid), MDS_OK);
    ASSERT_EQ(mds_cat_ns_getattr(cat, f.fileid, &f), MDS_OK);
    ASSERT_EQ(f.nlink, 2U);

    /* Name-order slot: two entries, same fileid, distinct legal cookies. */
    memset(&all, 0, sizeof(all));
    ASSERT_EQ(mds_cat_ns_readdir(cat, dir.fileid, NULL, 0, NULL, page_plain_cb, &all),
              MDS_OK);
    ASSERT_EQ(all.count, 2U);
    ASSERT_EQ(all.fileid[0], f.fileid);
    ASSERT_EQ(all.fileid[1], f.fileid);
    ASSERT_TRUE(all.cookie[0] >= COOKIE_MIN && all.cookie[1] >= COOKIE_MIN);
    ASSERT_TRUE(all.cookie[0] != all.cookie[1]);
    ASSERT_EQ(strcmp(all.name[0], "a"), 0);
    ASSERT_EQ(strcmp(all.name[1], "b"), 0);

    /* Cookie-resume slot, page size 1: both names exactly once. */
    memset(&p1, 0, sizeof(p1));
    ASSERT_EQ(mds_cat_ns_readdir_plus_from_cookie(cat, dir.fileid, 0, 1, NULL, page_cb,
                                                  &p1), MDS_OK);
    ASSERT_EQ(p1.count, 1U);
    memset(&p2, 0, sizeof(p2));
    ASSERT_EQ(mds_cat_ns_readdir_plus_from_cookie(cat, dir.fileid, p1.cookie[0], 1, NULL,
                                                  page_cb, &p2), MDS_OK);
    ASSERT_EQ(p2.count, 1U);
    ASSERT_TRUE(p2.cookie[0] > p1.cookie[0]);
    ASSERT_TRUE(strcmp(p1.name[0], p2.name[0]) != 0);
    ASSERT_EQ(p1.fileid[0], f.fileid);
    ASSERT_EQ(p2.fileid[0], f.fileid);
    memset(&p3, 0, sizeof(p3));
    ASSERT_EQ(mds_cat_ns_readdir_plus_from_cookie(cat, dir.fileid, p2.cookie[0], 1, NULL,
                                                  page_cb, &p3), MDS_OK);
    ASSERT_EQ(p3.count, 0U);

    /* Deleted cursor: remove the entry whose cookie is the cursor; the
     * resume still delivers exactly the entries after it. */
    ASSERT_EQ(mds_cat_ns_create(cat, NULL, dir.fileid, "c", MDS_FTYPE_REG, 0644, 0, 0,
                                NULL, &f), MDS_OK);
    ASSERT_EQ(mds_cat_ns_create(cat, NULL, dir.fileid, "d", MDS_FTYPE_REG, 0644, 0, 0,
                                NULL, &f), MDS_OK);
    /* Entries now: a, b (link), c, d in cookie order.  Cursor = b. */
    ASSERT_EQ(mds_cat_ns_remove(cat, NULL, dir.fileid, p2.name[0]), MDS_OK);
    memset(&p3, 0, sizeof(p3));
    ASSERT_EQ(mds_cat_ns_readdir_plus_from_cookie(cat, dir.fileid, p2.cookie[0], 0, NULL,
                                                  page_cb, &p3), MDS_OK);
    ASSERT_EQ(p3.count, 2U);
    ASSERT_EQ(strcmp(p3.name[0], "c"), 0);
    ASSERT_EQ(strcmp(p3.name[1], "d"), 0);
    ASSERT_TRUE(p3.cookie[0] > p2.cookie[0]);
    ASSERT_TRUE(p3.cookie[1] > p3.cookie[0]);

    /* A rebinding is a new dirent: rename gives the name a new cookie,
     * and the old cookie never comes back. */
    ASSERT_EQ(mds_cat_ns_rename(cat, NULL, dir.fileid, "c", dir.fileid, "cc"), MDS_OK);
    memset(&all, 0, sizeof(all));
    ASSERT_EQ(mds_cat_ns_readdir_plus_from_cookie(cat, dir.fileid, p3.cookie[1], 0, NULL,
                                                  page_cb, &all), MDS_OK);
    ASSERT_EQ(all.count, 1U);
    ASSERT_EQ(strcmp(all.name[0], "cc"), 0);
    ASSERT_TRUE(all.cookie[0] > p3.cookie[1]);

    mds_catalogue_close(cat);
}

/* -----------------------------------------------------------------------
 * 7. Layout renewal union never shrinks the persisted range
 * ----------------------------------------------------------------------- */

struct idx_ctx {
    uint32_t hits;
    uint64_t clientid;
    uint64_t fileid;
};

static int idx_cb(uint64_t clientid, uint64_t fileid, void *arg)
{
    struct idx_ctx *c = arg;

    c->hits++;
    c->clientid = clientid;
    c->fileid = fileid;
    return 0;
}

static void test_layout_grant_union_widens(void)
{
    struct mds_catalogue *cat = catalogue_memdb_open();
    struct nfs4_stateid sid = mk_sid(1, 0x50);
    struct nfs4_stateid fresh = mk_sid(4, 0x60);
    struct idx_ctx ic;
    uint32_t ds7[1] = { 7 };
    uint32_t ds9[1] = { 9 };
    uint64_t cid = 0;
    uint64_t fid = 0;
    uint32_t iomode = 0;
    uint64_t off = 0;
    uint64_t len = 0;
    uint32_t seqid = 0;

    ASSERT_TRUE(cat != NULL);
    ASSERT_EQ(mds_coord_layout_grant(cat, NULL, 11, 700, IOMODE_READ, 0, 1024, &sid,
                                     ds7, 1), MDS_OK);

    /* Disjoint later window: the row becomes the covering range. */
    sid.seqid = 3;
    ASSERT_EQ(mds_coord_layout_grant_union(cat, NULL, 11, 700, IOMODE_READ, 4096, 1024,
                                           &sid, ds9, 1), MDS_OK);
    ASSERT_EQ(mds_coord_layout_get_by_stateid(cat, sid.other, &cid, &fid, &iomode, &off,
                                              &len, &seqid), MDS_OK);
    ASSERT_EQ(off, 0U);
    ASSERT_EQ(len, 5120U);
    ASSERT_EQ(seqid, 3U);
    ASSERT_EQ(iomode, IOMODE_READ);

    /* An inner window changes nothing; a lower seqid does not regress. */
    sid.seqid = 2;
    ASSERT_EQ(mds_coord_layout_grant_union(cat, NULL, 11, 700, IOMODE_READ, 100, 10, &sid,
                                           ds7, 1), MDS_OK);
    ASSERT_EQ(mds_coord_layout_get_by_stateid(cat, sid.other, &cid, &fid, &iomode, &off,
                                              &len, &seqid), MDS_OK);
    ASSERT_EQ(off, 0U);
    ASSERT_EQ(len, 5120U);
    ASSERT_EQ(seqid, 3U);

    /* RW dominates and sticks. */
    sid.seqid = 5;
    ASSERT_EQ(mds_coord_layout_grant_union(cat, NULL, 11, 700, IOMODE_RW, 8192, 16, &sid,
                                           ds7, 1), MDS_OK);
    ASSERT_EQ(mds_coord_layout_get_by_stateid(cat, sid.other, &cid, &fid, &iomode, &off,
                                              &len, &seqid), MDS_OK);
    ASSERT_EQ(iomode, IOMODE_RW);
    ASSERT_EQ(len, 8208U);
    sid.seqid = 6;
    ASSERT_EQ(mds_coord_layout_grant_union(cat, NULL, 11, 700, IOMODE_READ, 0, 8, &sid,
                                           ds7, 1), MDS_OK);
    ASSERT_EQ(mds_coord_layout_get_by_stateid(cat, sid.other, &cid, &fid, &iomode, &off,
                                              &len, &seqid), MDS_OK);
    ASSERT_EQ(iomode, IOMODE_RW);
    ASSERT_EQ(seqid, 6U);

    /* The to-EOF sentinel dominates. */
    ASSERT_EQ(mds_coord_layout_grant_union(cat, NULL, 11, 700, IOMODE_RW, 1 << 20,
                                           UINT64_MAX, &sid, ds7, 1), MDS_OK);
    ASSERT_EQ(mds_coord_layout_get_by_stateid(cat, sid.other, &cid, &fid, &iomode, &off,
                                              &len, &seqid), MDS_OK);
    ASSERT_EQ(off, 0U);
    ASSERT_TRUE(len == UINT64_MAX);

    /* The renewal left the DS index as the first grant wrote it (the
     * RonDB union touches no index rows): still on DS 7, never on DS 9. */
    memset(&ic, 0, sizeof(ic));
    ASSERT_EQ(mds_coord_ds_layout_idx_scan(cat, 7, idx_cb, &ic), MDS_OK);
    ASSERT_EQ(ic.hits, 1U);
    ASSERT_EQ(ic.clientid, 11U);
    ASSERT_EQ(ic.fileid, 700U);
    memset(&ic, 0, sizeof(ic));
    ASSERT_EQ(mds_coord_ds_layout_idx_scan(cat, 9, idx_cb, &ic), MDS_OK);
    ASSERT_EQ(ic.hits, 0U);

    /* Union on an absent stateid is a full insert, index included. */
    ASSERT_EQ(mds_coord_layout_grant_union(cat, NULL, 12, 701, IOMODE_READ, 10, 20, &fresh,
                                           ds9, 1), MDS_OK);
    ASSERT_EQ(mds_coord_layout_get_by_stateid(cat, fresh.other, &cid, &fid, &iomode, &off,
                                              &len, &seqid), MDS_OK);
    ASSERT_EQ(cid, 12U);
    ASSERT_EQ(off, 10U);
    ASSERT_EQ(len, 20U);
    ASSERT_EQ(seqid, 4U);
    memset(&ic, 0, sizeof(ic));
    ASSERT_EQ(mds_coord_ds_layout_idx_scan(cat, 9, idx_cb, &ic), MDS_OK);
    ASSERT_EQ(ic.hits, 1U);

    /* The plain grant is the overwrite the union exists to avoid:
     * it DOES narrow the row (documented, and why the dispatcher no
     * longer substitutes it for a missing union slot). */
    ASSERT_EQ(mds_coord_layout_grant(cat, NULL, 11, 700, IOMODE_READ, 0, 16, &sid, ds7, 1),
              MDS_OK);
    ASSERT_EQ(mds_coord_layout_get_by_stateid(cat, sid.other, &cid, &fid, &iomode, &off,
                                              &len, &seqid), MDS_OK);
    ASSERT_EQ(len, 16U);

    /* LAYOUTRETURN keyed by (fileid, stateid); a second return is NOTFOUND. */
    ASSERT_EQ(mds_coord_layout_return(cat, NULL, sid.other, 11, 700, ds7, 1), MDS_OK);
    ASSERT_EQ(mds_coord_layout_return(cat, NULL, sid.other, 11, 700, ds7, 1),
              MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_coord_layout_get_by_stateid(cat, sid.other, NULL, NULL, NULL, NULL, NULL,
                                              NULL), MDS_ERR_NOTFOUND);
    /* fileid 0 (FREE_STATEID) matches on the stateid alone. */
    ASSERT_EQ(mds_coord_layout_return(cat, NULL, fresh.other, 12, 0, NULL, 0), MDS_OK);
    mds_catalogue_close(cat);
}

/* The dispatcher no longer substitutes a plain (narrowing) grant for a
 * missing union slot: NULL slot means MDS_ERR_NOSUPPORT and layout_grant
 * is never called (C5). */
static int g_fake_grants;

static enum mds_status fake_layout_grant(struct mds_catalogue *cat, struct mds_cat_txn *txn,
    uint64_t clientid, uint64_t fileid, uint32_t iomode, uint64_t offset, uint64_t length,
    const struct nfs4_stateid *stateid, const uint32_t *ds_ids, uint32_t ds_count)
{
    (void)cat; (void)txn; (void)clientid; (void)fileid; (void)iomode; (void)offset;
    (void)length; (void)stateid; (void)ds_ids; (void)ds_count;
    g_fake_grants++;
    return MDS_OK;
}

static void test_dispatcher_union_has_no_overwrite_fallback(void)
{
    struct mds_catalogue cat;
    struct mds_coordination_ops ops;
    struct nfs4_stateid sid = mk_sid(1, 0x70);

    memset(&cat, 0, sizeof(cat));
    memset(&ops, 0, sizeof(ops));
    ops.layout_grant = fake_layout_grant;
    ops.layout_grant_union = NULL;
    cat.coord_ops = &ops;
    g_fake_grants = 0;
    ASSERT_EQ(mds_coord_layout_grant_union(&cat, NULL, 1, 2, IOMODE_RW, 0, 4096, &sid,
                                           NULL, 0), MDS_ERR_NOSUPPORT);
    ASSERT_EQ(g_fake_grants, 0);
}

/* -----------------------------------------------------------------------
 * 8. LOCKT against foreign rows; owner scan; reap
 * ----------------------------------------------------------------------- */

struct lock_scan_ctx {
    uint32_t hits;
    uint64_t lock_ids[8];
};

static int lock_scan_cb(const struct mds_coord_lock_row *row, void *arg)
{
    struct lock_scan_ctx *c = arg;

    if (c->hits < 8) {
        c->lock_ids[c->hits] = row->lock_id;
    }
    c->hits++;
    return 0;
}

static struct mds_coord_lock_row mk_lock(uint64_t fileid, uint64_t lock_id, uint32_t type,
                                         uint64_t off, uint64_t len, uint64_t clientid,
                                         const char *owner)
{
    struct mds_coord_lock_row r;

    memset(&r, 0, sizeof(r));
    r.fileid = fileid;
    r.lock_id = lock_id;
    r.lock_type = type;
    r.offset = off;
    r.length = len;
    r.clientid = clientid;
    r.owner_len = (uint32_t)strlen(owner);
    memcpy(r.owner, owner, r.owner_len);
    return r;
}

static void test_lock_test_and_scan_owner(void)
{
    struct mds_catalogue *cat = catalogue_memdb_open();
    struct mds_coord_lock_row row = mk_lock(7, 1, LT_WRITE, 0, 100, 1, "o1");
    struct mds_coord_lock_row conflict;
    struct lock_scan_ctx sc;
    const uint8_t *o1 = (const uint8_t *)"o1";
    const uint8_t *o2 = (const uint8_t *)"o2";

    ASSERT_TRUE(cat != NULL);
    ASSERT_EQ(mds_coord_lock_put(cat, &row), MDS_OK);
    row = mk_lock(7, 2, LT_READ, 1000, 0, 1, "o1");   /* to EOF */
    ASSERT_EQ(mds_coord_lock_put(cat, &row), MDS_OK);
    row = mk_lock(8, 3, LT_READ, 0, 10, 1, "o1");
    ASSERT_EQ(mds_coord_lock_put(cat, &row), MDS_OK);
    row = mk_lock(7, 4, LT_READ, 500, 10, 2, "o2");
    ASSERT_EQ(mds_coord_lock_put(cat, &row), MDS_OK);

    /* Foreign WRITE overlapping the request: conflict, row reported. */
    ASSERT_EQ(mds_coord_lock_test(cat, 7, LT_READ, 50, 10, 2, o2, 2, &conflict),
              MDS_ERR_EXISTS);
    ASSERT_EQ(conflict.lock_id, 1U);
    ASSERT_EQ(conflict.clientid, 1U);
    /* The holder itself never conflicts with its own rows. */
    ASSERT_EQ(mds_coord_lock_test(cat, 7, LT_WRITE, 50, 10, 1, o1, 2, &conflict), MDS_OK);
    ASSERT_EQ(conflict.lock_id, 0U);
    /* Disjoint range. */
    ASSERT_EQ(mds_coord_lock_test(cat, 7, LT_WRITE, 200, 100, 2, o2, 2, &conflict), MDS_OK);
    /* Read vs read (blocking variant normalises to READ). */
    ASSERT_EQ(mds_coord_lock_test(cat, 7, LT_READW, 2000, 10, 2, o2, 2, &conflict), MDS_OK);
    /* Write vs the to-EOF read: conflict. */
    ASSERT_EQ(mds_coord_lock_test(cat, 7, LT_WRITE, 5000, 1, 2, o2, 2, &conflict),
              MDS_ERR_EXISTS);
    ASSERT_EQ(conflict.lock_id, 2U);
    /* Same client, different owner: still foreign. */
    ASSERT_EQ(mds_coord_lock_test(cat, 7, LT_WRITE, 505, 1, 1, o2, 2, &conflict),
              MDS_ERR_EXISTS);
    ASSERT_EQ(conflict.lock_id, 4U);
    /* Other file. */
    ASSERT_EQ(mds_coord_lock_test(cat, 9, LT_WRITE, 0, 0, 2, o2, 2, &conflict), MDS_OK);
    /* Argument checks. */
    ASSERT_EQ(mds_coord_lock_test(cat, 7, LT_WRITE, 0, 0, 2, NULL, 2, &conflict),
              MDS_ERR_INVAL);
    ASSERT_EQ(mds_coord_lock_test(cat, 7, LT_WRITE, 0, 0, 2, o2, 2, NULL), MDS_ERR_INVAL);

    /* Owner scan: (client 1, "o1") holds rows 1, 2 and 3 across two files. */
    memset(&sc, 0, sizeof(sc));
    ASSERT_EQ(mds_coord_lock_scan_owner(cat, 1, o1, 2, lock_scan_cb, &sc), MDS_OK);
    ASSERT_EQ(sc.hits, 3U);
    memset(&sc, 0, sizeof(sc));
    ASSERT_EQ(mds_coord_lock_scan_owner(cat, 2, o2, 2, lock_scan_cb, &sc), MDS_OK);
    ASSERT_EQ(sc.hits, 1U);
    ASSERT_EQ(sc.lock_ids[0], 4U);
    memset(&sc, 0, sizeof(sc));
    ASSERT_EQ(mds_coord_lock_scan_owner(cat, 1, o2, 2, lock_scan_cb, &sc), MDS_OK);
    ASSERT_EQ(sc.hits, 0U);
    memset(&sc, 0, sizeof(sc));
    ASSERT_EQ(mds_coord_lock_scan_file(cat, 7, lock_scan_cb, &sc), MDS_OK);
    ASSERT_EQ(sc.hits, 3U);

    /* Upsert keyed (fileid, lock_id); delete; reap. */
    row = mk_lock(7, 1, LT_READ, 0, 100, 1, "o1");
    ASSERT_EQ(mds_coord_lock_put(cat, &row), MDS_OK);
    ASSERT_EQ(mds_coord_lock_test(cat, 7, LT_READ, 50, 10, 2, o2, 2, &conflict), MDS_OK);
    ASSERT_EQ(mds_coord_lock_del(cat, 7, 1), MDS_OK);
    ASSERT_EQ(mds_coord_lock_del(cat, 7, 1), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_coord_lock_reap_client(cat, 1), MDS_OK);
    memset(&sc, 0, sizeof(sc));
    ASSERT_EQ(mds_coord_lock_scan_owner(cat, 1, o1, 2, lock_scan_cb, &sc), MDS_OK);
    ASSERT_EQ(sc.hits, 0U);
    memset(&sc, 0, sizeof(sc));
    ASSERT_EQ(mds_coord_lock_scan_file(cat, 7, lock_scan_cb, &sc), MDS_OK);
    ASSERT_EQ(sc.hits, 1U);
    ASSERT_EQ(sc.lock_ids[0], 4U);
    mds_catalogue_close(cat);
}

/* -----------------------------------------------------------------------
 * 9. Cluster ops: the Phase 1b registry / partition contract
 * ----------------------------------------------------------------------- */

struct node_row {
    uint32_t mds_id;
    uint64_t boot_epoch;
    uint64_t last_heartbeat_ns;
    uint16_t nfs_port;
    char     hostname[64];
};

struct node_list_ctx {
    uint32_t hits;
    struct node_row rows[4];
};

static int node_list_cb(uint32_t mds_id, uint64_t boot_epoch, const char *hostname,
                        uint16_t nfs_port, uint16_t grpc_port,
                        uint64_t last_heartbeat_ns, void *arg)
{
    struct node_list_ctx *c = arg;

    (void)grpc_port;
    if (c->hits < 4) {
        c->rows[c->hits].mds_id = mds_id;
        c->rows[c->hits].boot_epoch = boot_epoch;
        c->rows[c->hits].last_heartbeat_ns = last_heartbeat_ns;
        c->rows[c->hits].nfs_port = nfs_port;
        (void)snprintf(c->rows[c->hits].hostname, sizeof(c->rows[c->hits].hostname), "%s",
                       hostname);
    }
    c->hits++;
    return 0;
}

static int stale_cb(uint32_t mds_id, uint64_t boot_epoch, uint64_t last_heartbeat_ns,
                    void *arg)
{
    struct node_list_ctx *c = arg;

    if (c->hits < 4) {
        c->rows[c->hits].mds_id = mds_id;
        c->rows[c->hits].boot_epoch = boot_epoch;
        c->rows[c->hits].last_heartbeat_ns = last_heartbeat_ns;
    }
    c->hits++;
    return 0;
}

struct part_row {
    uint32_t id;
    uint32_t owner;
    uint8_t  state;
    char     path[64];
};

struct part_list_ctx {
    uint32_t hits;
    struct part_row rows[4];
};

static int part_list_cb(uint32_t partition_id, uint32_t owner_mds_id, uint8_t state,
                        const char *subtree_path, void *arg)
{
    struct part_list_ctx *c = arg;

    if (c->hits < 4) {
        c->rows[c->hits].id = partition_id;
        c->rows[c->hits].owner = owner_mds_id;
        c->rows[c->hits].state = state;
        (void)snprintf(c->rows[c->hits].path, sizeof(c->rows[c->hits].path), "%s",
                       subtree_path);
    }
    c->hits++;
    return 0;
}

static void test_cluster_registry_contract(void)
{
    struct mds_catalogue *cat = catalogue_memdb_open();
    struct node_list_ctx nl;
    struct part_list_ctx pl;
    uint64_t hb1;

    ASSERT_TRUE(cat != NULL);

    ASSERT_EQ(mds_cluster_node_register(cat, 1, 10, "mds1", 2049, 9401), MDS_OK);
    /* Duplicate live registration (equal epoch) and an older incarnation
     * are refused; the row is untouched. */
    ASSERT_EQ(mds_cluster_node_register(cat, 1, 10, "impostor", 1, 1), MDS_ERR_EXISTS);
    ASSERT_EQ(mds_cluster_node_register(cat, 1, 9, "older", 1, 1), MDS_ERR_EXISTS);
    memset(&nl, 0, sizeof(nl));
    ASSERT_EQ(mds_cluster_node_list(cat, node_list_cb, &nl), MDS_OK);
    ASSERT_EQ(nl.hits, 1U);
    ASSERT_EQ(nl.rows[0].boot_epoch, 10U);
    ASSERT_EQ(nl.rows[0].nfs_port, 2049U);
    ASSERT_EQ(strcmp(nl.rows[0].hostname, "mds1"), 0);
    ASSERT_TRUE(nl.rows[0].last_heartbeat_ns != 0);
    hb1 = nl.rows[0].last_heartbeat_ns;

    /* Old-epoch heartbeat: STALE and the row is unchanged. */
    ASSERT_EQ(mds_cluster_node_heartbeat(cat, 1, 9), MDS_ERR_STALE);
    memset(&nl, 0, sizeof(nl));
    ASSERT_EQ(mds_cluster_node_list(cat, node_list_cb, &nl), MDS_OK);
    ASSERT_EQ(nl.rows[0].boot_epoch, 10U);
    ASSERT_EQ(nl.rows[0].last_heartbeat_ns, hb1);
    /* Matching epoch: the timestamp advances (CLOCK_REALTIME ns). */
    ASSERT_EQ(mds_cluster_node_heartbeat(cat, 1, 10), MDS_OK);
    memset(&nl, 0, sizeof(nl));
    ASSERT_EQ(mds_cluster_node_list(cat, node_list_cb, &nl), MDS_OK);
    ASSERT_TRUE(nl.rows[0].last_heartbeat_ns >= hb1);
    /* Unknown node: NOTFOUND passes through unchanged. */
    ASSERT_EQ(mds_cluster_node_heartbeat(cat, 2, 1), MDS_ERR_NOTFOUND);

    /* A newer incarnation replaces the row (restart with a higher epoch). */
    ASSERT_EQ(mds_cluster_node_register(cat, 1, 11, "mds1b", 2050, 9402), MDS_OK);
    memset(&nl, 0, sizeof(nl));
    ASSERT_EQ(mds_cluster_node_list(cat, node_list_cb, &nl), MDS_OK);
    ASSERT_EQ(nl.hits, 1U);
    ASSERT_EQ(nl.rows[0].boot_epoch, 11U);
    ASSERT_EQ(strcmp(nl.rows[0].hostname, "mds1b"), 0);
    /* The old incarnation can neither heartbeat nor deregister it. */
    ASSERT_EQ(mds_cluster_node_heartbeat(cat, 1, 10), MDS_ERR_STALE);
    ASSERT_EQ(mds_cluster_node_deregister(cat, 1, 10), MDS_ERR_STALE);
    memset(&nl, 0, sizeof(nl));
    ASSERT_EQ(mds_cluster_node_list(cat, node_list_cb, &nl), MDS_OK);
    ASSERT_EQ(nl.hits, 1U);

    /* Stale scan: everything is older than a threshold in the future,
     * nothing is older than the epoch. */
    memset(&nl, 0, sizeof(nl));
    ASSERT_EQ(mds_cluster_node_scan_stale(cat, UINT64_MAX, stale_cb, &nl), MDS_OK);
    ASSERT_EQ(nl.hits, 1U);
    ASSERT_EQ(nl.rows[0].mds_id, 1U);
    ASSERT_EQ(nl.rows[0].boot_epoch, 11U);
    memset(&nl, 0, sizeof(nl));
    ASSERT_EQ(mds_cluster_node_scan_stale(cat, 1, stale_cb, &nl), MDS_OK);
    ASSERT_EQ(nl.hits, 0U);

    /* Deregister: epoch match deletes, a retry of the delete is MDS_OK. */
    ASSERT_EQ(mds_cluster_node_deregister(cat, 1, 11), MDS_OK);
    ASSERT_EQ(mds_cluster_node_deregister(cat, 1, 11), MDS_OK);
    ASSERT_EQ(mds_cluster_node_heartbeat(cat, 1, 11), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cluster_node_register(cat, 1, 11, "back", 2049, 9401), MDS_OK);
    ASSERT_EQ(mds_cluster_node_register(cat, 1, 12, "restarted", 1, 1), MDS_OK);
    ASSERT_EQ(mds_cluster_node_register(cat, 3, 1, NULL, 1, 1), MDS_ERR_INVAL);

    /* Partition map: the insert-only root claim, then upsert seeding. */
    ASSERT_EQ(mds_cluster_partition_put(cat, 0, 1, MDS_PARTITION_STATE_ACTIVE, "/", true),
              MDS_OK);
    ASSERT_EQ(mds_cluster_partition_put(cat, 0, 2, MDS_PARTITION_STATE_ACTIVE, "/", true),
              MDS_ERR_EXISTS);
    memset(&pl, 0, sizeof(pl));
    ASSERT_EQ(mds_cluster_partition_list(cat, part_list_cb, &pl), MDS_OK);
    ASSERT_EQ(pl.hits, 1U);
    ASSERT_EQ(pl.rows[0].owner, 1U);
    ASSERT_EQ(mds_cluster_partition_put(cat, 0, 2, MDS_PARTITION_STATE_MIGRATING, "/",
                                        false), MDS_OK);
    ASSERT_EQ(mds_cluster_partition_put(cat, 1, 2, MDS_PARTITION_STATE_ACTIVE, "/data",
                                        false), MDS_OK);
    memset(&pl, 0, sizeof(pl));
    ASSERT_EQ(mds_cluster_partition_list(cat, part_list_cb, &pl), MDS_OK);
    ASSERT_EQ(pl.hits, 2U);
    ASSERT_EQ(pl.rows[0].id, 0U);
    ASSERT_EQ(pl.rows[0].owner, 2U);
    ASSERT_EQ(pl.rows[0].state, MDS_PARTITION_STATE_MIGRATING);
    ASSERT_EQ(strcmp(pl.rows[1].path, "/data"), 0);
    ASSERT_EQ(mds_cluster_partition_put(cat, 2, 2, MDS_PARTITION_STATE_ACTIVE, NULL, false),
              MDS_ERR_INVAL);

    /* Owner CAS (the failover takeover): the write lands only while the
     * row still records the expected owner; a wrong expectation or an
     * absent row changes nothing. */
    ASSERT_EQ(mds_cluster_partition_cas(cat, 1, 7, 3, MDS_PARTITION_STATE_ACTIVE),
              MDS_ERR_STALE);
    ASSERT_EQ(mds_cluster_partition_cas(cat, 9, 2, 3, MDS_PARTITION_STATE_ACTIVE),
              MDS_ERR_NOTFOUND);
    memset(&pl, 0, sizeof(pl));
    ASSERT_EQ(mds_cluster_partition_list(cat, part_list_cb, &pl), MDS_OK);
    ASSERT_EQ(pl.hits, 2U);
    ASSERT_EQ(pl.rows[1].owner, 2U);
    ASSERT_EQ(mds_cluster_partition_cas(cat, 1, 2, 3, MDS_PARTITION_STATE_ACTIVE), MDS_OK);
    memset(&pl, 0, sizeof(pl));
    ASSERT_EQ(mds_cluster_partition_list(cat, part_list_cb, &pl), MDS_OK);
    ASSERT_EQ(pl.rows[1].id, 1U);
    ASSERT_EQ(pl.rows[1].owner, 3U);
    ASSERT_EQ(pl.rows[1].state, MDS_PARTITION_STATE_ACTIVE);
    ASSERT_EQ(strcmp(pl.rows[1].path, "/data"), 0);
    /* A replay of the same CAS is refused: the row no longer records
     * the expected owner. */
    ASSERT_EQ(mds_cluster_partition_cas(cat, 1, 2, 3, MDS_PARTITION_STATE_ACTIVE),
              MDS_ERR_STALE);
    ASSERT_EQ(mds_cluster_partition_cas(NULL, 1, 2, 3, MDS_PARTITION_STATE_ACTIVE),
              MDS_ERR_INVAL);

    /* Populated slots never make an in-process store a cluster store. */
    ASSERT_EQ(mds_cluster_supported(cat), false);
    mds_catalogue_close(cat);
}

/* -----------------------------------------------------------------------
 * 10. Coordination tables round-trip (the slots RonDB write through to)
 * ----------------------------------------------------------------------- */

struct open_scan_ctx {
    uint32_t hits;
};

static int open_scan_cb(const struct mds_coord_open_row *row, void *arg)
{
    struct open_scan_ctx *c = arg;

    (void)row;
    c->hits++;
    return 0;
}

static int deleg_scan_cb(const struct mds_coord_deleg_row *row, void *arg)
{
    uint32_t *n = arg;

    (void)row;
    (*n)++;
    return 0;
}

static int session_scan_cb(const struct mds_coord_session_row *row, void *arg)
{
    uint32_t *n = arg;

    (void)row;
    (*n)++;
    return 0;
}

struct journal_scan_ctx {
    uint32_t hits;
    uint8_t  last_state;
};

static int journal_scan_cb(const struct mds_coord_journal_record *rec, void *arg)
{
    struct journal_scan_ctx *c = arg;

    c->hits++;
    c->last_state = rec->state;
    return 0;
}

static int recovery_list_cb(uint64_t clientid, uint32_t owner_mds_id,
                            uint64_t owner_boot_epoch, void *arg)
{
    uint32_t *n = arg;

    (void)clientid; (void)owner_mds_id; (void)owner_boot_epoch;
    (*n)++;
    return 0;
}

static void test_coordination_tables_round_trip(void)
{
    struct mds_catalogue *cat = catalogue_memdb_open();
    struct mds_coord_open_row orow;
    struct mds_coord_open_row oget;
    struct mds_coord_deleg_row drow;
    struct mds_coord_deleg_row dget;
    struct mds_coord_client_row crow;
    struct mds_coord_client_row cget;
    struct mds_coord_session_row srow;
    struct mds_coord_session_row sget;
    struct mds_coord_drc_slot_row slot;
    struct mds_coord_journal_record jrec;
    struct mds_coord_journal_record jget;
    struct journal_scan_ctx jc;
    struct open_scan_ctx oc;
    uint8_t sid_a[12];
    uint8_t sid_b[12];
    uint8_t sess[16];
    uint8_t owner_out[16];
    uint32_t owner_len = 0;
    uint8_t verf[8] = { 1, 2, 3, 4, 5, 6, 7, 8 };
    uint8_t verf_out[8];
    uint32_t n = 0;

    ASSERT_TRUE(cat != NULL);
    memset(sid_a, 0xA1, sizeof(sid_a));
    memset(sid_b, 0xB2, sizeof(sid_b));
    memset(sess, 0x5E, sizeof(sess));

    /* Open state: upsert on stateid, both scans, NOTFOUND after delete. */
    memset(&orow, 0, sizeof(orow));
    memcpy(orow.stateid_other, sid_a, 12);
    orow.seqid = 1;
    orow.clientid = 100;
    orow.fileid = 500;
    orow.share_access = 3;
    ASSERT_EQ(mds_coord_open_put(cat, &orow), MDS_OK);
    orow.seqid = 2;
    ASSERT_EQ(mds_coord_open_put(cat, &orow), MDS_OK);
    memcpy(orow.stateid_other, sid_b, 12);
    orow.clientid = 101;
    ASSERT_EQ(mds_coord_open_put(cat, &orow), MDS_OK);
    ASSERT_EQ(mds_coord_open_get(cat, sid_a, &oget), MDS_OK);
    ASSERT_EQ(oget.seqid, 2U);
    ASSERT_EQ(oget.clientid, 100U);
    memset(&oc, 0, sizeof(oc));
    ASSERT_EQ(mds_coord_open_scan_file(cat, 500, open_scan_cb, &oc), MDS_OK);
    ASSERT_EQ(oc.hits, 2U);
    memset(&oc, 0, sizeof(oc));
    ASSERT_EQ(mds_coord_open_scan_client(cat, 101, open_scan_cb, &oc), MDS_OK);
    ASSERT_EQ(oc.hits, 1U);
    ASSERT_EQ(mds_coord_open_del(cat, sid_a), MDS_OK);
    ASSERT_EQ(mds_coord_open_del(cat, sid_a), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_coord_open_get(cat, sid_a, &oget), MDS_ERR_NOTFOUND);

    /* Delegations. */
    memset(&drow, 0, sizeof(drow));
    memcpy(drow.stateid_other, sid_a, 12);
    drow.clientid = 100;
    drow.fileid = 500;
    drow.deleg_type = 1;
    ASSERT_EQ(mds_coord_deleg_put(cat, &drow), MDS_OK);
    ASSERT_EQ(mds_coord_deleg_get(cat, sid_a, &dget), MDS_OK);
    ASSERT_EQ(dget.deleg_type, 1U);
    n = 0;
    ASSERT_EQ(mds_coord_deleg_scan_file(cat, 500, deleg_scan_cb, &n), MDS_OK);
    ASSERT_EQ(n, 1U);
    n = 0;
    ASSERT_EQ(mds_coord_deleg_scan_client(cat, 999, deleg_scan_cb, &n), MDS_OK);
    ASSERT_EQ(n, 0U);
    ASSERT_EQ(mds_coord_deleg_del(cat, sid_a), MDS_OK);
    ASSERT_EQ(mds_coord_deleg_del(cat, sid_a), MDS_ERR_NOTFOUND);

    /* Clients. */
    memset(&crow, 0, sizeof(crow));
    crow.clientid = 100;
    crow.co_ownerid_len = 3;
    memcpy(crow.co_ownerid, "abc", 3);
    crow.confirmed = true;
    ASSERT_EQ(mds_coord_client_put(cat, &crow), MDS_OK);
    ASSERT_EQ(mds_coord_client_get(cat, 100, &cget), MDS_OK);
    ASSERT_EQ(cget.confirmed, true);
    ASSERT_EQ(memcmp(cget.co_ownerid, "abc", 3), 0);
    ASSERT_EQ(mds_coord_client_del(cat, 100), MDS_OK);
    ASSERT_EQ(mds_coord_client_get(cat, 100, &cget), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_coord_client_del(cat, 100), MDS_ERR_NOTFOUND);

    /* Sessions. */
    memset(&srow, 0, sizeof(srow));
    memcpy(srow.session_id, sess, 16);
    srow.clientid = 100;
    srow.num_slots = 8;
    ASSERT_EQ(mds_coord_session_put(cat, &srow), MDS_OK);
    ASSERT_EQ(mds_coord_session_get(cat, sess, &sget), MDS_OK);
    ASSERT_EQ(sget.num_slots, 8U);
    n = 0;
    ASSERT_EQ(mds_coord_session_scan_client(cat, 100, session_scan_cb, &n), MDS_OK);
    ASSERT_EQ(n, 1U);
    ASSERT_EQ(mds_coord_session_del(cat, sess), MDS_OK);
    ASSERT_EQ(mds_coord_session_del(cat, sess), MDS_ERR_NOTFOUND);

    /* DRC slots: the cached reply comes back as a heap copy. */
    ASSERT_EQ(mds_coord_slot_put(cat, sess, 3, 7, "reply", 5), MDS_OK);
    memset(&slot, 0, sizeof(slot));
    ASSERT_EQ(mds_coord_slot_get(cat, sess, 3, &slot), MDS_OK);
    ASSERT_EQ(slot.seq_id, 7U);
    ASSERT_EQ(slot.reply_len, 5U);
    ASSERT_TRUE(slot.cached_reply != NULL);
    ASSERT_EQ(memcmp(slot.cached_reply, "reply", 5), 0);
    free(slot.cached_reply);
    ASSERT_EQ(mds_coord_slot_put(cat, sess, 3, 8, NULL, 0), MDS_OK);
    ASSERT_EQ(mds_coord_slot_get(cat, sess, 3, &slot), MDS_OK);
    ASSERT_EQ(slot.seq_id, 8U);
    ASSERT_EQ(slot.reply_len, 0U);
    ASSERT_TRUE(slot.cached_reply == NULL);
    ASSERT_EQ(mds_coord_slot_get(cat, sess, 4, &slot), MDS_ERR_NOTFOUND);

    /* Recovery: upsert, idempotent delete, list visible to any owner. */
    ASSERT_EQ(mds_coord_recovery_put(cat, NULL, 100, (const uint8_t *)"own", 3, verf),
              MDS_OK);
    ASSERT_EQ(mds_coord_recovery_put(cat, NULL, 100, (const uint8_t *)"own2", 4, verf),
              MDS_OK);
    ASSERT_EQ(mds_coord_recovery_get(cat, 100, owner_out, &owner_len, verf_out), MDS_OK);
    ASSERT_EQ(owner_len, 4U);
    ASSERT_EQ(memcmp(owner_out, "own2", 4), 0);
    ASSERT_EQ(memcmp(verf_out, verf, 8), 0);
    n = 0;
    ASSERT_EQ(mds_coord_recovery_list(cat, 42, recovery_list_cb, &n), MDS_OK);
    ASSERT_EQ(n, 1U);
    ASSERT_EQ(mds_coord_recovery_del(cat, NULL, 100), MDS_OK);
    ASSERT_EQ(mds_coord_recovery_del(cat, NULL, 100), MDS_OK);
    ASSERT_EQ(mds_coord_recovery_get(cat, 100, owner_out, &owner_len, verf_out),
              MDS_ERR_NOTFOUND);

    /* Journal: upsert on (txn_id, role); scan is oldest-first. */
    memset(&jrec, 0, sizeof(jrec));
    jrec.txn_id = 9;
    jrec.role = 0;
    jrec.state = 1;
    jrec.created_at_ns = 100;
    ASSERT_EQ(mds_coord_journal_put(cat, NULL, &jrec), MDS_OK);
    jrec.role = 1;
    jrec.created_at_ns = 200;
    ASSERT_EQ(mds_coord_journal_put(cat, NULL, &jrec), MDS_OK);
    jrec.role = 0;
    jrec.state = 2;
    jrec.created_at_ns = 300;
    ASSERT_EQ(mds_coord_journal_put(cat, NULL, &jrec), MDS_OK);
    ASSERT_EQ(mds_coord_journal_get(cat, NULL, 9, 0, &jget), MDS_OK);
    ASSERT_EQ(jget.state, 2U);
    memset(&jc, 0, sizeof(jc));
    ASSERT_EQ(mds_coord_journal_scan(cat, journal_scan_cb, &jc), MDS_OK);
    ASSERT_EQ(jc.hits, 2U);
    ASSERT_EQ(jc.last_state, 2U);
    ASSERT_EQ(mds_coord_journal_del(cat, NULL, 9, 0), MDS_OK);
    ASSERT_EQ(mds_coord_journal_del(cat, NULL, 9, 0), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_coord_journal_get(cat, NULL, 9, 1, &jget), MDS_OK);
    mds_catalogue_close(cat);
}

/* -----------------------------------------------------------------------
 * 11. Namespace semantics the reference backend pins
 * ----------------------------------------------------------------------- */

static void test_namespace_semantics(void)
{
    struct mds_catalogue *cat = catalogue_memdb_open();
    struct mds_inode d1;
    struct mds_inode d2;
    struct mds_inode f1;
    struct mds_inode f2;
    struct mds_inode got;
    struct mds_inode root;
    struct mds_inode attrs;
    struct page_ctx pg;
    uint64_t root_change;
    uint32_t inline_len = 0;
    bool empty = false;

    ASSERT_TRUE(cat != NULL);
    ASSERT_EQ(mds_cat_ns_getattr(cat, MDS_FILEID_ROOT, &root), MDS_OK);
    ASSERT_EQ(root.nlink, 2U);

    /* Directories: parent nlink follows the ".." links. */
    ASSERT_EQ(mds_cat_ns_create(cat, NULL, MDS_FILEID_ROOT, "d1", MDS_FTYPE_DIR, 0755, 0,
                                0, NULL, &d1), MDS_OK);
    ASSERT_EQ(mds_cat_ns_create(cat, NULL, MDS_FILEID_ROOT, "d2", MDS_FTYPE_DIR, 0755, 0,
                                0, NULL, &d2), MDS_OK);
    ASSERT_EQ(mds_cat_ns_getattr(cat, MDS_FILEID_ROOT, &root), MDS_OK);
    ASSERT_EQ(root.nlink, 4U);
    ASSERT_EQ(mds_cat_ns_create(cat, NULL, 424242, "x", MDS_FTYPE_REG, 0644, 0, 0, NULL,
                                &f1), MDS_ERR_NOTFOUND);

    /* Files under d1; a name collision is EXISTS; create under a file is
     * NOTDIR. */
    ASSERT_EQ(mds_cat_ns_create(cat, NULL, d1.fileid, "f1", MDS_FTYPE_REG, 0644, 0, 0,
                                NULL, &f1), MDS_OK);
    ASSERT_EQ(mds_cat_ns_create(cat, NULL, d1.fileid, "f1", MDS_FTYPE_REG, 0644, 0, 0,
                                NULL, &f2), MDS_ERR_EXISTS);
    ASSERT_EQ(mds_cat_ns_create(cat, NULL, f1.fileid, "sub", MDS_FTYPE_REG, 0644, 0, 0,
                                NULL, &f2), MDS_ERR_NOTDIR);
    ASSERT_EQ(mds_cat_ns_create(cat, NULL, d1.fileid, "f2", MDS_FTYPE_REG, 0644, 0, 0,
                                NULL, &f2), MDS_OK);
    ASSERT_EQ(mds_cat_ns_link(cat, NULL, d1.fileid, "f1", f2.fileid), MDS_ERR_EXISTS);
    ASSERT_EQ(mds_cat_ns_link(cat, NULL, d1.fileid, "dlink", d2.fileid), MDS_ERR_ISDIR);
    ASSERT_EQ(mds_cat_ns_link(cat, NULL, f1.fileid, "l", f2.fileid), MDS_ERR_NOTDIR);

    /* RMDIR of a non-empty directory is refused inside the mutation. */
    ASSERT_EQ(mds_cat_ns_remove(cat, NULL, MDS_FILEID_ROOT, "d1"), MDS_ERR_NOTEMPTY);
    ASSERT_EQ(mds_cat_ns_lookup(cat, MDS_FILEID_ROOT, "d1", &got), MDS_OK);
    ASSERT_EQ(mds_cat_dir_is_empty(cat, d2.fileid, &empty), MDS_OK);
    ASSERT_EQ(empty, true);

    /* Rename over: a REG victim's last link is deleted; dir onto a
     * non-empty dir is NOTEMPTY; non-dir onto dir is ISDIR; dir onto
     * non-dir is NOTDIR; both names on one inode is a no-op. */
    ASSERT_EQ(mds_cat_ns_rename(cat, NULL, d1.fileid, "f2", d1.fileid, "f1"), MDS_OK);
    ASSERT_EQ(mds_cat_ns_getattr(cat, f1.fileid, &got), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_ns_lookup(cat, d1.fileid, "f1", &got), MDS_OK);
    ASSERT_EQ(got.fileid, f2.fileid);
    ASSERT_EQ(mds_cat_ns_lookup(cat, d1.fileid, "f2", &got), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_ns_rename(cat, NULL, MDS_FILEID_ROOT, "d2", MDS_FILEID_ROOT, "d1"),
              MDS_ERR_NOTEMPTY);
    ASSERT_EQ(mds_cat_ns_rename(cat, NULL, d1.fileid, "f1", MDS_FILEID_ROOT, "d2"),
              MDS_ERR_ISDIR);
    ASSERT_EQ(mds_cat_ns_rename(cat, NULL, MDS_FILEID_ROOT, "d2", d1.fileid, "f1"),
              MDS_ERR_NOTDIR);
    ASSERT_EQ(mds_cat_ns_link(cat, NULL, d1.fileid, "f1b", f2.fileid), MDS_OK);
    ASSERT_EQ(mds_cat_ns_getattr(cat, d1.fileid, &d1), MDS_OK);
    root_change = d1.change;
    ASSERT_EQ(mds_cat_ns_rename(cat, NULL, d1.fileid, "f1", d1.fileid, "f1b"), MDS_OK);
    ASSERT_EQ(mds_cat_ns_getattr(cat, d1.fileid, &got), MDS_OK);
    ASSERT_EQ(got.change, root_change);           /* no-op: nothing bumped */
    ASSERT_EQ(dir_count(cat, d1.fileid), 2U);      /* both names still there */

    /* Cross-directory move of a directory carries the ".." link and
     * touches both parents. */
    ASSERT_EQ(mds_cat_ns_getattr(cat, MDS_FILEID_ROOT, &root), MDS_OK);
    root_change = root.change;
    ASSERT_EQ(mds_cat_ns_rename(cat, NULL, MDS_FILEID_ROOT, "d2", d1.fileid, "d2moved"),
              MDS_OK);
    ASSERT_EQ(mds_cat_ns_getattr(cat, MDS_FILEID_ROOT, &root), MDS_OK);
    ASSERT_EQ(root.nlink, 3U);
    ASSERT_TRUE(root.change > root_change);
    ASSERT_EQ(mds_cat_ns_getattr(cat, d1.fileid, &got), MDS_OK);
    ASSERT_EQ(got.nlink, 3U);
    ASSERT_EQ(mds_cat_ns_getattr(cat, d2.fileid, &got), MDS_OK);
    ASSERT_EQ(got.parent_fileid, d1.fileid);

    /* Keep-orphan rename: the final link survives as UNLINK_ORPHAN. */
    ASSERT_EQ(mds_cat_ns_create(cat, NULL, d1.fileid, "src", MDS_FTYPE_REG, 0644, 0, 0,
                                NULL, &f1), MDS_OK);
    ASSERT_EQ(mds_cat_ns_create(cat, NULL, d1.fileid, "dst", MDS_FTYPE_REG, 0644, 0, 0,
                                NULL, &f2), MDS_OK);
    ASSERT_EQ(mds_cat_ns_rename_flags(cat, NULL, d1.fileid, "src", d1.fileid, "dst",
                                      MDS_CAT_RNF_KEEP_DST_ORPHAN), MDS_OK);
    ASSERT_EQ(mds_cat_ns_getattr(cat, f2.fileid, &got), MDS_OK);
    ASSERT_EQ(got.nlink, 0U);
    ASSERT_TRUE((got.flags & MDS_IFLAG_UNLINK_ORPHAN) != 0);
    ASSERT_EQ(mds_cat_inode_del(cat, NULL, f2.fileid), MDS_OK);
    ASSERT_EQ(mds_cat_inode_del(cat, NULL, f2.fileid), MDS_ERR_NOTFOUND);

    /* Final unlink of a regular file removes inode, inline data, xattrs
     * and stripe map; the parent is touched. */
    ASSERT_EQ(mds_cat_inline_put(cat, NULL, f1.fileid, "data", 4), MDS_OK);
    ASSERT_EQ(mds_cat_xattr_put(cat, NULL, f1.fileid, "user.k", "v", 1), MDS_OK);
    {
        struct mds_ds_map_entry ent;

        memset(&ent, 0, sizeof(ent));
        ent.ds_id = 1;
        ASSERT_EQ(mds_cat_stripe_map_put(cat, NULL, f1.fileid, 1, 65536, 1, &ent), MDS_OK);
    }
    ASSERT_EQ(mds_cat_ns_getattr(cat, d1.fileid, &d1), MDS_OK);
    ASSERT_EQ(mds_cat_ns_remove(cat, NULL, d1.fileid, "dst"), MDS_OK);
    ASSERT_EQ(mds_cat_ns_getattr(cat, f1.fileid, &got), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_inline_get(cat, f1.fileid, NULL, 0, &inline_len), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_xattr_exists(cat, f1.fileid, "user.k"), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_stripe_map_get(cat, f1.fileid, NULL, NULL, NULL, NULL),
              MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_ns_getattr(cat, d1.fileid, &got), MDS_OK);
    ASSERT_EQ(got.change, d1.change + 1);

    /* setattr: grow-only SIZE_EXTEND, FLAGS, ctime/change bump. */
    ASSERT_EQ(mds_cat_ns_getattr(cat, f2.fileid, &got), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_ns_create(cat, NULL, d1.fileid, "sz", MDS_FTYPE_REG, 0644, 0, 0,
                                NULL, &f1), MDS_OK);
    memset(&attrs, 0, sizeof(attrs));
    attrs.size = 4096;
    ASSERT_EQ(mds_cat_ns_setattr(cat, NULL, f1.fileid, &attrs, MDS_ATTR_SIZE), MDS_OK);
    attrs.size = 1024;
    ASSERT_EQ(mds_cat_ns_setattr(cat, NULL, f1.fileid, &attrs, MDS_ATTR_SIZE_EXTEND),
              MDS_OK);
    ASSERT_EQ(mds_cat_ns_getattr(cat, f1.fileid, &got), MDS_OK);
    ASSERT_EQ(got.size, 4096U);
    ASSERT_EQ(got.change, f1.change + 2);
    attrs.size = 8192;
    ASSERT_EQ(mds_cat_ns_setattr(cat, NULL, f1.fileid, &attrs, MDS_ATTR_SIZE_EXTEND),
              MDS_OK);
    ASSERT_EQ(mds_cat_ns_getattr(cat, f1.fileid, &got), MDS_OK);
    ASSERT_EQ(got.size, 8192U);
    ASSERT_EQ(mds_cat_ns_setattr(cat, NULL, 424242, &attrs, MDS_ATTR_SIZE),
              MDS_ERR_NOTFOUND);

    /* Name-order readdir is sorted and honours max_entries. */
    ASSERT_EQ(mds_cat_ns_create(cat, NULL, d1.fileid, "zz", MDS_FTYPE_REG, 0644, 0, 0,
                                NULL, &f2), MDS_OK);
    ASSERT_EQ(mds_cat_ns_create(cat, NULL, d1.fileid, "aa", MDS_FTYPE_REG, 0644, 0, 0,
                                NULL, &f2), MDS_OK);
    /* d1 now holds aa, d2moved, f1, f1b, sz, zz. */
    memset(&pg, 0, sizeof(pg));
    ASSERT_EQ(mds_cat_ns_readdir(cat, d1.fileid, NULL, 0, NULL, page_plain_cb, &pg), MDS_OK);
    ASSERT_EQ(pg.count, 6U);
    ASSERT_EQ(strcmp(pg.name[0], "aa"), 0);
    ASSERT_EQ(strcmp(pg.name[5], "zz"), 0);
    for (uint32_t i = 1; i < pg.count; i++) {
        ASSERT_TRUE(strcmp(pg.name[i - 1], pg.name[i]) < 0);
    }
    memset(&pg, 0, sizeof(pg));
    ASSERT_EQ(mds_cat_ns_readdir(cat, d1.fileid, "f1b", 2, NULL, page_plain_cb, &pg), MDS_OK);
    ASSERT_EQ(pg.count, 2U);
    ASSERT_TRUE(strcmp(pg.name[0], "f1b") > 0);

    /* Directory removal drops the parent's ".." link. */
    ASSERT_EQ(mds_cat_ns_create(cat, NULL, MDS_FILEID_ROOT, "empty", MDS_FTYPE_DIR, 0755,
                                0, 0, NULL, &d2), MDS_OK);
    ASSERT_EQ(mds_cat_ns_getattr(cat, MDS_FILEID_ROOT, &root), MDS_OK);
    ASSERT_EQ(mds_cat_ns_remove(cat, NULL, MDS_FILEID_ROOT, "empty"), MDS_OK);
    ASSERT_EQ(mds_cat_ns_getattr(cat, d2.fileid, &got), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_ns_getattr(cat, MDS_FILEID_ROOT, &got), MDS_OK);
    ASSERT_EQ(got.nlink, root.nlink - 1);
    mds_catalogue_close(cat);
}

/* -----------------------------------------------------------------------
 * 12. GC queue order and the fused remove
 * ----------------------------------------------------------------------- */

static void test_gc_queue_and_fused_remove(void)
{
    struct mds_catalogue *cat = catalogue_memdb_open();
    struct mds_gc_entry entry;
    struct mds_gc_entry batch[4];
    struct mds_ds_map_entry ents[2];
    struct mds_inode f;
    struct mds_inode stale;
    uint32_t n = 0;
    bool folded = false;
    uint8_t fh[2] = { 1, 2 };

    ASSERT_TRUE(cat != NULL);
    ASSERT_EQ(mds_cat_gc_enqueue(cat, NULL, 10, 1, fh, 2), MDS_OK);
    ASSERT_EQ(mds_cat_gc_enqueue(cat, NULL, 11, 1, fh, 2), MDS_OK);
    ASSERT_EQ(mds_cat_gc_enqueue(cat, NULL, 12, 2, fh, 2), MDS_OK);
    ASSERT_EQ(mds_cat_gc_peek(cat, &entry), MDS_OK);
    ASSERT_EQ(entry.fileid, 10U);
    ASSERT_EQ(mds_cat_gc_dequeue(cat, NULL, entry.gc_seq), MDS_OK);
    ASSERT_EQ(mds_cat_gc_dequeue(cat, NULL, entry.gc_seq), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_gc_peek_batch(cat, batch, 4, &n), MDS_OK);
    ASSERT_EQ(n, 2U);
    ASSERT_TRUE(batch[0].gc_seq < batch[1].gc_seq);
    ASSERT_EQ(batch[0].fileid, 11U);
    ASSERT_EQ(mds_cat_gc_count(cat, &n), MDS_OK);
    ASSERT_EQ(n, 2U);

    /* Fused final unlink: the caller's rows land with the remove. */
    ASSERT_EQ(mds_cat_ns_create(cat, NULL, MDS_FILEID_ROOT, "g", MDS_FTYPE_REG, 0644, 0, 0,
                                NULL, &f), MDS_OK);
    memset(ents, 0, sizeof(ents));
    ents[0].ds_id = 5;
    ents[1].ds_id = 6;
    ASSERT_EQ(mds_cat_stripe_map_put(cat, NULL, f.fileid, 2, 65536, 1, ents), MDS_OK);
    stale = f;
    stale.fileid = f.fileid + 100;
    ASSERT_EQ(mds_cat_ns_remove_known_gc(cat, NULL, MDS_FILEID_ROOT, "g", &stale, 2, ents,
                                         2, MDS_GC_SWEEP_GEOM(2, 1), &folded),
              MDS_ERR_STALE);
    ASSERT_EQ(folded, false);
    ASSERT_EQ(mds_cat_ns_lookup(cat, MDS_FILEID_ROOT, "g", &stale), MDS_OK);
    ASSERT_EQ(mds_cat_ns_remove_known_gc(cat, NULL, MDS_FILEID_ROOT, "g", &f, 2, ents, 2,
                                         MDS_GC_SWEEP_GEOM(2, 1), &folded), MDS_OK);
    ASSERT_EQ(folded, true);
    ASSERT_EQ(mds_cat_gc_count(cat, &n), MDS_OK);
    ASSERT_EQ(n, 4U);
    ASSERT_EQ(mds_cat_stripe_map_get(cat, f.fileid, NULL, NULL, NULL, NULL),
              MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_ns_getattr(cat, f.fileid, &stale), MDS_ERR_NOTFOUND);
    mds_catalogue_close(cat);
}

/* -----------------------------------------------------------------------
 * 13. nlink_adjust saturates at both ends of the link count
 * ----------------------------------------------------------------------- */

/* Every int32_t delta is a legal argument, including INT32_MIN, whose
 * negation exists in no int32_t: a delta below -nlink lands on 0 and
 * never wraps, a delta past UINT32_MAX lands on UINT32_MAX, and a delta
 * inside the range is applied exactly. */
static void test_nlink_adjust_saturates(void)
{
    struct mds_catalogue *cat = catalogue_memdb_open();
    struct mds_inode f;
    struct mds_inode got;

    ASSERT_TRUE(cat != NULL);
    ASSERT_EQ(mds_cat_ns_create(cat, NULL, MDS_FILEID_ROOT, "n", MDS_FTYPE_REG,
                                0644, 0, 0, NULL, &f), MDS_OK);

    /* INT32_MIN on a small count clamps to 0. */
    ASSERT_EQ(mds_cat_ns_nlink_adjust(cat, f.fileid, INT32_MIN), MDS_OK);
    ASSERT_EQ(mds_cat_ns_getattr(cat, f.fileid, &got), MDS_OK);
    ASSERT_EQ(got.nlink, 0U);

    /* -1 on 0 stays 0. */
    ASSERT_EQ(mds_cat_ns_nlink_adjust(cat, f.fileid, -1), MDS_OK);
    ASSERT_EQ(mds_cat_ns_getattr(cat, f.fileid, &got), MDS_OK);
    ASSERT_EQ(got.nlink, 0U);

    /* INT32_MAX on a small count is the exact sum. */
    ASSERT_EQ(mds_cat_ns_nlink_adjust(cat, f.fileid, 2), MDS_OK);
    ASSERT_EQ(mds_cat_ns_nlink_adjust(cat, f.fileid, INT32_MAX), MDS_OK);
    ASSERT_EQ(mds_cat_ns_getattr(cat, f.fileid, &got), MDS_OK);
    ASSERT_EQ(got.nlink, (uint32_t)INT32_MAX + 2U);

    /* Past UINT32_MAX saturates instead of wrapping. */
    ASSERT_EQ(mds_cat_ns_nlink_adjust(cat, f.fileid, INT32_MAX), MDS_OK);
    ASSERT_EQ(mds_cat_ns_getattr(cat, f.fileid, &got), MDS_OK);
    ASSERT_EQ(got.nlink, UINT32_MAX);

    /* INT32_MIN from the top is applied exactly, not clamped:
     * UINT32_MAX + INT32_MIN == INT32_MAX. */
    ASSERT_EQ(mds_cat_ns_nlink_adjust(cat, f.fileid, INT32_MIN), MDS_OK);
    ASSERT_EQ(mds_cat_ns_getattr(cat, f.fileid, &got), MDS_OK);
    ASSERT_EQ(got.nlink, (uint32_t)INT32_MAX);

    ASSERT_EQ(mds_cat_ns_nlink_adjust(cat, 424242, 1), MDS_ERR_NOTFOUND);
    mds_catalogue_close(cat);
}

/* ----------------------------------------------------------------------- */

int main(void)
{
    fprintf(stdout, "test_memdb_backend\n");

    RUN_TEST(test_open_cfg_and_identity);
    RUN_TEST(test_two_instances_isolated);
    RUN_TEST(test_concurrent_creates_distinct_names);
    RUN_TEST(test_concurrent_creates_same_name);
    RUN_TEST(test_concurrent_setattr_no_lost_update);
    RUN_TEST(test_callbacks_reenter_same_handle);
    RUN_TEST(test_capacity_exhaustion_is_atomic);
    RUN_TEST(test_cookies_hard_link_paging);
    RUN_TEST(test_layout_grant_union_widens);
    RUN_TEST(test_dispatcher_union_has_no_overwrite_fallback);
    RUN_TEST(test_lock_test_and_scan_owner);
    RUN_TEST(test_cluster_registry_contract);
    RUN_TEST(test_coordination_tables_round_trip);
    RUN_TEST(test_namespace_semantics);
    RUN_TEST(test_gc_queue_and_fused_remove);
    RUN_TEST(test_nlink_adjust_saturates);

    fprintf(stdout, "\n  %d/%d tests passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
