/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_drain.c -- Unit tests for Seq 8 drain helpers and orchestrator.
 */

#include <stdio.h>
#include <assert.h>

/* Like assert() but not elided by NDEBUG. */
#define VERIFY(expr) do { if (!(expr)) { \
	fprintf(stderr, "VERIFY FAILED: %s (%s:%d)\n", \
		#expr, __FILE__, __LINE__); abort(); } } while (0)
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "pnfs_mds.h"
#include "test_helpers.h"
#include "subtree_map.h"
#include "cluster_membership.h"
#include "cluster_drain.h"
#include "mds_catalogue.h"
#include "migration.h"

/* -------------------------------------------------------------------
 * Test framework
 * ------------------------------------------------------------------- */

static int g_tests_run;
static int g_tests_passed;

#define RUN_TEST(fn) do { \
    g_tests_run++; \
    fprintf(stdout, "  " #fn ": "); \
    fn(); \
    g_tests_passed++; \
    fprintf(stdout, "PASS\n"); \
} while (0)

/* -------------------------------------------------------------------
 * DB helpers
 * ------------------------------------------------------------------- */

static const char *DB_PATH = "/tmp/test_drain.db";

static void cleanup_db(const char *path)
{
    char lockpath[512];
    (void)unlink(path);
    (void)snprintf(lockpath, sizeof(lockpath), "%s-lock", path);
    (void)unlink(lockpath);
}

static struct mds_catalogue *open_test_db(void)
{
    cleanup_db(DB_PATH);
    struct mds_catalogue *db = NULL;
    enum mds_status st = ((db = open_test_catalogue()) != NULL ? MDS_OK : MDS_ERR_IO);
    return (st == MDS_OK) ? db : NULL;
}

static struct mds_config make_cfg(uint32_t id, const char *host)
{
    struct mds_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.self.id = id;
    (void)snprintf(cfg.self.hostname, sizeof(cfg.self.hostname), "%s", host);
    cfg.self.nfs_port = 2049;
    cfg.self.grpc_port = 50051;
    return cfg;
}

/**
 * Helper: join a peer node and promote it to ACTIVE_SERVING.
 */
static void join_serving_peer(struct cluster_membership *ctx,
                              uint32_t id, const char *host,
                              uint16_t grpc)
{
    struct cluster_member m;
    memset(&m, 0, sizeof(m));
    m.mds_id = id;
    (void)snprintf(m.hostname, sizeof(m.hostname), "%s", host);
    m.nfs_port = 2049;
    m.grpc_port = grpc;
    assert(cluster_node_join(ctx, &m) == MDS_OK);
    /* Join defaults to NODE_IDLE (zero-init). Go straight to SERVING. */
    assert(cluster_membership_set_lifecycle(ctx, id, NODE_ACTIVE_SERVING)
           == MDS_OK);
}

/* -------------------------------------------------------------------
 * test_get_node_subtrees
 * ------------------------------------------------------------------- */

static void test_get_node_subtrees(void)
{
    struct subtree_map *smap = NULL;
    assert(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap) == MDS_OK);

    /* Self owns "/". Add "/data" owned by node 2. */
    assert(subtree_map_register_node(smap, 2, "mds2.local") == MDS_OK);
    assert(subtree_map_add(smap, "/data", 2, "mds2.local",
                           SUBTREE_ACTIVE, 1) == MDS_OK);
    assert(subtree_map_add(smap, "/home", 2, "mds2.local",
                           SUBTREE_ACTIVE, 1) == MDS_OK);

    struct subtree_entry *entries = NULL;
    uint32_t count = 0;

    /* Node 2 owns /data and /home. */
    assert(subtree_map_get_node_subtrees(smap, 2, &entries, &count) == MDS_OK);
    assert(count == 2);
    assert(entries != NULL);
    free(entries);

    /* Node 1 owns /. */
    assert(subtree_map_get_node_subtrees(smap, 1, &entries, &count) == MDS_OK);
    assert(count == 1);
    assert(entries != NULL);
    assert(strcmp(entries[0].path, "/") == 0);
    free(entries);

    /* Node 99 owns nothing. */
    assert(subtree_map_get_node_subtrees(smap, 99, &entries, &count) == MDS_OK);
    assert(count == 0);
    assert(entries == NULL);

    subtree_map_destroy(smap);
}

/* -------------------------------------------------------------------
 * test_path_to_fileid_resolution
 * ------------------------------------------------------------------- */

static void test_path_to_fileid_resolution(void)
{
    struct mds_catalogue *db = open_test_db();
    struct mds_catalogue *cat = NULL;
    assert(db != NULL);
    cat = db;

    /* Root. */
    uint64_t fid = 0;
    VERIFY(mds_cat_resolve_path(db, "/", &fid) == MDS_OK);
    assert(fid == MDS_FILEID_ROOT);

    /* Create /data directory. */
    struct mds_inode out;
    VERIFY(mds_cat_ns_create(cat, NULL, MDS_FILEID_ROOT, "data",
                         MDS_FTYPE_DIR, 0755, 0, 0,
			       NULL, &out) == MDS_OK);
    uint64_t data_fid = out.fileid;

    VERIFY(mds_cat_resolve_path(db, "/data", &fid) == MDS_OK);
    assert(fid == data_fid);

    /* Create /data/sub. */
    VERIFY(mds_cat_ns_create(cat, NULL, data_fid, "sub",
                         MDS_FTYPE_DIR, 0755, 0, 0,
			       NULL, &out) == MDS_OK);
    uint64_t sub_fid = out.fileid;

    VERIFY(mds_cat_resolve_path(db, "/data/sub", &fid) == MDS_OK);
    assert(fid == sub_fid);

    /* Missing path. */
    VERIFY(mds_cat_resolve_path(db, "/nonexistent", &fid) == MDS_ERR_NOTFOUND);

    /* Invalid args. */
    VERIFY(mds_cat_resolve_path(db, "relative", &fid) == MDS_ERR_INVAL);
    VERIFY(mds_cat_resolve_path(NULL, "/", &fid) == MDS_ERR_INVAL);
    VERIFY(mds_cat_resolve_path(db, "/", NULL) == MDS_ERR_INVAL);

    mds_catalogue_close(db);
    cleanup_db(DB_PATH);
}

/* -------------------------------------------------------------------
 * test_drain_self_rejects_null_db
 *
 * db == NULL must be caught at the parameter-validation guard.
 * ------------------------------------------------------------------- */

static void test_drain_self_rejects_null_db(void)
{
    struct mds_config cfg = make_cfg(1, "mds1.local");
    struct subtree_map *smap = NULL;
    struct cluster_membership *ctx = NULL;

    assert(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap) == MDS_OK);
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);

    /* db == NULL -> MDS_ERR_INVAL from the NULL-parameter guard. */
    assert(cluster_drain_self(ctx, smap, NULL, 2) == MDS_ERR_INVAL);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/* -------------------------------------------------------------------
 * test_drain_self_rejects_root_owner
 *
 * Self owns "/" and a valid serving destination exists.
 * drain must reject because root handoff is out of scope.
 * ------------------------------------------------------------------- */

static void test_drain_self_rejects_root_owner(void)
{
    struct mds_catalogue *db = open_test_db();
    struct mds_catalogue *cat = NULL;
    assert(db != NULL);
    cat = db;

    struct mds_config cfg = make_cfg(1, "mds1.local");
    struct subtree_map *smap = NULL;
    struct cluster_membership *ctx = NULL;

    assert(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap) == MDS_OK);
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);

    /* Add node 2 as a valid ACTIVE_SERVING destination. */
    join_serving_peer(ctx, 2, "mds2.local", 50052);

    /* Self owns "/" -- drain must reject. */
    assert(cluster_drain_self(ctx, smap, cat, 2) == MDS_ERR_PERM);

    /* Verify lifecycle was restored to ACTIVE_SERVING (undrain). */
    struct cluster_member self_m;
    assert(cluster_membership_get(ctx, 1, &self_m) == MDS_OK);
    assert(self_m.lifecycle == NODE_ACTIVE_SERVING);

    mds_catalogue_close(db);
    cleanup_db(DB_PATH);
    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/* -------------------------------------------------------------------
 * test_drain_self_rejects_non_serving_lifecycle
 *
 * If self is not ACTIVE_SERVING (e.g. already DRAINING), drain
 * must reject before doing any migration work.
 * ------------------------------------------------------------------- */

static void test_drain_self_rejects_non_serving_lifecycle(void)
{
    struct mds_catalogue *db = open_test_db();
    struct mds_catalogue *cat = NULL;
    assert(db != NULL);
    cat = db;

    struct mds_config cfg = make_cfg(1, "mds1.local");
    struct subtree_map *smap = NULL;
    struct cluster_membership *ctx = NULL;

    assert(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap) == MDS_OK);
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);

    /* Add node 2 as valid dest so preflight passes. */
    join_serving_peer(ctx, 2, "mds2.local", 50052);

    /* Move self to DRAINING so it's no longer ACTIVE_SERVING. */
    assert(cluster_membership_set_lifecycle(ctx, 1, NODE_DRAINING) == MDS_OK);

    /* drain must reject because lifecycle is wrong. */
    assert(cluster_drain_self(ctx, smap, cat, 2) == MDS_ERR_PERM);

    mds_catalogue_close(db);
    cleanup_db(DB_PATH);
    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/* -------------------------------------------------------------------
 * test_drain_self_rejects_dest_is_self
 *
 * dest_mds_id == self_id must be caught by the preflight guard.
 * ------------------------------------------------------------------- */

static void test_drain_self_rejects_dest_is_self(void)
{
    struct mds_catalogue *db = open_test_db();
    struct mds_catalogue *cat = NULL;
    assert(db != NULL);
    cat = db;

    struct mds_config cfg = make_cfg(1, "mds1.local");
    struct subtree_map *smap = NULL;
    struct cluster_membership *ctx = NULL;

    assert(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap) == MDS_OK);
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);

    /* dest == self -> MDS_ERR_INVAL. */
    assert(cluster_drain_self(ctx, smap, cat, 1) == MDS_ERR_INVAL);

    mds_catalogue_close(db);
    cleanup_db(DB_PATH);
    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/* -------------------------------------------------------------------
 * test_drain_self_rejects_dest_not_serving
 *
 * Destination exists but is not ACTIVE_SERVING (e.g. IDLE).
 * The preflight cluster_membership_can_own_subtrees() check must
 * reject before any migration work begins.
 * ------------------------------------------------------------------- */

static void test_drain_self_rejects_dest_not_serving(void)
{
    struct mds_catalogue *db = open_test_db();
    struct mds_catalogue *cat = NULL;
    assert(db != NULL);
    cat = db;

    struct mds_config cfg = make_cfg(1, "mds1.local");
    struct subtree_map *smap = NULL;
    struct cluster_membership *ctx = NULL;

    assert(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap) == MDS_OK);
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);

    /* Add node 2 but leave it in IDLE (not serving). */
    struct cluster_member m2;
    memset(&m2, 0, sizeof(m2));
    m2.mds_id = 2;
    (void)snprintf(m2.hostname, sizeof(m2.hostname), "mds2.local");
    m2.nfs_port = 2049;
    m2.grpc_port = 50052;
    assert(cluster_node_join(ctx, &m2) == MDS_OK);

    /* dest is not serving -> MDS_ERR_PERM from preflight. */
    assert(cluster_drain_self(ctx, smap, cat, 2) == MDS_ERR_PERM);

    mds_catalogue_close(db);
    cleanup_db(DB_PATH);
    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/* -------------------------------------------------------------------
 * main
 * ------------------------------------------------------------------- */

int main(void)
{
    fprintf(stdout, "test_drain:\n");

    RUN_TEST(test_get_node_subtrees);
    RUN_TEST(test_path_to_fileid_resolution);
    RUN_TEST(test_drain_self_rejects_null_db);
    RUN_TEST(test_drain_self_rejects_root_owner);
    RUN_TEST(test_drain_self_rejects_non_serving_lifecycle);
    RUN_TEST(test_drain_self_rejects_dest_is_self);
    RUN_TEST(test_drain_self_rejects_dest_not_serving);

    fprintf(stdout, "\n%d/%d drain tests passed.\n",
            g_tests_passed, g_tests_run);

    return (g_tests_passed == g_tests_run) ? 0 : 1;
}
