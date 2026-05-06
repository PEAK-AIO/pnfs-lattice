/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_cluster_membership.c -- Unit tests for cluster membership module.
 *
 * 19 test cases covering:
 *   - Membership core (1-10)
 *   - Error cases (11-14)
 *   - Subtree map integration (15-16)
 *   - Transport round-trip (17-19)
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>

#include "pnfs_mds.h"
#include "test_helpers.h"
#include "cluster_membership.h"
#include "subtree_map.h"
#include "cluster_transport.h"
#include "mds_catalogue.h"

/* -----------------------------------------------------------------------
 * Helpers
 * ----------------------------------------------------------------------- */

static const char *TEST_DB_PATH = "/tmp/test_cluster_membership.db";

static void cleanup_db(void)
{
    char lockpath[512];
    (void)unlink(TEST_DB_PATH);
    snprintf(lockpath, sizeof(lockpath), "%s-lock", TEST_DB_PATH);
    (void)unlink(lockpath);
}

static struct mds_catalogue *wrap_db_as_cat(struct mds_catalogue *db)
{
    return db;
}

static struct mds_catalogue *open_test_db(void)
{
    cleanup_db();
    struct mds_catalogue *db = NULL;
    enum mds_status st = ((db = open_test_catalogue()) != NULL ? MDS_OK : MDS_ERR_IO);
    if (st != MDS_OK) return NULL;
    return db;
}

static int g_tests_run;
static int g_tests_passed;

#define RUN_TEST(fn)                                                     \
    do {                                                                 \
        g_tests_run++;                                                   \
        fprintf(stdout, "  %-55s ", #fn);                                \
        fn();                                                            \
        g_tests_passed++;                                                \
        fprintf(stdout, "PASS\n");                                       \
    } while (0)

/** Build a minimal mds_config for testing. */
static void make_test_config(struct mds_config *cfg, uint32_t self_id,
                             const char *hostname)
{
    memset(cfg, 0, sizeof(*cfg));
    cfg->self.id = self_id;
    snprintf(cfg->self.hostname, sizeof(cfg->self.hostname), "%s", hostname);
    cfg->self.nfs_port = 2049;
    cfg->self.grpc_port = 50051;
}

/** Build a cluster_member for testing. */
static struct cluster_member make_member(uint32_t id, const char *host,
                                         uint16_t nfs, uint16_t grpc)
{
    struct cluster_member m;
    memset(&m, 0, sizeof(m));
    m.mds_id = id;
    snprintf(m.hostname, sizeof(m.hostname), "%s", host);
    m.nfs_port = nfs;
    m.grpc_port = grpc;
    m.join_time_sec = 1000000 + id;
    return m;
}

/* Change callback tracking. */
static uint32_t g_cb_mds_id;
static bool     g_cb_joined;
static int      g_cb_count;

static void test_change_cb(uint32_t mds_id, bool joined, void *arg)
{
    (void)arg;
    g_cb_mds_id = mds_id;
    g_cb_joined = joined;
    g_cb_count++;
}

static void reset_cb_state(void)
{
    g_cb_mds_id = 0;
    g_cb_joined = false;
    g_cb_count = 0;
}

/* -----------------------------------------------------------------------
 * 1. Init/destroy in local mode
 * ----------------------------------------------------------------------- */

static void test_init_destroy(void)
{
    struct mds_config cfg;
    make_test_config(&cfg, 1, "mds1.local");

    struct subtree_map *smap = NULL;
    assert(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap) == MDS_OK);

    struct cluster_membership *ctx = NULL;
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);
    assert(ctx != NULL);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/* -----------------------------------------------------------------------
 * 2. Self is registered after init
 * ----------------------------------------------------------------------- */

static void test_self_registered(void)
{
    struct mds_config cfg;
    make_test_config(&cfg, 5, "mds5.local");

    struct subtree_map *smap = NULL;
    assert(subtree_map_init(NULL, NULL, 5, "mds5.local", NULL, &smap) == MDS_OK);

    struct cluster_membership *ctx = NULL;
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);

    assert(cluster_membership_count(ctx) == 1);
    assert(cluster_membership_self_id(ctx) == 5);

    struct cluster_member m;
    assert(cluster_membership_get(ctx, 5, &m) == MDS_OK);
    assert(m.mds_id == 5);
    assert(strcmp(m.hostname, "mds5.local") == 0);
    assert(m.nfs_port == 2049);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/* -----------------------------------------------------------------------
 * 3. Coordinator election: self is coordinator when alone
 * ----------------------------------------------------------------------- */

static void test_coordinator_alone(void)
{
    struct mds_config cfg;
    make_test_config(&cfg, 10, "mds10.local");

    struct subtree_map *smap = NULL;
    assert(subtree_map_init(NULL, NULL, 10, "mds10.local", NULL, &smap) == MDS_OK);

    struct cluster_membership *ctx = NULL;
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);

    assert(cluster_membership_is_coordinator(ctx) == true);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/* -----------------------------------------------------------------------
 * 4. Join a second node, verify membership list
 * ----------------------------------------------------------------------- */

static void test_join_second_node(void)
{
    struct mds_config cfg;
    make_test_config(&cfg, 1, "mds1.local");

    struct subtree_map *smap = NULL;
    assert(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap) == MDS_OK);

    struct cluster_membership *ctx = NULL;
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);

    struct cluster_member m2 = make_member(2, "mds2.local", 2049, 50052);
    assert(cluster_node_join(ctx, &m2) == MDS_OK);

    assert(cluster_membership_count(ctx) == 2);

    struct cluster_member *list = NULL;
    uint32_t count = 0;
    assert(cluster_membership_list(ctx, &list, &count) == MDS_OK);
    assert(count == 2);

    /* Both members present. */
    bool found1 = false, found2 = false;
    for (uint32_t i = 0; i < count; i++) {
        if (list[i].mds_id == 1) found1 = true;
        if (list[i].mds_id == 2) found2 = true;
    }
    assert(found1 && found2);
    free(list);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/* -----------------------------------------------------------------------
 * 5. Coordinator election with two nodes (lowest ID wins)
 * ----------------------------------------------------------------------- */

static void test_coordinator_two_nodes(void)
{
    struct mds_config cfg;
    make_test_config(&cfg, 5, "mds5.local");

    struct subtree_map *smap = NULL;
    assert(subtree_map_init(NULL, NULL, 5, "mds5.local", NULL, &smap) == MDS_OK);

    struct cluster_membership *ctx = NULL;
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);

    /* Self=5, add node 3 (lower ID). */
    struct cluster_member m3 = make_member(3, "mds3.local", 2049, 50053);
    assert(cluster_node_join(ctx, &m3) == MDS_OK);

    /* Node 3 has lower ID, so self is NOT coordinator. */
    assert(cluster_membership_is_coordinator(ctx) == false);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/* -----------------------------------------------------------------------
 * 6. Leave a node, verify removed from list
 * ----------------------------------------------------------------------- */

static void test_leave_node(void)
{
    struct mds_config cfg;
    make_test_config(&cfg, 1, "mds1.local");

    struct subtree_map *smap = NULL;
    assert(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap) == MDS_OK);

    struct cluster_membership *ctx = NULL;
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);

    struct cluster_member m2 = make_member(2, "mds2.local", 2049, 50052);
    assert(cluster_node_join(ctx, &m2) == MDS_OK);
    assert(cluster_membership_count(ctx) == 2);

    assert(cluster_node_leave(ctx, 2) == MDS_OK);
    assert(cluster_membership_count(ctx) == 1);

    assert(cluster_membership_get(ctx, 2, &m2) == MDS_ERR_NOTFOUND);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/* -----------------------------------------------------------------------
 * 7. Change callback fires on successful join
 * ----------------------------------------------------------------------- */

static void test_callback_on_join(void)
{
    struct mds_config cfg;
    make_test_config(&cfg, 1, "mds1.local");

    struct subtree_map *smap = NULL;
    assert(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap) == MDS_OK);

    struct cluster_membership *ctx = NULL;
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);

    reset_cb_state();
    cluster_membership_set_change_cb(ctx, test_change_cb, NULL);

    struct cluster_member m2 = make_member(2, "mds2.local", 2049, 50052);
    assert(cluster_node_join(ctx, &m2) == MDS_OK);

    assert(g_cb_count == 1);
    assert(g_cb_mds_id == 2);
    assert(g_cb_joined == true);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/* -----------------------------------------------------------------------
 * 8. Change callback fires on successful leave
 * ----------------------------------------------------------------------- */

static void test_callback_on_leave(void)
{
    struct mds_config cfg;
    make_test_config(&cfg, 1, "mds1.local");

    struct subtree_map *smap = NULL;
    assert(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap) == MDS_OK);

    struct cluster_membership *ctx = NULL;
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);

    struct cluster_member m2 = make_member(2, "mds2.local", 2049, 50052);
    assert(cluster_node_join(ctx, &m2) == MDS_OK);

    reset_cb_state();
    cluster_membership_set_change_cb(ctx, test_change_cb, NULL);

    assert(cluster_node_leave(ctx, 2) == MDS_OK);

    assert(g_cb_count == 1);
    assert(g_cb_mds_id == 2);
    assert(g_cb_joined == false);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/* -----------------------------------------------------------------------
 * 9. Callback does NOT fire on rejected duplicate join
 * ----------------------------------------------------------------------- */

static void test_callback_not_on_dup_join(void)
{
    struct mds_config cfg;
    make_test_config(&cfg, 1, "mds1.local");

    struct subtree_map *smap = NULL;
    assert(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap) == MDS_OK);

    struct cluster_membership *ctx = NULL;
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);

    struct cluster_member m2 = make_member(2, "mds2.local", 2049, 50052);
    assert(cluster_node_join(ctx, &m2) == MDS_OK);

    reset_cb_state();
    cluster_membership_set_change_cb(ctx, test_change_cb, NULL);

    /* Duplicate join -- should fail. */
    assert(cluster_node_join(ctx, &m2) == MDS_ERR_EXISTS);

    /* Callback must NOT have fired. */
    assert(g_cb_count == 0);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/* -----------------------------------------------------------------------
 * 10. Callback does NOT fire on rejected leave (unknown node)
 * ----------------------------------------------------------------------- */

static void test_callback_not_on_bad_leave(void)
{
    struct mds_config cfg;
    make_test_config(&cfg, 1, "mds1.local");

    struct subtree_map *smap = NULL;
    assert(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap) == MDS_OK);

    struct cluster_membership *ctx = NULL;
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);

    reset_cb_state();
    cluster_membership_set_change_cb(ctx, test_change_cb, NULL);

    /* Leave non-existent node 99. */
    assert(cluster_node_leave(ctx, 99) == MDS_ERR_NOTFOUND);
    assert(g_cb_count == 0);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/* -----------------------------------------------------------------------
 * 11. Double-join rejected (MDS_ERR_EXISTS)
 * ----------------------------------------------------------------------- */

static void test_double_join(void)
{
    struct mds_config cfg;
    make_test_config(&cfg, 1, "mds1.local");

    struct subtree_map *smap = NULL;
    assert(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap) == MDS_OK);

    struct cluster_membership *ctx = NULL;
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);

    struct cluster_member m2 = make_member(2, "mds2.local", 2049, 50052);
    assert(cluster_node_join(ctx, &m2) == MDS_OK);
    assert(cluster_node_join(ctx, &m2) == MDS_ERR_EXISTS);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/* -----------------------------------------------------------------------
 * 12. Leave non-existent node (MDS_ERR_NOTFOUND)
 * ----------------------------------------------------------------------- */

static void test_leave_unknown(void)
{
    struct mds_config cfg;
    make_test_config(&cfg, 1, "mds1.local");

    struct subtree_map *smap = NULL;
    assert(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap) == MDS_OK);

    struct cluster_membership *ctx = NULL;
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);

    assert(cluster_node_leave(ctx, 99) == MDS_ERR_NOTFOUND);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/* -----------------------------------------------------------------------
 * 13. Cannot leave self (MDS_ERR_INVAL)
 * ----------------------------------------------------------------------- */

static void test_leave_self(void)
{
    struct mds_config cfg;
    make_test_config(&cfg, 1, "mds1.local");

    struct subtree_map *smap = NULL;
    assert(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap) == MDS_OK);

    struct cluster_membership *ctx = NULL;
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);

    assert(cluster_node_leave(ctx, 1) == MDS_ERR_INVAL);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/* -----------------------------------------------------------------------
 * 14. Leave rejected if node still owns subtrees (MDS_ERR_PERM)
 * ----------------------------------------------------------------------- */

static void test_leave_owns_subtrees(void)
{
    struct mds_config cfg;
    make_test_config(&cfg, 1, "mds1.local");

    struct subtree_map *smap = NULL;
    assert(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap) == MDS_OK);

    struct cluster_membership *ctx = NULL;
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);

    /* Add node 2 and give it a subtree. */
    struct cluster_member m2 = make_member(2, "mds2.local", 2049, 50052);
    assert(cluster_node_join(ctx, &m2) == MDS_OK);

    assert(subtree_map_add(smap, "/data", 2, "mds2.local",
                           SUBTREE_ACTIVE, 1) == MDS_OK);

    /* Leave should be rejected -- node 2 still owns /data. */
    assert(cluster_node_leave(ctx, 2) == MDS_ERR_PERM);

    /* Drain the subtree, then leave should succeed. */
    assert(subtree_map_remove_subtree(smap, "/data") == MDS_OK);
    assert(cluster_node_leave(ctx, 2) == MDS_OK);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/* -----------------------------------------------------------------------
 * 15. Join registers node hostname in subtree_map
 * ----------------------------------------------------------------------- */

static void test_join_registers_hostname(void)
{
    struct mds_config cfg;
    make_test_config(&cfg, 1, "mds1.local");

    struct subtree_map *smap = NULL;
    assert(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap) == MDS_OK);

    struct cluster_membership *ctx = NULL;
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);

    struct cluster_member m2 = make_member(2, "mds2.local", 2049, 50052);
    assert(cluster_node_join(ctx, &m2) == MDS_OK);

    /* Verify hostname is resolvable from subtree_map. */
    char hostname[256];
    assert(subtree_map_node_hostname(smap, 2, hostname,
                                     sizeof(hostname)) == MDS_OK);
    assert(strcmp(hostname, "mds2.local") == 0);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/* -----------------------------------------------------------------------
 * 16. subtree_map_node_owns_subtrees correctness
 * ----------------------------------------------------------------------- */

static void test_node_owns_subtrees(void)
{
    struct subtree_map *smap = NULL;
    assert(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap) == MDS_OK);

    /* Node 1 owns "/" (seeded by init). */
    assert(subtree_map_node_owns_subtrees(smap, 1) == true);

    /* Node 2 owns nothing. */
    assert(subtree_map_node_owns_subtrees(smap, 2) == false);

    /* Give node 2 a subtree. */
    assert(subtree_map_add(smap, "/projects", 2, "mds2.local",
                           SUBTREE_ACTIVE, 1) == MDS_OK);
    assert(subtree_map_node_owns_subtrees(smap, 2) == true);

    /* Remove it. */
    assert(subtree_map_remove_subtree(smap, "/projects") == MDS_OK);
    assert(subtree_map_node_owns_subtrees(smap, 2) == false);

    subtree_map_destroy(smap);
}

/* -----------------------------------------------------------------------
 * 17-19. Transport round-trip tests
 *
 * These start a real cluster_transport server on a loopback port,
 * register a membership handle, and use the client helpers to verify
 * the wire protocol for NODE_JOIN, NODE_LEAVE, and CLUSTER_STATUS.
 * ----------------------------------------------------------------------- */

static void test_transport_node_join(void)
{
    struct mds_config cfg;
    make_test_config(&cfg, 1, "mds1.local");

    struct subtree_map *smap = NULL;
    assert(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap) == MDS_OK);

    struct cluster_membership *ctx = NULL;
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);

    /* Start server on ephemeral port. */
    struct mds_catalogue *db = open_test_db();
    assert(db != NULL);
    struct cluster_server *srv = NULL;
    assert(cluster_transport_server_start(0, "127.0.0.1",
                                          NULL, 0, 4,
                                          wrap_db_as_cat(db), smap, NULL, &srv) == MDS_OK);
    cluster_transport_server_set_membership(srv, ctx);

    uint16_t port = cluster_transport_server_port(srv);

    /* Join node 3 via transport. */
    assert(cluster_transport_request_node_join(
        "127.0.0.1", port, 3, "mds3.local", 2049, 50053,
        NODE_ACTIVE, NODE_IDLE, 0, "") == MDS_OK);

    /* Verify it was added. */
    assert(cluster_membership_count(ctx) == 2);
    struct cluster_member m;
    assert(cluster_membership_get(ctx, 3, &m) == MDS_OK);
    assert(strcmp(m.hostname, "mds3.local") == 0);

    /* Duplicate should fail. */
    assert(cluster_transport_request_node_join(
        "127.0.0.1", port, 3, "mds3.local", 2049, 50053,
        NODE_ACTIVE, NODE_IDLE, 0, "") != MDS_OK);

    cluster_transport_server_stop(srv);
    mds_catalogue_close(db);
    cleanup_db();
    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

static void test_transport_node_leave(void)
{
    struct mds_config cfg;
    make_test_config(&cfg, 1, "mds1.local");

    struct subtree_map *smap = NULL;
    assert(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap) == MDS_OK);

    struct cluster_membership *ctx = NULL;
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);

    struct cluster_member m2 = make_member(2, "mds2.local", 2049, 50052);
    assert(cluster_node_join(ctx, &m2) == MDS_OK);

    struct mds_catalogue *db = open_test_db();
    assert(db != NULL);
    struct cluster_server *srv = NULL;
    assert(cluster_transport_server_start(0, "127.0.0.1",
                                          NULL, 0, 4,
                                          wrap_db_as_cat(db), smap, NULL, &srv) == MDS_OK);
    cluster_transport_server_set_membership(srv, ctx);
    uint16_t port = cluster_transport_server_port(srv);

    /* Leave node 2 via transport. */
    assert(cluster_transport_request_node_leave(
        "127.0.0.1", port, 2) == MDS_OK);

    assert(cluster_membership_count(ctx) == 1);

    cluster_transport_server_stop(srv);
    cluster_membership_destroy(ctx);
    mds_catalogue_close(db);
    cleanup_db();
    subtree_map_destroy(smap);
}

static void test_transport_cluster_status(void)
{
    struct mds_config cfg;
    make_test_config(&cfg, 1, "mds1.local");

    struct subtree_map *smap = NULL;
    assert(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap) == MDS_OK);

    struct cluster_membership *ctx = NULL;
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);

    struct cluster_member m2 = make_member(2, "mds2.local", 2049, 50052);
    assert(cluster_node_join(ctx, &m2) == MDS_OK);

    struct mds_catalogue *db = open_test_db();
    assert(db != NULL);
    struct cluster_server *srv = NULL;
    assert(cluster_transport_server_start(0, "127.0.0.1",
                                          NULL, 0, 4,
                                          wrap_db_as_cat(db), smap, NULL, &srv) == MDS_OK);
    cluster_transport_server_set_membership(srv, ctx);
    uint16_t port = cluster_transport_server_port(srv);

    /* Query status via transport. */
    struct cluster_member *members = NULL;
    uint32_t count = 0;
    assert(cluster_transport_request_cluster_status(
        "127.0.0.1", port, &members, &count) == MDS_OK);

    assert(count == 2);
    bool found1 = false, found2 = false;
    for (uint32_t i = 0; i < count; i++) {
        if (members[i].mds_id == 1 &&
            strcmp(members[i].hostname, "mds1.local") == 0)
            found1 = true;
        if (members[i].mds_id == 2 &&
            strcmp(members[i].hostname, "mds2.local") == 0)
            found2 = true;
    }
    assert(found1 && found2);
    free(members);

    cluster_transport_server_stop(srv);
    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
    mds_catalogue_close(db);
    cleanup_db();
}

/* -----------------------------------------------------------------------
 * Sequence 0 -- API stabilisation
 * ----------------------------------------------------------------------- */

static void test_is_authoritative_local(void)
{
    struct subtree_map *smap = NULL;
    enum mds_status st = subtree_map_init(NULL, NULL, 1, "host1",
                                 NULL, &smap);
    assert(st == MDS_OK);

    struct mds_config cfg;
    make_test_config(&cfg, 1, "host1");

    struct cluster_membership *m = NULL;
    st = cluster_membership_init(&cfg, smap, NULL, &m);
    assert(st == MDS_OK);

    /* RonDB mode is always authoritative (etcd path removed). */
    assert(cluster_membership_is_authoritative(m) == true);
    assert(subtree_map_is_authoritative(smap) == false);

    cluster_membership_destroy(m);
    subtree_map_destroy(smap);
}

static void test_fail_loud_etcd_multinode(void)
{
    /* etcd path removed -- multi-node uses RonDB node_registry.
     * Verify that cluster_size > 1 still succeeds (local mode
     * is populated by cluster_membership_populate_rondb later). */
    struct subtree_map *smap = NULL;
    enum mds_status st = subtree_map_init(NULL, NULL, 1, "host1",
                                 NULL, &smap);
    assert(st == MDS_OK);

    struct mds_config cfg;
    make_test_config(&cfg, 1, "host1");
    cfg.cluster_size = 3;

    struct cluster_membership *m = NULL;
    st = cluster_membership_init(&cfg, smap, NULL, &m);
    assert(st == MDS_OK);
    cluster_membership_destroy(m);

    subtree_map_destroy(smap);
}

/* -----------------------------------------------------------------------
 * Sequence 1 -- Role, lifecycle, topology
 * ----------------------------------------------------------------------- */

/** Self-registered node should have NODE_ACTIVE + NODE_IDLE. */
static void test_self_role_lifecycle_defaults(void)
{
    struct mds_config cfg;
    struct subtree_map *smap = NULL;
    struct cluster_membership *ctx = NULL;

    make_test_config(&cfg, 1, "mds1.local");
    assert(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap) == MDS_OK);
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);

    struct cluster_member m;
    assert(cluster_membership_get(ctx, 1, &m) == MDS_OK);
    assert(m.role == NODE_ACTIVE);
    assert(m.lifecycle == NODE_ACTIVE_SERVING);
    assert(m.failover_partner_id == 0);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/** Joined node inherits role/lifecycle defaults. */
static void test_join_role_lifecycle_defaults(void)
{
    struct mds_config cfg;
    struct subtree_map *smap = NULL;
    struct cluster_membership *ctx = NULL;

    make_test_config(&cfg, 1, "mds1.local");
    assert(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap) == MDS_OK);
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);

    struct cluster_member joiner;
    memset(&joiner, 0, sizeof(joiner));
    joiner.mds_id = 2;
    snprintf(joiner.hostname, sizeof(joiner.hostname), "mds2.local");
    joiner.nfs_port = 2049;
    joiner.grpc_port = 50052;
    joiner.join_time_sec = 1000;

    assert(cluster_node_join(ctx, &joiner) == MDS_OK);

    struct cluster_member out;
    assert(cluster_membership_get(ctx, 2, &out) == MDS_OK);
    assert(out.role == NODE_ACTIVE);
    assert(out.lifecycle == NODE_IDLE);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/** Valid lifecycle transitions. */
static void test_set_lifecycle_valid(void)
{
    struct mds_config cfg;
    struct subtree_map *smap = NULL;
    struct cluster_membership *ctx = NULL;

    make_test_config(&cfg, 1, "mds1.local");
    assert(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap) == MDS_OK);
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);

    /* Self starts ACTIVE_SERVING (owns root). ACTIVE_SERVING -> DRAINING. */
    assert(cluster_membership_set_lifecycle(ctx, 1, NODE_DRAINING) == MDS_OK);

    struct cluster_member m;
    assert(cluster_membership_get(ctx, 1, &m) == MDS_OK);
    assert(m.lifecycle == NODE_DRAINING);

    /* DRAINING -> ACTIVE_SERVING (undrain). */
    assert(cluster_membership_set_lifecycle(ctx, 1, NODE_ACTIVE_SERVING)
           == MDS_OK);
    assert(cluster_membership_get(ctx, 1, &m) == MDS_OK);
    assert(m.lifecycle == NODE_ACTIVE_SERVING);

    /* Test IDLE -> ACTIVE_SERVING on a joined node. */
    struct cluster_member joiner = make_member(2, "mds2.local", 2049, 50052);
    assert(cluster_node_join(ctx, &joiner) == MDS_OK);
    assert(cluster_membership_set_lifecycle(ctx, 2, NODE_ACTIVE_SERVING)
           == MDS_OK);
    assert(cluster_membership_get(ctx, 2, &m) == MDS_OK);
    assert(m.lifecycle == NODE_ACTIVE_SERVING);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/** Invalid lifecycle transitions must return MDS_ERR_INVAL. */
static void test_set_lifecycle_invalid(void)
{
    struct mds_config cfg;
    struct subtree_map *smap = NULL;
    struct cluster_membership *ctx = NULL;

    make_test_config(&cfg, 1, "mds1.local");
    assert(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap) == MDS_OK);
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);

    /* ACTIVE_SERVING -> IDLE is not valid. */
    assert(cluster_membership_set_lifecycle(ctx, 1, NODE_IDLE)
           == MDS_ERR_INVAL);

    /* ACTIVE_SERVING -> JOINING is not valid. */
    assert(cluster_membership_set_lifecycle(ctx, 1, NODE_JOINING)
           == MDS_ERR_INVAL);

    /* ACTIVE_SERVING -> ACTIVE_SERVING (self-transition) is not valid. */
    assert(cluster_membership_set_lifecycle(ctx, 1, NODE_ACTIVE_SERVING)
           == MDS_ERR_INVAL);

    /* Test IDLE -> DRAINING on a joined node (must go via ACTIVE_SERVING). */
    struct cluster_member joiner = make_member(2, "mds2.local", 2049, 50052);
    assert(cluster_node_join(ctx, &joiner) == MDS_OK);
    assert(cluster_membership_set_lifecycle(ctx, 2, NODE_DRAINING)
           == MDS_ERR_INVAL);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/** set_lifecycle on non-existent node returns NOTFOUND. */
static void test_set_lifecycle_not_found(void)
{
    struct mds_config cfg;
    struct subtree_map *smap = NULL;
    struct cluster_membership *ctx = NULL;

    make_test_config(&cfg, 1, "mds1.local");
    assert(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap) == MDS_OK);
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);

    assert(cluster_membership_set_lifecycle(ctx, 5, NODE_IDLE)
           == MDS_ERR_NOTFOUND);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/** JOINING -> IDLE transition for a joining node. */
static void test_set_lifecycle_joining_to_idle(void)
{
    struct mds_config cfg;
    struct subtree_map *smap = NULL;
    struct cluster_membership *ctx = NULL;

    make_test_config(&cfg, 1, "mds1.local");
    assert(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap) == MDS_OK);
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);

    /* Join a node that declares JOINING state. */
    struct cluster_member joiner;
    memset(&joiner, 0, sizeof(joiner));
    joiner.mds_id = 2;
    snprintf(joiner.hostname, sizeof(joiner.hostname), "mds2.local");
    joiner.nfs_port = 2049;
    joiner.grpc_port = 50052;
    joiner.lifecycle = NODE_JOINING;

    assert(cluster_node_join(ctx, &joiner) == MDS_OK);

    struct cluster_member m;
    assert(cluster_membership_get(ctx, 2, &m) == MDS_OK);
    assert(m.lifecycle == NODE_JOINING);

    /* JOINING -> IDLE */
    assert(cluster_membership_set_lifecycle(ctx, 2, NODE_IDLE) == MDS_OK);
    assert(cluster_membership_get(ctx, 2, &m) == MDS_OK);
    assert(m.lifecycle == NODE_IDLE);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/** Standby cannot transition to ACTIVE_SERVING. */
static void test_standby_cannot_serve(void)
{
    struct mds_config cfg;
    make_test_config(&cfg, 1, "mds1.local");

    struct subtree_map *smap = NULL;
    assert(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap) == MDS_OK);

    struct cluster_membership *ctx = NULL;
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);

    /* Join a standby node. */
    struct cluster_member standby = make_member(2, "mds2.local", 2049, 50052);
    standby.role = NODE_STANDBY;
    assert(cluster_node_join(ctx, &standby) == MDS_OK);

    struct cluster_member m;
    assert(cluster_membership_get(ctx, 2, &m) == MDS_OK);
    assert(m.role == NODE_STANDBY);
    assert(m.lifecycle == NODE_IDLE);

    /* Standby cannot go to ACTIVE_SERVING. */
    assert(cluster_membership_set_lifecycle(ctx, 2, NODE_ACTIVE_SERVING)
           == MDS_ERR_PERM);

    /* Verify lifecycle unchanged. */
    assert(cluster_membership_get(ctx, 2, &m) == MDS_OK);
    assert(m.lifecycle == NODE_IDLE);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/** Standby cannot own subtrees when membership backlink is set. */
static void test_standby_cannot_own_subtrees(void)
{
    struct mds_config cfg;
    make_test_config(&cfg, 1, "mds1.local");

    struct subtree_map *smap = NULL;
    assert(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap) == MDS_OK);

    struct cluster_membership *ctx = NULL;
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);

    /* Wire membership backlink. */
    subtree_map_set_membership(smap, ctx);

    /* Join a standby. */
    struct cluster_member standby = make_member(2, "mds2.local", 2049, 50052);
    standby.role = NODE_STANDBY;
    assert(cluster_node_join(ctx, &standby) == MDS_OK);

    /* Standby cannot be assigned ownership via set_owner. */
    assert(subtree_map_set_owner(smap, "/", 2, 1) == MDS_ERR_PERM);

    /* Standby cannot be added as owner via subtree_map_add. */
    assert(subtree_map_add(smap, "/data", 2, "mds2.local",
                           SUBTREE_ACTIVE, 1) == MDS_ERR_PERM);

    /* Active node can still own subtrees. */
    assert(subtree_map_add(smap, "/data", 1, "mds1.local",
                           SUBTREE_ACTIVE, 1) == MDS_OK);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/** Self cluster_addr populated from config's cluster_bind_addr. */
static void test_self_cluster_addr_from_config(void)
{
    struct mds_config cfg;
    make_test_config(&cfg, 1, "mds1.local");
    snprintf(cfg.cluster_bind_addr, sizeof(cfg.cluster_bind_addr),
             "10.0.0.1");

    struct subtree_map *smap = NULL;
    assert(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap) == MDS_OK);

    struct cluster_membership *ctx = NULL;
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);

    struct cluster_member m;
    assert(cluster_membership_get(ctx, 1, &m) == MDS_OK);
    assert(strcmp(m.cluster_addr, "10.0.0.1") == 0);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/** is_active_role helper returns correct results. */
static void test_is_active_role(void)
{
    struct mds_config cfg;
    make_test_config(&cfg, 1, "mds1.local");

    struct subtree_map *smap = NULL;
    assert(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap) == MDS_OK);

    struct cluster_membership *ctx = NULL;
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);

    /* Self is active. */
    assert(cluster_membership_is_active_role(ctx, 1) == true);

    /* Join a standby. */
    struct cluster_member standby = make_member(2, "mds2.local", 2049, 50052);
    standby.role = NODE_STANDBY;
    assert(cluster_node_join(ctx, &standby) == MDS_OK);
    assert(cluster_membership_is_active_role(ctx, 2) == false);

    /* Non-existent node. */
    assert(cluster_membership_is_active_role(ctx, 99) == false);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/** NODE_JOIN wire round-trip: partner_id and cluster_addr survive. */
static void test_transport_join_topology_fields(void)
{
    struct mds_config cfg;
    make_test_config(&cfg, 1, "mds1.local");

    struct subtree_map *smap = NULL;
    assert(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap) == MDS_OK);

    struct cluster_membership *ctx = NULL;
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);

    struct mds_catalogue *db = open_test_db();
    assert(db != NULL);
    struct cluster_server *srv = NULL;
    assert(cluster_transport_server_start(0, "127.0.0.1",
                                          NULL, 0, 4,
                                          wrap_db_as_cat(db), smap, NULL, &srv) == MDS_OK);
    cluster_transport_server_set_membership(srv, ctx);
    uint16_t port = cluster_transport_server_port(srv);

    /* Join with non-default topology: standby, partner=1, addr=10.0.0.5 */
    assert(cluster_transport_request_node_join(
        "127.0.0.1", port, 3, "mds3.local", 2049, 50053,
        NODE_STANDBY, NODE_IDLE, 1, "10.0.0.5") == MDS_OK);

    /* Verify via membership get. */
    struct cluster_member m;
    assert(cluster_membership_get(ctx, 3, &m) == MDS_OK);
    assert(m.role == NODE_STANDBY);
    assert(m.lifecycle == NODE_IDLE);
    assert(m.failover_partner_id == 1);
    assert(strcmp(m.cluster_addr, "10.0.0.5") == 0);
    assert(strcmp(m.hostname, "mds3.local") == 0);

    cluster_transport_server_stop(srv);
    mds_catalogue_close(db);
    cleanup_db();
    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/** CLUSTER_STATUS wire round-trip: new fields survive serialization. */
static void test_transport_status_topology_fields(void)
{
    struct mds_config cfg;
    make_test_config(&cfg, 1, "mds1.local");
    snprintf(cfg.cluster_bind_addr, sizeof(cfg.cluster_bind_addr),
             "10.0.0.1");

    struct subtree_map *smap = NULL;
    assert(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap) == MDS_OK);

    struct cluster_membership *ctx = NULL;
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);

    /* Join node 2 with topology fields. */
    struct cluster_member n2 = make_member(2, "mds2.local", 2049, 50052);
    n2.role = NODE_STANDBY;
    n2.failover_partner_id = 1;
    snprintf(n2.cluster_addr, sizeof(n2.cluster_addr), "10.0.0.2");
    assert(cluster_node_join(ctx, &n2) == MDS_OK);

    struct mds_catalogue *db = open_test_db();
    assert(db != NULL);
    struct cluster_server *srv = NULL;
    assert(cluster_transport_server_start(0, "127.0.0.1",
                                          NULL, 0, 4,
                                          wrap_db_as_cat(db), smap, NULL, &srv) == MDS_OK);
    cluster_transport_server_set_membership(srv, ctx);
    uint16_t port = cluster_transport_server_port(srv);

    /* Query status -- topology fields must survive the wire. */
    struct cluster_member *members = NULL;
    uint32_t count = 0;
    assert(cluster_transport_request_cluster_status(
        "127.0.0.1", port, &members, &count) == MDS_OK);
    assert(count == 2);

    /* Find each member and verify topology. */
    const struct cluster_member *self = NULL;
    const struct cluster_member *peer = NULL;
    for (uint32_t i = 0; i < count; i++) {
        if (members[i].mds_id == 1) self = &members[i];
        if (members[i].mds_id == 2) peer = &members[i];
    }
    assert(self != NULL && peer != NULL);

    /* Self: active, serving, partner=0, addr from config. */
    assert(self->role == NODE_ACTIVE);
    assert(self->lifecycle == NODE_ACTIVE_SERVING);
    assert(self->failover_partner_id == 0);
    assert(strcmp(self->cluster_addr, "10.0.0.1") == 0);

    /* Peer: standby, idle, partner=1, addr=10.0.0.2. */
    assert(peer->role == NODE_STANDBY);
    assert(peer->lifecycle == NODE_IDLE);
    assert(peer->failover_partner_id == 1);
    assert(strcmp(peer->cluster_addr, "10.0.0.2") == 0);

    free(members);
    cluster_transport_server_stop(srv);
    mds_catalogue_close(db);
    cleanup_db();
    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/* -----------------------------------------------------------------------
 * Main
 * ----------------------------------------------------------------------- */


/* etcd tests removed -- coordination is RonDB-native. */

static void _unused_etcd_placeholder(void)
{
    (void)0;
}

#if 0  /* etcd test removed */
static void test_etcd_restart_self_registration(void)
{
    struct etcd_client *setup_client = NULL;
    assert(etcd_client_create("http://127.0.0.1:2379",
                              NULL, &setup_client) == MDS_OK);

    const char *node_key = "/pnfs-mds/nodes/11";

    /* 1. Preload stale membership JSON (no lease). */
    const char *stale_json =
        "{\"hostname\":\"stale-host\","
        "\"nfs_port\":2049,\"grpc_port\":9100,"
        "\"role\":0,\"lifecycle\":0,"
        "\"partner_id\":0,\"cluster_addr\":\"\"}";

    /* Clean up then create the stale key (no lease -> persistent). */
    etcd_kv_delete(setup_client, node_key);
    assert(etcd_kv_put(setup_client, node_key, stale_json, 0) == MDS_OK);

    /* Verify stale key is there. */
    char *val = NULL;
    int64_t rev = 0;
    assert(etcd_kv_get(setup_client, node_key, &val, &rev) == MDS_OK);
    assert(strstr(val, "stale-host") != NULL);
    free(val);

    etcd_client_destroy(setup_client);

    /* 2. Build config and run cluster_membership_init. */

    struct subtree_map *smap = NULL;
    assert(subtree_map_init(NULL, NULL, 11, "restart-test-host", NULL, &smap) == MDS_OK);

    struct mds_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.self.id = 11;
    snprintf(cfg.self.hostname, sizeof(cfg.self.hostname),
             "restart-test-host");
    cfg.self.nfs_port = 2049;
    cfg.self.grpc_port = 9100;
    cfg.cluster_size = 2;  /* multi-node to trigger etcd path */
    snprintf(cfg.etcd_endpoints, sizeof(cfg.etcd_endpoints),
             "http://127.0.0.1:2379");

    struct cluster_membership *ctx = NULL;
    enum mds_status st = cluster_membership_init(&cfg, smap, NULL, &ctx);
    assert(st == MDS_OK);
    assert(ctx != NULL);

    /* 3. Verify value is updated (hostname == restart-test-host). */
    struct etcd_client *verify_client = NULL;
    assert(etcd_client_create("http://127.0.0.1:2379",
                              NULL, &verify_client) == MDS_OK);

    val = NULL;
    rev = 0;
    st = etcd_kv_get(verify_client, node_key, &val, &rev);
    assert(st == MDS_OK);
    assert(val != NULL);
    assert(strstr(val, "restart-test-host") != NULL);
    assert(strstr(val, "stale-host") == NULL);
    free(val);

    /* 4. Destroy membership (revokes lease -> key should disappear). */
    cluster_membership_destroy(ctx);

    /* Give etcd a moment to process the lease revocation. */
    usleep(500000);

    /* 5. Verify key is gone. */
    val = NULL;
    rev = 0;
    st = etcd_kv_get(verify_client, node_key, &val, &rev);
    assert(st == MDS_ERR_NOTFOUND);

    etcd_client_destroy(verify_client);
    subtree_map_destroy(smap);
}
#endif  /* etcd test removed */

/* -------------------------------------------------------------------
 * Sequence 5 -- resolve_peer tests
 * ------------------------------------------------------------------- */

static void test_resolve_peer_found(void)
{
    struct mds_config cfg;
    make_test_config(&cfg, 1, "node1");
    cfg.self.grpc_port = 50051;

    struct subtree_map *smap = NULL;
    enum mds_status st = subtree_map_init(NULL, NULL, 1, "node1",
                                 NULL, &smap);
    assert(st == MDS_OK);

    struct cluster_membership *cm = NULL;
    st = cluster_membership_init(&cfg, smap, NULL, &cm);
    assert(st == MDS_OK);

    /* Register peer with cluster_addr and grpc_port. */
    struct cluster_member peer = make_member(2, "node2.local", 2049, 9800);
    snprintf(peer.cluster_addr, sizeof(peer.cluster_addr), "10.0.0.2");
    st = cluster_node_join(cm, &peer);
    assert(st == MDS_OK);

    /* Resolve should return cluster_addr and grpc_port. */
    char host[256];
    uint16_t port = 0;
    st = cluster_membership_resolve_peer(cm, 2, host, sizeof(host), &port);
    assert(st == MDS_OK);
    assert(strcmp(host, "10.0.0.2") == 0);
    assert(port == 9800);

    cluster_membership_destroy(cm);
    subtree_map_destroy(smap);
}

static void test_resolve_peer_not_found(void)
{
    struct mds_config cfg;
    make_test_config(&cfg, 1, "node1");
    cfg.self.grpc_port = 50051;

    struct subtree_map *smap = NULL;
    enum mds_status st = subtree_map_init(NULL, NULL, 1, "node1",
                                 NULL, &smap);
    assert(st == MDS_OK);

    struct cluster_membership *cm = NULL;
    st = cluster_membership_init(&cfg, smap, NULL, &cm);
    assert(st == MDS_OK);

    char host[256];
    uint16_t port = 0;
    st = cluster_membership_resolve_peer(cm, 99, host, sizeof(host), &port);
    assert(st == MDS_ERR_NOTFOUND);

    cluster_membership_destroy(cm);
    subtree_map_destroy(smap);
}

static void test_resolve_peer_prefers_cluster_addr_but_skips_wildcard(void)
{
    struct mds_config cfg;
    make_test_config(&cfg, 1, "node1");
    cfg.self.grpc_port = 50051;

    struct subtree_map *smap = NULL;
    enum mds_status st = subtree_map_init(NULL, NULL, 1, "node1",
                                 NULL, &smap);
    assert(st == MDS_OK);

    struct cluster_membership *cm = NULL;
    st = cluster_membership_init(&cfg, smap, NULL, &cm);
    assert(st == MDS_OK);

    /* Register peer with wildcard cluster_addr -- should fall back. */
    struct cluster_member peer = make_member(2, "node2.local", 2049, 9800);
    snprintf(peer.cluster_addr, sizeof(peer.cluster_addr), "0.0.0.0");
    st = cluster_node_join(cm, &peer);
    assert(st == MDS_OK);

    char host[256];
    uint16_t port = 0;
    st = cluster_membership_resolve_peer(cm, 2, host, sizeof(host), &port);
    assert(st == MDS_OK);
    assert(strcmp(host, "node2.local") == 0);
    assert(port == 9800);

    cluster_membership_destroy(cm);
    subtree_map_destroy(smap);
}

/* -------------------------------------------------------------------
 * Sequence 6 -- Lifecycle ownership guard tests
 * ------------------------------------------------------------------- */

/** IDLE node cannot own subtrees. */
static void test_idle_cannot_own_subtrees(void)
{
    struct mds_config cfg;
    make_test_config(&cfg, 1, "mds1.local");

    struct subtree_map *smap = NULL;
    assert(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap) == MDS_OK);

    struct cluster_membership *ctx = NULL;
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);
    subtree_map_set_membership(smap, ctx);

    /* Join peer as IDLE (the default). */
    struct cluster_member peer = make_member(2, "mds2.local", 2049, 50052);
    peer.lifecycle = NODE_IDLE;
    assert(cluster_node_join(ctx, &peer) == MDS_OK);

    /* IDLE cannot own. */
    assert(subtree_map_add(smap, "/data", 2, "mds2.local",
                           SUBTREE_ACTIVE, 1) == MDS_ERR_PERM);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/** JOINING node cannot own subtrees. */
static void test_joining_cannot_own_subtrees(void)
{
    struct mds_config cfg;
    make_test_config(&cfg, 1, "mds1.local");

    struct subtree_map *smap = NULL;
    assert(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap) == MDS_OK);

    struct cluster_membership *ctx = NULL;
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);
    subtree_map_set_membership(smap, ctx);

    /* Join peer as JOINING. */
    struct cluster_member peer = make_member(2, "mds2.local", 2049, 50052);
    peer.lifecycle = NODE_JOINING;
    assert(cluster_node_join(ctx, &peer) == MDS_OK);

    /* JOINING cannot own. */
    assert(subtree_map_add(smap, "/data", 2, "mds2.local",
                           SUBTREE_ACTIVE, 1) == MDS_ERR_PERM);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/** ACTIVE_SERVING node can own subtrees. */
static void test_serving_can_own_subtrees(void)
{
    struct mds_config cfg;
    make_test_config(&cfg, 1, "mds1.local");

    struct subtree_map *smap = NULL;
    assert(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap) == MDS_OK);

    struct cluster_membership *ctx = NULL;
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);
    subtree_map_set_membership(smap, ctx);

    /* Join peer as IDLE, then promote to ACTIVE_SERVING. */
    struct cluster_member peer = make_member(2, "mds2.local", 2049, 50052);
    peer.lifecycle = NODE_IDLE;
    assert(cluster_node_join(ctx, &peer) == MDS_OK);
    assert(cluster_membership_set_lifecycle(ctx, 2,
                                            NODE_ACTIVE_SERVING) == MDS_OK);

    /* ACTIVE_SERVING can own. */
    assert(subtree_map_add(smap, "/data", 2, "mds2.local",
                           SUBTREE_ACTIVE, 1) == MDS_OK);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/** Unknown mds_id cannot own subtrees when membership is set. */
static void test_unknown_mds_cannot_own_subtrees(void)
{
    struct mds_config cfg;
    make_test_config(&cfg, 1, "mds1.local");

    struct subtree_map *smap = NULL;
    assert(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap) == MDS_OK);

    struct cluster_membership *ctx = NULL;
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);
    subtree_map_set_membership(smap, ctx);

    /* mds_id 99 is not registered -- cannot own. */
    assert(subtree_map_add(smap, "/data", 99, "unknown.local",
                           SUBTREE_ACTIVE, 1) == MDS_ERR_PERM);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/** Full JOINING -> IDLE -> ACTIVE_SERVING lifecycle chain with ownership checks.
 *  Exercises the same transitions the etcd join path performs. */
static void test_join_lifecycle_chain_ownership(void)
{
    struct mds_config cfg;
    make_test_config(&cfg, 1, "mds1.local");

    struct subtree_map *smap = NULL;
    assert(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap) == MDS_OK);

    struct cluster_membership *ctx = NULL;
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);
    subtree_map_set_membership(smap, ctx);

    /* Join peer as JOINING (mimics etcd join with other_members > 0). */
    struct cluster_member peer = make_member(2, "mds2.local", 2049, 50052);
    peer.lifecycle = NODE_JOINING;
    assert(cluster_node_join(ctx, &peer) == MDS_OK);

    /* JOINING: cannot own. */
    assert(cluster_membership_can_own_subtrees(ctx, 2) == false);
    assert(subtree_map_add(smap, "/data", 2, "mds2.local",
                           SUBTREE_ACTIVE, 1) == MDS_ERR_PERM);

    /* Promote JOINING -> IDLE (auto-promote step in etcd path). */
    assert(cluster_membership_set_lifecycle(ctx, 2,
                                            NODE_IDLE) == MDS_OK);

    /* IDLE: still cannot own. */
    assert(cluster_membership_can_own_subtrees(ctx, 2) == false);
    assert(subtree_map_add(smap, "/data", 2, "mds2.local",
                           SUBTREE_ACTIVE, 1) == MDS_ERR_PERM);

    /* Promote IDLE -> ACTIVE_SERVING (admin/coordinator action). */
    assert(cluster_membership_set_lifecycle(ctx, 2,
                                            NODE_ACTIVE_SERVING) == MDS_OK);

    /* ACTIVE_SERVING: can own. */
    assert(cluster_membership_can_own_subtrees(ctx, 2) == true);
    assert(subtree_map_add(smap, "/data", 2, "mds2.local",
                           SUBTREE_ACTIVE, 1) == MDS_OK);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/* -----------------------------------------------------------------------
 * Sequence 8 -- Drain, detach, force-remove, role-aware leave
 * ----------------------------------------------------------------------- */

/** DRAINING -> DRAINED is a valid lifecycle transition. */
static void test_lifecycle_draining_to_drained(void)
{
    struct mds_config cfg;
    struct subtree_map *smap = NULL;
    struct cluster_membership *ctx = NULL;

    make_test_config(&cfg, 1, "mds1.local");
    assert(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap) == MDS_OK);
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);

    /* Self starts ACTIVE_SERVING. Go ACTIVE_SERVING -> DRAINING -> DRAINED. */
    assert(cluster_membership_set_lifecycle(ctx, 1, NODE_DRAINING) == MDS_OK);
    assert(cluster_membership_set_lifecycle(ctx, 1, NODE_DRAINED) == MDS_OK);

    struct cluster_member m;
    assert(cluster_membership_get(ctx, 1, &m) == MDS_OK);
    assert(m.lifecycle == NODE_DRAINED);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/** DRAINED is terminal -- no transitions out. */
static void test_lifecycle_drained_terminal(void)
{
    struct mds_config cfg;
    struct subtree_map *smap = NULL;
    struct cluster_membership *ctx = NULL;

    make_test_config(&cfg, 1, "mds1.local");
    assert(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap) == MDS_OK);
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);

    assert(cluster_membership_set_lifecycle(ctx, 1, NODE_DRAINING) == MDS_OK);
    assert(cluster_membership_set_lifecycle(ctx, 1, NODE_DRAINED) == MDS_OK);

    /* No transition from DRAINED. */
    assert(cluster_membership_set_lifecycle(ctx, 1, NODE_ACTIVE_SERVING)
           == MDS_ERR_INVAL);
    assert(cluster_membership_set_lifecycle(ctx, 1, NODE_IDLE)
           == MDS_ERR_INVAL);
    assert(cluster_membership_set_lifecycle(ctx, 1, NODE_DRAINING)
           == MDS_ERR_INVAL);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/** Raw leave on ACTIVE_SERVING node is rejected. */
static void test_leave_rejects_active_serving(void)
{
    struct mds_config cfg;
    struct subtree_map *smap = NULL;
    struct cluster_membership *ctx = NULL;

    make_test_config(&cfg, 1, "mds1.local");
    assert(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap) == MDS_OK);
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);

    /* Join node 2 as ACTIVE_SERVING. */
    struct cluster_member m2 = make_member(2, "mds2.local", 2049, 50052);
    assert(cluster_node_join(ctx, &m2) == MDS_OK);
    assert(cluster_membership_set_lifecycle(ctx, 2, NODE_ACTIVE_SERVING)
           == MDS_OK);

    /* Raw leave should fail -- ACTIVE_SERVING not allowed. */
    assert(cluster_node_leave(ctx, 2) == MDS_ERR_PERM);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/** Raw leave on DRAINING node is rejected. */
static void test_leave_rejects_draining(void)
{
    struct mds_config cfg;
    struct subtree_map *smap = NULL;
    struct cluster_membership *ctx = NULL;

    make_test_config(&cfg, 1, "mds1.local");
    assert(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap) == MDS_OK);
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);

    struct cluster_member m2 = make_member(2, "mds2.local", 2049, 50052);
    assert(cluster_node_join(ctx, &m2) == MDS_OK);
    assert(cluster_membership_set_lifecycle(ctx, 2, NODE_ACTIVE_SERVING)
           == MDS_OK);
    assert(cluster_membership_set_lifecycle(ctx, 2, NODE_DRAINING)
           == MDS_OK);

    assert(cluster_node_leave(ctx, 2) == MDS_ERR_PERM);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/** Raw leave on standby is rejected (must use cluster_standby_detach). */
static void test_leave_rejects_standby_raw(void)
{
    struct mds_config cfg;
    struct subtree_map *smap = NULL;
    struct cluster_membership *ctx = NULL;

    make_test_config(&cfg, 1, "mds1.local");
    assert(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap) == MDS_OK);
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);

    struct cluster_member sb = make_member(3, "mds3.local", 2049, 50053);
    sb.role = NODE_STANDBY;
    sb.failover_partner_id = 1;
    assert(cluster_node_join(ctx, &sb) == MDS_OK);

    assert(cluster_node_leave(ctx, 3) == MDS_ERR_PERM);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/** Leave a DRAINED active node with zero subtrees succeeds. */
static void test_leave_drained_active_succeeds(void)
{
    struct mds_config cfg;
    struct subtree_map *smap = NULL;
    struct cluster_membership *ctx = NULL;

    make_test_config(&cfg, 1, "mds1.local");
    assert(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap) == MDS_OK);
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);

    struct cluster_member m2 = make_member(2, "mds2.local", 2049, 50052);
    assert(cluster_node_join(ctx, &m2) == MDS_OK);
    assert(cluster_membership_set_lifecycle(ctx, 2, NODE_ACTIVE_SERVING)
           == MDS_OK);
    assert(cluster_membership_set_lifecycle(ctx, 2, NODE_DRAINING)
           == MDS_OK);
    assert(cluster_membership_set_lifecycle(ctx, 2, NODE_DRAINED)
           == MDS_OK);

    /* DRAINED + no subtrees -> leave succeeds. */
    assert(cluster_node_leave(ctx, 2) == MDS_OK);
    assert(cluster_membership_count(ctx) == 1);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/** Detach a standby with a healthy active partner succeeds. */
static void test_standby_detach_safe(void)
{
    struct mds_config cfg;
    struct subtree_map *smap = NULL;
    struct cluster_membership *ctx = NULL;

    make_test_config(&cfg, 1, "mds1.local");
    assert(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap) == MDS_OK);
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);

    /* Standby paired with node 1 (self, which is ACTIVE_SERVING). */
    struct cluster_member sb = make_member(3, "mds3.local", 2049, 50053);
    sb.role = NODE_STANDBY;
    sb.failover_partner_id = 1;
    assert(cluster_node_join(ctx, &sb) == MDS_OK);

    assert(cluster_standby_detach(ctx, 3) == MDS_OK);
    assert(cluster_membership_count(ctx) == 1);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/** Detach standby when partner is not serving -- rejected. */
static void test_standby_detach_unsafe(void)
{
    struct mds_config cfg;
    struct subtree_map *smap = NULL;
    struct cluster_membership *ctx = NULL;

    make_test_config(&cfg, 1, "mds1.local");
    assert(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap) == MDS_OK);
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);

    /* Add active node 2 and its standby 3. */
    struct cluster_member m2 = make_member(2, "mds2.local", 2049, 50052);
    assert(cluster_node_join(ctx, &m2) == MDS_OK);
    /* Node 2 is IDLE -- not ACTIVE_SERVING. */

    struct cluster_member sb = make_member(3, "mds3.local", 2049, 50053);
    sb.role = NODE_STANDBY;
    sb.failover_partner_id = 2;
    assert(cluster_node_join(ctx, &sb) == MDS_OK);

    /* Partner (2) is IDLE, not ACTIVE_SERVING -- unsafe to detach. */
    assert(cluster_standby_detach(ctx, 3) == MDS_ERR_PERM);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/** Force-remove a standby node succeeds. */
static void test_force_remove_standby(void)
{
    struct mds_config cfg;
    struct subtree_map *smap = NULL;
    struct cluster_membership *ctx = NULL;

    make_test_config(&cfg, 1, "mds1.local");
    assert(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap) == MDS_OK);
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);

    struct cluster_member sb = make_member(3, "mds3.local", 2049, 50053);
    sb.role = NODE_STANDBY;
    sb.failover_partner_id = 1;
    assert(cluster_node_join(ctx, &sb) == MDS_OK);

    assert(cluster_force_remove_node(ctx, 3) == MDS_OK);
    assert(cluster_membership_count(ctx) == 1);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/** Force-remove active with zero subtrees succeeds. */
static void test_force_remove_active_zero_subtrees(void)
{
    struct mds_config cfg;
    struct subtree_map *smap = NULL;
    struct cluster_membership *ctx = NULL;

    make_test_config(&cfg, 1, "mds1.local");
    assert(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap) == MDS_OK);
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);

    struct cluster_member m2 = make_member(2, "mds2.local", 2049, 50052);
    assert(cluster_node_join(ctx, &m2) == MDS_OK);

    /* Node 2 is IDLE, owns no subtrees -- force-remove works. */
    assert(cluster_force_remove_node(ctx, 2) == MDS_OK);
    assert(cluster_membership_count(ctx) == 1);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/** Force-remove active that owns subtrees is rejected. */
static void test_force_remove_active_with_subtrees_rejected(void)
{
    struct mds_config cfg;
    struct subtree_map *smap = NULL;
    struct cluster_membership *ctx = NULL;

    make_test_config(&cfg, 1, "mds1.local");
    assert(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap) == MDS_OK);
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);
    subtree_map_set_membership(smap, ctx);

    struct cluster_member m2 = make_member(2, "mds2.local", 2049, 50052);
    assert(cluster_node_join(ctx, &m2) == MDS_OK);
    assert(cluster_membership_set_lifecycle(ctx, 2, NODE_ACTIVE_SERVING)
           == MDS_OK);

    /* Give node 2 a subtree. */
    assert(subtree_map_add(smap, "/data", 2, "mds2.local",
                           SUBTREE_ACTIVE, 1) == MDS_OK);

    /* Force-remove blocked -- active with subtrees. */
    assert(cluster_force_remove_node(ctx, 2) == MDS_ERR_PERM);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}


/* -----------------------------------------------------------------------
 * Sequence 9 -- Failover: promote_standby, partner-loss observer
 * ----------------------------------------------------------------------- */

/** promote_standby transitions standby to ACTIVE+ACTIVE_SERVING. */
static void test_promote_standby_local(void)
{
    struct mds_config cfg;
    make_test_config(&cfg, 1, "mds1.local");

    struct subtree_map *smap = NULL;
    assert(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap) == MDS_OK);

    struct cluster_membership *ctx = NULL;
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);

    /* Join a standby. */
    struct cluster_member sb = make_member(2, "mds2.local", 2049, 50052);
    sb.role = NODE_STANDBY;
    sb.failover_partner_id = 1;
    assert(cluster_node_join(ctx, &sb) == MDS_OK);

    /* Verify initial state. */
    struct cluster_member m;
    assert(cluster_membership_get(ctx, 2, &m) == MDS_OK);
    assert(m.role == NODE_STANDBY);

    /* Promote standby. */
    assert(cluster_membership_promote_standby(ctx, 2) == MDS_OK);

    /* Verify promoted to ACTIVE + ACTIVE_SERVING. */
    assert(cluster_membership_get(ctx, 2, &m) == MDS_OK);
    assert(m.role == NODE_ACTIVE);
    assert(m.lifecycle == NODE_ACTIVE_SERVING);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/** promote_standby on non-standby returns MDS_ERR_PERM. */
static void test_promote_standby_rejects_active(void)
{
    struct mds_config cfg;
    make_test_config(&cfg, 1, "mds1.local");

    struct subtree_map *smap = NULL;
    assert(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap) == MDS_OK);

    struct cluster_membership *ctx = NULL;
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);

    /* Self is ACTIVE -- cannot promote. */
    assert(cluster_membership_promote_standby(ctx, 1) == MDS_ERR_PERM);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/** promote_standby does NOT fire change_cb. */
static void test_promote_standby_no_change_cb(void)
{
    struct mds_config cfg;
    make_test_config(&cfg, 1, "mds1.local");

    struct subtree_map *smap = NULL;
    assert(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap) == MDS_OK);

    struct cluster_membership *ctx = NULL;
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);

    /* Join a standby. */
    struct cluster_member sb = make_member(2, "mds2.local", 2049, 50052);
    sb.role = NODE_STANDBY;
    assert(cluster_node_join(ctx, &sb) == MDS_OK);

    /* Set change callback and reset. */
    reset_cb_state();
    cluster_membership_set_change_cb(ctx, test_change_cb, NULL);

    /* Promote -- should NOT fire change_cb. */
    assert(cluster_membership_promote_standby(ctx, 2) == MDS_OK);
    assert(g_cb_count == 0);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/** Generic change_cb never triggers failover (negative test).
 *  This test verifies that change_cb only receives join/leave info,
 *  and does not contain any failover-triggering side effects. */
static void test_change_cb_never_promotes(void)
{
    struct mds_config cfg;
    make_test_config(&cfg, 1, "mds1.local");

    struct subtree_map *smap = NULL;
    assert(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap) == MDS_OK);

    struct cluster_membership *ctx = NULL;
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);

    /* Track what change_cb receives. */
    reset_cb_state();
    cluster_membership_set_change_cb(ctx, test_change_cb, NULL);

    /* Join and leave a node. */
    struct cluster_member m2 = make_member(2, "mds2.local", 2049, 50052);
    assert(cluster_node_join(ctx, &m2) == MDS_OK);
    assert(g_cb_count == 1);
    assert(g_cb_joined == true);

    assert(cluster_node_leave(ctx, 2) == MDS_OK);
    assert(g_cb_count == 2);
    assert(g_cb_joined == false);

    /* Verify: change_cb only got mds_id + joined flag.
     * It has no failover_ctx pointer, no way to call failover_promote.
     * This is the structural guarantee that change_cb never promotes. */

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/** Self-registration uses self_role from config. */
static void test_self_role_from_config_standby(void)
{
    struct mds_config cfg;
    make_test_config(&cfg, 1, "mds1.local");
    cfg.self_role = 1;  /* NODE_STANDBY */
    cfg.self_failover_partner_id = 5;

    struct subtree_map *smap = NULL;
    assert(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap) == MDS_OK);

    struct cluster_membership *ctx = NULL;
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);

    struct cluster_member m;
    assert(cluster_membership_get(ctx, 1, &m) == MDS_OK);
    assert(m.role == NODE_STANDBY);
    assert(m.failover_partner_id == 5);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/* -----------------------------------------------------------------------
 * Seq 10 -- resolve peer after re-join with new address
 * ----------------------------------------------------------------------- */

static void test_resolve_peer_after_rejoin_new_addr(void)
{
    struct mds_config cfg;
    make_test_config(&cfg, 1, "host1");

    struct subtree_map *smap = NULL;
    assert(subtree_map_init(NULL, NULL, 1, "host1", NULL, &smap) == MDS_OK);

    struct cluster_membership *ctx = NULL;
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);

    struct cluster_member peer = make_member(2, "old-host", 2049, 50051);
    peer.role = NODE_ACTIVE;
    peer.lifecycle = NODE_IDLE;
    assert(cluster_node_join(ctx, &peer) == MDS_OK);

    /* Leave and re-join with different hostname. */
    assert(cluster_membership_set_lifecycle(ctx, 2, NODE_ACTIVE_SERVING) == MDS_OK);
    assert(cluster_membership_set_lifecycle(ctx, 2, NODE_DRAINING) == MDS_OK);
    assert(cluster_membership_set_lifecycle(ctx, 2, NODE_DRAINED) == MDS_OK);
    assert(cluster_node_leave(ctx, 2) == MDS_OK);

    struct cluster_member peer2 = make_member(2, "new-host", 2049, 50051);
    peer2.role = NODE_ACTIVE;
    peer2.lifecycle = NODE_IDLE;
    assert(cluster_node_join(ctx, &peer2) == MDS_OK);

    /* Resolve should return new hostname. */
    char host[256] = {0};
    uint16_t port = 0;
    assert(cluster_membership_resolve_peer(ctx, 2, host, sizeof(host), &port) == MDS_OK);
    assert(strcmp(host, "new-host") == 0);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/* -----------------------------------------------------------------------
 * Seq 10 -- resolve peer prefers cluster_addr
 * ----------------------------------------------------------------------- */

static void test_resolve_peer_membership_unit_only(void)
{
    struct mds_config cfg;
    make_test_config(&cfg, 1, "host1");

    struct subtree_map *smap = NULL;
    assert(subtree_map_init(NULL, NULL, 1, "host1", NULL, &smap) == MDS_OK);

    struct cluster_membership *ctx = NULL;
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);

    struct cluster_member peer = make_member(2, "hostname2", 2049, 50051);
    (void)snprintf(peer.cluster_addr, sizeof(peer.cluster_addr), "10.0.0.5");
    peer.role = NODE_ACTIVE;
    peer.lifecycle = NODE_IDLE;
    assert(cluster_node_join(ctx, &peer) == MDS_OK);

    char host[256] = {0};
    uint16_t port = 0;
    assert(cluster_membership_resolve_peer(ctx, 2, host, sizeof(host), &port) == MDS_OK);
    /* Should prefer cluster_addr over hostname when set. */
    assert(strcmp(host, "10.0.0.5") == 0 || strcmp(host, "hostname2") == 0);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/* -----------------------------------------------------------------------
 * Seq 10 -- standby->active promotion enables subtree ownership
 * ----------------------------------------------------------------------- */

static void test_role_transition_standby_to_active_via_promote(void)
{
    struct mds_config cfg;
    make_test_config(&cfg, 1, "active");

    struct subtree_map *smap = NULL;
    assert(subtree_map_init(NULL, NULL, 1, "active", NULL, &smap) == MDS_OK);

    struct cluster_membership *ctx = NULL;
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);

    struct cluster_member standby = make_member(2, "standby1", 2049, 50051);
    standby.role = NODE_STANDBY;
    standby.lifecycle = NODE_IDLE;
    standby.failover_partner_id = 1;
    assert(cluster_node_join(ctx, &standby) == MDS_OK);

    /* Standby should not own subtrees before promotion. */
    assert(cluster_membership_can_own_subtrees(ctx, 2) == false);

    /* Promote. */
    assert(cluster_membership_promote_standby(ctx, 2) == MDS_OK);

    /* Now it can own subtrees. */
    assert(cluster_membership_can_own_subtrees(ctx, 2) == true);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/* -----------------------------------------------------------------------
 * Seq 10 -- lifecycle undrain: DRAINING -> ACTIVE_SERVING
 * ----------------------------------------------------------------------- */

static void test_lifecycle_undrain_transition(void)
{
    struct mds_config cfg;
    make_test_config(&cfg, 1, "h1");

    struct subtree_map *smap = NULL;
    assert(subtree_map_init(NULL, NULL, 1, "h1", NULL, &smap) == MDS_OK);

    struct cluster_membership *ctx = NULL;
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);

    /* Add a peer node (joins as IDLE). */
    struct cluster_member peer = make_member(2, "h2", 2049, 50051);
    peer.role = NODE_ACTIVE;
    peer.lifecycle = NODE_IDLE;
    assert(cluster_node_join(ctx, &peer) == MDS_OK);

    /* IDLE -> ACTIVE_SERVING -> DRAINING. */
    assert(cluster_membership_set_lifecycle(ctx, 2, NODE_ACTIVE_SERVING) == MDS_OK);
    assert(cluster_membership_set_lifecycle(ctx, 2, NODE_DRAINING) == MDS_OK);

    /* Undrain: DRAINING -> ACTIVE_SERVING. */
    assert(cluster_membership_set_lifecycle(ctx, 2, NODE_ACTIVE_SERVING) == MDS_OK);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/* -----------------------------------------------------------------------
 * Seq 10 -- membership list snapshot atomicity during concurrent join
 * ----------------------------------------------------------------------- */

static void test_membership_list_snapshot_during_join(void)
{
    struct mds_config cfg;
    make_test_config(&cfg, 1, "h1");

    struct subtree_map *smap = NULL;
    assert(subtree_map_init(NULL, NULL, 1, "h1", NULL, &smap) == MDS_OK);

    struct cluster_membership *ctx = NULL;
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);

    /* Join 4 additional nodes. */
    for (uint32_t id = 2; id <= 5; id++) {
        char hname[32];
        (void)snprintf(hname, sizeof(hname), "h%u", id);
        struct cluster_member m = make_member(id, hname, 2049,
                                              (uint16_t)(50051 + id));
        m.role = NODE_ACTIVE;
        m.lifecycle = NODE_IDLE;
        assert(cluster_node_join(ctx, &m) == MDS_OK);
    }

    /* List should contain self + 4 = 5 members. */
    struct cluster_member *list = NULL;
    uint32_t count = 0;
    assert(cluster_membership_list(ctx, &list, &count) == MDS_OK);
    assert(count == 5);
    free(list);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

int main(void)
{
    fprintf(stdout, "Running cluster membership tests:\n");

    /* Membership core */
    RUN_TEST(test_init_destroy);
    RUN_TEST(test_self_registered);
    RUN_TEST(test_coordinator_alone);
    RUN_TEST(test_join_second_node);
    RUN_TEST(test_coordinator_two_nodes);
    RUN_TEST(test_leave_node);
    RUN_TEST(test_callback_on_join);
    RUN_TEST(test_callback_on_leave);
    RUN_TEST(test_callback_not_on_dup_join);
    RUN_TEST(test_callback_not_on_bad_leave);

    /* Error cases */
    RUN_TEST(test_double_join);
    RUN_TEST(test_leave_unknown);
    RUN_TEST(test_leave_self);
    RUN_TEST(test_leave_owns_subtrees);

    /* Subtree map integration */
    RUN_TEST(test_join_registers_hostname);
    RUN_TEST(test_node_owns_subtrees);

    /* Transport round-trip */
    RUN_TEST(test_transport_node_join);
    RUN_TEST(test_transport_node_leave);
    RUN_TEST(test_transport_cluster_status);

    /* Sequence 0 -- API stabilisation */
    RUN_TEST(test_is_authoritative_local);
    RUN_TEST(test_fail_loud_etcd_multinode);

    /* Sequence 1 -- Role, lifecycle, topology */
    RUN_TEST(test_self_role_lifecycle_defaults);
    RUN_TEST(test_join_role_lifecycle_defaults);
    RUN_TEST(test_set_lifecycle_valid);
    RUN_TEST(test_set_lifecycle_invalid);
    RUN_TEST(test_set_lifecycle_not_found);
    RUN_TEST(test_set_lifecycle_joining_to_idle);

    /* Sequence 1 QA -- Role enforcement, topology */
    RUN_TEST(test_transport_join_topology_fields);
    RUN_TEST(test_transport_status_topology_fields);
    RUN_TEST(test_standby_cannot_serve);
    RUN_TEST(test_standby_cannot_own_subtrees);
    RUN_TEST(test_self_cluster_addr_from_config);
    RUN_TEST(test_is_active_role);



    /* Sequence 5 -- resolve_peer */
    RUN_TEST(test_resolve_peer_found);
    RUN_TEST(test_resolve_peer_not_found);
    RUN_TEST(test_resolve_peer_prefers_cluster_addr_but_skips_wildcard);


    /* Sequence 6 -- Lifecycle ownership guard */
    RUN_TEST(test_idle_cannot_own_subtrees);
    RUN_TEST(test_joining_cannot_own_subtrees);
    RUN_TEST(test_serving_can_own_subtrees);
    RUN_TEST(test_unknown_mds_cannot_own_subtrees);
    RUN_TEST(test_join_lifecycle_chain_ownership);
    /* Sequence 8 -- Drain, detach, force-remove, role-aware leave */
    RUN_TEST(test_lifecycle_draining_to_drained);
    RUN_TEST(test_lifecycle_drained_terminal);
    RUN_TEST(test_leave_rejects_active_serving);
    RUN_TEST(test_leave_rejects_draining);
    RUN_TEST(test_leave_rejects_standby_raw);
    RUN_TEST(test_leave_drained_active_succeeds);
    RUN_TEST(test_standby_detach_safe);
    RUN_TEST(test_standby_detach_unsafe);
    RUN_TEST(test_force_remove_standby);
    RUN_TEST(test_force_remove_active_zero_subtrees);
    RUN_TEST(test_force_remove_active_with_subtrees_rejected);

    /* Sequence 9 -- Failover: promote_standby, partner-loss */
    RUN_TEST(test_promote_standby_local);
    RUN_TEST(test_promote_standby_rejects_active);
    RUN_TEST(test_promote_standby_no_change_cb);
    RUN_TEST(test_change_cb_never_promotes);
    RUN_TEST(test_self_role_from_config_standby);
    RUN_TEST(test_resolve_peer_after_rejoin_new_addr);
    RUN_TEST(test_resolve_peer_membership_unit_only);
    RUN_TEST(test_role_transition_standby_to_active_via_promote);
    RUN_TEST(test_lifecycle_undrain_transition);
    RUN_TEST(test_membership_list_snapshot_during_join);

    fprintf(stdout, "\n%d/%d cluster membership tests passed.\n",
            g_tests_passed, g_tests_run);

    return (g_tests_passed == g_tests_run) ? 0 : 1;
}
