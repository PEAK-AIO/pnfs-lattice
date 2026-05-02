/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_cluster.c — Authoritative-mode etcd cluster integration tests.
 *
 * Each test uses a unique etcd key prefix for isolation.
 * Gated by PNFS_TEST_ETCD=1 environment variable.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>
#include <inttypes.h>
#include <time.h>
#include <stdatomic.h>

#include "pnfs_mds.h"
#include "cluster_membership.h"
#include "cluster_transport.h"
#include "subtree_map.h"
#include "migration.h"

/* -----------------------------------------------------------------------
 * Test infrastructure
 * ----------------------------------------------------------------------- */

static int g_tests_run;
static int g_tests_passed;

#define RUN_TEST(fn)                                                     \
    do {                                                                 \
        g_tests_run++;                                                   \
        fprintf(stdout, "  %-50s ", #fn);                                \
        fn();                                                            \
        g_tests_passed++;                                                \
        fprintf(stdout, "PASS\n");                                       \
    } while (0)


/* -----------------------------------------------------------------------
 * 1. test_etcd_join — node join registers in etcd
 * ----------------------------------------------------------------------- */

#if 0  /* etcd tests removed — coordination is RonDB-native */
static void test_etcd_join(void)
{
    char prefix[256];
    make_test_prefix(prefix, sizeof(prefix), "join");

    struct mds_config cfg;
    make_etcd_config(&cfg, 1, "node1.local", prefix);

    struct subtree_map *smap = NULL;
    assert(subtree_map_init(ETCD_URL, prefix, 1, "node1.local", NULL, &smap) == MDS_OK);

    struct cluster_membership *ctx = NULL;
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);

    /* Add a second node. */
    struct cluster_member m2;
    memset(&m2, 0, sizeof(m2));
    m2.mds_id = 2;
    (void)snprintf(m2.hostname, sizeof(m2.hostname), "node2.local");
    m2.nfs_port = 2049;
    m2.grpc_port = 50053;
    m2.role = NODE_ACTIVE;
    m2.lifecycle = NODE_IDLE;
    assert(cluster_node_join(ctx, &m2) == MDS_OK);

    /* Verify via raw etcd get. */
    struct etcd_client *cl = NULL;
    assert(etcd_client_create(ETCD_URL, NULL, &cl) == MDS_OK);

    char key[512];
    (void)snprintf(key, sizeof(key), "%s/nodes/2", prefix);
    char *val = NULL;
    int64_t rev = 0;
    assert(etcd_kv_get(cl, key, &val, &rev) == MDS_OK);
    assert(val != NULL);
    assert(strstr(val, "node2.local") != NULL);
    free(val);

    /* Verify membership list. */
    struct cluster_member *list = NULL;
    uint32_t count = 0;
    assert(cluster_membership_list(ctx, &list, &count) == MDS_OK);
    assert(count == 2);
    free(list);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
    cleanup_prefix(cl, prefix);
    etcd_client_destroy(cl);
}

/* -----------------------------------------------------------------------
 * 2. test_etcd_subtree_owner — subtree ownership reflected in etcd
 * ----------------------------------------------------------------------- */

static void test_etcd_subtree_owner(void)
{
    char prefix[256];
    make_test_prefix(prefix, sizeof(prefix), "subtree");

    struct subtree_map *smap = NULL;
    assert(subtree_map_init(ETCD_URL, prefix, 1, "node1.local", NULL, &smap) == MDS_OK);

    /* Add a subtree. */
    assert(subtree_map_add(smap, "/data", 1, "node1.local",
                           SUBTREE_ACTIVE, 0) == MDS_OK);

    /* Verify via etcd. */
    struct etcd_client *cl = NULL;
    assert(etcd_client_create(ETCD_URL, NULL, &cl) == MDS_OK);

    char key[512];
    (void)snprintf(key, sizeof(key), "%s/subtrees/data", prefix);
    char *val = NULL;
    int64_t rev = 0;
    assert(etcd_kv_get(cl, key, &val, &rev) == MDS_OK);
    assert(val != NULL);
    free(val);

    /* Snapshot should include root + /data = 2 entries. */
    struct subtree_entry *snap = NULL;
    uint32_t snap_count = 0;
    assert(subtree_map_list_snapshot(smap, &snap, &snap_count) == MDS_OK);
    assert(snap_count >= 2);
    free(snap);

    subtree_map_destroy(smap);
    cleanup_prefix(cl, prefix);
    etcd_client_destroy(cl);
}

/* -----------------------------------------------------------------------
 * 3. test_etcd_referral_update — owner change reflected in lookup
 * ----------------------------------------------------------------------- */

static void test_etcd_referral_update(void)
{
    char prefix[256];
    make_test_prefix(prefix, sizeof(prefix), "referral");

    struct subtree_map *smap = NULL;
    assert(subtree_map_init(ETCD_URL, prefix, 1, "node1.local", NULL, &smap) == MDS_OK);

    assert(subtree_map_add(smap, "/vol1", 1, "node1.local",
                           SUBTREE_ACTIVE, 0) == MDS_OK);

    /* Change owner from 1 → 2. */
    struct subtree_entry entry;
    assert(subtree_map_lookup(smap, "/vol1", &entry) == MDS_OK);
    assert(subtree_map_set_owner(smap, "/vol1", 2,
                                 entry.version) == MDS_OK);

    /* Verify lookup reflects new owner. */
    assert(subtree_map_lookup(smap, "/vol1", &entry) == MDS_OK);
    assert(entry.owner_mds_id == 2);

    struct etcd_client *cl = NULL;
    assert(etcd_client_create(ETCD_URL, NULL, &cl) == MDS_OK);
    subtree_map_destroy(smap);
    cleanup_prefix(cl, prefix);
    etcd_client_destroy(cl);
}

/* -----------------------------------------------------------------------
 * 4. test_etcd_drain_leave — drain + leave lifecycle
 * ----------------------------------------------------------------------- */

static void test_etcd_drain_leave(void)
{
    char prefix[256];
    make_test_prefix(prefix, sizeof(prefix), "drain");

    struct mds_config cfg;
    make_etcd_config(&cfg, 1, "drain-node.local", prefix);

    struct subtree_map *smap = NULL;
    assert(subtree_map_init(ETCD_URL, prefix, 1, "drain-node.local", NULL, &smap) == MDS_OK);

    struct cluster_membership *ctx = NULL;
    assert(cluster_membership_init(&cfg, smap, NULL, &ctx) == MDS_OK);

    /* Verify self was auto-registered. */
    struct cluster_member *list = NULL;
    uint32_t count = 0;
    assert(cluster_membership_list(ctx, &list, &count) == MDS_OK);
    assert(count >= 1);

    /* Find self's lifecycle. */
    bool found = false;
    for (uint32_t i = 0; i < count; i++) {
        if (list[i].mds_id == 1) {
            found = true;
            break;
        }
    }
    assert(found);
    free(list);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);

    struct etcd_client *cl = NULL;
    assert(etcd_client_create(ETCD_URL, NULL, &cl) == MDS_OK);
    cleanup_prefix(cl, prefix);
    etcd_client_destroy(cl);
}

/* -----------------------------------------------------------------------
 * 5. test_etcd_restart_snapshot — destroy + re-init from same prefix
 * ----------------------------------------------------------------------- */

static void test_etcd_restart_snapshot(void)
{
    char prefix[256];
    make_test_prefix(prefix, sizeof(prefix), "restart");

    /* Phase 1: write state. */
    struct subtree_map *smap1 = NULL;
    assert(subtree_map_init(ETCD_URL, prefix, 1, "r-node.local", NULL, &smap1) == MDS_OK);
    assert(subtree_map_add(smap1, "/snap", 1, "r-node.local",
                           SUBTREE_ACTIVE, 0) == MDS_OK);
    subtree_map_destroy(smap1);

    /* Phase 2: re-init from same prefix → should find /snap. */
    struct subtree_map *smap2 = NULL;
    assert(subtree_map_init(ETCD_URL, prefix, 1, "r-node.local", NULL, &smap2) == MDS_OK);

    struct subtree_entry entry;
    assert(subtree_map_lookup(smap2, "/snap", &entry) == MDS_OK);
    assert(entry.owner_mds_id == 1);

    subtree_map_destroy(smap2);

    struct etcd_client *cl = NULL;
    assert(etcd_client_create(ETCD_URL, NULL, &cl) == MDS_OK);
    cleanup_prefix(cl, prefix);
    etcd_client_destroy(cl);
}

/* -----------------------------------------------------------------------
 * 6. test_etcd_migration_tracker — tracker lifecycle
 * ----------------------------------------------------------------------- */

#endif  /* etcd tests removed */

static void test_etcd_migration_tracker(void)
{
    struct migration_tracker *t = NULL;
    assert(migration_tracker_create(&t) == MDS_OK);

    /* Initially idle. */
    enum migration_state mstate = MIG_DONE;
    char mpath[256];
    uint32_t total = 99, done = 99;
    migration_tracker_get_progress(t, &mstate, mpath, sizeof(mpath),
                                   &total, &done);
    assert(mstate == MIG_IDLE);
    assert(mpath[0] == '\0');
    assert(total == 0);
    assert(done == 0);

    /* Begin a migration. */
    assert(migration_tracker_begin(t, "/test/path", 100) == MDS_OK);
    migration_tracker_get_progress(t, &mstate, mpath, sizeof(mpath),
                                   &total, &done);
    assert(mstate == MIG_FREEZING);
    assert(strcmp(mpath, "/test/path") == 0);
    assert(total == 100);
    assert(done == 0);

    /* Second begin should fail (single-active). */
    assert(migration_tracker_begin(t, "/other", 50) == MDS_ERR_DELAY);

    /* Update progress. */
    migration_tracker_update(t, 30);
    migration_tracker_update(t, 20);
    migration_tracker_get_progress(t, &mstate, mpath, sizeof(mpath),
                                   &total, &done);
    assert(done == 50);

    /* Finish. */
    migration_tracker_finish(t, MIG_DONE);
    migration_tracker_get_progress(t, &mstate, mpath, sizeof(mpath),
                                   &total, &done);
    assert(mstate == MIG_DONE);

    /* Can begin again after done. */
    assert(migration_tracker_begin(t, "/another", 10) == MDS_OK);
    migration_tracker_finish(t, MIG_IDLE);

    migration_tracker_destroy(t);

    /* NULL tracker → idle. */
    migration_tracker_get_progress(NULL, &mstate, mpath, sizeof(mpath),
                                   &total, &done);
    assert(mstate == MIG_IDLE);
    assert(done == 0);
}

/* -----------------------------------------------------------------------
 * main
 * ----------------------------------------------------------------------- */

int main(void)
{
    fprintf(stdout, "test_cluster: integration tests\n");

    RUN_TEST(test_etcd_migration_tracker);

    fprintf(stdout, "\ntest_cluster: %d/%d passed\n",
            g_tests_passed, g_tests_run);
    return (g_tests_passed == g_tests_run) ? 0 : 1;
}
