/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_failover.c — Unit tests for the failover promotion state machine.
 *
 * Tests cover: init/destroy, promote happy-path, role guards,
 * partner-alive abort, subtree takeover, grace entry, client
 * recovery tracking, reclaim accept/reject, no-demote invariant,
 * replication health gate, self-fencing guard, failover_take_over,
 * partner-loss filtering, and idempotent promotion.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>

#include "pnfs_mds.h"
#include "test_helpers.h"
#include "mds_coordination.h"
#include "failover.h"
#include "subtree_map.h"
#include "mds_catalogue.h"
#include "grace.h"
#include "cluster_membership.h"
#include "health.h"

/* -------------------------------------------------------------------
 * Test infrastructure
 * ------------------------------------------------------------------- */

static int tests_run;
static int tests_passed;
static int test_failed;

/* 16 MiB — plenty for unit tests. */
#define TEST_MAP_SIZE (16ULL * 1024 * 1024)

#define ASSERT_EQ(a, b) do {                                    \
    if ((a) != (b)) {                                           \
        fprintf(stderr, "  FAIL %s:%d: %s != %s\n",            \
                __FILE__, __LINE__, #a, #b);                    \
        test_failed = 1;                                        \
        return;                                                 \
    }                                                           \
} while (0)

#define ASSERT_NE(a, b) do {                                    \
    if ((a) == (b)) {                                           \
        fprintf(stderr, "  FAIL %s:%d: %s == %s\n",            \
                __FILE__, __LINE__, #a, #b);                    \
        test_failed = 1;                                        \
        return;                                                 \
    }                                                           \
} while (0)

#define ASSERT_TRUE(cond) do {                                  \
    if (!(cond)) {                                              \
        fprintf(stderr, "  FAIL %s:%d: !(%s)\n",               \
                __FILE__, __LINE__, #cond);                     \
        test_failed = 1;                                        \
        return;                                                 \
    }                                                           \
} while (0)

#define RUN_TEST(fn) do {                                       \
    tests_run++;                                                \
    test_failed = 0;                                            \
    fprintf(stdout, "  %-52s", #fn);                            \
    fflush(stdout);                                             \
    fn();                                                       \
    if (!test_failed) {                                         \
        tests_passed++;                                         \
        fprintf(stdout, "PASS\n");                              \
    } else {                                                    \
        fprintf(stdout, "FAIL\n");                              \
    }                                                           \
} while (0)

/* -------------------------------------------------------------------
 * Temp-DB helpers
 * ------------------------------------------------------------------- */

static char *make_temp_db_path(void)
{
    char tmpl[] = "/tmp/pnfs-fo-test-XXXXXX";
    char *dir;
    char *path;
    size_t len;

    dir = mkdtemp(tmpl);
    assert(dir != NULL);

    len = strlen(dir) + sizeof("/data.mdb");
    path = malloc(len);
    assert(path != NULL);
    snprintf(path, len, "%s/data.mdb", dir);
    return path;
}

static void cleanup_temp_db(const char *path)
{
    char lock_path[512];

    unlink(path);
    snprintf(lock_path, sizeof(lock_path), "%s-lock", path);
    unlink(lock_path);

    /* Remove parent directory. */
    {
        char dir[512];
        size_t plen;
        const char *slash;

        slash = strrchr(path, '/');
        if (slash != NULL) {
            plen = (size_t)(slash - path);
            memcpy(dir, path, plen);
            dir[plen] = '\0';
            rmdir(dir);
        }
    }
}

/* -------------------------------------------------------------------
 * Injectable detect callback
 * ------------------------------------------------------------------- */

/* Returns non-zero = partner dead (default). */
static int detect_dead(uint32_t partner_id, void *arg)
{
    (void)partner_id;
    (void)arg;
    return 1;
}

/* Returns 0 = partner alive. */
static int detect_alive(uint32_t partner_id, void *arg)
{
    (void)partner_id;
    (void)arg;
    return 0;
}

/* -------------------------------------------------------------------
 * Shared setup: create map + catalogue + seed two recovery records
 * ------------------------------------------------------------------- */

#define SELF_ID    2
#define PARTNER_ID 1
#define CLIENT_A   0x1001
#define CLIENT_B   0x1002

static void seed_recovery_records(struct mds_catalogue *db)
{
    uint8_t owner_a[] = "clientA.example.com";
    uint8_t owner_b[] = "clientB.example.com";
    uint8_t verifier[8] = {0x01, 0x02, 0x03, 0x04,
                           0x05, 0x06, 0x07, 0x08};
    enum mds_status st;

    st = mds_coord_recovery_put(db, NULL, CLIENT_A,
                                      owner_a, sizeof(owner_a),
                                      verifier);
    assert(st == MDS_OK);

    st = mds_coord_recovery_put(db, NULL, CLIENT_B,
                                      owner_b, sizeof(owner_b),
                                      verifier);
    assert(st == MDS_OK);
}

/* -------------------------------------------------------------------
 * Tests
 * ------------------------------------------------------------------- */

/* 1. Init / destroy round-trip. */
static void test_init_destroy(void)
{
    struct subtree_map *map = NULL;
    struct mds_catalogue *db = NULL;
    struct mds_catalogue *cat = NULL;
    struct failover_ctx *ctx = NULL;
    char *db_path;
    enum mds_status st;

    db_path = make_temp_db_path();
    st = ((db = open_test_catalogue()) != NULL ? MDS_OK : MDS_ERR_IO);
    ASSERT_EQ(st, MDS_OK);
    cat = db;

    st = subtree_map_init(NULL, NULL, SELF_ID, "standby.local",
                                 NULL, &map);
    ASSERT_EQ(st, MDS_OK);

    struct failover_cfg cfg = {
        .self_id          = SELF_ID,
        .partner_id       = PARTNER_ID,
        .map              = map,
        .cat              = cat,
        .grace_period_sec = 90,
        .detect_cb        = detect_dead,
        .detect_arg       = NULL,
        .membership       = NULL,
        .hm               = NULL,
    };
    ASSERT_EQ(failover_init(&cfg, &ctx), MDS_OK);
    ASSERT_NE(ctx, NULL);
    ASSERT_EQ(failover_get_role(ctx), FAILOVER_STANDBY);

    failover_destroy(ctx);
    subtree_map_destroy(map);
    mds_catalogue_close(db);
    cleanup_temp_db(db_path);
    free(db_path);
}

/* 2. NULL parameters rejected. */
static void test_init_null_params(void)
{
    struct failover_ctx *ctx = NULL;

    ASSERT_EQ(failover_init(NULL, &ctx), MDS_ERR_INVAL);
    ASSERT_EQ(ctx, NULL);

    /* Also: NULL output pointer. */
    struct failover_cfg cfg = {0};
    ASSERT_EQ(failover_init(&cfg, NULL), MDS_ERR_INVAL);
}

/* 3. Full promote success: detect dead -> takeover -> grace -> PRIMARY. */
static void test_promote_success(void)
{
    struct subtree_map *map = NULL;
    struct mds_catalogue *db = NULL;
    struct mds_catalogue *cat = NULL;
    struct failover_ctx *ctx = NULL;
    char *db_path;
    enum mds_status st;

    grace_init();

    db_path = make_temp_db_path();
    st = ((db = open_test_catalogue()) != NULL ? MDS_OK : MDS_ERR_IO);
    ASSERT_EQ(st, MDS_OK);
    cat = db;

    st = subtree_map_init(NULL, NULL, SELF_ID, "standby.local",
                                 NULL, &map);
    ASSERT_EQ(st, MDS_OK);

    /* Add a subtree owned by the partner. */
    st = subtree_map_add(map, "/data", PARTNER_ID, "primary.local",
                         SUBTREE_ACTIVE, 1);
    ASSERT_EQ(st, MDS_OK);

    seed_recovery_records(db);

    struct failover_cfg cfg = {
        .self_id          = SELF_ID,
        .partner_id       = PARTNER_ID,
        .map              = map,
        .cat              = cat,
        .grace_period_sec = 90,
        .detect_cb        = detect_dead,
        .detect_arg       = NULL,
        .membership       = NULL,
        .hm               = NULL,
    };
    ASSERT_EQ(failover_init(&cfg, &ctx), MDS_OK);
    ASSERT_EQ(failover_promote(ctx), MDS_OK);
    ASSERT_EQ(failover_get_role(ctx), FAILOVER_PRIMARY);

    /* /data should now be owned by SELF_ID. */
    struct subtree_entry entry;
    st = subtree_map_lookup(map, "/data", &entry);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(entry.owner_mds_id, (uint32_t)SELF_ID);

    /* Grace should be active. */
    ASSERT_TRUE(grace_is_active());

    grace_exit();
    failover_destroy(ctx);
    subtree_map_destroy(map);
    mds_catalogue_close(db);
    cleanup_temp_db(db_path);
    free(db_path);
}

/* 4. Promote from non-STANDBY is rejected (idempotent second promote). */
static void test_promote_from_non_standby(void)
{
    struct subtree_map *map = NULL;
    struct mds_catalogue *db = NULL;
    struct mds_catalogue *cat = NULL;
    struct failover_ctx *ctx = NULL;
    char *db_path;
    enum mds_status st;

    grace_init();

    db_path = make_temp_db_path();
    st = ((db = open_test_catalogue()) != NULL ? MDS_OK : MDS_ERR_IO);
    ASSERT_EQ(st, MDS_OK);
    cat = db;

    st = subtree_map_init(NULL, NULL, SELF_ID, "standby.local",
                                 NULL, &map);
    ASSERT_EQ(st, MDS_OK);

    st = subtree_map_add(map, "/data", PARTNER_ID, "primary.local",
                         SUBTREE_ACTIVE, 1);
    ASSERT_EQ(st, MDS_OK);

    seed_recovery_records(db);

    struct failover_cfg cfg = {
        .self_id          = SELF_ID,
        .partner_id       = PARTNER_ID,
        .map              = map,
        .cat              = cat,
        .grace_period_sec = 90,
        .detect_cb        = detect_dead,
        .detect_arg       = NULL,
        .membership       = NULL,
        .hm               = NULL,
    };
    ASSERT_EQ(failover_init(&cfg, &ctx), MDS_OK);

    /* First promote succeeds. */
    ASSERT_EQ(failover_promote(ctx), MDS_OK);
    ASSERT_EQ(failover_get_role(ctx), FAILOVER_PRIMARY);

    /* Second promote must fail — already PRIMARY. */
    ASSERT_EQ(failover_promote(ctx), MDS_ERR_PERM);
    ASSERT_EQ(failover_get_role(ctx), FAILOVER_PRIMARY);

    grace_exit();
    failover_destroy(ctx);
    subtree_map_destroy(map);
    mds_catalogue_close(db);
    cleanup_temp_db(db_path);
    free(db_path);
}

/* 5. Partner alive — promote aborted, stays STANDBY. */
static void test_promote_partner_alive(void)
{
    struct subtree_map *map = NULL;
    struct mds_catalogue *db = NULL;
    struct mds_catalogue *cat = NULL;
    struct failover_ctx *ctx = NULL;
    char *db_path;
    enum mds_status st;

    db_path = make_temp_db_path();
    st = ((db = open_test_catalogue()) != NULL ? MDS_OK : MDS_ERR_IO);
    ASSERT_EQ(st, MDS_OK);
    cat = db;

    st = subtree_map_init(NULL, NULL, SELF_ID, "standby.local",
                                 NULL, &map);
    ASSERT_EQ(st, MDS_OK);

    struct failover_cfg cfg = {
        .self_id          = SELF_ID,
        .partner_id       = PARTNER_ID,
        .map              = map,
        .cat              = cat,
        .grace_period_sec = 90,
        .detect_cb        = detect_alive,  /* partner alive */
        .detect_arg       = NULL,
        .membership       = NULL,
        .hm               = NULL,
    };
    ASSERT_EQ(failover_init(&cfg, &ctx), MDS_OK);
    ASSERT_EQ(failover_promote(ctx), MDS_ERR_PERM);
    ASSERT_EQ(failover_get_role(ctx), FAILOVER_STANDBY);

    failover_destroy(ctx);
    subtree_map_destroy(map);
    mds_catalogue_close(db);
    cleanup_temp_db(db_path);
    free(db_path);
}

/* 6. Subtree takeover reassigns all partner subtrees (failover path). */
static void test_subtree_takeover(void)
{
    struct subtree_map *map = NULL;
    enum mds_status st;

    st = subtree_map_init(NULL, NULL, SELF_ID, "standby.local",
                                 NULL, &map);
    ASSERT_EQ(st, MDS_OK);

    st = subtree_map_add(map, "/data", PARTNER_ID, "primary.local",
                         SUBTREE_ACTIVE, 1);
    ASSERT_EQ(st, MDS_OK);

    st = subtree_map_add(map, "/home", PARTNER_ID, "primary.local",
                         SUBTREE_ACTIVE, 1);
    ASSERT_EQ(st, MDS_OK);

    uint32_t taken = 0;
    st = subtree_map_failover_take_over(map, PARTNER_ID, SELF_ID, &taken);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(taken, 2U);

    /* Verify both reassigned. */
    struct subtree_entry e;
    st = subtree_map_lookup(map, "/data", &e);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(e.owner_mds_id, (uint32_t)SELF_ID);

    st = subtree_map_lookup(map, "/home", &e);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(e.owner_mds_id, (uint32_t)SELF_ID);

    /* Root ("/") should still be ours, not taken over. */
    st = subtree_map_lookup(map, "/", &e);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(e.owner_mds_id, (uint32_t)SELF_ID);

    subtree_map_destroy(map);
}

/* 7. Grace active after successful promote. */
static void test_promote_enters_grace(void)
{
    struct subtree_map *map = NULL;
    struct mds_catalogue *db = NULL;
    struct mds_catalogue *cat = NULL;
    struct failover_ctx *ctx = NULL;
    char *db_path;
    enum mds_status st;

    grace_init();

    db_path = make_temp_db_path();
    st = ((db = open_test_catalogue()) != NULL ? MDS_OK : MDS_ERR_IO);
    ASSERT_EQ(st, MDS_OK);
    cat = db;

    st = subtree_map_init(NULL, NULL, SELF_ID, "standby.local",
                                 NULL, &map);
    ASSERT_EQ(st, MDS_OK);

    seed_recovery_records(db);

    struct failover_cfg cfg = {
        .self_id          = SELF_ID,
        .partner_id       = PARTNER_ID,
        .map              = map,
        .cat              = cat,
        .grace_period_sec = 90,
        .detect_cb        = detect_dead,
        .detect_arg       = NULL,
        .membership       = NULL,
        .hm               = NULL,
    };
    ASSERT_EQ(failover_init(&cfg, &ctx), MDS_OK);
    ASSERT_EQ(failover_promote(ctx), MDS_OK);

    ASSERT_TRUE(grace_is_active());
    ASSERT_EQ(grace_pending_count(), 2U);

    grace_exit();
    failover_destroy(ctx);
    subtree_map_destroy(map);
    mds_catalogue_close(db);
    cleanup_temp_db(db_path);
    free(db_path);
}

/* 8. Recovery clients tracked: both A and B are recovering. */
static void test_grace_client_tracking(void)
{
    struct subtree_map *map = NULL;
    struct mds_catalogue *db = NULL;
    struct mds_catalogue *cat = NULL;
    struct failover_ctx *ctx = NULL;
    char *db_path;
    enum mds_status st;

    grace_init();

    db_path = make_temp_db_path();
    st = ((db = open_test_catalogue()) != NULL ? MDS_OK : MDS_ERR_IO);
    ASSERT_EQ(st, MDS_OK);
    cat = db;

    st = subtree_map_init(NULL, NULL, SELF_ID, "standby.local",
                                 NULL, &map);
    ASSERT_EQ(st, MDS_OK);

    seed_recovery_records(db);

    struct failover_cfg cfg = {
        .self_id          = SELF_ID,
        .partner_id       = PARTNER_ID,
        .map              = map,
        .cat              = cat,
        .grace_period_sec = 90,
        .detect_cb        = detect_dead,
        .detect_arg       = NULL,
        .membership       = NULL,
        .hm               = NULL,
    };
    ASSERT_EQ(failover_init(&cfg, &ctx), MDS_OK);
    ASSERT_EQ(failover_promote(ctx), MDS_OK);

    ASSERT_TRUE(grace_client_is_recovering(CLIENT_A));
    ASSERT_TRUE(grace_client_is_recovering(CLIENT_B));

    /* Unknown client is NOT recovering. */
    ASSERT_TRUE(!grace_client_is_recovering(0xDEAD));

    grace_exit();
    failover_destroy(ctx);
    subtree_map_destroy(map);
    mds_catalogue_close(db);
    cleanup_temp_db(db_path);
    free(db_path);
}

/* 9. Reclaim accepted — grace_client_reclaimed succeeds, count drops. */
static void test_reclaim_accepted(void)
{
    struct subtree_map *map = NULL;
    struct mds_catalogue *db = NULL;
    struct mds_catalogue *cat = NULL;
    struct failover_ctx *ctx = NULL;
    char *db_path;
    enum mds_status st;

    grace_init();

    db_path = make_temp_db_path();
    st = ((db = open_test_catalogue()) != NULL ? MDS_OK : MDS_ERR_IO);
    ASSERT_EQ(st, MDS_OK);
    cat = db;

    st = subtree_map_init(NULL, NULL, SELF_ID, "standby.local",
                                 NULL, &map);
    ASSERT_EQ(st, MDS_OK);

    seed_recovery_records(db);

    struct failover_cfg cfg = {
        .self_id          = SELF_ID,
        .partner_id       = PARTNER_ID,
        .map              = map,
        .cat              = cat,
        .grace_period_sec = 90,
        .detect_cb        = detect_dead,
        .detect_arg       = NULL,
        .membership       = NULL,
        .hm               = NULL,
    };
    ASSERT_EQ(failover_init(&cfg, &ctx), MDS_OK);
    ASSERT_EQ(failover_promote(ctx), MDS_OK);

    ASSERT_EQ(grace_pending_count(), 2U);

    /* Client A reclaims. */
    ASSERT_EQ(grace_client_reclaimed(CLIENT_A), 0);
    ASSERT_EQ(grace_pending_count(), 1U);
    ASSERT_TRUE(!grace_client_is_recovering(CLIENT_A));

    /* Client B reclaims — auto-exits grace. */
    ASSERT_EQ(grace_client_reclaimed(CLIENT_B), 0);
    ASSERT_EQ(grace_pending_count(), 0U);
    ASSERT_TRUE(!grace_is_active());

    failover_destroy(ctx);
    subtree_map_destroy(map);
    mds_catalogue_close(db);
    cleanup_temp_db(db_path);
    free(db_path);
}

/* 10. Reclaim rejected for unknown client. */
static void test_reclaim_rejected_unknown(void)
{
    struct subtree_map *map = NULL;
    struct mds_catalogue *db = NULL;
    struct mds_catalogue *cat = NULL;
    struct failover_ctx *ctx = NULL;
    char *db_path;
    enum mds_status st;

    grace_init();

    db_path = make_temp_db_path();
    st = ((db = open_test_catalogue()) != NULL ? MDS_OK : MDS_ERR_IO);
    ASSERT_EQ(st, MDS_OK);
    cat = db;

    st = subtree_map_init(NULL, NULL, SELF_ID, "standby.local",
                                 NULL, &map);
    ASSERT_EQ(st, MDS_OK);

    seed_recovery_records(db);

    struct failover_cfg cfg = {
        .self_id          = SELF_ID,
        .partner_id       = PARTNER_ID,
        .map              = map,
        .cat              = cat,
        .grace_period_sec = 90,
        .detect_cb        = detect_dead,
        .detect_arg       = NULL,
        .membership       = NULL,
        .hm               = NULL,
    };
    ASSERT_EQ(failover_init(&cfg, &ctx), MDS_OK);
    ASSERT_EQ(failover_promote(ctx), MDS_OK);

    /* Unknown client is rejected. */
    ASSERT_EQ(grace_client_reclaimed(0xDEAD), -1);

    /* Count unchanged. */
    ASSERT_EQ(grace_pending_count(), 2U);

    grace_exit();
    failover_destroy(ctx);
    subtree_map_destroy(map);
    mds_catalogue_close(db);
    cleanup_temp_db(db_path);
    free(db_path);
}

/* -------------------------------------------------------------------
 * Seq 9 — New tests
 * ------------------------------------------------------------------- */

/* 11. Replication health gate — promotion rejected when repl unhealthy. */
static void test_promote_repl_unhealthy(void)
{
    struct subtree_map *map = NULL;
    struct mds_catalogue *db = NULL;
    struct mds_catalogue *cat = NULL;
    struct failover_ctx *ctx = NULL;
    struct health_monitor *hm = NULL;
    char *db_path;
    enum mds_status st;

    db_path = make_temp_db_path();
    st = ((db = open_test_catalogue()) != NULL ? MDS_OK : MDS_ERR_IO);
    ASSERT_EQ(st, MDS_OK);
    cat = db;

    st = subtree_map_init(NULL, NULL, SELF_ID, "standby.local",
                                 NULL, &map);
    ASSERT_EQ(st, MDS_OK);

    /* Create health monitor and set replication unhealthy. */
    ASSERT_EQ(health_monitor_init(NULL, 5000, false, &hm), 0);
    health_monitor_test_set_repl_ok(hm, 0);

    struct failover_cfg cfg = {
        .self_id          = SELF_ID,
        .partner_id       = PARTNER_ID,
        .map              = map,
        .cat              = cat,
        .grace_period_sec = 90,
        .detect_cb        = detect_dead,
        .detect_arg       = NULL,
        .membership       = NULL,
        .hm               = hm,
    };
    ASSERT_EQ(failover_init(&cfg, &ctx), MDS_OK);

    /* Promote should be rejected due to unhealthy replication. */
    ASSERT_EQ(failover_promote(ctx), MDS_ERR_PERM);
    ASSERT_EQ(failover_get_role(ctx), FAILOVER_STANDBY);

    failover_destroy(ctx);
    health_monitor_destroy(hm);
    subtree_map_destroy(map);
    mds_catalogue_close(db);
    cleanup_temp_db(db_path);
    free(db_path);
}

/* 12. Init succeeds with detect_cb=NULL (authoritative mode). */
static void test_init_no_detect_cb(void)
{
    struct subtree_map *map = NULL;
    struct mds_catalogue *db = NULL;
    struct mds_catalogue *cat = NULL;
    struct failover_ctx *ctx = NULL;
    char *db_path;
    enum mds_status st;

    grace_init();

    db_path = make_temp_db_path();
    st = ((db = open_test_catalogue()) != NULL ? MDS_OK : MDS_ERR_IO);
    ASSERT_EQ(st, MDS_OK);
    cat = db;

    st = subtree_map_init(NULL, NULL, SELF_ID, "standby.local",
                                 NULL, &map);
    ASSERT_EQ(st, MDS_OK);

    seed_recovery_records(db);

    struct failover_cfg cfg = {
        .self_id          = SELF_ID,
        .partner_id       = PARTNER_ID,
        .map              = map,
        .cat              = cat,
        .grace_period_sec = 90,
        .detect_cb        = NULL,  /* authoritative mode */
        .detect_arg       = NULL,
        .membership       = NULL,
        .hm               = NULL,
    };
    /* Init should succeed with detect_cb=NULL. */
    ASSERT_EQ(failover_init(&cfg, &ctx), MDS_OK);

    /* Promote should also succeed (detect_cb skipped). */
    ASSERT_EQ(failover_promote(ctx), MDS_OK);
    ASSERT_EQ(failover_get_role(ctx), FAILOVER_PRIMARY);

    grace_exit();
    failover_destroy(ctx);
    subtree_map_destroy(map);
    mds_catalogue_close(db);
    cleanup_temp_db(db_path);
    free(db_path);
}

/* 13. failover_take_over works in local mode (no role check). */
static void test_failover_take_over_local(void)
{
    struct subtree_map *map = NULL;
    enum mds_status st;

    st = subtree_map_init(NULL, NULL, SELF_ID, "standby.local",
                                 NULL, &map);
    ASSERT_EQ(st, MDS_OK);

    st = subtree_map_add(map, "/data", PARTNER_ID, "primary.local",
                         SUBTREE_ACTIVE, 1);
    ASSERT_EQ(st, MDS_OK);

    st = subtree_map_add(map, "/home", PARTNER_ID, "primary.local",
                         SUBTREE_ACTIVE, 1);
    ASSERT_EQ(st, MDS_OK);

    uint32_t taken = 0;
    /* failover_take_over bypasses owner_role_ok. */
    st = subtree_map_failover_take_over(map, PARTNER_ID, SELF_ID, &taken);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(taken, 2U);

    /* Verify ownership transferred. */
    struct subtree_entry e;
    st = subtree_map_lookup(map, "/data", &e);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(e.owner_mds_id, (uint32_t)SELF_ID);

    subtree_map_destroy(map);
}

/* 14. Duplicate promote is idempotent (second returns MDS_ERR_PERM). */
static void test_promote_idempotent(void)
{
    struct subtree_map *map = NULL;
    struct mds_catalogue *db = NULL;
    struct mds_catalogue *cat = NULL;
    struct failover_ctx *ctx = NULL;
    char *db_path;
    enum mds_status st;

    grace_init();

    db_path = make_temp_db_path();
    st = ((db = open_test_catalogue()) != NULL ? MDS_OK : MDS_ERR_IO);
    ASSERT_EQ(st, MDS_OK);
    cat = db;

    st = subtree_map_init(NULL, NULL, SELF_ID, "standby.local",
                                 NULL, &map);
    ASSERT_EQ(st, MDS_OK);

    seed_recovery_records(db);

    struct failover_cfg cfg = {
        .self_id          = SELF_ID,
        .partner_id       = PARTNER_ID,
        .map              = map,
        .cat              = cat,
        .grace_period_sec = 90,
        .detect_cb        = detect_dead,
        .detect_arg       = NULL,
        .membership       = NULL,
        .hm               = NULL,
    };
    ASSERT_EQ(failover_init(&cfg, &ctx), MDS_OK);
    ASSERT_EQ(failover_promote(ctx), MDS_OK);

    /* Second promote: role is now PRIMARY, returns PERM. */
    ASSERT_EQ(failover_promote(ctx), MDS_ERR_PERM);
    ASSERT_EQ(failover_get_role(ctx), FAILOVER_PRIMARY);

    grace_exit();
    failover_destroy(ctx);
    subtree_map_destroy(map);
    mds_catalogue_close(db);
    cleanup_temp_db(db_path);
    free(db_path);
}

/* 15. Promote with membership: promote_standby is called. */
static void test_promote_with_membership(void)
{
    struct subtree_map *map = NULL;
    struct mds_catalogue *db = NULL;
    struct mds_catalogue *cat = NULL;
    struct failover_ctx *ctx = NULL;
    struct cluster_membership *membership = NULL;
    char *db_path;
    enum mds_status st;

    grace_init();

    db_path = make_temp_db_path();
    st = ((db = open_test_catalogue()) != NULL ? MDS_OK : MDS_ERR_IO);
    ASSERT_EQ(st, MDS_OK);
    cat = db;

    st = subtree_map_init(NULL, NULL, SELF_ID, "standby.local",
                                 NULL, &map);
    ASSERT_EQ(st, MDS_OK);

    /* Create membership with self as standby (via config). */
    struct mds_config mcfg;
    memset(&mcfg, 0, sizeof(mcfg));
    mcfg.self.id = SELF_ID;
    snprintf(mcfg.self.hostname, sizeof(mcfg.self.hostname), "standby.local");
    mcfg.self.nfs_port = 2049;
    mcfg.self.grpc_port = 50051;
    mcfg.self_role = 1;  /* NODE_STANDBY */
    mcfg.self_failover_partner_id = PARTNER_ID;

    st = cluster_membership_init(&mcfg, map, NULL, &membership);
    ASSERT_EQ(st, MDS_OK);

    /* Verify self was registered as standby. */
    struct cluster_member self_m;
    st = cluster_membership_get(membership, SELF_ID, &self_m);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(self_m.role, NODE_STANDBY);
    ASSERT_EQ(self_m.failover_partner_id, (uint32_t)PARTNER_ID);

    seed_recovery_records(db);

    struct failover_cfg cfg = {
        .self_id          = SELF_ID,
        .partner_id       = PARTNER_ID,
        .map              = map,
        .cat              = cat,
        .grace_period_sec = 90,
        .detect_cb        = detect_dead,
        .detect_arg       = NULL,
        .membership       = membership,
        .hm               = NULL,
    };
    ASSERT_EQ(failover_init(&cfg, &ctx), MDS_OK);
    ASSERT_EQ(failover_promote(ctx), MDS_OK);
    ASSERT_EQ(failover_get_role(ctx), FAILOVER_PRIMARY);

    /* After promote_standby, self should be ACTIVE + ACTIVE_SERVING. */
    st = cluster_membership_get(membership, SELF_ID, &self_m);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(self_m.role, NODE_ACTIVE);
    ASSERT_EQ(self_m.lifecycle, NODE_ACTIVE_SERVING);

    grace_exit();
    failover_destroy(ctx);
    cluster_membership_destroy(membership);
    subtree_map_destroy(map);
    mds_catalogue_close(db);
    cleanup_temp_db(db_path);
    free(db_path);
}

/* -------------------------------------------------------------------
 * main
 * ------------------------------------------------------------------- */

int main(void)
{
    fprintf(stdout, "Running failover unit tests:\n");

    RUN_TEST(test_init_destroy);
    RUN_TEST(test_init_null_params);
    RUN_TEST(test_promote_success);
    RUN_TEST(test_promote_from_non_standby);
    RUN_TEST(test_promote_partner_alive);
    RUN_TEST(test_subtree_takeover);
    RUN_TEST(test_promote_enters_grace);
    RUN_TEST(test_grace_client_tracking);
    RUN_TEST(test_reclaim_accepted);
    RUN_TEST(test_reclaim_rejected_unknown);

    /* Seq 9 — New tests */
    RUN_TEST(test_promote_repl_unhealthy);
    RUN_TEST(test_init_no_detect_cb);
    RUN_TEST(test_failover_take_over_local);
    RUN_TEST(test_promote_idempotent);
    RUN_TEST(test_promote_with_membership);

    fprintf(stdout, "\n  %d/%d tests passed.\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
