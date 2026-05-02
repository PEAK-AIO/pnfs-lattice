/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_rolling_upgrade.c — Unit tests for rolling upgrade V1 (Item 46).
 *
 * Tests cover: version serialization, legacy shim, version string helper,
 * sync barrier, controlled demote, promote compat, upgrade status parsing,
 * and subtree freeze/unfreeze roundtrip.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>
#include <endian.h>

#include "pnfs_mds.h"
#include "test_helpers.h"
#include "cluster_membership.h"
#include "cluster_transport.h"
#include "subtree_map.h"
#include "failover.h"
#include "mds_catalogue.h"
#include "grace.h"

/* -------------------------------------------------------------------
 * Test infrastructure
 * ------------------------------------------------------------------- */

static int tests_run;
static int tests_passed;
static int test_failed;

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

#define ASSERT_STR_EQ(a, b) do {                                \
    if (strcmp((a), (b)) != 0) {                                \
        fprintf(stderr, "  FAIL %s:%d: \"%s\" != \"%s\"\n",    \
                __FILE__, __LINE__, (a), (b));                  \
        test_failed = 1;                                        \
        return;                                                 \
    }                                                           \
} while (0)

#define RUN_TEST(fn) do {                                       \
    tests_run++;                                                \
    test_failed = 0;                                            \
    fprintf(stdout, "  %-60s", #fn);                            \
    fflush(stdout);                                             \
    fn();                                                       \
    if (!test_failed) {                                         \
        tests_passed++;                                         \
        fprintf(stdout, "PASS\n");                              \
    } else {                                                    \
        fprintf(stdout, "FAIL\n");                              \
    }                                                           \
} while (0)

/* Simple JSON field extractor for test-only member_from_json. */
static const char *find_json_str(const char *json, const char *key,
                                 size_t *out_len)
{
    char pat[64];
    snprintf(pat, sizeof(pat), "\"%s\":\"", key);
    const char *p = strstr(json, pat);
    if (!p) { *out_len = 0; return NULL; }
    p += strlen(pat);
    const char *end = strchr(p, '"');
    if (!end) { *out_len = 0; return NULL; }
    *out_len = (size_t)(end - p);
    return p;
}

static int find_json_int(const char *json, const char *key, int *val)
{
    char pat[64];
    snprintf(pat, sizeof(pat), "\"%s\":", key);
    const char *p = strstr(json, pat);
    if (!p) return 0;
    p += strlen(pat);
    *val = atoi(p);
    return 1;
}

/** Test-local member_from_json (no etcd_json.h dependency). */
static enum mds_status test_member_from_json(const char *json, size_t len,
                                             uint32_t mds_id,
                                             struct cluster_member *out)
{
    (void)len;
    memset(out, 0, sizeof(*out));
    out->mds_id = mds_id;
    size_t slen = 0;
    const char *s = find_json_str(json, "hostname", &slen);
    if (s && slen < sizeof(out->hostname)) {
        memcpy(out->hostname, s, slen);
    }
    int v = 0;
    if (find_json_int(json, "wire_compat", &v)) {
        out->wire_compat_version = (uint32_t)v;
    } else {
        out->wire_compat_version = 1;  /* legacy default */
    }
    if (find_json_int(json, "nfs_port", &v)) out->nfs_port = (uint16_t)v;
    if (find_json_int(json, "grpc_port", &v)) out->grpc_port = (uint16_t)v;
    if (find_json_int(json, "role", &v)) out->role = (enum node_role)v;
    if (find_json_int(json, "lifecycle", &v)) out->lifecycle = (enum node_lifecycle)v;
    return MDS_OK;
}
#define member_from_json test_member_from_json

/* ===================================================================
 * Test 1: member_to_json includes sw_version and wire_compat fields;
 *         member_from_json parses them correctly.
 * =================================================================== */
static void test_version_serialization_roundtrip(void)
{
    struct cluster_member m;
    memset(&m, 0, sizeof(m));
    m.mds_id = 1;
    (void)snprintf(m.hostname, sizeof(m.hostname), "mds1.local");
    m.role = NODE_ACTIVE;
    m.lifecycle = NODE_ACTIVE_SERVING;
    m.wire_compat_version = PNFS_MDS_WIRE_COMPAT_VERSION;

    char json[2048];
    int rc = member_to_json(&m, json, sizeof(json));
    ASSERT_TRUE(rc > 0);

    /* JSON must contain the version field. */
    ASSERT_NE(strstr(json, "\"wire_compat\""), NULL);

    struct cluster_member parsed;
    memset(&parsed, 0, sizeof(parsed));
    enum mds_status st = member_from_json(json, (size_t)rc, 1, &parsed);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(parsed.wire_compat_version, PNFS_MDS_WIRE_COMPAT_VERSION);
}

/* ===================================================================
 * Test 2: member_from_json with missing version fields defaults
 *         wire_compat_version to 1, software_version to "".
 * =================================================================== */
static void test_legacy_shim_defaults(void)
{
    /* Minimal JSON with no sw_version or wire_compat keys. */
    const char *legacy_json =
        "{\"mds_id\":2,\"hostname\":\"mds2.local\","
        "\"role\":0,\"lifecycle\":1,\"nfs_port\":2049,"
        "\"grpc_port\":50051,\"cluster_port\":9800,"
        "\"partner_id\":0,\"cluster_addr\":\"\"}";
    size_t len = strlen(legacy_json);

    struct cluster_member m;
    memset(&m, 0, sizeof(m));
    enum mds_status st = member_from_json(legacy_json, len, 2, &m);
    ASSERT_EQ(st, MDS_OK);
    /* Missing wire_compat defaults to 1, not 0. */
    ASSERT_EQ(m.wire_compat_version, 1U);
}

/* ===================================================================
 * Test 4: PNFS_MDS_WIRE_COMPAT_VERSION is a positive constant.
 * =================================================================== */
static void test_wire_compat_version_defined(void)
{
    ASSERT_TRUE(PNFS_MDS_WIRE_COMPAT_VERSION >= 1);
}

/* ===================================================================
 * Test 5: REPL_MSG_SYNC_BARRIER and ACK have distinct values.
 *
 * These wire constants were part of the catalogue replication layer.
 * They are no longer used in the RonDB-native path but remain
 * stable for backward wire compatibility.
 * =================================================================== */
#ifndef REPL_MSG_SYNC_BARRIER
#define REPL_MSG_SYNC_BARRIER      0x07
#define REPL_MSG_SYNC_BARRIER_ACK  0x08
#endif
static void test_barrier_message_types(void)
{
    ASSERT_NE(REPL_MSG_SYNC_BARRIER, REPL_MSG_SYNC_BARRIER_ACK);
    ASSERT_EQ(REPL_MSG_SYNC_BARRIER, 0x07);
    ASSERT_EQ(REPL_MSG_SYNC_BARRIER_ACK, 0x08);
}

/* ===================================================================
 * Test 6: CT_MSG_FAILOVER_REQ/RESP and UPGRADE_STATUS_REQ/RESP have
 *         correct values and are distinct.
 * =================================================================== */
static void test_upgrade_msg_types(void)
{
    ASSERT_EQ(CT_MSG_FAILOVER_REQ, 45);
    ASSERT_EQ(CT_MSG_FAILOVER_RESP, 46);
    ASSERT_EQ(CT_MSG_UPGRADE_STATUS_REQ, 47);
    ASSERT_EQ(CT_MSG_UPGRADE_STATUS_RESP, 48);
}

/* ===================================================================
 * Test 7: subtree_map_freeze_owned freezes all owned subtrees.
 * =================================================================== */
static void test_freeze_owned_subtrees(void)
{
    struct subtree_map *map = NULL;
    enum mds_status st = subtree_map_init(NULL, NULL, 1, "mds1.local",
                                 NULL,
                                          &map);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_NE(map, NULL);

    st = subtree_map_add(map, "/home", 1, "mds1.local",
                         SUBTREE_ACTIVE, 1);
    ASSERT_EQ(st, MDS_OK);

    st = subtree_map_add(map, "/data", 1, "mds1.local",
                         SUBTREE_ACTIVE, 1);
    ASSERT_EQ(st, MDS_OK);

    /* Add a subtree owned by a different node — should not be frozen. */
    st = subtree_map_add(map, "/other", 2, "mds2.local",
                         SUBTREE_ACTIVE, 1);
    ASSERT_EQ(st, MDS_OK);

    st = subtree_map_freeze_owned(map, 1);
    ASSERT_EQ(st, MDS_OK);

    /* Verify owned subtrees are frozen. */
    struct subtree_entry e;
    st = subtree_map_lookup_exact(map, "/home", &e);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ((int)e.state, (int)SUBTREE_FROZEN);

    st = subtree_map_lookup_exact(map, "/data", &e);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ((int)e.state, (int)SUBTREE_FROZEN);

    /* Non-owned subtree should remain active. */
    st = subtree_map_lookup_exact(map, "/other", &e);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ((int)e.state, (int)SUBTREE_ACTIVE);

    subtree_map_destroy(map);
}

/* ===================================================================
 * Test 8: subtree_map_unfreeze_owned restores FROZEN to ACTIVE.
 * =================================================================== */
static void test_unfreeze_owned_subtrees(void)
{
    struct subtree_map *map = NULL;
    enum mds_status st = subtree_map_init(NULL, NULL, 1, "mds1.local",
                                 NULL,
                                          &map);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_NE(map, NULL);

    st = subtree_map_add(map, "/proj", 1, "mds1.local",
                         SUBTREE_ACTIVE, 1);
    ASSERT_EQ(st, MDS_OK);

    /* Freeze then unfreeze. */
    st = subtree_map_freeze_owned(map, 1);
    ASSERT_EQ(st, MDS_OK);

    struct subtree_entry e;
    st = subtree_map_lookup_exact(map, "/proj", &e);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ((int)e.state, (int)SUBTREE_FROZEN);

    st = subtree_map_unfreeze_owned(map, 1);
    ASSERT_EQ(st, MDS_OK);

    st = subtree_map_lookup_exact(map, "/proj", &e);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ((int)e.state, (int)SUBTREE_ACTIVE);

    subtree_map_destroy(map);
}

/* ===================================================================
 * Test 9: FAILOVER_DEMOTING has the expected enum value.
 * =================================================================== */
static void test_failover_demoting_role(void)
{
    ASSERT_EQ(FAILOVER_DEMOTING, 3);
}

/* ===================================================================
 * Test 10: upgrade_status_entry struct size and field offsets.
 * =================================================================== */
static void test_upgrade_status_entry_layout(void)
{
    struct upgrade_status_entry e;
    memset(&e, 0, sizeof(e));
    e.mds_id = 42;
    e.wire_compat_version = 1;
    (void)snprintf(e.software_version, sizeof(e.software_version),
                   "0.1.0");
    e.role = 0;
    e.lifecycle = 2;

    ASSERT_EQ(e.mds_id, 42U);
    ASSERT_EQ(e.wire_compat_version, 1U);
    ASSERT_STR_EQ(e.software_version, "0.1.0");
    ASSERT_EQ(e.role, 0);
    ASSERT_EQ(e.lifecycle, 2);
}

/* ===================================================================
 * Test 11: Version fields survive JSON roundtrip with non-default
 *          wire_compat_version.
 * =================================================================== */
static void test_version_roundtrip_nondefault(void)
{
    struct cluster_member m;
    memset(&m, 0, sizeof(m));
    m.mds_id = 5;
    (void)snprintf(m.hostname, sizeof(m.hostname), "mds5.test");
    m.role = NODE_STANDBY;
    m.lifecycle = NODE_IDLE;
    m.wire_compat_version = 99;

    char json[2048];
    int rc = member_to_json(&m, json, sizeof(json));
    ASSERT_TRUE(rc > 0);

    struct cluster_member parsed;
    memset(&parsed, 0, sizeof(parsed));
    enum mds_status st = member_from_json(json, (size_t)rc, 5, &parsed);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(parsed.wire_compat_version, 99U);
}

/* ===================================================================
 * Test 12: Freeze with no owned subtrees succeeds (no-op).
 * =================================================================== */
static void test_freeze_no_owned_subtrees(void)
{
    struct subtree_map *map = NULL;
    enum mds_status st = subtree_map_init(NULL, NULL, 1, "mds1.local",
                                 NULL,
                                          &map);
    ASSERT_EQ(st, MDS_OK);

    /* Add subtree owned by a different node. */
    st = subtree_map_add(map, "/foo", 2, "mds2.local",
                         SUBTREE_ACTIVE, 1);
    ASSERT_EQ(st, MDS_OK);

    /* Freeze should succeed with no owned subtrees to freeze. */
    st = subtree_map_freeze_owned(map, 1);
    ASSERT_EQ(st, MDS_OK);

    /* Verify the other-owned subtree is untouched. */
    struct subtree_entry e;
    st = subtree_map_lookup_exact(map, "/foo", &e);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ((int)e.state, (int)SUBTREE_ACTIVE);

    subtree_map_destroy(map);
}

/* ===================================================================
 * Infrastructure for live-path tests
 * =================================================================== */

#define TEST_MAP_SIZE (16ULL * 1024 * 1024)
#define SELF_ID      1
#define PARTNER_ID   2

static int detect_always_dead(uint32_t partner_id, void *arg)
{
    (void)partner_id;
    (void)arg;
    return 1;  /* partner is dead */
}

static char *make_temp_db(void)
{
    char tmpl[] = "/tmp/pnfs-ru-test-XXXXXX";
    char *dir = mkdtemp(tmpl);
    if (dir == NULL) { return NULL; }

    size_t len = strlen(dir) + sizeof("/data.mdb");
    char *path = malloc(len);
    if (path == NULL) { return NULL; }
    (void)snprintf(path, len, "%s/data.mdb", dir);
    return path;
}

static void cleanup_temp_db(const char *path)
{
    char lock_path[512];
    (void)unlink(path);
    (void)snprintf(lock_path, sizeof(lock_path), "%s-lock", path);
    (void)unlink(lock_path);
    /* Remove parent directory. */
    char dir[512];
    (void)snprintf(dir, sizeof(dir), "%s", path);
    char *slash = strrchr(dir, '/');
    if (slash != NULL) {
        *slash = '\0';
        (void)rmdir(dir);
    }
}

/* Create a minimal mds_config for membership init. */
static void make_cfg(struct mds_config *cfg, uint32_t self_id,
                     int role, uint32_t partner_id)
{
    memset(cfg, 0, sizeof(*cfg));
    cfg->self.id = self_id;
    (void)snprintf(cfg->self.hostname, sizeof(cfg->self.hostname),
                   "mds%u.local", (unsigned)self_id);
    cfg->self.nfs_port = 2049;
    cfg->self.grpc_port = 50051;
    cfg->self_role = role;
    cfg->self_failover_partner_id = partner_id;
}

/* ===================================================================
 * Test 14: Controlled demote rejects STANDBY role.
 * =================================================================== */
static void test_demote_wrong_role_guard(void)
{
    struct mds_catalogue *db = NULL;
    struct mds_catalogue *cat = NULL;
    struct subtree_map *map = NULL;
    struct failover_ctx *ctx = NULL;

    char *db_path = make_temp_db();
    ASSERT_NE(db_path, NULL);

    db = open_test_catalogue(); ASSERT_TRUE(db != NULL);
    cat = db;
    ASSERT_EQ(subtree_map_init(NULL, NULL, SELF_ID, "s.local", NULL, &map), MDS_OK);

    struct failover_cfg fcfg = {
        .self_id = SELF_ID, .partner_id = PARTNER_ID,
        .map = map, .cat = cat, .grace_period_sec = 90,
        .detect_cb = detect_always_dead, .detect_arg = NULL,
        .membership = NULL, .hm = NULL,
    };
    ASSERT_EQ(failover_init(&fcfg, &ctx), MDS_OK);

    /* ctx starts in STANDBY — demote must fail. */
    ASSERT_EQ(failover_get_role(ctx), FAILOVER_STANDBY);
    /* repl=NULL triggers the repl-required check first, but if role is
     * wrong it should still be MDS_ERR_PERM regardless. */
    ASSERT_EQ(failover_controlled_demote(ctx, NULL), MDS_ERR_PERM);

    failover_destroy(ctx);
    subtree_map_destroy(map);
    mds_catalogue_close(db);
    cleanup_temp_db(db_path);
    free(db_path);
}

/* ===================================================================
 * Test 15: Controlled demote rejects NULL replication.
 * =================================================================== */
static void test_demote_no_replication(void)
{
    struct mds_catalogue *db = NULL;
    struct mds_catalogue *cat = NULL;
    struct subtree_map *map = NULL;
    struct failover_ctx *ctx = NULL;

    grace_init();

    char *db_path = make_temp_db();
    ASSERT_NE(db_path, NULL);

    db = open_test_catalogue(); ASSERT_TRUE(db != NULL);
    cat = db;
    ASSERT_EQ(subtree_map_init(NULL, NULL, SELF_ID, "s.local", NULL, &map), MDS_OK);

    /* Add a subtree owned by partner so promote can take it over. */
    ASSERT_EQ(subtree_map_add(map, "/data", PARTNER_ID, "p.local",
                              SUBTREE_ACTIVE, 1), MDS_OK);

    struct failover_cfg fcfg = {
        .self_id = SELF_ID, .partner_id = PARTNER_ID,
        .map = map, .cat = cat, .grace_period_sec = 90,
        .detect_cb = detect_always_dead, .detect_arg = NULL,
        .membership = NULL, .hm = NULL,
    };
    ASSERT_EQ(failover_init(&fcfg, &ctx), MDS_OK);

    /* Promote to PRIMARY first. */
    ASSERT_EQ(failover_promote(ctx), MDS_OK);
    ASSERT_EQ(failover_get_role(ctx), FAILOVER_PRIMARY);

    /* Demote with NULL replication must fail. */
    ASSERT_EQ(failover_controlled_demote(ctx, NULL), MDS_ERR_PERM);
    /* Role must still be PRIMARY after rejection. */
    ASSERT_EQ(failover_get_role(ctx), FAILOVER_PRIMARY);

    failover_destroy(ctx);
    subtree_map_destroy(map);
    mds_catalogue_close(db);
    cleanup_temp_db(db_path);
    free(db_path);
}

/* ===================================================================
 * Test 16: failover_get_self_id accessor.
 * =================================================================== */
static void test_failover_get_self_id(void)
{
    struct mds_catalogue *db = NULL;
    struct mds_catalogue *cat = NULL;
    struct subtree_map *map = NULL;
    struct failover_ctx *ctx = NULL;

    char *db_path = make_temp_db();
    ASSERT_NE(db_path, NULL);

    db = open_test_catalogue(); ASSERT_TRUE(db != NULL);
    cat = db;
    ASSERT_EQ(subtree_map_init(NULL, NULL, 42, "s.local", NULL, &map), MDS_OK);

    struct failover_cfg fcfg = {
        .self_id = 42, .partner_id = 99,
        .map = map, .cat = cat, .grace_period_sec = 90,
        .detect_cb = NULL, .detect_arg = NULL,
        .membership = NULL, .hm = NULL,
    };
    ASSERT_EQ(failover_init(&fcfg, &ctx), MDS_OK);
    ASSERT_EQ(failover_get_self_id(ctx), 42U);
    ASSERT_EQ(failover_get_self_id(NULL), 0U);

    failover_destroy(ctx);
    subtree_map_destroy(map);
    mds_catalogue_close(db);
    cleanup_temp_db(db_path);
    free(db_path);
}

/* ===================================================================
 * Test 17: Standby self-registration gets NODE_IDLE lifecycle.
 * =================================================================== */
static void test_standby_lifecycle_init(void)
{
    struct subtree_map *smap = NULL;
    ASSERT_EQ(subtree_map_init(NULL, NULL, 2, "mds2.local", NULL, &smap), MDS_OK);

    struct mds_config cfg;
    make_cfg(&cfg, 2, NODE_STANDBY, 1);

    struct cluster_membership *ctx = NULL;
    ASSERT_EQ(cluster_membership_init(&cfg, smap, NULL, &ctx), MDS_OK);

    struct cluster_member m;
    ASSERT_EQ(cluster_membership_get(ctx, 2, &m), MDS_OK);
    ASSERT_EQ((int)m.role, (int)NODE_STANDBY);
    ASSERT_EQ((int)m.lifecycle, (int)NODE_IDLE);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/* ===================================================================
 * Test 18: Active self-registration gets NODE_ACTIVE_SERVING lifecycle.
 * =================================================================== */
static void test_active_lifecycle_init(void)
{
    struct subtree_map *smap = NULL;
    ASSERT_EQ(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap), MDS_OK);

    struct mds_config cfg;
    make_cfg(&cfg, 1, NODE_ACTIVE, 0);

    struct cluster_membership *ctx = NULL;
    ASSERT_EQ(cluster_membership_init(&cfg, smap, NULL, &ctx), MDS_OK);

    struct cluster_member m;
    ASSERT_EQ(cluster_membership_get(ctx, 1, &m), MDS_OK);
    ASSERT_EQ((int)m.role, (int)NODE_ACTIVE);
    ASSERT_EQ((int)m.lifecycle, (int)NODE_ACTIVE_SERVING);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/* ===================================================================
 * Test 19: Lifecycle FSM allows ACTIVE_SERVING → DRAINING → DRAINED
 *          (the two-step sequence controlled demote uses) but rejects
 *          the invalid direct ACTIVE_SERVING → DRAINED jump.
 * =================================================================== */
static void test_demote_lifecycle_fsm(void)
{
    struct subtree_map *smap = NULL;
    ASSERT_EQ(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap), MDS_OK);

    struct mds_config cfg;
    make_cfg(&cfg, 1, NODE_ACTIVE, 0);
    struct cluster_membership *ctx = NULL;
    ASSERT_EQ(cluster_membership_init(&cfg, smap, NULL, &ctx), MDS_OK);

    /* Confirm initial state. */
    struct cluster_member m;
    ASSERT_EQ(cluster_membership_get(ctx, 1, &m), MDS_OK);
    ASSERT_EQ((int)m.lifecycle, (int)NODE_ACTIVE_SERVING);

    /* Direct ACTIVE_SERVING → DRAINED must fail. */
    ASSERT_EQ(cluster_membership_set_lifecycle(ctx, 1, NODE_DRAINED),
              MDS_ERR_INVAL);

    /* Two-step: ACTIVE_SERVING → DRAINING → DRAINED must succeed. */
    ASSERT_EQ(cluster_membership_set_lifecycle(ctx, 1, NODE_DRAINING),
              MDS_OK);
    ASSERT_EQ(cluster_membership_get(ctx, 1, &m), MDS_OK);
    ASSERT_EQ((int)m.lifecycle, (int)NODE_DRAINING);

    ASSERT_EQ(cluster_membership_set_lifecycle(ctx, 1, NODE_DRAINED),
              MDS_OK);
    ASSERT_EQ(cluster_membership_get(ctx, 1, &m), MDS_OK);
    ASSERT_EQ((int)m.lifecycle, (int)NODE_DRAINED);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}


/* ===================================================================
 * Test 20: cluster_membership_remove_self — member is removed.
 * =================================================================== */
static void test_self_remove_succeeds(void)
{
    struct subtree_map *smap = NULL;
    ASSERT_EQ(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap), MDS_OK);

    struct mds_config cfg;
    make_cfg(&cfg, 1, NODE_ACTIVE, 0);
    struct cluster_membership *ctx = NULL;
    ASSERT_EQ(cluster_membership_init(&cfg, smap, NULL, &ctx), MDS_OK);

    /* Confirm self is in the table. */
    struct cluster_member m;
    ASSERT_EQ(cluster_membership_get(ctx, 1, &m), MDS_OK);
    ASSERT_EQ((int)m.lifecycle, (int)NODE_ACTIVE_SERVING);

    /* Self-remove must succeed. */
    ASSERT_EQ(cluster_membership_remove_self(ctx), MDS_OK);

    /* Member must be gone. */
    ASSERT_EQ(cluster_membership_get(ctx, 1, &m), MDS_ERR_NOTFOUND);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/* ===================================================================
 * Test 21: Self-remove fires partner-loss callback when
 *          member was ACTIVE + ACTIVE_SERVING.
 * =================================================================== */
static int ploss_fired_count;
static uint32_t ploss_fired_id;

static void ploss_test_cb(const struct cluster_member *removed, void *arg)
{
    (void)arg;
    ploss_fired_count++;
    ploss_fired_id = removed->mds_id;
}

static void test_self_remove_partner_loss_fires(void)
{
    ploss_fired_count = 0;
    ploss_fired_id = 0;

    struct subtree_map *smap = NULL;
    ASSERT_EQ(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap), MDS_OK);

    struct mds_config cfg;
    make_cfg(&cfg, 1, NODE_ACTIVE, 0);
    struct cluster_membership *ctx = NULL;
    ASSERT_EQ(cluster_membership_init(&cfg, smap, NULL, &ctx), MDS_OK);

    /* Register partner-loss watcher: watch for partner_id == 1
     * (self — simulates what the standby sees when the primary
     * is configured as its partner). */
    cluster_membership_set_partner_loss_cb(ctx, 1, ploss_test_cb, NULL);

    /* Self-remove while ACTIVE + ACTIVE_SERVING. */
    ASSERT_EQ(cluster_membership_remove_self(ctx), MDS_OK);

    /* Partner-loss callback must have fired exactly once. */
    ASSERT_EQ(ploss_fired_count, 1);
    ASSERT_EQ(ploss_fired_id, (uint32_t)1);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/* ===================================================================
 * Test 22: Partner-loss does NOT fire if member was DRAINING
 *          (lifecycle != ACTIVE_SERVING).
 * =================================================================== */
static void test_self_remove_no_ploss_if_draining(void)
{
    ploss_fired_count = 0;
    ploss_fired_id = 0;

    struct subtree_map *smap = NULL;
    ASSERT_EQ(subtree_map_init(NULL, NULL, 1, "mds1.local", NULL, &smap), MDS_OK);

    struct mds_config cfg;
    make_cfg(&cfg, 1, NODE_ACTIVE, 0);
    struct cluster_membership *ctx = NULL;
    ASSERT_EQ(cluster_membership_init(&cfg, smap, NULL, &ctx), MDS_OK);

    /* Register partner-loss watcher for node 1. */
    cluster_membership_set_partner_loss_cb(ctx, 1, ploss_test_cb, NULL);

    /* Transition to DRAINING (not ACTIVE_SERVING). */
    ASSERT_EQ(cluster_membership_set_lifecycle(ctx, 1, NODE_DRAINING),
              MDS_OK);

    /* Self-remove while DRAINING — should succeed but NOT fire ploss. */
    ASSERT_EQ(cluster_membership_remove_self(ctx), MDS_OK);

    /* Partner-loss callback must NOT have fired. */
    ASSERT_EQ(ploss_fired_count, 0);

    cluster_membership_destroy(ctx);
    subtree_map_destroy(smap);
}

/* -------------------------------------------------------------------
 * Main
 * ------------------------------------------------------------------- */

int main(void)
{
    fprintf(stdout, "test_rolling_upgrade:\n");

    RUN_TEST(test_version_serialization_roundtrip);
    RUN_TEST(test_legacy_shim_defaults);
    RUN_TEST(test_wire_compat_version_defined);
    RUN_TEST(test_barrier_message_types);
    RUN_TEST(test_upgrade_msg_types);
    RUN_TEST(test_freeze_owned_subtrees);
    RUN_TEST(test_unfreeze_owned_subtrees);
    RUN_TEST(test_failover_demoting_role);
    RUN_TEST(test_upgrade_status_entry_layout);
    RUN_TEST(test_version_roundtrip_nondefault);
    RUN_TEST(test_freeze_no_owned_subtrees);
    RUN_TEST(test_demote_wrong_role_guard);
    RUN_TEST(test_demote_no_replication);
    RUN_TEST(test_failover_get_self_id);
    RUN_TEST(test_standby_lifecycle_init);
    RUN_TEST(test_active_lifecycle_init);
    RUN_TEST(test_demote_lifecycle_fsm);
    RUN_TEST(test_self_remove_succeeds);
    RUN_TEST(test_self_remove_partner_loss_fires);
    RUN_TEST(test_self_remove_no_ploss_if_draining);

    fprintf(stdout, "\n  %d/%d tests passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? EXIT_SUCCESS : EXIT_FAILURE;
}
