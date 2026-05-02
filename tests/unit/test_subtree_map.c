/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_subtree_map.c — Unit tests for the subtree ownership map,
 *                      referral_build, and related APIs.
 */

#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>

#include "pnfs_mds.h"
#include "subtree_map.h"
#include "subtree_split.h"
#include "cluster_membership.h"

/* -------------------------------------------------------------------
 * Helpers
 * ------------------------------------------------------------------- */

static int passed;
static int failed;

#define ASSERT_EQ(a, b) do { \
    if ((a) != (b)) { \
        fprintf(stderr, "  FAIL %s:%d: %s != %s\n", \
                __FILE__, __LINE__, #a, #b); \
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

#define ASSERT_STREQ(a, b) do { \
    if (strcmp((a), (b)) != 0) { \
        fprintf(stderr, "  FAIL %s:%d: \"%s\" != \"%s\"\n", \
                __FILE__, __LINE__, (a), (b)); \
        failed++; \
        return; \
    } \
} while (0)

#define PASS() do { \
    passed++; \
    fprintf(stdout, "  PASS\n"); \
} while (0)

/* -------------------------------------------------------------------
 * test_init_destroy
 * ------------------------------------------------------------------- */

static void test_init_destroy(void)
{
    fprintf(stdout, "  test_init_destroy: ");

    struct subtree_map *map = NULL;
    enum mds_status st;

    /* NULL out pointer should fail. */
    st = subtree_map_init(NULL, NULL, 0, "mds0.local", NULL, NULL);
    ASSERT_EQ(st, MDS_ERR_INVAL);

    /* Normal init. */
    st = subtree_map_init(NULL, NULL, 0, "mds0.local",
                                 NULL, &map);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_TRUE(map != NULL);
    ASSERT_EQ(subtree_map_self_id(map), 0U);

    /* Root entry should exist. */
    struct subtree_entry entry;
    st = subtree_map_lookup(map, "/", &entry);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_STREQ(entry.path, "/");
    ASSERT_EQ(entry.owner_mds_id, 0U);
    ASSERT_EQ(entry.state, SUBTREE_ACTIVE);
    ASSERT_EQ(entry.version, 1ULL);

    subtree_map_destroy(map);
    subtree_map_destroy(NULL);  /* Should be safe. */

    PASS();
}

/* -------------------------------------------------------------------
 * test_lookup_root
 * ------------------------------------------------------------------- */

static void test_lookup_root(void)
{
    fprintf(stdout, "  test_lookup_root: ");

    struct subtree_map *map = NULL;
    enum mds_status st = subtree_map_init(NULL, NULL, 42, "mds42.local",
                                 NULL, &map);
    ASSERT_EQ(st, MDS_OK);

    struct subtree_entry entry;

    /* Any path should match root when no other entries exist. */
    st = subtree_map_lookup(map, "/foo/bar/baz", &entry);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(entry.owner_mds_id, 42U);
    ASSERT_STREQ(entry.path, "/");

    subtree_map_destroy(map);
    PASS();
}

/* -------------------------------------------------------------------
 * test_lookup_subtree
 * ------------------------------------------------------------------- */

static void test_lookup_subtree(void)
{
    fprintf(stdout, "  test_lookup_subtree: ");

    struct subtree_map *map = NULL;
    enum mds_status st = subtree_map_init(NULL, NULL, 0, "mds0.local",
                                 NULL, &map);
    ASSERT_EQ(st, MDS_OK);

    /* Add two subtrees owned by different MDS nodes. */
    st = subtree_map_add(map, "/home", 0, "mds0.local",
                         SUBTREE_ACTIVE, 1);
    ASSERT_EQ(st, MDS_OK);

    st = subtree_map_add(map, "/data", 1, "mds1.remote",
                         SUBTREE_ACTIVE, 1);
    ASSERT_EQ(st, MDS_OK);

    /* Duplicate add should fail. */
    st = subtree_map_add(map, "/data", 1, "mds1.remote",
                         SUBTREE_ACTIVE, 1);
    ASSERT_EQ(st, MDS_ERR_EXISTS);

    struct subtree_entry entry;

    /* /home/alice → matches /home, owned by MDS 0. */
    st = subtree_map_lookup(map, "/home/alice", &entry);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_STREQ(entry.path, "/home");
    ASSERT_EQ(entry.owner_mds_id, 0U);

    /* /data/shared → matches /data, owned by MDS 1. */
    st = subtree_map_lookup(map, "/data/shared", &entry);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_STREQ(entry.path, "/data");
    ASSERT_EQ(entry.owner_mds_id, 1U);

    /* /etc → matches / (root fallback), owned by MDS 0. */
    st = subtree_map_lookup(map, "/etc", &entry);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_STREQ(entry.path, "/");

    subtree_map_destroy(map);
    PASS();
}

/* -------------------------------------------------------------------
 * test_longest_prefix
 * ------------------------------------------------------------------- */

static void test_longest_prefix(void)
{
    fprintf(stdout, "  test_longest_prefix: ");

    struct subtree_map *map = NULL;
    enum mds_status st = subtree_map_init(NULL, NULL, 0, "mds0.local",
                                 NULL, &map);
    ASSERT_EQ(st, MDS_OK);

    st = subtree_map_add(map, "/home", 0, NULL, SUBTREE_ACTIVE, 1);
    ASSERT_EQ(st, MDS_OK);

    st = subtree_map_add(map, "/home/alice", 2, "mds2.local",
                         SUBTREE_ACTIVE, 1);
    ASSERT_EQ(st, MDS_OK);

    struct subtree_entry entry;

    /* /home/alice/file → matches /home/alice (longer prefix). */
    st = subtree_map_lookup(map, "/home/alice/file", &entry);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_STREQ(entry.path, "/home/alice");
    ASSERT_EQ(entry.owner_mds_id, 2U);

    /* /home/bob → matches /home (not /home/alice). */
    st = subtree_map_lookup(map, "/home/bob", &entry);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_STREQ(entry.path, "/home");
    ASSERT_EQ(entry.owner_mds_id, 0U);

    /* /homework → should NOT match /home (different word boundary). */
    st = subtree_map_lookup(map, "/homework", &entry);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_STREQ(entry.path, "/");  /* Falls back to root. */

    subtree_map_destroy(map);
    PASS();
}

/* -------------------------------------------------------------------
 * test_is_local
 * ------------------------------------------------------------------- */

static void test_is_local(void)
{
    fprintf(stdout, "  test_is_local: ");

    struct subtree_map *map = NULL;
    enum mds_status st = subtree_map_init(NULL, NULL, 0, "mds0.local",
                                 NULL, &map);
    ASSERT_EQ(st, MDS_OK);

    st = subtree_map_add(map, "/data", 1, "mds1.remote",
                         SUBTREE_ACTIVE, 1);
    ASSERT_EQ(st, MDS_OK);

    /* / is owned by self (MDS 0) → local. */
    ASSERT_TRUE(subtree_map_is_local(map, "/"));
    ASSERT_TRUE(subtree_map_is_local(map, "/etc/foo"));

    /* /data is owned by MDS 1 → not local. */
    ASSERT_TRUE(!subtree_map_is_local(map, "/data"));
    ASSERT_TRUE(!subtree_map_is_local(map, "/data/shared/file"));

    subtree_map_destroy(map);
    PASS();
}

/* -------------------------------------------------------------------
 * test_set_owner_cas
 * ------------------------------------------------------------------- */

static void test_set_owner_cas(void)
{
    fprintf(stdout, "  test_set_owner_cas: ");

    struct subtree_map *map = NULL;
    enum mds_status st = subtree_map_init(NULL, NULL, 0, "mds0.local",
                                 NULL, &map);
    ASSERT_EQ(st, MDS_OK);

    st = subtree_map_add(map, "/home", 0, NULL, SUBTREE_ACTIVE, 1);
    ASSERT_EQ(st, MDS_OK);

    /* CAS with wrong version should fail. */
    st = subtree_map_set_owner(map, "/home", 1, /*expected_ver=*/99);
    ASSERT_EQ(st, MDS_ERR_STALE);

    /* CAS with correct version should succeed. */
    st = subtree_map_set_owner(map, "/home", 1, /*expected_ver=*/1);
    ASSERT_EQ(st, MDS_OK);

    /* Verify new owner and incremented version. */
    struct subtree_entry entry;
    st = subtree_map_lookup(map, "/home", &entry);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(entry.owner_mds_id, 1U);
    ASSERT_EQ(entry.version, 2ULL);

    /* Non-existent path should fail. */
    st = subtree_map_set_owner(map, "/nope", 1, 1);
    ASSERT_EQ(st, MDS_ERR_NOTFOUND);

    subtree_map_destroy(map);
    PASS();
}

/* -------------------------------------------------------------------
 * test_set_state
 * ------------------------------------------------------------------- */

static void test_set_state(void)
{
    fprintf(stdout, "  test_set_state: ");

    struct subtree_map *map = NULL;
    enum mds_status st = subtree_map_init(NULL, NULL, 0, "mds0.local",
                                 NULL, &map);
    ASSERT_EQ(st, MDS_OK);

    st = subtree_map_add(map, "/home", 0, NULL, SUBTREE_ACTIVE, 1);
    ASSERT_EQ(st, MDS_OK);

    /* Transition ACTIVE → MIGRATING. */
    st = subtree_map_set_state(map, "/home", SUBTREE_MIGRATING, 1);
    ASSERT_EQ(st, MDS_OK);

    struct subtree_entry entry;
    st = subtree_map_lookup(map, "/home", &entry);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(entry.state, SUBTREE_MIGRATING);
    ASSERT_EQ(entry.version, 2ULL);

    /* Transition MIGRATING → FROZEN. */
    st = subtree_map_set_state(map, "/home", SUBTREE_FROZEN, 2);
    ASSERT_EQ(st, MDS_OK);

    st = subtree_map_lookup(map, "/home", &entry);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(entry.state, SUBTREE_FROZEN);
    ASSERT_EQ(entry.version, 3ULL);

    /* Stale version should fail. */
    st = subtree_map_set_state(map, "/home", SUBTREE_ACTIVE, 1);
    ASSERT_EQ(st, MDS_ERR_STALE);

    subtree_map_destroy(map);
    PASS();
}

/* -------------------------------------------------------------------
 * test_referral_build
 * ------------------------------------------------------------------- */

static void test_referral_build(void)
{
    fprintf(stdout, "  test_referral_build: ");

    struct subtree_map *map = NULL;
    enum mds_status st = subtree_map_init(NULL, NULL, 0, "mds0.local",
                                 NULL, &map);
    ASSERT_EQ(st, MDS_OK);

    st = subtree_map_add(map, "/data", 1, "mds1.remote",
                         SUBTREE_ACTIVE, 1);
    ASSERT_EQ(st, MDS_OK);

    struct mds_fs_location loc;
    memset(&loc, 0, sizeof(loc));

    /* Build referral for /data/shared → should resolve to MDS 1. */
    st = referral_build(map, "/data/shared", &loc);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_STREQ(loc.server, "mds1.remote");
    ASSERT_STREQ(loc.rootpath, "/"); /* target serves at its root */

    /* Build referral for / → should resolve to self (MDS 0). */
    st = referral_build(map, "/etc/foo", &loc);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_STREQ(loc.server, "mds0.local");
    ASSERT_STREQ(loc.rootpath, "/");

    /* Node hostname lookup. */
    char hostname[256];
    st = subtree_map_node_hostname(map, 1, hostname, sizeof(hostname));
    ASSERT_EQ(st, MDS_OK);
    ASSERT_STREQ(hostname, "mds1.remote");

    st = subtree_map_node_hostname(map, 99, hostname, sizeof(hostname));
    ASSERT_EQ(st, MDS_ERR_NOTFOUND);

    subtree_map_destroy(map);
    PASS();
}

/* -------------------------------------------------------------------
 * test_fs_locations_encode
 * ------------------------------------------------------------------- */

/* Forward declaration — defined in referral.c */
enum mds_status referral_encode_fs_locations(
    const struct mds_fs_location *loc,
    void *xdr_out, size_t *out_len);

static void test_fs_locations_encode(void)
{
    fprintf(stdout, "  test_fs_locations_encode: ");

    struct mds_fs_location loc;
    snprintf(loc.server, sizeof(loc.server), "mds1.remote");
    snprintf(loc.rootpath, sizeof(loc.rootpath), "/data");

    uint8_t buf[512];
    size_t len = sizeof(buf);

    enum mds_status st = referral_encode_fs_locations(&loc, buf, &len);
    ASSERT_EQ(st, MDS_OK);

    /* Verify encoding: [server_len][server][rootpath_len][rootpath]. */
    uint32_t slen, rlen;
    memcpy(&slen, buf, sizeof(slen));
    ASSERT_EQ(slen, (uint32_t)strlen("mds1.remote"));
    ASSERT_TRUE(memcmp(buf + 4, "mds1.remote", slen) == 0);

    memcpy(&rlen, buf + 4 + slen, sizeof(rlen));
    ASSERT_EQ(rlen, (uint32_t)strlen("/data"));
    ASSERT_TRUE(memcmp(buf + 4 + slen + 4, "/data", rlen) == 0);

    /* Too-small buffer should fail and report needed size. */
    size_t small = 2;
    st = referral_encode_fs_locations(&loc, buf, &small);
    ASSERT_EQ(st, MDS_ERR_INVAL);
    ASSERT_TRUE(small > 2);

    PASS();
}

/* -----------------------------------------------------------------------
 * Subtree JSON roundtrip (Sequence 3)
 * ----------------------------------------------------------------------- */

extern int subtree_to_json(const struct subtree_entry *e,
                            char *buf, size_t cap);

/** Test-local subtree_from_json (etcd_json.h removed). */
static enum mds_status subtree_from_json(const char *json, size_t len,
                                         const char *path,
                                         int64_t mod_revision,
                                         struct subtree_entry *out)
{
    (void)len;
    memset(out, 0, sizeof(*out));
    snprintf(out->path, sizeof(out->path), "%s", path);
    out->version = (uint64_t)mod_revision;
    int owner = 0, state = 0;
    const char *p = strstr(json, "\"owner_mds_id\":");
    if (p) { owner = atoi(p + 15); }
    p = strstr(json, "\"state\":");
    if (p) { state = atoi(p + 8); }
    out->owner_mds_id = (uint32_t)owner;
    out->state = (enum subtree_state)state;
    return MDS_OK;
}

static void test_subtree_json_roundtrip(void)
{
    fprintf(stdout, "  test_subtree_json_roundtrip");

    struct subtree_entry orig;
    memset(&orig, 0, sizeof(orig));
    snprintf(orig.path, sizeof(orig.path), "/data/shard1");
    orig.owner_mds_id = 5;
    orig.state = SUBTREE_ACTIVE;
    orig.version = 99;  /* version is NOT serialised */

    char buf[512];
    int n = subtree_to_json(&orig, buf, sizeof(buf));
    ASSERT_TRUE(n > 0 && (size_t)n < sizeof(buf));

    struct subtree_entry parsed;
    ASSERT_EQ(subtree_from_json(buf, (size_t)n, "/data/shard1", 42, &parsed),
              MDS_OK);

    ASSERT_STREQ(parsed.path, "/data/shard1");
    ASSERT_EQ(parsed.owner_mds_id, (uint32_t)5);
    ASSERT_EQ(parsed.state, SUBTREE_ACTIVE);
    /* version := mod_revision, not serialised value */
    ASSERT_EQ(parsed.version, (uint64_t)42);

    PASS();
}

/* -----------------------------------------------------------------------
 * take_over rejected in etcd mode (Sequence 3)
 *
 * We cannot construct a real etcd-mode map without etcd, but we can
 * test the local-mode take_over still works and check the public API
 * subtree_map_is_authoritative() returns false for local mode.
 * ----------------------------------------------------------------------- */

static void test_take_over_local_mode(void)
{
    fprintf(stdout, "  test_take_over_local_mode");

    struct subtree_map *map = NULL;
    ASSERT_EQ(subtree_map_init(NULL, NULL, 1, "host1", NULL, &map), MDS_OK);

    /* Local mode: authoritative = false */
    ASSERT_TRUE(!subtree_map_is_authoritative(map));

    /* Add a subtree owned by MDS 2 */
    ASSERT_EQ(subtree_map_add(map, "/other", 2, "host2", SUBTREE_ACTIVE, 1),
              MDS_OK);

    /* take_over MDS 2 → MDS 1 in local mode: should succeed */
    uint32_t taken = 0;
    ASSERT_EQ(subtree_map_take_over(map, 2, 1, &taken), MDS_OK);
    ASSERT_EQ(taken, (uint32_t)1);

    /* Verify the subtree is now owned by MDS 1 */
    struct subtree_entry e;
    ASSERT_EQ(subtree_map_lookup_exact(map, "/other", &e), MDS_OK);
    ASSERT_EQ(e.owner_mds_id, (uint32_t)1);

    subtree_map_destroy(map);
    PASS();
}

/* -------------------------------------------------------------------
 * main
 * ------------------------------------------------------------------- */


/* -----------------------------------------------------------------------
 * Blocker-regression: path-based writes survive index shifts.
 *
 * After removing an entry (which compacts the internal array), subsequent
 * set_owner / set_state must still target the correct path — not a stale
 * array index that now points at a different subtree.
 * ----------------------------------------------------------------------- */

static void test_subtree_write_stable_path(void)
{
    fprintf(stdout, "  test_subtree_write_stable_path");

    struct subtree_map *map = NULL;
    ASSERT_EQ(subtree_map_init(NULL, NULL, 0, "mds0.local", NULL, &map), MDS_OK);

    /* Populate several subtrees */
    ASSERT_EQ(subtree_map_add(map, "/alpha", 0, NULL,
                              SUBTREE_ACTIVE, 1), MDS_OK);
    ASSERT_EQ(subtree_map_add(map, "/beta", 1, "mds1.local",
                              SUBTREE_ACTIVE, 1), MDS_OK);
    ASSERT_EQ(subtree_map_add(map, "/gamma", 2, "mds2.local",
                              SUBTREE_ACTIVE, 1), MDS_OK);

    /* Mutate /beta (middle entry) */
    ASSERT_EQ(subtree_map_set_owner(map, "/beta", 0, 1), MDS_OK);

    /* Remove /alpha — shifts internal array indices */
    ASSERT_EQ(subtree_map_remove_subtree(map, "/alpha"), MDS_OK);

    /* set_state on /gamma — must hit /gamma, not /beta */
    ASSERT_EQ(subtree_map_set_state(map, "/gamma",
                                    SUBTREE_MIGRATING, 1), MDS_OK);

    struct subtree_entry entry;
    ASSERT_EQ(subtree_map_lookup_exact(map, "/gamma", &entry), MDS_OK);
    ASSERT_EQ(entry.state, SUBTREE_MIGRATING);
    ASSERT_EQ(entry.owner_mds_id, (uint32_t)2);

    /* /beta must be unaffected by the /gamma mutation */
    ASSERT_EQ(subtree_map_lookup_exact(map, "/beta", &entry), MDS_OK);
    ASSERT_EQ(entry.state, SUBTREE_ACTIVE);
    ASSERT_EQ(entry.owner_mds_id, (uint32_t)0);

    /* Add /delta — shifts indices again */
    ASSERT_EQ(subtree_map_add(map, "/delta", 3, "mds3.local",
                              SUBTREE_ACTIVE, 1), MDS_OK);

    /* set_owner on /beta after the shift */
    ASSERT_EQ(subtree_map_set_owner(map, "/beta", 1, 2), MDS_OK);
    ASSERT_EQ(subtree_map_lookup_exact(map, "/beta", &entry), MDS_OK);
    ASSERT_EQ(entry.owner_mds_id, (uint32_t)1);

    /* /delta must be unaffected */
    ASSERT_EQ(subtree_map_lookup_exact(map, "/delta", &entry), MDS_OK);
    ASSERT_EQ(entry.owner_mds_id, (uint32_t)3);

    subtree_map_destroy(map);
    PASS();
}

/* -------------------------------------------------------------------
 * test_transfer_owner_requires_migrating
 *
 * Verify subtree_map_transfer_owner_if_migrating() rejects transfer
 * on ACTIVE subtrees and succeeds on MIGRATING ones.
 * ------------------------------------------------------------------- */
static void test_transfer_owner_requires_migrating(void)
{
    fprintf(stdout, "  test_transfer_owner_requires_migrating: ");
    fflush(stdout);

    struct subtree_map *map = NULL;
    enum mds_status st = subtree_map_init(NULL, NULL, 1, "localhost",
                                 NULL, &map);
    ASSERT_EQ(st, MDS_OK);

    /* Set up membership so role checks are active. */
    struct mds_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.self.id = 1;
    snprintf(cfg.self.hostname, sizeof(cfg.self.hostname), "localhost");
    cfg.self.nfs_port = 2049;
    cfg.self.grpc_port = 50051;

    struct cluster_membership *cm = NULL;
    st = cluster_membership_init(&cfg, map, NULL, &cm);
    ASSERT_EQ(st, MDS_OK);
    subtree_map_set_membership(map, cm);

    /* Register a peer node that can own subtrees. */
    struct cluster_member peer;
    memset(&peer, 0, sizeof(peer));
    peer.mds_id = 2;
    snprintf(peer.hostname, sizeof(peer.hostname), "mds2.local");
    peer.nfs_port = 2049;
    peer.grpc_port = 50051;
    st = cluster_node_join(cm, &peer);
    ASSERT_EQ(st, MDS_OK);
    /* Promote IDLE -> ACTIVE_SERVING so can_own_subtrees passes. */
    st = cluster_membership_set_lifecycle(cm, 2, NODE_ACTIVE_SERVING);
    ASSERT_EQ(st, MDS_OK);

    /* Add /data subtree in ACTIVE state. */
    st = subtree_map_add(map, "/data", 1, "localhost",
                         SUBTREE_ACTIVE, 1);
    ASSERT_EQ(st, MDS_OK);

    /* transfer_owner should FAIL on ACTIVE subtree. */
    st = subtree_map_transfer_owner_if_migrating(map, "/data", 2, 1);
    ASSERT_EQ(st, MDS_ERR_INVAL);

    /* Set state to MIGRATING. */
    st = subtree_map_set_state(map, "/data", SUBTREE_MIGRATING, 1);
    ASSERT_EQ(st, MDS_OK);

    /* Now transfer should succeed. */
    struct subtree_entry entry;
    st = subtree_map_lookup_exact(map, "/data", &entry);
    ASSERT_EQ(st, MDS_OK);
    st = subtree_map_transfer_owner_if_migrating(
        map, "/data", 2, entry.version);
    ASSERT_EQ(st, MDS_OK);

    /* Verify owner changed. */
    st = subtree_map_lookup_exact(map, "/data", &entry);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(entry.owner_mds_id, (uint32_t)2);

    cluster_membership_destroy(cm);
    subtree_map_destroy(map);
    PASS();
}

/* -------------------------------------------------------------------
 * test_set_owner_still_works_for_failover
 *
 * Verify the unmodified subtree_map_set_owner() still works on
 * ACTIVE subtrees — proving the failover path is not broken.
 * ------------------------------------------------------------------- */
static void test_set_owner_still_works_for_failover(void)
{
    fprintf(stdout, "  test_set_owner_still_works_for_failover: ");
    fflush(stdout);

    struct subtree_map *map = NULL;
    enum mds_status st = subtree_map_init(NULL, NULL, 1, "localhost",
                                 NULL, &map);
    ASSERT_EQ(st, MDS_OK);

    /* Set up membership. */
    struct mds_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.self.id = 1;
    snprintf(cfg.self.hostname, sizeof(cfg.self.hostname), "localhost");
    cfg.self.nfs_port = 2049;
    cfg.self.grpc_port = 50051;

    struct cluster_membership *cm = NULL;
    st = cluster_membership_init(&cfg, map, NULL, &cm);
    ASSERT_EQ(st, MDS_OK);
    subtree_map_set_membership(map, cm);

    /* Add peer that can own subtrees. */
    struct cluster_member peer;
    memset(&peer, 0, sizeof(peer));
    peer.mds_id = 2;
    snprintf(peer.hostname, sizeof(peer.hostname), "mds2.local");
    peer.nfs_port = 2049;
    peer.grpc_port = 50051;
    st = cluster_node_join(cm, &peer);
    ASSERT_EQ(st, MDS_OK);
    /* Promote IDLE -> ACTIVE_SERVING so can_own_subtrees passes. */
    st = cluster_membership_set_lifecycle(cm, 2, NODE_ACTIVE_SERVING);
    ASSERT_EQ(st, MDS_OK);

    /* Add /data subtree in ACTIVE state. */
    st = subtree_map_add(map, "/data", 1, "localhost",
                         SUBTREE_ACTIVE, 1);
    ASSERT_EQ(st, MDS_OK);

    /* set_owner on ACTIVE subtree should SUCCEED (failover path). */
    st = subtree_map_set_owner(map, "/data", 2, 1);
    ASSERT_EQ(st, MDS_OK);

    struct subtree_entry entry;
    st = subtree_map_lookup_exact(map, "/data", &entry);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(entry.owner_mds_id, (uint32_t)2);

    cluster_membership_destroy(cm);
    subtree_map_destroy(map);
    PASS();
}

/* -----------------------------------------------------------------------
 * Seq 10 — get_node_subtrees returns multiple
 * ----------------------------------------------------------------------- */

static void test_get_node_subtrees_multiple(void)
{
    struct subtree_map *map = NULL;
    assert(subtree_map_init(NULL, NULL, 1, "test.local", NULL, &map) == MDS_OK);

    assert(subtree_map_add(map, "/a", 1, NULL, SUBTREE_ACTIVE, 0) == MDS_OK);
    assert(subtree_map_add(map, "/b", 1, NULL, SUBTREE_ACTIVE, 0) == MDS_OK);
    assert(subtree_map_add(map, "/c", 1, NULL, SUBTREE_ACTIVE, 0) == MDS_OK);

    struct subtree_entry *entries = NULL;
    uint32_t count = 0;
    assert(subtree_map_get_node_subtrees(map, 1, &entries, &count) == MDS_OK);
    /* Root "/" plus /a, /b, /c = 4 */
    assert(count == 4);
    free(entries);

    PASS();
    subtree_map_destroy(map);
}

/* -----------------------------------------------------------------------
 * Seq 10 — get_node_subtrees empty for unknown owner
 * ----------------------------------------------------------------------- */

static void test_get_node_subtrees_empty(void)
{
    struct subtree_map *map = NULL;
    assert(subtree_map_init(NULL, NULL, 1, "test.local", NULL, &map) == MDS_OK);

    struct subtree_entry *entries = NULL;
    uint32_t count = 0;
    assert(subtree_map_get_node_subtrees(map, 99, &entries, &count) == MDS_OK);
    assert(count == 0);
    /* entries may be NULL for 0 count. */
    free(entries);

    PASS();
    subtree_map_destroy(map);
}

/* -----------------------------------------------------------------------
 * Seq 10 — list_snapshot returns all entries
 * ----------------------------------------------------------------------- */

static void test_list_snapshot_all(void)
{
    struct subtree_map *map = NULL;
    assert(subtree_map_init(NULL, NULL, 1, "test.local", NULL, &map) == MDS_OK);

    assert(subtree_map_add(map, "/x", 1, NULL, SUBTREE_ACTIVE, 0) == MDS_OK);
    assert(subtree_map_add(map, "/y", 2, NULL, SUBTREE_ACTIVE, 0) == MDS_OK);
    assert(subtree_map_add(map, "/z", 1, NULL, SUBTREE_ACTIVE, 0) == MDS_OK);
    assert(subtree_map_add(map, "/w", 2, NULL, SUBTREE_ACTIVE, 0) == MDS_OK);

    struct subtree_entry *snap = NULL;
    uint32_t count = 0;
    assert(subtree_map_list_snapshot(map, &snap, &count) == MDS_OK);
    /* Root + 4 additions = 5. */
    assert(count == 5);
    free(snap);

    PASS();
    subtree_map_destroy(map);
}

/* -----------------------------------------------------------------------
 * Seq 10 — list after take_over reflects new owner
 * ----------------------------------------------------------------------- */

static void test_subtree_list_after_takeover(void)
{
    struct subtree_map *map = NULL;
    assert(subtree_map_init(NULL, NULL, 1, "test.local", NULL, &map) == MDS_OK);

    assert(subtree_map_add(map, "/d1", 2, NULL, SUBTREE_ACTIVE, 0) == MDS_OK);
    assert(subtree_map_add(map, "/d2", 2, NULL, SUBTREE_ACTIVE, 0) == MDS_OK);

    /* Take over from owner 2 → owner 1. */
    uint32_t takeover_count = 0;
    assert(subtree_map_take_over(map, 2, 1, &takeover_count) == MDS_OK);

    struct subtree_entry *entries = NULL;
    uint32_t count = 0;
    assert(subtree_map_get_node_subtrees(map, 1, &entries, &count) == MDS_OK);

    /* Root (owner 1) + /d1 + /d2 = at least 3 owned by node 1. */
    bool found_d1 = false, found_d2 = false;
    for (uint32_t i = 0; i < count; i++) {
        if (strcmp(entries[i].path, "/d1") == 0) { found_d1 = true; }
        if (strcmp(entries[i].path, "/d2") == 0) { found_d2 = true; }
    }
    assert(found_d1);
    assert(found_d2);
    free(entries);

    PASS();
    subtree_map_destroy(map);
}

int main(void)
{
    fprintf(stdout, "test_subtree_map:\n");

    test_init_destroy();
    test_lookup_root();
    test_lookup_subtree();
    test_longest_prefix();
    test_is_local();
    test_set_owner_cas();
    test_set_state();
    test_referral_build();
    test_fs_locations_encode();
    test_subtree_json_roundtrip();
    test_take_over_local_mode();
    test_subtree_write_stable_path();
    test_transfer_owner_requires_migrating();
    test_set_owner_still_works_for_failover();
    test_get_node_subtrees_multiple();
    test_get_node_subtrees_empty();
    test_list_snapshot_all();
    test_subtree_list_after_takeover();

    fprintf(stdout, "\n  %d passed, %d failed\n", passed, failed);
    return failed ? 1 : 0;
}
