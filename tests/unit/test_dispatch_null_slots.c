/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_dispatch_null_slots.c -- The dispatcher never calls a NULL slot.
 *
 * A backend under construction (or one that legitimately lacks a
 * feature) leaves vtable slots NULL.  Contract C4/C5
 * (catalogue_internal.h): an absent slot is MDS_ERR_NOSUPPORT from the
 * dispatcher, never a NULL function-pointer call.  This test fabricates
 * a catalogue whose authority, coordination, cluster and lifecycle
 * tables are entirely NULL and drives every guarded entry point with
 * otherwise valid arguments, so a guard that goes missing shows up as a
 * crash here rather than in a daemon.  Argument validation (INVAL)
 * stays ahead of the slot check, so every call below passes arguments
 * that survive it.
 *
 * The mandatory namespace core (ns_lookup, ns_create, ...) is not
 * covered: every backend populates it at construction and slot_matrix
 * pins that.
 */

#include <stdio.h>
#include <string.h>
#include <time.h>

#include "mds_catalogue.h"
#include "mds_coordination.h"
#include "mds_cluster.h"
#include "catalogue_internal.h"
#include "open_state.h"    /* struct nfs4_stateid */
#include "quota.h"         /* struct mds_quota_rule / mds_quota_usage */

/* ----------------------------------------------------------------------- */

static int tests_run    = 0;
static int tests_passed = 0;
static int test_failed  = 0;  /* Set by ASSERT_* so RUN_TEST records it. */

#define ASSERT_EQ(a, b) do { \
    if ((a) != (b)) { \
        fprintf(stderr, "  FAIL %s:%d: %s (%d) != %s (%d)\n", \
            __FILE__, __LINE__, #a, (int)(a), #b, (int)(b)); \
        test_failed = 1; \
        return; \
    } \
} while (0)

#define RUN_TEST(fn) do { \
    fprintf(stdout, "  %-55s", #fn); \
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

/* ----------------------------------------------------------------------- */

/* Every table present, every slot NULL: the shape of a backend that
 * implements no optional slot at all. */
static struct mds_authority_ops    null_auth_ops;
static struct mds_coordination_ops null_coord_ops;
static struct mds_cluster_ops      null_cluster_ops;
static struct mds_catalogue_ops    null_cat_ops;

static struct mds_catalogue make_test_cat(void)
{
    struct mds_catalogue cat;

    memset(&cat, 0, sizeof(cat));
    memset(&null_auth_ops, 0, sizeof(null_auth_ops));
    memset(&null_coord_ops, 0, sizeof(null_coord_ops));
    memset(&null_cluster_ops, 0, sizeof(null_cluster_ops));
    memset(&null_cat_ops, 0, sizeof(null_cat_ops));
    cat.auth_ops = &null_auth_ops;
    cat.coord_ops = &null_coord_ops;
    cat.cluster_ops = &null_cluster_ops;
    cat.ops = &null_cat_ops;
    cat.backend = MDS_BACKEND_FDB;
    return cat;
}

static int xattr_cb(const char *name, size_t name_len, void *arg)
{
    (void)name;
    (void)name_len;
    (void)arg;
    return 0;
}

static int stripe_scan_cb(uint64_t fileid, uint32_t stripe_count,
                          uint32_t stripe_unit, uint32_t mirror_count,
                          const struct mds_ds_map_entry *entries, void *ctx)
{
    (void)fileid;
    (void)stripe_count;
    (void)stripe_unit;
    (void)mirror_count;
    (void)entries;
    (void)ctx;
    return 0;
}

static int remove_pending_cb(const struct mds_remove_pending_entry *entry,
                             void *ctx)
{
    (void)entry;
    (void)ctx;
    return 0;
}

static int readdir_plus_cb(const struct mds_cat_dirent *entry,
                           const struct mds_inode *inode,
                           bool inode_valid, void *arg)
{
    int *calls = arg;

    (void)entry;
    (void)inode;
    (void)inode_valid;
    (*calls)++;
    return 0;
}

/* --- Inline data / xattr ---------------------------------------------- */

static void test_inline_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    uint8_t buf[16];
    uint32_t outlen = 0;

    ASSERT_EQ(mds_cat_inline_get(&cat, 42, buf, sizeof(buf), &outlen),
              MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_cat_inline_put(&cat, NULL, 42, buf, sizeof(buf)),
              MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_cat_inline_del(&cat, NULL, 42), MDS_ERR_NOSUPPORT);
}

static void test_xattr_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    void *val = NULL;
    uint32_t vallen = 0;
    uint8_t v[4] = {1, 2, 3, 4};

    ASSERT_EQ(mds_cat_xattr_get(&cat, 42, "user.k", &val, &vallen),
              MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_cat_xattr_put(&cat, NULL, 42, "user.k", v, sizeof(v)),
              MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_cat_xattr_del(&cat, NULL, 42, "user.k"), MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_cat_xattr_list(&cat, 42, xattr_cb, NULL), MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_cat_xattr_exists(&cat, 42, "user.k"), MDS_ERR_NOSUPPORT);
}

/* --- Stripe maps -------------------------------------------------------- */

static void test_stripe_map_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    struct mds_ds_map_entry entry;
    struct mds_ds_map_entry *entries = NULL;
    uint32_t sc = 0;
    uint32_t su = 0;
    uint32_t mc = 0;

    memset(&entry, 0, sizeof(entry));
    ASSERT_EQ(mds_cat_stripe_map_get(&cat, 42, &sc, &su, &mc, &entries),
              MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_cat_stripe_map_put(&cat, NULL, 42, 1, 65536, 1, &entry),
              MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_cat_stripe_map_del(&cat, NULL, 42), MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_cat_stripe_map_scan(&cat, stripe_scan_cb, NULL),
              MDS_ERR_NOSUPPORT);
}

/* --- DS registry / provisioning --------------------------------------- */

static void test_ds_registry_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    struct mds_ds_info info;
    struct mds_ds_info *list = NULL;
    uint32_t count = 0;
    uint8_t secret[32];
    uint64_t epoch = 0;

    memset(&info, 0, sizeof(info));
    memset(secret, 0, sizeof(secret));
    ASSERT_EQ(mds_cat_ds_get(&cat, 1, &info), MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_cat_ds_put(&cat, NULL, &info), MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_cat_ds_del(&cat, NULL, 1), MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_cat_ds_list(&cat, &list, &count), MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_cat_ds_provision_get(&cat, 1, secret, sizeof(secret), &epoch),
              MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_cat_ds_provision_put(&cat, NULL, 1, secret, sizeof(secret), 7),
              MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_cat_ds_provision_del(&cat, NULL, 1), MDS_ERR_NOSUPPORT);
}

/* --- Quota -------------------------------------------------------------- */

static void test_quota_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    struct mds_quota_rule rule;
    struct mds_quota_usage usage;

    memset(&rule, 0, sizeof(rule));
    memset(&usage, 0, sizeof(usage));
    ASSERT_EQ(mds_cat_quota_rule_get(&cat, 1, 1000, &rule), MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_cat_quota_rule_put(&cat, NULL, 1, 1000, &rule),
              MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_cat_quota_usage_get(&cat, 1, 1000, &usage), MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_cat_quota_usage_put(&cat, NULL, 1, 1000, &usage),
              MDS_ERR_NOSUPPORT);
}

/* --- GC queue ------------------------------------------------------------ */

static void test_gc_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    struct mds_gc_entry entry;
    struct mds_gc_entry batch[4];
    uint8_t fh[8] = {0};
    uint32_t n = 0;

    memset(&entry, 0, sizeof(entry));
    ASSERT_EQ(mds_cat_gc_enqueue(&cat, NULL, 42, 1, fh, sizeof(fh)),
              MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_cat_gc_enqueue_hint(&cat, NULL, 42, 1, fh, sizeof(fh), 0),
              MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_cat_gc_peek(&cat, &entry), MDS_ERR_NOSUPPORT);
    /* No batch slot and no single-row slot: the fallback reports the
     * missing slot, never a NULL call. */
    ASSERT_EQ(mds_cat_gc_peek_batch(&cat, batch, 4, &n), MDS_ERR_NOSUPPORT);
    ASSERT_EQ(n, 0U);
    ASSERT_EQ(mds_cat_gc_dequeue(&cat, NULL, 1), MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_cat_gc_count(&cat, &n), MDS_ERR_NOSUPPORT);
}

/* --- Async-REMOVE manifest --------------------------------------------- */

static void test_remove_pending_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    struct mds_remove_pending_entry entries[2];
    uint64_t seq = 0;
    uint32_t n = 0;

    ASSERT_EQ(mds_cat_remove_pending_enqueue(&cat, NULL, 2, "f", 42, 1, &seq),
              MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_cat_remove_pending_enqueue_unlink(&cat, NULL, 2, "f", 42, 1,
                                                    &seq),
              MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_cat_remove_pending_peek_batch(&cat, 1, entries, 2, &n),
              MDS_ERR_NOSUPPORT);
    ASSERT_EQ(n, 0U);
    ASSERT_EQ(mds_cat_remove_pending_claim(&cat, 1, 1, 1, 1, 1),
              MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_cat_remove_pending_complete(&cat, 1), MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_cat_remove_pending_bump_retry(&cat, 1), MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_cat_remove_pending_count(&cat, &n), MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_cat_remove_pending_scan_all(&cat, remove_pending_cb, NULL),
              MDS_ERR_NOSUPPORT);
}

/* --- Prealloc pool / shard map / ext dirent / link anchor -------------- */

static void test_prealloc_pool_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    struct mds_prealloc_pool_row *rows = NULL;
    uint8_t fh[8] = {0};
    uint32_t n = 0;

    ASSERT_EQ(mds_cat_prealloc_pool_insert(&cat, 42, 1, fh, sizeof(fh), 1, 65536),
              MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_cat_prealloc_pool_scan(&cat, 1, &rows, &n), MDS_ERR_NOSUPPORT);
    ASSERT_EQ(n, 0U);
    ASSERT_EQ(mds_cat_prealloc_pool_delete(&cat, 42), MDS_ERR_NOSUPPORT);
}

static void test_shard_fileid_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    uint32_t shard = 0;

    ASSERT_EQ(mds_cat_shard_fileid_get(&cat, 42, &shard), MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_cat_shard_fileid_put(&cat, NULL, 42, 1), MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_cat_shard_fileid_del(&cat, NULL, 42), MDS_ERR_NOSUPPORT);
}

static void test_ext_dirent_link_anchor_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    uint32_t owner = 0;
    uint64_t target = 0;
    uint8_t type = 0;
    uint64_t anchor = 0;

    ASSERT_EQ(mds_cat_ext_dirent_get(&cat, 2, "x", &owner, &target, &type,
                                     &anchor),
              MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_cat_ext_dirent_put(&cat, NULL, 2, "x", 1, 42, 1, 7),
              MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_cat_ext_dirent_del(&cat, NULL, 2, "x"), MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_cat_link_anchor_put(&cat, NULL, 7, 1, 2, "x"),
              MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_cat_link_anchor_del(&cat, NULL, 7), MDS_ERR_NOSUPPORT);
}

/* --- READDIR cookie resume ---------------------------------------------- */

/* A backend with neither ns_readdir_plus_from nor dirent_name_for_child
 * cannot resume a READDIR from a cookie.  The dispatcher must say so
 * (NOSUPPORT, and no entry delivered) rather than answer an empty,
 * drained page: that MDS_OK would silently end every directory listing
 * at its second page. */
static void test_readdir_cookie_resume_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    char name[MDS_MAX_NAME + 1];
    int calls = 0;

    ASSERT_EQ(mds_cat_ns_dirent_name_for_child(&cat, 2, 42, name, sizeof(name)),
              MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_cat_ns_readdir_plus_from_cookie(&cat, 2, 42, 0, NULL,
                                                  readdir_plus_cb, &calls),
              MDS_ERR_NOSUPPORT);
    ASSERT_EQ(calls, 0);
}

/* --- Raw rows, composite namespace ops, stats -------------------------- */

/* The raw-row slots belong to the core every backend populates; the
 * dispatcher has always answered an absent one with INVAL (inode_put:
 * NOSUPPORT).  Pinned as is: the point is that none of them is a NULL
 * call. */
static void test_raw_rows_guarded(void)
{
    struct mds_catalogue cat = make_test_cat();
    struct mds_inode ino;

    memset(&ino, 0, sizeof(ino));
    ino.fileid = 42;
    ino.type = MDS_FTYPE_REG;
    ASSERT_EQ(mds_cat_dirent_put(&cat, NULL, 2, "f", 42, (uint8_t)MDS_FTYPE_REG),
              MDS_ERR_INVAL);
    ASSERT_EQ(mds_cat_dirent_insert(&cat, NULL, 2, "f", 42,
                                    (uint8_t)MDS_FTYPE_REG),
              MDS_ERR_INVAL);
    ASSERT_EQ(mds_cat_dirent_del(&cat, NULL, 2, "f"), MDS_ERR_INVAL);
    ASSERT_EQ(mds_cat_inode_put(&cat, NULL, &ino), MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_cat_inode_del(&cat, NULL, 42), MDS_ERR_INVAL);
}

static void test_ns_composites_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    struct mds_inode child;
    struct mds_inode out;
    struct nfs4_stateid sid;
    struct mds_ds_map_entry layout_entry;
    struct timespec now = {1, 0};
    bool layout_ok = true;
    bool gc_folded = true;
    uint32_t pop_su = 1;

    memset(&child, 0, sizeof(child));
    memset(&out, 0, sizeof(out));
    memset(&sid, 0, sizeof(sid));
    child.fileid = 42;
    child.type = MDS_FTYPE_REG;
    ASSERT_EQ(mds_cat_ns_create_with_layout(&cat, 2, "f", MDS_FTYPE_REG, 0644,
                                            0, 0, NULL, &out, 1, 1, 0,
                                            UINT64_MAX, &sid, 1, &layout_ok,
                                            &layout_entry, &pop_su),
              MDS_ERR_NOSUPPORT);
    ASSERT_EQ(layout_ok, false);
    ASSERT_EQ(pop_su, 0U);
    ASSERT_EQ(mds_cat_ns_remove_known_gc(&cat, NULL, 2, "f", &child, 1, NULL, 0,
                                         0, &gc_folded),
              MDS_ERR_NOSUPPORT);
    ASSERT_EQ(gc_folded, false);
    ASSERT_EQ(mds_cat_ns_parent_touch(&cat, 2, 1, now), MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_cat_ns_create_with_layout_supported(&cat), false);
    ASSERT_EQ(mds_cat_ns_parent_touch_supported(&cat), false);
}

static void test_backend_client_stats_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    struct mds_cat_backend_client_stats st;

    ASSERT_EQ(mds_cat_backend_client_stats(&cat, &st), MDS_ERR_NOSUPPORT);
}

/* --- Coordination: layout state and client recovery ------------------- */

static void test_coord_layout_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    struct nfs4_stateid sid;
    uint8_t other[12] = {0};
    uint32_t ds_ids[1] = {1};
    uint64_t clientid = 0;
    uint64_t fileid = 0;
    uint32_t iomode = 0;
    uint64_t offset = 0;
    uint64_t length = 0;
    uint32_t seqid = 0;
    bool has_layout = true;

    memset(&sid, 0, sizeof(sid));
    ASSERT_EQ(mds_coord_layout_grant(&cat, NULL, 1, 42, 1, 0, UINT64_MAX, &sid,
                                     ds_ids, 1),
              MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_coord_layout_return(&cat, NULL, other, 1, 42, ds_ids, 1),
              MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_coord_layout_get_by_stateid(&cat, other, &clientid, &fileid,
                                              &iomode, &offset, &length,
                                              &seqid),
              MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_coord_layout_scan_for_file(&cat, 42, &has_layout),
              MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_coord_layout_del_all_for_client(&cat, 1), MDS_ERR_NOSUPPORT);
}

static void test_coord_recovery_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    uint8_t owner[8] = {0};
    uint8_t verifier[8] = {0};
    uint32_t owner_len = sizeof(owner);

    ASSERT_EQ(mds_coord_recovery_put(&cat, NULL, 1, owner, sizeof(owner),
                                     verifier),
              MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_coord_recovery_get(&cat, 1, owner, &owner_len, verifier),
              MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_coord_recovery_del(&cat, NULL, 1), MDS_ERR_NOSUPPORT);
}

/* --- Lifecycle ----------------------------------------------------------- */

static void test_lifecycle_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();

    ASSERT_EQ(mds_catalogue_bootstrap(&cat), MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_catalogue_bootstrap_supported(&cat), false);
    ASSERT_EQ(mds_catalogue_image_feed_stop(&cat), MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_catalogue_image_feed_supported(&cat), false);
    ASSERT_EQ(mds_catalogue_backend_handle(&cat) == NULL, true);
}

/* ----------------------------------------------------------------------- */

int main(void)
{
    fprintf(stdout, "test_dispatch_null_slots:\n");

    RUN_TEST(test_inline_nosupport);
    RUN_TEST(test_xattr_nosupport);
    RUN_TEST(test_stripe_map_nosupport);
    RUN_TEST(test_ds_registry_nosupport);
    RUN_TEST(test_quota_nosupport);
    RUN_TEST(test_gc_nosupport);
    RUN_TEST(test_remove_pending_nosupport);
    RUN_TEST(test_prealloc_pool_nosupport);
    RUN_TEST(test_shard_fileid_nosupport);
    RUN_TEST(test_ext_dirent_link_anchor_nosupport);
    RUN_TEST(test_readdir_cookie_resume_nosupport);
    RUN_TEST(test_raw_rows_guarded);
    RUN_TEST(test_ns_composites_nosupport);
    RUN_TEST(test_backend_client_stats_nosupport);
    RUN_TEST(test_coord_layout_nosupport);
    RUN_TEST(test_coord_recovery_nosupport);
    RUN_TEST(test_lifecycle_nosupport);

    fprintf(stdout, "\n  %d/%d tests passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
