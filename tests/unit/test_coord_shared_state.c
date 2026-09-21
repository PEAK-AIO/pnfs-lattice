/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_coord_shared_state.c -- Unit tests for shared protocol-state
 *                              coordination API (shared-attr Stage 1+).
 *
 * Stage 1: verifies dispatch stubs return MDS_ERR_NOSUPPORT when
 * the vtable entries are NULL.
 * Stage 2+: tests will be extended as real implementations are wired.
 */

#include <stdio.h>
#include <string.h>

#include "mds_catalogue.h"
#include "mds_coordination.h"
#include "mds_cluster.h"
#include "catalogue_internal.h"
#include "open_state.h"    /* struct nfs4_stateid */

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

/* A failed assertion must fail the suite: RUN_TEST previously
 * counted every test as passed because the ASSERT_* macros only
 * printed and returned, which let stale assertions rot silently. */
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

/**
 * Build a minimal catalogue handle with coord_ops that have all
 * shared-state pointers set to NULL (Stage 1 state).
 */
static struct mds_coordination_ops null_coord_ops;
static struct mds_catalogue_ops    null_cat_ops;

static struct mds_catalogue make_test_cat(void)
{
    struct mds_catalogue cat;
    memset(&cat, 0, sizeof(cat));
    memset(&null_coord_ops, 0, sizeof(null_coord_ops));
    memset(&null_cat_ops, 0, sizeof(null_cat_ops));
    cat.coord_ops = &null_coord_ops;
    cat.ops = &null_cat_ops;
    cat.backend = MDS_BACKEND_RONDB;
    return cat;
}

/* --- Open/share state ------------------------------------------------- */

static void test_open_put_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    struct mds_coord_open_row row;
    memset(&row, 0, sizeof(row));
    ASSERT_EQ(mds_coord_open_put(&cat, &row), MDS_ERR_NOSUPPORT);
}

static void test_open_get_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    struct mds_coord_open_row row;
    uint8_t sid[12] = {0};
    ASSERT_EQ(mds_coord_open_get(&cat, sid, &row), MDS_ERR_NOSUPPORT);
}

static void test_open_del_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    uint8_t sid[12] = {0};
    ASSERT_EQ(mds_coord_open_del(&cat, sid), MDS_ERR_NOSUPPORT);
}

static void test_open_scan_file_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    ASSERT_EQ(mds_coord_open_scan_file(&cat, 42, NULL, NULL),
              MDS_ERR_NOSUPPORT);
}

static void test_open_scan_client_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    ASSERT_EQ(mds_coord_open_scan_client(&cat, 1, NULL, NULL),
              MDS_ERR_NOSUPPORT);
}

/* --- Byte-range locks ------------------------------------------------- */

static void test_lock_put_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    struct mds_coord_lock_row row;
    memset(&row, 0, sizeof(row));
    ASSERT_EQ(mds_coord_lock_put(&cat, &row), MDS_ERR_NOSUPPORT);
}

static void test_lock_del_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    ASSERT_EQ(mds_coord_lock_del(&cat, 42, 1), MDS_ERR_NOSUPPORT);
}

static void test_lock_test_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    struct mds_coord_lock_row conflict;
    ASSERT_EQ(mds_coord_lock_test(&cat, 42, 1, 0, 100, 1, NULL, 0,
                                   &conflict), MDS_ERR_NOSUPPORT);
}

static void test_lock_scan_file_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    ASSERT_EQ(mds_coord_lock_scan_file(&cat, 42, NULL, NULL),
              MDS_ERR_NOSUPPORT);
}

static void test_lock_reap_client_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    ASSERT_EQ(mds_coord_lock_reap_client(&cat, 1), MDS_ERR_NOSUPPORT);
}

/* --- Delegations ------------------------------------------------------ */

static void test_deleg_put_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    struct mds_coord_deleg_row row;
    memset(&row, 0, sizeof(row));
    ASSERT_EQ(mds_coord_deleg_put(&cat, &row), MDS_ERR_NOSUPPORT);
}

static void test_deleg_get_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    struct mds_coord_deleg_row row;
    uint8_t sid[12] = {0};
    ASSERT_EQ(mds_coord_deleg_get(&cat, sid, &row), MDS_ERR_NOSUPPORT);
}

static void test_deleg_del_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    uint8_t sid[12] = {0};
    ASSERT_EQ(mds_coord_deleg_del(&cat, sid), MDS_ERR_NOSUPPORT);
}

/* --- Client identity -------------------------------------------------- */

static void test_client_put_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    struct mds_coord_client_row row;
    memset(&row, 0, sizeof(row));
    ASSERT_EQ(mds_coord_client_put(&cat, &row), MDS_ERR_NOSUPPORT);
}

static void test_client_get_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    struct mds_coord_client_row row;
    ASSERT_EQ(mds_coord_client_get(&cat, 1, &row), MDS_ERR_NOSUPPORT);
}

static void test_client_del_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    ASSERT_EQ(mds_coord_client_del(&cat, 1), MDS_ERR_NOSUPPORT);
}

/* --- Sessions --------------------------------------------------------- */

static void test_session_put_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    struct mds_coord_session_row row;
    memset(&row, 0, sizeof(row));
    ASSERT_EQ(mds_coord_session_put(&cat, &row), MDS_ERR_NOSUPPORT);
}

static void test_session_get_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    struct mds_coord_session_row row;
    uint8_t sid[16] = {0};
    ASSERT_EQ(mds_coord_session_get(&cat, sid, &row), MDS_ERR_NOSUPPORT);
}

static void test_session_del_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    uint8_t sid[16] = {0};
    ASSERT_EQ(mds_coord_session_del(&cat, sid), MDS_ERR_NOSUPPORT);
}

/* --- DRC slots -------------------------------------------------------- */

static void test_slot_put_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    uint8_t sid[16] = {0};
    ASSERT_EQ(mds_coord_slot_put(&cat, sid, 0, 1, NULL, 0),
              MDS_ERR_NOSUPPORT);
}

static void test_slot_get_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    struct mds_coord_drc_slot_row row;
    uint8_t sid[16] = {0};
    ASSERT_EQ(mds_coord_slot_get(&cat, sid, 0, &row),
              MDS_ERR_NOSUPPORT);
}

/* --- NULL catalogue safety -------------------------------------------- */

static void test_null_cat_returns_nosupport(void)
{
    struct mds_coord_open_row orow;
    memset(&orow, 0, sizeof(orow));
    ASSERT_EQ(mds_coord_open_put(NULL, &orow), MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_coord_lock_put(NULL, NULL), MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_coord_deleg_put(NULL, NULL), MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_coord_client_put(NULL, NULL), MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_coord_session_put(NULL, NULL), MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_coord_slot_put(NULL, NULL, 0, 0, NULL, 0),
              MDS_ERR_NOSUPPORT);
}

/* --- Optional lifecycle / fused slots: NULL slot behaviour ------------ */

static struct mds_authority_ops null_auth_ops;

/** make_test_cat() plus an all-NULL authority table. */
static struct mds_catalogue make_test_cat_with_auth(void)
{
    struct mds_catalogue cat = make_test_cat();
    memset(&null_auth_ops, 0, sizeof(null_auth_ops));
    cat.auth_ops = &null_auth_ops;
    return cat;
}

static void test_bootstrap_nosupport_when_slot_null(void)
{
    struct mds_catalogue cat = make_test_cat();
    ASSERT_EQ(mds_catalogue_bootstrap_supported(&cat), false);
    ASSERT_EQ(mds_catalogue_bootstrap(&cat), MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_catalogue_bootstrap_supported(NULL), false);
    ASSERT_EQ(mds_catalogue_bootstrap(NULL), MDS_ERR_INVAL);
}

static void test_backend_handle_null_when_slot_null(void)
{
    struct mds_catalogue cat = make_test_cat();
    cat.backend_private = &cat;  /* must NOT be returned or cast */
    ASSERT_EQ(mds_catalogue_backend_handle(&cat) == NULL, 1);
    ASSERT_EQ(mds_catalogue_backend_handle(NULL) == NULL, 1);
}

static void test_shared_authority_follows_caps(void)
{
    struct mds_catalogue cat = make_test_cat();
    ASSERT_EQ(mds_catalogue_shared_authority(&cat), false);
    cat.caps |= MDS_CAT_CAP_SHARED_AUTHORITY;
    ASSERT_EQ(mds_catalogue_shared_authority(&cat), true);
    ASSERT_EQ(mds_catalogue_shared_authority(NULL), false);
}

static void test_shared_state_supported_requires_all_three(void)
{
    struct mds_catalogue cat = make_test_cat();
    ASSERT_EQ(mds_coord_shared_state_supported(&cat), false);
    ASSERT_EQ(mds_coord_shared_state_supported(NULL), false);
}

static void test_layoutget_fused_nosupport_when_slot_null(void)
{
    struct mds_catalogue cat = make_test_cat();
    struct nfs4_stateid sid;
    uint32_t sc = 7, su = 7, mc = 7;
    struct mds_ds_map_entry *entries = (struct mds_ds_map_entry *)&cat;

    memset(&sid, 0, sizeof(sid));
    ASSERT_EQ(mds_coord_layoutget_fused_supported(&cat), false);
    ASSERT_EQ(mds_coord_layoutget_fused(&cat, 42, &sc, &su, &mc, &entries,
                                        &sid, 1, 2, 0, UINT64_MAX, 1),
              MDS_ERR_NOSUPPORT);
    /* Documented safe state: no entries buffer handed back. */
    ASSERT_EQ(entries == NULL, 1);
    /* NULL out-params are a caller bug, not a NOSUPPORT. */
    ASSERT_EQ(mds_coord_layoutget_fused(&cat, 42, NULL, &su, &mc, &entries,
                                        &sid, 1, 2, 0, UINT64_MAX, 1),
              MDS_ERR_INVAL);
}

static void test_ns_create_with_layout_nosupport_when_slot_null(void)
{
    struct mds_catalogue cat = make_test_cat_with_auth();
    struct nfs4_stateid sid;
    struct mds_inode out;
    struct mds_ds_map_entry entry;
    bool layout_ok = true;
    uint32_t pop_unit = 99;

    memset(&sid, 0, sizeof(sid));
    memset(&entry, 0xFF, sizeof(entry));
    ASSERT_EQ(mds_cat_ns_create_with_layout_supported(&cat), false);
    ASSERT_EQ(mds_cat_ns_create_with_layout(&cat, 2, "f", MDS_FTYPE_REG,
                                            0644, 0, 0, NULL, &out,
                                            1, 2, 0, UINT64_MAX, &sid, 1,
                                            &layout_ok, &entry, &pop_unit),
              MDS_ERR_NOSUPPORT);
    /* Documented safe state for every out-param. */
    ASSERT_EQ(layout_ok, false);
    ASSERT_EQ(pop_unit, (uint32_t)0);
    ASSERT_EQ(entry.ds_id, (uint32_t)0);
    ASSERT_EQ(entry.nfs_fh_len, (uint32_t)0);
}

/* --- Fake slots: the dispatcher must forward every argument ----------- */

struct fused_capture {
    struct mds_catalogue *cat;
    uint64_t fileid;
    const struct nfs4_stateid *stateid;
    uint64_t clientid;
    uint32_t iomode;
    uint64_t offset;
    uint64_t length;
    uint32_t mds_id;
    int calls;
};
static struct fused_capture g_fused;
static struct mds_ds_map_entry g_fused_entry;

static enum mds_status fake_layoutget_fused(
    struct mds_catalogue *cat, uint64_t fileid,
    uint32_t *stripe_count, uint32_t *stripe_unit,
    uint32_t *mirror_count, struct mds_ds_map_entry **entries,
    const struct nfs4_stateid *stateid,
    uint64_t clientid, uint32_t iomode, uint64_t offset,
    uint64_t length, uint32_t mds_id)
{
    g_fused.cat = cat;
    g_fused.fileid = fileid;
    g_fused.stateid = stateid;
    g_fused.clientid = clientid;
    g_fused.iomode = iomode;
    g_fused.offset = offset;
    g_fused.length = length;
    g_fused.mds_id = mds_id;
    g_fused.calls++;
    *stripe_count = 1;
    *stripe_unit = 65536;
    *mirror_count = 1;
    *entries = &g_fused_entry;
    return MDS_ERR_DELAY;  /* any distinctive status */
}

static void test_layoutget_fused_forwards_arguments(void)
{
    struct mds_catalogue cat = make_test_cat();
    struct mds_coordination_ops ops;
    struct nfs4_stateid sid;
    uint32_t sc = 0, su = 0, mc = 0;
    struct mds_ds_map_entry *entries = NULL;

    memset(&ops, 0, sizeof(ops));
    ops.layoutget_fused = fake_layoutget_fused;
    cat.coord_ops = &ops;
    memset(&g_fused, 0, sizeof(g_fused));
    memset(&sid, 0x3C, sizeof(sid));

    ASSERT_EQ(mds_coord_layoutget_fused_supported(&cat), true);
    ASSERT_EQ(mds_coord_layoutget_fused(&cat, 0x1234, &sc, &su, &mc,
                                        &entries, &sid, 0x55, 2,
                                        4096, 8192, 9),
              MDS_ERR_DELAY);
    ASSERT_EQ(g_fused.calls, 1);
    ASSERT_EQ(g_fused.cat == &cat, 1);
    ASSERT_EQ(g_fused.fileid, (uint64_t)0x1234);
    ASSERT_EQ(g_fused.stateid == &sid, 1);
    ASSERT_EQ(g_fused.clientid, (uint64_t)0x55);
    ASSERT_EQ(g_fused.iomode, (uint32_t)2);
    ASSERT_EQ(g_fused.offset, (uint64_t)4096);
    ASSERT_EQ(g_fused.length, (uint64_t)8192);
    ASSERT_EQ(g_fused.mds_id, (uint32_t)9);
    ASSERT_EQ(sc, (uint32_t)1);
    ASSERT_EQ(su, (uint32_t)65536);
    ASSERT_EQ(mc, (uint32_t)1);
    ASSERT_EQ(entries == &g_fused_entry, 1);
}

struct create_capture {
    struct mds_catalogue *cat;
    uint64_t parent;
    const char *name;
    enum mds_file_type type;
    uint32_t mode;
    uint64_t uid, gid;
    struct ds_prealloc_ctx *prealloc;
    uint64_t layout_clientid;
    uint32_t layout_iomode;
    uint64_t layout_offset, layout_length;
    const struct nfs4_stateid *layout_stateid;
    uint32_t layout_mds_id;
    int calls;
};
static struct create_capture g_create;

static enum mds_status fake_ns_create_with_layout(
    struct mds_catalogue *cat, uint64_t parent, const char *name,
    enum mds_file_type type, uint32_t mode, uint64_t uid, uint64_t gid,
    struct ds_prealloc_ctx *prealloc, struct mds_inode *out,
    uint64_t layout_clientid, uint32_t layout_iomode,
    uint64_t layout_offset, uint64_t layout_length,
    const struct nfs4_stateid *layout_stateid, uint32_t layout_mds_id,
    bool *layout_ok, struct mds_ds_map_entry *layout_entry_out,
    uint32_t *layout_pop_stripe_unit_out)
{
    g_create.cat = cat;
    g_create.parent = parent;
    g_create.name = name;
    g_create.type = type;
    g_create.mode = mode;
    g_create.uid = uid;
    g_create.gid = gid;
    g_create.prealloc = prealloc;
    g_create.layout_clientid = layout_clientid;
    g_create.layout_iomode = layout_iomode;
    g_create.layout_offset = layout_offset;
    g_create.layout_length = layout_length;
    g_create.layout_stateid = layout_stateid;
    g_create.layout_mds_id = layout_mds_id;
    g_create.calls++;
    memset(out, 0, sizeof(*out));
    out->fileid = 777;
    *layout_ok = true;
    layout_entry_out->ds_id = 3;
    *layout_pop_stripe_unit_out = 131072;
    return MDS_OK;
}

static void test_ns_create_with_layout_forwards_arguments(void)
{
    struct mds_catalogue cat = make_test_cat_with_auth();
    struct mds_authority_ops ops;
    struct nfs4_stateid sid;
    struct mds_inode out;
    struct mds_ds_map_entry entry;
    bool layout_ok = false;
    uint32_t pop_unit = 0;
    int fake_prealloc;

    memset(&ops, 0, sizeof(ops));
    ops.ns_create_with_layout = fake_ns_create_with_layout;
    cat.auth_ops = &ops;
    memset(&g_create, 0, sizeof(g_create));
    memset(&sid, 0x7E, sizeof(sid));
    memset(&entry, 0, sizeof(entry));

    ASSERT_EQ(mds_cat_ns_create_with_layout_supported(&cat), true);
    ASSERT_EQ(mds_cat_ns_create_with_layout(
                  &cat, 2, "newfile", MDS_FTYPE_REG, 0640, 1000, 2000,
                  (struct ds_prealloc_ctx *)&fake_prealloc, &out,
                  0xC1, 2, 0, UINT64_MAX, &sid, 4,
                  &layout_ok, &entry, &pop_unit),
              MDS_OK);
    ASSERT_EQ(g_create.calls, 1);
    ASSERT_EQ(g_create.cat == &cat, 1);
    ASSERT_EQ(g_create.parent, (uint64_t)2);
    ASSERT_EQ(strcmp(g_create.name, "newfile"), 0);
    ASSERT_EQ(g_create.type, MDS_FTYPE_REG);
    ASSERT_EQ(g_create.mode, (uint32_t)0640);
    ASSERT_EQ(g_create.uid, (uint64_t)1000);
    ASSERT_EQ(g_create.gid, (uint64_t)2000);
    ASSERT_EQ(g_create.prealloc == (struct ds_prealloc_ctx *)&fake_prealloc,
              1);
    ASSERT_EQ(g_create.layout_clientid, (uint64_t)0xC1);
    ASSERT_EQ(g_create.layout_iomode, (uint32_t)2);
    ASSERT_EQ(g_create.layout_offset, (uint64_t)0);
    ASSERT_EQ(g_create.layout_length, UINT64_MAX);
    ASSERT_EQ(g_create.layout_stateid == &sid, 1);
    ASSERT_EQ(g_create.layout_mds_id, (uint32_t)4);
    ASSERT_EQ(out.fileid, (uint64_t)777);
    ASSERT_EQ(layout_ok, true);
    ASSERT_EQ(entry.ds_id, (uint32_t)3);
    ASSERT_EQ(pop_unit, (uint32_t)131072);
}

static int g_bootstrap_calls;
static enum mds_status fake_bootstrap(struct mds_catalogue *cat)
{
    (void)cat;
    g_bootstrap_calls++;
    return MDS_ERR_IO;
}

static void *fake_backend_handle(const struct mds_catalogue *cat)
{
    return cat->backend_private;
}

static void test_lifecycle_slots_dispatch(void)
{
    struct mds_catalogue cat = make_test_cat();
    struct mds_catalogue_ops ops;
    int private_state;

    memset(&ops, 0, sizeof(ops));
    ops.bootstrap = fake_bootstrap;
    ops.backend_handle = fake_backend_handle;
    cat.ops = &ops;
    cat.backend_private = &private_state;
    g_bootstrap_calls = 0;

    ASSERT_EQ(mds_catalogue_bootstrap_supported(&cat), true);
    ASSERT_EQ(mds_catalogue_bootstrap(&cat), MDS_ERR_IO);
    ASSERT_EQ(g_bootstrap_calls, 1);
    ASSERT_EQ(mds_catalogue_backend_handle(&cat) == &private_state, 1);
}

/* --- Backend identity ------------------------------------------------- */

static void test_backend_type_null_is_none(void)
{
    struct mds_catalogue cat = make_test_cat();

    /* A NULL handle is "no catalogue", never a guess at RonDB. */
    ASSERT_EQ(mds_catalogue_backend_type(NULL), MDS_BACKEND_NONE);
    ASSERT_EQ(mds_catalogue_backend_type(&cat), MDS_BACKEND_RONDB);
    cat.backend = MDS_BACKEND_MEMDB;
    ASSERT_EQ(mds_catalogue_backend_type(&cat), MDS_BACKEND_MEMDB);
    /* Appended values keep the existing numbering. */
    ASSERT_EQ((int)MDS_BACKEND_RONDB, 0);
    ASSERT_EQ((int)MDS_BACKEND_MEMDB, 1);
    ASSERT_EQ((int)MDS_BACKEND_FDB, 2);
    ASSERT_EQ((int)MDS_BACKEND_NONE, 3);
}

/* --- Cluster ops: NULL table / NULL slots -> NOSUPPORT ---------------- */

static int cluster_node_cb_never(uint32_t mds_id, uint64_t boot_epoch,
                                 const char *hostname,
                                 uint16_t nfs_port, uint16_t grpc_port,
                                 uint64_t last_heartbeat_ns, void *ctx)
{
    (void)mds_id; (void)boot_epoch; (void)hostname; (void)nfs_port;
    (void)grpc_port; (void)last_heartbeat_ns; (void)ctx;
    return 0;
}

static int cluster_stale_cb_never(uint32_t mds_id, uint64_t boot_epoch,
                                  uint64_t last_heartbeat_ns, void *ctx)
{
    (void)mds_id; (void)boot_epoch; (void)last_heartbeat_ns; (void)ctx;
    return 0;
}

static int cluster_partition_cb_never(uint32_t partition_id,
                                      uint32_t owner_mds_id, uint8_t state,
                                      const char *subtree_path, void *ctx)
{
    (void)partition_id; (void)owner_mds_id; (void)state;
    (void)subtree_path; (void)ctx;
    return 0;
}

/** Every dispatcher on @cat must report MDS_ERR_NOSUPPORT. */
static void assert_cluster_all_nosupport(struct mds_catalogue *cat)
{
    ASSERT_EQ(mds_cluster_node_register(cat, 1, 10, "h", 2049, 9400),
              MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_cluster_node_heartbeat(cat, 1, 10), MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_cluster_node_deregister(cat, 1, 10), MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_cluster_node_list(cat, cluster_node_cb_never, NULL),
              MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_cluster_node_scan_stale(cat, 5, cluster_stale_cb_never,
                                          NULL),
              MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_cluster_partition_list(cat, cluster_partition_cb_never,
                                         NULL),
              MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_cluster_partition_put(cat, 0, 1,
                                        MDS_PARTITION_STATE_ACTIVE, "/",
                                        false),
              MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_cluster_supported(cat), false);
}

static struct mds_cluster_ops null_cluster_ops;

static void test_cluster_nosupport_when_slots_null(void)
{
    struct mds_catalogue cat = make_test_cat();

    /* Backend without cluster services: no table at all. */
    cat.cluster_ops = NULL;
    assert_cluster_all_nosupport(&cat);
    if (test_failed) { return; }

    /* Table present, every slot NULL. */
    memset(&null_cluster_ops, 0, sizeof(null_cluster_ops));
    cat.cluster_ops = &null_cluster_ops;
    cat.caps |= MDS_CAT_CAP_MULTI_PROCESS; /* caps alone never suffice */
    assert_cluster_all_nosupport(&cat);
}

/* --- Cluster ops: fake slots must see every argument unchanged --------- */

struct cluster_capture {
    struct mds_catalogue *cat;
    uint32_t mds_id;
    uint64_t boot_epoch;
    const char *hostname;
    uint16_t nfs_port;
    uint16_t grpc_port;
    uint64_t threshold_ns;
    uint32_t partition_id;
    uint32_t owner_mds_id;
    uint8_t state;
    const char *subtree_path;
    bool insert_only;
    mds_cluster_node_cb node_cb;
    mds_cluster_stale_cb stale_cb;
    mds_cluster_partition_cb partition_cb;
    void *ctx;
    int calls;
};
static struct cluster_capture g_cl;

static enum mds_status fake_node_register(struct mds_catalogue *cat,
    uint32_t mds_id, uint64_t boot_epoch, const char *hostname,
    uint16_t nfs_port, uint16_t grpc_port)
{
    g_cl.cat = cat; g_cl.mds_id = mds_id; g_cl.boot_epoch = boot_epoch;
    g_cl.hostname = hostname; g_cl.nfs_port = nfs_port;
    g_cl.grpc_port = grpc_port; g_cl.calls++;
    return MDS_ERR_EXISTS;   /* distinctive: must pass through (C4) */
}

static enum mds_status fake_node_heartbeat(struct mds_catalogue *cat,
    uint32_t mds_id, uint64_t boot_epoch)
{
    g_cl.cat = cat; g_cl.mds_id = mds_id; g_cl.boot_epoch = boot_epoch;
    g_cl.calls++;
    return MDS_ERR_NOTFOUND;
}

static enum mds_status fake_node_deregister(struct mds_catalogue *cat,
    uint32_t mds_id, uint64_t boot_epoch)
{
    g_cl.cat = cat; g_cl.mds_id = mds_id; g_cl.boot_epoch = boot_epoch;
    g_cl.calls++;
    return MDS_ERR_STALE;
}

static enum mds_status fake_node_list(struct mds_catalogue *cat,
    mds_cluster_node_cb cb, void *ctx)
{
    g_cl.cat = cat; g_cl.node_cb = cb; g_cl.ctx = ctx; g_cl.calls++;
    return MDS_ERR_IO;
}

static enum mds_status fake_node_scan_stale(struct mds_catalogue *cat,
    uint64_t threshold_ns, mds_cluster_stale_cb cb, void *ctx)
{
    g_cl.cat = cat; g_cl.threshold_ns = threshold_ns;
    g_cl.stale_cb = cb; g_cl.ctx = ctx; g_cl.calls++;
    return MDS_ERR_DELAY;
}

static enum mds_status fake_partition_list(struct mds_catalogue *cat,
    mds_cluster_partition_cb cb, void *ctx)
{
    g_cl.cat = cat; g_cl.partition_cb = cb; g_cl.ctx = ctx; g_cl.calls++;
    return MDS_ERR_GRACE;
}

static enum mds_status fake_partition_put(struct mds_catalogue *cat,
    uint32_t partition_id, uint32_t owner_mds_id, uint8_t state,
    const char *subtree_path, bool insert_only)
{
    g_cl.cat = cat; g_cl.partition_id = partition_id;
    g_cl.owner_mds_id = owner_mds_id; g_cl.state = state;
    g_cl.subtree_path = subtree_path; g_cl.insert_only = insert_only;
    g_cl.calls++;
    return insert_only ? MDS_ERR_EXISTS : MDS_OK;
}

static const struct mds_cluster_ops fake_cluster_ops = {
    .node_register   = fake_node_register,
    .node_heartbeat  = fake_node_heartbeat,
    .node_deregister = fake_node_deregister,
    .node_list       = fake_node_list,
    .node_scan_stale = fake_node_scan_stale,
    .partition_list  = fake_partition_list,
    .partition_put   = fake_partition_put,
};

static void test_cluster_slots_forward_arguments(void)
{
    struct mds_catalogue cat = make_test_cat();
    int ctx_token;
    const char *host = "mds-a.example";
    const char *path = "/shard7";

    cat.cluster_ops = &fake_cluster_ops;

    memset(&g_cl, 0, sizeof(g_cl));
    ASSERT_EQ(mds_cluster_node_register(&cat, 3, 0xB00700, host,
                                        2049, 9401),
              MDS_ERR_EXISTS);
    ASSERT_EQ(g_cl.calls, 1);
    ASSERT_EQ(g_cl.cat == &cat, 1);
    ASSERT_EQ(g_cl.mds_id, (uint32_t)3);
    ASSERT_EQ(g_cl.boot_epoch, (uint64_t)0xB00700);
    ASSERT_EQ(g_cl.hostname == host, 1);
    ASSERT_EQ(g_cl.nfs_port, (uint16_t)2049);
    ASSERT_EQ(g_cl.grpc_port, (uint16_t)9401);

    memset(&g_cl, 0, sizeof(g_cl));
    ASSERT_EQ(mds_cluster_node_heartbeat(&cat, 4, 0xB00701),
              MDS_ERR_NOTFOUND);
    ASSERT_EQ(g_cl.calls, 1);
    ASSERT_EQ(g_cl.mds_id, (uint32_t)4);
    ASSERT_EQ(g_cl.boot_epoch, (uint64_t)0xB00701);

    /* deregister carries the epoch from the start (conditional
     * delete later): it must reach the slot, not be dropped. */
    memset(&g_cl, 0, sizeof(g_cl));
    ASSERT_EQ(mds_cluster_node_deregister(&cat, 5, 0xB00702),
              MDS_ERR_STALE);
    ASSERT_EQ(g_cl.calls, 1);
    ASSERT_EQ(g_cl.mds_id, (uint32_t)5);
    ASSERT_EQ(g_cl.boot_epoch, (uint64_t)0xB00702);

    memset(&g_cl, 0, sizeof(g_cl));
    ASSERT_EQ(mds_cluster_node_list(&cat, cluster_node_cb_never,
                                    &ctx_token),
              MDS_ERR_IO);
    ASSERT_EQ(g_cl.calls, 1);
    ASSERT_EQ(g_cl.node_cb == cluster_node_cb_never, 1);
    ASSERT_EQ(g_cl.ctx == &ctx_token, 1);

    memset(&g_cl, 0, sizeof(g_cl));
    ASSERT_EQ(mds_cluster_node_scan_stale(&cat, 15000000000ULL,
                                          cluster_stale_cb_never,
                                          &ctx_token),
              MDS_ERR_DELAY);
    ASSERT_EQ(g_cl.calls, 1);
    ASSERT_EQ(g_cl.threshold_ns, (uint64_t)15000000000ULL);
    ASSERT_EQ(g_cl.stale_cb == cluster_stale_cb_never, 1);
    ASSERT_EQ(g_cl.ctx == &ctx_token, 1);

    memset(&g_cl, 0, sizeof(g_cl));
    ASSERT_EQ(mds_cluster_partition_list(&cat, cluster_partition_cb_never,
                                         &ctx_token),
              MDS_ERR_GRACE);
    ASSERT_EQ(g_cl.calls, 1);
    ASSERT_EQ(g_cl.partition_cb == cluster_partition_cb_never, 1);
    ASSERT_EQ(g_cl.ctx == &ctx_token, 1);

    /* insert_only both ways, and the status the slot chose for each. */
    memset(&g_cl, 0, sizeof(g_cl));
    ASSERT_EQ(mds_cluster_partition_put(&cat, 7, 2,
                                        MDS_PARTITION_STATE_MIGRATING,
                                        path, true),
              MDS_ERR_EXISTS);
    ASSERT_EQ(g_cl.calls, 1);
    ASSERT_EQ(g_cl.partition_id, (uint32_t)7);
    ASSERT_EQ(g_cl.owner_mds_id, (uint32_t)2);
    ASSERT_EQ(g_cl.state, (uint8_t)MDS_PARTITION_STATE_MIGRATING);
    ASSERT_EQ(g_cl.subtree_path == path, 1);
    ASSERT_EQ(g_cl.insert_only, true);

    memset(&g_cl, 0, sizeof(g_cl));
    ASSERT_EQ(mds_cluster_partition_put(&cat, 0, 1,
                                        MDS_PARTITION_STATE_ACTIVE,
                                        "/", false),
              MDS_OK);
    ASSERT_EQ(g_cl.calls, 1);
    ASSERT_EQ(g_cl.insert_only, false);
    ASSERT_EQ(g_cl.state, (uint8_t)MDS_PARTITION_STATE_ACTIVE);
}

/* --- Cluster ops: invalid arguments -> INVAL, slot never reached ------- */

static void test_cluster_inval_on_null_arguments(void)
{
    struct mds_catalogue cat = make_test_cat();

    cat.cluster_ops = &fake_cluster_ops;
    memset(&g_cl, 0, sizeof(g_cl));

    /* NULL handle. */
    ASSERT_EQ(mds_cluster_node_register(NULL, 1, 1, "h", 1, 1),
              MDS_ERR_INVAL);
    ASSERT_EQ(mds_cluster_node_heartbeat(NULL, 1, 1), MDS_ERR_INVAL);
    ASSERT_EQ(mds_cluster_node_deregister(NULL, 1, 1), MDS_ERR_INVAL);
    ASSERT_EQ(mds_cluster_node_list(NULL, cluster_node_cb_never, NULL),
              MDS_ERR_INVAL);
    ASSERT_EQ(mds_cluster_node_scan_stale(NULL, 1, cluster_stale_cb_never,
                                          NULL),
              MDS_ERR_INVAL);
    ASSERT_EQ(mds_cluster_partition_list(NULL, cluster_partition_cb_never,
                                         NULL),
              MDS_ERR_INVAL);
    ASSERT_EQ(mds_cluster_partition_put(NULL, 0, 1, 0, "/", false),
              MDS_ERR_INVAL);

    /* NULL callback / string on a live table. */
    ASSERT_EQ(mds_cluster_node_register(&cat, 1, 1, NULL, 1, 1),
              MDS_ERR_INVAL);
    ASSERT_EQ(mds_cluster_node_list(&cat, NULL, NULL), MDS_ERR_INVAL);
    ASSERT_EQ(mds_cluster_node_scan_stale(&cat, 1, NULL, NULL),
              MDS_ERR_INVAL);
    ASSERT_EQ(mds_cluster_partition_list(&cat, NULL, NULL), MDS_ERR_INVAL);
    ASSERT_EQ(mds_cluster_partition_put(&cat, 0, 1, 0, NULL, true),
              MDS_ERR_INVAL);

    /* Argument validation precedes the slot: nothing was forwarded. */
    ASSERT_EQ(g_cl.calls, 0);

    /* NULL callback on a NULL-slot table is still the caller's bug. */
    cat.cluster_ops = &null_cluster_ops;
    ASSERT_EQ(mds_cluster_node_list(&cat, NULL, NULL), MDS_ERR_INVAL);

    /* mds_cluster_supported() is a predicate, not a dispatcher. */
    ASSERT_EQ(mds_cluster_supported(NULL), false);
}

/* --- mds_cluster_supported(): five slots AND the multi-process cap ---- */

static void test_cluster_supported_predicate(void)
{
    struct mds_catalogue cat = make_test_cat();
    struct mds_cluster_ops ops;

    /* All slots, no capability: an in-process store for tests. */
    ops = fake_cluster_ops;
    cat.cluster_ops = &ops;
    cat.caps = MDS_CAT_CAP_SHARED_AUTHORITY;
    ASSERT_EQ(mds_cluster_supported(&cat), false);

    /* Capability without a table. */
    cat.caps |= MDS_CAT_CAP_MULTI_PROCESS;
    cat.cluster_ops = NULL;
    ASSERT_EQ(mds_cluster_supported(&cat), false);

    /* All slots plus the capability. */
    cat.cluster_ops = &ops;
    ASSERT_EQ(mds_cluster_supported(&cat), true);

    /* The two optional slots are not required. */
    ops.node_deregister = NULL;
    ops.node_scan_stale = NULL;
    ASSERT_EQ(mds_cluster_supported(&cat), true);

    /* Each required slot missing on its own -> false. */
    ops = fake_cluster_ops;
    ops.node_register = NULL;
    ASSERT_EQ(mds_cluster_supported(&cat), false);
    ops = fake_cluster_ops;
    ops.node_heartbeat = NULL;
    ASSERT_EQ(mds_cluster_supported(&cat), false);
    ops = fake_cluster_ops;
    ops.node_list = NULL;
    ASSERT_EQ(mds_cluster_supported(&cat), false);
    ops = fake_cluster_ops;
    ops.partition_list = NULL;
    ASSERT_EQ(mds_cluster_supported(&cat), false);
    ops = fake_cluster_ops;
    ops.partition_put = NULL;
    ASSERT_EQ(mds_cluster_supported(&cat), false);

    /* Partition states are the documented row encoding. */
    ASSERT_EQ(MDS_PARTITION_STATE_ACTIVE, 0);
    ASSERT_EQ(MDS_PARTITION_STATE_MIGRATING, 1);
    ASSERT_EQ(MDS_PARTITION_STATE_FROZEN, 2);
}

/* --- READDIR cookie guard: a reserved cookie never reaches the caller -- */

#ifndef NDEBUG

/** Cookie the fake producers deliver; set per test case. */
static uint64_t g_fake_cookie;
static int g_caller_cb_calls;

static int fake_caller_readdir_cb(const struct mds_cat_dirent *entry,
                                  void *arg)
{
    (void)entry; (void)arg;
    g_caller_cb_calls++;
    return 0;
}

static int fake_caller_readdir_plus_cb(const struct mds_cat_dirent *entry,
                                       const struct mds_inode *inode,
                                       bool inode_valid, void *arg)
{
    (void)entry; (void)inode; (void)inode_valid; (void)arg;
    g_caller_cb_calls++;
    return 0;
}

/* Producer delivering one entry whose cookie is g_fake_cookie. */
static void fake_fill_dirent(struct mds_cat_dirent *d)
{
    memset(d, 0, sizeof(*d));
    d->fileid = 77;
    d->cookie = g_fake_cookie;
    d->type = MDS_FTYPE_REG;
    memcpy(d->name, "x", 2);
}

static enum mds_status fake_ns_readdir(struct mds_catalogue *cat,
    uint64_t parent, const char *start_after, uint32_t max_entries,
    struct mds_cat_txn *txn, mds_readdir_cb cb, void *ctx)
{
    struct mds_cat_dirent d;

    (void)cat; (void)parent; (void)start_after; (void)max_entries;
    (void)txn;
    fake_fill_dirent(&d);
    (void)cb(&d, ctx);
    return MDS_OK;
}

static enum mds_status fake_ns_readdir_plus_from(struct mds_catalogue *cat,
    uint64_t parent, uint64_t start_after_cookie, uint32_t max_entries,
    struct mds_cat_txn *txn, mds_readdir_plus_cb cb, void *ctx)
{
    struct mds_cat_dirent d;

    (void)cat; (void)parent; (void)start_after_cookie; (void)max_entries;
    (void)txn;
    fake_fill_dirent(&d);
    (void)cb(&d, NULL, false, ctx);
    return MDS_OK;
}

static enum mds_status fake_ns_getattr_notfound(struct mds_catalogue *cat,
    uint64_t fileid, struct mds_inode *inode)
{
    (void)cat; (void)fileid; (void)inode;
    return MDS_ERR_NOTFOUND;
}

static void test_readdir_cookie_guard(void)
{
    struct mds_catalogue cat = make_test_cat();
    struct mds_authority_ops ops;
    uint64_t bad[3] = { 0, 1, 2 };
    unsigned i;

    memset(&ops, 0, sizeof(ops));
    ops.ns_readdir = fake_ns_readdir;
    ops.ns_readdir_plus_from = fake_ns_readdir_plus_from;
    ops.ns_getattr = fake_ns_getattr_notfound;
    cat.auth_ops = &ops;

    for (i = 0; i < 3; i++) {
        g_fake_cookie = bad[i];

        /* Plain readdir. */
        g_caller_cb_calls = 0;
        ASSERT_EQ(mds_cat_ns_readdir(&cat, 2, NULL, 0, NULL,
                                     fake_caller_readdir_cb, NULL),
                  MDS_ERR_INVAL);
        ASSERT_EQ(g_caller_cb_calls, 0);

        /* Cookie-resume fast path. */
        g_caller_cb_calls = 0;
        ASSERT_EQ(mds_cat_ns_readdir_plus_from_cookie(
                      &cat, 2, 0, 0, NULL,
                      fake_caller_readdir_plus_cb, NULL),
                  MDS_ERR_INVAL);
        ASSERT_EQ(g_caller_cb_calls, 0);

        /* readdir_plus fallback (no fused slot): the entry comes from
         * ns_readdir and is guarded before the per-entry getattr. */
        g_caller_cb_calls = 0;
        ASSERT_EQ(mds_cat_ns_readdir_plus(&cat, 2, NULL, 0, NULL,
                                          fake_caller_readdir_plus_cb,
                                          NULL),
                  MDS_ERR_INVAL);
        ASSERT_EQ(g_caller_cb_calls, 0);
    }

    /* The smallest legal cookie passes straight through on all three. */
    g_fake_cookie = 3;
    g_caller_cb_calls = 0;
    ASSERT_EQ(mds_cat_ns_readdir(&cat, 2, NULL, 0, NULL,
                                 fake_caller_readdir_cb, NULL), MDS_OK);
    ASSERT_EQ(mds_cat_ns_readdir_plus_from_cookie(
                  &cat, 2, 0, 0, NULL, fake_caller_readdir_plus_cb, NULL),
              MDS_OK);
    ASSERT_EQ(mds_cat_ns_readdir_plus(&cat, 2, NULL, 0, NULL,
                                      fake_caller_readdir_plus_cb, NULL),
              MDS_OK);
    ASSERT_EQ(g_caller_cb_calls, 3);

    /* A NULL callback is the caller's bug, not the producer's. */
    ASSERT_EQ(mds_cat_ns_readdir(&cat, 2, NULL, 0, NULL, NULL, NULL),
              MDS_ERR_INVAL);
}

#endif /* !NDEBUG */

/* ----------------------------------------------------------------------- */

int main(void)
{
    fprintf(stdout, "test_coord_shared_state\n");

    /* Open/share state */
    RUN_TEST(test_open_put_nosupport);
    RUN_TEST(test_open_get_nosupport);
    RUN_TEST(test_open_del_nosupport);
    RUN_TEST(test_open_scan_file_nosupport);
    RUN_TEST(test_open_scan_client_nosupport);

    /* Byte-range locks */
    RUN_TEST(test_lock_put_nosupport);
    RUN_TEST(test_lock_del_nosupport);
    RUN_TEST(test_lock_test_nosupport);
    RUN_TEST(test_lock_scan_file_nosupport);
    RUN_TEST(test_lock_reap_client_nosupport);

    /* Delegations */
    RUN_TEST(test_deleg_put_nosupport);
    RUN_TEST(test_deleg_get_nosupport);
    RUN_TEST(test_deleg_del_nosupport);

    /* Client identity */
    RUN_TEST(test_client_put_nosupport);
    RUN_TEST(test_client_get_nosupport);
    RUN_TEST(test_client_del_nosupport);

    /* Sessions */
    RUN_TEST(test_session_put_nosupport);
    RUN_TEST(test_session_get_nosupport);
    RUN_TEST(test_session_del_nosupport);

    /* DRC slots */
    RUN_TEST(test_slot_put_nosupport);
    RUN_TEST(test_slot_get_nosupport);

    /* NULL safety */
    RUN_TEST(test_null_cat_returns_nosupport);

    /* Optional lifecycle / fused slots */
    RUN_TEST(test_bootstrap_nosupport_when_slot_null);
    RUN_TEST(test_backend_handle_null_when_slot_null);
    RUN_TEST(test_shared_authority_follows_caps);
    RUN_TEST(test_shared_state_supported_requires_all_three);
    RUN_TEST(test_layoutget_fused_nosupport_when_slot_null);
    RUN_TEST(test_ns_create_with_layout_nosupport_when_slot_null);
    RUN_TEST(test_layoutget_fused_forwards_arguments);
    RUN_TEST(test_ns_create_with_layout_forwards_arguments);
    RUN_TEST(test_lifecycle_slots_dispatch);

    /* Backend identity + cluster services vtable */
    RUN_TEST(test_backend_type_null_is_none);
    RUN_TEST(test_cluster_nosupport_when_slots_null);
    RUN_TEST(test_cluster_slots_forward_arguments);
    RUN_TEST(test_cluster_inval_on_null_arguments);
    RUN_TEST(test_cluster_supported_predicate);
#ifndef NDEBUG
    RUN_TEST(test_readdir_cookie_guard);
#endif

    fprintf(stdout, "\n  %d/%d tests passed\n",
        tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
