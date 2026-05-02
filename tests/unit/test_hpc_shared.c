/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_hpc_shared.c — pure-function unit tests for the Phase G HPC
 * striping-hint helpers (decoder + geometry picker).  No catalogue,
 * no compound, no daemon — just the math.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <stdint.h>
#include <assert.h>
#include <time.h>

#include "pnfs_mds.h"      /* MDS_MAX_STRIPES, mds_status */
#include "hpc_shared.h"
#include "mds_catalogue.h"
#include "ds_prealloc.h"
#include "test_helpers.h"

static int passed;
static int failed;

#define ASSERT_TRUE(cond) do {                                          \
    if (!(cond)) {                                                      \
        fprintf(stderr, "  FAIL %s:%d: %s\n",                           \
                __FILE__, __LINE__, #cond);                             \
        failed++;                                                       \
        return;                                                         \
    }                                                                   \
} while (0)

#define ASSERT_EQ(a, b) ASSERT_TRUE((a) == (b))

/* Encode a pnfs_hpc_hint into a 16-byte big-endian buffer. */
static void encode_be(uint8_t out[HPC_HINT_BODY_SIZE],
                      uint64_t size, uint32_t clients, uint32_t flags)
{
    out[0]  = (uint8_t)(size >> 56);
    out[1]  = (uint8_t)(size >> 48);
    out[2]  = (uint8_t)(size >> 40);
    out[3]  = (uint8_t)(size >> 32);
    out[4]  = (uint8_t)(size >> 24);
    out[5]  = (uint8_t)(size >> 16);
    out[6]  = (uint8_t)(size >>  8);
    out[7]  = (uint8_t)(size);
    out[8]  = (uint8_t)(clients >> 24);
    out[9]  = (uint8_t)(clients >> 16);
    out[10] = (uint8_t)(clients >>  8);
    out[11] = (uint8_t)(clients);
    out[12] = (uint8_t)(flags >> 24);
    out[13] = (uint8_t)(flags >> 16);
    out[14] = (uint8_t)(flags >>  8);
    out[15] = (uint8_t)(flags);
}

/* -----------------------------------------------------------------------
 * Decoder tests
 * ----------------------------------------------------------------------- */

static void test_decode_round_trip(void)
{
    fprintf(stdout, "  decode_round_trip:                ");

    uint8_t wire[HPC_HINT_BODY_SIZE];
    encode_be(wire, 0x1122334455667788ULL, 0xAABBCCDDU, 0xDEADBEEFU);

    struct pnfs_hpc_hint h;
    enum mds_status st = hpc_hint_decode_xdr_body(wire, sizeof(wire), &h);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(h.expected_file_size,    0x1122334455667788ULL);
    ASSERT_EQ(h.expected_client_count, 0xAABBCCDDU);
    ASSERT_EQ(h.flags,                 0xDEADBEEFU);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_decode_zero_body(void)
{
    fprintf(stdout, "  decode_zero_body:                 ");

    uint8_t wire[HPC_HINT_BODY_SIZE] = {0};
    struct pnfs_hpc_hint h;
    h.expected_file_size = 0xFFFFu;  /* poison */
    enum mds_status st = hpc_hint_decode_xdr_body(wire, sizeof(wire), &h);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(h.expected_file_size,    0u);
    ASSERT_EQ(h.expected_client_count, 0u);
    ASSERT_EQ(h.flags,                 0u);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_decode_invalid_args(void)
{
    fprintf(stdout, "  decode_invalid_args:              ");

    uint8_t wire[HPC_HINT_BODY_SIZE] = {0};
    struct pnfs_hpc_hint h;

    ASSERT_EQ(hpc_hint_decode_xdr_body(NULL, sizeof(wire), &h),
              MDS_ERR_INVAL);
    ASSERT_EQ(hpc_hint_decode_xdr_body(wire, sizeof(wire), NULL),
              MDS_ERR_INVAL);
    /* Wrong length, both shorter and longer. */
    ASSERT_EQ(hpc_hint_decode_xdr_body(wire, sizeof(wire) - 1, &h),
              MDS_ERR_INVAL);
    ASSERT_EQ(hpc_hint_decode_xdr_body(wire, sizeof(wire) + 1, &h),
              MDS_ERR_INVAL);
    ASSERT_EQ(hpc_hint_decode_xdr_body(wire, 0, &h),
              MDS_ERR_INVAL);

    fprintf(stdout, "PASS\n");
    passed++;
}

/* -----------------------------------------------------------------------
 * Geometry-picker tests
 * ----------------------------------------------------------------------- */

static void test_geom_tier1_by_size(void)
{
    fprintf(stdout, "  geom_tier1_by_size:               ");

    struct pnfs_hpc_hint h = {
        .expected_file_size    = (1ULL << 40),  /* 1 TiB */
        .expected_client_count = 1,
        .flags                 = 0,
    };
    uint32_t sc = 1;
    uint32_t su = 65536;
    bool overridden = hpc_hint_select_geometry(&h, 200, &sc, &su);
    ASSERT_TRUE(overridden);
    ASSERT_EQ(sc, 200u);
    ASSERT_EQ(su, 1u << 20);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_geom_tier1_by_clients(void)
{
    fprintf(stdout, "  geom_tier1_by_clients:            ");

    struct pnfs_hpc_hint h = {
        .expected_file_size    = 1024,
        .expected_client_count = 1024,
        .flags                 = 0,
    };
    uint32_t sc = 1;
    uint32_t su = 65536;
    bool overridden = hpc_hint_select_geometry(&h, 32, &sc, &su);
    ASSERT_TRUE(overridden);
    ASSERT_EQ(sc, 32u);
    ASSERT_EQ(su, 1u << 20);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_geom_tier1_caps_at_max_stripes(void)
{
    fprintf(stdout, "  geom_tier1_caps_at_max_stripes:   ");

    struct pnfs_hpc_hint h = {
        .expected_file_size    = (4ULL << 40),  /* 4 TiB */
        .expected_client_count = 0,
        .flags                 = 0,
    };
    uint32_t sc = 1;
    uint32_t su = 65536;
    /* Pretend we have more DSes than MDS_MAX_STRIPES. */
    bool overridden = hpc_hint_select_geometry(
        &h, MDS_MAX_STRIPES + 100, &sc, &su);
    ASSERT_TRUE(overridden);
    ASSERT_EQ(sc, (uint32_t)MDS_MAX_STRIPES);
    ASSERT_EQ(su, 1u << 20);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_geom_tier2(void)
{
    fprintf(stdout, "  geom_tier2:                       ");

    struct pnfs_hpc_hint h = {
        .expected_file_size    = (64ULL << 30),  /* 64 GiB */
        .expected_client_count = 4,
        .flags                 = 0,
    };
    uint32_t sc = 1;
    uint32_t su = 65536;
    bool overridden = hpc_hint_select_geometry(&h, 100, &sc, &su);
    ASSERT_TRUE(overridden);
    ASSERT_EQ(sc, 64u);
    ASSERT_EQ(su, 512u << 10);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_geom_tier2_clamps_to_online(void)
{
    fprintf(stdout, "  geom_tier2_clamps_to_online:      ");

    struct pnfs_hpc_hint h = {
        .expected_file_size    = (100ULL << 30), /* 100 GiB */
        .expected_client_count = 8,
        .flags                 = 0,
    };
    uint32_t sc = 1;
    uint32_t su = 65536;
    /* Only 16 DSes online, but tier 2 wants 64. */
    bool overridden = hpc_hint_select_geometry(&h, 16, &sc, &su);
    ASSERT_TRUE(overridden);
    ASSERT_EQ(sc, 16u);
    ASSERT_EQ(su, 512u << 10);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_geom_tier0_no_override(void)
{
    fprintf(stdout, "  geom_tier0_no_override:           ");

    struct pnfs_hpc_hint h = {
        .expected_file_size    = (1ULL << 30),   /* 1 GiB */
        .expected_client_count = 4,
        .flags                 = 0,
    };
    uint32_t sc = 7;       /* sentinel default */
    uint32_t su = 65536;   /* sentinel default */
    bool overridden = hpc_hint_select_geometry(&h, 32, &sc, &su);
    ASSERT_TRUE(!overridden);
    ASSERT_EQ(sc, 7u);
    ASSERT_EQ(su, 65536u);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_geom_zero_dses(void)
{
    fprintf(stdout, "  geom_zero_dses:                   ");

    struct pnfs_hpc_hint h = {
        .expected_file_size    = (1ULL << 40),
        .expected_client_count = 0,
        .flags                 = 0,
    };
    uint32_t sc = 7;
    uint32_t su = 65536;
    bool overridden = hpc_hint_select_geometry(&h, 0, &sc, &su);
    ASSERT_TRUE(!overridden);
    ASSERT_EQ(sc, 7u);
    ASSERT_EQ(su, 65536u);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_geom_null_args(void)
{
    fprintf(stdout, "  geom_null_args:                   ");

    struct pnfs_hpc_hint h = {0};
    uint32_t sc = 1;
    uint32_t su = 65536;
    ASSERT_TRUE(!hpc_hint_select_geometry(NULL, 4, &sc, &su));
    ASSERT_TRUE(!hpc_hint_select_geometry(&h, 4, NULL, &su));
    ASSERT_TRUE(!hpc_hint_select_geometry(&h, 4, &sc, NULL));

    fprintf(stdout, "PASS\n");
    passed++;
}

/* -----------------------------------------------------------------------
 * hpc_shared_create_wide_layout (Phase C / Steps 4 + 5)
 *
 * These tests run against the in-memory catalogue (no proxy attached)
 * and use the synthetic-FH knob on ds_prealloc to exercise the
 * full sequence (alloc fileid → inode + dirent → wide batch → stripe
 * map persistence) without a live DS mount.
 * ----------------------------------------------------------------------- */

static void seed_ds_for_create(struct mds_catalogue *db, uint32_t ds_id)
{
    struct mds_ds_info info;
    struct mds_cat_txn *txn = NULL;

    memset(&info, 0, sizeof(info));
    info.ds_id = ds_id;
    info.state = DS_ONLINE;
    info.port = 2049;
    info.tcp_port = 2049;
    info.transport = DS_TRANSPORT_TCP;
    info.mode = DS_MODE_GENERIC;
    snprintf(info.addr, sizeof(info.addr), "10.0.0.%u:/data", ds_id);
    snprintf(info.host, sizeof(info.host), "10.0.0.%u", ds_id);
    snprintf(info.export_path, sizeof(info.export_path), "/data");

    if (mds_cat_txn_begin(db, MDS_CAT_TXN_WRITE, &txn) != MDS_OK) {
        return;
    }
    if (mds_cat_ds_put(db, txn, &info) != MDS_OK) {
        mds_cat_txn_abort(txn);
        return;
    }
    mds_cat_txn_commit(txn);
}

static void test_create_wide_invalid_args(void)
{
    fprintf(stdout, "  create_wide_invalid_args:         ");

    struct mds_inode out;
    /* All-NULL: invalid. */
    ASSERT_EQ(hpc_shared_create_wide_layout(NULL, NULL, 0, NULL,
                                            0, 0, 0, 0, 0, 0,
                                            0, 0, 0, false, &out),
              MDS_ERR_INVAL);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_create_wide_happy_4_stripes(void)
{
    fprintf(stdout, "  create_wide_happy_4_stripes:      ");

    struct mds_catalogue *db = open_test_catalogue();
    assert(db != NULL);
    seed_ds_for_create(db, 0);
    seed_ds_for_create(db, 1);
    seed_ds_for_create(db, 2);
    seed_ds_for_create(db, 3);

    struct ds_prealloc_ctx *prealloc = NULL;
    int rc = ds_prealloc_init(db, NULL, 16, &prealloc);
    ASSERT_EQ(rc, 0);
    ds_prealloc_test_enable_synthetic_fh(prealloc, true);

    struct mds_inode child;
    enum mds_status st = hpc_shared_create_wide_layout(
        db, prealloc,
        MDS_FILEID_ROOT,                /* parent: root */
        "hpc_wide_test",                /* child name */
        0644,
        0, 0,                           /* uid, gid */
        4, 1, 65536,                    /* stripe=4, mirror=1, su=64K */
        DS_TRANSPORT_TCP, 0, 0,         /* required, preferred, caps */
        true,                           /* strict_unique_ds */
        &child);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_TRUE(child.fileid != 0);
    ASSERT_EQ(child.type, MDS_FTYPE_REG);
    ASSERT_TRUE((child.flags & MDS_IFLAG_HPC_SHARED) != 0);
    /* Phase 3 of the QA plan: PENDING bit must be cleared on the
     * returned inode after step 5 (clear-flag setattr) succeeds. */
    ASSERT_TRUE((child.flags & MDS_IFLAG_HPC_CREATE_PENDING) == 0);
    ASSERT_EQ(child.stripe_count, 4u);
    ASSERT_EQ(child.mirror_count, 1u);
    ASSERT_EQ(child.stripe_unit, 65536u);

    /* Verify the catalogue persisted the inode. */
    struct mds_inode persisted;
    ASSERT_EQ(mds_cat_ns_getattr(db, child.fileid, &persisted), MDS_OK);
    ASSERT_TRUE((persisted.flags & MDS_IFLAG_HPC_SHARED) != 0);
    /* Phase 3 of the QA plan: PENDING bit must also be cleared in the
     * persisted inode — if it lingered the read path would hide the
     * file from clients. */
    ASSERT_TRUE((persisted.flags & MDS_IFLAG_HPC_CREATE_PENDING) == 0);
    ASSERT_EQ(persisted.fileid, child.fileid);

    /* Verify the dirent points to the inode. */
    struct mds_inode dirent_child;
    ASSERT_EQ(mds_cat_ns_lookup(db, MDS_FILEID_ROOT, "hpc_wide_test",
                                &dirent_child),
              MDS_OK);
    ASSERT_EQ(dirent_child.fileid, child.fileid);

    /* Verify the wide stripe map is the one ds_prealloc_batch built. */
    uint32_t got_sc = 0, got_su = 0, got_mc = 0;
    struct mds_ds_map_entry *got_entries = NULL;
    ASSERT_EQ(mds_cat_stripe_map_get(db, child.fileid,
                                     &got_sc, &got_su, &got_mc,
                                     &got_entries),
              MDS_OK);
    ASSERT_EQ(got_sc, 4u);
    ASSERT_EQ(got_su, 65536u);
    ASSERT_EQ(got_mc, 1u);
    ASSERT_TRUE(got_entries != NULL);
    /* Each slot must have an FH (synthetic 16 B in this test). */
    for (uint32_t i = 0; i < 4; i++) {
        ASSERT_EQ(got_entries[i].nfs_fh_len, 16u);
    }
    /* All 4 stripes must land on distinct DSes (strict_unique_ds). */
    bool seen[64] = {false};
    for (uint32_t i = 0; i < 4; i++) {
        uint32_t did = got_entries[i].ds_id;
        ASSERT_TRUE(did < 64);
        ASSERT_TRUE(!seen[did]);
        seen[did] = true;
    }
    free(got_entries);

    ds_prealloc_destroy(prealloc);
    mds_catalogue_close(db);

    fprintf(stdout, "PASS\n");
    passed++;
}

/* QA Phase 5: hpc_shared_create_wide_layout returns NOSUPPORT (not
 * INVAL, not silently succeeds) when mirror_count > 1.  Mirroring on
 * the HPC wide pre-warm path is explicitly out of scope. */
static void test_create_wide_mirror_count_unsupp(void)
{
    fprintf(stdout, "  create_wide_mirror_count_unsupp:  ");

    struct mds_catalogue *db = open_test_catalogue();
    assert(db != NULL);
    seed_ds_for_create(db, 0);
    seed_ds_for_create(db, 1);

    struct ds_prealloc_ctx *prealloc = NULL;
    int rc = ds_prealloc_init(db, NULL, 16, &prealloc);
    ASSERT_EQ(rc, 0);
    ds_prealloc_test_enable_synthetic_fh(prealloc, true);

    struct mds_inode child;
    memset(&child, 0xAB, sizeof(child));  /* poison */
    enum mds_status st = hpc_shared_create_wide_layout(
        db, prealloc,
        MDS_FILEID_ROOT, "mirror_unsupp",
        0644, 0, 0,
        2,                              /* stripe_count */
        2,                              /* mirror_count > 1 */
        65536,
        DS_TRANSPORT_TCP, 0, 0,
        true,                           /* strict_unique_ds */
        &child);
    ASSERT_EQ(st, MDS_ERR_NOSUPPORT);

    /* No dirent / inode persisted on the rejected path. */
    struct mds_inode look;
    enum mds_status look_st = mds_cat_ns_lookup(
        db, MDS_FILEID_ROOT, "mirror_unsupp", &look);
    ASSERT_EQ(look_st, MDS_ERR_NOTFOUND);

    ds_prealloc_destroy(prealloc);
    mds_catalogue_close(db);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_create_wide_strict_unique_underflow(void)
{
    fprintf(stdout, "  create_wide_strict_unique_underflow: ");

    /* Only 2 DSes online but caller requests 4 unique stripes:
     * the helper must roll back cleanly — no inode, no dirent. */
    struct mds_catalogue *db = open_test_catalogue();
    assert(db != NULL);
    seed_ds_for_create(db, 0);
    seed_ds_for_create(db, 1);

    struct ds_prealloc_ctx *prealloc = NULL;
    int rc = ds_prealloc_init(db, NULL, 16, &prealloc);
    ASSERT_EQ(rc, 0);
    ds_prealloc_test_enable_synthetic_fh(prealloc, true);

    struct mds_inode child;
    memset(&child, 0xCD, sizeof(child));  /* poison */
    enum mds_status st = hpc_shared_create_wide_layout(
        db, prealloc,
        MDS_FILEID_ROOT, "underflow_test",
        0644, 0, 0,
        4, 1, 0,
        DS_TRANSPORT_TCP, 0, 0,
        true,                           /* strict_unique_ds */
        &child);
    ASSERT_EQ(st, MDS_ERR_NOSPC);

    /* Catalogue must NOT carry the dirent we tried to create. */
    struct mds_inode dirent_child;
    enum mds_status look_st = mds_cat_ns_lookup(
        db, MDS_FILEID_ROOT, "underflow_test", &dirent_child);
    ASSERT_EQ(look_st, MDS_ERR_NOTFOUND);

    ds_prealloc_destroy(prealloc);
    mds_catalogue_close(db);

    fprintf(stdout, "PASS\n");
    passed++;
}

/* Phase 3 of the QA plan — simulate the crash-between-rows orphan.
 *
 * Construct an inode at the catalogue layer that mirrors the state
 * left behind by an MDS that crashed after committing the inode +
 * dirent rows but before the stripe_map row.  The catalogue layer
 * does not filter the PENDING bit — it is the NFS-facing read path
 * (compound_inode_get / compound_lookup_local_child) that does — so
 * we assert that:
 *   (a) mds_cat_ns_getattr surfaces the orphan with the flag set
 *       (cleanup paths need to see it);
 *   (b) clearing the flag via mds_cat_ns_setattr is observable on a
 *       follow-up read (the same primitive the helper uses on its
 *       success path).
 *
 * This complements the happy-path assertions in
 * test_create_wide_happy_4_stripes by pinning the lifecycle of the
 * flag without depending on compound_data plumbing.
 */
static void test_pending_flag_lifecycle(void)
{
    fprintf(stdout, "  pending_flag_lifecycle:           ");

    struct mds_catalogue *db = open_test_catalogue();
    assert(db != NULL);

    /* Allocate a fileid and persist a synthetic orphan inode with the
     * PENDING bit set, mirroring hpc_create_inode_and_dirent's pre-
     * stripe-map state.  No DSes / no stripe map — we only need the
     * inode row for this lifecycle check. */
    uint64_t fid = 0;
    ASSERT_EQ(mds_cat_alloc_fileid(db, NULL, &fid), MDS_OK);
    ASSERT_TRUE(fid != 0);

    struct timespec now;
    clock_gettime(CLOCK_REALTIME, &now);

    struct mds_inode pending;
    memset(&pending, 0, sizeof(pending));
    pending.fileid = fid;
    pending.type = MDS_FTYPE_REG;
    pending.mode = 0644;
    pending.nlink = 1;
    pending.atime = now;
    pending.mtime = now;
    pending.ctime = now;
    pending.change = 1;
    pending.generation = 1;
    pending.flags = MDS_IFLAG_HPC_SHARED | MDS_IFLAG_HPC_CREATE_PENDING;

    struct mds_cat_txn *txn = NULL;
    ASSERT_EQ(mds_cat_txn_begin(db, MDS_CAT_TXN_WRITE, &txn), MDS_OK);
    ASSERT_EQ(mds_cat_inode_put(db, txn, &pending), MDS_OK);
    ASSERT_EQ(mds_cat_txn_commit(txn), MDS_OK);

    /* (a) Catalogue layer surfaces the orphan with PENDING set. */
    struct mds_inode read_back;
    ASSERT_EQ(mds_cat_ns_getattr(db, fid, &read_back), MDS_OK);
    ASSERT_TRUE((read_back.flags & MDS_IFLAG_HPC_CREATE_PENDING) != 0);
    ASSERT_TRUE((read_back.flags & MDS_IFLAG_HPC_SHARED) != 0);

    /* (b) Clearing the flag via the same primitive the helper uses on
     *     its success path makes the file visible on subsequent reads. */
    struct mds_inode cleared = read_back;
    cleared.flags &= ~MDS_IFLAG_HPC_CREATE_PENDING;
    ASSERT_EQ(mds_cat_ns_setattr(db, NULL, fid, &cleared,
                                 MDS_ATTR_FLAGS),
              MDS_OK);

    struct mds_inode after;
    ASSERT_EQ(mds_cat_ns_getattr(db, fid, &after), MDS_OK);
    ASSERT_TRUE((after.flags & MDS_IFLAG_HPC_CREATE_PENDING) == 0);
    ASSERT_TRUE((after.flags & MDS_IFLAG_HPC_SHARED) != 0);

    mds_catalogue_close(db);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_geom_unknown_flags_ignored(void)
{
    fprintf(stdout, "  geom_unknown_flags_ignored:       ");

    /* Set every flag bit; the geometry picker must not be perturbed. */
    struct pnfs_hpc_hint h = {
        .expected_file_size    = 1024,
        .expected_client_count = 4,
        .flags                 = 0xFFFFFFFFU,
    };
    uint32_t sc = 7;
    uint32_t su = 65536;
    bool overridden = hpc_hint_select_geometry(&h, 32, &sc, &su);
    ASSERT_TRUE(!overridden);
    ASSERT_EQ(sc, 7u);
    ASSERT_EQ(su, 65536u);

    fprintf(stdout, "PASS\n");
    passed++;
}

int main(void)
{
    fprintf(stdout, "test_hpc_shared:\n");

    test_decode_round_trip();
    test_decode_zero_body();
    test_decode_invalid_args();
    test_geom_tier1_by_size();
    test_geom_tier1_by_clients();
    test_geom_tier1_caps_at_max_stripes();
    test_geom_tier2();
    test_geom_tier2_clamps_to_online();
    test_geom_tier0_no_override();
    test_geom_zero_dses();
    test_geom_null_args();
    test_geom_unknown_flags_ignored();

    /* Phase C / Steps 4 + 5 — wide create. */
    test_create_wide_invalid_args();
    test_create_wide_happy_4_stripes();
    test_create_wide_mirror_count_unsupp();
    test_create_wide_strict_unique_underflow();

    /* Phase 3 of the QA plan — PENDING flag lifecycle. */
    test_pending_flag_lifecycle();

    fprintf(stdout, "\n  %d passed, %d failed\n", passed, failed);
    return failed > 0 ? 1 : 0;
}
