/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_hpc_shared.c -- pure-function unit tests for the Phase G HPC
 * striping-hint helpers (decoder + geometry picker).  No catalogue,
 * no compound, no daemon -- just the math.
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

static void create_legacy_pending_file(
    struct mds_catalogue *catalogue,
    const char *name,
    struct mds_inode *out_inode);
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
 * full sequence (alloc fileid -> inode + dirent -> wide batch -> stripe
 * map persistence) without a live DS mount.
 * ----------------------------------------------------------------------- */

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
static void test_pending_recovery_scan(void)
{
    struct hpc_pending_recovery_stats stats;
    struct mds_catalogue *catalogue;
    struct mds_ds_map_entry entry;
    struct mds_inode complete;
    struct mds_inode incomplete;

    fprintf(stdout, "  pending_recovery_scan:            ");
    catalogue = open_test_catalogue();
    assert(catalogue != NULL);
    create_legacy_pending_file(catalogue, "scan-complete", &complete);
    create_legacy_pending_file(catalogue, "scan-incomplete", &incomplete);
    memset(&entry, 0, sizeof(entry));
    entry.ds_id = 91;
    entry.nfs_fh_len = 1;
    entry.nfs_fh[0] = 0x91;
    ASSERT_EQ(mds_cat_stripe_map_put(
                  catalogue, NULL, complete.fileid, 1, 65536, 1, &entry),
              MDS_OK);

    ASSERT_EQ(hpc_shared_recover_pending_scan(catalogue, &stats), MDS_OK);
    ASSERT_EQ(stats.promoted, 1u);
    ASSERT_EQ(stats.reaped, 1u);
    ASSERT_EQ(mds_cat_ns_getattr(catalogue, complete.fileid, &complete),
              MDS_OK);
    ASSERT_TRUE((complete.flags & MDS_IFLAG_HPC_CREATE_PENDING) == 0);
    ASSERT_EQ(mds_cat_ns_getattr(catalogue, incomplete.fileid, &incomplete),
              MDS_ERR_NOTFOUND);
    mds_catalogue_close(catalogue);
    fprintf(stdout, "PASS\n");
    passed++;
}
static void test_atomic_wide_create(void)
{
    struct mds_catalogue *catalogue;
    struct mds_ds_map_entry stripe_entries[2];
    struct mds_inode child;
    struct mds_inode collision;
    struct mds_inode lookup_child;
    struct mds_inode parent_before;
    struct mds_inode parent_after;
    struct mds_ds_map_entry *read_entries = NULL;
    uint32_t stripe_count = 0;
    uint32_t stripe_unit = 0;
    uint32_t mirror_count = 0;
    uint64_t fileid = 0;
    uint64_t collision_fileid = 0;
    enum mds_status status;

    fprintf(stdout, "  atomic_wide_create:                ");
    catalogue = open_test_catalogue();
    assert(catalogue != NULL);
    memset(stripe_entries, 0, sizeof(stripe_entries));
    stripe_entries[0].ds_id = 11;
    stripe_entries[0].nfs_fh_len = 2;
    stripe_entries[0].nfs_fh[0] = 0xa1;
    stripe_entries[0].nfs_fh[1] = 0xa2;
    stripe_entries[1].ds_id = 12;
    stripe_entries[1].nfs_fh_len = 2;
    stripe_entries[1].nfs_fh[0] = 0xb1;
    stripe_entries[1].nfs_fh[1] = 0xb2;

    ASSERT_EQ(mds_cat_ns_getattr(catalogue, MDS_FILEID_ROOT, &parent_before),
              MDS_OK);
    ASSERT_EQ(mds_cat_alloc_fileid(catalogue, NULL, &fileid), MDS_OK);
    memset(&child, 0, sizeof(child));
    child.fileid = fileid;
    child.parent_fileid = MDS_FILEID_ROOT;
    child.type = MDS_FTYPE_REG;
    child.mode = 0644;
    child.nlink = 1;
    child.change = 1;
    child.generation = 1;
    child.flags = MDS_IFLAG_HPC_SHARED;

    status = mds_cat_ns_create_wide(
        catalogue, MDS_FILEID_ROOT, "atomic-wide", &child, 2, 131072, 1,
        stripe_entries);
    ASSERT_EQ(status, MDS_OK);
    ASSERT_EQ(mds_cat_ns_lookup(catalogue, MDS_FILEID_ROOT, "atomic-wide",
                                &lookup_child),
              MDS_OK);
    ASSERT_EQ(lookup_child.fileid, child.fileid);
    ASSERT_TRUE((lookup_child.flags & MDS_IFLAG_HPC_CREATE_PENDING) == 0);
    ASSERT_EQ(mds_cat_stripe_map_get(catalogue, child.fileid, &stripe_count,
                                     &stripe_unit, &mirror_count,
                                     &read_entries),
              MDS_OK);
    ASSERT_EQ(stripe_count, 2u);
    ASSERT_EQ(stripe_unit, 131072u);
    ASSERT_EQ(mirror_count, 1u);
    ASSERT_TRUE(read_entries != NULL);
    ASSERT_EQ(read_entries[0].ds_id, stripe_entries[0].ds_id);
    ASSERT_EQ(read_entries[1].ds_id, stripe_entries[1].ds_id);
    free(read_entries);
    read_entries = NULL;

    ASSERT_EQ(mds_cat_ns_getattr(catalogue, MDS_FILEID_ROOT, &parent_after),
              MDS_OK);
    ASSERT_EQ(parent_after.change, parent_before.change + 1);
    ASSERT_EQ(mds_cat_alloc_fileid(catalogue, NULL, &collision_fileid),
              MDS_OK);
    collision = child;
    collision.fileid = collision_fileid;

    status = mds_cat_ns_create_wide(
        catalogue, MDS_FILEID_ROOT, "atomic-wide", &collision, 2, 131072, 1,
        stripe_entries);
    ASSERT_EQ(status, MDS_ERR_EXISTS);
    ASSERT_EQ(mds_cat_ns_getattr(catalogue, collision_fileid, &lookup_child),
              MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_stripe_map_get(catalogue, collision_fileid,
                                     &stripe_count, &stripe_unit,
                                     &mirror_count, &read_entries),
              MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_ns_getattr(catalogue, MDS_FILEID_ROOT, &parent_before),
              MDS_OK);
    ASSERT_EQ(parent_before.change, parent_after.change);

    mds_catalogue_close(catalogue);
    fprintf(stdout, "PASS\n");
    passed++;
}

static void create_legacy_pending_file(
    struct mds_catalogue *catalogue,
    const char *name,
    struct mds_inode *out_inode)
{
    struct mds_inode pending;
    struct mds_cat_txn *transaction = NULL;
    struct timespec now;
    uint64_t fileid = 0;

    ASSERT_EQ(mds_cat_alloc_fileid(catalogue, NULL, &fileid), MDS_OK);
    ASSERT_TRUE(fileid != 0);
    clock_gettime(CLOCK_REALTIME, &now);
    memset(&pending, 0, sizeof(pending));
    pending.fileid = fileid;
    pending.parent_fileid = MDS_FILEID_ROOT;
    pending.type = MDS_FTYPE_REG;
    pending.mode = 0644;
    pending.nlink = 1;
    pending.atime = now;
    pending.mtime = now;
    pending.ctime = now;
    pending.change = 1;
    pending.generation = 1;
    pending.flags = MDS_IFLAG_HPC_SHARED | MDS_IFLAG_HPC_CREATE_PENDING;

    ASSERT_EQ(mds_cat_txn_begin(
                  catalogue, MDS_CAT_TXN_WRITE, &transaction),
              MDS_OK);
    ASSERT_EQ(mds_cat_inode_put(catalogue, transaction, &pending), MDS_OK);
    ASSERT_EQ(mds_cat_dirent_put(
                  catalogue, transaction, MDS_FILEID_ROOT, name,
                  pending.fileid, (uint8_t)pending.type),
              MDS_OK);
    ASSERT_EQ(mds_cat_txn_commit(transaction), MDS_OK);
    *out_inode = pending;
}

static void test_pending_recovery_complete_map(void)
{
    struct compound_data compound;
    struct mds_catalogue *catalogue;
    struct mds_ds_map_entry entries[2];
    struct mds_ds_map_entry *read_entries = NULL;
    struct mds_inode pending;
    struct mds_inode recovered;
    uint32_t stripe_count = 0;
    uint32_t stripe_unit = 0;
    uint32_t mirror_count = 0;

    fprintf(stdout, "  pending_recovery_complete_map:    ");
    catalogue = open_test_catalogue();
    assert(catalogue != NULL);
    create_legacy_pending_file(catalogue, "legacy-complete", &pending);
    memset(entries, 0, sizeof(entries));
    entries[0].ds_id = 71;
    entries[0].nfs_fh_len = 1;
    entries[0].nfs_fh[0] = 0x71;
    entries[1].ds_id = 72;
    entries[1].nfs_fh_len = 1;
    entries[1].nfs_fh[0] = 0x72;
    ASSERT_EQ(mds_cat_stripe_map_put(
                  catalogue, NULL, pending.fileid, 2, 65536, 1, entries),
              MDS_OK);
    memset(&compound, 0, sizeof(compound));
    compound.cat = catalogue;

    ASSERT_EQ(hpc_shared_recover_pending(&compound, &pending), MDS_OK);
    ASSERT_TRUE((pending.flags & MDS_IFLAG_HPC_CREATE_PENDING) == 0);
    ASSERT_EQ(mds_cat_ns_lookup(
                  catalogue, MDS_FILEID_ROOT, "legacy-complete", &recovered),
              MDS_OK);
    ASSERT_TRUE((recovered.flags & MDS_IFLAG_HPC_CREATE_PENDING) == 0);
    ASSERT_EQ(mds_cat_stripe_map_get(
                  catalogue, pending.fileid, &stripe_count, &stripe_unit,
                  &mirror_count, &read_entries),
              MDS_OK);
    ASSERT_EQ(stripe_count, 2u);
    ASSERT_EQ(stripe_unit, 65536u);
    ASSERT_EQ(mirror_count, 1u);
    free(read_entries);
    mds_catalogue_close(catalogue);
    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_pending_recovery_incomplete_map(void)
{
    struct compound_data compound;
    struct mds_catalogue *catalogue;
    struct mds_ds_map_entry entry;
    struct mds_inode pending;
    struct mds_inode recovered;
    uint32_t gc_count = 0;

    fprintf(stdout, "  pending_recovery_incomplete_map:  ");
    catalogue = open_test_catalogue();
    assert(catalogue != NULL);
    create_legacy_pending_file(catalogue, "legacy-incomplete", &pending);
    memset(&entry, 0, sizeof(entry));
    entry.ds_id = 81;
    entry.nfs_fh_len = 1;
    entry.nfs_fh[0] = 0x81;
    /* A zero stripe unit is incomplete metadata but retains a DS handle
     * that recovery must queue before reaping the legacy namespace row. */
    ASSERT_EQ(mds_cat_stripe_map_put(
                  catalogue, NULL, pending.fileid, 1, 0, 1, &entry),
              MDS_OK);
    memset(&compound, 0, sizeof(compound));
    compound.cat = catalogue;

    ASSERT_EQ(hpc_shared_recover_pending(&compound, &pending),
              MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_ns_lookup(
                  catalogue, MDS_FILEID_ROOT, "legacy-incomplete",
                  &recovered),
              MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_ns_getattr(catalogue, pending.fileid, &recovered),
              MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_gc_count(catalogue, &gc_count), MDS_OK);
    ASSERT_TRUE(gc_count > 0);
    mds_catalogue_close(catalogue);
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

    /* Phase C / Steps 4 + 5 -- wide create (community subset). */
    test_create_wide_invalid_args();
    test_atomic_wide_create();

    /* Recovery tests fabricate freshly-written legacy rows; disable
     * the rolling-upgrade reap grace so incomplete rows are eligible
     * immediately instead of after the production age window. */
    hpc_shared_test_set_pending_reap_grace(0);
    test_pending_recovery_complete_map();
    test_pending_recovery_incomplete_map();
    test_pending_recovery_scan();

    fprintf(stdout, "\n  %d passed, %d failed\n", passed, failed);
    return failed > 0 ? 1 : 0;
}
