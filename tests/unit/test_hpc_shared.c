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

    /* Phase C / Steps 4 + 5 — wide create (community subset). */
    test_create_wide_invalid_args();

    /* Phase 3 of the QA plan — PENDING flag lifecycle. */
    test_pending_flag_lifecycle();

    fprintf(stdout, "\n  %d passed, %d failed\n", passed, failed);
    return failed > 0 ? 1 : 0;
}
