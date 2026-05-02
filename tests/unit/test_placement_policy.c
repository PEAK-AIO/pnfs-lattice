/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_placement_policy.c - Phase 1 unit tests for placement_select_ex.
 *
 * Exercises RR / WRR / CAPACITY on a synthetic ds_list.  No catalogue,
 * no daemon, no network.  Distribution checks use large sample counts
 * so the statistical bounds are loose enough not to flake under a
 * normal RNG.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

#include "pnfs_mds.h"
#include "placement.h"

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

/* -----------------------------------------------------------------------
 * Helpers
 * ----------------------------------------------------------------------- */

static void mk_ds(struct mds_ds_info *ds, uint32_t id,
                  uint32_t state, uint64_t total, uint64_t used)
{
    memset(ds, 0, sizeof(*ds));
    ds->ds_id = id;
    ds->state = state;
    ds->total_bytes = total;
    ds->used_bytes = used;
    ds->mode = DS_MODE_GENERIC;
    ds->transport = DS_TRANSPORT_TCP;
}

/* -----------------------------------------------------------------------
 * Tests
 * ----------------------------------------------------------------------- */

static void test_rr_delegates_to_placement_select(void)
{
    fprintf(stdout, "  rr_delegates_to_placement_select: ");

    struct mds_ds_info ds[3];
    struct mds_ds_map_entry ent;

    mk_ds(&ds[0], 1, DS_ONLINE, 1000, 100);
    mk_ds(&ds[1], 2, DS_ONLINE, 1000, 100);
    mk_ds(&ds[2], 3, DS_ONLINE, 1000, 100);

    memset(&ent, 0, sizeof(ent));
    enum mds_status st =
        placement_select_ex(PLACEMENT_RR, ds, 3, 1, 1, 65536, &ent);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_TRUE(ent.ds_id == 1 || ent.ds_id == 2 || ent.ds_id == 3);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_capacity_picks_emptiest(void)
{
    fprintf(stdout, "  capacity_picks_emptiest:          ");

    struct mds_ds_info ds[3];
    struct mds_ds_map_entry ent;

    /* ds 1: 100 free.  ds 2: 900 free.  ds 3: 500 free. */
    mk_ds(&ds[0], 1, DS_ONLINE, 1000, 900);
    mk_ds(&ds[1], 2, DS_ONLINE, 1000, 100);
    mk_ds(&ds[2], 3, DS_ONLINE, 1000, 500);

    /* CAPACITY is deterministic: must always pick ds 2 (900 free). */
    for (int i = 0; i < 100; i++) {
        memset(&ent, 0, sizeof(ent));
        enum mds_status st =
            placement_select_ex(PLACEMENT_CAPACITY, ds, 3, 1, 1,
                                65536, &ent);
        ASSERT_EQ(st, MDS_OK);
        ASSERT_EQ(ent.ds_id, 2u);
    }

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_capacity_skips_offline(void)
{
    fprintf(stdout, "  capacity_skips_offline:           ");

    struct mds_ds_info ds[3];
    struct mds_ds_map_entry ent;

    /* Emptiest DS is offline; capacity must pick the emptiest ONLINE. */
    mk_ds(&ds[0], 1, DS_ONLINE, 1000, 700);    /* 300 free */
    mk_ds(&ds[1], 2, DS_OFFLINE, 1000, 0);     /* 1000 free, but OFFLINE */
    mk_ds(&ds[2], 3, DS_ONLINE, 1000, 200);    /* 800 free */

    memset(&ent, 0, sizeof(ent));
    enum mds_status st =
        placement_select_ex(PLACEMENT_CAPACITY, ds, 3, 1, 1,
                            65536, &ent);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(ent.ds_id, 3u);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_wrr_weight_bias(void)
{
    fprintf(stdout, "  wrr_weight_bias:                  ");

    struct mds_ds_info ds[2];
    struct mds_ds_map_entry ent;
    const int trials = 5000;
    int count_ds1 = 0;
    int count_ds2 = 0;

    /* 10:1 weight ratio so the expected distribution is extreme
     * enough to absorb any reasonable RNG noise. */
    mk_ds(&ds[0], 1, DS_ONLINE, 11000, 10000);  /* 1000 free */
    mk_ds(&ds[1], 2, DS_ONLINE, 10000, 0);      /* 10000 free */

    for (int i = 0; i < trials; i++) {
        memset(&ent, 0, sizeof(ent));
        enum mds_status st =
            placement_select_ex(PLACEMENT_WEIGHTED_RR, ds, 2, 1, 1,
                                65536, &ent);
        ASSERT_EQ(st, MDS_OK);
        if (ent.ds_id == 1) {
            count_ds1++;
        } else if (ent.ds_id == 2) {
            count_ds2++;
        }
    }

    /* Expected: ds2 wins ~10/11 of the time ~= 4545 of 5000.
     * Accept anything in [4200, 4900] (loose but easily passes). */
    ASSERT_TRUE(count_ds2 > 4200 && count_ds2 < 4900);
    ASSERT_TRUE(count_ds1 + count_ds2 == trials);

    fprintf(stdout, "PASS (ds1=%d ds2=%d of %d)\n",
            count_ds1, count_ds2, trials);
    passed++;
}

/**
 * Phase A: operator-configured ds->weight drives the WRR
 * distribution even when total_bytes/used_bytes are zero (the
 * common case with 3rd-party DSes like NetApp / Isilon that do not
 * report capacity to the MDS).
 */
static void test_wrr_explicit_weight_respected(void)
{
    fprintf(stdout, "  wrr_explicit_weight_respected:    ");

    struct mds_ds_info ds[2];
    struct mds_ds_map_entry ent;
    const int trials = 5000;
    int count_ds1 = 0;
    int count_ds2 = 0;

    /* Both DSes report zero capacity (3rd-party scenario).
     * Only the explicit weight differs: 10:1 ratio, ds2 should win
     * roughly 10/11 of the time. */
    mk_ds(&ds[0], 1, DS_ONLINE, 0, 0);
    ds[0].weight = 10;
    mk_ds(&ds[1], 2, DS_ONLINE, 0, 0);
    ds[1].weight = 100;

    for (int i = 0; i < trials; i++) {
        memset(&ent, 0, sizeof(ent));
        enum mds_status st =
            placement_select_ex(PLACEMENT_WEIGHTED_RR, ds, 2, 1, 1,
                                65536, &ent);
        ASSERT_EQ(st, MDS_OK);
        if (ent.ds_id == 1) {
            count_ds1++;
        } else if (ent.ds_id == 2) {
            count_ds2++;
        }
    }

    /* Expected: ds2 ~ 10/11 of trials ~ 4545 of 5000. */
    ASSERT_TRUE(count_ds2 > 4200 && count_ds2 < 4900);
    ASSERT_TRUE(count_ds1 + count_ds2 == trials);

    fprintf(stdout, "PASS (ds1=%d ds2=%d of %d)\n",
            count_ds1, count_ds2, trials);
    passed++;
}

/**
 * Explicit ds->weight takes precedence over free_bytes when both
 * are set.  Confirms the two signals do not compete; weight wins.
 */
static void test_wrr_explicit_weight_overrides_free(void)
{
    fprintf(stdout, "  wrr_weight_overrides_free:        ");

    struct mds_ds_info ds[2];
    struct mds_ds_map_entry ent;
    const int trials = 3000;
    int count_ds2 = 0;

    /* ds1 has more free space, but ds2 has higher admin weight.
     * With weight taking precedence, ds2 should win decisively. */
    mk_ds(&ds[0], 1, DS_ONLINE, 10000, 0);   /* 10000 free, weight=1  */
    ds[0].weight = 1;
    mk_ds(&ds[1], 2, DS_ONLINE, 1000,  0);   /* 1000 free,  weight=20 */
    ds[1].weight = 20;

    for (int i = 0; i < trials; i++) {
        memset(&ent, 0, sizeof(ent));
        enum mds_status st =
            placement_select_ex(PLACEMENT_WEIGHTED_RR, ds, 2, 1, 1,
                                65536, &ent);
        ASSERT_EQ(st, MDS_OK);
        if (ent.ds_id == 2) {
            count_ds2++;
        }
    }
    /* 20/21 ~ 95% of trials. */
    ASSERT_TRUE(count_ds2 > (trials * 9) / 10);
    fprintf(stdout, "PASS (ds2=%d of %d)\n", count_ds2, trials);
    passed++;
}

static void test_wrr_offline_excluded(void)
{
    fprintf(stdout, "  wrr_offline_excluded:             ");

    struct mds_ds_info ds[3];
    struct mds_ds_map_entry ent;

    mk_ds(&ds[0], 1, DS_ONLINE,  1000,   0);
    mk_ds(&ds[1], 2, DS_OFFLINE, 9999,   0); /* should never be picked */
    mk_ds(&ds[2], 3, DS_ONLINE,  1000, 500);

    for (int i = 0; i < 200; i++) {
        memset(&ent, 0, sizeof(ent));
        enum mds_status st =
            placement_select_ex(PLACEMENT_WEIGHTED_RR, ds, 3, 1, 1,
                                65536, &ent);
        ASSERT_EQ(st, MDS_OK);
        ASSERT_TRUE(ent.ds_id == 1 || ent.ds_id == 3);
    }

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_no_online_returns_nospc(void)
{
    fprintf(stdout, "  no_online_returns_nospc:          ");

    struct mds_ds_info ds[2];
    struct mds_ds_map_entry ent;

    mk_ds(&ds[0], 1, DS_OFFLINE, 1000, 0);
    mk_ds(&ds[1], 2, DS_OFFLINE, 1000, 0);

    memset(&ent, 0, sizeof(ent));
    enum mds_status st =
        placement_select_ex(PLACEMENT_WEIGHTED_RR, ds, 2, 1, 1,
                            65536, &ent);
    ASSERT_EQ(st, MDS_ERR_NOSPC);

    st = placement_select_ex(PLACEMENT_CAPACITY, ds, 2, 1, 1,
                             65536, &ent);
    ASSERT_EQ(st, MDS_ERR_NOSPC);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_multi_stripe_falls_back_to_rr(void)
{
    fprintf(stdout, "  multi_stripe_falls_back_to_rr:    ");

    struct mds_ds_info ds[4];
    struct mds_ds_map_entry ent[4];

    mk_ds(&ds[0], 1, DS_ONLINE, 1000, 0);
    mk_ds(&ds[1], 2, DS_ONLINE, 1000, 0);
    mk_ds(&ds[2], 3, DS_ONLINE, 1000, 0);
    mk_ds(&ds[3], 4, DS_ONLINE, 1000, 0);

    /* stripe=2, mirror=2: Phase 1 alt policies must defer to the
     * RR multi-DS path.  Confirms D5 fallback is wired. */
    memset(ent, 0, sizeof(ent));
    enum mds_status st =
        placement_select_ex(PLACEMENT_CAPACITY, ds, 4, 2, 2,
                            65536, ent);
    ASSERT_EQ(st, MDS_OK);

    /* Four distinct DS picks, no mirror of a stripe on same DS. */
    for (int s = 0; s < 2; s++) {
        uint32_t a = ent[s * 2 + 0].ds_id;
        uint32_t b = ent[s * 2 + 1].ds_id;
        ASSERT_TRUE(a != 0 && b != 0 && a != b);
    }

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_invalid_args(void)
{
    fprintf(stdout, "  invalid_args:                     ");

    struct mds_ds_map_entry ent;
    memset(&ent, 0, sizeof(ent));

    ASSERT_EQ(placement_select_ex(PLACEMENT_RR, NULL, 0, 1, 1, 0,
                                  &ent),
              MDS_ERR_INVAL);
    struct mds_ds_info ds;
    mk_ds(&ds, 1, DS_ONLINE, 1000, 0);
    ASSERT_EQ(placement_select_ex(PLACEMENT_RR, &ds, 1, 0, 1, 0,
                                  &ent),
              MDS_ERR_INVAL);
    ASSERT_EQ(placement_select_ex(PLACEMENT_RR, &ds, 1, 1, 0, 0,
                                  &ent),
              MDS_ERR_INVAL);

    fprintf(stdout, "PASS\n");
    passed++;
}

/* -----------------------------------------------------------------------
 * Phase H of docs/hpc-nto1-plan.md — ds_filter_compatible_preferred
 *
 * Smoke-tests the four behavioural quadrants:
 *   1. preferred subset non-empty  -> only preferred DSes returned
 *   2. preferred subset empty      -> graceful fallback to required set
 *   3. zero preference passed      -> bit-identical to ds_filter_compatible
 *   4. invalid args                -> MDS_ERR_INVAL
 * Plus a target-bit check (RDMA preferred but only TCP available -> all
 * required DSes returned, no NULL/0 collapse).
 * ----------------------------------------------------------------------- */

static bool ds_array_contains(const struct mds_ds_info *arr, uint32_t n,
                              uint32_t ds_id)
{
    for (uint32_t i = 0; i < n; i++) {
        if (arr[i].ds_id == ds_id) {
            return true;
        }
    }
    return false;
}

static void test_filter_preferred_returns_only_preferred(void)
{
    fprintf(stdout, "  filter_preferred_only_preferred:  ");

    struct mds_ds_info ds[3];
    /* ds 1: TCP only (not preferred) */
    mk_ds(&ds[0], 1, DS_ONLINE, 1000, 0);
    ds[0].transport = DS_TRANSPORT_TCP;
    /* ds 2: TCP+RDMA, GPUDirect-capable (preferred) */
    mk_ds(&ds[1], 2, DS_ONLINE, 1000, 0);
    ds[1].transport = DS_TRANSPORT_TCP | DS_TRANSPORT_RDMA;
    ds[1].capabilities = DS_CAP_GPUDIRECT;
    /* ds 3: RDMA-only, GPUDirect-capable (preferred) */
    mk_ds(&ds[2], 3, DS_ONLINE, 1000, 0);
    ds[2].transport = DS_TRANSPORT_RDMA;
    ds[2].capabilities = DS_CAP_GPUDIRECT;

    struct mds_ds_info *out = NULL;
    uint32_t out_count = 0;
    enum mds_status st = ds_filter_compatible_preferred(
        ds, 3,
        DS_MODE_GENERIC,
        DS_TRANSPORT_TCP | DS_TRANSPORT_RDMA,
        DS_TRANSPORT_RDMA, DS_CAP_GPUDIRECT,
        &out, &out_count);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(out_count, 2u);
    ASSERT_TRUE(ds_array_contains(out, out_count, 2));
    ASSERT_TRUE(ds_array_contains(out, out_count, 3));
    ASSERT_TRUE(!ds_array_contains(out, out_count, 1));
    free(out);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_filter_preferred_falls_back_when_none(void)
{
    fprintf(stdout, "  filter_preferred_falls_back:      ");

    /* No RDMA / GPUDirect anywhere — all three are TCP-only.
     * Expected behaviour: HPC-shared still gets a layout, the
     * preferred subset is empty so the helper hands back the
     * full required-matching set unchanged. */
    struct mds_ds_info ds[3];
    mk_ds(&ds[0], 1, DS_ONLINE, 1000, 0);
    mk_ds(&ds[1], 2, DS_ONLINE, 1000, 0);
    mk_ds(&ds[2], 3, DS_ONLINE, 1000, 0);

    struct mds_ds_info *out = NULL;
    uint32_t out_count = 0;
    enum mds_status st = ds_filter_compatible_preferred(
        ds, 3,
        DS_MODE_GENERIC, DS_TRANSPORT_TCP,
        DS_TRANSPORT_RDMA, DS_CAP_GPUDIRECT,
        &out, &out_count);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(out_count, 3u);
    ASSERT_TRUE(ds_array_contains(out, out_count, 1));
    ASSERT_TRUE(ds_array_contains(out, out_count, 2));
    ASSERT_TRUE(ds_array_contains(out, out_count, 3));
    free(out);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_filter_preferred_zero_pref_matches_legacy(void)
{
    fprintf(stdout, "  filter_preferred_zero_pref_legacy:");

    /* Mixed fleet, but caller passes zero preferences.  Result must
     * be identical to ds_filter_compatible (every ONLINE +
     * required-matching DS, no preference filter applied). */
    struct mds_ds_info ds[3];
    mk_ds(&ds[0], 1, DS_ONLINE, 1000, 0);
    ds[0].transport = DS_TRANSPORT_TCP;
    mk_ds(&ds[1], 2, DS_OFFLINE, 1000, 0);
    ds[1].transport = DS_TRANSPORT_TCP | DS_TRANSPORT_RDMA;
    mk_ds(&ds[2], 3, DS_ONLINE, 1000, 0);
    ds[2].transport = DS_TRANSPORT_RDMA;

    struct mds_ds_info *legacy = NULL;
    uint32_t legacy_count = 0;
    ASSERT_EQ(ds_filter_compatible(
                  ds, 3, DS_MODE_GENERIC,
                  DS_TRANSPORT_TCP | DS_TRANSPORT_RDMA,
                  &legacy, &legacy_count), MDS_OK);

    struct mds_ds_info *pref = NULL;
    uint32_t pref_count = 0;
    ASSERT_EQ(ds_filter_compatible_preferred(
                  ds, 3, DS_MODE_GENERIC,
                  DS_TRANSPORT_TCP | DS_TRANSPORT_RDMA,
                  0, 0,
                  &pref, &pref_count), MDS_OK);

    ASSERT_EQ(legacy_count, pref_count);
    for (uint32_t i = 0; i < pref_count; i++) {
        ASSERT_TRUE(ds_array_contains(legacy,
                                      legacy_count, pref[i].ds_id));
    }
    free(legacy);
    free(pref);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_filter_preferred_skips_offline(void)
{
    fprintf(stdout, "  filter_preferred_skips_offline:   ");

    /* The only RDMA+GPUDirect DS is OFFLINE.  Preferred subset must
     * be empty (offline DSes never count) and the helper must hand
     * back the ONLINE TCP fallback set. */
    struct mds_ds_info ds[3];
    mk_ds(&ds[0], 1, DS_ONLINE, 1000, 0);
    ds[0].transport = DS_TRANSPORT_TCP;
    mk_ds(&ds[1], 2, DS_OFFLINE, 1000, 0);
    ds[1].transport = DS_TRANSPORT_RDMA;
    ds[1].capabilities = DS_CAP_GPUDIRECT;
    mk_ds(&ds[2], 3, DS_ONLINE, 1000, 0);
    ds[2].transport = DS_TRANSPORT_TCP;

    struct mds_ds_info *out = NULL;
    uint32_t out_count = 0;
    enum mds_status st = ds_filter_compatible_preferred(
        ds, 3,
        DS_MODE_GENERIC, DS_TRANSPORT_TCP,
        DS_TRANSPORT_RDMA, DS_CAP_GPUDIRECT,
        &out, &out_count);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(out_count, 2u);
    ASSERT_TRUE(ds_array_contains(out, out_count, 1));
    ASSERT_TRUE(ds_array_contains(out, out_count, 3));
    ASSERT_TRUE(!ds_array_contains(out, out_count, 2));
    free(out);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_filter_preferred_invalid_args(void)
{
    fprintf(stdout, "  filter_preferred_invalid_args:    ");

    struct mds_ds_info *out = NULL;
    uint32_t out_count = 0;
    ASSERT_EQ(ds_filter_compatible_preferred(
                  NULL, 0, DS_MODE_GENERIC, DS_TRANSPORT_TCP,
                  DS_TRANSPORT_RDMA, 0,
                  &out, &out_count), MDS_ERR_INVAL);

    struct mds_ds_info ds;
    mk_ds(&ds, 1, DS_ONLINE, 1000, 0);
    ASSERT_EQ(ds_filter_compatible_preferred(
                  &ds, 1, DS_MODE_GENERIC, DS_TRANSPORT_TCP,
                  DS_TRANSPORT_RDMA, 0,
                  NULL, &out_count), MDS_ERR_INVAL);
    ASSERT_EQ(ds_filter_compatible_preferred(
                  &ds, 1, DS_MODE_GENERIC, DS_TRANSPORT_TCP,
                  DS_TRANSPORT_RDMA, 0,
                  &out, NULL), MDS_ERR_INVAL);

    fprintf(stdout, "PASS\n");
    passed++;
}

int main(void)
{
    fprintf(stdout, "test_placement_policy:\n");

    test_rr_delegates_to_placement_select();
    test_capacity_picks_emptiest();
    test_capacity_skips_offline();
    test_wrr_weight_bias();
    test_wrr_explicit_weight_respected();
    test_wrr_explicit_weight_overrides_free();
    test_wrr_offline_excluded();
    test_no_online_returns_nospc();
    test_multi_stripe_falls_back_to_rr();
    test_invalid_args();
    test_filter_preferred_returns_only_preferred();
    test_filter_preferred_falls_back_when_none();
    test_filter_preferred_zero_pref_matches_legacy();
    test_filter_preferred_skips_offline();
    test_filter_preferred_invalid_args();

    fprintf(stdout, "\n  %d passed, %d failed\n", passed, failed);
    return failed > 0 ? 1 : 0;
}
