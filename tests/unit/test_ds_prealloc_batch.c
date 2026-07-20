/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_ds_prealloc_batch.c -- Phase C of docs/hpc-nto1-plan.md.
 *
 * Tests the wide pre-warm batch API (ds_prealloc_batch + result
 * destroy).  Coverage matches the master plan S8 unit-test list:
 *
 *   - happy path 1x1, 4x1, 8x1
 *   - strict_unique_ds rejects when online_count < stripe_count
 *     and creates ZERO DS files (no rollback work needed)
 *   - FH capture failure (no proxy + synthetic FH disabled) returns
 *     MDS_ERR_NOSPC and leaves out->entries == NULL
 *   - successive fileids rotate the DS window (fileid % online)
 *   - identical fileid_hint yields an identical DS spread
 *   - batch_result_destroy is NULL-safe and idempotent
 *
 * The tests run against the in-memory test catalogue and use the
 * synthetic-FH knob (ds_prealloc_test_enable_synthetic_fh) to
 * decouple the FH-capture path from a live DS mount.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>
#include <pthread.h>

#include "ds_prealloc.h"
#include "mds_catalogue.h"
#include "test_helpers.h"

/* -------------------------------------------------------------------
 * Helpers (mirrors test_ds_prealloc.c style)
 * ------------------------------------------------------------------- */

static int g_pass;
static int g_fail;

#define ASSERT_EQ(a, b) do { \
    long long _a = (long long)(a), _b = (long long)(b); \
    if (_a != _b) { \
        fprintf(stdout, "FAIL %s:%d: %s == %lld, expected %lld\n", \
                __FILE__, __LINE__, #a, _a, _b); \
        g_fail++; return; \
    } g_pass++; \
} while (0)

#define ASSERT_NE(a, b) do { \
    long long _a = (long long)(a), _b = (long long)(b); \
    if (_a == _b) { \
        fprintf(stdout, "FAIL %s:%d: %s == %lld (expected !=)\n", \
                __FILE__, __LINE__, #a, _a); \
        g_fail++; return; \
    } g_pass++; \
} while (0)

#define ASSERT_TRUE(x) do { \
    if (!(x)) { \
        fprintf(stdout, "FAIL %s:%d: !(%s)\n", \
                __FILE__, __LINE__, #x); \
        g_fail++; return; \
    } g_pass++; \
} while (0)

static void seed_ds_profile(struct mds_catalogue *db, uint32_t ds_id,
                            uint32_t state, uint8_t mode,
                            uint8_t transport, uint32_t capabilities)
{
    struct mds_ds_info info;
    struct mds_cat_txn *txn = NULL;

    memset(&info, 0, sizeof(info));
    info.ds_id = ds_id;
    info.state = state;
    info.port = 2049;
    info.tcp_port = 2049;
    info.transport = transport;
    info.mode = mode;
    info.capabilities = capabilities;
    snprintf(info.addr, sizeof(info.addr), "10.0.0.%u:/data", ds_id);
    snprintf(info.host, sizeof(info.host), "10.0.0.%u", ds_id);
    snprintf(info.export_path, sizeof(info.export_path), "/data");

    if (mds_cat_txn_begin(db, MDS_CAT_TXN_WRITE, &txn) != MDS_OK) { return; }
    if (mds_cat_ds_put(db, txn, &info) != MDS_OK) {
        mds_cat_txn_abort(txn); return;
    }
    mds_cat_txn_commit(txn);
}

static void seed_ds(struct mds_catalogue *db, uint32_t ds_id)
{
    seed_ds_profile(db, ds_id, DS_ONLINE, DS_MODE_GENERIC,
                    DS_TRANSPORT_TCP, 0);
}

static struct ds_prealloc_ctx *make_ctx(struct mds_catalogue *db,
                                        uint32_t n_ds,
                                        bool synthetic_fh)
{
    struct ds_prealloc_ctx *ctx = NULL;
    for (uint32_t i = 0; i < n_ds; i++) {
        seed_ds(db, i);
    }
    if (ds_prealloc_init(db, NULL, 16, &ctx) != 0) {
        return NULL;
    }
    ds_prealloc_test_enable_synthetic_fh(ctx, synthetic_fh);
    return ctx;
}

static void make_req(struct ds_prealloc_batch_request *req,
                     uint32_t stripes, uint32_t mirrors,
                     bool strict_unique)
{
    memset(req, 0, sizeof(*req));
    req->stripe_count        = stripes;
    req->mirror_count        = mirrors;
    req->stripe_unit         = 65536;
    req->required_mode       = DS_MODE_GENERIC;
    req->required_transport  = DS_TRANSPORT_TCP;
    req->preferred_transport = 0;
    req->preferred_caps      = 0;
    req->strict_unique_ds    = strict_unique;
}

/* -------------------------------------------------------------------
 * Test 1: bad-args bouquet
 * ------------------------------------------------------------------- */

static void test_invalid_args(void)
{
    struct ds_prealloc_batch_request req;
    struct ds_prealloc_batch_result  res;

    make_req(&req, 1, 1, false);
    /* NULL ctx. */
    ASSERT_EQ(ds_prealloc_batch(NULL, &req, &res), MDS_ERR_INVAL);
    /* NULL req. */
    {
        struct ds_prealloc_ctx *ctx;
        struct mds_catalogue *db = open_test_catalogue();
        assert(db != NULL);
        seed_ds(db, 0);
        ds_prealloc_init(db, NULL, 16, &ctx);
        ASSERT_EQ(ds_prealloc_batch(ctx, NULL, &res), MDS_ERR_INVAL);
        ASSERT_EQ(ds_prealloc_batch(ctx, &req, NULL), MDS_ERR_INVAL);
        /* stripe_count = 0 invalid. */
        struct ds_prealloc_batch_request bad = req;
        bad.stripe_count = 0;
        ASSERT_EQ(ds_prealloc_batch(ctx, &bad, &res), MDS_ERR_INVAL);
        /* stripe_count > MDS_MAX_STRIPES invalid. */
        bad = req;
        bad.stripe_count = MDS_MAX_STRIPES + 1U;
        ASSERT_EQ(ds_prealloc_batch(ctx, &bad, &res), MDS_ERR_INVAL);
        /* mirror_count > MDS_MAX_MIRRORS invalid. */
        bad = req;
        bad.mirror_count = MDS_MAX_MIRRORS + 1U;
        ASSERT_EQ(ds_prealloc_batch(ctx, &bad, &res), MDS_ERR_INVAL);
        ds_prealloc_destroy(ctx);
        mds_catalogue_close(db);
    }
}

/* -------------------------------------------------------------------
 * Test 2: happy path 1x1 with synthetic FH
 * ------------------------------------------------------------------- */

static void test_happy_1x1(void)
{
    struct mds_catalogue *db = open_test_catalogue();
    assert(db != NULL);
    struct ds_prealloc_ctx *ctx = make_ctx(db, 2, true);
    assert(ctx != NULL);

    struct ds_prealloc_batch_request req;
    struct ds_prealloc_batch_result  res;
    make_req(&req, 1, 1, false);

    enum mds_status st = ds_prealloc_batch(ctx, &req, &res);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_NE(res.entries, NULL);
    ASSERT_EQ(res.stripe_count, 1);
    ASSERT_EQ(res.mirror_count, 1);
    ASSERT_NE(res.fileid, 0);
    ASSERT_EQ(res.entries[0].nfs_fh_len, 16);  /* synthetic FH = 16 B */

    ds_prealloc_batch_result_destroy(&res);
    ASSERT_EQ(res.entries, NULL);
    ASSERT_EQ(res.fileid, 0);

    ds_prealloc_destroy(ctx);
    mds_catalogue_close(db);
}
/* ------------------------------------------------------------------- */
/* Non-strict placement must retain the effective geometry and must not
 * mistake a valid DS ID of zero for an unfilled phantom entry. */
/* ------------------------------------------------------------------- */

static void test_non_strict_degradation_keeps_ds_zero(void)
{
    struct mds_catalogue *db = open_test_catalogue();
    struct ds_prealloc_ctx *ctx;
    struct ds_prealloc_batch_request req;
    struct ds_prealloc_batch_result res;
    bool saw_zero = false;
    bool saw_one = false;

    assert(db != NULL);
    ctx = make_ctx(db, 2, true);
    assert(ctx != NULL);

    make_req(&req, 3, 1, false);
    req.fileid_hint = 100;
    ASSERT_EQ(ds_prealloc_batch(ctx, &req, &res), MDS_OK);
    ASSERT_EQ(res.stripe_count, 2);
    ASSERT_EQ(res.mirror_count, 1);
    for (uint32_t entry_index = 0; entry_index < res.stripe_count;
         entry_index++) {
        ASSERT_EQ(res.entries[entry_index].nfs_fh_len, 16);
        if (res.entries[entry_index].ds_id == 0) {
            saw_zero = true;
        }
        if (res.entries[entry_index].ds_id == 1) {
            saw_one = true;
        }
    }
    ASSERT_TRUE(saw_zero);
    ASSERT_TRUE(saw_one);

    ds_prealloc_batch_result_destroy(&res);
    ds_prealloc_destroy(ctx);
    mds_catalogue_close(db);
}

/* ------------------------------------------------------------------- */
/* Placement stores entries stripe-major: [s0m0, s0m1, s1m0, s1m1]. */
/* ------------------------------------------------------------------- */

static void test_multi_mirror_entries_are_stripe_major(void)
{
    struct mds_catalogue *db = open_test_catalogue();
    struct ds_prealloc_ctx *ctx;
    struct ds_prealloc_batch_request req;
    struct ds_prealloc_batch_result res;

    assert(db != NULL);
    ctx = make_ctx(db, 4, true);
    assert(ctx != NULL);

    make_req(&req, 2, 2, true);
    req.fileid_hint = 100;
    ASSERT_EQ(ds_prealloc_batch(ctx, &req, &res), MDS_OK);
    ASSERT_EQ(res.stripe_count, 2);
    ASSERT_EQ(res.mirror_count, 2);
    ASSERT_EQ(res.entries[0].ds_id, 0);
    ASSERT_EQ(res.entries[1].ds_id, 1);
    ASSERT_EQ(res.entries[2].ds_id, 2);
    ASSERT_EQ(res.entries[3].ds_id, 3);
    for (uint32_t entry_index = 0; entry_index < 4; entry_index++) {
        ASSERT_EQ(res.entries[entry_index].nfs_fh_len, 16);
    }

    ds_prealloc_batch_result_destroy(&res);
    ds_prealloc_destroy(ctx);
    mds_catalogue_close(db);
}

/* ------------------------------------------------------------------- */
/* Required mode/transport exclude incompatible DSes; a populated
 * preferred subset deterministically wins over the required set. */
/* ------------------------------------------------------------------- */

static void test_profile_filtering_and_preference(void)
{
    struct mds_catalogue *db = open_test_catalogue();
    struct ds_prealloc_ctx *ctx = NULL;
    struct ds_prealloc_batch_request req;
    struct ds_prealloc_batch_result res;
    bool saw_rdma_only = false;
    bool saw_rdma_preferred = false;

    assert(db != NULL);
    seed_ds_profile(db, 10, DS_ONLINE, DS_MODE_GENERIC,
                    DS_TRANSPORT_TCP, 0);
    seed_ds_profile(db, 20, DS_ONLINE, DS_MODE_GENERIC,
                    DS_TRANSPORT_RDMA, 0);
    seed_ds_profile(db, 30, DS_ONLINE, DS_MODE_GENERIC,
                    DS_TRANSPORT_TCP | DS_TRANSPORT_RDMA,
                    DS_CAP_GPUDIRECT);
    seed_ds_profile(db, 40, DS_ONLINE, 0, DS_TRANSPORT_RDMA,
                    DS_CAP_GPUDIRECT);
    ASSERT_EQ(ds_prealloc_init(db, NULL, 16, &ctx), 0);
    ds_prealloc_test_enable_synthetic_fh(ctx, true);

    make_req(&req, 1, 1, false);
    req.fileid_hint = 100;
    req.required_transport = DS_TRANSPORT_RDMA;
    req.preferred_transport = DS_TRANSPORT_RDMA;
    req.preferred_caps = DS_CAP_GPUDIRECT;
    ASSERT_EQ(ds_prealloc_batch(ctx, &req, &res), MDS_OK);
    ASSERT_EQ(res.entries[0].ds_id, 30);
    ds_prealloc_batch_result_destroy(&res);

    make_req(&req, 2, 1, true);
    req.fileid_hint = 100;
    req.required_transport = DS_TRANSPORT_RDMA;
    ASSERT_EQ(ds_prealloc_batch(ctx, &req, &res), MDS_OK);
    for (uint32_t entry_index = 0; entry_index < res.stripe_count;
         entry_index++) {
        if (res.entries[entry_index].ds_id == 20) {
            saw_rdma_only = true;
        }
        if (res.entries[entry_index].ds_id == 30) {
            saw_rdma_preferred = true;
        }
        ASSERT_TRUE(res.entries[entry_index].ds_id != 10);
        ASSERT_TRUE(res.entries[entry_index].ds_id != 40);
    }
    ASSERT_TRUE(saw_rdma_only);
    ASSERT_TRUE(saw_rdma_preferred);

    ds_prealloc_batch_result_destroy(&res);
    ds_prealloc_destroy(ctx);
    mds_catalogue_close(db);
}

/* -------------------------------------------------------------------
 * Test 3: happy path 4x1 -- verifies parallel FH capture and the
 * deterministic spread (no two stripes on the same DS when
 * compat_count >= stripe_count).
 * ------------------------------------------------------------------- */

static void test_happy_4x1_unique_spread(void)
{
    struct mds_catalogue *db = open_test_catalogue();
    assert(db != NULL);
    struct ds_prealloc_ctx *ctx = make_ctx(db, 4, true);
    assert(ctx != NULL);

    struct ds_prealloc_batch_request req;
    struct ds_prealloc_batch_result  res;
    make_req(&req, 4, 1, true);

    enum mds_status st = ds_prealloc_batch(ctx, &req, &res);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(res.stripe_count, 4);

    /* All 4 entries should have distinct ds_ids and a captured FH. */
    bool seen[64] = {false};
    for (uint32_t i = 0; i < 4; i++) {
        ASSERT_EQ(res.entries[i].nfs_fh_len, 16);
        ASSERT_TRUE(res.entries[i].ds_id < 64);
        ASSERT_TRUE(!seen[res.entries[i].ds_id]);
        seen[res.entries[i].ds_id] = true;
    }

    ds_prealloc_batch_result_destroy(&res);
    ds_prealloc_destroy(ctx);
    mds_catalogue_close(db);
}

/* -------------------------------------------------------------------
 * Test 4: strict_unique_ds rejects when online_count < stripe_count.
 * Caller must observe MDS_ERR_NOSPC and out->entries == NULL.
 * ------------------------------------------------------------------- */

static void test_strict_unique_underflow(void)
{
    struct mds_catalogue *db = open_test_catalogue();
    assert(db != NULL);
    struct ds_prealloc_ctx *ctx = make_ctx(db, 2, true);
    assert(ctx != NULL);

    struct ds_prealloc_batch_request req;
    struct ds_prealloc_batch_result  res;
    make_req(&req, 4, 1, true);  /* ask for 4 unique, only 2 online */

    enum mds_status st = ds_prealloc_batch(ctx, &req, &res);
    ASSERT_EQ(st, MDS_ERR_NOSPC);
    ASSERT_EQ(res.entries, NULL);
    ASSERT_EQ(res.fileid, 0);

    ds_prealloc_destroy(ctx);
    mds_catalogue_close(db);
}

/* -------------------------------------------------------------------
 * Test 5: FH-capture failure rolls back.  No proxy attached and
 * synthetic-FH knob disabled -- every worker hits the !proxy branch
 * and signals first_error, so the batch must return NOSPC and free
 * the entries buffer (no leak; valgrind exercises this in qa-check).
 * ------------------------------------------------------------------- */

static void test_fh_capture_failure_rolls_back(void)
{
    struct mds_catalogue *db = open_test_catalogue();
    assert(db != NULL);
    struct ds_prealloc_ctx *ctx = make_ctx(db, 4, false);
    assert(ctx != NULL);

    struct ds_prealloc_batch_request req;
    struct ds_prealloc_batch_result  res;
    make_req(&req, 4, 1, false);

    enum mds_status st = ds_prealloc_batch(ctx, &req, &res);
    ASSERT_EQ(st, MDS_ERR_NOSPC);
    ASSERT_EQ(res.entries, NULL);

    ds_prealloc_destroy(ctx);
    mds_catalogue_close(db);
}

/* -------------------------------------------------------------------
 * Test 6: fileid-rotated placement.
 *
 *   - Same fileid_hint => identical DS spread (deterministic).
 *   - Distinct consecutive fileids with stripe_count < online_count
 *     must not all land on the same DS set (the old plan-cache /
 *     capacity-hot-N bug).
 * ------------------------------------------------------------------- */

static void test_fileid_rotates_spread(void)
{
    struct mds_catalogue *db = open_test_catalogue();
    assert(db != NULL);
    /* 8 ONLINE DSes, 4-wide layouts => windows of length 4 rotate. */
    struct ds_prealloc_ctx *ctx = make_ctx(db, 8, true);
    assert(ctx != NULL);

    struct ds_prealloc_batch_request req;
    struct ds_prealloc_batch_result  r1, r2, r3;
    make_req(&req, 4, 1, true);

    /* Same hint => same spread. */
    req.fileid_hint = 100;
    ASSERT_EQ(ds_prealloc_batch(ctx, &req, &r1), MDS_OK);
    ASSERT_EQ(ds_prealloc_batch(ctx, &req, &r2), MDS_OK);
    for (uint32_t i = 0; i < 4; i++) {
        ASSERT_EQ(r1.entries[i].ds_id, r2.entries[i].ds_id);
    }
    ASSERT_EQ(r1.fileid, r2.fileid);

    /* fileid+1 shifts the window by one ONLINE slot. */
    req.fileid_hint = 101;
    ASSERT_EQ(ds_prealloc_batch(ctx, &req, &r3), MDS_OK);
    ASSERT_NE(r1.entries[0].ds_id, r3.entries[0].ds_id);

    /* Across two consecutive 4-wide files on 8 DSes, union covers
     * more than 4 distinct DSes (not stuck on a hot set). */
    bool seen[64] = {false};
    uint32_t uniq = 0;
    for (uint32_t i = 0; i < 4; i++) {
        if (!seen[r1.entries[i].ds_id]) {
            seen[r1.entries[i].ds_id] = true;
            uniq++;
        }
        if (!seen[r3.entries[i].ds_id]) {
            seen[r3.entries[i].ds_id] = true;
            uniq++;
        }
    }
    ASSERT_TRUE(uniq > 4);

    ds_prealloc_batch_result_destroy(&r1);
    ds_prealloc_batch_result_destroy(&r2);
    ds_prealloc_batch_result_destroy(&r3);
    ds_prealloc_destroy(ctx);
    mds_catalogue_close(db);
}

/* -------------------------------------------------------------------
 * Test 7: caller-supplied fileid_hint reused verbatim.  Covers the
 * Phase C / Step 5 OPEN-CREATE integration where the catalogue has
 * already allocated and persisted an inode and the wide pre-warm
 * must capture FHs against that exact fileid (not a fresh one from
 * mds_cat_alloc_fileid).
 * ------------------------------------------------------------------- */

static void test_fileid_hint_is_honored(void)
{
    struct mds_catalogue *db = open_test_catalogue();
    assert(db != NULL);
    struct ds_prealloc_ctx *ctx = make_ctx(db, 4, true);
    assert(ctx != NULL);

    struct ds_prealloc_batch_request req;
    struct ds_prealloc_batch_result  res;
    make_req(&req, 4, 1, true);
    /* Magic value chosen well outside the memdb's auto-allocated
     * range so an accidental collision with an actual fileid is
     * impossible.  The batch should round-trip it unchanged. */
    req.fileid_hint = 0xDEADBEEF42ULL;

    enum mds_status st = ds_prealloc_batch(ctx, &req, &res);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(res.fileid, 0xDEADBEEF42ULL);
    ASSERT_EQ(res.stripe_count, 4);

    ds_prealloc_batch_result_destroy(&res);
    ds_prealloc_destroy(ctx);
    mds_catalogue_close(db);
}

static void test_fileid_hint_zero_allocates(void)
{
    /* Regression guard: fileid_hint == 0 must preserve the
     * pre-Phase-C behaviour of allocating a fresh fileid via the
     * catalogue's alloc op.  Two back-to-back batches must produce
     * distinct fileids. */
    struct mds_catalogue *db = open_test_catalogue();
    assert(db != NULL);
    struct ds_prealloc_ctx *ctx = make_ctx(db, 4, true);
    assert(ctx != NULL);

    struct ds_prealloc_batch_request req;
    struct ds_prealloc_batch_result  r1, r2;
    make_req(&req, 1, 1, false);
    /* fileid_hint stays 0 from make_req's memset. */

    ASSERT_EQ(ds_prealloc_batch(ctx, &req, &r1), MDS_OK);
    ASSERT_EQ(ds_prealloc_batch(ctx, &req, &r2), MDS_OK);
    ASSERT_NE(r1.fileid, 0);
    ASSERT_NE(r2.fileid, 0);
    ASSERT_NE(r1.fileid, r2.fileid);

    ds_prealloc_batch_result_destroy(&r1);
    ds_prealloc_batch_result_destroy(&r2);
    ds_prealloc_destroy(ctx);
    mds_catalogue_close(db);
}

/* -------------------------------------------------------------------
 * Test 9: batch_result_destroy null-safety + idempotency.
 * ------------------------------------------------------------------- */

static void test_destroy_null_safe(void)
{
    /* NULL pointer must not crash. */
    ds_prealloc_batch_result_destroy(NULL);
    g_pass++;

    /* Already-zeroed struct must be safe to destroy twice. */
    struct ds_prealloc_batch_result res;
    memset(&res, 0, sizeof(res));
    ds_prealloc_batch_result_destroy(&res);
    ds_prealloc_batch_result_destroy(&res);
    ASSERT_EQ(res.entries, NULL);
}

int main(void)
{
    printf("test_ds_prealloc_batch:\n");

    test_invalid_args();
    printf("  test_invalid_args                       PASS\n");

    test_happy_1x1();
    printf("  test_happy_1x1                          PASS\n");
    test_non_strict_degradation_keeps_ds_zero();
    printf("  test_non_strict_degradation_keeps_ds_zero PASS\n");

    test_multi_mirror_entries_are_stripe_major();
    printf("  test_multi_mirror_entries_are_stripe_major PASS\n");

    test_profile_filtering_and_preference();
    printf("  test_profile_filtering_and_preference   PASS\n");

    test_happy_4x1_unique_spread();
    printf("  test_happy_4x1_unique_spread            PASS\n");

    test_strict_unique_underflow();
    printf("  test_strict_unique_underflow            PASS\n");

    test_fh_capture_failure_rolls_back();
    printf("  test_fh_capture_failure_rolls_back      PASS\n");

    test_fileid_rotates_spread();
    printf("  test_fileid_rotates_spread              PASS\n");

    test_fileid_hint_is_honored();
    printf("  test_fileid_hint_is_honored             PASS\n");

    test_fileid_hint_zero_allocates();
    printf("  test_fileid_hint_zero_allocates         PASS\n");

    test_destroy_null_safe();
    printf("  test_destroy_null_safe                  PASS\n");

    printf("\n  %d assertions passed, %d failed\n", g_pass, g_fail);
    return g_fail > 0 ? 1 : 0;
}
