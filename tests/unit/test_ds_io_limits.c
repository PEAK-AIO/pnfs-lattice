/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_ds_io_limits.c -- Wave 5 T5.1 unit tests for the per-DS I/O
 * limit store + FSINFO prober policy (src/mds/ds_io_limits.c).
 *
 * The probe function is injected (ds_io_limits_set_probe_fn) so the
 * policy is exercised without a live DS:
 *   1. Disabled module reports the legacy 1 MiB constants.
 *   2. Enabled-but-unprobed DS reports the 64 KiB fallback.
 *   3. Cap at 1 MiB + round-down to 4 KiB; generation bumps on
 *      change only.
 *   4. A failed probe keeps last-known-good and counts a failure.
 *   5. Decoded limits below 4 KiB mark the DS ineligible and the
 *      min-across-DSes excludes it.
 *   6. A capability DECREASE recalls the DS's layouts (real
 *      layout_recall coordinator against memdb) after publishing.
 *   7. The sweep skips OFFLINE and non-generic DSes.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>
#include <unistd.h>

#include "pnfs_mds.h"
#include "mds_catalogue.h"
#include "ds_io_limits.h"
#include "layout_recall.h"
#include "mds_metrics.h"
#include "test_helpers.h"

/* Like assert() but not elided by NDEBUG. */
#define VERIFY(expr) do { if (!(expr)) { \
    fprintf(stderr, "VERIFY FAILED: %s (%s:%d)\n", \
        #expr, __FILE__, __LINE__); abort(); } } while (0)

static int pass_count;
static int fail_count;

#define ASSERT_EQ(a, b) do { \
    long long _a = (long long)(a), _b = (long long)(b); \
    if (_a != _b) { \
        fprintf(stderr, "FAIL %s:%d: %s == %lld, expected %lld\n", \
                __FILE__, __LINE__, #a, _a, _b); \
        fail_count++; return; \
    } pass_count++; \
} while (0)

#define ASSERT_TRUE(cond) do { \
    if (!(cond)) { \
        fprintf(stderr, "FAIL %s:%d: !(%s)\n", \
                __FILE__, __LINE__, #cond); \
        fail_count++; return; \
    } pass_count++; \
} while (0)

/* -----------------------------------------------------------------------
 * Stub probe function
 *
 * Dispatches on the export path so two DSes on the same host can
 * report different limits ("exp1" gets the override pair when set).
 * ----------------------------------------------------------------------- */

static uint32_t stub_rt;        /* default rtmax */
static uint32_t stub_wt;        /* default wtmax */
static uint32_t stub_rt_exp1;   /* override for ":/exp1" (0 = none) */
static uint32_t stub_wt_exp1;
static int      stub_fail;      /* non-zero: every probe fails */
static int      stub_calls;

static int stub_probe(const char *host, uint16_t port,
                      const char *export_path,
                      uint32_t *rtmax_out, uint32_t *wtmax_out,
                      uint32_t timeout_ms)
{
    (void)host;
    (void)port;
    (void)timeout_ms;

    stub_calls++;
    if (stub_fail) {
        return -1;
    }
    if (stub_rt_exp1 != 0 && strstr(export_path, "exp1") != NULL) {
        *rtmax_out = stub_rt_exp1;
        *wtmax_out = stub_wt_exp1;
    } else {
        *rtmax_out = stub_rt;
        *wtmax_out = stub_wt;
    }
    return 0;
}

/** Reset module + stub state so every test starts from scratch. */
static void reset_all(void)
{
    ds_io_limits_shutdown();
    stub_rt = 0;
    stub_wt = 0;
    stub_rt_exp1 = 0;
    stub_wt_exp1 = 0;
    stub_fail = 0;
    stub_calls = 0;
}

/** Register one DS row in the catalogue. */
static void seed_ds(struct mds_catalogue *cat, uint32_t ds_id,
                    uint32_t state, uint8_t mode, const char *addr)
{
    struct mds_ds_info info;
    struct mds_cat_txn *txn = NULL;

    memset(&info, 0, sizeof(info));
    info.ds_id = ds_id;
    info.state = state;
    info.mode = mode;
    info.transport = DS_TRANSPORT_TCP;
    info.port = 2049;
    (void)snprintf(info.addr, sizeof(info.addr), "%s", addr);

    VERIFY(mds_cat_txn_begin(cat, MDS_CAT_TXN_WRITE, &txn) == MDS_OK);
    VERIFY(mds_cat_ds_put(cat, txn, &info) == MDS_OK);
    mds_cat_txn_commit(txn);
}

/* -----------------------------------------------------------------------
 * Test 1: disabled module reports legacy constants everywhere
 * ----------------------------------------------------------------------- */

static void test_disabled_legacy(void)
{
    uint32_t r = 0;
    uint32_t w = 0;
    uint32_t gen = 99;

    reset_all();
    ASSERT_TRUE(!ds_io_limits_enabled());

    ds_io_limits_get(5, &r, &w, &gen);
    ASSERT_EQ(r, DS_IOLIMIT_LEGACY_BYTES);
    ASSERT_EQ(w, DS_IOLIMIT_LEGACY_BYTES);
    ASSERT_EQ(gen, 0);

    ds_io_limits_min(&r, &w);
    ASSERT_EQ(r, DS_IOLIMIT_LEGACY_BYTES);
    ASSERT_EQ(w, DS_IOLIMIT_LEGACY_BYTES);

    ASSERT_TRUE(ds_io_limits_eligible(5));

    /* Sweep on a disabled module is a no-op. */
    ds_io_limits_sweep(NULL);
    ASSERT_EQ(stub_calls, 0);
}

/* -----------------------------------------------------------------------
 * Test 2: enabled but never probed -> unverified fallback
 * ----------------------------------------------------------------------- */

static void test_enabled_unprobed_fallback(void)
{
    uint32_t r = 0;
    uint32_t w = 0;
    uint32_t gen = 99;

    reset_all();
    ASSERT_EQ(ds_io_limits_enable(), 0);
    ASSERT_TRUE(ds_io_limits_enabled());

    /* Unknown DS: safe fallback, generation 0. */
    ds_io_limits_get(7, &r, &w, &gen);
    ASSERT_EQ(r, DS_IOLIMIT_FALLBACK_BYTES);
    ASSERT_EQ(w, DS_IOLIMIT_FALLBACK_BYTES);
    ASSERT_EQ(gen, 0);

    /* Nothing probed yet: the min pair stays at the legacy value
     * (MAXREAD/MAXWRITE must not shrink below what an all-legacy
     * deployment advertises just because probing is enabled). */
    ds_io_limits_min(&r, &w);
    ASSERT_EQ(r, DS_IOLIMIT_LEGACY_BYTES);
    ASSERT_EQ(w, DS_IOLIMIT_LEGACY_BYTES);

    /* Out-of-range ds_id degrades to legacy, stays eligible. */
    ds_io_limits_get(MDS_MAX_DS_NODES + 5, &r, &w, &gen);
    ASSERT_EQ(r, DS_IOLIMIT_LEGACY_BYTES);
    ASSERT_EQ(gen, 0);
    ASSERT_TRUE(ds_io_limits_eligible(MDS_MAX_DS_NODES + 5));

    ds_io_limits_shutdown();
}

/* -----------------------------------------------------------------------
 * Test 3: cap + round-down policy; generation bumps on change only
 * ----------------------------------------------------------------------- */

static void test_policy_cap_round_generation(void)
{
    struct mds_catalogue *cat = NULL;
    uint32_t r = 0;
    uint32_t w = 0;
    uint32_t gen = 0;
    uint64_t recalls_before;

    reset_all();
    cat = open_test_catalogue(); VERIFY(cat != NULL);
    seed_ds(cat, 1, DS_ONLINE, DS_MODE_GENERIC, "127.0.0.1:/exp");

    ASSERT_EQ(ds_io_limits_enable(), 0);
    ds_io_limits_set_probe_fn(stub_probe);

    /* 2 MiB rtmax is capped at 1 MiB; 65535 wtmax rounds down to
     * the next 4 KiB multiple (61440). */
    stub_rt = 2U * 1024U * 1024U;
    stub_wt = 65535U;
    ds_io_limits_sweep(cat);
    ASSERT_EQ(stub_calls, 1);

    ds_io_limits_get(1, &r, &w, &gen);
    ASSERT_EQ(r, DS_IOLIMIT_CAP_BYTES);
    ASSERT_EQ(w, 61440);
    ASSERT_EQ(gen, 1);
    ASSERT_TRUE(ds_io_limits_eligible(1));

    ds_io_limits_min(&r, &w);
    ASSERT_EQ(r, DS_IOLIMIT_CAP_BYTES);
    ASSERT_EQ(w, 61440);

    /* Same values again: no generation bump. */
    ds_io_limits_sweep(cat);
    ds_io_limits_get(1, &r, &w, &gen);
    ASSERT_EQ(gen, 1);

    /* A shrink with no recall coordinator attached still publishes
     * the new values + generation but issues (and counts) no
     * recall. */
    recalls_before = atomic_load(
        &g_branch_metrics.ds_iolimit_capability_recalls);
    stub_rt = 524288U;
    stub_wt = 524288U;
    ds_io_limits_sweep(cat);
    ds_io_limits_get(1, &r, &w, &gen);
    ASSERT_EQ(r, 524288);
    ASSERT_EQ(w, 524288);
    ASSERT_EQ(gen, 2);
    ASSERT_EQ(atomic_load(&g_branch_metrics.ds_iolimit_capability_recalls),
              recalls_before);

    ds_io_limits_shutdown();
    mds_catalogue_close(cat);
}

/* -----------------------------------------------------------------------
 * Test 4: probe failure keeps last-known-good
 * ----------------------------------------------------------------------- */

static void test_probe_failure_keeps_last_known_good(void)
{
    struct mds_catalogue *cat = NULL;
    uint32_t r = 0;
    uint32_t w = 0;
    uint32_t gen = 0;
    uint64_t fails_before;

    reset_all();
    cat = open_test_catalogue(); VERIFY(cat != NULL);
    seed_ds(cat, 1, DS_ONLINE, DS_MODE_GENERIC, "127.0.0.1:/exp");

    ASSERT_EQ(ds_io_limits_enable(), 0);
    ds_io_limits_set_probe_fn(stub_probe);

    stub_rt = 524288U;
    stub_wt = 524288U;
    ds_io_limits_sweep(cat);
    ds_io_limits_get(1, &r, &w, &gen);
    ASSERT_EQ(r, 524288);
    ASSERT_EQ(gen, 1);

    /* Lost probe: values + generation + eligibility unchanged;
     * failure counter bumps. */
    fails_before = atomic_load(&g_branch_metrics.ds_iolimit_probe_failures);
    stub_fail = 1;
    ds_io_limits_sweep(cat);
    ds_io_limits_get(1, &r, &w, &gen);
    ASSERT_EQ(r, 524288);
    ASSERT_EQ(w, 524288);
    ASSERT_EQ(gen, 1);
    ASSERT_TRUE(ds_io_limits_eligible(1));
    ASSERT_EQ(atomic_load(&g_branch_metrics.ds_iolimit_probe_failures),
              fails_before + 1);

    /* Recovery re-verifies without a generation bump (same values). */
    stub_fail = 0;
    ds_io_limits_sweep(cat);
    ds_io_limits_get(1, &r, &w, &gen);
    ASSERT_EQ(gen, 1);

    ds_io_limits_shutdown();
    mds_catalogue_close(cat);
}

/* -----------------------------------------------------------------------
 * Test 5: limits below 4 KiB -> ineligible; min excludes the DS
 * ----------------------------------------------------------------------- */

static void test_below_4k_ineligible(void)
{
    struct mds_catalogue *cat = NULL;
    uint32_t r = 0;
    uint32_t w = 0;
    uint32_t gen = 0;

    reset_all();
    cat = open_test_catalogue(); VERIFY(cat != NULL);
    seed_ds(cat, 1, DS_ONLINE, DS_MODE_GENERIC, "127.0.0.1:/exp1");
    seed_ds(cat, 2, DS_ONLINE, DS_MODE_GENERIC, "127.0.0.1:/exp2");

    ASSERT_EQ(ds_io_limits_enable(), 0);
    ds_io_limits_set_probe_fn(stub_probe);

    /* ds 1 decodes 2048 (rounds to 0 -> broken); ds 2 is healthy. */
    stub_rt_exp1 = 2048U;
    stub_wt_exp1 = 2048U;
    stub_rt = DS_IOLIMIT_CAP_BYTES;
    stub_wt = DS_IOLIMIT_CAP_BYTES;
    ds_io_limits_sweep(cat);
    ASSERT_EQ(stub_calls, 2);

    ASSERT_TRUE(!ds_io_limits_eligible(1));
    ASSERT_TRUE(ds_io_limits_eligible(2));

    /* The broken DS keeps advertising its previous (fallback)
     * values for already-granted layouts and bumps its generation. */
    ds_io_limits_get(1, &r, &w, &gen);
    ASSERT_EQ(r, DS_IOLIMIT_FALLBACK_BYTES);
    ASSERT_EQ(w, DS_IOLIMIT_FALLBACK_BYTES);
    ASSERT_EQ(gen, 1);

    /* min excludes the ineligible DS. */
    ds_io_limits_min(&r, &w);
    ASSERT_EQ(r, DS_IOLIMIT_CAP_BYTES);
    ASSERT_EQ(w, DS_IOLIMIT_CAP_BYTES);

    ds_io_limits_shutdown();
    mds_catalogue_close(cat);
}

/* -----------------------------------------------------------------------
 * Test 6: capability decrease recalls the DS's layouts
 * ----------------------------------------------------------------------- */

static void test_shrink_recalls_layouts(void)
{
    struct mds_catalogue *cat = NULL;
    struct layout_recall *lr = NULL;
    uint32_t r = 0;
    uint32_t w = 0;
    uint32_t gen = 0;
    uint64_t recalls_before;

    reset_all();
    cat = open_test_catalogue(); VERIFY(cat != NULL);
    seed_ds(cat, 1, DS_ONLINE, DS_MODE_GENERIC, "127.0.0.1:/exp");
    VERIFY(layout_recall_init(cat, NULL, 0, &lr) == 0);

    ASSERT_EQ(ds_io_limits_enable(), 0);
    ds_io_limits_set_probe_fn(stub_probe);
    ds_io_limits_set_recall(lr);

    recalls_before = atomic_load(
        &g_branch_metrics.ds_iolimit_capability_recalls);

    /* First verification (fallback -> 1 MiB is growth): no recall. */
    stub_rt = DS_IOLIMIT_CAP_BYTES;
    stub_wt = DS_IOLIMIT_CAP_BYTES;
    ds_io_limits_sweep(cat);
    ASSERT_EQ(atomic_load(&g_branch_metrics.ds_iolimit_capability_recalls),
              recalls_before);

    /* DECREASE: publish new values, then recall. */
    stub_rt = 524288U;
    stub_wt = 524288U;
    ds_io_limits_sweep(cat);
    ds_io_limits_get(1, &r, &w, &gen);
    ASSERT_EQ(r, 524288);
    ASSERT_EQ(w, 524288);
    ASSERT_EQ(gen, 2);
    ASSERT_EQ(atomic_load(&g_branch_metrics.ds_iolimit_capability_recalls),
              recalls_before + 1);

    ds_io_limits_shutdown();
    layout_recall_destroy(lr);
    mds_catalogue_close(cat);
}

/* -----------------------------------------------------------------------
 * Test 7: sweep skips OFFLINE / non-generic / malformed-addr DSes
 * ----------------------------------------------------------------------- */

static void test_sweep_skips_ineligible_rows(void)
{
    struct mds_catalogue *cat = NULL;

    reset_all();
    cat = open_test_catalogue(); VERIFY(cat != NULL);
    seed_ds(cat, 1, DS_OFFLINE, DS_MODE_GENERIC, "127.0.0.1:/exp");
    seed_ds(cat, 2, DS_ONLINE, 0 /* non-generic */, "127.0.0.1:/exp");
    seed_ds(cat, 3, DS_ONLINE, DS_MODE_GENERIC, "noexportpath");

    ASSERT_EQ(ds_io_limits_enable(), 0);
    ds_io_limits_set_probe_fn(stub_probe);

    stub_rt = DS_IOLIMIT_CAP_BYTES;
    stub_wt = DS_IOLIMIT_CAP_BYTES;
    ds_io_limits_sweep(cat);

    /* No row qualified for a probe. */
    ASSERT_EQ(stub_calls, 0);

    ds_io_limits_shutdown();
    mds_catalogue_close(cat);
}

/* -----------------------------------------------------------------------
 * Test 8: min across multiple healthy DSes
 * ----------------------------------------------------------------------- */

static void test_min_across_multiple_ds(void)
{
    struct mds_catalogue *cat = NULL;
    uint32_t r = 0;
    uint32_t w = 0;

    reset_all();
    cat = open_test_catalogue(); VERIFY(cat != NULL);
    seed_ds(cat, 1, DS_ONLINE, DS_MODE_GENERIC, "127.0.0.1:/exp1");
    seed_ds(cat, 2, DS_ONLINE, DS_MODE_GENERIC, "127.0.0.1:/exp2");

    ASSERT_EQ(ds_io_limits_enable(), 0);
    ds_io_limits_set_probe_fn(stub_probe);

    stub_rt_exp1 = 524288U;
    stub_wt_exp1 = 786432U;   /* 768 KiB */
    stub_rt = DS_IOLIMIT_CAP_BYTES;
    stub_wt = DS_IOLIMIT_CAP_BYTES;
    ds_io_limits_sweep(cat);

    ds_io_limits_min(&r, &w);
    ASSERT_EQ(r, 524288);
    ASSERT_EQ(w, 786432);

    ds_io_limits_shutdown();
    mds_catalogue_close(cat);
}

/* -----------------------------------------------------------------------
 * Test 9: prober thread lifecycle (start requires enable; stop joins)
 * ----------------------------------------------------------------------- */

static void test_prober_lifecycle(void)
{
    struct mds_catalogue *cat = NULL;
    uint32_t r = 0;
    uint32_t gen = 0;

    reset_all();
    cat = open_test_catalogue(); VERIFY(cat != NULL);
    seed_ds(cat, 1, DS_ONLINE, DS_MODE_GENERIC, "127.0.0.1:/exp");

    /* start rejects: not enabled / NULL cat / zero interval. */
    ASSERT_EQ(ds_io_limits_start(cat, 60000), -1);
    ASSERT_EQ(ds_io_limits_enable(), 0);
    ds_io_limits_set_probe_fn(stub_probe);
    ASSERT_EQ(ds_io_limits_start(NULL, 60000), -1);
    ASSERT_EQ(ds_io_limits_start(cat, 0), -1);

    /* Real start: the immediate startup sweep verifies the DS.
     * A long interval keeps the test deterministic (exactly one
     * sweep before stop). */
    stub_rt = 524288U;
    stub_wt = 524288U;
    ASSERT_EQ(ds_io_limits_start(cat, 3600000U), 0);
    for (int i = 0; i < 500; i++) {
        ds_io_limits_get(1, &r, NULL, &gen);
        if (gen != 0) {
            break;
        }
        usleep(10000);
    }
    ASSERT_EQ(r, 524288);
    ASSERT_EQ(gen, 1);

    /* Idempotent start while running; stop joins; double stop OK. */
    ASSERT_EQ(ds_io_limits_start(cat, 3600000U), 0);
    ds_io_limits_stop();
    ds_io_limits_stop();

    ds_io_limits_shutdown();
    mds_catalogue_close(cat);
}

int main(void)
{
    fprintf(stdout, "test_ds_io_limits:\n");

    test_disabled_legacy();
    test_enabled_unprobed_fallback();
    test_policy_cap_round_generation();
    test_probe_failure_keeps_last_known_good();
    test_below_4k_ineligible();
    test_shrink_recalls_layouts();
    test_sweep_skips_ineligible_rows();
    test_min_across_multiple_ds();
    test_prober_lifecycle();

    fprintf(stdout, "%d checks passed, %d failed.\n",
            pass_count, fail_count);
    return (fail_count == 0) ? 0 : 1;
}
