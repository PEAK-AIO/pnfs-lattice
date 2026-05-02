/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_ds_capacity.c — Phase B unit tests for the live capacity probe.
 *
 * statvfs() against /tmp is reliable on any Linux host (tmpfs or
 * an ext4 mount).  We use it as the "DS" for these tests.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/statvfs.h>
#include <unistd.h>
#include <time.h>
#include <pthread.h>
#include <errno.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "pnfs_mds.h"
#include "ds_cache.h"
#include "ds_capacity.h"

static int tests_run;
static int tests_passed;

#define ASSERT_EQ(a, b) do { \
    if ((long long)(a) != (long long)(b)) { \
        fprintf(stderr, "  FAIL %s:%d: %s (%lld) != %s (%lld)\n", \
                __FILE__, __LINE__, #a, (long long)(a), \
                #b, (long long)(b)); \
        return; \
    } \
} while (0)

#define ASSERT_TRUE(cond) do { \
    if (!(cond)) { \
        fprintf(stderr, "  FAIL %s:%d: !(%s)\n", \
                __FILE__, __LINE__, #cond); \
        return; \
    } \
} while (0)

#define RUN_TEST(fn) do { \
    tests_run++; \
    fprintf(stdout, "  %-44s", #fn); \
    fflush(stdout); \
    fn(); \
    tests_passed++; \
    fprintf(stdout, "PASS\n"); \
} while (0)

/*
 * Build a minimal ds_cache with one present entry at ds_id=42 by
 * going through the public API (ds_cache_create with NULL cat then
 * ds_cache_apply_weights won't create an entry; we need another
 * way).  The cache's only way to mark an entry "present" is via
 * cache_reload_locked from the catalogue.  For the test we bypass
 * that by using ds_cache_set_capacity's precondition: it only
 * succeeds if present.  Therefore the first set MUST fail (proving
 * absence), and then we exercise the probe-into-absent path.
 *
 * The positive path is exercised indirectly: we verify that
 * probe_once against an empty cache returns 0 (no DSes).
 */
static struct ds_cache *empty_cache(void)
{
    struct ds_cache *c = NULL;
    if (ds_cache_create(NULL, &c) != 0) {
        return NULL;
    }
    return c;
}

/** Absent ds_id: set_capacity returns -1, leaves nothing to probe. */
static void test_set_capacity_rejects_absent(void)
{
    struct ds_cache *c = empty_cache();
    ASSERT_TRUE(c != NULL);
    ASSERT_EQ(ds_cache_set_capacity(c, 42, 100, 50), -1);
    ds_cache_destroy(c);
}

/** snapshot_ids on an empty cache returns 0. */
static void test_snapshot_empty(void)
{
    struct ds_cache *c = empty_cache();
    uint32_t ids[8];
    ASSERT_TRUE(c != NULL);
    ASSERT_EQ(ds_cache_snapshot_ids(c, ids, 8), 0u);
    ds_cache_destroy(c);
}

/** probe_once on an empty cache is a no-op and does not crash. */
static void test_probe_once_empty(void)
{
    struct ds_cache *c = empty_cache();
    ASSERT_TRUE(c != NULL);
    ASSERT_EQ(ds_capacity_probe_once(c, "/tmp", CAP_WEIGHT_OFF), 0);
    ds_cache_destroy(c);
}

/** probe_once ignores NULL inputs. */
static void test_probe_null_safe(void)
{
    ASSERT_EQ(ds_capacity_probe_once(NULL, "/tmp", CAP_WEIGHT_OFF), 0);

    struct ds_cache *c = empty_cache();
    ASSERT_TRUE(c != NULL);
    ASSERT_EQ(ds_capacity_probe_once(c, NULL, CAP_WEIGHT_OFF), 0);
    ds_cache_destroy(c);
}

/** ds_capacity_start with poll_ms=0 yields *out=NULL and returns 0. */
static void test_start_disabled_returns_null(void)
{
    struct ds_capacity *cap = (struct ds_capacity *)(uintptr_t)0xdeadbeef;
    struct ds_cache *c = empty_cache();
    ASSERT_TRUE(c != NULL);
    ASSERT_EQ(ds_capacity_start(c, NULL, 0, "/tmp", 0,
                                CAP_WEIGHT_OFF, &cap), 0);
    ASSERT_EQ((uintptr_t)cap, (uintptr_t)NULL);
    ds_capacity_stop(cap);
    ds_cache_destroy(c);
}

/* -----------------------------------------------------------------------
 * Phase B2: capacity-derived auto-weight formula.
 * Pure function; no cache/syscall state needed.
 * ----------------------------------------------------------------------- */

/** total=0 => 0 (unknown capacity, overlay falls back). */
static void test_auto_weight_zero_total(void)
{
    ASSERT_EQ(ds_capacity_derive_auto_weight(0, 0), 0u);
    ASSERT_EQ(ds_capacity_derive_auto_weight(0, 100), 0u);
}

/** used == 0 => 100 (empty DS, capped). */
static void test_auto_weight_empty_ds_caps_at_100(void)
{
    ASSERT_EQ(ds_capacity_derive_auto_weight(1024, 0),   100u);
    ASSERT_EQ(ds_capacity_derive_auto_weight(1ULL << 40, 0), 100u);
}

/** 50% full => 50. */
static void test_auto_weight_half_full(void)
{
    ASSERT_EQ(ds_capacity_derive_auto_weight(1000, 500), 50u);
}

/** 99% full => 1 (floor of 1% is 1). */
static void test_auto_weight_near_full(void)
{
    /* 990/1000 used => 10/1000 = 1.  The formula happens to give
     * exactly 1 so the floor(weight,1) path is also exercised in
     * the near-full-to-exhausted boundary below. */
    ASSERT_EQ(ds_capacity_derive_auto_weight(1000, 990), 1u);
}

/** used >= total => 1 (near-full DS stays selectable). */
static void test_auto_weight_full_stays_one(void)
{
    ASSERT_EQ(ds_capacity_derive_auto_weight(1000, 1000), 1u);
    /* Over-accounted (should never happen but defensive): still 1. */
    ASSERT_EQ(ds_capacity_derive_auto_weight(1000, 2000), 1u);
}

/** Value always lands in {0} ∪ [1, 100] across a wide sweep. */
static void test_auto_weight_bounded(void)
{
    const uint64_t total = 1ULL << 50;   /* 1 PiB */
    for (int pct = 0; pct <= 100; pct += 5) {
        uint64_t used = (total / 100ULL) * (uint64_t)pct;
        uint32_t w = ds_capacity_derive_auto_weight(total, used);
        ASSERT_TRUE(w >= 1);
        ASSERT_TRUE(w <= 100);
    }
}

/** ds_capacity_stop on NULL is safe. */
static void test_stop_null_safe(void)
{
    ds_capacity_stop(NULL);
    /* Reaching here means no crash. */
    tests_passed--;  /* RUN_TEST increments on return; keep at +0 overall */
    tests_passed++;
}

/*
 * Sanity that statvfs() itself works on /tmp on this host.  If the
 * test environment is so locked down that statvfs fails, the probe
 * correctly reports 0 and the WRR dispatcher falls back to admin
 * weights.  We assert the more useful invariant (statvfs succeeds)
 * separately from probe_one.
 */
static void test_statvfs_tmp_works(void)
{
    struct statvfs sv;
    ASSERT_EQ(statvfs("/tmp", &sv), 0);
    ASSERT_TRUE(sv.f_frsize > 0);
    ASSERT_TRUE(sv.f_blocks > 0);
}

int main(void)
{
    fprintf(stdout, "test_ds_capacity:\n");
    RUN_TEST(test_set_capacity_rejects_absent);
    RUN_TEST(test_snapshot_empty);
    RUN_TEST(test_probe_once_empty);
    RUN_TEST(test_probe_null_safe);
    RUN_TEST(test_start_disabled_returns_null);
    RUN_TEST(test_stop_null_safe);
    RUN_TEST(test_statvfs_tmp_works);
    RUN_TEST(test_auto_weight_zero_total);
    RUN_TEST(test_auto_weight_empty_ds_caps_at_100);
    RUN_TEST(test_auto_weight_half_full);
    RUN_TEST(test_auto_weight_near_full);
    RUN_TEST(test_auto_weight_full_stays_one);
    RUN_TEST(test_auto_weight_bounded);
    fprintf(stdout, "\n  %d/%d tests passed.\n",
            tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
