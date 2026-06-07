/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_lease_table.c -- Unit tests for the per-(DS-stripe) lease table.
 */

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "lease_table.h"

#define VERIFY(expr) do { if (!(expr)) { \
    fprintf(stderr, "VERIFY FAILED: %s (%s:%d)\n", \
        #expr, __FILE__, __LINE__); abort(); } } while (0)

static int tests_run;
static int tests_passed;

#define RUN_TEST(fn) do { \
    tests_run++; \
    fprintf(stdout, "  %-55s", #fn); \
    fflush(stdout); \
    fn(); \
    tests_passed++; \
    fprintf(stdout, "PASS\n"); \
} while (0)

/* 1: init succeeds and destroy is NULL-safe. */
static void test_init_and_destroy(void)
{
    struct stripe_lease_table *t = NULL;
    VERIFY(stripe_lease_table_init(&t) == 0);
    VERIFY(t != NULL);
    stripe_lease_table_destroy(t);
    stripe_lease_table_destroy(NULL); /* NULL-safe */
}

/* 2: same-client repeat acquire does not conflict (renewal path). */
static void test_same_client_renew_no_conflict(void)
{
    struct stripe_lease_table *t = NULL;
    VERIFY(stripe_lease_table_init(&t) == 0);
    VERIFY(stripe_lease_acquire(t, 100, 1, 0, 0, 0, 4096, 5000) == 0);
    VERIFY(!stripe_lease_check_conflict(t, 100, 1, 0, 0, 4096));
    VERIFY(stripe_lease_acquire(t, 100, 1, 0, 0, 0, 4096, 5000) == 0);
    VERIFY(!stripe_lease_check_conflict(t, 100, 1, 0, 0, 4096));
    stripe_lease_table_destroy(t);
}

/* 3: different client overlapping ds_range on same stripe -> conflict. */
static void test_other_client_overlap_conflicts(void)
{
    struct stripe_lease_table *t = NULL;
    VERIFY(stripe_lease_table_init(&t) == 0);
    VERIFY(stripe_lease_acquire(t, 100, 1, 0, 0, 0, 8192, 5000) == 0);
    VERIFY(stripe_lease_check_conflict(t, 100, 2, 0, 4096, 4096));
    stripe_lease_table_destroy(t);
}

/* 4: different client disjoint ds_range on same stripe -> no conflict. */
static void test_other_client_disjoint_no_conflict(void)
{
    struct stripe_lease_table *t = NULL;
    VERIFY(stripe_lease_table_init(&t) == 0);
    VERIFY(stripe_lease_acquire(t, 100, 1, 0, 0, 0, 4096, 5000) == 0);
    VERIFY(!stripe_lease_check_conflict(t, 100, 2, 0, 4096, 4096));
    stripe_lease_table_destroy(t);
}

/* 5: different stripe_index -> never conflict. */
static void test_different_stripe_no_conflict(void)
{
    struct stripe_lease_table *t = NULL;
    VERIFY(stripe_lease_table_init(&t) == 0);
    VERIFY(stripe_lease_acquire(t, 100, 1, 0, 0, 0, 65536, 5000) == 0);
    VERIFY(!stripe_lease_check_conflict(t, 100, 2, 1, 0, 65536));
    stripe_lease_table_destroy(t);
}

/* 6: different fileid -> never conflict. */
static void test_different_fileid_no_conflict(void)
{
    struct stripe_lease_table *t = NULL;
    VERIFY(stripe_lease_table_init(&t) == 0);
    VERIFY(stripe_lease_acquire(t, 100, 1, 0, 0, 0, 65536, 5000) == 0);
    VERIFY(!stripe_lease_check_conflict(t, 101, 2, 0, 0, 65536));
    stripe_lease_table_destroy(t);
}

/* 7: release_all_for removes every entry for (fileid, clientid). */
static void test_release_all_for(void)
{
    struct stripe_lease_table *t = NULL;
    VERIFY(stripe_lease_table_init(&t) == 0);
    /* Three stripes -- after the mixer these likely land in different shards. */
    VERIFY(stripe_lease_acquire(t, 100, 1, 0,  0, 0, 65536, 5000) == 0);
    VERIFY(stripe_lease_acquire(t, 100, 1, 0,  1, 0, 65536, 5000) == 0);
    VERIFY(stripe_lease_acquire(t, 100, 1, 0, 17, 0, 65536, 5000) == 0);
    stripe_lease_release_all_for(t, 100, 1);
    /* All three slots should be free for a different client now. */
    VERIFY(!stripe_lease_check_conflict(t, 100, 2,  0, 0, 65536));
    VERIFY(!stripe_lease_check_conflict(t, 100, 2,  1, 0, 65536));
    VERIFY(!stripe_lease_check_conflict(t, 100, 2, 17, 0, 65536));
    stripe_lease_table_destroy(t);
}

/* 8: release_all_for leaves other clients untouched. */
static void test_release_all_for_isolated(void)
{
    struct stripe_lease_table *t = NULL;
    VERIFY(stripe_lease_table_init(&t) == 0);
    VERIFY(stripe_lease_acquire(t, 100, 1, 0, 0, 0, 4096, 5000) == 0);
    VERIFY(stripe_lease_acquire(t, 100, 2, 0, 1, 0, 4096, 5000) == 0);
    stripe_lease_release_all_for(t, 100, 1);
    /* client 2's lease on stripe 1 must still be there. */
    VERIFY(stripe_lease_check_conflict(t, 100, 3, 1, 0, 4096));
    /* client 1's lease on stripe 0 must be gone. */
    VERIFY(!stripe_lease_check_conflict(t, 100, 3, 0, 0, 4096));
    stripe_lease_table_destroy(t);
}

/* 9: explicit release of an exact (fileid, clientid, stripe, ds_off). */
static void test_release_exact_tuple(void)
{
    struct stripe_lease_table *t = NULL;
    VERIFY(stripe_lease_table_init(&t) == 0);
    VERIFY(stripe_lease_acquire(t, 100, 1, 0, 0, 1024, 4096, 5000) == 0);
    VERIFY(stripe_lease_check_conflict(t, 100, 2, 0, 1024, 4096));
    stripe_lease_release(t, 100, 1, 0, 1024);
    VERIFY(!stripe_lease_check_conflict(t, 100, 2, 0, 1024, 4096));
    stripe_lease_table_destroy(t);
}

/* 10: release of a nonexistent entry is a no-op. */
static void test_release_nonexistent_noop(void)
{
    struct stripe_lease_table *t = NULL;
    VERIFY(stripe_lease_table_init(&t) == 0);
    stripe_lease_release(t, 100, 1, 0, 0);
    stripe_lease_release_all_for(t, 100, 1);
    stripe_lease_table_destroy(t);
}

/* 11: ds_offset uniqueness -- two same-(fileid,client,stripe) entries
 * with distinct ds_offsets coexist. */
static void test_distinct_ds_offsets_coexist(void)
{
    struct stripe_lease_table *t = NULL;
    VERIFY(stripe_lease_table_init(&t) == 0);
    VERIFY(stripe_lease_acquire(t, 100, 1, 0, 0,    0, 4096, 5000) == 0);
    VERIFY(stripe_lease_acquire(t, 100, 1, 0, 0, 8192, 4096, 5000) == 0);
    /* Different client overlapping the first slice but not the second. */
    VERIFY( stripe_lease_check_conflict(t, 100, 2, 0,    0, 4096));
    VERIFY( stripe_lease_check_conflict(t, 100, 2, 0, 8192, 4096));
    VERIFY(!stripe_lease_check_conflict(t, 100, 2, 0, 4096, 4096));
    stripe_lease_table_destroy(t);
}

/* 12: expiry causes a previously-conflicting entry to disappear. */
static void test_expiry_lazy_eviction(void)
{
    struct stripe_lease_table *t = NULL;
    VERIFY(stripe_lease_table_init(&t) == 0);
    VERIFY(stripe_lease_acquire(t, 100, 1, 0, 0, 0, 4096, 50) == 0); /* 50 ms */
    VERIFY(stripe_lease_check_conflict(t, 100, 2, 0, 0, 4096));
    struct timespec _ns = {0, 80 * 1000 * 1000}; nanosleep(&_ns, NULL); /* 80 ms */
    VERIFY(!stripe_lease_check_conflict(t, 100, 2, 0, 0, 4096));
    stripe_lease_table_destroy(t);
}

/* 13: acquire with duration_ms == 0 fails. */
static void test_acquire_zero_duration_invalid(void)
{
    struct stripe_lease_table *t = NULL;
    VERIFY(stripe_lease_table_init(&t) == 0);
    VERIFY(stripe_lease_acquire(t, 100, 1, 0, 0, 0, 4096, 0) == -1);
    stripe_lease_table_destroy(t);
}

/* 14: NULL-safety -- every API gracefully accepts NULL. */
static void test_null_safety(void)
{
    VERIFY(stripe_lease_table_init(NULL) == -1);
    stripe_lease_table_destroy(NULL);
    VERIFY(!stripe_lease_check_conflict(NULL, 0, 0, 0, 0, 4096));
    VERIFY(stripe_lease_acquire(NULL, 0, 0, 0, 0, 0, 4096, 1000) == -1);
    stripe_lease_release(NULL, 0, 0, 0, 0);
    stripe_lease_release_all_for(NULL, 0, 0);
}

int main(void)
{
    fprintf(stdout, "test_lease_table:\n");
    RUN_TEST(test_init_and_destroy);
    RUN_TEST(test_same_client_renew_no_conflict);
    RUN_TEST(test_other_client_overlap_conflicts);
    RUN_TEST(test_other_client_disjoint_no_conflict);
    RUN_TEST(test_different_stripe_no_conflict);
    RUN_TEST(test_different_fileid_no_conflict);
    RUN_TEST(test_release_all_for);
    RUN_TEST(test_release_all_for_isolated);
    RUN_TEST(test_release_exact_tuple);
    RUN_TEST(test_release_nonexistent_noop);
    RUN_TEST(test_distinct_ds_offsets_coexist);
    RUN_TEST(test_expiry_lazy_eviction);
    RUN_TEST(test_acquire_zero_duration_invalid);
    RUN_TEST(test_null_safety);
    fprintf(stdout, "%d/%d tests passed\n", tests_passed, tests_run);
    return tests_passed == tests_run ? 0 : 1;
}
