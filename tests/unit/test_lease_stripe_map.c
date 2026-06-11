/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_lease_stripe_map.c -- Unit tests for the pure stripe-slice helper.
 */

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "lease_stripe_map.h"

#define VERIFY(expr) do { if (!(expr)) { \
    fprintf(stderr, "VERIFY FAILED: %s (%s:%d)\n", \
        #expr, __FILE__, __LINE__); abort(); } } while (0)

static int tests_run;
static int tests_passed;

#define RUN_TEST(fn) do { \
    tests_run++; \
    fprintf(stdout, "  %-50s", #fn); \
    fflush(stdout); \
    fn(); \
    tests_passed++; \
    fprintf(stdout, "PASS\n"); \
} while (0)

/* Helper: assert a slice equals the expected triple. */
static void assert_slice(const struct stripe_slice *s,
                         uint32_t idx, uint64_t off, uint64_t len)
{
    VERIFY(s->stripe_index == idx);
    VERIFY(s->ds_offset == off);
    VERIFY(s->ds_length == len);
}

/* 1: single byte at stripe 0 -> one slice. */
static void test_single_byte(void)
{
    struct stripe_slice s[4];
    int n = lease_range_to_stripe_slices(0, 1, 65536, s, 4);
    VERIFY(n == 1);
    assert_slice(&s[0], 0, 0, 1);
}

/* 2: a full single stripe at exact boundary, length = stripe_unit. */
static void test_full_single_stripe(void)
{
    struct stripe_slice s[4];
    int n = lease_range_to_stripe_slices(0, 65536, 65536, s, 4);
    VERIFY(n == 1);
    assert_slice(&s[0], 0, 0, 65536);
}

/* 3: two-stripe straddle starting at boundary. */
static void test_two_stripe_boundary_start(void)
{
    struct stripe_slice s[4];
    int n = lease_range_to_stripe_slices(0, 65537, 65536, s, 4);
    VERIFY(n == 2);
    assert_slice(&s[0], 0, 0, 65536);
    assert_slice(&s[1], 1, 0, 1);
}

/* 4: three-stripe straddle, mid-stripe start. */
static void test_three_stripe_mid_start(void)
{
    struct stripe_slice s[4];
    /* offset=100, length=65536*2 + 5, stripe=65536 */
    int n = lease_range_to_stripe_slices(100, 131077, 65536, s, 4);
    VERIFY(n == 3);
    assert_slice(&s[0], 0, 100, 65536 - 100);
    assert_slice(&s[1], 1, 0, 65536);
    assert_slice(&s[2], 2, 0, 100 + 5);
}

/* 5: range starts mid-stripe, ends mid-same-stripe. */
static void test_mid_stripe_short(void)
{
    struct stripe_slice s[4];
    int n = lease_range_to_stripe_slices(1000, 5000, 65536, s, 4);
    VERIFY(n == 1);
    assert_slice(&s[0], 0, 1000, 5000);
}

/* 6: range starts mid-stripe, ends exactly at boundary. */
static void test_mid_to_boundary(void)
{
    struct stripe_slice s[4];
    int n = lease_range_to_stripe_slices(100, 65536 - 100, 65536, s, 4);
    VERIFY(n == 1);
    assert_slice(&s[0], 0, 100, 65536 - 100);
}

/* 7: stripe_index for high but valid offset. */
static void test_high_stripe_index(void)
{
    struct stripe_slice s[4];
    /* offset = 1000 * 65536 + 7, length = 10. */
    uint64_t off = (uint64_t)1000 * 65536 + 7;
    int n = lease_range_to_stripe_slices(off, 10, 65536, s, 4);
    VERIFY(n == 1);
    assert_slice(&s[0], 1000, 7, 10);
}

/* 8: range exactly fills max_slices. */
static void test_exact_max_slices(void)
{
    struct stripe_slice s[3];
    int n = lease_range_to_stripe_slices(0, 65536 * 3, 65536, s, 3);
    VERIFY(n == 3);
    assert_slice(&s[0], 0, 0, 65536);
    assert_slice(&s[1], 1, 0, 65536);
    assert_slice(&s[2], 2, 0, 65536);
}

/* 9: ENOSPC -- range needs 4 stripes, capacity is 3. */
static void test_enospc_overflow(void)
{
    struct stripe_slice s[3];
    int n = lease_range_to_stripe_slices(0, 65536 * 3 + 1, 65536, s, 3);
    VERIFY(n == -ENOSPC);
}

/* 10: EINVAL -- stripe_unit = 0. */
static void test_einval_stripe_unit_zero(void)
{
    struct stripe_slice s[4];
    int n = lease_range_to_stripe_slices(0, 4096, 0, s, 4);
    VERIFY(n == -EINVAL);
}

/* 11: EINVAL -- NULL out. */
static void test_einval_null_out(void)
{
    int n = lease_range_to_stripe_slices(0, 4096, 65536, NULL, 4);
    VERIFY(n == -EINVAL);
}

/* 12: EINVAL -- zero length. */
static void test_einval_zero_length(void)
{
    struct stripe_slice s[4];
    int n = lease_range_to_stripe_slices(0, 0, 65536, s, 4);
    VERIFY(n == -EINVAL);
}

/* 13: EINVAL -- UINT64_MAX whole-file sentinel rejected. */
static void test_einval_uint64_max(void)
{
    struct stripe_slice s[4];
    int n = lease_range_to_stripe_slices(0, UINT64_MAX, 65536, s, 4);
    VERIFY(n == -EINVAL);
}

int main(void)
{
    fprintf(stdout, "test_lease_stripe_map:\n");
    RUN_TEST(test_single_byte);
    RUN_TEST(test_full_single_stripe);
    RUN_TEST(test_two_stripe_boundary_start);
    RUN_TEST(test_three_stripe_mid_start);
    RUN_TEST(test_mid_stripe_short);
    RUN_TEST(test_mid_to_boundary);
    RUN_TEST(test_high_stripe_index);
    RUN_TEST(test_exact_max_slices);
    RUN_TEST(test_enospc_overflow);
    RUN_TEST(test_einval_stripe_unit_zero);
    RUN_TEST(test_einval_null_out);
    RUN_TEST(test_einval_zero_length);
    RUN_TEST(test_einval_uint64_max);
    fprintf(stdout, "%d/%d tests passed\n", tests_passed, tests_run);
    return tests_passed == tests_run ? 0 : 1;
}
