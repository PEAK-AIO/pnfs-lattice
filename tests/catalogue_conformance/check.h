/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * check.h -- Assertion helpers shared by the conformance tests.
 *
 * CHECK*   record a failure and continue (every row gets reported).
 * REQUIRE* record a failure and return from the enclosing void
 *          function (the rest of the case cannot run without it).
 *
 * Counters are per translation unit: each conformance test is one
 * executable built from one .c file plus the harness.
 */

#ifndef CATALOGUE_CONFORMANCE_CHECK_H
#define CATALOGUE_CONFORMANCE_CHECK_H

#include <stdio.h>

static unsigned g_check_total;
static unsigned g_check_failed;

#define CHECK(cond) do {                                               \
    g_check_total++;                                                   \
    if (!(cond)) {                                                     \
        g_check_failed++;                                              \
        (void)fprintf(stderr, "  FAIL %s:%d: %s\n", __FILE__, __LINE__,  \
                      #cond);                                          \
    }                                                                  \
} while (0)

#define CHECK_EQ(a, b) do {                                            \
    long long check_a_ = (long long)(a);                               \
    long long check_b_ = (long long)(b);                               \
    g_check_total++;                                                   \
    if (check_a_ != check_b_) {                                        \
        g_check_failed++;                                              \
        (void)fprintf(stderr, "  FAIL %s:%d: %s (%lld) != %s (%lld)\n",  \
                      __FILE__, __LINE__, #a, check_a_, #b, check_b_); \
    }                                                                  \
} while (0)

#define REQUIRE(cond) do {                                             \
    g_check_total++;                                                   \
    if (!(cond)) {                                                     \
        g_check_failed++;                                              \
        (void)fprintf(stderr, "  FAIL %s:%d: %s (required)\n",           \
                      __FILE__, __LINE__, #cond);                      \
        return;                                                        \
    }                                                                  \
} while (0)

#define REQUIRE_EQ(a, b) do {                                          \
    long long check_a_ = (long long)(a);                               \
    long long check_b_ = (long long)(b);                               \
    g_check_total++;                                                   \
    if (check_a_ != check_b_) {                                        \
        g_check_failed++;                                              \
        (void)fprintf(stderr,                                          \
                      "  FAIL %s:%d: %s (%lld) != %s (%lld) (required)\n", \
                      __FILE__, __LINE__, #a, check_a_, #b, check_b_); \
        return;                                                        \
    }                                                                  \
} while (0)

/** Print the summary line and return the process exit code. */
static inline int check_summary(const char *test_name)
{
    (void)printf("\n%s: %u/%u checks passed\n", test_name,
                 g_check_total - g_check_failed, g_check_total);
    return (g_check_failed == 0) ? 0 : 1;
}

#endif /* CATALOGUE_CONFORMANCE_CHECK_H */
