/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_quota.c -- Unit tests for quota rule and usage CRUD.
 *
 * RonDB-native: uses catalogue API for all operations.
 * Tests skip gracefully if no RonDB cluster is available.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "pnfs_mds.h"
#include "mds_catalogue.h"
#include "quota.h"
#include "test_helpers.h"

static int tests_run, tests_passed;
static int test_failed;   /* Set by ASSERT_* so RUN_TEST records it. */

#define ASSERT_EQ(a, b) do { \
    if ((a) != (b)) { \
        fprintf(stderr, "  FAIL %s:%d: %s (%d) != %s (%d)\n", \
                __FILE__, __LINE__, #a, (int)(a), #b, (int)(b)); \
        test_failed = 1; \
        return; \
    } \
} while (0)

#define ASSERT_TRUE(cond) do { \
    if (!(cond)) { \
        fprintf(stderr, "  FAIL %s:%d: !(%s)\n", __FILE__, __LINE__, #cond); \
        test_failed = 1; \
        return; \
    } \
} while (0)

/* Skipped tests count as passed via RUN_TEST (no failure recorded). */
#define SKIP(msg) do { \
    fprintf(stdout, "SKIP (%s)\n", (msg)); return; \
} while (0)

/* A failed assertion must fail the suite: RUN_TEST previously
 * counted every test as passed because the ASSERT_* macros only
 * printed and returned, which let stale assertions rot silently. */
#define RUN_TEST(fn) do { \
    tests_run++; fprintf(stdout, "  %-50s", #fn); fflush(stdout); \
    test_failed = 0; \
    fn(); \
    if (test_failed == 0) { \
        tests_passed++; fprintf(stdout, "PASS\n"); \
    } else { \
        fprintf(stdout, "FAILED\n"); \
    } \
} while (0)

static void test_quota_rule_crud(void)
{
    struct mds_catalogue *cat = open_test_catalogue();
    if (cat == NULL) SKIP("no RonDB");

    struct mds_quota_rule rule;
    memset(&rule, 0, sizeof(rule));

    /* Get non-existent rule -> NOTFOUND. */
    enum mds_status st = mds_cat_quota_rule_get(cat, 0, 1000, &rule);
    ASSERT_EQ(st, MDS_ERR_NOTFOUND);

    mds_catalogue_close(cat);
}

static void test_quota_usage_crud(void)
{
    struct mds_catalogue *cat = open_test_catalogue();
    if (cat == NULL) SKIP("no RonDB");

    struct mds_quota_usage usage;
    memset(&usage, 0, sizeof(usage));

    /* Get non-existent usage -> NOTFOUND. */
    enum mds_status st = mds_cat_quota_usage_get(cat, 0, 1000, &usage);
    ASSERT_EQ(st, MDS_ERR_NOTFOUND);

    mds_catalogue_close(cat);
}

int main(void)
{
    fprintf(stdout, "test_quota (RonDB-native)\n");

    RUN_TEST(test_quota_rule_crud);
    RUN_TEST(test_quota_usage_crud);

    fprintf(stdout, "\n  %d/%d tests passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
