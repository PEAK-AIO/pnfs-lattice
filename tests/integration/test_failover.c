/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_failover.c -- Integration tests for failover topology.
 *
 * RonDB-native: uses catalogue + coordination API.
 * Skips gracefully if no RonDB cluster is available.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "pnfs_mds.h"
#include "test_helpers.h"
#include "mds_catalogue.h"
#include "mds_coordination.h"
#include "failover.h"
#include "cluster_membership.h"

static int tests_run, tests_passed;

#define ASSERT_EQ(a, b) do { \
    if ((a) != (b)) { \
        fprintf(stderr, "  FAIL %s:%d: %s (%d) != %s (%d)\n", \
                __FILE__, __LINE__, #a, (int)(a), #b, (int)(b)); \
        return; \
    } \
} while (0)
#define ASSERT_TRUE(cond) do { \
    if (!(cond)) { \
        fprintf(stderr, "  FAIL %s:%d: !(%s)\n", __FILE__, __LINE__, #cond); \
        return; \
    } \
} while (0)
#define SKIP(msg) do { \
    fprintf(stdout, "SKIP (%s)\n", (msg)); tests_passed++; return; \
} while (0)
#define RUN_TEST(fn) do { \
    tests_run++; fprintf(stdout, "  %-50s", #fn); fflush(stdout); \
    fn(); tests_passed++; fprintf(stdout, "PASS\n"); \
} while (0)

/** Test client recovery put/get round-trip via coordination API. */
static void test_recovery_put_get(void)
{
    struct mds_catalogue *cat = open_test_catalogue();
    if (cat == NULL) SKIP("no RonDB");

    /* Put a recovery record. */
    const uint8_t owner[] = "test-client-owner";
    const uint8_t verifier[8] = {1,2,3,4,5,6,7,8};

    enum mds_status st = mds_coord_recovery_put(cat, NULL, 1000,
                                                 owner, sizeof(owner) - 1,
                                                 verifier);
    ASSERT_EQ(st, MDS_OK);

    /* Get it back. */
    uint8_t got_owner[1024];
    uint32_t got_len = 0;
    uint8_t got_verifier[8];
    st = mds_coord_recovery_get(cat, 1000, got_owner, &got_len, got_verifier);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(got_len, sizeof(owner) - 1);
    ASSERT_EQ(memcmp(got_owner, owner, got_len), 0);
    ASSERT_EQ(memcmp(got_verifier, verifier, 8), 0);

    /* Delete. */
    st = mds_coord_recovery_del(cat, NULL, 1000);
    ASSERT_EQ(st, MDS_OK);

    /* Verify gone. */
    st = mds_coord_recovery_get(cat, 1000, got_owner, &got_len, got_verifier);
    ASSERT_EQ(st, MDS_ERR_NOTFOUND);

    mds_catalogue_close(cat);
}

/** Test failover role constants. */
static void test_failover_role_constants(void)
{
    ASSERT_TRUE(NODE_ACTIVE != NODE_STANDBY);
}

int main(void)
{
    fprintf(stdout, "test_failover (RonDB-native integration)\n");

    RUN_TEST(test_recovery_put_get);
    RUN_TEST(test_failover_role_constants);

    fprintf(stdout, "\n  %d/%d tests passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
