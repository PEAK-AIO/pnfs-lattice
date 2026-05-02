/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_coord_shared_state.c — Unit tests for shared protocol-state
 *                              coordination API (shared-attr Stage 1+).
 *
 * Stage 1: verifies dispatch stubs return MDS_ERR_NOSUPPORT when
 * the vtable entries are NULL.
 * Stage 2+: tests will be extended as real implementations are wired.
 */

#include <stdio.h>
#include <string.h>

#include "mds_catalogue.h"
#include "mds_coordination.h"
#include "catalogue_internal.h"

/* ----------------------------------------------------------------------- */

static int tests_run    = 0;
static int tests_passed = 0;

#define ASSERT_EQ(a, b) do { \
    if ((a) != (b)) { \
        fprintf(stderr, "  FAIL %s:%d: %s (%d) != %s (%d)\n", \
            __FILE__, __LINE__, #a, (int)(a), #b, (int)(b)); \
        return; \
    } \
} while (0)

#define RUN_TEST(fn) do { \
    fprintf(stdout, "  %-55s", #fn); \
    tests_run++; \
    fn(); \
    tests_passed++; \
    fprintf(stdout, "OK\n"); \
} while (0)

/* ----------------------------------------------------------------------- */

/**
 * Build a minimal catalogue handle with coord_ops that have all
 * shared-state pointers set to NULL (Stage 1 state).
 */
static struct mds_coordination_ops null_coord_ops;
static struct mds_catalogue_ops    null_cat_ops;

static struct mds_catalogue make_test_cat(void)
{
    struct mds_catalogue cat;
    memset(&cat, 0, sizeof(cat));
    memset(&null_coord_ops, 0, sizeof(null_coord_ops));
    memset(&null_cat_ops, 0, sizeof(null_cat_ops));
    cat.coord_ops = &null_coord_ops;
    cat.ops = &null_cat_ops;
    cat.backend = MDS_BACKEND_RONDB;
    return cat;
}

/* --- Open/share state ------------------------------------------------- */

static void test_open_put_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    struct mds_coord_open_row row;
    memset(&row, 0, sizeof(row));
    ASSERT_EQ(mds_coord_open_put(&cat, &row), MDS_ERR_NOSUPPORT);
}

static void test_open_get_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    struct mds_coord_open_row row;
    uint8_t sid[12] = {0};
    ASSERT_EQ(mds_coord_open_get(&cat, sid, &row), MDS_ERR_NOSUPPORT);
}

static void test_open_del_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    uint8_t sid[12] = {0};
    ASSERT_EQ(mds_coord_open_del(&cat, sid), MDS_ERR_NOSUPPORT);
}

static void test_open_scan_file_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    ASSERT_EQ(mds_coord_open_scan_file(&cat, 42, NULL, NULL),
              MDS_ERR_NOSUPPORT);
}

static void test_open_scan_client_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    ASSERT_EQ(mds_coord_open_scan_client(&cat, 1, NULL, NULL),
              MDS_ERR_NOSUPPORT);
}

/* --- Byte-range locks ------------------------------------------------- */

static void test_lock_put_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    struct mds_coord_lock_row row;
    memset(&row, 0, sizeof(row));
    ASSERT_EQ(mds_coord_lock_put(&cat, &row), MDS_ERR_NOSUPPORT);
}

static void test_lock_del_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    ASSERT_EQ(mds_coord_lock_del(&cat, 42, 1), MDS_ERR_NOSUPPORT);
}

static void test_lock_test_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    struct mds_coord_lock_row conflict;
    ASSERT_EQ(mds_coord_lock_test(&cat, 42, 1, 0, 100, 1, NULL, 0,
                                   &conflict), MDS_ERR_NOSUPPORT);
}

static void test_lock_scan_file_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    ASSERT_EQ(mds_coord_lock_scan_file(&cat, 42, NULL, NULL),
              MDS_ERR_NOSUPPORT);
}

static void test_lock_reap_client_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    ASSERT_EQ(mds_coord_lock_reap_client(&cat, 1), MDS_ERR_NOSUPPORT);
}

/* --- Delegations ------------------------------------------------------ */

static void test_deleg_put_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    struct mds_coord_deleg_row row;
    memset(&row, 0, sizeof(row));
    ASSERT_EQ(mds_coord_deleg_put(&cat, &row), MDS_ERR_NOSUPPORT);
}

static void test_deleg_get_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    struct mds_coord_deleg_row row;
    uint8_t sid[12] = {0};
    ASSERT_EQ(mds_coord_deleg_get(&cat, sid, &row), MDS_ERR_NOSUPPORT);
}

static void test_deleg_del_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    uint8_t sid[12] = {0};
    ASSERT_EQ(mds_coord_deleg_del(&cat, sid), MDS_ERR_NOSUPPORT);
}

/* --- Client identity -------------------------------------------------- */

static void test_client_put_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    struct mds_coord_client_row row;
    memset(&row, 0, sizeof(row));
    ASSERT_EQ(mds_coord_client_put(&cat, &row), MDS_ERR_NOSUPPORT);
}

static void test_client_get_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    struct mds_coord_client_row row;
    ASSERT_EQ(mds_coord_client_get(&cat, 1, &row), MDS_ERR_NOSUPPORT);
}

static void test_client_del_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    ASSERT_EQ(mds_coord_client_del(&cat, 1), MDS_ERR_NOSUPPORT);
}

/* --- Sessions --------------------------------------------------------- */

static void test_session_put_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    struct mds_coord_session_row row;
    memset(&row, 0, sizeof(row));
    ASSERT_EQ(mds_coord_session_put(&cat, &row), MDS_ERR_NOSUPPORT);
}

static void test_session_get_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    struct mds_coord_session_row row;
    uint8_t sid[16] = {0};
    ASSERT_EQ(mds_coord_session_get(&cat, sid, &row), MDS_ERR_NOSUPPORT);
}

static void test_session_del_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    uint8_t sid[16] = {0};
    ASSERT_EQ(mds_coord_session_del(&cat, sid), MDS_ERR_NOSUPPORT);
}

/* --- DRC slots -------------------------------------------------------- */

static void test_slot_put_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    uint8_t sid[16] = {0};
    ASSERT_EQ(mds_coord_slot_put(&cat, sid, 0, 1, NULL, 0),
              MDS_ERR_NOSUPPORT);
}

static void test_slot_get_nosupport(void)
{
    struct mds_catalogue cat = make_test_cat();
    struct mds_coord_drc_slot_row row;
    uint8_t sid[16] = {0};
    ASSERT_EQ(mds_coord_slot_get(&cat, sid, 0, &row),
              MDS_ERR_NOSUPPORT);
}

/* --- NULL catalogue safety -------------------------------------------- */

static void test_null_cat_returns_nosupport(void)
{
    struct mds_coord_open_row orow;
    memset(&orow, 0, sizeof(orow));
    ASSERT_EQ(mds_coord_open_put(NULL, &orow), MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_coord_lock_put(NULL, NULL), MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_coord_deleg_put(NULL, NULL), MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_coord_client_put(NULL, NULL), MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_coord_session_put(NULL, NULL), MDS_ERR_NOSUPPORT);
    ASSERT_EQ(mds_coord_slot_put(NULL, NULL, 0, 0, NULL, 0),
              MDS_ERR_NOSUPPORT);
}

/* ----------------------------------------------------------------------- */

int main(void)
{
    fprintf(stdout, "test_coord_shared_state\n");

    /* Open/share state */
    RUN_TEST(test_open_put_nosupport);
    RUN_TEST(test_open_get_nosupport);
    RUN_TEST(test_open_del_nosupport);
    RUN_TEST(test_open_scan_file_nosupport);
    RUN_TEST(test_open_scan_client_nosupport);

    /* Byte-range locks */
    RUN_TEST(test_lock_put_nosupport);
    RUN_TEST(test_lock_del_nosupport);
    RUN_TEST(test_lock_test_nosupport);
    RUN_TEST(test_lock_scan_file_nosupport);
    RUN_TEST(test_lock_reap_client_nosupport);

    /* Delegations */
    RUN_TEST(test_deleg_put_nosupport);
    RUN_TEST(test_deleg_get_nosupport);
    RUN_TEST(test_deleg_del_nosupport);

    /* Client identity */
    RUN_TEST(test_client_put_nosupport);
    RUN_TEST(test_client_get_nosupport);
    RUN_TEST(test_client_del_nosupport);

    /* Sessions */
    RUN_TEST(test_session_put_nosupport);
    RUN_TEST(test_session_get_nosupport);
    RUN_TEST(test_session_del_nosupport);

    /* DRC slots */
    RUN_TEST(test_slot_put_nosupport);
    RUN_TEST(test_slot_get_nosupport);

    /* NULL safety */
    RUN_TEST(test_null_cat_returns_nosupport);

    fprintf(stdout, "\n  %d/%d tests passed\n",
        tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
