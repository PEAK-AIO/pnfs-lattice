/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_backup.c — Verify backup API stubs return MDS_ERR_NOSUPPORT.
 *
 * The backup module relied on the legacy backend's backend copy and has
 * not been ported to the catalogue API.  All four public entry points are
 * stubbed to return MDS_ERR_NOSUPPORT.  These tests confirm that contract
 * so callers get a clear, documented error.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "backup.h"
#include "mds_catalogue.h"
#include "test_helpers.h"
#include "pnfs_mds.h"

/* -----------------------------------------------------------------------
 * Minimal test harness
 * ----------------------------------------------------------------------- */

static int tests_run    = 0;
static int tests_passed = 0;

#define RUN_TEST(fn) do {                                               \
    tests_run++;                                                        \
    printf("  %-48s ", #fn);                                            \
    fn();                                                               \
    tests_passed++;                                                     \
    printf("PASS\n");                                                   \
} while (0)

#define ASSERT_EQ(a, b) do {                                            \
    if ((a) != (b)) {                                                   \
        fprintf(stderr, "FAIL: %s:%d: %s (%d) != %s (%d)\n",           \
                __FILE__, __LINE__, #a, (int)(a), #b, (int)(b));        \
        abort();                                                        \
    }                                                                   \
} while (0)

#define ASSERT_TRUE(x)  ASSERT_EQ(!!(x), 1)

/* -----------------------------------------------------------------------
 * Tests — all backup APIs must return MDS_ERR_NOSUPPORT
 * ----------------------------------------------------------------------- */

static void test_create_returns_nosupport(void)
{
    struct mds_catalogue *cat = open_test_catalogue();
    ASSERT_TRUE(cat != NULL);

    struct mds_backup_report rpt;
    memset(&rpt, 0, sizeof(rpt));

    ASSERT_EQ(mds_backup_create(cat, "/tmp/test_bak", 0, &rpt),
              MDS_ERR_NOSUPPORT);

    /* Also confirm NULL report is accepted without crash. */
    ASSERT_EQ(mds_backup_create(cat, "/tmp/test_bak", 0, NULL),
              MDS_ERR_NOSUPPORT);

    mds_catalogue_close(cat);
}

static void test_verify_returns_nosupport(void)
{
    struct mds_backup_report rpt;
    memset(&rpt, 0, sizeof(rpt));

    ASSERT_EQ(mds_backup_verify("/tmp/nonexistent.bak", &rpt),
              MDS_ERR_NOSUPPORT);
}

static void test_restore_returns_nosupport(void)
{
    ASSERT_EQ(mds_backup_restore("/tmp/nonexistent.bak",
                                 "/tmp/dest", 0),
              MDS_ERR_NOSUPPORT);
}

static void test_info_returns_nosupport(void)
{
    struct mds_backup_report rpt;
    memset(&rpt, 0, sizeof(rpt));

    ASSERT_EQ(mds_backup_info("/tmp/nonexistent.bak", &rpt),
              MDS_ERR_NOSUPPORT);
}

static void test_create_with_flags_returns_nosupport(void)
{
    struct mds_catalogue *cat = open_test_catalogue();
    ASSERT_TRUE(cat != NULL);

    ASSERT_EQ(mds_backup_create(cat, "/tmp/test_bak",
              MDS_BACKUP_COMPACT | MDS_BACKUP_FSYNC_BEFORE_COPY, NULL),
              MDS_ERR_NOSUPPORT);

    mds_catalogue_close(cat);
}

static void test_restore_with_overwrite_returns_nosupport(void)
{
    ASSERT_EQ(mds_backup_restore("/tmp/a.bak", "/tmp/b",
              MDS_BACKUP_RESTORE_OVERWRITE),
              MDS_ERR_NOSUPPORT);
}

/* -----------------------------------------------------------------------
 * main
 * ----------------------------------------------------------------------- */

int main(void)
{
    fprintf(stdout, "Running backup tests (RonDB-native stubs):\n");

    RUN_TEST(test_create_returns_nosupport);
    RUN_TEST(test_verify_returns_nosupport);
    RUN_TEST(test_restore_returns_nosupport);
    RUN_TEST(test_info_returns_nosupport);
    RUN_TEST(test_create_with_flags_returns_nosupport);
    RUN_TEST(test_restore_with_overwrite_returns_nosupport);

    fprintf(stdout, "\n%d/%d backup tests passed.\n",
            tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
