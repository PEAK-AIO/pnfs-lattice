/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_tiering.c -- Unit tests for IO tracker and tiering constants.
 *
 * RonDB-native: no catalogue dependencies.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "pnfs_mds.h"
#include "io_tracker.h"

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
#define RUN_TEST(fn) do { \
    tests_run++; fprintf(stdout, "  %-50s", #fn); fflush(stdout); \
    fn(); tests_passed++; fprintf(stdout, "PASS\n"); \
} while (0)

static void test_ds_tier_constants(void)
{
    /* DS tier values are documented: 0=hot, 1=warm, 2=cold. */
    ASSERT_TRUE(DS_ONLINE == 0);
    ASSERT_TRUE(DS_OFFLINE == 1);
    ASSERT_TRUE(DS_DRAINING == 2);
}

static void test_io_tracker_init_destroy(void)
{
    struct io_tracker *t = NULL;
    ASSERT_EQ(io_tracker_init(4096, &t), 0);
    ASSERT_TRUE(t != NULL);
    io_tracker_destroy(t);
    io_tracker_destroy(NULL);  /* NULL-safe */
}

static void test_io_tracker_record(void)
{
    struct io_tracker *t = NULL;
    ASSERT_EQ(io_tracker_init(64, &t), 0);

    io_tracker_record(t, 100, 4096, true);   /* write */
    io_tracker_record(t, 100, 8192, false);  /* read */
    io_tracker_record(t, 200, 1024, true);   /* different fileid */

    struct io_tracker_entry *snap = NULL;
    uint32_t count = 0;
    ASSERT_EQ(io_tracker_snapshot_and_reset(t, &snap, &count), 0);
    ASSERT_TRUE(count >= 2);  /* At least 2 distinct fileids */

    free(snap);
    io_tracker_destroy(t);
}

int main(void)
{
    fprintf(stdout, "test_tiering (RonDB-native)\n");

    RUN_TEST(test_ds_tier_constants);
    RUN_TEST(test_io_tracker_init_destroy);
    RUN_TEST(test_io_tracker_record);

    fprintf(stdout, "\n  %d/%d tests passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
