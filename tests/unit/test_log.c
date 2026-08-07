/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_log.c -- Unit tests for the leveled logging subsystem
 * (src/common/log.c): the string->enum config helpers and the
 * per-component level filtering in mds_log().
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "pnfs_mds.h"

static int tests_run;
static int tests_passed;
static int test_failed;   /* Set by ASSERT_* so RUN_TEST records it. */

#define ASSERT_TRUE(cond) do { \
    if (!(cond)) { \
        fprintf(stderr, "  FAIL %s:%d: !(%s)\n", \
                __FILE__, __LINE__, #cond); \
        test_failed = 1; \
        return; \
    } \
} while (0)

/* A failed assertion must fail the suite: RUN_TEST previously
 * counted every test as passed because the ASSERT_* macros only
 * printed and returned, which let stale assertions rot silently. */
#define RUN_TEST(fn) do { \
    tests_run++; \
    test_failed = 0; \
    fprintf(stdout, "  %-50s", #fn); \
    fflush(stdout); \
    fn(); \
    if (test_failed == 0) { \
        tests_passed++; \
        fprintf(stdout, "PASS\n"); \
    } else { \
        fprintf(stdout, "FAILED\n"); \
    } \
} while (0)

/* ------------------------------------------------------------------- */

static void test_level_from_str(void)
{
    /* Case-insensitive token match for every level. */
    ASSERT_TRUE(mds_log_level_from_str("fatal") == LOG_FATAL);
    ASSERT_TRUE(mds_log_level_from_str("ERROR") == LOG_ERROR);
    ASSERT_TRUE(mds_log_level_from_str("Warn")  == LOG_WARN);
    ASSERT_TRUE(mds_log_level_from_str("info")  == LOG_INFO);
    ASSERT_TRUE(mds_log_level_from_str("debug") == LOG_DEBUG);
    ASSERT_TRUE(mds_log_level_from_str("trace") == LOG_TRACE);
    /* Unknown / NULL -> -1. */
    ASSERT_TRUE(mds_log_level_from_str("bogus") == -1);
    ASSERT_TRUE(mds_log_level_from_str("") == -1);
    ASSERT_TRUE(mds_log_level_from_str(NULL) == -1);
}

static void test_component_from_str(void)
{
    ASSERT_TRUE(mds_log_component_from_str("mds")     == LOG_COMP_MDS);
    ASSERT_TRUE(mds_log_component_from_str("fsal")    == LOG_COMP_FSAL);
    ASSERT_TRUE(mds_log_component_from_str("CLUSTER") == LOG_COMP_CLUSTER);
    ASSERT_TRUE(mds_log_component_from_str("repl")    == LOG_COMP_REPL);
    ASSERT_TRUE(mds_log_component_from_str("Cat")     == LOG_COMP_CAT);
    ASSERT_TRUE(mds_log_component_from_str("bpf")     == LOG_COMP_BPF);
    ASSERT_TRUE(mds_log_component_from_str("nfs")     == LOG_COMP_NFS);
    ASSERT_TRUE(mds_log_component_from_str("bogus")   == -1);
    ASSERT_TRUE(mds_log_component_from_str(NULL)      == -1);
}

/* Count newline-terminated records in a file (-1 if unreadable). */
static int count_lines(const char *path)
{
    FILE *f = fopen(path, "r");
    if (f == NULL) {
        return -1;
    }
    int lines = 0;
    int c;
    while ((c = fgetc(f)) != EOF) {
        if (c == '\n') {
            lines++;
        }
    }
    (void)fclose(f);
    return lines;
}

static void test_level_filtering(void)
{
    char path[128];
    (void)snprintf(path, sizeof(path), "/tmp/pnfs-log-%d.log",
                   (int)getpid());
    (void)unlink(path);

    mds_log_init(path);
    /* Restrict CAT to WARN: fatal/error/warn pass; info/debug/trace drop. */
    mds_log_set_level(LOG_COMP_CAT, LOG_WARN);

    mds_log(LOG_COMP_CAT, LOG_INFO,  "info should be dropped %d", 1);
    mds_log(LOG_COMP_CAT, LOG_WARN,  "warn should appear %d", 2);
    mds_log(LOG_COMP_CAT, LOG_ERROR, "error should appear %d", 3);
    mds_log(LOG_COMP_CAT, LOG_DEBUG, "debug should be dropped %d", 4);

    mds_log_shutdown();

    /* Exactly two records survive the WARN threshold. */
    ASSERT_TRUE(count_lines(path) == 2);
    (void)unlink(path);
}

static void test_invalid_component_is_dropped(void)
{
    char path[128];
    (void)snprintf(path, sizeof(path), "/tmp/pnfs-log-bad-%d.log",
                   (int)getpid());
    (void)unlink(path);

    mds_log_init(path);
    /* Out-of-range component must not write or crash. */
    mds_log(-1, LOG_FATAL, "must not appear");
    mds_log(LOG_COMP_COUNT, LOG_FATAL, "must not appear");
    mds_log_shutdown();

    ASSERT_TRUE(count_lines(path) == 0);
    (void)unlink(path);
}

int main(void)
{
    fprintf(stdout, "test_log\n");

    RUN_TEST(test_level_from_str);
    RUN_TEST(test_component_from_str);
    RUN_TEST(test_level_filtering);
    RUN_TEST(test_invalid_component_is_dropped);

    fprintf(stdout, "\n  %d/%d tests passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
