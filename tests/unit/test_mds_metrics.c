/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_mds_metrics.c -- Unit tests for metrics registry.
 */

#include <stdio.h>
#include <string.h>
#include <stdatomic.h>

#include "mds_metrics.h"
#include "mds_op_metrics.h"

static int passed = 0;
static int failed = 0;

#define ASSERT_TRUE(cond) do { \
    if (!(cond)) { \
        fprintf(stderr, "  FAIL %s:%d: %s\n", \
                __FILE__, __LINE__, #cond); \
        failed++; return; \
    } \
} while (0)

#define ASSERT_EQ(a, b) ASSERT_TRUE((a) == (b))

static void test_metrics_reset(void)
{
    fprintf(stdout, "  test_metrics_reset:                ");

    atomic_fetch_add(&g_metrics.cat_commits_ok, 10);
    mds_metrics_reset();

    struct mds_metrics_snapshot s = mds_metrics_snapshot();
    ASSERT_EQ(s.cat_commits_ok, 0);
    ASSERT_EQ(s.repl_deltas_sent, 0);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_metrics_increment(void)
{
    fprintf(stdout, "  test_metrics_increment:            ");

    mds_metrics_reset();
    atomic_fetch_add(&g_metrics.repl_deltas_sent, 5);
    atomic_fetch_add(&g_metrics.repl_bytes_sent, 1024);
    atomic_fetch_add(&g_metrics.cat_commits_ok, 3);
    atomic_fetch_add(&g_metrics.cat_commits_fail, 1);

    struct mds_metrics_snapshot s = mds_metrics_snapshot();
    ASSERT_EQ(s.repl_deltas_sent, 5);
    ASSERT_EQ(s.repl_bytes_sent, 1024);
    ASSERT_EQ(s.cat_commits_ok, 3);
    ASSERT_EQ(s.cat_commits_fail, 1);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_metrics_prometheus(void)
{
    fprintf(stdout, "  test_metrics_prometheus:           ");

    mds_metrics_reset();
    atomic_fetch_add(&g_metrics.cat_commits_ok, 42);

    struct mds_metrics_snapshot s = mds_metrics_snapshot();
    s.repl_health_ok = 1;

    char buf[4096];
    int n = mds_metrics_prometheus(&s, buf, sizeof(buf));
    ASSERT_TRUE(n > 0);
    ASSERT_TRUE(strstr(buf, "pnfs_mds_cat_commits_ok 42") != NULL);
    ASSERT_TRUE(strstr(buf, "pnfs_mds_repl_health_ok 1") != NULL);
    ASSERT_TRUE(strstr(buf, "# TYPE") != NULL);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_metrics_prometheus_truncation(void)
{
    fprintf(stdout, "  test_metrics_prometheus_truncate:  ");

    struct mds_metrics_snapshot s;
    memset(&s, 0, sizeof(s));

    char buf[10];
    int n = mds_metrics_prometheus(&s, buf, sizeof(buf));
    ASSERT_EQ(n, -1);

    fprintf(stdout, "PASS\n");
    passed++;
}

/* Wave 6: the branch (v2) render must expose the decision
 * instrumentation counters the lab reads via mds-metrics-diff. */
static void test_metrics_prometheus_v2_wave6(void)
{
    fprintf(stdout, "  test_metrics_prometheus_v2_wave6:  ");

    /* The v2 render includes the always-on per-op and per-cat-op
     * histogram families (~50 KB with zero observations), so the
     * buffer must be sized like the real scrape consumer's. */
    struct mds_metrics_snapshot s;
    static char buf[131072];
    int n;

    memset(&s, 0, sizeof(s));
    atomic_fetch_add(&g_branch_metrics.cat_transient_retries, 3);
    atomic_fetch_add(&g_branch_metrics.cat_transient_backoff_us, 1500);
    atomic_fetch_add(&g_branch_metrics.cat_transient_retry_exhausted, 1);

    n = mds_metrics_prometheus_v2(&s, &g_branch_metrics,
                                  buf, sizeof(buf));
    ASSERT_TRUE(n > 0);
    ASSERT_TRUE(strstr(buf, "pnfs_mds_cat_transient_retries 3") != NULL);
    ASSERT_TRUE(strstr(buf,
        "pnfs_mds_cat_transient_backoff_us 1500") != NULL);
    ASSERT_TRUE(strstr(buf,
        "pnfs_mds_cat_transient_retry_exhausted 1") != NULL);

    fprintf(stdout, "PASS\n");
    passed++;
}

/* Every op/cat-op enum entry must have a name-table row: designated
 * initializers silently leave gaps as NULL, which would put "(null)"
 * into /metrics label values.  Also pins the Wave 6 addition. */
static void test_op_metrics_name_tables_complete(void)
{
    fprintf(stdout, "  test_op_metrics_name_tables:       ");

    for (int i = 0; i < (int)MDS_OPC__COUNT; i++) {
        ASSERT_TRUE(mds_op_class_name((enum mds_op_class)i) != NULL);
    }
    for (int i = 0; i < (int)MDS_CATOP__COUNT; i++) {
        ASSERT_TRUE(mds_cat_op_name((enum mds_cat_op)i) != NULL);
    }
    ASSERT_TRUE(strcmp(mds_cat_op_name(MDS_CATOP_UNLINK_RECALL),
                       "unlink_recall") == 0);

    fprintf(stdout, "PASS\n");
    passed++;
}

int main(void)
{
    fprintf(stdout, "test_mds_metrics:\n");

    test_metrics_reset();
    test_metrics_increment();
    test_metrics_prometheus();
    test_metrics_prometheus_truncation();
    test_metrics_prometheus_v2_wave6();
    test_op_metrics_name_tables_complete();

    fprintf(stdout, "\n  %d passed, %d failed\n", passed, failed);
    return failed > 0 ? 1 : 0;
}
