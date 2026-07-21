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

static void test_async_remove_metrics(void)
{
    static char buf[256 * 1024];
    struct mds_metrics_snapshot snapshot;

    fprintf(stdout, "  test_async_remove_metrics:         ");
    memset(&snapshot, 0, sizeof(snapshot));
    atomic_store(&g_branch_metrics.gc_pending, 12);
    atomic_store(&g_branch_metrics.gc_claimed, 3);
    atomic_store(&g_branch_metrics.gc_oldest_age_seconds, 7);
    atomic_store(&g_branch_metrics.gc_retries_total, 4);
    atomic_store(&g_branch_metrics.gc_open_blocked_total, 5);
    atomic_store(&g_branch_metrics.gc_deferred_quota_bytes, 1024);
    atomic_store(&g_branch_metrics.remove_async_backpressure_active, 1);
    ASSERT_TRUE(mds_metrics_prometheus_v2(
                    &snapshot, &g_branch_metrics, buf, sizeof(buf)) > 0);
    ASSERT_TRUE(strstr(buf, "pnfs_mds_gc_pending 12") != NULL);
    ASSERT_TRUE(strstr(buf, "pnfs_mds_gc_claimed 3") != NULL);
    ASSERT_TRUE(strstr(buf, "pnfs_mds_gc_retries_total 4") != NULL);
    ASSERT_TRUE(strstr(buf, "pnfs_mds_gc_open_blocked_total 5") != NULL);
    ASSERT_TRUE(strstr(buf, "pnfs_mds_gc_deferred_quota_bytes 1024") != NULL);
    ASSERT_TRUE(strstr(
                    buf, "pnfs_mds_remove_async_backpressure_active 1") !=
                NULL);

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

int main(void)
{
    fprintf(stdout, "test_mds_metrics:\n");

    test_metrics_reset();
    test_metrics_increment();
    test_metrics_prometheus();
    test_metrics_prometheus_truncation();
    test_async_remove_metrics();

    fprintf(stdout, "\n  %d passed, %d failed\n", passed, failed);
    return failed > 0 ? 1 : 0;
}
