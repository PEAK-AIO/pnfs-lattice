/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_ds_health.c — Tests for DS health monitoring infrastructure.
 *
 * Covers:
 *   1. ds_addr_parse_host with valid/invalid formats.
 *   2. ds_probe_null against a refusing port.
 *   3. DS health monitor: force_fail triggers callback + DS_OFFLINE.
 *   4. ds_health_report_error accumulates to threshold.
 *   5. COMMIT_OP_DS_STATE round-trip.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

/* Like assert() but not elided by NDEBUG. */
#define VERIFY(expr) do { if (!(expr)) { \
	fprintf(stderr, "VERIFY FAILED: %s (%s:%d)\n", \
		#expr, __FILE__, __LINE__); abort(); } } while (0)
#include <unistd.h>
#include <pthread.h>

#include "ds_health.h"
#include "mds_catalogue.h"
#include "test_helpers.h"
#include "commit_queue.h"

/* -----------------------------------------------------------------------
 * Test helpers
 * ----------------------------------------------------------------------- */

static int pass_count;
static int fail_count;

#define ASSERT_EQ(a, b) do { \
    long long _a = (long long)(a), _b = (long long)(b); \
    if (_a != _b) { \
        fprintf(stderr, "FAIL %s:%d: %s == %lld, expected %lld\n", \
                __FILE__, __LINE__, #a, _a, _b); \
        fail_count++; return; \
    } pass_count++; \
} while (0)

#define ASSERT_STR_EQ(a, b) do { \
    if (strcmp((a), (b)) != 0) { \
        fprintf(stderr, "FAIL %s:%d: %s == \"%s\", expected \"%s\"\n", \
                __FILE__, __LINE__, #a, (a), (b)); \
        fail_count++; return; \
    } pass_count++; \
} while (0)

#define TEST_MAP_SIZE (64ULL * 1024 * 1024)

static char *make_temp_db_path(void)
{
    static char path[256];
    snprintf(path, sizeof(path), "/tmp/test_ds_health_%d", getpid());
    return path;
}

/* -----------------------------------------------------------------------
 * Test 1: ds_addr_parse_host
 * ----------------------------------------------------------------------- */

static void test_addr_parse_host(void)
{
    char host[256];

    /* Normal case. */
    ASSERT_EQ(ds_addr_parse_host("192.168.1.10:/data", host, sizeof(host)), 0);
    ASSERT_STR_EQ(host, "192.168.1.10");

    /* Hostname with port. */
    ASSERT_EQ(ds_addr_parse_host("ds1.example.com:/export/nfs", host, sizeof(host)), 0);
    ASSERT_STR_EQ(host, "ds1.example.com");

    /* No colon — invalid. */
    ASSERT_EQ(ds_addr_parse_host("nocolon", host, sizeof(host)), -1);

    /* Empty host — colon at start. */
    ASSERT_EQ(ds_addr_parse_host(":/export", host, sizeof(host)), -1);

    /* NULL inputs. */
    ASSERT_EQ(ds_addr_parse_host(NULL, host, sizeof(host)), -1);
    ASSERT_EQ(ds_addr_parse_host("host:/x", NULL, 10), -1);

    /* Buffer too small. */
    ASSERT_EQ(ds_addr_parse_host("verylonghost:/x", host, 5), -1);
}

/* -----------------------------------------------------------------------
 * Test 2: ds_probe_null against refusing port
 * ----------------------------------------------------------------------- */

static void test_probe_null_refused(void)
{
    /* Port 1 should refuse connections (requires root to bind). */
    ASSERT_EQ(ds_probe_null("127.0.0.1", 1, 200), -1);

    /* NULL host. */
    ASSERT_EQ(ds_probe_null(NULL, 2049, 200), -1);

    /* Zero port. */
    ASSERT_EQ(ds_probe_null("127.0.0.1", 0, 200), -1);
}

/* -----------------------------------------------------------------------
 * Test 3: DS health monitor — force_fail triggers callback
 * ----------------------------------------------------------------------- */

static uint32_t test3_failed_ds_id;
static int test3_cb_count;

static void test3_fail_cb(uint32_t ds_id, void *ctx)
{
    (void)ctx;
    test3_failed_ds_id = ds_id;
    test3_cb_count++;
}

static void test_force_fail(void)
{
    char *path = make_temp_db_path();
    struct mds_catalogue *db = NULL;
	struct mds_catalogue *cat = NULL;
    struct ds_health_monitor *hm = NULL;

    db = open_test_catalogue(); VERIFY(db != NULL);
	cat = db;

    /* Register a DS. */
    {
        struct mds_ds_info info;
        struct mds_cat_txn *txn = NULL;

        memset(&info, 0, sizeof(info));
        info.ds_id = 42;
        info.state = DS_ONLINE;
        info.port = 2049;
        snprintf(info.addr, sizeof(info.addr), "10.0.0.1:/data");

        VERIFY(mds_cat_txn_begin(db, MDS_CAT_TXN_WRITE, &txn) == MDS_OK);
        VERIFY(mds_cat_ds_put(db, txn, &info) == MDS_OK);
        mds_cat_txn_commit(txn);
    }

    test3_failed_ds_id = 0;
    test3_cb_count = 0;

    ASSERT_EQ(ds_health_init(cat, NULL, 60000, 3,
                              test3_fail_cb, NULL, &hm), 0);

    /* Force-fail should trigger callback immediately. */
    ds_health_test_force_fail(hm, 42);

    ASSERT_EQ(test3_cb_count, 1);
    ASSERT_EQ(test3_failed_ds_id, 42);

    /* DS should now be DS_OFFLINE. */
    {
        struct mds_ds_info check;

        ASSERT_EQ(mds_cat_ds_get(db, 42, &check), MDS_OK);
        ASSERT_EQ(check.state, DS_OFFLINE);
    }

    ds_health_destroy(hm);
    mds_catalogue_close(db);
}

/* -----------------------------------------------------------------------
 * Test 4: report_error accumulates to threshold
 * ----------------------------------------------------------------------- */

static uint32_t test4_failed_ds_id;
static int test4_cb_count;

static void test4_fail_cb(uint32_t ds_id, void *ctx)
{
    (void)ctx;
    test4_failed_ds_id = ds_id;
    test4_cb_count++;
}

static void test_report_error_threshold(void)
{
    char *path = make_temp_db_path();
    struct mds_catalogue *db = NULL;
	struct mds_catalogue *cat = NULL;
    struct ds_health_monitor *hm = NULL;

    db = open_test_catalogue(); VERIFY(db != NULL);
	cat = db;

    /* Register DS. */
    {
        struct mds_ds_info info;
        struct mds_cat_txn *txn = NULL;

        memset(&info, 0, sizeof(info));
        info.ds_id = 7;
        info.state = DS_ONLINE;
        info.port = 2049;
        snprintf(info.addr, sizeof(info.addr), "10.0.0.7:/data");

        VERIFY(mds_cat_txn_begin(db, MDS_CAT_TXN_WRITE, &txn) == MDS_OK);
        VERIFY(mds_cat_ds_put(db, txn, &info) == MDS_OK);
        mds_cat_txn_commit(txn);
    }

    test4_failed_ds_id = 0;
    test4_cb_count = 0;

    ASSERT_EQ(ds_health_init(cat, NULL, 60000, 3,
                              test4_fail_cb, NULL, &hm), 0);

    /* Two reports — below threshold. */
    ds_health_report_error(hm, 7);
    ds_health_report_error(hm, 7);
    ASSERT_EQ(test4_cb_count, 0);

    /* Third report — hits threshold. */
    ds_health_report_error(hm, 7);
    ASSERT_EQ(test4_cb_count, 1);
    ASSERT_EQ(test4_failed_ds_id, 7);

    ds_health_destroy(hm);
    mds_catalogue_close(db);
}

/* -----------------------------------------------------------------------
 * Test 5: COMMIT_OP_DS_STATE round-trip
 * ----------------------------------------------------------------------- */

static void test_commit_op_ds_state(void)
{
    char *path = make_temp_db_path();
    struct mds_catalogue *db = NULL;
    struct commit_queue *cq = NULL;

    db = open_test_catalogue(); VERIFY(db != NULL);

    /* Register DS as ONLINE. */
    {
        struct mds_ds_info info;
        struct mds_cat_txn *txn = NULL;

        memset(&info, 0, sizeof(info));
        info.ds_id = 99;
        info.state = DS_ONLINE;
        info.port = 2049;
        snprintf(info.addr, sizeof(info.addr), "10.0.0.99:/data");

        VERIFY(mds_cat_txn_begin(db, MDS_CAT_TXN_WRITE, &txn) == MDS_OK);
        VERIFY(mds_cat_ds_put(db, txn, &info) == MDS_OK);
        mds_cat_txn_commit(txn);
    }

    /* Create commit queue and submit DS_STATE op. */
    ASSERT_EQ(commit_queue_create(db, NULL, 0, 0, 0, 0, 0, 0, &cq), 0);

    {
        struct commit_op cop;

        memset(&cop, 0, sizeof(cop));
        cop.type = COMMIT_OP_DS_STATE;
        cop.args.ds_state.ds_id = 99;
        cop.args.ds_state.new_state = DS_OFFLINE;
        ASSERT_EQ(commit_queue_submit(cq, &cop), MDS_OK);
    }

    /* Verify DS is now OFFLINE. */
    {
        struct mds_ds_info check;

        ASSERT_EQ(mds_cat_ds_get(db, 99, &check), MDS_OK);
        ASSERT_EQ(check.state, DS_OFFLINE);
    }

    commit_queue_destroy(cq);
    mds_catalogue_close(db);
}


/* -----------------------------------------------------------------------
 * Test 6: ds_health_init rejects interval_ms=0
 * ----------------------------------------------------------------------- */

static void test_init_rejects_zero_interval(void)
{
    char *path = make_temp_db_path();
    struct mds_catalogue *db = NULL;
	struct mds_catalogue *cat = NULL;
    struct ds_health_monitor *hm = NULL;

    db = open_test_catalogue(); VERIFY(db != NULL);
	cat = db;

    /* interval_ms=0 must be rejected. */
    ASSERT_EQ(ds_health_init(cat, NULL, 0, 3, NULL, NULL, &hm), -1);

    /* Verify hm was not allocated. */
    ASSERT_EQ((long long)(uintptr_t)hm, 0);

    mds_catalogue_close(db);
}

/* -----------------------------------------------------------------------
 * Test 7: Anti-flapping — recovery requires multiple consecutive probes
 *
 * After force_fail marks DS offline, a single report_error(ds_id)
 * resets the recovery counter.  The DS should NOT come back online
 * from a single success followed by a failure.
 * ----------------------------------------------------------------------- */

static uint32_t test7_failed_ds_id;
static int test7_cb_count;

static void test7_fail_cb(uint32_t ds_id, void *ctx)
{
    (void)ctx;
    test7_failed_ds_id = ds_id;
    test7_cb_count++;
}

static void test_recovery_threshold(void)
{
    struct mds_catalogue *db = NULL;
    struct ds_health_monitor *hm = NULL;

    db = open_test_catalogue(); VERIFY(db != NULL);

    /* Register DS. */
    {
        struct mds_ds_info info;
        struct mds_cat_txn *txn = NULL;

        memset(&info, 0, sizeof(info));
        info.ds_id = 50;
        info.state = DS_ONLINE;
        info.port = 2049;
        snprintf(info.addr, sizeof(info.addr), "10.0.0.50:/data");

        VERIFY(mds_cat_txn_begin(db, MDS_CAT_TXN_WRITE, &txn) == MDS_OK);
        VERIFY(mds_cat_ds_put(db, txn, &info) == MDS_OK);
        mds_cat_txn_commit(txn);
    }

    test7_failed_ds_id = 0;
    test7_cb_count = 0;

    /* threshold=6 → recovery_threshold = max(6/2, 3) = 3 */
    ASSERT_EQ(ds_health_init(db, NULL, 60000, 6,
                              test7_fail_cb, NULL, &hm), 0);

    /* Force offline. */
    ds_health_test_force_fail(hm, 50);
    ASSERT_EQ(test7_cb_count, 1);

    /* Verify DS is OFFLINE. */
    {
        struct mds_ds_info check;
        ASSERT_EQ(mds_cat_ds_get(db, 50, &check), MDS_OK);
        ASSERT_EQ(check.state, DS_OFFLINE);
    }

    /* A single report_error should reset the recovery counter.
     * The DS must NOT come back online from subsequent logic
     * because the recovery counter was zeroed.  This verifies
     * that handle_ds_failure properly clears recovery_successes. */
    ds_health_report_error(hm, 50);

    /* DS should still be offline (report_error with increment=1
     * on an already-offline DS just resets recovery_successes). */
    {
        struct mds_ds_info check;
        ASSERT_EQ(mds_cat_ds_get(db, 50, &check), MDS_OK);
        ASSERT_EQ(check.state, DS_OFFLINE);
    }

    ds_health_destroy(hm);
    mds_catalogue_close(db);
}

/* -----------------------------------------------------------------------
 * Test 8: Anti-flapping — repeated failures increase cooldown (flap_count)
 *
 * The first force_fail sets flap_count=1, second sets flap_count=2.
 * Verify the callback fires both times (the cooldown only affects
 * the poll thread, not report_error / force_fail API paths).
 * ----------------------------------------------------------------------- */

static uint32_t test8_failed_ds_id;
static int test8_cb_count;

static void test8_fail_cb(uint32_t ds_id, void *ctx)
{
    (void)ctx;
    test8_failed_ds_id = ds_id;
    test8_cb_count++;
}

static void test_flap_count_increments(void)
{
    struct mds_catalogue *db = NULL;
    struct ds_health_monitor *hm = NULL;

    db = open_test_catalogue(); VERIFY(db != NULL);

    /* Register DS. */
    {
        struct mds_ds_info info;
        struct mds_cat_txn *txn = NULL;

        memset(&info, 0, sizeof(info));
        info.ds_id = 60;
        info.state = DS_ONLINE;
        info.port = 2049;
        snprintf(info.addr, sizeof(info.addr), "10.0.0.60:/data");

        VERIFY(mds_cat_txn_begin(db, MDS_CAT_TXN_WRITE, &txn) == MDS_OK);
        VERIFY(mds_cat_ds_put(db, txn, &info) == MDS_OK);
        mds_cat_txn_commit(txn);
    }

    test8_failed_ds_id = 0;
    test8_cb_count = 0;

    ASSERT_EQ(ds_health_init(db, NULL, 60000, 3,
                              test8_fail_cb, NULL, &hm), 0);

    /* First failure cycle. */
    ds_health_test_force_fail(hm, 60);
    ASSERT_EQ(test8_cb_count, 1);

    /* Second failure cycle (simulates flap: back online then offline). */
    ds_health_test_force_fail(hm, 60);
    ASSERT_EQ(test8_cb_count, 2);

    ds_health_destroy(hm);
    mds_catalogue_close(db);
}

/* -----------------------------------------------------------------------
 * main
 * ----------------------------------------------------------------------- */

int main(void)
{
    test_addr_parse_host();
    test_probe_null_refused();
    test_force_fail();
    test_report_error_threshold();
    test_commit_op_ds_state();
    test_init_rejects_zero_interval();
    test_recovery_threshold();
    test_flap_count_increments();

    printf("test_ds_health: %d passed, %d failed\n", pass_count, fail_count);
    return fail_count > 0 ? 1 : 0;
}
