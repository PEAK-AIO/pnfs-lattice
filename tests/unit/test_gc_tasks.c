/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_gc_tasks.c -- Memory-only tests for recoverable GC task leases.
 */

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "mds_catalogue.h"
#include "pnfs_mds.h"
#include "test_helpers.h"

static int tests_run;
static int tests_passed;

#define ASSERT_EQ(actual, expected) do {                                    \
    long long actual_value = (long long)(actual);                            \
    long long expected_value = (long long)(expected);                        \
    if (actual_value != expected_value) {                                    \
        fprintf(stderr, "FAIL %s:%d: %s=%lld, expected %s=%lld\n",         \
                __FILE__, __LINE__, #actual, actual_value, #expected,       \
                expected_value);                                             \
        exit(1);                                                             \
    }                                                                        \
} while (0)

#define RUN_TEST(test_fn) do {                                               \
    tests_run++;                                                             \
    fprintf(stdout, "  %-40s ", #test_fn);                                 \
    test_fn();                                                               \
    tests_passed++;                                                          \
    fprintf(stdout, "PASS\n");                                              \
} while (0)

static struct mds_gc_task make_task(uint64_t task_id)
{
    struct mds_gc_task task;

    memset(&task, 0, sizeof(task));
    task.task_kind = MDS_GC_TASK_LEGACY_DS_UNLINK;
    task.task_id = task_id;
    task.fileid = 1000U + task_id;
    task.ds_id = 1;
    task.nfs_fh_len = 4;
    memcpy(task.nfs_fh, "task", task.nfs_fh_len);
    return task;
}

static void claim_one(struct mds_catalogue *cat, uint32_t owner_mds_id,
                      uint64_t owner_boot_epoch, uint32_t lease_ms,
                      struct mds_gc_task *claimed)
{
    uint32_t count = 0;

    ASSERT_EQ(mds_cat_gc_task_claim_batch(
                  cat, claimed, 1, &count, owner_mds_id, owner_boot_epoch,
                  lease_ms, 1000U),
              MDS_OK);
    ASSERT_EQ(count, 1);
}

static void test_replay_and_owner_fencing(void)
{
    struct mds_catalogue *cat = open_test_catalogue();
    struct mds_gc_task task = make_task(41);
    struct mds_gc_task claimed;
    uint32_t count = 0;

    ASSERT_EQ(mds_cat_gc_task_enqueue(cat, NULL, &task), MDS_OK);
    task.ds_id = 99;
    ASSERT_EQ(mds_cat_gc_task_enqueue(cat, NULL, &task), MDS_OK);

    claim_one(cat, 7U, 101U, 1000U, &claimed);
    ASSERT_EQ(claimed.task_id, 41);
    ASSERT_EQ(claimed.ds_id, 1);
    ASSERT_EQ(claimed.attempt_count, 1);
    ASSERT_EQ(mds_cat_gc_task_renew(
                  cat, MDS_GC_TASK_LEGACY_DS_UNLINK, 41, 8U, 101U, 1000U),
              MDS_ERR_STALE);
    ASSERT_EQ(mds_cat_gc_task_reschedule(
                  cat, MDS_GC_TASK_LEGACY_DS_UNLINK, 41, 7U, 101U,
                  MDS_ERR_IO, 0),
              MDS_OK);

    claim_one(cat, 8U, 202U, 1000U, &claimed);
    ASSERT_EQ(claimed.attempt_count, 2);
    ASSERT_EQ(mds_cat_gc_task_complete(
                  cat, MDS_GC_TASK_LEGACY_DS_UNLINK, 41, 7U, 101U),
              MDS_ERR_STALE);
    ASSERT_EQ(mds_cat_gc_task_complete(
                  cat, MDS_GC_TASK_LEGACY_DS_UNLINK, 41, 8U, 202U),
              MDS_OK);
    ASSERT_EQ(mds_cat_gc_task_claim_batch(
                  cat, &claimed, 1, &count, 8U, 202U, 1000U, 1000U),
              MDS_OK);
    ASSERT_EQ(count, 0);
    mds_catalogue_close(cat);
}
static void test_legacy_enqueue_is_durable(void)
{
    struct mds_catalogue *cat = open_test_catalogue();
    struct mds_gc_task claimed;
    const uint8_t fh[] = { 'l', 'e', 'g', 'a', 'c', 'y' };
    uint32_t count = 0;

    ASSERT_EQ(mds_cat_gc_enqueue(cat, NULL, 500U, 3U, fh, sizeof(fh)),
              MDS_OK);
    claim_one(cat, 5U, 505U, 1000U, &claimed);
    ASSERT_EQ(claimed.task_kind, MDS_GC_TASK_LEGACY_DS_UNLINK);
    ASSERT_EQ(claimed.fileid, 500);
    ASSERT_EQ(claimed.ds_id, 3);
    ASSERT_EQ(claimed.nfs_fh_len, sizeof(fh));
    ASSERT_EQ(mds_cat_gc_task_complete(
                  cat, claimed.task_kind, claimed.task_id, 5U, 505U),
              MDS_OK);
    ASSERT_EQ(mds_cat_gc_count(cat, &count), MDS_OK);
    ASSERT_EQ(count, 0);
    mds_catalogue_close(cat);
}

static void test_expired_lease_is_replayable(void)
{
    struct mds_catalogue *cat = open_test_catalogue();
    struct mds_gc_task task = make_task(42);
    struct mds_gc_task claimed;

    ASSERT_EQ(mds_cat_gc_task_enqueue(cat, NULL, &task), MDS_OK);
    claim_one(cat, 3U, 303U, 1U, &claimed);
    usleep(5000U);
    claim_one(cat, 4U, 404U, 1000U, &claimed);
    ASSERT_EQ(claimed.task_id, 42);
    ASSERT_EQ(claimed.attempt_count, 2);
    ASSERT_EQ(mds_cat_gc_task_complete(
                  cat, MDS_GC_TASK_LEGACY_DS_UNLINK, 42, 4U, 404U),
              MDS_OK);
    mds_catalogue_close(cat);
}

static void test_malformed_task_does_not_block_valid_claim(void)
{
    struct mds_catalogue *cat = open_test_catalogue();
    struct mds_gc_task malformed = make_task(43);
    struct mds_gc_task valid = make_task(44);
    struct mds_gc_task claimed[2];
    struct mds_gc_task quarantined;
    uint32_t count = 0;

    malformed.state = MDS_GC_TASK_PENDING;
    malformed.nfs_fh_len = 0;
    memset(malformed.nfs_fh, 0, sizeof(malformed.nfs_fh));
    ASSERT_EQ(catalogue_memdb_inject_raw_gc_task(cat, &malformed), MDS_OK);
    ASSERT_EQ(mds_cat_gc_task_enqueue(cat, NULL, &valid), MDS_OK);

    ASSERT_EQ(mds_cat_gc_task_claim_batch(
                  cat, claimed, 2, &count, 9U, 909U, 1000U, 1000U),
              MDS_OK);
    ASSERT_EQ(count, 1);
    ASSERT_EQ(claimed[0].task_id, valid.task_id);
    ASSERT_EQ(catalogue_memdb_get_gc_task(
                  cat, malformed.task_kind, malformed.task_id,
                  &quarantined),
              MDS_OK);
    ASSERT_EQ(quarantined.state, MDS_GC_TASK_QUARANTINED);
    ASSERT_EQ(quarantined.last_error, MDS_ERR_INVAL);
    ASSERT_EQ(mds_cat_gc_task_complete(
                  cat, claimed[0].task_kind, claimed[0].task_id,
                  9U, 909U),
              MDS_OK);
    ASSERT_EQ(mds_cat_gc_task_claim_batch(
                  cat, claimed, 2, &count, 9U, 909U, 1000U, 1000U),
              MDS_OK);
    ASSERT_EQ(count, 0);
    mds_catalogue_close(cat);
}

int main(void)
{
    fprintf(stdout, "durable GC task tests\n");
    RUN_TEST(test_replay_and_owner_fencing);
    RUN_TEST(test_expired_lease_is_replayable);
    RUN_TEST(test_legacy_enqueue_is_durable);
    RUN_TEST(test_malformed_task_does_not_block_valid_claim);
    fprintf(stdout, "%d/%d tests passed\n", tests_passed, tests_run);
    return tests_passed == tests_run ? 0 : 1;
}
