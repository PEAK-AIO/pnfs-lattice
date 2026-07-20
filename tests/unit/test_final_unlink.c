/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_final_unlink.c -- Memory-only atomic final-file unlink tests.
 */

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "mds_catalogue.h"
#include "pnfs_mds.h"
#include "quota.h"
#include "test_helpers.h"
#include "compound.h"

static int tests_run;
static int tests_passed;

#define ASSERT_EQ(actual, expected) do {                                    \
    long long actual_value = (long long)(actual);                            \
    long long expected_value = (long long)(expected);                        \
    if (actual_value != expected_value) {                                    \
        fprintf(stderr, "FAIL %s:%d: %s=%lld, expected %s=%lld\n",          \
                __FILE__, __LINE__, #actual, actual_value, #expected,       \
                expected_value);                                             \
        exit(1);                                                             \
    }                                                                        \
} while (0)

#define ASSERT_TRUE(condition) do {                                          \
    if (!(condition)) {                                                      \
        fprintf(stderr, "FAIL %s:%d: %s\n",                                  \
                __FILE__, __LINE__, #condition);                            \
        exit(1);                                                             \
    }                                                                        \
} while (0)

#define RUN_TEST(test_fn) do {                                               \
    tests_run++;                                                             \
    fprintf(stdout, "  %-40s ", #test_fn);                                  \
    test_fn();                                                               \
    tests_passed++;                                                          \
    fprintf(stdout, "PASS\n");                                              \
} while (0)

static void test_final_file_unlink_is_atomic(void)
{
    struct mds_catalogue *cat = open_test_catalogue();
    struct mds_inode child;
    struct mds_inode parent_before;
    struct mds_inode retained_child;
    struct mds_final_unlink_result result;
    struct mds_gc_entry legacy_entry;
    struct mds_gc_task task;
    struct mds_ds_map_entry entry;
    struct mds_ds_map_entry *retained_entries = NULL;
    uint64_t child_fileid;
    uint64_t child_generation;
    uint32_t claimed_count = 0;
    uint32_t stripe_count = 0;
    uint32_t stripe_unit = 0;
    uint32_t mirror_count = 0;
    const uint8_t filehandle[] = { 'd', 's', '-', 'f', 'h' };

    ASSERT_TRUE(cat != NULL);
    ASSERT_EQ(mds_cat_ns_create(cat, NULL, MDS_FILEID_ROOT, "final",
                                MDS_FTYPE_REG, 0644, 1000, 1000, NULL,
                                &child),
              MDS_OK);
    child_fileid = child.fileid;
    child_generation = child.generation;
    memset(&entry, 0, sizeof(entry));
    entry.ds_id = 17;
    entry.nfs_fh_len = sizeof(filehandle);
    memcpy(entry.nfs_fh, filehandle, sizeof(filehandle));
    ASSERT_EQ(mds_cat_stripe_map_put(cat, NULL, child_fileid, 1, 65536, 1,
                                     &entry),
              MDS_OK);
    ASSERT_EQ(mds_cat_ns_getattr(cat, MDS_FILEID_ROOT, &parent_before),
              MDS_OK);

    ASSERT_EQ(mds_cat_ns_remove_final_file(
                  cat, MDS_FILEID_ROOT, "final", child_fileid,
                  child_generation, &result),
              MDS_OK);
    ASSERT_EQ(result.parent_change_before, parent_before.change);
    ASSERT_EQ(result.parent_change_after, parent_before.change + 1U);
    ASSERT_EQ(mds_cat_ns_lookup(cat, MDS_FILEID_ROOT, "final", &child),
              MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_ns_getattr(cat, child_fileid, &retained_child), MDS_OK);
    ASSERT_EQ(retained_child.nlink, 0);
    ASSERT_TRUE((retained_child.flags & MDS_IFLAG_DELETE_PENDING) != 0);
    ASSERT_EQ(retained_child.generation, child_generation);
    ASSERT_EQ(mds_cat_stripe_map_get(
                  cat, child_fileid, &stripe_count, &stripe_unit,
                  &mirror_count, &retained_entries),
              MDS_OK);
    ASSERT_EQ(stripe_count, 1);
    ASSERT_EQ(stripe_unit, 65536);
    ASSERT_EQ(mirror_count, 1);
    ASSERT_EQ(retained_entries[0].ds_id, entry.ds_id);
    free(retained_entries);

    memset(&legacy_entry, 0, sizeof(legacy_entry));
    ASSERT_EQ(mds_cat_gc_peek(cat, &legacy_entry), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_gc_task_claim_batch(
                  cat, &task, 1, &claimed_count, 7, 701, 1000, 1000),
              MDS_OK);
    ASSERT_EQ(claimed_count, 1);
    ASSERT_EQ(task.task_kind, MDS_GC_TASK_FILE_UNLINK);
    ASSERT_EQ(task.task_id, child_fileid);
    ASSERT_EQ(task.fileid, child_fileid);
    ASSERT_EQ(task.inode_generation, child_generation);

    ASSERT_EQ(mds_cat_ns_remove_final_file(
                  cat, MDS_FILEID_ROOT, "final", child_fileid,
                  child_generation, &result),
              MDS_ERR_NOTFOUND);
    mds_catalogue_close(cat);
}
static void test_file_unlink_finalizer_releases_retained_state(void)
{
    struct mds_catalogue *cat = open_test_catalogue();
    struct mds_inode child;
    struct mds_inode attrs;
    struct mds_final_unlink_result result;
    struct mds_gc_task task;
    struct mds_ds_map_entry entry;
    struct mds_quota_usage group_usage;
    struct mds_ds_map_entry *entries = NULL;
    uint32_t claimed_count = 0;
    uint32_t stripe_count = 0;
    uint32_t stripe_unit = 0;
    uint32_t mirror_count = 0;

    ASSERT_TRUE(cat != NULL);
    ASSERT_EQ(mds_cat_ns_create(cat, NULL, MDS_FILEID_ROOT, "reclaim",
                                MDS_FTYPE_REG, 0644, 101, 202, NULL,
                                &child),
              MDS_OK);
    memset(&attrs, 0, sizeof(attrs));
    attrs.size = 4096;
    ASSERT_EQ(mds_cat_ns_setattr(cat, NULL, child.fileid, &attrs,
                                 MDS_ATTR_SIZE),
              MDS_OK);
    memset(&entry, 0, sizeof(entry));
    entry.ds_id = 17;
    ASSERT_EQ(mds_cat_stripe_map_put(cat, NULL, child.fileid, 1, 65536, 1,
                                     &entry),
              MDS_OK);
    memset(&group_usage, 0, sizeof(group_usage));
    group_usage.used_bytes = 8192;
    group_usage.used_inodes = 2;
    ASSERT_EQ(mds_cat_quota_usage_put(
                  cat, NULL, MDS_QUOTA_GROUP_USAGE, 202, &group_usage),
              MDS_OK);
    ASSERT_EQ(mds_cat_ns_remove_final_file(
                  cat, MDS_FILEID_ROOT, "reclaim", child.fileid,
                  child.generation, &result),
              MDS_OK);
    ASSERT_EQ(mds_cat_gc_task_claim_batch(
                  cat, &task, 1, &claimed_count, 7, 701, 1000, 1000),
              MDS_OK);
    ASSERT_EQ(claimed_count, 1);
    ASSERT_EQ(mds_cat_gc_task_finalize_file(
                  cat, child.fileid, child.generation, 8, 801),
              MDS_ERR_STALE);
    ASSERT_EQ(mds_cat_gc_task_finalize_file(
                  cat, child.fileid, child.generation, 7, 701),
              MDS_OK);
    ASSERT_EQ(mds_cat_ns_getattr(cat, child.fileid, &attrs),
              MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_stripe_map_get(
                  cat, child.fileid, &stripe_count, &stripe_unit,
                  &mirror_count, &entries),
              MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_quota_usage_get(
                  cat, MDS_QUOTA_USER_USAGE, 101, &group_usage),
              MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_quota_usage_get(
                  cat, MDS_QUOTA_GROUP_USAGE, 202, &group_usage),
              MDS_OK);
    ASSERT_EQ(group_usage.used_bytes, 4096);
    ASSERT_EQ(group_usage.used_inodes, 1);
    ASSERT_EQ(mds_cat_gc_task_claim_batch(
                  cat, &task, 1, &claimed_count, 7, 701, 1000, 1000),
              MDS_OK);
    ASSERT_EQ(claimed_count, 0);
    mds_catalogue_close(cat);
}

static void test_file_unlink_task_quarantine(void)
{
    struct mds_catalogue *cat = open_test_catalogue();
    struct mds_inode child;
    struct mds_final_unlink_result result;
    struct mds_gc_task task;
    uint32_t claimed_count = 0;

    ASSERT_TRUE(cat != NULL);
    ASSERT_EQ(mds_cat_ns_create(cat, NULL, MDS_FILEID_ROOT, "broken",
                                MDS_FTYPE_REG, 0644, 1000, 1000, NULL,
                                &child),
              MDS_OK);
    ASSERT_EQ(mds_cat_ns_remove_final_file(
                  cat, MDS_FILEID_ROOT, "broken", child.fileid,
                  child.generation, &result),
              MDS_OK);
    ASSERT_EQ(mds_cat_gc_task_claim_batch(
                  cat, &task, 1, &claimed_count, 7, 701, 1000, 1000),
              MDS_OK);
    ASSERT_EQ(claimed_count, 1);
    ASSERT_EQ(mds_cat_gc_task_quarantine(
                  cat, task.task_kind, task.task_id, 7, 701,
                  MDS_ERR_STALE),
              MDS_OK);
    ASSERT_EQ(mds_cat_gc_task_claim_batch(
                  cat, &task, 1, &claimed_count, 7, 701, 1000, 1000),
              MDS_OK);
    ASSERT_EQ(claimed_count, 0);
    mds_catalogue_close(cat);
}
static void test_final_file_unlink_rejects_hardlink(void)
{
    struct mds_catalogue *cat = open_test_catalogue();
    struct mds_inode child;
    struct mds_inode retained_child;
    struct mds_final_unlink_result result;
    struct mds_gc_task task;
    uint32_t claimed_count = 0;

    ASSERT_TRUE(cat != NULL);
    ASSERT_EQ(mds_cat_ns_create(cat, NULL, MDS_FILEID_ROOT, "linked",
                                MDS_FTYPE_REG, 0644, 1000, 1000, NULL,
                                &child),
              MDS_OK);
    ASSERT_EQ(mds_cat_ns_link(cat, NULL, MDS_FILEID_ROOT, "linked-alias",
                              child.fileid),
              MDS_OK);
    ASSERT_EQ(mds_cat_ns_remove_final_file(
                  cat, MDS_FILEID_ROOT, "linked", child.fileid,
                  child.generation, &result),
              MDS_ERR_STALE);
    ASSERT_EQ(mds_cat_ns_lookup(cat, MDS_FILEID_ROOT, "linked", &child),
              MDS_OK);
    ASSERT_EQ(mds_cat_ns_lookup(cat, MDS_FILEID_ROOT, "linked-alias",
                                &retained_child),
              MDS_OK);
    ASSERT_EQ(retained_child.nlink, 2);
    ASSERT_TRUE((retained_child.flags & MDS_IFLAG_DELETE_PENDING) == 0);
    ASSERT_EQ(mds_cat_gc_task_claim_batch(
                  cat, &task, 1, &claimed_count, 7, 701, 1000, 1000),
              MDS_OK);
    ASSERT_EQ(claimed_count, 0);
    mds_catalogue_close(cat);
}

static void test_raced_async_remove_delays_and_relooks_up(void)
{
    struct mds_catalogue *cat = open_test_catalogue();
    struct compound_data compound;
    struct mds_inode original;
    struct mds_inode replacement;
    struct mds_inode retained;
    struct mds_gc_task task;
    struct nfs4_op operations[2];
    struct nfs4_result results[2] = {0};
    uint32_t claimed_count = 0;
    uint32_t result_count;

    ASSERT_TRUE(cat != NULL);
    ASSERT_EQ(test_create_file(
                  cat, MDS_FILEID_ROOT, "raced-remove", 0644, &original),
              MDS_OK);
    catalogue_memdb_fail_next_final_unlink(cat, MDS_ERR_STALE);

    memset(operations, 0, sizeof(operations));
    operations[0].opnum = OP_PUTROOTFH;
    operations[1].opnum = OP_REMOVE;
    snprintf(operations[1].arg.remove.name,
             sizeof(operations[1].arg.remove.name), "%s", "raced-remove");
    compound_init(&compound);
    compound.cat = cat;
    compound.cfg_remove_async = true;
    result_count = compound_process(
        &compound, operations, results, 2);
    ASSERT_EQ(result_count, 2);
    ASSERT_EQ(results[1].status, NFS4ERR_DELAY);
    ASSERT_EQ(mds_cat_ns_lookup(
                  cat, MDS_FILEID_ROOT, "raced-remove", &retained),
              MDS_OK);
    ASSERT_EQ(retained.fileid, original.fileid);
    ASSERT_EQ(mds_cat_gc_task_claim_batch(
                  cat, &task, 1, &claimed_count, 7, 701, 1000, 1000),
              MDS_OK);
    ASSERT_EQ(claimed_count, 0);

    ASSERT_EQ(mds_cat_ns_remove(
                  cat, NULL, MDS_FILEID_ROOT, "raced-remove"),
              MDS_OK);
    ASSERT_EQ(test_create_file(
                  cat, MDS_FILEID_ROOT, "raced-remove", 0644, &replacement),
              MDS_OK);
    ASSERT_TRUE(replacement.fileid != original.fileid);

    compound_init(&compound);
    compound.cat = cat;
    compound.cfg_remove_async = true;
    result_count = compound_process(
        &compound, operations, results, 2);
    ASSERT_EQ(result_count, 2);
    ASSERT_EQ(results[1].status, NFS4_OK);
    ASSERT_EQ(mds_cat_ns_lookup(
                  cat, MDS_FILEID_ROOT, "raced-remove", &retained),
              MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_ns_getattr(cat, replacement.fileid, &retained), MDS_OK);
    ASSERT_TRUE((retained.flags & MDS_IFLAG_DELETE_PENDING) != 0);
    ASSERT_EQ(mds_cat_ns_getattr(cat, original.fileid, &retained),
              MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_gc_task_claim_batch(
                  cat, &task, 1, &claimed_count, 7, 701, 1000, 1000),
              MDS_OK);
    ASSERT_EQ(claimed_count, 1);
    ASSERT_EQ(task.task_id, replacement.fileid);
    mds_catalogue_close(cat);
}

int main(void)
{
    fprintf(stdout, "atomic final-file unlink tests\n");
    RUN_TEST(test_final_file_unlink_is_atomic);
    RUN_TEST(test_final_file_unlink_rejects_hardlink);
    RUN_TEST(test_file_unlink_finalizer_releases_retained_state);
    RUN_TEST(test_file_unlink_task_quarantine);
    RUN_TEST(test_raced_async_remove_delays_and_relooks_up);
    fprintf(stdout, "%d/%d tests passed\n", tests_passed, tests_run);
    return tests_passed == tests_run ? 0 : 1;
}
