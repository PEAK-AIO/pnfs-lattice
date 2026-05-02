/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_subtree_split.c — Unit tests for subtree split/merge.
 *
 * Tests cover: valid split/merge, all rejection cases, migration
 * rollback, depth limits, and no duplicates after split+merge cycle.
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

#include "pnfs_mds.h"
#include "subtree_map.h"
#include "subtree_split.h"
#include "mds_catalogue.h"
#include "test_helpers.h"
#include "test_helpers.h"

/* -------------------------------------------------------------------
 * Test infrastructure
 * ------------------------------------------------------------------- */

static int tests_run;
static int tests_passed;
static int test_failed;

#define TEST_MAP_SIZE (16ULL * 1024 * 1024)
#define SELF_ID  0

#define ASSERT_EQ(a, b) do {                                    \
    if ((a) != (b)) {                                           \
        fprintf(stderr, "  FAIL %s:%d: %s != %s\n",            \
                __FILE__, __LINE__, #a, #b);                    \
        test_failed = 1;                                        \
        return;                                                 \
    }                                                           \
} while (0)

#define ASSERT_NE(a, b) do {                                    \
    if ((a) == (b)) {                                           \
        fprintf(stderr, "  FAIL %s:%d: %s == %s\n",            \
                __FILE__, __LINE__, #a, #b);                    \
        test_failed = 1;                                        \
        return;                                                 \
    }                                                           \
} while (0)

#define ASSERT_TRUE(cond) do {                                  \
    if (!(cond)) {                                              \
        fprintf(stderr, "  FAIL %s:%d: !(%s)\n",               \
                __FILE__, __LINE__, #cond);                     \
        test_failed = 1;                                        \
        return;                                                 \
    }                                                           \
} while (0)

#define RUN_TEST(fn) do {                                       \
    tests_run++;                                                \
    test_failed = 0;                                            \
    fprintf(stdout, "  %-50s", #fn);                            \
    fflush(stdout);                                             \
    fn();                                                       \
    if (!test_failed) {                                         \
        tests_passed++;                                         \
        fprintf(stdout, "PASS\n");                              \
    } else {                                                    \
        fprintf(stdout, "FAIL\n");                              \
    }                                                           \
} while (0)

/* -------------------------------------------------------------------
 * DB helpers
 * ------------------------------------------------------------------- */

static char *make_temp_db_path(void)
{
    char tmpl[] = "/tmp/pnfs-split-test-XXXXXX";
    char *dir;
    char *path;
    size_t len;

    dir = mkdtemp(tmpl);
    assert(dir != NULL);

    len = strlen(dir) + sizeof("/data.mdb");
    path = malloc(len);
    assert(path != NULL);
    snprintf(path, len, "%s/data.mdb", dir);
    return path;
}

static void cleanup_temp_db(const char *path)
{
    char lock_path[512];

    unlink(path);
    snprintf(lock_path, sizeof(lock_path), "%s-lock", path);
    unlink(lock_path);

    {
        char dir[512];
        size_t plen;
        const char *slash;

        slash = strrchr(path, '/');
        if (slash != NULL) {
            plen = (size_t)(slash - path);
            memcpy(dir, path, plen);
            dir[plen] = '\0';
            rmdir(dir);
        }
    }
}

/** Create a directory inode at the given fileid in the catalogue. */
static void seed_dir_inode(struct mds_catalogue *db, uint64_t fileid)
{
    struct mds_inode inode;
    enum mds_status st;
    struct mds_cat_txn *txn = NULL;

    memset(&inode, 0, sizeof(inode));
    inode.fileid = fileid;
    inode.type   = MDS_FTYPE_DIR;
    inode.mode   = 0755;
    inode.nlink  = 2;
    inode.change = 1;

    st = mds_cat_txn_begin(db, MDS_CAT_TXN_WRITE, &txn);
    assert(st == MDS_OK);
    st = mds_cat_inode_put(db, txn, &inode);
    assert(st == MDS_OK);
    VERIFY(mds_cat_txn_commit(txn) == 0);
}

/** Create a regular file inode at the given fileid in the catalogue. */
static void seed_file_inode(struct mds_catalogue *db, uint64_t fileid)
{
    struct mds_inode inode;
    enum mds_status st;
    struct mds_cat_txn *txn = NULL;

    memset(&inode, 0, sizeof(inode));
    inode.fileid = fileid;
    inode.type   = MDS_FTYPE_REG;
    inode.mode   = 0644;
    inode.nlink  = 1;
    inode.change = 1;

    st = mds_cat_txn_begin(db, MDS_CAT_TXN_WRITE, &txn);
    assert(st == MDS_OK);
    st = mds_cat_inode_put(db, txn, &inode);
    assert(st == MDS_OK);
    VERIFY(mds_cat_txn_commit(txn) == 0);
}

/* -------------------------------------------------------------------
 * Tests
 * ------------------------------------------------------------------- */

/* 1. Split creates child subtree — both parent and child in map. */
static void test_split_creates_child_subtree(void)
{
    struct subtree_map *map = NULL;
    struct mds_catalogue *db = NULL;
    struct mds_catalogue *cat = NULL;
    char *db_path;
    enum mds_status st;

    db_path = make_temp_db_path();
    st = ((db = open_test_catalogue()) != NULL ? MDS_OK : MDS_ERR_IO);
    ASSERT_EQ(st, MDS_OK);
    cat = db;

    st = subtree_map_init(NULL, NULL, SELF_ID, "mds0.local",
                                 NULL, &map);
    ASSERT_EQ(st, MDS_OK);

    st = subtree_map_add(map, "/data", SELF_ID, NULL,
                         SUBTREE_ACTIVE, 1);
    ASSERT_EQ(st, MDS_OK);

    /* Seed a directory inode for the child. */
    seed_dir_inode(db, 200);

    st = subtree_split_execute(map, cat, "/data", "/data/hot",
                               200, SELF_ID, NULL, 0);
    ASSERT_EQ(st, MDS_OK);

    /* Both parent and child exist in map. */
    struct subtree_entry e;
    st = subtree_map_lookup(map, "/data", &e);
    ASSERT_EQ(st, MDS_OK);

    st = subtree_map_lookup(map, "/data/hot", &e);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(e.owner_mds_id, (uint32_t)SELF_ID);

    subtree_map_destroy(map);
    mds_catalogue_close(db);
    cleanup_temp_db(db_path);
    free(db_path);
}

/* 2. After local split, child is owned locally. */
static void test_split_child_is_local(void)
{
    struct subtree_map *map = NULL;
    struct mds_catalogue *db = NULL;
    struct mds_catalogue *cat = NULL;
    char *db_path;
    enum mds_status st;

    db_path = make_temp_db_path();
    st = ((db = open_test_catalogue()) != NULL ? MDS_OK : MDS_ERR_IO);
    ASSERT_EQ(st, MDS_OK);
    cat = db;

    st = subtree_map_init(NULL, NULL, SELF_ID, "mds0.local",
                                 NULL, &map);
    ASSERT_EQ(st, MDS_OK);

    st = subtree_map_add(map, "/data", SELF_ID, NULL,
                         SUBTREE_ACTIVE, 1);
    ASSERT_EQ(st, MDS_OK);

    seed_dir_inode(db, 300);

    st = subtree_split_execute(map, cat, "/data", "/data/users",
                               300, SELF_ID, NULL, 0);
    ASSERT_EQ(st, MDS_OK);

    struct subtree_entry e;
    st = subtree_map_lookup(map, "/data/users", &e);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(e.owner_mds_id, (uint32_t)SELF_ID);

    subtree_map_destroy(map);
    mds_catalogue_close(db);
    cleanup_temp_db(db_path);
    free(db_path);
}

/* 3. Merge removes child — only parent remains. */
static void test_merge_removes_child(void)
{
    struct subtree_map *map = NULL;
    struct mds_catalogue *db = NULL;
    struct mds_catalogue *cat = NULL;
    char *db_path;
    enum mds_status st;

    db_path = make_temp_db_path();
    st = ((db = open_test_catalogue()) != NULL ? MDS_OK : MDS_ERR_IO);
    ASSERT_EQ(st, MDS_OK);
    cat = db;

    st = subtree_map_init(NULL, NULL, SELF_ID, "mds0.local",
                                 NULL, &map);
    ASSERT_EQ(st, MDS_OK);

    st = subtree_map_add(map, "/data", SELF_ID, NULL,
                         SUBTREE_ACTIVE, 1);
    ASSERT_EQ(st, MDS_OK);

    seed_dir_inode(db, 400);

    st = subtree_split_execute(map, cat, "/data", "/data/hot",
                               400, SELF_ID, NULL, 0);
    ASSERT_EQ(st, MDS_OK);

    /* Now merge. */
    st = subtree_merge_execute(map, db, "/data", "/data/hot", NULL);
    ASSERT_EQ(st, MDS_OK);

    /* Child gone from map. */
    struct subtree_entry e;
    st = subtree_map_lookup_exact(map, "/data/hot", &e);
    ASSERT_EQ(st, MDS_ERR_NOTFOUND);

    /* Parent still there. */
    st = subtree_map_lookup(map, "/data", &e);
    ASSERT_EQ(st, MDS_OK);

    subtree_map_destroy(map);
    mds_catalogue_close(db);
    cleanup_temp_db(db_path);
    free(db_path);
}

/* 4. Split rejects nonexistent child inode. */
static void test_split_rejects_nonexistent_dir(void)
{
    struct subtree_map *map = NULL;
    struct mds_catalogue *db = NULL;
    struct mds_catalogue *cat = NULL;
    char *db_path;
    enum mds_status st;

    db_path = make_temp_db_path();
    st = ((db = open_test_catalogue()) != NULL ? MDS_OK : MDS_ERR_IO);
    ASSERT_EQ(st, MDS_OK);
    cat = db;

    st = subtree_map_init(NULL, NULL, SELF_ID, "mds0.local",
                                 NULL, &map);
    ASSERT_EQ(st, MDS_OK);

    st = subtree_map_add(map, "/data", SELF_ID, NULL,
                         SUBTREE_ACTIVE, 1);
    ASSERT_EQ(st, MDS_OK);

    /* fileid 999 does not exist. */
    st = subtree_split_execute(map, cat, "/data", "/data/ghost",
                               999, SELF_ID, NULL, 0);
    ASSERT_NE(st, MDS_OK);

    subtree_map_destroy(map);
    mds_catalogue_close(db);
    cleanup_temp_db(db_path);
    free(db_path);
}

/* 5. Split rejects non-directory child. */
static void test_split_rejects_non_directory(void)
{
    struct subtree_map *map = NULL;
    struct mds_catalogue *db = NULL;
    struct mds_catalogue *cat = NULL;
    char *db_path;
    enum mds_status st;

    db_path = make_temp_db_path();
    st = ((db = open_test_catalogue()) != NULL ? MDS_OK : MDS_ERR_IO);
    ASSERT_EQ(st, MDS_OK);
    cat = db;

    st = subtree_map_init(NULL, NULL, SELF_ID, "mds0.local",
                                 NULL, &map);
    ASSERT_EQ(st, MDS_OK);

    st = subtree_map_add(map, "/data", SELF_ID, NULL,
                         SUBTREE_ACTIVE, 1);
    ASSERT_EQ(st, MDS_OK);

    /* Seed a regular file, not a directory. */
    seed_file_inode(db, 500);

    st = subtree_split_execute(map, cat, "/data", "/data/file.txt",
                               500, SELF_ID, NULL, 0);
    ASSERT_EQ(st, MDS_ERR_INVAL);

    subtree_map_destroy(map);
    mds_catalogue_close(db);
    cleanup_temp_db(db_path);
    free(db_path);
}

/* 6. Split rejects already-registered subtree root. */
static void test_split_rejects_already_subtree_root(void)
{
    struct subtree_map *map = NULL;
    struct mds_catalogue *db = NULL;
    struct mds_catalogue *cat = NULL;
    char *db_path;
    enum mds_status st;

    db_path = make_temp_db_path();
    st = ((db = open_test_catalogue()) != NULL ? MDS_OK : MDS_ERR_IO);
    ASSERT_EQ(st, MDS_OK);
    cat = db;

    st = subtree_map_init(NULL, NULL, SELF_ID, "mds0.local",
                                 NULL, &map);
    ASSERT_EQ(st, MDS_OK);

    st = subtree_map_add(map, "/data", SELF_ID, NULL,
                         SUBTREE_ACTIVE, 1);
    ASSERT_EQ(st, MDS_OK);

    seed_dir_inode(db, 600);

    /* First split succeeds. */
    st = subtree_split_execute(map, cat, "/data", "/data/hot",
                               600, SELF_ID, NULL, 0);
    ASSERT_EQ(st, MDS_OK);

    /* Second split with same child must fail. */
    st = subtree_split_execute(map, cat, "/data", "/data/hot",
                               600, SELF_ID, NULL, 0);
    ASSERT_EQ(st, MDS_ERR_EXISTS);

    subtree_map_destroy(map);
    mds_catalogue_close(db);
    cleanup_temp_db(db_path);
    free(db_path);
}

/* 7. Split rejects depth exceeded (max_depth=1 means only 1 level). */
static void test_split_rejects_depth_exceeded(void)
{
    struct subtree_map *map = NULL;
    enum mds_status st;

    st = subtree_map_init(NULL, NULL, SELF_ID, "mds0.local",
                                 NULL, &map);
    ASSERT_EQ(st, MDS_OK);

    st = subtree_map_add(map, "/data", SELF_ID, NULL,
                         SUBTREE_ACTIVE, 1);
    ASSERT_EQ(st, MDS_OK);

    /* "/data/a/b/c/d" has depth 4 — exceeds max_depth=3. */
    st = subtree_map_validate_split(map, "/data", "/data/a/b/c/d", 3);
    ASSERT_EQ(st, MDS_ERR_INVAL);

    /* "/data/a" has depth 1 — within default max. */
    st = subtree_map_validate_split(map, "/data", "/data/a", 0);
    ASSERT_EQ(st, MDS_OK);

    subtree_map_destroy(map);
}

/* 8. Split rejects unowned parent. */
static void test_split_rejects_unowned_parent(void)
{
    struct subtree_map *map = NULL;
    enum mds_status st;

    st = subtree_map_init(NULL, NULL, SELF_ID, "mds0.local",
                                 NULL, &map);
    ASSERT_EQ(st, MDS_OK);

    /* /remote owned by MDS 5 (not us). */
    st = subtree_map_add(map, "/remote", 5, "other.local",
                         SUBTREE_ACTIVE, 1);
    ASSERT_EQ(st, MDS_OK);

    st = subtree_map_validate_split(map, "/remote", "/remote/sub", 0);
    ASSERT_EQ(st, MDS_ERR_PERM);

    subtree_map_destroy(map);
}

/* 9. Migration failure causes rollback (no transport = simulated failure). */
static void test_migration_failure_rolls_back(void)
{
    struct subtree_map *map = NULL;
    struct mds_catalogue *db = NULL;
    struct mds_catalogue *cat = NULL;
    char *db_path;
    enum mds_status st;

    db_path = make_temp_db_path();
    st = ((db = open_test_catalogue()) != NULL ? MDS_OK : MDS_ERR_IO);
    ASSERT_EQ(st, MDS_OK);
    cat = db;

    st = subtree_map_init(NULL, NULL, SELF_ID, "mds0.local",
                                 NULL, &map);
    ASSERT_EQ(st, MDS_OK);

    st = subtree_map_add(map, "/data", SELF_ID, NULL,
                         SUBTREE_ACTIVE, 1);
    ASSERT_EQ(st, MDS_OK);

    seed_dir_inode(db, 700);

    /* Split to remote MDS (id=3) with NULL transport → must fail
     * and rollback the child entry. */
    st = subtree_split_execute(map, cat, "/data", "/data/hot",
                               700, 3, NULL, 0);
    ASSERT_NE(st, MDS_OK);

    /* /data/hot should NOT be in the map. */
    struct subtree_entry e;
    st = subtree_map_lookup_exact(map, "/data/hot", &e);
    ASSERT_EQ(st, MDS_ERR_NOTFOUND);

    subtree_map_destroy(map);
    mds_catalogue_close(db);
    cleanup_temp_db(db_path);
    free(db_path);
}

/* 10. Split + merge cycle leaves map in original state. */
static void test_no_duplicate_entries_after_split_merge(void)
{
    struct subtree_map *map = NULL;
    struct mds_catalogue *db = NULL;
    struct mds_catalogue *cat = NULL;
    char *db_path;
    enum mds_status st;

    db_path = make_temp_db_path();
    st = ((db = open_test_catalogue()) != NULL ? MDS_OK : MDS_ERR_IO);
    ASSERT_EQ(st, MDS_OK);
    cat = db;

    st = subtree_map_init(NULL, NULL, SELF_ID, "mds0.local",
                                 NULL, &map);
    ASSERT_EQ(st, MDS_OK);

    st = subtree_map_add(map, "/data", SELF_ID, NULL,
                         SUBTREE_ACTIVE, 1);
    ASSERT_EQ(st, MDS_OK);

    seed_dir_inode(db, 800);

    /* Split. */
    st = subtree_split_execute(map, cat, "/data", "/data/hot",
                               800, SELF_ID, NULL, 0);
    ASSERT_EQ(st, MDS_OK);

    /* Merge back. */
    st = subtree_merge_execute(map, db, "/data", "/data/hot", NULL);
    ASSERT_EQ(st, MDS_OK);

    /* Child gone. */
    struct subtree_entry e;
    st = subtree_map_lookup_exact(map, "/data/hot", &e);
    ASSERT_EQ(st, MDS_ERR_NOTFOUND);

    /* Parent still there. */
    st = subtree_map_lookup(map, "/data", &e);
    ASSERT_EQ(st, MDS_OK);

    /* Root still there. */
    st = subtree_map_lookup(map, "/", &e);
    ASSERT_EQ(st, MDS_OK);

    /* Can re-split (no duplicate). */
    st = subtree_split_execute(map, cat, "/data", "/data/hot",
                               800, SELF_ID, NULL, 0);
    ASSERT_EQ(st, MDS_OK);

    subtree_map_destroy(map);
    mds_catalogue_close(db);
    cleanup_temp_db(db_path);
    free(db_path);
}

/* -------------------------------------------------------------------
 * Op counter tests (37b)
 * ------------------------------------------------------------------- */

/* 11. Op counter increments correctly. */
static void test_op_counter_increments(void)
{
    struct subtree_map *map = NULL;
    enum mds_status st;
    uint64_t count = 0;

    st = subtree_map_init(NULL, NULL, SELF_ID, "mds0.local",
                                 NULL, &map);
    ASSERT_EQ(st, MDS_OK);

    st = subtree_map_add(map, "/data", SELF_ID, NULL,
                         SUBTREE_ACTIVE, 1);
    ASSERT_EQ(st, MDS_OK);

    /* Starts at 0. */
    st = subtree_map_get_stats(map, "/data", &count);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(count, 0ULL);

    /* Increment 100 times. */
    for (int i = 0; i < 100; i++) {
        st = subtree_map_inc_ops(map, "/data");
        ASSERT_EQ(st, MDS_OK);
    }

    st = subtree_map_get_stats(map, "/data", &count);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(count, 100ULL);

    subtree_map_destroy(map);
}

/* 12. Op counter reset to 0. */
static void test_op_counter_reset(void)
{
    struct subtree_map *map = NULL;
    enum mds_status st;
    uint64_t count = 0;

    st = subtree_map_init(NULL, NULL, SELF_ID, "mds0.local",
                                 NULL, &map);
    ASSERT_EQ(st, MDS_OK);

    st = subtree_map_add(map, "/data", SELF_ID, NULL,
                         SUBTREE_ACTIVE, 1);
    ASSERT_EQ(st, MDS_OK);

    for (int i = 0; i < 50; i++) {
        subtree_map_inc_ops(map, "/data");
    }

    st = subtree_map_reset_stats(map, "/data");
    ASSERT_EQ(st, MDS_OK);

    st = subtree_map_get_stats(map, "/data", &count);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(count, 0ULL);

    subtree_map_destroy(map);
}

/* 13. Op counter is thread-safe (no lost increments). */
#include <pthread.h>

#define THREAD_COUNT 4
#define INC_PER_THREAD 10000

struct inc_arg {
    struct subtree_map *map;
    const char *path;
};

static void *inc_thread(void *arg)
{
    struct inc_arg *ia = (struct inc_arg *)arg;

    for (int i = 0; i < INC_PER_THREAD; i++) {
        subtree_map_inc_ops(ia->map, ia->path);
    }
    return NULL;
}

static void test_op_counter_concurrent(void)
{
    struct subtree_map *map = NULL;
    enum mds_status st;
    uint64_t count = 0;
    pthread_t threads[THREAD_COUNT];
    struct inc_arg arg;

    st = subtree_map_init(NULL, NULL, SELF_ID, "mds0.local",
                                 NULL, &map);
    ASSERT_EQ(st, MDS_OK);

    st = subtree_map_add(map, "/data", SELF_ID, NULL,
                         SUBTREE_ACTIVE, 1);
    ASSERT_EQ(st, MDS_OK);

    arg.map  = map;
    arg.path = "/data";

    for (int i = 0; i < THREAD_COUNT; i++) {
        int rc = pthread_create(&threads[i], NULL, inc_thread, &arg);
        assert(rc == 0);
    }

    for (int i = 0; i < THREAD_COUNT; i++) {
        pthread_join(threads[i], NULL);
    }

    st = subtree_map_get_stats(map, "/data", &count);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(count, (uint64_t)(THREAD_COUNT * INC_PER_THREAD));

    subtree_map_destroy(map);
}


/* -------------------------------------------------------------------
 * Split evaluator tests (37c)
 * ------------------------------------------------------------------- */

/* 14. Evaluator no action below threshold. */
static void test_evaluator_no_action_below_threshold(void)
{
    struct subtree_map *map = NULL;
    struct split_evaluator *eval = NULL;
    enum mds_status st;

    st = subtree_map_init(NULL, NULL, SELF_ID, "mds0.local",
                                 NULL, &map);
    ASSERT_EQ(st, MDS_OK);

    st = subtree_map_add(map, "/data", SELF_ID, NULL,
                         SUBTREE_ACTIVE, 1);
    ASSERT_EQ(st, MDS_OK);

    /* Increment below threshold. */
    for (int i = 0; i < 100; i++) {
        subtree_map_inc_ops(map, "/data");
    }

    struct split_eval_cfg cfg = {
        .split_threshold    = 10000,
        .eval_interval_sec  = 300,
        .auto_split_enabled = true,
    };

    int rc = split_evaluator_start(map, NULL, NULL, &cfg, &eval);
    ASSERT_EQ(rc, 0);
    ASSERT_NE(eval, NULL);

    /* Run one evaluation cycle synchronously. */
    split_evaluator_run_once(eval);

    /* No proposals (100 < 10000). */
    ASSERT_EQ(split_evaluator_proposal_count(eval), 0ULL);

    split_evaluator_stop(eval);
    subtree_map_destroy(map);
}

/* 15. Evaluator identifies hot subtree. */
static void test_evaluator_identifies_hot_subtree(void)
{
    struct subtree_map *map = NULL;
    struct split_evaluator *eval = NULL;
    enum mds_status st;

    st = subtree_map_init(NULL, NULL, SELF_ID, "mds0.local",
                                 NULL, &map);
    ASSERT_EQ(st, MDS_OK);

    st = subtree_map_add(map, "/data", SELF_ID, NULL,
                         SUBTREE_ACTIVE, 1);
    ASSERT_EQ(st, MDS_OK);

    /* Push well above threshold. */
    for (int i = 0; i < 20000; i++) {
        subtree_map_inc_ops(map, "/data");
    }

    struct split_eval_cfg cfg = {
        .split_threshold    = 10000,
        .eval_interval_sec  = 300,
        .auto_split_enabled = true,
    };

    int rc = split_evaluator_start(map, NULL, NULL, &cfg, &eval);
    ASSERT_EQ(rc, 0);

    /* First hot interval — creates tracking entry but not yet PENDING
     * (default sustained_intervals = 2). */
    split_evaluator_run_once(eval);
    ASSERT_EQ(split_evaluator_proposal_count(eval), 0ULL);

    /* Second hot interval — promote to PENDING. */
    for (int j = 0; j < 20000; j++) {
        subtree_map_inc_ops(map, "/data");
    }
    split_evaluator_run_once(eval);

    /* Should have proposed a split after sustained threshold. */
    ASSERT_EQ(split_evaluator_proposal_count(eval), 1ULL);

    /* Counter should have been reset. */
    uint64_t count = 0;
    st = subtree_map_get_stats(map, "/data", &count);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(count, 0ULL);

    split_evaluator_stop(eval);
    subtree_map_destroy(map);
}

/* 16. Evaluator disabled by default. */
static void test_evaluator_disabled_by_default(void)
{
    struct subtree_map *map = NULL;
    struct split_evaluator *eval = NULL;
    enum mds_status st;

    st = subtree_map_init(NULL, NULL, SELF_ID, "mds0.local",
                                 NULL, &map);
    ASSERT_EQ(st, MDS_OK);

    struct split_eval_cfg cfg = {
        .split_threshold    = SPLIT_EVAL_DEFAULT_THRESHOLD,
        .eval_interval_sec  = SPLIT_EVAL_DEFAULT_INTERVAL,
        .auto_split_enabled = SPLIT_EVAL_DEFAULT_ENABLED,
    };

    int rc = split_evaluator_start(map, NULL, NULL, &cfg, &eval);
    ASSERT_EQ(rc, 0);
    /* eval should be NULL when disabled. */
    ASSERT_EQ(eval, NULL);

    /* proposal_count on NULL is safe (returns 0). */
    ASSERT_EQ(split_evaluator_proposal_count(eval), 0ULL);

    /* stop on NULL is safe. */
    split_evaluator_stop(eval);

    subtree_map_destroy(map);
}

/* -------------------------------------------------------------------
 * main
 * ------------------------------------------------------------------- */

/* Proposal store test forward declarations. */
static void test_proposal_list_after_hot(void);
static void test_proposal_approve(void);
static void test_proposal_reject(void);
static void test_proposal_approve_unknown_path(void);
static void test_auto_execute_toggle(void);
static void test_proposal_decay(void);
static void test_proposal_requires_consecutive_hot_intervals(void);
static void test_evaluator_respects_min_children(void);

int main(void)
{
    fprintf(stdout, "Running subtree split/merge unit tests:\n");

    RUN_TEST(test_split_creates_child_subtree);
    RUN_TEST(test_split_child_is_local);
    RUN_TEST(test_merge_removes_child);
    RUN_TEST(test_split_rejects_nonexistent_dir);
    RUN_TEST(test_split_rejects_non_directory);
    RUN_TEST(test_split_rejects_already_subtree_root);
    RUN_TEST(test_split_rejects_depth_exceeded);
    RUN_TEST(test_split_rejects_unowned_parent);
    RUN_TEST(test_migration_failure_rolls_back);
    RUN_TEST(test_no_duplicate_entries_after_split_merge);
    RUN_TEST(test_op_counter_increments);
    RUN_TEST(test_op_counter_reset);
    RUN_TEST(test_op_counter_concurrent);
    RUN_TEST(test_evaluator_no_action_below_threshold);
    RUN_TEST(test_evaluator_identifies_hot_subtree);
    RUN_TEST(test_evaluator_disabled_by_default);

    /* Proposal store tests (Tier 3 Phase 1) */
    RUN_TEST(test_proposal_list_after_hot);
    RUN_TEST(test_proposal_approve);
    RUN_TEST(test_proposal_reject);
    RUN_TEST(test_proposal_approve_unknown_path);
    RUN_TEST(test_auto_execute_toggle);
    RUN_TEST(test_proposal_decay);
    RUN_TEST(test_proposal_requires_consecutive_hot_intervals);
    RUN_TEST(test_evaluator_respects_min_children);

    fprintf(stdout, "\n  %d/%d tests passed.\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}


/* -------------------------------------------------------------------
 * Proposal store tests (Tier 3 Phase 1)
 * ------------------------------------------------------------------- */

/* 17. Proposal listing after hot detection. */
static void test_proposal_list_after_hot(void)
{
    struct subtree_map *map = NULL;
    struct split_evaluator *eval = NULL;
    enum mds_status st;

    st = subtree_map_init(NULL, NULL, SELF_ID, "mds0.local",
                                 NULL, &map);
    ASSERT_EQ(st, MDS_OK);
    st = subtree_map_add(map, "/hot", SELF_ID, NULL,
                         SUBTREE_ACTIVE, 1);
    ASSERT_EQ(st, MDS_OK);

    for (int i = 0; i < 20000; i++) {
        subtree_map_inc_ops(map, "/hot");
    }

    struct split_eval_cfg cfg = {
        .split_threshold    = 10000,
        .eval_interval_sec  = 300,
        .auto_split_enabled = true,
        .sustained_intervals = 1,
    };

    int rc = split_evaluator_start(map, NULL, NULL, &cfg, &eval);
    ASSERT_EQ(rc, 0);
    split_evaluator_run_once(eval);

    struct split_proposal *arr = NULL;
    uint32_t count = 0;
    rc = split_evaluator_list_proposals(eval, &arr, &count);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(count, 1U);
    ASSERT_EQ(strcmp(arr[0].path, "/hot"), 0);
    ASSERT_EQ(arr[0].state, PROPOSAL_PENDING);
    ASSERT_TRUE(arr[0].sampled_ops >= 20000ULL);
    ASSERT_TRUE(arr[0].hot_intervals >= 1U);

    free(arr);
    split_evaluator_stop(eval);
    subtree_map_destroy(map);
}

/* 18. Approve transitions PENDING -> APPROVED. */
static void test_proposal_approve(void)
{
    struct subtree_map *map = NULL;
    struct split_evaluator *eval = NULL;
    enum mds_status st;

    st = subtree_map_init(NULL, NULL, SELF_ID, "mds0.local",
                                 NULL, &map);
    ASSERT_EQ(st, MDS_OK);
    st = subtree_map_add(map, "/ap", SELF_ID, NULL,
                         SUBTREE_ACTIVE, 1);
    ASSERT_EQ(st, MDS_OK);

    for (int i = 0; i < 15000; i++) {
        subtree_map_inc_ops(map, "/ap");
    }

    struct split_eval_cfg cfg = {
        .split_threshold    = 10000,
        .eval_interval_sec  = 300,
        .auto_split_enabled = true,
        .sustained_intervals = 1,
    };

    int rc = split_evaluator_start(map, NULL, NULL, &cfg, &eval);
    ASSERT_EQ(rc, 0);
    split_evaluator_run_once(eval);

    /* Approve the proposal. */
    rc = split_evaluator_approve(eval, "/ap");
    ASSERT_EQ(rc, 0);

    struct split_proposal *arr = NULL;
    uint32_t count = 0;
    rc = split_evaluator_list_proposals(eval, &arr, &count);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(count, 1U);
    ASSERT_EQ(arr[0].state, PROPOSAL_APPROVED);

    free(arr);
    split_evaluator_stop(eval);
    subtree_map_destroy(map);
}

/* 19. Reject transitions PENDING -> REJECTED with reason. */
static void test_proposal_reject(void)
{
    struct subtree_map *map = NULL;
    struct split_evaluator *eval = NULL;
    enum mds_status st;

    st = subtree_map_init(NULL, NULL, SELF_ID, "mds0.local",
                                 NULL, &map);
    ASSERT_EQ(st, MDS_OK);
    st = subtree_map_add(map, "/rj", SELF_ID, NULL,
                         SUBTREE_ACTIVE, 1);
    ASSERT_EQ(st, MDS_OK);

    for (int i = 0; i < 15000; i++) {
        subtree_map_inc_ops(map, "/rj");
    }

    struct split_eval_cfg cfg = {
        .split_threshold    = 10000,
        .eval_interval_sec  = 300,
        .auto_split_enabled = true,
        .sustained_intervals = 1,
    };

    int rc = split_evaluator_start(map, NULL, NULL, &cfg, &eval);
    ASSERT_EQ(rc, 0);
    split_evaluator_run_once(eval);

    rc = split_evaluator_reject(eval, "/rj", "not enough peers");
    ASSERT_EQ(rc, 0);

    struct split_proposal *arr = NULL;
    uint32_t count = 0;
    rc = split_evaluator_list_proposals(eval, &arr, &count);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(count, 1U);
    ASSERT_EQ(arr[0].state, PROPOSAL_REJECTED);
    ASSERT_TRUE(strstr(arr[0].reject_reason, "not enough peers") != NULL);

    free(arr);
    split_evaluator_stop(eval);
    subtree_map_destroy(map);
}

/* 20. Approve on nonexistent path returns -1. */
static void test_proposal_approve_unknown_path(void)
{
    struct subtree_map *map = NULL;
    struct split_evaluator *eval = NULL;
    enum mds_status st;

    st = subtree_map_init(NULL, NULL, SELF_ID, "mds0.local",
                                 NULL, &map);
    ASSERT_EQ(st, MDS_OK);

    struct split_eval_cfg cfg = {
        .split_threshold    = 10000,
        .eval_interval_sec  = 300,
        .auto_split_enabled = true,
        .sustained_intervals = 1,
    };

    int rc = split_evaluator_start(map, NULL, NULL, &cfg, &eval);
    ASSERT_EQ(rc, 0);

    rc = split_evaluator_approve(eval, "/nonexistent");
    ASSERT_EQ(rc, -1);

    split_evaluator_stop(eval);
    subtree_map_destroy(map);
}

/* 21. Auto-execute toggle. */
static void test_auto_execute_toggle(void)
{
    struct subtree_map *map = NULL;
    struct split_evaluator *eval = NULL;
    enum mds_status st;

    st = subtree_map_init(NULL, NULL, SELF_ID, "mds0.local",
                                 NULL, &map);
    ASSERT_EQ(st, MDS_OK);

    struct split_eval_cfg cfg = {
        .split_threshold    = 10000,
        .eval_interval_sec  = 300,
        .auto_split_enabled = true,
        .auto_execute       = false,
        .sustained_intervals = 1,
    };

    int rc = split_evaluator_start(map, NULL, NULL, &cfg, &eval);
    ASSERT_EQ(rc, 0);

    ASSERT_EQ(split_evaluator_get_auto_execute(eval), false);
    split_evaluator_set_auto_execute(eval, true);
    ASSERT_EQ(split_evaluator_get_auto_execute(eval), true);
    split_evaluator_set_auto_execute(eval, false);
    ASSERT_EQ(split_evaluator_get_auto_execute(eval), false);

    split_evaluator_stop(eval);
    subtree_map_destroy(map);
}

/* 22. Hot intervals decay below threshold. */
static void test_proposal_decay(void)
{
    struct subtree_map *map = NULL;
    struct split_evaluator *eval = NULL;
    enum mds_status st;

    st = subtree_map_init(NULL, NULL, SELF_ID, "mds0.local",
                                 NULL, &map);
    ASSERT_EQ(st, MDS_OK);
    st = subtree_map_add(map, "/dk", SELF_ID, NULL,
                         SUBTREE_ACTIVE, 1);
    ASSERT_EQ(st, MDS_OK);

    /* First pass: hot. */
    for (int i = 0; i < 15000; i++) {
        subtree_map_inc_ops(map, "/dk");
    }

    struct split_eval_cfg cfg = {
        .split_threshold    = 10000,
        .eval_interval_sec  = 300,
        .auto_split_enabled = true,
        .sustained_intervals = 1,
    };

    int rc = split_evaluator_start(map, NULL, NULL, &cfg, &eval);
    ASSERT_EQ(rc, 0);
    split_evaluator_run_once(eval);

    /* Second pass: cold — hot_intervals should decay. */
    split_evaluator_run_once(eval);

    struct split_proposal *arr = NULL;
    uint32_t count = 0;
    rc = split_evaluator_list_proposals(eval, &arr, &count);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(count, 1U);
    /* hot_intervals went from 1 to 0 after cold pass. */
    ASSERT_EQ(arr[0].hot_intervals, 0U);

    free(arr);
    split_evaluator_stop(eval);
    subtree_map_destroy(map);
}

/* 23. Sustained intervals must be consecutive. */
static void test_proposal_requires_consecutive_hot_intervals(void)
{
    struct subtree_map *map = NULL;
    struct split_evaluator *eval = NULL;
    struct split_proposal *arr = NULL;
    uint32_t count = 0;
    enum mds_status st;

    st = subtree_map_init(NULL, NULL, SELF_ID, "mds0.local",
                                 NULL, &map);
    ASSERT_EQ(st, MDS_OK);
    st = subtree_map_add(map, "/gap", SELF_ID, NULL,
                         SUBTREE_ACTIVE, 1);
    ASSERT_EQ(st, MDS_OK);

    struct split_eval_cfg cfg = {
        .split_threshold    = 10000,
        .eval_interval_sec  = 300,
        .auto_split_enabled = true,
        .sustained_intervals = 2,
    };

    int rc = split_evaluator_start(map, NULL, NULL, &cfg, &eval);
    ASSERT_EQ(rc, 0);

    for (int i = 0; i < 15000; i++) {
        subtree_map_inc_ops(map, "/gap");
    }
    split_evaluator_run_once(eval);
    ASSERT_EQ(split_evaluator_proposal_count(eval), 0ULL);

    /* Cold interval breaks the sequence. */
    split_evaluator_run_once(eval);
    ASSERT_EQ(split_evaluator_proposal_count(eval), 0ULL);

    for (int i = 0; i < 15000; i++) {
        subtree_map_inc_ops(map, "/gap");
    }
    split_evaluator_run_once(eval);
    ASSERT_EQ(split_evaluator_proposal_count(eval), 0ULL);

    rc = split_evaluator_list_proposals(eval, &arr, &count);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(count, 1U);
    ASSERT_EQ(arr[0].state, PROPOSAL_COOLDOWN);
    ASSERT_EQ(arr[0].hot_intervals, 1U);

    free(arr);
    split_evaluator_stop(eval);
    subtree_map_destroy(map);
}

/* 24. min_children skips small directories when enabled. */
static void test_evaluator_respects_min_children(void)
{
    struct subtree_map *map = NULL;
    struct split_evaluator *eval = NULL;
    struct split_proposal *arr = NULL;
    struct mds_catalogue *db = NULL;
    struct mds_catalogue *cat = NULL;
    struct mds_inode dir_inode;
    struct mds_inode tmp_inode;
    char *db_path;
    uint32_t count = 0;
    enum mds_status st;

    db_path = make_temp_db_path();
    st = ((db = open_test_catalogue()) != NULL ? MDS_OK : MDS_ERR_IO);
    ASSERT_EQ(st, MDS_OK);
    cat = db;

    st = subtree_map_init(NULL, NULL, SELF_ID, "mds0.local",
                                 NULL, &map);
    ASSERT_EQ(st, MDS_OK);

    st = mds_cat_ns_create(db, NULL, MDS_FILEID_ROOT, "data",
                       MDS_FTYPE_DIR, 0755, 0, 0,
                       NULL, &dir_inode);
    ASSERT_EQ(st, MDS_OK);
    st = test_create_file(db, dir_inode.fileid, "onlychild",
                          0644, &tmp_inode);
    ASSERT_EQ(st, MDS_OK);

    st = subtree_map_add(map, "/data", SELF_ID, NULL,
                         SUBTREE_ACTIVE, 1);
    ASSERT_EQ(st, MDS_OK);

    for (int i = 0; i < 15000; i++) {
        subtree_map_inc_ops(map, "/data");
    }

    struct split_eval_cfg cfg = {
        .split_threshold    = 10000,
        .eval_interval_sec  = 300,
        .auto_split_enabled = true,
        .sustained_intervals = 1,
        .min_children       = 2,
    };

    int rc = split_evaluator_start(map, cat, NULL, &cfg, &eval);
    ASSERT_EQ(rc, 0);
    split_evaluator_run_once(eval);

    ASSERT_EQ(split_evaluator_proposal_count(eval), 0ULL);
    rc = split_evaluator_list_proposals(eval, &arr, &count);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(count, 0U);

    free(arr);
    split_evaluator_stop(eval);
    subtree_map_destroy(map);
    mds_catalogue_close(db);
    cleanup_temp_db(db_path);
    free(db_path);
}
