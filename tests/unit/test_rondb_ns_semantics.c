/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_rondb_ns_semantics.c -- Backend-neutral pins for the namespace
 * and recovery semantics the RonDB shim implements inside its
 * transactions, exercised here on the reference in-memory backend so
 * both backends answer identically:
 *
 *   ns_link    NOTFOUND for a missing target, EXISTS for a name
 *              collision, ISDIR for a directory target, NOTDIR for a
 *              non-directory parent -- and every failure leaves the
 *              store untouched (no dirent, no nlink / change bump);
 *              success bumps the target's nlink, change and ctime and
 *              the parent's change in the same call.
 *   ns_remove  RMDIR of a non-empty directory is refused inside the
 *              mutating operation (C3) with nothing changed; RMDIR of
 *              an empty directory deletes the directory inode row and
 *              drops the parent's ".." link; a replay is NOTFOUND.
 *   ns_rename  a rename over a non-empty directory is NOTEMPTY, decided
 *              inside the mutating operation, with nothing changed; a
 *              rename over an empty directory deletes the victim's
 *              inode row, drops its ".." link from the destination
 *              parent and rebinds the name to the source inode.
 *   ns_remove_known_gc
 *              the caller's child snapshot is re-validated inside the
 *              mutating operation: a name that now resolves to another
 *              inode (the snapshot's inode still alive under a second
 *              name) and a snapshot whose link count no longer predicts
 *              the final-link answer are both refused with STALE and
 *              nothing changes -- the wrong inode is never deleted, no
 *              GC row lands, the parent is untouched.
 *   recovery   rows record the owning MDS; recovery_list(owner)
 *              returns that owner's rows plus unassigned (owner 0)
 *              rows and never another owner's; 0 lists every row.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "pnfs_mds.h"
#include "mds_catalogue.h"
#include "mds_coordination.h"
#include "catalogue_memdb.h"
#include "test_helpers.h"

/* ----------------------------------------------------------------------- */

static int tests_run;
static int tests_passed;
static int test_failed;

#define ASSERT_EQ(a, b) do { \
    long long _a = (long long)(a); \
    long long _b = (long long)(b); \
    if (_a != _b) { \
        fprintf(stderr, "  FAIL %s:%d: %s (%lld) != %s (%lld)\n", \
                __FILE__, __LINE__, #a, _a, #b, _b); \
        test_failed = 1; \
        return; \
    } \
} while (0)

#define ASSERT_TRUE(cond) do { \
    if (!(cond)) { \
        fprintf(stderr, "  FAIL %s:%d: !(%s)\n", __FILE__, __LINE__, #cond); \
        test_failed = 1; \
        return; \
    } \
} while (0)

#define RUN_TEST(fn) do { \
    fprintf(stdout, "  %-52s", #fn); \
    fflush(stdout); \
    tests_run++; \
    test_failed = 0; \
    fn(); \
    if (test_failed == 0) { \
        tests_passed++; \
        fprintf(stdout, "OK\n"); \
    } else { \
        fprintf(stdout, "FAILED\n"); \
    } \
} while (0)

/* ----------------------------------------------------------------------- */

struct count_ctx {
    uint32_t count;
};

static int count_readdir_cb(const struct mds_cat_dirent *entry, void *arg)
{
    struct count_ctx *c = arg;

    (void)entry;
    c->count++;
    return 0;
}

static uint32_t dir_count(struct mds_catalogue *cat, uint64_t dir)
{
    struct count_ctx c = { 0 };

    if (mds_cat_ns_readdir(cat, dir, NULL, 0, NULL, count_readdir_cb, &c) != MDS_OK) {
        return UINT32_MAX;
    }
    return c.count;
}

/* A directory "d" under the root holding one regular file "f". */
struct fixture {
    struct mds_catalogue *cat;
    struct mds_inode root;
    struct mds_inode d;
    struct mds_inode f;
};

static bool fixture_open(struct fixture *fx)
{
    memset(fx, 0, sizeof(*fx));
    fx->cat = catalogue_memdb_open();
    if (fx->cat == NULL) {
        return false;
    }
    if (mds_cat_ns_create(fx->cat, NULL, MDS_FILEID_ROOT, "d", MDS_FTYPE_DIR, 0755,
                          0, 0, NULL, &fx->d) != MDS_OK ||
        mds_cat_ns_create(fx->cat, NULL, fx->d.fileid, "f", MDS_FTYPE_REG, 0644,
                          0, 0, NULL, &fx->f) != MDS_OK ||
        mds_cat_ns_getattr(fx->cat, MDS_FILEID_ROOT, &fx->root) != MDS_OK ||
        mds_cat_ns_getattr(fx->cat, fx->d.fileid, &fx->d) != MDS_OK ||
        mds_cat_ns_getattr(fx->cat, fx->f.fileid, &fx->f) != MDS_OK) {
        mds_catalogue_close(fx->cat);
        fx->cat = NULL;
        return false;
    }
    return true;
}

static void fixture_close(struct fixture *fx)
{
    mds_catalogue_close(fx->cat);
    fx->cat = NULL;
}

/* -----------------------------------------------------------------------
 * ns_link
 * ----------------------------------------------------------------------- */

/* A failed link leaves parent and target exactly as they were: same
 * change counter, same nlink, no new name. */
static bool link_failure_left_no_trace(struct fixture *fx, const char *name)
{
    struct mds_inode d;
    struct mds_inode f;
    struct mds_inode seen;

    if (mds_cat_ns_getattr(fx->cat, fx->d.fileid, &d) != MDS_OK ||
        mds_cat_ns_getattr(fx->cat, fx->f.fileid, &f) != MDS_OK) {
        return false;
    }
    return d.change == fx->d.change && d.nlink == fx->d.nlink &&
           f.change == fx->f.change && f.nlink == fx->f.nlink &&
           mds_cat_ns_lookup(fx->cat, fx->d.fileid, name, &seen) == MDS_ERR_NOTFOUND &&
           dir_count(fx->cat, fx->d.fileid) == 1U;
}

static void test_link_missing_target_is_notfound(void)
{
    struct fixture fx;

    ASSERT_TRUE(fixture_open(&fx));
    ASSERT_EQ(mds_cat_ns_link(fx.cat, NULL, fx.d.fileid, "l", 424242), MDS_ERR_NOTFOUND);
    ASSERT_TRUE(link_failure_left_no_trace(&fx, "l"));
    /* Missing parent is NOTFOUND as well, and the target is untouched. */
    ASSERT_EQ(mds_cat_ns_link(fx.cat, NULL, 424242, "l", fx.f.fileid), MDS_ERR_NOTFOUND);
    ASSERT_TRUE(link_failure_left_no_trace(&fx, "l"));
    fixture_close(&fx);
}

static void test_link_name_collision_is_exists(void)
{
    struct fixture fx;
    struct mds_inode other;
    struct mds_inode seen;

    ASSERT_TRUE(fixture_open(&fx));
    ASSERT_EQ(mds_cat_ns_create(fx.cat, NULL, MDS_FILEID_ROOT, "other", MDS_FTYPE_REG,
                                0644, 0, 0, NULL, &other), MDS_OK);
    ASSERT_EQ(mds_cat_ns_getattr(fx.cat, fx.d.fileid, &fx.d), MDS_OK);

    /* "f" already names fx.f; linking `other` under that name collides
     * and neither inode changes. */
    ASSERT_EQ(mds_cat_ns_link(fx.cat, NULL, fx.d.fileid, "f", other.fileid), MDS_ERR_EXISTS);
    ASSERT_EQ(mds_cat_ns_lookup(fx.cat, fx.d.fileid, "f", &seen), MDS_OK);
    ASSERT_EQ(seen.fileid, fx.f.fileid);
    ASSERT_EQ(seen.nlink, 1U);
    ASSERT_EQ(mds_cat_ns_getattr(fx.cat, other.fileid, &seen), MDS_OK);
    ASSERT_EQ(seen.nlink, 1U);
    ASSERT_EQ(seen.change, other.change);
    ASSERT_EQ(mds_cat_ns_getattr(fx.cat, fx.d.fileid, &seen), MDS_OK);
    ASSERT_EQ(seen.change, fx.d.change);
    /* Replay of a committed link is EXISTS too, with nlink bumped once. */
    ASSERT_EQ(mds_cat_ns_link(fx.cat, NULL, fx.d.fileid, "l", other.fileid), MDS_OK);
    ASSERT_EQ(mds_cat_ns_link(fx.cat, NULL, fx.d.fileid, "l", other.fileid), MDS_ERR_EXISTS);
    ASSERT_EQ(mds_cat_ns_getattr(fx.cat, other.fileid, &seen), MDS_OK);
    ASSERT_EQ(seen.nlink, 2U);
    fixture_close(&fx);
}

static void test_link_directory_target_is_isdir(void)
{
    struct fixture fx;
    struct mds_inode seen;

    ASSERT_TRUE(fixture_open(&fx));
    ASSERT_EQ(mds_cat_ns_link(fx.cat, NULL, MDS_FILEID_ROOT, "dl", fx.d.fileid), MDS_ERR_ISDIR);
    ASSERT_EQ(mds_cat_ns_lookup(fx.cat, MDS_FILEID_ROOT, "dl", &seen), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_ns_getattr(fx.cat, fx.d.fileid, &seen), MDS_OK);
    ASSERT_EQ(seen.nlink, fx.d.nlink);
    ASSERT_EQ(seen.change, fx.d.change);
    ASSERT_EQ(mds_cat_ns_getattr(fx.cat, MDS_FILEID_ROOT, &seen), MDS_OK);
    ASSERT_EQ(seen.change, fx.root.change);
    fixture_close(&fx);
}

static void test_link_non_directory_parent_is_notdir(void)
{
    struct fixture fx;
    struct mds_inode other;
    struct mds_inode seen;

    ASSERT_TRUE(fixture_open(&fx));
    ASSERT_EQ(mds_cat_ns_create(fx.cat, NULL, MDS_FILEID_ROOT, "other", MDS_FTYPE_REG,
                                0644, 0, 0, NULL, &other), MDS_OK);
    ASSERT_EQ(mds_cat_ns_link(fx.cat, NULL, fx.f.fileid, "x", other.fileid), MDS_ERR_NOTDIR);
    ASSERT_EQ(mds_cat_ns_getattr(fx.cat, other.fileid, &seen), MDS_OK);
    ASSERT_EQ(seen.nlink, 1U);
    ASSERT_EQ(seen.change, other.change);
    ASSERT_EQ(mds_cat_ns_getattr(fx.cat, fx.f.fileid, &seen), MDS_OK);
    ASSERT_EQ(seen.change, fx.f.change);
    fixture_close(&fx);
}

/* One call: the new name resolves to the target, the target's nlink,
 * change and ctime moved together, the parent's change moved. */
static void test_link_success_is_one_step(void)
{
    struct fixture fx;
    struct mds_inode seen;
    struct mds_inode parent;

    ASSERT_TRUE(fixture_open(&fx));
    ASSERT_EQ(mds_cat_ns_link(fx.cat, NULL, fx.d.fileid, "l", fx.f.fileid), MDS_OK);
    ASSERT_EQ(mds_cat_ns_lookup(fx.cat, fx.d.fileid, "l", &seen), MDS_OK);
    ASSERT_EQ(seen.fileid, fx.f.fileid);
    ASSERT_EQ(seen.nlink, 2U);
    ASSERT_EQ(seen.change, fx.f.change + 1);
    ASSERT_TRUE(seen.ctime.tv_sec > fx.f.ctime.tv_sec ||
                (seen.ctime.tv_sec == fx.f.ctime.tv_sec &&
                 seen.ctime.tv_nsec >= fx.f.ctime.tv_nsec));
    ASSERT_EQ(mds_cat_ns_getattr(fx.cat, fx.d.fileid, &parent), MDS_OK);
    ASSERT_EQ(parent.change, fx.d.change + 1);
    ASSERT_EQ(parent.nlink, fx.d.nlink);   /* hard links never touch parent nlink */
    ASSERT_EQ(dir_count(fx.cat, fx.d.fileid), 2U);
    fixture_close(&fx);
}

/* -----------------------------------------------------------------------
 * ns_remove on directories
 * ----------------------------------------------------------------------- */

static void test_rmdir_non_empty_refused_without_change(void)
{
    struct fixture fx;
    struct mds_inode seen;

    ASSERT_TRUE(fixture_open(&fx));
    ASSERT_EQ(mds_cat_ns_remove(fx.cat, NULL, MDS_FILEID_ROOT, "d"), MDS_ERR_NOTEMPTY);
    /* Nothing moved: name, directory inode, child and parent counters. */
    ASSERT_EQ(mds_cat_ns_lookup(fx.cat, MDS_FILEID_ROOT, "d", &seen), MDS_OK);
    ASSERT_EQ(seen.fileid, fx.d.fileid);
    ASSERT_EQ(seen.nlink, fx.d.nlink);
    ASSERT_EQ(seen.change, fx.d.change);
    ASSERT_EQ(mds_cat_ns_lookup(fx.cat, fx.d.fileid, "f", &seen), MDS_OK);
    ASSERT_EQ(seen.fileid, fx.f.fileid);
    ASSERT_EQ(mds_cat_ns_getattr(fx.cat, MDS_FILEID_ROOT, &seen), MDS_OK);
    ASSERT_EQ(seen.nlink, fx.root.nlink);
    ASSERT_EQ(seen.change, fx.root.change);
    fixture_close(&fx);
}

static void test_rmdir_empty_deletes_inode_and_parent_link(void)
{
    struct fixture fx;
    struct mds_inode seen;

    ASSERT_TRUE(fixture_open(&fx));
    ASSERT_EQ(mds_cat_ns_remove(fx.cat, NULL, fx.d.fileid, "f"), MDS_OK);
    ASSERT_EQ(mds_cat_ns_getattr(fx.cat, MDS_FILEID_ROOT, &fx.root), MDS_OK);

    ASSERT_EQ(mds_cat_ns_remove(fx.cat, NULL, MDS_FILEID_ROOT, "d"), MDS_OK);
    /* The directory inode row is gone (a stale FH must go STALE, not
     * keep resolving an nlink=1 orphan), the name is gone, and the
     * parent lost the ".." link. */
    ASSERT_EQ(mds_cat_ns_getattr(fx.cat, fx.d.fileid, &seen), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_ns_lookup(fx.cat, MDS_FILEID_ROOT, "d", &seen), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_ns_getattr(fx.cat, MDS_FILEID_ROOT, &seen), MDS_OK);
    ASSERT_EQ(seen.nlink, fx.root.nlink - 1);
    ASSERT_EQ(seen.change, fx.root.change + 1);
    /* Replayed RMDIR: NOTFOUND, counters advance once. */
    ASSERT_EQ(mds_cat_ns_remove(fx.cat, NULL, MDS_FILEID_ROOT, "d"), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_ns_getattr(fx.cat, MDS_FILEID_ROOT, &fx.root), MDS_OK);
    ASSERT_EQ(fx.root.nlink, seen.nlink);
    ASSERT_EQ(fx.root.change, seen.change);
    fixture_close(&fx);
}

/* Emptiness is re-decided by every removal path that can take a
 * directory: the resolved-child variant behaves like the plain one. */
static void test_rmdir_known_child_non_empty_refused(void)
{
    struct fixture fx;
    struct mds_inode seen;

    ASSERT_TRUE(fixture_open(&fx));
    ASSERT_EQ(mds_cat_ns_remove_known(fx.cat, NULL, MDS_FILEID_ROOT, "d", &fx.d, 0),
              MDS_ERR_NOTEMPTY);
    ASSERT_EQ(mds_cat_ns_lookup(fx.cat, fx.d.fileid, "f", &seen), MDS_OK);
    ASSERT_EQ(mds_cat_ns_remove(fx.cat, NULL, fx.d.fileid, "f"), MDS_OK);
    ASSERT_EQ(mds_cat_ns_remove_known(fx.cat, NULL, MDS_FILEID_ROOT, "d", &fx.d, 0),
              MDS_OK);
    ASSERT_EQ(mds_cat_ns_getattr(fx.cat, fx.d.fileid, &seen), MDS_ERR_NOTFOUND);
    fixture_close(&fx);
}

/* -----------------------------------------------------------------------
 * ns_rename over directories
 * ----------------------------------------------------------------------- */

/* Renaming empty "e" onto "d", which still holds "f": refused, and
 * both names, both directory inodes and the parent are untouched. */
static void test_rename_over_non_empty_dir_refused_without_change(void)
{
    struct fixture fx;
    struct mds_inode e;
    struct mds_inode seen;

    ASSERT_TRUE(fixture_open(&fx));
    ASSERT_EQ(mds_cat_ns_create(fx.cat, NULL, MDS_FILEID_ROOT, "e", MDS_FTYPE_DIR, 0755,
                                0, 0, NULL, &e), MDS_OK);
    ASSERT_EQ(mds_cat_ns_getattr(fx.cat, MDS_FILEID_ROOT, &fx.root), MDS_OK);

    ASSERT_EQ(mds_cat_ns_rename(fx.cat, NULL, MDS_FILEID_ROOT, "e", MDS_FILEID_ROOT, "d"),
              MDS_ERR_NOTEMPTY);
    ASSERT_EQ(mds_cat_ns_lookup(fx.cat, MDS_FILEID_ROOT, "e", &seen), MDS_OK);
    ASSERT_EQ(seen.fileid, e.fileid);
    ASSERT_EQ(seen.change, e.change);
    ASSERT_EQ(mds_cat_ns_lookup(fx.cat, MDS_FILEID_ROOT, "d", &seen), MDS_OK);
    ASSERT_EQ(seen.fileid, fx.d.fileid);
    ASSERT_EQ(seen.nlink, fx.d.nlink);
    ASSERT_EQ(seen.change, fx.d.change);
    ASSERT_EQ(mds_cat_ns_lookup(fx.cat, fx.d.fileid, "f", &seen), MDS_OK);
    ASSERT_EQ(seen.fileid, fx.f.fileid);
    ASSERT_EQ(mds_cat_ns_getattr(fx.cat, MDS_FILEID_ROOT, &seen), MDS_OK);
    ASSERT_EQ(seen.nlink, fx.root.nlink);
    ASSERT_EQ(seen.change, fx.root.change);
    ASSERT_EQ(dir_count(fx.cat, MDS_FILEID_ROOT), 2U);
    fixture_close(&fx);
}

/* Renaming "d" (non-empty source is fine) onto empty "e" in the same
 * parent: the victim's inode row is gone, "d" is gone, "e" resolves to
 * the source inode with its contents, and the parent lost one ".."
 * link and advanced its change counter once. */
static void test_rename_over_empty_dir_deletes_victim(void)
{
    struct fixture fx;
    struct mds_inode e;
    struct mds_inode seen;

    ASSERT_TRUE(fixture_open(&fx));
    ASSERT_EQ(mds_cat_ns_create(fx.cat, NULL, MDS_FILEID_ROOT, "e", MDS_FTYPE_DIR, 0755,
                                0, 0, NULL, &e), MDS_OK);
    ASSERT_EQ(mds_cat_ns_getattr(fx.cat, MDS_FILEID_ROOT, &fx.root), MDS_OK);

    ASSERT_EQ(mds_cat_ns_rename(fx.cat, NULL, MDS_FILEID_ROOT, "d", MDS_FILEID_ROOT, "e"),
              MDS_OK);
    ASSERT_EQ(mds_cat_ns_getattr(fx.cat, e.fileid, &seen), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_ns_lookup(fx.cat, MDS_FILEID_ROOT, "d", &seen), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_ns_lookup(fx.cat, MDS_FILEID_ROOT, "e", &seen), MDS_OK);
    ASSERT_EQ(seen.fileid, fx.d.fileid);
    ASSERT_EQ(seen.nlink, fx.d.nlink);
    ASSERT_EQ(mds_cat_ns_lookup(fx.cat, fx.d.fileid, "f", &seen), MDS_OK);
    ASSERT_EQ(seen.fileid, fx.f.fileid);
    ASSERT_EQ(mds_cat_ns_getattr(fx.cat, MDS_FILEID_ROOT, &seen), MDS_OK);
    ASSERT_EQ(seen.nlink, fx.root.nlink - 1);
    ASSERT_EQ(seen.change, fx.root.change + 1);
    ASSERT_EQ(dir_count(fx.cat, MDS_FILEID_ROOT), 1U);
    /* A replay finds no source: NOTFOUND, nothing else moves. */
    ASSERT_EQ(mds_cat_ns_rename(fx.cat, NULL, MDS_FILEID_ROOT, "d", MDS_FILEID_ROOT, "e"),
              MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_ns_getattr(fx.cat, MDS_FILEID_ROOT, &fx.root), MDS_OK);
    ASSERT_EQ(fx.root.nlink, seen.nlink);
    ASSERT_EQ(fx.root.change, seen.change);
    fixture_close(&fx);
}

/* Cross-directory: "d" moves onto empty "x/y".  The victim is gone,
 * the source inode now hangs under "x" (which lost y's ".." link and
 * gained d's), and the old parent lost d's. */
static void test_rename_over_empty_dir_cross_directory(void)
{
    struct fixture fx;
    struct mds_inode x;
    struct mds_inode y;
    struct mds_inode seen;

    ASSERT_TRUE(fixture_open(&fx));
    ASSERT_EQ(mds_cat_ns_create(fx.cat, NULL, MDS_FILEID_ROOT, "x", MDS_FTYPE_DIR, 0755,
                                0, 0, NULL, &x), MDS_OK);
    ASSERT_EQ(mds_cat_ns_create(fx.cat, NULL, x.fileid, "y", MDS_FTYPE_DIR, 0755,
                                0, 0, NULL, &y), MDS_OK);
    ASSERT_EQ(mds_cat_ns_getattr(fx.cat, MDS_FILEID_ROOT, &fx.root), MDS_OK);
    ASSERT_EQ(mds_cat_ns_getattr(fx.cat, x.fileid, &x), MDS_OK);

    ASSERT_EQ(mds_cat_ns_rename(fx.cat, NULL, MDS_FILEID_ROOT, "d", x.fileid, "y"), MDS_OK);
    ASSERT_EQ(mds_cat_ns_getattr(fx.cat, y.fileid, &seen), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_ns_lookup(fx.cat, MDS_FILEID_ROOT, "d", &seen), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_ns_lookup(fx.cat, x.fileid, "y", &seen), MDS_OK);
    ASSERT_EQ(seen.fileid, fx.d.fileid);
    ASSERT_EQ(seen.parent_fileid, x.fileid);
    ASSERT_EQ(mds_cat_ns_lookup(fx.cat, fx.d.fileid, "f", &seen), MDS_OK);
    ASSERT_EQ(seen.fileid, fx.f.fileid);
    ASSERT_EQ(mds_cat_ns_getattr(fx.cat, x.fileid, &seen), MDS_OK);
    ASSERT_EQ(seen.nlink, x.nlink);          /* -y +d */
    ASSERT_EQ(seen.change, x.change + 1);
    ASSERT_EQ(mds_cat_ns_getattr(fx.cat, MDS_FILEID_ROOT, &seen), MDS_OK);
    ASSERT_EQ(seen.nlink, fx.root.nlink - 1); /* -d */
    ASSERT_EQ(seen.change, fx.root.change + 1);
    fixture_close(&fx);
}

/* -----------------------------------------------------------------------
 * ns_remove_known_gc snapshot guards
 * ----------------------------------------------------------------------- */

static void gc_entry_init(struct mds_ds_map_entry *e)
{
    memset(e, 0, sizeof(*e));
    e->ds_id = 1;
    e->nfs_fh_len = 4;
    e->nfs_fh[0] = 0xAB;
}

/* "g" named c1, which was then linked as "h" and unlinked as "g", and
 * "g" was re-created as c2.  A remove that still carries the c1
 * snapshot must not delete c2's name, must not delete c1 (alive under
 * "h") and must not queue c1's DS objects for collection. */
static void test_remove_known_gc_stale_snapshot_alive_elsewhere(void)
{
    struct fixture fx;
    struct mds_inode c1;
    struct mds_inode c2;
    struct mds_inode seen;
    struct mds_ds_map_entry gc;
    uint32_t gc_before = 0;
    uint32_t gc_after = 0;
    bool folded = true;

    ASSERT_TRUE(fixture_open(&fx));
    ASSERT_EQ(mds_cat_ns_create(fx.cat, NULL, fx.d.fileid, "g", MDS_FTYPE_REG, 0644,
                                0, 0, NULL, &c1), MDS_OK);
    ASSERT_EQ(mds_cat_ns_link(fx.cat, NULL, fx.d.fileid, "h", c1.fileid), MDS_OK);
    ASSERT_EQ(mds_cat_ns_remove(fx.cat, NULL, fx.d.fileid, "g"), MDS_OK);
    ASSERT_EQ(mds_cat_ns_create(fx.cat, NULL, fx.d.fileid, "g", MDS_FTYPE_REG, 0644,
                                0, 0, NULL, &c2), MDS_OK);
    ASSERT_TRUE(c1.fileid != c2.fileid);
    ASSERT_EQ(mds_cat_ns_getattr(fx.cat, fx.d.fileid, &fx.d), MDS_OK);
    ASSERT_EQ(mds_cat_gc_count(fx.cat, &gc_before), MDS_OK);

    gc_entry_init(&gc);
    ASSERT_EQ(mds_cat_ns_remove_known_gc(fx.cat, NULL, fx.d.fileid, "g", &c1, 1, &gc, 1,
                                         MDS_GC_SWEEP_GEOM(1, 1), &folded),
              MDS_ERR_STALE);
    ASSERT_EQ(folded, false);
    /* "g" still names c2, c1 is still alive under "h" with one link. */
    ASSERT_EQ(mds_cat_ns_lookup(fx.cat, fx.d.fileid, "g", &seen), MDS_OK);
    ASSERT_EQ(seen.fileid, c2.fileid);
    ASSERT_EQ(mds_cat_ns_lookup(fx.cat, fx.d.fileid, "h", &seen), MDS_OK);
    ASSERT_EQ(seen.fileid, c1.fileid);
    ASSERT_EQ(mds_cat_ns_getattr(fx.cat, c1.fileid, &seen), MDS_OK);
    ASSERT_EQ(seen.nlink, 1U);
    ASSERT_EQ(mds_cat_ns_getattr(fx.cat, c2.fileid, &seen), MDS_OK);
    ASSERT_EQ(mds_cat_gc_count(fx.cat, &gc_after), MDS_OK);
    ASSERT_EQ(gc_after, gc_before);
    ASSERT_EQ(mds_cat_ns_getattr(fx.cat, fx.d.fileid, &seen), MDS_OK);
    ASSERT_EQ(seen.change, fx.d.change);
    ASSERT_EQ(dir_count(fx.cat, fx.d.fileid), 3U);

    /* The current snapshot removes exactly c2 and queues its row. */
    ASSERT_EQ(mds_cat_ns_remove_known_gc(fx.cat, NULL, fx.d.fileid, "g", &c2, 1, &gc, 1,
                                         MDS_GC_SWEEP_GEOM(1, 1), &folded), MDS_OK);
    ASSERT_EQ(folded, true);
    ASSERT_EQ(mds_cat_ns_getattr(fx.cat, c2.fileid, &seen), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_ns_getattr(fx.cat, c1.fileid, &seen), MDS_OK);
    ASSERT_EQ(mds_cat_gc_count(fx.cat, &gc_after), MDS_OK);
    ASSERT_EQ(gc_after, gc_before + 1);
    fixture_close(&fx);
}

/* The name still resolves to the snapshot's inode but its link count
 * moved: a LINK since the snapshot (snapshot says final, live nlink 2)
 * and a REMOVE of the other name since the snapshot (snapshot says
 * non-final, live nlink 1) are both refused with STALE and nothing
 * changes.  A fresh snapshot then removes the inode. */
static void test_remove_known_gc_stale_link_count_is_refused(void)
{
    struct fixture fx;
    struct mds_inode c1;
    struct mds_inode snap2;
    struct mds_inode seen;
    struct mds_ds_map_entry gc;
    uint32_t gc_before = 0;
    uint32_t gc_after = 0;
    bool folded = true;

    ASSERT_TRUE(fixture_open(&fx));
    ASSERT_EQ(mds_cat_ns_create(fx.cat, NULL, fx.d.fileid, "g", MDS_FTYPE_REG, 0644,
                                0, 0, NULL, &c1), MDS_OK);
    ASSERT_EQ(c1.nlink, 1U);
    /* c1 snapshot says "final"; a link lands. */
    ASSERT_EQ(mds_cat_ns_link(fx.cat, NULL, fx.d.fileid, "h", c1.fileid), MDS_OK);
    ASSERT_EQ(mds_cat_ns_getattr(fx.cat, fx.d.fileid, &fx.d), MDS_OK);
    ASSERT_EQ(mds_cat_gc_count(fx.cat, &gc_before), MDS_OK);
    gc_entry_init(&gc);
    ASSERT_EQ(mds_cat_ns_remove_known_gc(fx.cat, NULL, fx.d.fileid, "g", &c1, 1, &gc, 1,
                                         MDS_GC_SWEEP_GEOM(1, 1), &folded),
              MDS_ERR_STALE);
    ASSERT_EQ(folded, false);
    ASSERT_EQ(mds_cat_ns_lookup(fx.cat, fx.d.fileid, "g", &seen), MDS_OK);
    ASSERT_EQ(seen.fileid, c1.fileid);
    ASSERT_EQ(seen.nlink, 2U);
    ASSERT_EQ(mds_cat_gc_count(fx.cat, &gc_after), MDS_OK);
    ASSERT_EQ(gc_after, gc_before);
    ASSERT_EQ(mds_cat_ns_getattr(fx.cat, fx.d.fileid, &seen), MDS_OK);
    ASSERT_EQ(seen.change, fx.d.change);

    /* Fresh snapshot says "non-final" (nlink 2); the other name goes. */
    ASSERT_EQ(mds_cat_ns_getattr(fx.cat, c1.fileid, &snap2), MDS_OK);
    ASSERT_EQ(snap2.nlink, 2U);
    ASSERT_EQ(mds_cat_ns_remove(fx.cat, NULL, fx.d.fileid, "h"), MDS_OK);
    ASSERT_EQ(mds_cat_ns_getattr(fx.cat, fx.d.fileid, &fx.d), MDS_OK);
    ASSERT_EQ(mds_cat_ns_remove_known_gc(fx.cat, NULL, fx.d.fileid, "g", &snap2, 1, &gc, 1,
                                         MDS_GC_SWEEP_GEOM(1, 1), &folded),
              MDS_ERR_STALE);
    ASSERT_EQ(folded, false);
    ASSERT_EQ(mds_cat_ns_lookup(fx.cat, fx.d.fileid, "g", &seen), MDS_OK);
    ASSERT_EQ(seen.fileid, c1.fileid);
    ASSERT_EQ(seen.nlink, 1U);
    ASSERT_EQ(mds_cat_gc_count(fx.cat, &gc_after), MDS_OK);
    ASSERT_EQ(gc_after, gc_before);
    ASSERT_EQ(mds_cat_ns_getattr(fx.cat, fx.d.fileid, &seen), MDS_OK);
    ASSERT_EQ(seen.change, fx.d.change);

    /* A snapshot that matches the store removes the last link. */
    ASSERT_EQ(mds_cat_ns_getattr(fx.cat, c1.fileid, &snap2), MDS_OK);
    ASSERT_EQ(mds_cat_ns_remove_known_gc(fx.cat, NULL, fx.d.fileid, "g", &snap2, 1, &gc, 1,
                                         MDS_GC_SWEEP_GEOM(1, 1), &folded), MDS_OK);
    ASSERT_EQ(folded, true);
    ASSERT_EQ(mds_cat_ns_getattr(fx.cat, c1.fileid, &seen), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_gc_count(fx.cat, &gc_after), MDS_OK);
    ASSERT_EQ(gc_after, gc_before + 1);
    fixture_close(&fx);
}

/* -----------------------------------------------------------------------
 * Recovery ownership
 * ----------------------------------------------------------------------- */

#define OWNED_MAX 8

struct owned_ctx {
    uint32_t count;
    uint64_t clientid[OWNED_MAX];
    uint32_t owner[OWNED_MAX];
    uint64_t epoch[OWNED_MAX];
};

static int owned_cb(uint64_t clientid, uint32_t owner_mds_id,
                    uint64_t owner_boot_epoch, void *arg)
{
    struct owned_ctx *c = arg;

    if (c->count < OWNED_MAX) {
        c->clientid[c->count] = clientid;
        c->owner[c->count] = owner_mds_id;
        c->epoch[c->count] = owner_boot_epoch;
    }
    c->count++;
    return 0;
}

static struct mds_catalogue *open_with_identity(uint32_t mds_id)
{
    struct mds_config cfg;
    struct mds_catalogue *cat = NULL;

    memset(&cfg, 0, sizeof(cfg));
    cfg.catalogue_backend = MDS_BACKEND_MEMDB;
    cfg.self.id = mds_id;
    if (catalogue_memdb_open_cfg(&cfg, &cat) != MDS_OK) {
        return NULL;
    }
    return cat;
}

static void test_recovery_rows_record_their_owner(void)
{
    struct mds_catalogue *cat = open_with_identity(7);
    const uint8_t verf[8] = { 1, 2, 3, 4, 5, 6, 7, 8 };
    struct owned_ctx oc;

    ASSERT_TRUE(cat != NULL);
    ASSERT_EQ(mds_coord_recovery_put(cat, NULL, 0x7001, (const uint8_t *)"a", 1, verf),
              MDS_OK);
    ASSERT_EQ(mds_coord_recovery_put(cat, NULL, 0x7002, (const uint8_t *)"b", 1, verf),
              MDS_OK);

    /* The owner sees its rows, with the stored identity reported. */
    memset(&oc, 0, sizeof(oc));
    ASSERT_EQ(mds_coord_recovery_list(cat, 7, owned_cb, &oc), MDS_OK);
    ASSERT_EQ(oc.count, 2U);
    ASSERT_EQ(oc.owner[0], 7U);
    ASSERT_EQ(oc.owner[1], 7U);
    ASSERT_EQ(oc.epoch[0], 0U);

    /* Another owner sees nothing of them; 0 lists everything. */
    memset(&oc, 0, sizeof(oc));
    ASSERT_EQ(mds_coord_recovery_list(cat, 8, owned_cb, &oc), MDS_OK);
    ASSERT_EQ(oc.count, 0U);
    memset(&oc, 0, sizeof(oc));
    ASSERT_EQ(mds_coord_recovery_list(cat, 0, owned_cb, &oc), MDS_OK);
    ASSERT_EQ(oc.count, 2U);

    /* A rewritten row keeps the writer's identity; a deleted one is
     * gone from every listing. */
    ASSERT_EQ(mds_coord_recovery_put(cat, NULL, 0x7001, (const uint8_t *)"aa", 2, verf),
              MDS_OK);
    ASSERT_EQ(mds_coord_recovery_del(cat, NULL, 0x7002), MDS_OK);
    memset(&oc, 0, sizeof(oc));
    ASSERT_EQ(mds_coord_recovery_list(cat, 7, owned_cb, &oc), MDS_OK);
    ASSERT_EQ(oc.count, 1U);
    ASSERT_EQ(oc.clientid[0], 0x7001U);
    ASSERT_EQ(oc.owner[0], 7U);
    mds_catalogue_close(cat);
}

/* Rows written without an identity (owner 0, as pre-ownership binaries
 * wrote them) are unassigned: every owner filter still sees them. */
static void test_recovery_unassigned_rows_visible_to_any_owner(void)
{
    struct mds_catalogue *cat = catalogue_memdb_open();
    const uint8_t verf[8] = { 8, 7, 6, 5, 4, 3, 2, 1 };
    struct owned_ctx oc;

    ASSERT_TRUE(cat != NULL);
    ASSERT_EQ(mds_coord_recovery_put(cat, NULL, 0x9001, (const uint8_t *)"z", 1, verf),
              MDS_OK);
    memset(&oc, 0, sizeof(oc));
    ASSERT_EQ(mds_coord_recovery_list(cat, 42, owned_cb, &oc), MDS_OK);
    ASSERT_EQ(oc.count, 1U);
    ASSERT_EQ(oc.owner[0], 0U);
    mds_catalogue_close(cat);
}

/* ----------------------------------------------------------------------- */

int main(void)
{
    fprintf(stdout, "test_rondb_ns_semantics (memdb parity):\n");

    RUN_TEST(test_link_missing_target_is_notfound);
    RUN_TEST(test_link_name_collision_is_exists);
    RUN_TEST(test_link_directory_target_is_isdir);
    RUN_TEST(test_link_non_directory_parent_is_notdir);
    RUN_TEST(test_link_success_is_one_step);
    RUN_TEST(test_rmdir_non_empty_refused_without_change);
    RUN_TEST(test_rmdir_empty_deletes_inode_and_parent_link);
    RUN_TEST(test_rmdir_known_child_non_empty_refused);
    RUN_TEST(test_rename_over_non_empty_dir_refused_without_change);
    RUN_TEST(test_rename_over_empty_dir_deletes_victim);
    RUN_TEST(test_rename_over_empty_dir_cross_directory);
    RUN_TEST(test_remove_known_gc_stale_snapshot_alive_elsewhere);
    RUN_TEST(test_remove_known_gc_stale_link_count_is_refused);
    RUN_TEST(test_recovery_rows_record_their_owner);
    RUN_TEST(test_recovery_unassigned_rows_visible_to_any_owner);

    fprintf(stdout, "\ntest_rondb_ns_semantics: %d/%d passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
