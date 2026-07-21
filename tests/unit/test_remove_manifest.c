/*
 * Copyright (c) 2026 PeakAIO. All rights reserved.
 * SPDX-License-Identifier: LicenseRef-PeakAIO-Proprietary
 *
 * test_remove_manifest.c — Unit tests for the async-REMOVE delete
 * manifest (mds.conf `remove_async`; src/mds/remove_manifest.c).
 *
 * Covers
 * ------
 *   1. NULL-safety of every public entry point (feature-off paths).
 *   2. init / destroy lifecycle against the memdb backend.
 *   3. submit: tombstone visible + durable manifest row; the started
 *      drainer executes the guarded remove (dirent gone, row
 *      completed, tombstone cleared).
 *   4. force-drain of a single entry (CREATE/RENAME/LINK collision
 *      contract) without the drainer running.
 *   5. force-drain of a whole directory (RMDIR contract).
 *   6. duplicate submit on a tombstoned name is refused (sync
 *      fallback contract).
 *   7. destroy() shutdown flush executes every pending remove.
 *   8. startup reload: persisted rows become tombstones before the
 *      drainer runs (crash-recovery contract).
 *   9. child_fileid guard: a manifest row whose expected fileid no
 *      longer matches completes as a benign mismatch and the live
 *      file SURVIVES.
 *
 * All paths use the memdb backend (no RonDB, no DS mounts) so the
 * tests are hermetic.  memdb lacks ns_remove_info_verified, so these
 * tests also exercise the drainer's NOSUPPORT two-step fallback.
 */

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "pnfs_mds.h"
#include "test_helpers.h"
#include "mds_catalogue.h"
#include "remove_manifest.h"

/* -----------------------------------------------------------------------
 * Minimal test framework (same shape as test_unlink_pending.c)
 * ----------------------------------------------------------------------- */

static int tests_run = 0;
static int tests_passed = 0;

#define RUN_TEST(fn) do {                                            \
	tests_run++;                                                 \
	fprintf(stdout, "  %-50s ", #fn);                            \
	fn();                                                        \
	tests_passed++;                                              \
	fprintf(stdout, "PASS\n");                                   \
} while (0)

#define ASSERT_EQ(a, b) do {                                         \
	long long _av = (long long)(a);                              \
	long long _bv = (long long)(b);                              \
	if (_av != _bv) {                                            \
		fprintf(stderr, "FAIL at %s:%d: %s (=%lld) != %s (=%lld)\n", \
		        __FILE__, __LINE__, #a, _av, #b, _bv);       \
		exit(1);                                             \
	}                                                            \
} while (0)

#define ASSERT_TRUE(cond) do {                                       \
	if (!(cond)) {                                               \
		fprintf(stderr, "FAIL at %s:%d: !(%s)\n",            \
		        __FILE__, __LINE__, #cond);                  \
		exit(1);                                             \
	}                                                            \
} while (0)

/* -----------------------------------------------------------------------
 * Fixture helpers
 * ----------------------------------------------------------------------- */

/* Create a regular file under @parent and return its inode. */
static struct mds_inode make_file(struct mds_catalogue *cat,
				  uint64_t parent, const char *name)
{
	struct mds_inode ino;

	memset(&ino, 0, sizeof(ino));
	ASSERT_EQ(mds_cat_ns_create(cat, NULL, parent, name,
				    MDS_FTYPE_REG, 0644, 1000, 1000,
				    NULL, &ino), MDS_OK);
	ASSERT_TRUE(ino.fileid != 0);
	return ino;
}

static bool name_exists(struct mds_catalogue *cat, uint64_t parent,
			const char *name)
{
	struct mds_inode ino;

	return mds_cat_ns_lookup(cat, parent, name, &ino) == MDS_OK;
}

static uint32_t manifest_rows(struct mds_catalogue *cat)
{
	uint32_t n = 0;

	(void)mds_cat_remove_pending_count(cat, &n);
	return n;
}

/* init with test-friendly knobs; drainer NOT started. */
static struct remove_manifest *make_manifest(struct mds_catalogue *cat)
{
	struct remove_manifest *rm = NULL;

	ASSERT_EQ(remove_manifest_init(cat, NULL, NULL, NULL, NULL,
				       /* mds_id */ 1,
				       /* boot_epoch */ 1,
				       /* max_pending */ 128,
				       /* workers */ 1,
				       /* batch */ 8,
				       /* poll_ms */ 20,
				       /* claim_ttl_ns */
				       60ULL * 1000000000ULL,
				       &rm), 0);
	ASSERT_TRUE(rm != NULL);
	return rm;
}

static bool wait_drained(struct remove_manifest *rm,
			 struct mds_catalogue *cat, uint32_t timeout_ms)
{
	const uint32_t step = 10;
	uint32_t elapsed = 0;

	while (elapsed < timeout_ms) {
		if (remove_manifest_pending(rm) == 0 &&
		    manifest_rows(cat) == 0) {
			return true;
		}
		usleep((useconds_t)step * 1000U);
		elapsed += step;
	}
	return remove_manifest_pending(rm) == 0 && manifest_rows(cat) == 0;
}

/* -----------------------------------------------------------------------
 * 1. NULL-safety (feature-off fast paths)
 * ----------------------------------------------------------------------- */

static void test_null_safety(void)
{
	ASSERT_TRUE(!remove_manifest_is_tombstoned(NULL, 2, "x"));
	ASSERT_EQ(remove_manifest_force_drain_entry(NULL, 2, "x"), 0);
	ASSERT_EQ(remove_manifest_force_drain_dir(NULL, 2), 0);
	ASSERT_EQ(remove_manifest_pending(NULL), 0);
	ASSERT_EQ(remove_manifest_submit(NULL, 2, "x", 3, 1, true), -1);
	ASSERT_EQ(remove_manifest_load(NULL), -1);
	ASSERT_EQ(remove_manifest_start(NULL), -1);
	remove_manifest_destroy(NULL); /* must not crash */
}

/* -----------------------------------------------------------------------
 * 2. init / destroy lifecycle
 * ----------------------------------------------------------------------- */

static void test_init_lifecycle(void)
{
	struct mds_catalogue *cat = open_test_catalogue();
	struct remove_manifest *rm;

	ASSERT_TRUE(cat != NULL);
	rm = make_manifest(cat);
	ASSERT_EQ(remove_manifest_pending(rm), 0);
	remove_manifest_destroy(rm);
	mds_catalogue_close(cat);
}

/* -----------------------------------------------------------------------
 * 3. submit + drainer execution
 * ----------------------------------------------------------------------- */

static void test_submit_and_drain(void)
{
	struct mds_catalogue *cat = open_test_catalogue();
	struct remove_manifest *rm = make_manifest(cat);
	struct mds_inode f = make_file(cat, MDS_FILEID_ROOT, "f1");

	ASSERT_EQ(remove_manifest_submit(rm, MDS_FILEID_ROOT, "f1",
					 f.fileid, f.generation, true), 0);
	/* Acked state: tombstone hides the name, one durable row. */
	ASSERT_TRUE(remove_manifest_is_tombstoned(rm, MDS_FILEID_ROOT,
						  "f1"));
	ASSERT_EQ(remove_manifest_pending(rm), 1);
	ASSERT_EQ(manifest_rows(cat), 1);
	/* delete-at-ack: the dirent is ALREADY gone at ack time
	 * (DB-authoritative hiding on every MDS). */
	ASSERT_TRUE(!name_exists(cat, MDS_FILEID_ROOT, "f1"));

	ASSERT_EQ(remove_manifest_start(rm), 0);
	ASSERT_TRUE(wait_drained(rm, cat, 5000));

	/* Drained: dirent gone, row completed, tombstone cleared. */
	ASSERT_TRUE(!name_exists(cat, MDS_FILEID_ROOT, "f1"));
	ASSERT_TRUE(!remove_manifest_is_tombstoned(rm, MDS_FILEID_ROOT,
						   "f1"));

	remove_manifest_destroy(rm);
	mds_catalogue_close(cat);
}

/* -----------------------------------------------------------------------
 * 4. force-drain of one entry (CREATE-collision contract)
 * ----------------------------------------------------------------------- */

static void test_force_drain_entry(void)
{
	struct mds_catalogue *cat = open_test_catalogue();
	struct remove_manifest *rm = make_manifest(cat);
	struct mds_inode f = make_file(cat, MDS_FILEID_ROOT, "f2");

	ASSERT_EQ(remove_manifest_submit(rm, MDS_FILEID_ROOT, "f2",
					 f.fileid, f.generation, true), 0);
	/* Drainer never started: the colliding op executes it. */
	ASSERT_EQ(remove_manifest_force_drain_entry(rm, MDS_FILEID_ROOT,
						    "f2"), 0);
	ASSERT_TRUE(!name_exists(cat, MDS_FILEID_ROOT, "f2"));
	ASSERT_TRUE(!remove_manifest_is_tombstoned(rm, MDS_FILEID_ROOT,
						   "f2"));
	ASSERT_EQ(manifest_rows(cat), 0);
	ASSERT_EQ(remove_manifest_pending(rm), 0);

	/* Draining an absent entry is a no-op success. */
	ASSERT_EQ(remove_manifest_force_drain_entry(rm, MDS_FILEID_ROOT,
						    "f2"), 0);

	remove_manifest_destroy(rm);
	mds_catalogue_close(cat);
}

/* -----------------------------------------------------------------------
 * 5. force-drain of a directory (RMDIR contract)
 * ----------------------------------------------------------------------- */

static void test_force_drain_dir(void)
{
	struct mds_catalogue *cat = open_test_catalogue();
	struct remove_manifest *rm = make_manifest(cat);
	struct mds_inode dir;
	struct mds_inode f[3];
	const char *names[3] = { "a", "b", "c" };

	memset(&dir, 0, sizeof(dir));
	ASSERT_EQ(mds_cat_ns_create(cat, NULL, MDS_FILEID_ROOT, "sub",
				    MDS_FTYPE_DIR, 0755, 1000, 1000,
				    NULL, &dir), MDS_OK);
	for (int i = 0; i < 3; i++) {
		f[i] = make_file(cat, dir.fileid, names[i]);
		ASSERT_EQ(remove_manifest_submit(rm, dir.fileid, names[i],
						 f[i].fileid,
						 f[i].generation, true), 0);
	}
	ASSERT_EQ(remove_manifest_pending(rm), 3);

	ASSERT_EQ(remove_manifest_force_drain_dir(rm, dir.fileid), 0);
	ASSERT_EQ(remove_manifest_pending(rm), 0);
	ASSERT_EQ(manifest_rows(cat), 0);
	for (int i = 0; i < 3; i++) {
		ASSERT_TRUE(!name_exists(cat, dir.fileid, names[i]));
	}

	remove_manifest_destroy(rm);
	mds_catalogue_close(cat);
}

/* -----------------------------------------------------------------------
 * 6. duplicate submit refused (sync-fallback contract)
 * ----------------------------------------------------------------------- */

static void test_duplicate_submit_refused(void)
{
	struct mds_catalogue *cat = open_test_catalogue();
	struct remove_manifest *rm = make_manifest(cat);
	struct mds_inode f = make_file(cat, MDS_FILEID_ROOT, "dup");

	ASSERT_EQ(remove_manifest_submit(rm, MDS_FILEID_ROOT, "dup",
					 f.fileid, f.generation, true), 0);
	ASSERT_EQ(remove_manifest_submit(rm, MDS_FILEID_ROOT, "dup",
					 f.fileid, f.generation, true), -1);
	ASSERT_EQ(remove_manifest_pending(rm), 1);
	ASSERT_EQ(manifest_rows(cat), 1);

	remove_manifest_destroy(rm);
	mds_catalogue_close(cat);
}

/* -----------------------------------------------------------------------
 * 7. destroy() shutdown flush
 * ----------------------------------------------------------------------- */

static void test_shutdown_flush(void)
{
	struct mds_catalogue *cat = open_test_catalogue();
	struct remove_manifest *rm = make_manifest(cat);
	struct mds_inode f1 = make_file(cat, MDS_FILEID_ROOT, "s1");
	struct mds_inode f2 = make_file(cat, MDS_FILEID_ROOT, "s2");

	ASSERT_EQ(remove_manifest_submit(rm, MDS_FILEID_ROOT, "s1",
					 f1.fileid, f1.generation, true), 0);
	ASSERT_EQ(remove_manifest_submit(rm, MDS_FILEID_ROOT, "s2",
					 f2.fileid, f2.generation, true), 0);

	/* No drainer started; destroy must flush both. */
	remove_manifest_destroy(rm);

	ASSERT_TRUE(!name_exists(cat, MDS_FILEID_ROOT, "s1"));
	ASSERT_TRUE(!name_exists(cat, MDS_FILEID_ROOT, "s2"));
	ASSERT_EQ(manifest_rows(cat), 0);

	mds_catalogue_close(cat);
}

/* -----------------------------------------------------------------------
 * 8. startup reload (crash-recovery contract)
 * ----------------------------------------------------------------------- */

static void test_reload_from_manifest(void)
{
	struct mds_catalogue *cat = open_test_catalogue();
	struct mds_inode f = make_file(cat, MDS_FILEID_ROOT, "crash");
	uint64_t seq = 0;
	struct remove_manifest *rm;

	/* Simulate a pre-crash delete-at-ack: the durable row exists
	 * AND the ack transaction already removed the dirent + flagged
	 * the inode (the real shape of a crash in this port). */
	ASSERT_EQ(mds_cat_remove_pending_enqueue_unlink(cat, NULL,
						 MDS_FILEID_ROOT, "crash",
						 f.fileid, f.generation,
						 &seq), MDS_OK);
	ASSERT_TRUE(seq != 0);

	rm = make_manifest(cat);
	ASSERT_EQ(remove_manifest_load(rm), 1);
	/* Reload must hide the name BEFORE any drainer runs. */
	ASSERT_TRUE(remove_manifest_is_tombstoned(rm, MDS_FILEID_ROOT,
						  "crash"));
	ASSERT_EQ(remove_manifest_pending(rm), 1);

	/* destroy's shutdown flush executes the reloaded row. */
	remove_manifest_destroy(rm);
	ASSERT_TRUE(!name_exists(cat, MDS_FILEID_ROOT, "crash"));
	ASSERT_EQ(manifest_rows(cat), 0);

	mds_catalogue_close(cat);
}

/* -----------------------------------------------------------------------
 * 9. child_fileid guard: mismatched rows never delete a live file
 * ----------------------------------------------------------------------- */

static void test_guard_mismatch_preserves_file(void)
{
	struct mds_catalogue *cat = open_test_catalogue();
	struct mds_inode f = make_file(cat, MDS_FILEID_ROOT, "keepme");
	uint64_t seq = 0;
	struct remove_manifest *rm;

	/* A stale row pointing at a DIFFERENT fileid (as after a
	 * force-drain + recreate on another node, or corruption). */
	ASSERT_EQ(mds_cat_remove_pending_enqueue(cat, NULL,
						 MDS_FILEID_ROOT, "keepme",
						 f.fileid + 999,
						 f.generation,
						 &seq), MDS_OK);

	rm = make_manifest(cat);
	ASSERT_EQ(remove_manifest_load(rm), 1);
	remove_manifest_destroy(rm);

	/* The row completed as a benign mismatch; the file SURVIVES. */
	ASSERT_TRUE(name_exists(cat, MDS_FILEID_ROOT, "keepme"));
	ASSERT_EQ(manifest_rows(cat), 0);

	mds_catalogue_close(cat);
}

/* -----------------------------------------------------------------------
 * main
 * ----------------------------------------------------------------------- */

/* PHASE-R TEST: unlink-at-ack — dirent gone + inode flagged at submit;
 * force-drain finalizes the inode via the DELETE_PENDING inference. */
static void test_unlink_at_ack(void)
{
	struct mds_catalogue *cat = open_test_catalogue();
	struct remove_manifest *rm = make_manifest(cat);
	struct mds_inode f = make_file(cat, MDS_FILEID_ROOT, "uax");
	struct mds_inode chk;

	ASSERT_EQ(remove_manifest_submit(rm, MDS_FILEID_ROOT, "uax",
					 f.fileid, f.generation, true), 0);
	/* Name must be gone from the CATALOGUE itself (any-MDS view),
	 * not merely tombstoned in this node's memory. */
	ASSERT_TRUE(!name_exists(cat, MDS_FILEID_ROOT, "uax"));
	ASSERT_EQ(manifest_rows(cat), 1);
	memset(&chk, 0, sizeof(chk));
	ASSERT_EQ(mds_cat_ns_getattr(cat, f.fileid, &chk), MDS_OK);
	ASSERT_TRUE((chk.flags & MDS_IFLAG_DELETE_PENDING) != 0U);
	free(chk.ds_map);

	/* Drain: inode finalized, row completed. */
	ASSERT_EQ(remove_manifest_force_drain_entry(rm, MDS_FILEID_ROOT,
						    "uax"), 0);
	memset(&chk, 0, sizeof(chk));
	ASSERT_TRUE(mds_cat_ns_getattr(cat, f.fileid, &chk) != MDS_OK);
	ASSERT_EQ(manifest_rows(cat), 0);
	ASSERT_TRUE(!name_exists(cat, MDS_FILEID_ROOT, "uax"));

	remove_manifest_destroy(rm);
	mds_catalogue_close(cat);
}


/* -----------------------------------------------------------------------
 * 11. orphan-tombstone scrubber (F3): a row drained by a PEER leaves a
 *     stale local tombstone; one scrub pass drops it.  A sibling entry
 *     whose row still exists is claimed and drained by the same pass.
 * ----------------------------------------------------------------------- */

struct seq_find {
	const char *name;
	uint64_t dir;
	uint64_t seq;
};

static int seq_find_cb(const struct mds_remove_pending_entry *e, void *arg)
{
	struct seq_find *sf = arg;

	if (e->dir_fileid == sf->dir && strcmp(e->name, sf->name) == 0) {
		sf->seq = e->remove_seq;
	}
	return 0;
}

static uint64_t row_seq(struct mds_catalogue *cat, uint64_t dir,
			const char *name)
{
	struct seq_find sf = { name, dir, 0 };

	(void)mds_cat_remove_pending_scan_all(cat, seq_find_cb, &sf);
	return sf.seq;
}

static void test_scrub_orphans(void)
{
	struct mds_catalogue *cat = open_test_catalogue();
	struct remove_manifest *rm = make_manifest(cat);
	struct mds_inode a = make_file(cat, MDS_FILEID_ROOT, "sc1");
	struct mds_inode b = make_file(cat, MDS_FILEID_ROOT, "sc2");

	ASSERT_EQ(remove_manifest_submit(rm, MDS_FILEID_ROOT, "sc1",
					 a.fileid, a.generation, true), 0);
	ASSERT_EQ(remove_manifest_submit(rm, MDS_FILEID_ROOT, "sc2",
					 b.fileid, b.generation, true), 0);
	ASSERT_EQ(remove_manifest_pending(rm), 2);
	ASSERT_EQ(manifest_rows(cat), 2);

	/* Simulate a PEER draining sc1: its row disappears while the
	 * local tombstone stays behind. */
	ASSERT_EQ(mds_cat_remove_pending_complete(cat,
			(int64_t)row_seq(cat, MDS_FILEID_ROOT, "sc1")),
		  MDS_OK);
	ASSERT_EQ(manifest_rows(cat), 1);
	ASSERT_EQ(remove_manifest_pending(rm), 2);

	/* One scrub pass (age threshold 0): the orphaned sc1 note is
	 * dropped; sc2 has a live overdue row, so it is claimed and
	 * drained inline by the same pass. */
	ASSERT_EQ(remove_manifest_scrub_orphans(rm, 0), 1);
	ASSERT_EQ(remove_manifest_pending(rm), 0);
	ASSERT_TRUE(!remove_manifest_is_tombstoned(rm, MDS_FILEID_ROOT,
						   "sc1"));
	ASSERT_TRUE(!remove_manifest_is_tombstoned(rm, MDS_FILEID_ROOT,
						   "sc2"));
	ASSERT_EQ(manifest_rows(cat), 0);
	ASSERT_TRUE(!name_exists(cat, MDS_FILEID_ROOT, "sc2"));

	remove_manifest_destroy(rm);
	mds_catalogue_close(cat);
}

int main(void)
{
	fprintf(stdout, "test_remove_manifest\n");

	RUN_TEST(test_null_safety);
	RUN_TEST(test_init_lifecycle);
	RUN_TEST(test_submit_and_drain);
	RUN_TEST(test_force_drain_entry);
	RUN_TEST(test_force_drain_dir);
	RUN_TEST(test_duplicate_submit_refused);
	RUN_TEST(test_shutdown_flush);
	RUN_TEST(test_reload_from_manifest);
	RUN_TEST(test_guard_mismatch_preserves_file);
	RUN_TEST(test_unlink_at_ack);
	RUN_TEST(test_scrub_orphans);

	fprintf(stdout, "\n  %d/%d tests passed\n", tests_run, tests_passed);
	return (tests_run == tests_passed) ? 0 : 1;
}
