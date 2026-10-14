/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_catalogue.c -- Unit tests for the backend-neutral catalogue API.
 *
 * Exercises mds_catalogue_open/close and the catalogue operations
 * against the catalogue backend, verifying parity with the raw catalogue/ns
 * surface and correct lifecycle semantics.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>

#include "pnfs_mds.h"
#include "test_helpers.h"
#include "harness.h"        /* conformance_open_checked */
#include "mds_catalogue.h"
#include "mds_coordination.h"
#include "mds_cluster.h"
#include "commit_queue.h"
#include "compound.h"      /* struct compound_data */
#include "compound_internal.h" /* cat_getattr, cat_on_root_db */
#include "open_state.h"    /* struct nfs4_stateid */

/* -----------------------------------------------------------------------
 * Test helpers
 * ----------------------------------------------------------------------- */

#define TEST_MAP_SIZE (16ULL * 1024 * 1024)

static int tests_run;
static int tests_passed;

/* Set by ASSERT_* on failure so RUN_TEST can detect a failed test
 * (the bare `return` from the assertion macro is otherwise invisible
 * to the caller).  Reset to 0 by RUN_TEST before each test runs. */
static int current_test_failed;

#define ASSERT_EQ(a, b) do {						\
	if ((a) != (b)) {						\
		fprintf(stderr, "  FAIL %s:%d: %s != %s\n",		\
			__FILE__, __LINE__, #a, #b);			\
		current_test_failed = 1;				\
		return;							\
	}								\
} while (0)

#define ASSERT_NE(a, b) do {						\
	if ((a) == (b)) {						\
		fprintf(stderr, "  FAIL %s:%d: %s == %s\n",		\
			__FILE__, __LINE__, #a, #b);			\
		current_test_failed = 1;				\
		return;							\
	}								\
} while (0)

#define ASSERT_TRUE(x)  ASSERT_NE((x), 0)

#define RUN_TEST(fn) do {						\
	tests_run++;							\
	fprintf(stdout, "  %-50s", #fn);				\
	fflush(stdout);							\
	current_test_failed = 0;					\
	fn();								\
	if (current_test_failed) {					\
		fprintf(stdout, "FAIL\n");				\
	} else {							\
		tests_passed++;						\
		fprintf(stdout, "PASS\n");				\
	}								\
} while (0)

static char *make_temp_db_path(void)
{
	char tmpl[] = "/tmp/pnfs-mds-cat-XXXXXX";
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

	if (path == NULL)
		return;
	unlink(path);
	snprintf(lock_path, sizeof(lock_path), "%s-lock", path);
	unlink(lock_path);

	{
		char dir[512];
		const char *slash;

		slash = strrchr(path, '/');
		if (slash != NULL) {
			size_t plen = (size_t)(slash - path);

			memcpy(dir, path, plen);
			dir[plen] = '\0';
			rmdir(dir);
		}
	}
}

/* Scratch directory of the current test.  Every namespace operation in
 * this file runs under it rather than under the root: a persistent
 * store (RonDB) keeps the namespace across runs and is shared with
 * running daemons, so fixed names directly under "/" collide with
 * earlier runs and root-level entry counts are never the fixture's.
 * open_test_cat() creates it, close_test_cat() removes it again. */
static uint64_t g_dir;

/** Open a catalogue on the backend selected by CATALOGUE_TEST_BACKEND
 * (memdb by default) through the conformance harness; an unavailable
 * backend exits 77 before any test runs.  path_out is still populated
 * so the existing close_test_cat() cleanup stays stable. */
static struct mds_catalogue *open_test_cat(char **path_out)
{
	struct mds_catalogue *cat;

	*path_out = make_temp_db_path();

	cat = conformance_open_checked();
	assert(cat != NULL);
	g_dir = 0;
	assert(conformance_scratch_dir(cat, &g_dir) == MDS_OK);
	return cat;
}

static void close_test_cat(struct mds_catalogue *cat, char *path)
{
	conformance_scratch_cleanup(cat, g_dir);
	g_dir = 0;
	mds_catalogue_close(cat);
	cleanup_temp_db(path);
	free(path);
}

/* A fileid no row of this store carries a layout for: fileids are
 * allocated monotonically and never reused, so a fresh allocation has
 * an empty layout table even on a persistent store that other suites
 * (and earlier runs) left rows in. */
static uint64_t fresh_fileid(struct mds_catalogue *cat)
{
	uint64_t fid = 0;

	assert(mds_cat_alloc_fileid(cat, NULL, &fid) == MDS_OK);
	assert(fid != 0);
	return fid;
}

/* -----------------------------------------------------------------------
 * 1. test_catalogue_open_close -- lifecycle
 * ----------------------------------------------------------------------- */

static void test_catalogue_open_close(void)
{
	struct mds_catalogue *cat;
	char *path;

	cat = open_test_cat(&path);
	ASSERT_NE(cat, NULL);

	/* Double-close is safe (NULL after first close). */
	conformance_scratch_cleanup(cat, g_dir);
	g_dir = 0;
	mds_catalogue_close(cat);
	mds_catalogue_close(NULL);  /* must not crash */

	cleanup_temp_db(path);
	free(path);
}

/* -----------------------------------------------------------------------
 * 2. test_catalogue_ns_create_lookup -- create + lookup round-trip
 * ----------------------------------------------------------------------- */

static void test_catalogue_ns_create_lookup(void)
{
	struct mds_catalogue *cat;
	struct mds_inode child, looked_up;
	char *path;

	cat = open_test_cat(&path);

	ASSERT_EQ(mds_cat_ns_create(cat, NULL, g_dir,
				    "testdir", MDS_FTYPE_DIR,
				    0755, 1000, 1000, NULL, &child),
		  MDS_OK);

	ASSERT_NE(child.fileid, (uint64_t)0);
	ASSERT_EQ(child.type, MDS_FTYPE_DIR);
	ASSERT_EQ(child.mode, (uint32_t)0755);
	ASSERT_EQ(child.nlink, (uint32_t)2);

	/* Lookup returns the same inode. */
	ASSERT_EQ(mds_cat_ns_lookup(cat, g_dir,
				    "testdir", &looked_up),
		  MDS_OK);
	ASSERT_EQ(looked_up.fileid, child.fileid);

	/* Duplicate create returns EXISTS. */
	ASSERT_EQ(mds_cat_ns_create(cat, NULL, g_dir,
				    "testdir", MDS_FTYPE_DIR,
				    0755, 0, 0, NULL, &child),
		  MDS_ERR_EXISTS);

	close_test_cat(cat, path);
}

struct layout_idx_scan_ctx {
	uint64_t expected_clientid;
	uint64_t expected_fileid;
	uint32_t hits;
	bool saw_unexpected;
};

static int layout_idx_scan_cb(uint64_t clientid, uint64_t fileid, void *ctx)
{
	struct layout_idx_scan_ctx *scan = ctx;

	if (scan == NULL) {
		return 1;
	}
	if (clientid != scan->expected_clientid ||
	    fileid != scan->expected_fileid) {
		scan->saw_unexpected = true;
	}
	scan->hits++;
	return 0;
}

static void test_catalogue_coordination_cq_accepts_wide_ds_count(void)
{
	struct mds_catalogue *cat;
	struct commit_queue *cq = NULL;
	struct commit_op cop;
	struct nfs4_stateid sid;
	struct layout_idx_scan_ctx scan;
	uint64_t got_cid = 0;
	uint64_t got_fid = 0;
	uint32_t ds_ids[32];
	bool has_layout = false;
	uint64_t fid;
	char *path;
	uint32_t i;

	ASSERT_TRUE(COMMIT_OP_LAYOUT_MAX_DS > 16);
	cat = open_test_cat(&path);
	fid = fresh_fileid(cat);
	ASSERT_EQ(commit_queue_create(cat,
				      NULL, 0, 0, 0, 0, 0, 0, &cq), 0);
	ASSERT_NE(cq, NULL);
	mds_catalogue_set_cq(cat, cq);

	memset(&sid, 0, sizeof(sid));
	sid.seqid = 1;
	memset(sid.other, 0xCD, sizeof(sid.other));
	for (i = 0; i < 32; i++) {
		ds_ids[i] = i + 1;
	}

	memset(&cop, 0, sizeof(cop));
	cop.type = COMMIT_OP_LAYOUT_STATE_PUT;
	cop.args.layout_put.clientid = 100;
	cop.args.layout_put.fileid = fid;
	cop.args.layout_put.iomode = 2;
	cop.args.layout_put.offset = 0;
	cop.args.layout_put.length = UINT64_MAX;
	cop.args.layout_put.stateid = sid;
	cop.args.layout_put.ds_ids = ds_ids;
	cop.args.layout_put.ds_count = 32;
	ASSERT_EQ(commit_queue_submit(cq, &cop), MDS_OK);

	ASSERT_EQ(mds_coord_layout_get_by_stateid(cat, sid.other,
						  &got_cid, &got_fid,
						  NULL, NULL,
						  NULL, NULL),
		  MDS_OK);
	ASSERT_EQ(got_cid, (uint64_t)100);
	ASSERT_EQ(got_fid, fid);
	ASSERT_EQ(mds_coord_layout_scan_for_file(cat, fid, &has_layout),
		  MDS_OK);
	ASSERT_TRUE(has_layout);

	memset(&scan, 0, sizeof(scan));
	scan.expected_clientid = 100;
	scan.expected_fileid = fid;
	ASSERT_EQ(mds_coord_ds_layout_idx_scan(cat, ds_ids[20],
					       layout_idx_scan_cb, &scan),
		  MDS_OK);
	ASSERT_EQ(scan.hits, (uint32_t)1);
	ASSERT_EQ(scan.saw_unexpected, false);

	memset(&cop, 0, sizeof(cop));
	cop.type = COMMIT_OP_LAYOUT_STATE_DEL;
	cop.args.layout_del.clientid = 100;
	cop.args.layout_del.fileid = fid;
	memcpy(cop.args.layout_del.stateid_other, sid.other,
	       sizeof(cop.args.layout_del.stateid_other));
	cop.args.layout_del.ds_ids = ds_ids;
	cop.args.layout_del.ds_count = 32;
	ASSERT_EQ(commit_queue_submit(cq, &cop), MDS_OK);

	ASSERT_EQ(mds_coord_layout_get_by_stateid(cat, sid.other,
						  NULL, NULL, NULL, NULL,
						  NULL, NULL),
		  MDS_ERR_NOTFOUND);
	has_layout = true;
	ASSERT_EQ(mds_coord_layout_scan_for_file(cat, fid, &has_layout),
		  MDS_OK);
	ASSERT_EQ(has_layout, false);

	memset(&scan, 0, sizeof(scan));
	scan.expected_clientid = 100;
	scan.expected_fileid = fid;
	ASSERT_EQ(mds_coord_ds_layout_idx_scan(cat, ds_ids[20],
					       layout_idx_scan_cb, &scan),
		  MDS_OK);
	ASSERT_EQ(scan.hits, (uint32_t)0);

	mds_catalogue_set_cq(cat, NULL);
	commit_queue_destroy(cq);
	close_test_cat(cat, path);
}

/*
 * Regression: auth_ops->dirent_insert must be wired.  The async-REMOVE
 * merge (fc6bc2b) dropped the RonDB vtable entries and every HPC
 * hpc_shared create failed with EINVAL; this test fails the same way
 * if memdb (or any backend under test) loses the op again.
 *
 * Split in two so the fixture is always restored: the ASSERT_* macros
 * return from the function they run in, and the checks below leave a
 * raw alias dirent "b" -> inode("a") behind that the harness cannot
 * remove (see dirent_insert_only_restore).
 */
static void dirent_insert_only_checks(struct mds_catalogue *cat,
				      uint64_t child_fid)
{
	uint64_t fid = 0;
	uint8_t typ = 0;

	/* Insert-only succeeds for a new name. */
	ASSERT_EQ(mds_cat_dirent_insert(cat, NULL, g_dir, "b",
					child_fid, (uint8_t)MDS_FTYPE_REG),
		  MDS_OK);
	ASSERT_EQ(mds_cat_dirent_get(cat, g_dir, "b", &fid, &typ),
		  MDS_OK);
	ASSERT_EQ(fid, child_fid);

	/* Second insert of the same name must NOT upsert. */
	ASSERT_EQ(mds_cat_dirent_insert(cat, NULL, g_dir, "b",
					child_fid + 1,
					(uint8_t)MDS_FTYPE_REG),
		  MDS_ERR_EXISTS);
	fid = 0;
	ASSERT_EQ(mds_cat_dirent_get(cat, g_dir, "b", &fid, &typ),
		  MDS_OK);
	ASSERT_EQ(fid, child_fid);
}

/*
 * "b" is a raw dirent aliasing "a"'s inode with no nlink behind it.
 * The harness cleanup removes entries in readdir order: removing "a"
 * first deletes the inode, after which ns_remove("b") fails on the
 * missing inode and the alias -- and with it the scratch directory --
 * stays behind forever on a shared persistent store.  Drop the alias
 * with dirent_del (no inode bookkeeping) BEFORE the ordinary remove of
 * "a", then prove the directory is empty so the harness's removal of
 * it cannot fail.
 */
static void dirent_insert_only_restore(struct mds_catalogue *cat)
{
	uint64_t fid = 0;
	uint8_t typ = 0;
	bool empty = false;

	if (mds_cat_dirent_get(cat, g_dir, "b", &fid, &typ) == MDS_OK) {
		ASSERT_EQ(mds_cat_dirent_del(cat, NULL, g_dir, "b"), MDS_OK);
	}
	ASSERT_EQ(mds_cat_dirent_get(cat, g_dir, "b", &fid, &typ),
		  MDS_ERR_NOTFOUND);
	ASSERT_EQ(mds_cat_ns_remove(cat, NULL, g_dir, "a"), MDS_OK);
	ASSERT_EQ(mds_cat_dir_is_empty(cat, g_dir, &empty), MDS_OK);
	ASSERT_TRUE(empty);
}

static void test_catalogue_dirent_insert_only(void)
{
	struct mds_catalogue *cat;
	struct mds_inode child;
	char *path;

	cat = open_test_cat(&path);

	memset(&child, 0, sizeof(child));
	if (mds_cat_ns_create(cat, NULL, g_dir, "a", MDS_FTYPE_REG, 0644,
			      0, 0, NULL, &child) != MDS_OK) {
		fprintf(stderr, "  FAIL %s:%d: create \"a\"\n",
			__FILE__, __LINE__);
		current_test_failed = 1;
	} else {
		dirent_insert_only_checks(cat, child.fileid);
		dirent_insert_only_restore(cat);
	}

	close_test_cat(cat, path);
}

static void test_catalogue_coordination_dispatch_rejects_null_ds_ids(void)
{
	struct mds_catalogue *cat;
	struct nfs4_stateid sid;
	char *path;

	cat = open_test_cat(&path);

	memset(&sid, 0, sizeof(sid));
	sid.seqid = 1;
	memset(sid.other, 0xAB, sizeof(sid.other));

	/* ds_count > 0 with NULL ds_ids must be rejected at the dispatch
	 * boundary so backends never see (count, NULL).  Mirrors the new
	 * guard added to mds_coord_layout_grant / _return in
	 * src/catalogue/catalogue_dispatch.c. */
	ASSERT_EQ(mds_coord_layout_grant(cat, NULL, 1234, 5678,
					 2 /* RW */, 0, UINT64_MAX,
					 &sid, NULL, 1),
		  MDS_ERR_INVAL);

	ASSERT_EQ(mds_coord_layout_return(cat, NULL, sid.other,
					  1234, 5678, NULL, 4),
		  MDS_ERR_INVAL);

	/* ds_count == 0 with NULL ds_ids stays valid -- used by callers
	 * that legitimately want to delete only the layout_state row
	 * (e.g. layout_recall.c::revoke_layout when ds_id == 0). */
	ASSERT_EQ(mds_coord_layout_grant(cat, NULL, 1234, 5678,
					 2 /* RW */, 0, UINT64_MAX,
					 &sid, NULL, 0),
		  MDS_OK);
	ASSERT_EQ(mds_coord_layout_return(cat, NULL, sid.other,
					  1234, 5678, NULL, 0),
		  MDS_OK);

	close_test_cat(cat, path);
}

/* -----------------------------------------------------------------------
 * 3. test_catalogue_ns_setattr_getattr -- setattr + getattr round-trip
 * ----------------------------------------------------------------------- */

static void test_catalogue_ns_setattr_getattr(void)
{
	struct mds_catalogue *cat;
	struct mds_inode child, got;
	struct mds_inode attrs;
	char *path;

	cat = open_test_cat(&path);

	ASSERT_EQ(mds_cat_ns_create(cat, NULL, g_dir,
				    "attrs.dir", MDS_FTYPE_DIR,
				    0755, 1000, 1000, NULL, &child),
		  MDS_OK);

	/* Change mode to 0755. */
	memset(&attrs, 0, sizeof(attrs));
	attrs.mode = 0755;
	ASSERT_EQ(mds_cat_ns_setattr(cat, NULL, child.fileid,
				     &attrs, 1U /* MDS_ATTR_MODE */),
		  MDS_OK);

	ASSERT_EQ(mds_cat_ns_getattr(cat, child.fileid, &got), MDS_OK);
	ASSERT_EQ(got.mode, (uint32_t)0755);

	close_test_cat(cat, path);
}

/* -----------------------------------------------------------------------
 * 4. test_catalogue_ns_remove -- remove + verify NOTFOUND
 * ----------------------------------------------------------------------- */

static void test_catalogue_ns_remove(void)
{
	struct mds_catalogue *cat;
	struct mds_inode child, looked_up;
	char *path;

	cat = open_test_cat(&path);

	ASSERT_EQ(mds_cat_ns_create(cat, NULL, g_dir,
				    "todelete", MDS_FTYPE_DIR,
				    0755, 0, 0, NULL, &child),
		  MDS_OK);

	ASSERT_EQ(mds_cat_ns_remove(cat, NULL, g_dir,
				    "todelete"),
		  MDS_OK);

	ASSERT_EQ(mds_cat_ns_lookup(cat, g_dir,
				    "todelete", &looked_up),
		  MDS_ERR_NOTFOUND);

	close_test_cat(cat, path);
}

/* -----------------------------------------------------------------------
 * 5. test_catalogue_ns_readdir -- readdir with start_after pagination
 * ----------------------------------------------------------------------- */

struct readdir_ctx {
	uint32_t count;
	char     first_name[256];
};

static int readdir_counter(const struct mds_cat_dirent *entry, void *arg)
{
	struct readdir_ctx *ctx = arg;

	if (ctx->count == 0) {
		snprintf(ctx->first_name, sizeof(ctx->first_name),
			 "%s", entry->name);
	}
	ctx->count++;
	return 0;
}

static void test_catalogue_ns_readdir(void)
{
	struct mds_catalogue *cat;
	struct mds_inode child;
	struct readdir_ctx ctx;
	char *path;

	cat = open_test_cat(&path);

	/* Create three directories: aaa, bbb, ccc. */
	ASSERT_EQ(mds_cat_ns_create(cat, NULL, g_dir,
				    "aaa", MDS_FTYPE_DIR, 0755,
				    0, 0, NULL, &child), MDS_OK);
	ASSERT_EQ(mds_cat_ns_create(cat, NULL, g_dir,
				    "bbb", MDS_FTYPE_DIR, 0755,
				    0, 0, NULL, &child), MDS_OK);
	ASSERT_EQ(mds_cat_ns_create(cat, NULL, g_dir,
				    "ccc", MDS_FTYPE_DIR, 0755,
				    0, 0, NULL, &child), MDS_OK);

	/* Full readdir. */
	memset(&ctx, 0, sizeof(ctx));
	ASSERT_EQ(mds_cat_ns_readdir(cat, g_dir, NULL, 0,
				     NULL, readdir_counter, &ctx),
		  MDS_OK);
	ASSERT_EQ(ctx.count, (uint32_t)3);

	/* Readdir with start_after="aaa" -- should skip "aaa". */
	memset(&ctx, 0, sizeof(ctx));
	ASSERT_EQ(mds_cat_ns_readdir(cat, g_dir, "aaa", 0,
				     NULL, readdir_counter, &ctx),
		  MDS_OK);
	ASSERT_EQ(ctx.count, (uint32_t)2);

	close_test_cat(cat, path);
}

/* -----------------------------------------------------------------------
 * 5b. test_catalogue_ns_readdir_plus_* -- dispatch fallback coverage
 *
 * The in-memory test backend leaves auth_ops->ns_readdir_plus NULL,
 * so these cases exercise the null-safe dispatch wrapper in
 * catalogue_dispatch.c (ns_readdir + per-entry ns_getattr driving
 * the mds_readdir_plus_cb signature).  They verify that the caller
 * sees correct dirent + inode pairs, stable ordering, pagination via
 * start_after, and that a zero-entry directory is a clean no-op.
 * ----------------------------------------------------------------------- */

#define READDIR_PLUS_MAX 64

struct readdir_plus_collect {
	uint32_t count;
	char name[READDIR_PLUS_MAX][256];
	uint64_t fileid[READDIR_PLUS_MAX];
	uint32_t mode[READDIR_PLUS_MAX];
	bool valid[READDIR_PLUS_MAX];
};

static int readdir_plus_collect_cb(const struct mds_cat_dirent *entry,
				   const struct mds_inode *inode,
				   bool inode_valid,
				   void *arg)
{
	struct readdir_plus_collect *c = arg;

	if (c->count >= READDIR_PLUS_MAX) {
		return -1;
	}
	snprintf(c->name[c->count], sizeof(c->name[c->count]),
		 "%s", entry->name);
	c->fileid[c->count] = entry->fileid;
	c->valid[c->count] = inode_valid;
	c->mode[c->count] = inode_valid ? inode->mode : 0U;
	c->count++;
	return 0;
}

static void test_catalogue_ns_readdir_plus_empty(void)
{
	struct mds_catalogue *cat;
	struct mds_inode child;
	struct readdir_plus_collect got;
	char *path;

	cat = open_test_cat(&path);

	/* Create an empty subdir and list it. */
	ASSERT_EQ(mds_cat_ns_create(cat, NULL, g_dir,
				    "empty", MDS_FTYPE_DIR, 0755,
				    0, 0, NULL, &child), MDS_OK);

	memset(&got, 0, sizeof(got));
	ASSERT_EQ(mds_cat_ns_readdir_plus(cat, child.fileid, NULL, 0,
					  NULL,
					  readdir_plus_collect_cb, &got),
		  MDS_OK);
	ASSERT_EQ(got.count, (uint32_t)0);

	close_test_cat(cat, path);
}

static void test_catalogue_ns_readdir_plus_three_entries(void)
{
	struct mds_catalogue *cat;
	struct mds_inode child;
	struct readdir_plus_collect got;
	char *path;
	uint32_t i;
	bool saw_aaa = false, saw_bbb = false, saw_ccc = false;

	cat = open_test_cat(&path);

	ASSERT_EQ(mds_cat_ns_create(cat, NULL, g_dir,
				    "aaa", MDS_FTYPE_DIR, 0700,
				    0, 0, NULL, &child), MDS_OK);
	ASSERT_EQ(mds_cat_ns_create(cat, NULL, g_dir,
				    "bbb", MDS_FTYPE_REG, 0644,
				    0, 0, NULL, &child), MDS_OK);
	ASSERT_EQ(mds_cat_ns_create(cat, NULL, g_dir,
				    "ccc", MDS_FTYPE_DIR, 0755,
				    0, 0, NULL, &child), MDS_OK);

	/* Full fused readdir_plus: three entries, each with valid inode. */
	memset(&got, 0, sizeof(got));
	ASSERT_EQ(mds_cat_ns_readdir_plus(cat, g_dir, NULL, 0,
					  NULL,
					  readdir_plus_collect_cb, &got),
		  MDS_OK);
	ASSERT_EQ(got.count, (uint32_t)3);

	for (i = 0; i < got.count; i++) {
		ASSERT_TRUE(got.valid[i]);
		if (strcmp(got.name[i], "aaa") == 0) {
			ASSERT_EQ(got.mode[i], (uint32_t)0700);
			saw_aaa = true;
		} else if (strcmp(got.name[i], "bbb") == 0) {
			ASSERT_EQ(got.mode[i], (uint32_t)0644);
			saw_bbb = true;
		} else if (strcmp(got.name[i], "ccc") == 0) {
			ASSERT_EQ(got.mode[i], (uint32_t)0755);
			saw_ccc = true;
		}
	}
	ASSERT_TRUE(saw_aaa);
	ASSERT_TRUE(saw_bbb);
	ASSERT_TRUE(saw_ccc);

	close_test_cat(cat, path);
}

static void test_catalogue_ns_readdir_plus_start_after(void)
{
	struct mds_catalogue *cat;
	struct mds_inode child;
	struct readdir_plus_collect got;
	char *path;

	cat = open_test_cat(&path);

	ASSERT_EQ(mds_cat_ns_create(cat, NULL, g_dir,
				    "aaa", MDS_FTYPE_DIR, 0755,
				    0, 0, NULL, &child), MDS_OK);
	ASSERT_EQ(mds_cat_ns_create(cat, NULL, g_dir,
				    "bbb", MDS_FTYPE_DIR, 0755,
				    0, 0, NULL, &child), MDS_OK);
	ASSERT_EQ(mds_cat_ns_create(cat, NULL, g_dir,
				    "ccc", MDS_FTYPE_DIR, 0755,
				    0, 0, NULL, &child), MDS_OK);

	/* start_after="aaa" should drop "aaa" from the result. */
	memset(&got, 0, sizeof(got));
	ASSERT_EQ(mds_cat_ns_readdir_plus(cat, g_dir, "aaa", 0,
					  NULL,
					  readdir_plus_collect_cb, &got),
		  MDS_OK);
	ASSERT_EQ(got.count, (uint32_t)2);

	close_test_cat(cat, path);
}

static void test_catalogue_ns_readdir_max_entries(void)
{
	struct mds_catalogue *cat;
	struct mds_inode child;
	struct readdir_ctx ctx;
	char *path;

	cat = open_test_cat(&path);

	ASSERT_EQ(mds_cat_ns_create(cat, NULL, g_dir,
				    "aaa", MDS_FTYPE_DIR, 0755,
				    0, 0, NULL, &child), MDS_OK);
	ASSERT_EQ(mds_cat_ns_create(cat, NULL, g_dir,
				    "bbb", MDS_FTYPE_DIR, 0755,
				    0, 0, NULL, &child), MDS_OK);
	ASSERT_EQ(mds_cat_ns_create(cat, NULL, g_dir,
				    "ccc", MDS_FTYPE_DIR, 0755,
				    0, 0, NULL, &child), MDS_OK);

	memset(&ctx, 0, sizeof(ctx));
	ASSERT_EQ(mds_cat_ns_readdir(cat, g_dir, NULL, 2,
				     NULL, readdir_counter, &ctx),
		  MDS_OK);
	ASSERT_EQ(ctx.count, (uint32_t)2);
	ASSERT_EQ(strcmp(ctx.first_name, "aaa"), 0);

	close_test_cat(cat, path);
}

static void test_catalogue_dirent_name_for_child(void)
{
	struct mds_catalogue *cat;
	struct mds_inode child;
	char name[256];
	char *path;

	cat = open_test_cat(&path);

	ASSERT_EQ(mds_cat_ns_create(cat, NULL, g_dir,
				    "bravo", MDS_FTYPE_REG, 0644,
				    0, 0, NULL, &child), MDS_OK);

	memset(name, 0, sizeof(name));
	ASSERT_EQ(mds_cat_ns_dirent_name_for_child(
			  cat, g_dir, child.fileid,
			  name, sizeof(name)),
		  MDS_OK);
	ASSERT_EQ(strcmp(name, "bravo"), 0);

	close_test_cat(cat, path);
}

/* -----------------------------------------------------------------------
 * 5d. test_catalogue_readdir_cookie -- backend-assigned READDIR cookies
 *
 * Every dirent a backend delivers carries a cookie (struct
 * mds_cat_dirent.cookie) that is never 0, 1 or 2 and is unique within
 * the directory; readdir_plus_from_cookie resumes with the entries
 * whose cookie is strictly greater.  The cookie a given entry carries
 * must be the same on the plain readdir, the readdir_plus fallback and
 * the cookie-resume path.  How a backend derives the cookie (child
 * fileid on RonDB, a per-dirent sequence on a backend that is hard-link
 * safe) is not part of this contract -- the conformance suite covers
 * the hard-link case per backend.
 * ----------------------------------------------------------------------- */

/* Smallest cookie a producer may hand out (MDS_READDIR_COOKIE_MIN in
 * catalogue_dispatch.c): 0, 1 and 2 are reserved by RFC 8881. */
#define TEST_COOKIE_MIN 3U

struct cookie_collect {
	uint32_t count;
	uint64_t fileid[READDIR_PLUS_MAX];
	uint64_t cookie[READDIR_PLUS_MAX];
	char name[READDIR_PLUS_MAX][MDS_MAX_NAME + 1];
};

static int cookie_collect_cb(const struct mds_cat_dirent *entry, void *arg)
{
	struct cookie_collect *c = arg;

	if (c->count >= READDIR_PLUS_MAX) {
		return -1;
	}
	c->fileid[c->count] = entry->fileid;
	c->cookie[c->count] = entry->cookie;
	snprintf(c->name[c->count], sizeof(c->name[c->count]), "%s",
		 entry->name);
	c->count++;
	return 0;
}

static int cookie_collect_plus_cb(const struct mds_cat_dirent *entry,
				  const struct mds_inode *inode,
				  bool inode_valid,
				  void *arg)
{
	(void)inode;
	(void)inode_valid;
	return cookie_collect_cb(entry, arg);
}

/* Cookie contract for one collected listing: never reserved and unique
 * within the listing. */
static bool cookies_valid(const struct cookie_collect *c)
{
	uint32_t i, j;

	for (i = 0; i < c->count; i++) {
		if (c->cookie[i] < TEST_COOKIE_MIN) {
			return false;
		}
		for (j = 0; j < i; j++) {
			if (c->cookie[j] == c->cookie[i]) {
				return false;
			}
		}
	}
	return true;
}

/* Every name of the three-entry fixture delivered exactly once. */
static bool names_once(const struct cookie_collect *c)
{
	unsigned aaa = 0, bbb = 0, ccc = 0;
	uint32_t i;

	for (i = 0; i < c->count; i++) {
		if (strcmp(c->name[i], "aaa") == 0) {
			aaa++;
		} else if (strcmp(c->name[i], "bbb") == 0) {
			bbb++;
		} else if (strcmp(c->name[i], "ccc") == 0) {
			ccc++;
		} else {
			return false;
		}
	}
	return aaa == 1 && bbb == 1 && ccc == 1;
}

/* The cookie of a given entry (matched by fileid) is the same on every
 * path that delivers it. */
static bool cookies_agree(const struct cookie_collect *a,
			  const struct cookie_collect *b)
{
	uint32_t i, j;

	if (a->count != b->count) {
		return false;
	}
	for (i = 0; i < a->count; i++) {
		bool matched = false;

		for (j = 0; j < b->count; j++) {
			if (b->fileid[j] == a->fileid[i]) {
				matched = (b->cookie[j] == a->cookie[i]);
				break;
			}
		}
		if (!matched) {
			return false;
		}
	}
	return true;
}

static void test_catalogue_readdir_cookie_contract(void)
{
	struct mds_catalogue *cat;
	struct mds_inode child;
	struct cookie_collect got;
	struct cookie_collect plain;
	uint64_t mid_cookie;
	uint64_t last_cookie;
	char *path;

	cat = open_test_cat(&path);

	ASSERT_EQ(mds_cat_ns_create(cat, NULL, g_dir,
				    "aaa", MDS_FTYPE_DIR, 0755,
				    0, 0, NULL, &child), MDS_OK);
	ASSERT_EQ(mds_cat_ns_create(cat, NULL, g_dir,
				    "bbb", MDS_FTYPE_REG, 0644,
				    0, 0, NULL, &child), MDS_OK);
	ASSERT_EQ(mds_cat_ns_create(cat, NULL, g_dir,
				    "ccc", MDS_FTYPE_DIR, 0755,
				    0, 0, NULL, &child), MDS_OK);

	/* Plain readdir. */
	memset(&plain, 0, sizeof(plain));
	ASSERT_EQ(mds_cat_ns_readdir(cat, g_dir, NULL, 0, NULL,
				     cookie_collect_cb, &plain), MDS_OK);
	ASSERT_EQ(plain.count, (uint32_t)3);
	ASSERT_TRUE(cookies_valid(&plain));
	ASSERT_TRUE(names_once(&plain));

	/* readdir_plus (fused or the dispatcher fallback): the producer's
	 * cookie reaches the caller unchanged. */
	memset(&got, 0, sizeof(got));
	ASSERT_EQ(mds_cat_ns_readdir_plus(cat, g_dir, NULL, 0,
					  NULL, cookie_collect_plus_cb, &got),
		  MDS_OK);
	ASSERT_EQ(got.count, (uint32_t)3);
	ASSERT_TRUE(cookies_valid(&got));
	ASSERT_TRUE(names_once(&got));
	ASSERT_TRUE(cookies_agree(&plain, &got));

	/* Cookie-resume path, first page: ascending cookies. */
	memset(&got, 0, sizeof(got));
	ASSERT_EQ(mds_cat_ns_readdir_plus_from_cookie(cat, g_dir,
						      0, 0, NULL,
						      cookie_collect_plus_cb,
						      &got),
		  MDS_OK);
	ASSERT_EQ(got.count, (uint32_t)3);
	ASSERT_TRUE(cookies_valid(&got));
	ASSERT_TRUE(names_once(&got));
	ASSERT_TRUE(cookies_agree(&plain, &got));
	ASSERT_TRUE(got.cookie[0] < got.cookie[1]);
	ASSERT_TRUE(got.cookie[1] < got.cookie[2]);
	mid_cookie = got.cookie[1];
	last_cookie = got.cookie[2];

	/* Page size 1, resuming with the last delivered cookie: every name
	 * exactly once and cookies strictly increasing across resumes. */
	{
		struct cookie_collect paged;
		uint64_t cursor = 0;
		unsigned pages = 0;

		memset(&paged, 0, sizeof(paged));
		for (;;) {
			struct cookie_collect one;

			memset(&one, 0, sizeof(one));
			ASSERT_EQ(mds_cat_ns_readdir_plus_from_cookie(
					  cat, g_dir, cursor, 1, NULL,
					  cookie_collect_plus_cb, &one),
				  MDS_OK);
			if (one.count == 0) {
				break;
			}
			ASSERT_EQ(one.count, (uint32_t)1);
			ASSERT_TRUE(one.cookie[0] > cursor);
			ASSERT_TRUE(paged.count < READDIR_PLUS_MAX);
			paged.fileid[paged.count] = one.fileid[0];
			paged.cookie[paged.count] = one.cookie[0];
			snprintf(paged.name[paged.count],
				 sizeof(paged.name[0]), "%s", one.name[0]);
			paged.count++;
			cursor = one.cookie[0];
			pages++;
			ASSERT_TRUE(pages <= 4);
		}
		ASSERT_EQ(pages, 3U);
		ASSERT_TRUE(cookies_valid(&paged));
		ASSERT_TRUE(names_once(&paged));
		ASSERT_TRUE(cookies_agree(&plain, &paged));
	}

	/* Resume after the middle entry: exactly the strictly-greater one. */
	memset(&got, 0, sizeof(got));
	ASSERT_EQ(mds_cat_ns_readdir_plus_from_cookie(cat, g_dir,
						      mid_cookie, 0, NULL,
						      cookie_collect_plus_cb,
						      &got),
		  MDS_OK);
	ASSERT_EQ(got.count, (uint32_t)1);
	ASSERT_EQ(got.cookie[0], last_cookie);

	/* Resume after the last entry: drained. */
	memset(&got, 0, sizeof(got));
	ASSERT_EQ(mds_cat_ns_readdir_plus_from_cookie(cat, g_dir,
						      last_cookie, 0, NULL,
						      cookie_collect_plus_cb,
						      &got),
		  MDS_OK);
	ASSERT_EQ(got.count, (uint32_t)0);

	close_test_cat(cat, path);
}

/* -----------------------------------------------------------------------
 * 5c. test_catalogue_stripe_map_wide -- Phase A foundation check
 *
 * Verifies that the catalogue can store and retrieve stripe maps at the
 * Phase A ceiling (MDS_MAX_STRIPES = 1024).  Goes through the full
 * mds_cat_stripe_map_put / get / del path so any heap-conversion
 * mistakes (forgotten realloc, missing free) surface as ASAN errors
 * or memory leaks under Valgrind.
 * ----------------------------------------------------------------------- */

static void test_catalogue_stripe_map_wide_round_trip(void)
{
	struct mds_catalogue *cat;
	struct mds_inode child;
	struct mds_ds_map_entry *entries_in = NULL;
	struct mds_ds_map_entry *entries_out = NULL;
	uint32_t got_sc = 0, got_su = 0, got_mc = 0;
	char *path;
	const uint32_t cases[] = {16, 128, 512, MDS_MAX_STRIPES};
	const uint32_t case_count = sizeof(cases) / sizeof(cases[0]);
	uint32_t c, i;

	/* Sanity: Phase A guarantees the ceiling is at least 1024. */
	ASSERT_TRUE(MDS_MAX_STRIPES >= 1024);

	cat = open_test_cat(&path);

	ASSERT_EQ(mds_cat_ns_create(cat, NULL, g_dir,
				    "wide.bin", MDS_FTYPE_REG, 0644,
				    0, 0, NULL, &child), MDS_OK);

	for (c = 0; c < case_count; c++) {
		const uint32_t stripe_count = cases[c];
		const uint32_t stripe_unit  = 1U << 20;  /* 1 MiB */
		const uint32_t mirror_count = 1;
		const uint32_t total = stripe_count * mirror_count;

		entries_in = calloc(total, sizeof(*entries_in));
		ASSERT_NE(entries_in, NULL);
		for (i = 0; i < total; i++) {
			entries_in[i].ds_id = (uint32_t)(i + 1);
			entries_in[i].nfs_fh_len = 4;
			entries_in[i].nfs_fh[0] = (uint8_t)(i & 0xFF);
			entries_in[i].nfs_fh[1] = (uint8_t)((i >> 8) & 0xFF);
			entries_in[i].nfs_fh[2] = 0xAA;
			entries_in[i].nfs_fh[3] = 0x55;
		}

		ASSERT_EQ(mds_cat_stripe_map_put(cat, NULL, child.fileid,
						 stripe_count, stripe_unit,
						 mirror_count, entries_in),
			  MDS_OK);

		entries_out = NULL;
		got_sc = got_su = got_mc = 0;
		ASSERT_EQ(mds_cat_stripe_map_get(cat, child.fileid,
						 &got_sc, &got_su, &got_mc,
						 &entries_out),
			  MDS_OK);
		ASSERT_EQ(got_sc, stripe_count);
		ASSERT_EQ(got_su, stripe_unit);
		ASSERT_EQ(got_mc, mirror_count);
		ASSERT_NE(entries_out, NULL);
		for (i = 0; i < total; i++) {
			ASSERT_EQ(entries_out[i].ds_id, (uint32_t)(i + 1));
			ASSERT_EQ(entries_out[i].nfs_fh_len, (uint32_t)4);
			ASSERT_EQ((int)entries_out[i].nfs_fh[2], 0xAA);
		}
		free(entries_out);
		free(entries_in);
		entries_in = NULL;
		entries_out = NULL;
	}

	ASSERT_EQ(mds_cat_stripe_map_del(cat, NULL, child.fileid), MDS_OK);

	/* After delete, get should return NOTFOUND. */
	ASSERT_EQ(mds_cat_stripe_map_get(cat, child.fileid,
					 &got_sc, &got_su, &got_mc,
					 &entries_out),
		  MDS_ERR_NOTFOUND);

	close_test_cat(cat, path);
}

/* -----------------------------------------------------------------------
 * 6. test_catalogue_layout_grant_return -- layout state round-trip
 * ----------------------------------------------------------------------- */

static void test_catalogue_layout_grant_return(void)
{
	struct mds_catalogue *cat;
	struct nfs4_stateid sid;
	uint64_t got_cid, got_fid;
	uint32_t got_iomode, got_seqid;
	uint64_t got_off, got_len;
	uint32_t ds_ids[1] = {1};
	bool has_layout = false;
	uint64_t fid;
	char *path;

	cat = open_test_cat(&path);
	fid = fresh_fileid(cat);

	memset(&sid, 0, sizeof(sid));
	sid.seqid = 1;
	memset(sid.other, 0xAB, sizeof(sid.other));

	/* Grant. */
	ASSERT_EQ(mds_coord_layout_grant(cat, NULL, 100, fid,
					 2 /* RW */, 0, UINT64_MAX,
					 &sid, ds_ids, 1),
		  MDS_OK);

	/* Verify by stateid lookup. */
	ASSERT_EQ(mds_coord_layout_get_by_stateid(cat, sid.other,
						  &got_cid, &got_fid,
						  &got_iomode, &got_off,
						  &got_len, &got_seqid),
		  MDS_OK);
	ASSERT_EQ(got_cid, (uint64_t)100);
	ASSERT_EQ(got_fid, fid);

	/* Scan for file. */
	ASSERT_EQ(mds_coord_layout_scan_for_file(cat, fid, &has_layout),
		  MDS_OK);
	ASSERT_TRUE(has_layout);

	/* Return. */
	ASSERT_EQ(mds_coord_layout_return(cat, NULL, sid.other,
					  100, fid, ds_ids, 1),
		  MDS_OK);

	/* Should be gone. */
	has_layout = true;
	ASSERT_EQ(mds_coord_layout_scan_for_file(cat, fid, &has_layout),
		  MDS_OK);
	ASSERT_EQ(has_layout, false);

	close_test_cat(cat, path);
}

/* -----------------------------------------------------------------------
 * 6b. test_catalogue_layout_iter_file -- per-file holder enumeration
 *
 * Locks the behavioural contract that rondb_shim_layout_iter_file must
 * preserve after the partition-pruned rewrite: enumerate exactly the
 * holders of the target fileid (correct clientid / iomode / seqid
 * tuple), never a holder of a different file, and return cleanly with
 * zero holders for a file that has none.
 * ----------------------------------------------------------------------- */

struct layout_iter_test_ctx {
	uint32_t hits;
	bool     saw_c10, saw_c11, saw_c12;
	bool     saw_foreign;
	uint32_t iomode_c10, iomode_c11, iomode_c12;
	uint32_t seqid_c10, seqid_c11, seqid_c12;
};

static int layout_iter_test_cb(uint64_t clientid,
			       const struct nfs4_stateid *stateid,
			       uint32_t iomode, void *ctx)
{
	struct layout_iter_test_ctx *c = ctx;
	uint32_t seqid = (stateid != NULL) ? stateid->seqid : 0;

	c->hits++;
	switch (clientid) {
	case 10: c->saw_c10 = true; c->iomode_c10 = iomode; c->seqid_c10 = seqid; break;
	case 11: c->saw_c11 = true; c->iomode_c11 = iomode; c->seqid_c11 = seqid; break;
	case 12: c->saw_c12 = true; c->iomode_c12 = iomode; c->seqid_c12 = seqid; break;
	case 99: c->saw_foreign = true; break;
	default: break;
	}
	return 0;
}

static void test_catalogue_layout_iter_file(void)
{
	struct mds_catalogue *cat;
	struct nfs4_stateid sid;
	uint32_t ds_ids[1] = {1};
	struct layout_iter_test_ctx ctx;
	uint64_t fid, other, none;
	char *path;

	cat = open_test_cat(&path);
	fid = fresh_fileid(cat);
	other = fresh_fileid(cat);
	none = fresh_fileid(cat);

	/* Three holders on one file (distinct clientid / iomode / seqid). */
	memset(&sid, 0, sizeof(sid));
	sid.seqid = 1; memset(sid.other, 0x10, sizeof(sid.other));
	ASSERT_EQ(mds_coord_layout_grant(cat, NULL, 10, fid, 1,
					 0, UINT64_MAX, &sid, ds_ids, 1),
		  MDS_OK);
	sid.seqid = 2; memset(sid.other, 0x11, sizeof(sid.other));
	ASSERT_EQ(mds_coord_layout_grant(cat, NULL, 11, fid, 2,
					 0, UINT64_MAX, &sid, ds_ids, 1),
		  MDS_OK);
	sid.seqid = 3; memset(sid.other, 0x12, sizeof(sid.other));
	ASSERT_EQ(mds_coord_layout_grant(cat, NULL, 12, fid, 1,
					 0, UINT64_MAX, &sid, ds_ids, 1),
		  MDS_OK);

	/* A holder on a different file must never appear for fid. */
	sid.seqid = 9; memset(sid.other, 0x99, sizeof(sid.other));
	ASSERT_EQ(mds_coord_layout_grant(cat, NULL, 99, other, 2,
					 0, UINT64_MAX, &sid, ds_ids, 1),
		  MDS_OK);

	/* Enumerate holders of fid. */
	memset(&ctx, 0, sizeof(ctx));
	ASSERT_EQ(mds_coord_layout_iter_file(cat, fid,
					     layout_iter_test_cb, &ctx),
		  MDS_OK);
	ASSERT_EQ(ctx.hits, (uint32_t)3);
	ASSERT_TRUE(ctx.saw_c10);
	ASSERT_TRUE(ctx.saw_c11);
	ASSERT_TRUE(ctx.saw_c12);
	ASSERT_EQ(ctx.saw_foreign, false);
	ASSERT_EQ(ctx.iomode_c10, (uint32_t)1);
	ASSERT_EQ(ctx.iomode_c11, (uint32_t)2);
	ASSERT_EQ(ctx.iomode_c12, (uint32_t)1);
	ASSERT_EQ(ctx.seqid_c10, (uint32_t)1);
	ASSERT_EQ(ctx.seqid_c11, (uint32_t)2);
	ASSERT_EQ(ctx.seqid_c12, (uint32_t)3);

	/* A file with no layouts yields zero holders, no error. */
	memset(&ctx, 0, sizeof(ctx));
	ASSERT_EQ(mds_coord_layout_iter_file(cat, none,
					     layout_iter_test_cb, &ctx),
		  MDS_OK);
	ASSERT_EQ(ctx.hits, (uint32_t)0);

	close_test_cat(cat, path);
}

/* -----------------------------------------------------------------------
 * 7. test_catalogue_recovery_put_get_del -- client recovery round-trip
 * ----------------------------------------------------------------------- */

static void test_catalogue_recovery_put_get_del(void)
{
	struct mds_catalogue *cat;
	uint8_t owner_in[8] = {1, 2, 3, 4, 5, 6, 7, 8};
	uint8_t verifier_in[8] = {0xDE, 0xAD, 0xBE, 0xEF, 0, 0, 0, 0};
	uint8_t owner_out[1024];
	uint32_t owner_out_len = 0;
	uint8_t verifier_out[8];
	char *path;

	cat = open_test_cat(&path);

	ASSERT_EQ(mds_coord_recovery_put(cat, NULL, 42,
					 owner_in, 8, verifier_in),
		  MDS_OK);

	memset(owner_out, 0, sizeof(owner_out));
	memset(verifier_out, 0, sizeof(verifier_out));
	ASSERT_EQ(mds_coord_recovery_get(cat, 42, owner_out,
					 &owner_out_len, verifier_out),
		  MDS_OK);
	ASSERT_EQ(owner_out_len, (uint32_t)8);
	ASSERT_EQ(memcmp(owner_out, owner_in, 8), 0);
	ASSERT_EQ(memcmp(verifier_out, verifier_in, 8), 0);

	ASSERT_EQ(mds_coord_recovery_del(cat, NULL, 42), MDS_OK);

	ASSERT_EQ(mds_coord_recovery_get(cat, 42, owner_out,
					 &owner_out_len, verifier_out),
		  MDS_ERR_NOTFOUND);

	close_test_cat(cat, path);
}

/* -----------------------------------------------------------------------
 * 8. test_catalogue_txn_commit -- txn begin / commit visibility
 *
 * The original test also exercised the abort path ("create in txn,
 * abort, verify the entry is NOT visible").  That assertion described
 * a rollback no backend provides: struct mds_cat_txn is a grouping
 * context (contract C6, catalogue_internal.h / mds_catalogue.h) --
 * every operation is committed by the backend on its own and abort
 * only frees the token, on RonDB and memdb alike.  This test keeps the
 * commit path; the conformance suite (tests/catalogue_conformance/
 * test_token_semantics.c) pins the abort semantics positively (the
 * create survives the abort) on every backend.
 * ----------------------------------------------------------------------- */

static void test_catalogue_txn_commit_abort(void)
{
	struct mds_catalogue *cat;
	struct mds_cat_txn *txn = NULL;
	struct mds_inode child, looked_up;
	char *path;

	cat = open_test_cat(&path);

	/* Commit path: create in txn, commit, verify visible. */
	ASSERT_EQ(mds_cat_txn_begin(cat, MDS_CAT_TXN_WRITE, &txn),
		  MDS_OK);
	ASSERT_NE(txn, NULL);
	ASSERT_EQ(mds_cat_ns_create(cat, txn, g_dir,
				    "committed.dir", MDS_FTYPE_DIR,
				    0755, 0, 0, NULL, &child),
		  MDS_OK);
	ASSERT_EQ(mds_cat_txn_commit(txn), MDS_OK);

	ASSERT_EQ(mds_cat_ns_lookup(cat, g_dir,
				    "committed.dir", &looked_up),
		  MDS_OK);

	/* Abort path: begin/abort cycle must complete cleanly without
	 * crashing or leaking the txn handle, even on a backend that
	 * does not roll back individual writes.  Visibility of the
	 * pre-abort write is intentionally NOT asserted (see header). */
	ASSERT_EQ(mds_cat_txn_begin(cat, MDS_CAT_TXN_WRITE, &txn),
		  MDS_OK);
	ASSERT_EQ(mds_cat_ns_create(cat, txn, g_dir,
				    "aborted.dir", MDS_FTYPE_DIR,
				    0755, 0, 0, NULL, &child),
		  MDS_OK);
	mds_cat_txn_abort(txn);

	close_test_cat(cat, path);
}

/* -----------------------------------------------------------------------
 * 9. test_catalogue_escape_hatch -- escape hatch returns valid handle
 * ----------------------------------------------------------------------- */

static void test_catalogue_escape_hatch(void)
{
	struct mds_catalogue *cat;
	struct mds_catalogue *db;
	char *path;

	cat = open_test_cat(&path);

	db = cat;
	ASSERT_NE(db, NULL);

	/* The returned db must be usable for raw catalogue reads. */
	{
		struct mds_inode root;

		ASSERT_EQ(mds_cat_ns_getattr(db, MDS_FILEID_ROOT,
					     &root),
			  MDS_OK);
		ASSERT_EQ(root.fileid, (uint64_t)MDS_FILEID_ROOT);
	}

	/* NULL catalogue returns NULL. */
	/* catalogue escape hatch removed */

	close_test_cat(cat, path);
}

/* -----------------------------------------------------------------------
 * 10. test_catalogue_shard_routing_guard -- RETIRED.
 *
 * The Phase-12 RonDB-only refactor collapsed the per-shard catalogue
 * routing concept: cd->cat is now the single source of truth, and
 * cat_getattr / cat_on_root_db etc. are thin one-liners over
 * cd->cat (see src/mds/compound_internal.h:cat_on_root_db, line ~422).
 * The old behaviour this test validated -- helpers "falling through
 * to raw catalogue on cd->db" when cd was swapped to a child shard
 * -- no longer exists, and the test's own setup (allocating fileids
 * from two memdb instances whose sequences both start at the same
 * base, then asserting the same fileid is NOT visible in the second
 * instance) cannot be satisfied without either teaching memdb about
 * non-overlapping fileid ranges or rewriting the test entirely.
 *
 * Coverage of the surviving routing contract (cat_getattr just
 * hits cd->cat) is implicit in the rest of test_catalogue and in
 * test_compound's full-flow tests against the same backend.
 *
 * Same applies to test_catalogue_root_global_helper_routing below.
 * ----------------------------------------------------------------------- */

#if 0  /* retired: see header comment above. */
static void test_catalogue_shard_routing_guard(void)
{
	struct mds_catalogue *cat;
	struct mds_catalogue *child_db = NULL;
	struct compound_data cd;
	struct mds_inode inode, child_inode;
	char *root_path;
	char *child_path;

	/* Open root catalogue. */
	cat = open_test_cat(&root_path);

	/* Open a separate catalogue env as "child shard". */
	child_path = make_temp_db_path();
	ASSERT_EQ((child_db = open_test_catalogue()) != NULL ? MDS_OK : MDS_ERR_IO,
		  MDS_OK);

	/* Create a directory in root only. */
	ASSERT_EQ(mds_cat_ns_create(cat, NULL, MDS_FILEID_ROOT,
				    "root_only", MDS_FTYPE_DIR,
				    0755, 0, 0, NULL, &inode),
		  MDS_OK);

	/* Create a directory in child only (via raw catalogue). */
	ASSERT_EQ(mds_cat_ns_create(child_db, NULL, MDS_FILEID_ROOT,
				"child_only", MDS_FTYPE_DIR,
				0755, 0, 0, NULL, &child_inode),
		  MDS_OK);

	/* Simulate compound_data on root shard. */
	memset(&cd, 0, sizeof(cd));
	cd.cat = cat;
	cd.cat = cat; /* root db */

	/* cat_getattr on root shard: should use catalogue and find it. */
	{
		struct mds_inode got;

		ASSERT_EQ(cat_getattr(&cd, inode.fileid, &got), MDS_OK);
		ASSERT_EQ(got.fileid, inode.fileid);
	}

	/* Now swap cd->db to child shard (simulates apply_shard). */
	cd.cat = child_db;

	/* cat_getattr should fall through to raw catalogue on child_db.
	 * root_only.txt should NOT be found (it's in root, not child). */
	{
		struct mds_inode got;

		ASSERT_EQ(cat_getattr(&cd, inode.fileid, &got),
			  MDS_ERR_NOTFOUND);
	}

	/* child_only.txt should be found via raw catalogue on child_db. */
	{
		struct mds_inode got;

		ASSERT_EQ(cat_getattr(&cd, child_inode.fileid, &got),
			  MDS_OK);
		ASSERT_EQ(got.fileid, child_inode.fileid);
	}

	mds_catalogue_close(child_db);
	cleanup_temp_db(child_path);
	free(child_path);
	close_test_cat(cat, root_path);
}

/* ----------------------------------------------------------------------- */

static void test_catalogue_root_global_helper_routing(void)
{
	struct mds_catalogue *cat;
	struct mds_catalogue *child_db = NULL;
	struct compound_data cd;
	struct mds_ds_info info;
	struct mds_ds_info got_info;
	struct mds_ds_info *ds_list = NULL;
	struct nfs4_stateid sid;
	uint8_t secret[32];
	uint8_t got_secret[32];
	uint64_t epoch = 0;
	uint64_t got_cid = 0;
	uint64_t got_fid = 0;
	uint32_t count = 0;
	uint32_t ds_ids[1] = {7};
	bool has_layout = false;
	char *root_path;
	char *child_path;
	uint32_t i;

	cat = open_test_cat(&root_path);

	child_path = make_temp_db_path();
	ASSERT_EQ((child_db = open_test_catalogue()) != NULL ? MDS_OK : MDS_ERR_IO, MDS_OK);

	memset(&info, 0, sizeof(info));
	info.ds_id = ds_ids[0];
	info.state = DS_ONLINE;
	info.mode = DS_MODE_GENERIC;
	info.transport = DS_TRANSPORT_TCP;
	info.capabilities = 0;
	info.port = 2049;
	snprintf(info.addr, sizeof(info.addr), "%s", "10.0.0.7:/export7");
	ASSERT_EQ(mds_cat_ds_put(cat, NULL, &info), MDS_OK);

	for (i = 0; i < 32; i++) {
		secret[i] = (uint8_t)(0xA0 + i);
	}
	ASSERT_EQ(mds_cat_ds_provision_put(cat, NULL, ds_ids[0],
					   secret, sizeof(secret), 77),
		  MDS_OK);

	memset(&sid, 0, sizeof(sid));
	sid.seqid = 1;
	memset(sid.other, 0x5A, sizeof(sid.other));
	ASSERT_EQ(mds_coord_layout_grant(cat, NULL, 101, 202, 2,
					 0, UINT64_MAX, &sid,
					 ds_ids, 1),
		  MDS_OK);

	memset(&cd, 0, sizeof(cd));
	cd.cat = cat;
	cd.cat = child_db;
	cd.cat = cat;

	ASSERT_EQ(cat_on_root_db(&cd), false);
	ASSERT_TRUE(cat_on_root_global_db(&cd));

	ASSERT_EQ(cat_ds_get(&cd, ds_ids[0], &got_info), MDS_OK);
	ASSERT_EQ(got_info.ds_id, info.ds_id);

	ASSERT_EQ(cat_ds_list(&cd, &ds_list, &count), MDS_OK);
	ASSERT_EQ(count, (uint32_t)1);
	ASSERT_EQ(ds_list[0].ds_id, info.ds_id);
	free(ds_list);

	memset(got_secret, 0, sizeof(got_secret));
	ASSERT_EQ(cat_ds_provision_get(&cd, ds_ids[0], got_secret,
				       sizeof(got_secret), &epoch),
		  MDS_OK);
	ASSERT_EQ(epoch, (uint64_t)77);
	ASSERT_EQ(memcmp(got_secret, secret, sizeof(secret)), 0);

	ASSERT_EQ(coord_layout_get_by_stateid(&cd, sid.other,
					      &got_cid, &got_fid,
					      NULL, NULL, NULL, NULL),
		  MDS_OK);
	ASSERT_EQ(got_cid, (uint64_t)101);
	ASSERT_EQ(got_fid, (uint64_t)202);

	ASSERT_EQ(coord_layout_scan_for_file(&cd, 202, &has_layout), MDS_OK);
	ASSERT_TRUE(has_layout);

	mds_catalogue_close(child_db);
	cleanup_temp_db(child_path);
	free(child_path);
	close_test_cat(cat, root_path);
}
#endif  /* retired: shard-routing tests, see comment near line ~666. */

/* -----------------------------------------------------------------------
 * test_catalogue_ns_rename_keep_orphan — MDS_CAT_RNF_KEEP_DST_ORPHAN
 *
 * Overwriting the final link of a regular file via the flags-aware
 * rename must either delete the destination inode row (flags == 0,
 * the legacy semantics) or keep it flagged MDS_IFLAG_UNLINK_ORPHAN
 * with nlink 0 (KEEP_DST_ORPHAN — the unlink-of-open contract that
 * op_rename relies on for pynfs RNM21).
 * ----------------------------------------------------------------------- */

static void test_catalogue_ns_rename_keep_orphan(void)
{
	struct mds_catalogue *cat;
	struct mds_inode src, dst, looked;
	char *path;

	cat = open_test_cat(&path);

	/* Case 1: KEEP_DST_ORPHAN — the overwritten inode survives. */
	ASSERT_EQ(mds_cat_ns_create(cat, NULL, g_dir,
				    "ren-src", MDS_FTYPE_REG,
				    0644, 1000, 1000, NULL, &src),
		  MDS_OK);
	ASSERT_EQ(mds_cat_ns_create(cat, NULL, g_dir,
				    "ren-dst", MDS_FTYPE_REG,
				    0644, 1000, 1000, NULL, &dst),
		  MDS_OK);
	ASSERT_EQ(mds_cat_ns_rename_flags(cat, NULL,
					  g_dir, "ren-src",
					  g_dir, "ren-dst",
					  MDS_CAT_RNF_KEEP_DST_ORPHAN),
		  MDS_OK);
	/* Name now resolves to the source file... */
	ASSERT_EQ(mds_cat_ns_lookup(cat, g_dir,
				    "ren-dst", &looked), MDS_OK);
	ASSERT_EQ(looked.fileid, src.fileid);
	/* ...and the overwritten inode row survived as an orphan. */
	ASSERT_EQ(mds_cat_ns_getattr(cat, dst.fileid, &looked), MDS_OK);
	ASSERT_EQ(looked.nlink, (uint32_t)0);
	ASSERT_TRUE((looked.flags & MDS_IFLAG_UNLINK_ORPHAN) != 0U);

	/* Case 2: flags == 0 — the overwritten inode row is deleted. */
	ASSERT_EQ(mds_cat_ns_create(cat, NULL, g_dir,
				    "ren-src2", MDS_FTYPE_REG,
				    0644, 1000, 1000, NULL, &src),
		  MDS_OK);
	ASSERT_EQ(mds_cat_ns_create(cat, NULL, g_dir,
				    "ren-dst2", MDS_FTYPE_REG,
				    0644, 1000, 1000, NULL, &dst),
		  MDS_OK);
	ASSERT_EQ(mds_cat_ns_rename_flags(cat, NULL,
					  g_dir, "ren-src2",
					  g_dir, "ren-dst2",
					  0U),
		  MDS_OK);
	ASSERT_EQ(mds_cat_ns_getattr(cat, dst.fileid, &looked),
		  MDS_ERR_NOTFOUND);

	close_test_cat(cat, path);
}

/* -----------------------------------------------------------------------
 * test_catalogue_memdb_identity_and_capabilities
 *
 * The in-memory backend must report its own identity (no longer
 * impersonating RonDB), expose no native handle, advertise the shared-
 * authority capability the cross-subtree rename path relies on, and
 * degrade every optional slot it does not implement to NOSUPPORT with
 * the documented out-param state -- exactly what a third backend that
 * skips the fused fast paths would see.
 * ----------------------------------------------------------------------- */

static void test_catalogue_memdb_identity_and_capabilities(void)
{
	struct mds_catalogue *cat;
	struct nfs4_stateid sid;
	struct mds_inode out;
	struct mds_ds_map_entry entry;
	struct mds_ds_map_entry *entries;
	uint32_t sc = 1, su = 1, mc = 1;
	bool layout_ok = true;
	uint32_t pop_unit = 1;
	char *path;

	cat = open_test_cat(&path);

	ASSERT_EQ(mds_catalogue_backend_type(cat), MDS_BACKEND_MEMDB);
	ASSERT_EQ(mds_catalogue_backend_handle(cat) == NULL, 1);
	ASSERT_TRUE(mds_catalogue_shared_authority(cat));

	/* No schema to bootstrap. */
	ASSERT_EQ(mds_catalogue_bootstrap_supported(cat), false);
	ASSERT_EQ(mds_catalogue_bootstrap(cat), MDS_ERR_NOSUPPORT);

	/* Fused fast paths absent: callers fall back to the split ops. */
	memset(&sid, 0, sizeof(sid));
	ASSERT_EQ(mds_coord_layoutget_fused_supported(cat), false);
	entries = &entry;
	ASSERT_EQ(mds_coord_layoutget_fused(cat, MDS_FILEID_ROOT, &sc, &su,
					    &mc, &entries, &sid, 1, 2, 0,
					    UINT64_MAX, 1),
		  MDS_ERR_NOSUPPORT);
	ASSERT_EQ(entries == NULL, 1);

	ASSERT_EQ(mds_cat_ns_create_with_layout_supported(cat), false);
	memset(&entry, 0xFF, sizeof(entry));
	ASSERT_EQ(mds_cat_ns_create_with_layout(cat, MDS_FILEID_ROOT,
						"fused", MDS_FTYPE_REG,
						0644, 0, 0, NULL, &out,
						1, 2, 0, UINT64_MAX, &sid, 1,
						&layout_ok, &entry, &pop_unit),
		  MDS_ERR_NOSUPPORT);
	ASSERT_EQ(layout_ok, false);
	ASSERT_EQ(pop_unit, (uint32_t)0);
	ASSERT_EQ(entry.nfs_fh_len, (uint32_t)0);
	/* And nothing was created behind the NOSUPPORT. */
	ASSERT_EQ(mds_cat_ns_lookup(cat, MDS_FILEID_ROOT, "fused", &out),
		  MDS_ERR_NOTFOUND);

	/* memdb populates the open/lock/deleg slots (as stubs), so the
	 * daemon wires write-through and the tables see NOSUPPORT /
	 * OK from the individual rows -- the documented "nothing to
	 * persist" outcome. */
	ASSERT_TRUE(mds_coord_shared_state_supported(cat));

	/* An in-process store is never a multi-process cluster store,
	 * whatever cluster slots it may grow for in-process tests: the
	 * daemon must refuse cluster_size > 1 on it by construction. */
	ASSERT_EQ(mds_cluster_supported(cat), false);

	close_test_cat(cat, path);
}

/* -----------------------------------------------------------------------
 * Main
 * ----------------------------------------------------------------------- */

int main(void)
{
	fprintf(stdout, "test_catalogue (backend=%s):\n",
		conformance_backend_name());

	RUN_TEST(test_catalogue_open_close);
	RUN_TEST(test_catalogue_ns_create_lookup);
	RUN_TEST(test_catalogue_ns_setattr_getattr);
	RUN_TEST(test_catalogue_ns_remove);
	RUN_TEST(test_catalogue_ns_readdir);
	RUN_TEST(test_catalogue_ns_readdir_plus_empty);
	RUN_TEST(test_catalogue_ns_readdir_plus_three_entries);
	RUN_TEST(test_catalogue_ns_readdir_plus_start_after);
	RUN_TEST(test_catalogue_ns_readdir_max_entries);
	RUN_TEST(test_catalogue_dirent_name_for_child);
	RUN_TEST(test_catalogue_readdir_cookie_contract);
	RUN_TEST(test_catalogue_dirent_insert_only);
	RUN_TEST(test_catalogue_ns_rename_keep_orphan);
	RUN_TEST(test_catalogue_stripe_map_wide_round_trip);
	RUN_TEST(test_catalogue_layout_grant_return);
	RUN_TEST(test_catalogue_layout_iter_file);
	RUN_TEST(test_catalogue_coordination_cq_accepts_wide_ds_count);
	RUN_TEST(test_catalogue_coordination_dispatch_rejects_null_ds_ids);
	RUN_TEST(test_catalogue_recovery_put_get_del);
	RUN_TEST(test_catalogue_txn_commit_abort);
	RUN_TEST(test_catalogue_escape_hatch);
	/* Identity assertions are about the in-memory backend itself; on
	 * any other backend the harness selects they would test nothing. */
	if (conformance_backend_is("memdb")) {
		RUN_TEST(test_catalogue_memdb_identity_and_capabilities);
	}
	/* test_catalogue_shard_routing_guard and
	 * test_catalogue_root_global_helper_routing retired -- see
	 * comment at the function definitions. */

		fprintf(stdout, "\ntest_catalogue: %d/%d passed\n",
			tests_passed, tests_run);
		conformance_shutdown();
		return (tests_passed == tests_run) ? 0 : 1;
}
