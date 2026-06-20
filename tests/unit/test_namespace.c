/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_namespace.c -- Unit tests for namespace operations.
 *
 * Each test creates a fresh catalogue environment in a mkdtemp directory,
 * exercises the namespace API, asserts invariants, and cleans up.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>
#include <time.h>
#include <pthread.h>

#include "pnfs_mds.h"
#include "test_helpers.h"

/* -----------------------------------------------------------------------
 * Test helpers (catalogue test helpers)
 * ----------------------------------------------------------------------- */

#define TEST_MAP_SIZE (16ULL * 1024 * 1024)

static int tests_run;
static int tests_passed;

#define ASSERT_EQ(a, b) do {						\
	if ((a) != (b)) {						\
		fprintf(stderr, "  FAIL %s:%d: %s != %s\n",		\
			__FILE__, __LINE__, #a, #b);			\
		return;							\
	}								\
} while (0)

#define ASSERT_NE(a, b) do {						\
	if ((a) == (b)) {						\
		fprintf(stderr, "  FAIL %s:%d: %s == %s\n",		\
			__FILE__, __LINE__, #a, #b);			\
		return;							\
	}								\
} while (0)

#define ASSERT_TRUE(x)  ASSERT_NE((x), 0)

#define RUN_TEST(fn) do {						\
	tests_run++;							\
	fprintf(stdout, "  %-40s", #fn);				\
	fflush(stdout);							\
	fn();								\
	tests_passed++;							\
	fprintf(stdout, "PASS\n");					\
} while (0)

static char *make_temp_db_path(void)
{
	char tmpl[] = "/tmp/pnfs-mds-test-XXXXXX";
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

/** Open a fresh catalogue + verify success. */
static struct mds_catalogue *open_test_db(char **path_out)
{
	struct mds_catalogue *db = NULL;
	enum mds_status st;

	*path_out = make_temp_db_path();
	st = ((db = open_test_catalogue()) != NULL ? MDS_OK : MDS_ERR_IO);
	assert(st == MDS_OK);
	assert(db != NULL);
	return db;
}

static void close_test_db(struct mds_catalogue *db, char *path)
{
	mds_catalogue_close(db);
	cleanup_temp_db(path);
	free(path);
}

/* -----------------------------------------------------------------------
 * test_lookup_root -- root inode accessible via getattr
 * ----------------------------------------------------------------------- */

static void test_lookup_root(void)
{
	struct mds_catalogue *db;
	struct mds_inode root;
	char *path;

	db = open_test_db(&path);

	ASSERT_EQ(mds_ns_getattr(db, MDS_FILEID_ROOT, &root), MDS_OK);
	ASSERT_EQ(root.fileid, (uint64_t)MDS_FILEID_ROOT);
	ASSERT_EQ(root.type, MDS_FTYPE_DIR);
	ASSERT_EQ(root.nlink, (uint32_t)2);
	ASSERT_EQ(root.mode, (uint32_t)0755);

	close_test_db(db, path);
}

/* -----------------------------------------------------------------------
 * test_create_file -- create a regular file, verify inode + lookup
 * ----------------------------------------------------------------------- */

static void test_create_file(void)
{
	struct mds_catalogue *db;
	struct mds_inode child, looked_up, root;
	char *path;

	db = open_test_db(&path);

	ASSERT_EQ(mds_cat_ns_create(db, NULL, MDS_FILEID_ROOT, "hello.txt",
				MDS_FTYPE_REG, 0644, 1000, 1000,
			       NULL, &child),
		  MDS_OK);

	ASSERT_NE(child.fileid, (uint64_t)0);
	ASSERT_EQ(child.type, MDS_FTYPE_REG);
	ASSERT_EQ(child.mode, (uint32_t)0644);
	ASSERT_EQ(child.nlink, (uint32_t)1);
	ASSERT_EQ(child.uid, (uint64_t)1000);
	ASSERT_EQ(child.gid, (uint64_t)1000);
	ASSERT_EQ(child.change, (uint64_t)1);

	/* Lookup must find the same inode. */
	ASSERT_EQ(mds_ns_lookup(db, MDS_FILEID_ROOT, "hello.txt", &looked_up),
		  MDS_OK);
	ASSERT_EQ(looked_up.fileid, child.fileid);
	ASSERT_EQ(looked_up.type, MDS_FTYPE_REG);

	/* Parent nlink stays at 2 (files don't increment it). */
	ASSERT_EQ(mds_ns_getattr(db, MDS_FILEID_ROOT, &root), MDS_OK);
	ASSERT_EQ(root.nlink, (uint32_t)2);

	/* Duplicate create returns EXISTS. */
	ASSERT_EQ(mds_cat_ns_create(db, NULL, MDS_FILEID_ROOT, "hello.txt",
				MDS_FTYPE_REG, 0644, 0, 0,
			       NULL, &child),
		  MDS_ERR_EXISTS);

	close_test_db(db, path);
}

/* -----------------------------------------------------------------------
 * test_create_directory -- nlink bookkeeping
 * ----------------------------------------------------------------------- */

static void test_create_directory(void)
{
	struct mds_catalogue *db;
	struct mds_inode child, root;
	char *path;

	db = open_test_db(&path);

	ASSERT_EQ(mds_cat_ns_create(db, NULL, MDS_FILEID_ROOT, "subdir",
				MDS_FTYPE_DIR, 0755, 0, 0,
			       NULL, &child), MDS_OK);

	ASSERT_EQ(child.type, MDS_FTYPE_DIR);
	ASSERT_EQ(child.nlink, (uint32_t)2); /* "." + ".." */

	/* Parent nlink should now be 3 (self + ".." from child). */
	ASSERT_EQ(mds_ns_getattr(db, MDS_FILEID_ROOT, &root), MDS_OK);
	ASSERT_EQ(root.nlink, (uint32_t)3);

	close_test_db(db, path);
}

/* -----------------------------------------------------------------------
 * test_remove_file -- create, remove, verify gone
 * ----------------------------------------------------------------------- */

static void test_remove_file(void)
{
	struct mds_catalogue *db;
	struct mds_inode child, tmp;
	char *path;

	db = open_test_db(&path);

	ASSERT_EQ(mds_cat_ns_create(db, NULL, MDS_FILEID_ROOT, "gone.txt",
				MDS_FTYPE_REG, 0644, 0, 0,
			       NULL, &child), MDS_OK);

	ASSERT_EQ(mds_ns_remove(db, MDS_FILEID_ROOT, "gone.txt"), MDS_OK);

	/* Lookup must fail. */
	ASSERT_EQ(mds_ns_lookup(db, MDS_FILEID_ROOT, "gone.txt", &tmp),
		  MDS_ERR_NOTFOUND);

	/* Inode should be gone (nlink was 1). */
	ASSERT_EQ(mds_ns_getattr(db, child.fileid, &tmp), MDS_ERR_NOTFOUND);

	/* Removing again returns NOTFOUND. */
	ASSERT_EQ(mds_ns_remove(db, MDS_FILEID_ROOT, "gone.txt"),
		  MDS_ERR_NOTFOUND);

	close_test_db(db, path);
}

/* -----------------------------------------------------------------------
 * test_remove_dir_empty_check -- NOTEMPTY for non-empty; success for empty
 * ----------------------------------------------------------------------- */

static void test_remove_dir_empty_check(void)
{
	struct mds_catalogue *db;
	struct mds_inode dir, file, root;
	char *path;

	db = open_test_db(&path);

	/* Create subdir + a file inside it. */
	ASSERT_EQ(mds_cat_ns_create(db, NULL, MDS_FILEID_ROOT, "mydir",
				MDS_FTYPE_DIR, 0755, 0, 0,
			       NULL, &dir), MDS_OK);
	ASSERT_EQ(mds_cat_ns_create(db, NULL, dir.fileid, "inner.txt",
				MDS_FTYPE_REG, 0644, 0, 0,
			       NULL, &file), MDS_OK);

	/* Cannot remove non-empty directory. */
	ASSERT_EQ(mds_ns_remove(db, MDS_FILEID_ROOT, "mydir"),
		  MDS_ERR_NOTEMPTY);

	/* Remove the inner file first, then the directory succeeds. */
	ASSERT_EQ(mds_ns_remove(db, dir.fileid, "inner.txt"), MDS_OK);
	ASSERT_EQ(mds_ns_remove(db, MDS_FILEID_ROOT, "mydir"), MDS_OK);

	/* Parent nlink should be back to 2. */
	ASSERT_EQ(mds_ns_getattr(db, MDS_FILEID_ROOT, &root), MDS_OK);
	ASSERT_EQ(root.nlink, (uint32_t)2);

	close_test_db(db, path);
}

/* -----------------------------------------------------------------------
 * test_rename_same_dir -- rename within a directory
 * ----------------------------------------------------------------------- */

static void test_rename_same_dir(void)
{
	struct mds_catalogue *db;
	struct mds_inode child, looked_up;
	char *path;

	db = open_test_db(&path);

	ASSERT_EQ(mds_cat_ns_create(db, NULL, MDS_FILEID_ROOT, "old_name",
				MDS_FTYPE_REG, 0644, 0, 0,
			       NULL, &child), MDS_OK);

	ASSERT_EQ(mds_ns_rename(db, MDS_FILEID_ROOT, "old_name",
				MDS_FILEID_ROOT, "new_name"), MDS_OK);

	/* Old name gone. */
	ASSERT_EQ(mds_ns_lookup(db, MDS_FILEID_ROOT, "old_name", &looked_up),
		  MDS_ERR_NOTFOUND);

	/* New name resolves to same fileid. */
	ASSERT_EQ(mds_ns_lookup(db, MDS_FILEID_ROOT, "new_name", &looked_up),
		  MDS_OK);
	ASSERT_EQ(looked_up.fileid, child.fileid);

	close_test_db(db, path);
}

/* -----------------------------------------------------------------------
 * test_rename_cross_dir -- rename directory across parents
 * ----------------------------------------------------------------------- */

static void test_rename_cross_dir(void)
{
	struct mds_catalogue *db;
	struct mds_inode dir_a, dir_b, sub, a_after, b_after;
	char *path;

	db = open_test_db(&path);

	/* Create two directories under root, and a subdir under dir_a. */
	ASSERT_EQ(mds_cat_ns_create(db, NULL, MDS_FILEID_ROOT, "dir_a",
				MDS_FTYPE_DIR, 0755, 0, 0,
			       NULL, &dir_a), MDS_OK);
	ASSERT_EQ(mds_cat_ns_create(db, NULL, MDS_FILEID_ROOT, "dir_b",
				MDS_FTYPE_DIR, 0755, 0, 0,
			       NULL, &dir_b), MDS_OK);
	ASSERT_EQ(mds_cat_ns_create(db, NULL, dir_a.fileid, "child",
				MDS_FTYPE_DIR, 0755, 0, 0,
			       NULL, &sub), MDS_OK);

	/* dir_a nlink = 2 + 1 subdir = 3. */
	ASSERT_EQ(mds_ns_getattr(db, dir_a.fileid, &a_after), MDS_OK);
	ASSERT_EQ(a_after.nlink, (uint32_t)3);

	/* Rename the sub-directory from dir_a to dir_b. */
	ASSERT_EQ(mds_ns_rename(db, dir_a.fileid, "child",
				dir_b.fileid, "child"), MDS_OK);

	/* dir_a nlink should drop to 2 (lost a subdirectory). */
	ASSERT_EQ(mds_ns_getattr(db, dir_a.fileid, &a_after), MDS_OK);
	ASSERT_EQ(a_after.nlink, (uint32_t)2);

	/* dir_b nlink should be 3 (gained a subdirectory). */
	ASSERT_EQ(mds_ns_getattr(db, dir_b.fileid, &b_after), MDS_OK);
	ASSERT_EQ(b_after.nlink, (uint32_t)3);

	close_test_db(db, path);
}

/* -----------------------------------------------------------------------
 * test_rename_replace -- rename over an existing file
 * ----------------------------------------------------------------------- */

static void test_rename_replace(void)
{
	struct mds_catalogue *db;
	struct mds_inode src, dst, looked_up;
	char *path;

	db = open_test_db(&path);

	ASSERT_EQ(mds_cat_ns_create(db, NULL, MDS_FILEID_ROOT, "src_file",
				MDS_FTYPE_REG, 0644, 0, 0,
			       NULL, &src), MDS_OK);
	ASSERT_EQ(mds_cat_ns_create(db, NULL, MDS_FILEID_ROOT, "dst_file",
				MDS_FTYPE_REG, 0644, 0, 0,
			       NULL, &dst), MDS_OK);

	ASSERT_EQ(mds_ns_rename(db, MDS_FILEID_ROOT, "src_file",
				MDS_FILEID_ROOT, "dst_file"), MDS_OK);

	/* "dst_file" now points to the source inode. */
	ASSERT_EQ(mds_ns_lookup(db, MDS_FILEID_ROOT, "dst_file", &looked_up),
		  MDS_OK);
	ASSERT_EQ(looked_up.fileid, src.fileid);

	/* Old dst inode should be gone (nlink was 1). */
	ASSERT_EQ(mds_ns_getattr(db, dst.fileid, &looked_up),
		  MDS_ERR_NOTFOUND);

	close_test_db(db, path);
}

/* -----------------------------------------------------------------------
 * test_setattr -- modify mode and uid, verify ctime bumped
 * ----------------------------------------------------------------------- */

static void test_setattr(void)
{
	struct mds_catalogue *db;
	struct mds_inode child, attrs, after;
	char *path;

	db = open_test_db(&path);

	ASSERT_EQ(mds_cat_ns_create(db, NULL, MDS_FILEID_ROOT, "chmod_me",
				MDS_FTYPE_REG, 0644, 1000, 1000,
			       NULL, &child),
		  MDS_OK);

	memset(&attrs, 0, sizeof(attrs));
	attrs.mode = 0600;
	attrs.uid = 0;

	ASSERT_EQ(mds_ns_setattr(db, child.fileid, &attrs,
				  MDS_ATTR_MODE | MDS_ATTR_UID), MDS_OK);

	ASSERT_EQ(mds_ns_getattr(db, child.fileid, &after), MDS_OK);
	ASSERT_EQ(after.mode, (uint32_t)0600);
	ASSERT_EQ(after.uid, (uint64_t)0);
	ASSERT_EQ(after.gid, (uint64_t)1000); /* untouched */
	ASSERT_TRUE(after.change > child.change);

	close_test_db(db, path);
}

/* -----------------------------------------------------------------------
 * test_link -- hard link, nlink increment, remove one link
 * ----------------------------------------------------------------------- */

static void test_link(void)
{
	struct mds_catalogue *db;
	struct mds_inode file, via_link, after_remove;
	char *path;

	db = open_test_db(&path);

	ASSERT_EQ(mds_cat_ns_create(db, NULL, MDS_FILEID_ROOT, "original",
				MDS_FTYPE_REG, 0644, 0, 0,
			       NULL, &file), MDS_OK);
	ASSERT_EQ(file.nlink, (uint32_t)1);

	/* Create a hard link. */
	ASSERT_EQ(mds_ns_link(db, MDS_FILEID_ROOT, "link1", file.fileid),
		  MDS_OK);

	/* nlink should now be 2. */
	ASSERT_EQ(mds_ns_getattr(db, file.fileid, &via_link), MDS_OK);
	ASSERT_EQ(via_link.nlink, (uint32_t)2);

	/* Remove the original name -- inode survives (nlink=1). */
	ASSERT_EQ(mds_ns_remove(db, MDS_FILEID_ROOT, "original"), MDS_OK);
	ASSERT_EQ(mds_ns_getattr(db, file.fileid, &after_remove), MDS_OK);
	ASSERT_EQ(after_remove.nlink, (uint32_t)1);

	/* Duplicate link name returns EXISTS. */
	ASSERT_EQ(mds_ns_link(db, MDS_FILEID_ROOT, "link1", file.fileid),
		  MDS_ERR_EXISTS);

	close_test_db(db, path);
}

/* -----------------------------------------------------------------------
 * test_link_dir_rejected -- hard links to directories are not allowed
 * ----------------------------------------------------------------------- */

static void test_link_dir_rejected(void)
{
	struct mds_catalogue *db;
	struct mds_inode dir;
	char *path;

	db = open_test_db(&path);

	ASSERT_EQ(mds_cat_ns_create(db, NULL, MDS_FILEID_ROOT, "somedir",
				MDS_FTYPE_DIR, 0755, 0, 0,
			       NULL, &dir), MDS_OK);

	ASSERT_EQ(mds_ns_link(db, MDS_FILEID_ROOT, "dirlink", dir.fileid),
		  MDS_ERR_ISDIR);

	close_test_db(db, path);
}

/* -----------------------------------------------------------------------
 * test_readdir_basic -- list entries in a directory
 * ----------------------------------------------------------------------- */

struct rd_ctx {
	struct mds_ns_dirent entries[32];
	int count;
};

static int rd_cb(const struct mds_ns_dirent *entry, void *arg)
{
	struct rd_ctx *ctx = arg;

	if (ctx->count >= 32)
		return -1;
	ctx->entries[ctx->count++] = *entry;
	return 0;
}

static void test_readdir_basic(void)
{
	struct mds_catalogue *db;
	struct mds_inode f1, f2, f3;
	struct rd_ctx ctx;
	char *path;

	db = open_test_db(&path);

	ASSERT_EQ(mds_cat_ns_create(db, NULL, MDS_FILEID_ROOT, "charlie",
				MDS_FTYPE_REG, 0644, 0, 0,
			       NULL, &f1), MDS_OK);
	ASSERT_EQ(mds_cat_ns_create(db, NULL, MDS_FILEID_ROOT, "alpha",
				MDS_FTYPE_REG, 0644, 0, 0,
			       NULL, &f2), MDS_OK);
	ASSERT_EQ(mds_cat_ns_create(db, NULL, MDS_FILEID_ROOT, "bravo",
				MDS_FTYPE_DIR, 0755, 0, 0,
			       NULL, &f3), MDS_OK);

	memset(&ctx, 0, sizeof(ctx));
	ASSERT_EQ(mds_ns_readdir(db, MDS_FILEID_ROOT, NULL, rd_cb, &ctx),
		  MDS_OK);

	/* catalogue keys are sorted -- expect alpha, bravo, charlie. */
	ASSERT_EQ(ctx.count, 3);
	ASSERT_EQ(strcmp(ctx.entries[0].name, "alpha"), 0);
	ASSERT_EQ(strcmp(ctx.entries[1].name, "bravo"), 0);
	ASSERT_EQ(strcmp(ctx.entries[2].name, "charlie"), 0);

	close_test_db(db, path);
}

/* -----------------------------------------------------------------------
 * test_readdir_pagination -- start_after skips earlier entries
 * ----------------------------------------------------------------------- */

static void test_readdir_pagination(void)
{
	struct mds_catalogue *db;
	struct mds_inode tmp;
	struct rd_ctx ctx;
	char *path;

	db = open_test_db(&path);

	ASSERT_EQ(mds_cat_ns_create(db, NULL, MDS_FILEID_ROOT, "aaa",
				MDS_FTYPE_REG, 0644, 0, 0,
			       NULL, &tmp), MDS_OK);
	ASSERT_EQ(mds_cat_ns_create(db, NULL, MDS_FILEID_ROOT, "bbb",
				MDS_FTYPE_REG, 0644, 0, 0,
			       NULL, &tmp), MDS_OK);
	ASSERT_EQ(mds_cat_ns_create(db, NULL, MDS_FILEID_ROOT, "ccc",
				MDS_FTYPE_REG, 0644, 0, 0,
			       NULL, &tmp), MDS_OK);
	ASSERT_EQ(mds_cat_ns_create(db, NULL, MDS_FILEID_ROOT, "ddd",
				MDS_FTYPE_REG, 0644, 0, 0,
			       NULL, &tmp), MDS_OK);

	/* Start after "bbb" -- should get "ccc" and "ddd". */
	memset(&ctx, 0, sizeof(ctx));
	ASSERT_EQ(mds_ns_readdir(db, MDS_FILEID_ROOT, "bbb", rd_cb, &ctx),
		  MDS_OK);
	ASSERT_EQ(ctx.count, 2);
	ASSERT_EQ(strcmp(ctx.entries[0].name, "ccc"), 0);
	ASSERT_EQ(strcmp(ctx.entries[1].name, "ddd"), 0);

	close_test_db(db, path);
}

/* -----------------------------------------------------------------------
 * test_readdir_pagination_difflen -- different-length names
 * ----------------------------------------------------------------------- */

static void test_readdir_pagination_difflen(void)
{
	struct mds_catalogue *db;
	struct mds_inode tmp;
	struct rd_ctx ctx;
	char *path;

	db = open_test_db(&path);

	/* Names of varying length: "a", "aa", "b", "ba", "c". */
	ASSERT_EQ(mds_cat_ns_create(db, NULL, MDS_FILEID_ROOT, "a",
				MDS_FTYPE_REG, 0644, 0, 0,
			       NULL, &tmp), MDS_OK);
	ASSERT_EQ(mds_cat_ns_create(db, NULL, MDS_FILEID_ROOT, "aa",
				MDS_FTYPE_REG, 0644, 0, 0,
			       NULL, &tmp), MDS_OK);
	ASSERT_EQ(mds_cat_ns_create(db, NULL, MDS_FILEID_ROOT, "b",
				MDS_FTYPE_REG, 0644, 0, 0,
			       NULL, &tmp), MDS_OK);
	ASSERT_EQ(mds_cat_ns_create(db, NULL, MDS_FILEID_ROOT, "ba",
				MDS_FTYPE_REG, 0644, 0, 0,
			       NULL, &tmp), MDS_OK);
	ASSERT_EQ(mds_cat_ns_create(db, NULL, MDS_FILEID_ROOT, "c",
				MDS_FTYPE_REG, 0644, 0, 0,
			       NULL, &tmp), MDS_OK);

	/* Start after "b" -- should get "ba" and "c" only. */
	memset(&ctx, 0, sizeof(ctx));
	ASSERT_EQ(mds_ns_readdir(db, MDS_FILEID_ROOT, "b", rd_cb, &ctx),
		  MDS_OK);
	ASSERT_EQ(ctx.count, 2);
	ASSERT_EQ(strcmp(ctx.entries[0].name, "ba"), 0);
	ASSERT_EQ(strcmp(ctx.entries[1].name, "c"), 0);

	close_test_db(db, path);
}

/* -----------------------------------------------------------------------
 * test_readdir_notdir -- READDIR on a regular file returns NOTDIR
 * ----------------------------------------------------------------------- */

static void test_readdir_notdir(void)
{
	struct mds_catalogue *db;
	struct mds_inode file;
	struct rd_ctx ctx;
	char *path;

	db = open_test_db(&path);

	ASSERT_EQ(mds_cat_ns_create(db, NULL, MDS_FILEID_ROOT, "regfile",
				MDS_FTYPE_REG, 0644, 0, 0,
			       NULL, &file), MDS_OK);

	memset(&ctx, 0, sizeof(ctx));
	ASSERT_EQ(mds_ns_readdir(db, file.fileid, NULL, rd_cb, &ctx),
		  MDS_ERR_NOTDIR);

	close_test_db(db, path);
}

/* -----------------------------------------------------------------------
 * test_concurrent_mkdir -- parallel directory creates under one parent
 * ----------------------------------------------------------------------- */

#define CONC_MKDIR_THREADS 32

struct conc_mkdir_shared_arg {
	struct mds_catalogue *db;
	uint64_t              parent_fid;
	uint32_t              idx;
	enum mds_status       result;
};

static void *conc_mkdir_shared_worker(void *vp)
{
	struct conc_mkdir_shared_arg *a = vp;
	struct mds_inode out;

	a->result = mds_cat_ns_create(
		a->db, NULL, a->parent_fid, "shared",
		MDS_FTYPE_DIR, 0755, 0, 0, NULL, &out);
	return NULL;
}

struct conc_mkdir_unique_arg {
	struct mds_catalogue *db;
	uint64_t              parent_fid;
	uint32_t              idx;
	enum mds_status       result;
};

static void *conc_mkdir_unique_worker(void *vp)
{
	struct conc_mkdir_unique_arg *a = vp;
	struct mds_inode out;
	char name[32];

	(void)snprintf(name, sizeof(name), "rank%u", a->idx);
	a->result = mds_cat_ns_create(
		a->db, NULL, a->parent_fid, name,
		MDS_FTYPE_DIR, 0755, 0, 0, NULL, &out);
	return NULL;
}

static void test_concurrent_mkdir(void)
{
	struct mds_catalogue *db;
	struct mds_inode parent;
	char *path;
	pthread_t tid[CONC_MKDIR_THREADS];
	struct conc_mkdir_unique_arg uargs[CONC_MKDIR_THREADS];
	struct conc_mkdir_shared_arg sargs[CONC_MKDIR_THREADS];
	uint32_t ok_count = 0;
	uint32_t exist_count = 0;
	uint32_t i;

	db = open_test_db(&path);

	ASSERT_EQ(mds_cat_ns_create(db, NULL, MDS_FILEID_ROOT, "parent",
				    MDS_FTYPE_DIR, 0755, 0, 0,
				    NULL, &parent), MDS_OK);

	/* Unique names: every thread succeeds. */
	for (i = 0; i < CONC_MKDIR_THREADS; i++) {
		uargs[i].db = db;
		uargs[i].parent_fid = parent.fileid;
		uargs[i].idx = i;
		uargs[i].result = MDS_ERR_IO;
		ASSERT_EQ(pthread_create(&tid[i], NULL,
					 conc_mkdir_unique_worker,
					 &uargs[i]), 0);
	}
	for (i = 0; i < CONC_MKDIR_THREADS; i++) {
		pthread_join(tid[i], NULL);
		ASSERT_EQ(uargs[i].result, MDS_OK);
	}

	/* Shared name: exactly one winner, rest EXISTS. */
	for (i = 0; i < CONC_MKDIR_THREADS; i++) {
		sargs[i].db = db;
		sargs[i].parent_fid = parent.fileid;
		sargs[i].idx = i;
		sargs[i].result = MDS_ERR_IO;
		ASSERT_EQ(pthread_create(&tid[i], NULL,
					 conc_mkdir_shared_worker,
					 &sargs[i]), 0);
	}
	for (i = 0; i < CONC_MKDIR_THREADS; i++) {
		pthread_join(tid[i], NULL);
		if (sargs[i].result == MDS_OK) {
			ok_count++;
		} else if (sargs[i].result == MDS_ERR_EXISTS) {
			exist_count++;
		}
	}
	ASSERT_EQ(ok_count, (uint32_t)1);
	ASSERT_EQ(exist_count, (uint32_t)(CONC_MKDIR_THREADS - 1));

	close_test_db(db, path);
}

/* -----------------------------------------------------------------------
 * main
 * ----------------------------------------------------------------------- */

int main(void)
{
	fprintf(stdout, "Running namespace tests:\n");

	RUN_TEST(test_lookup_root);
	RUN_TEST(test_create_file);
	RUN_TEST(test_create_directory);
	RUN_TEST(test_remove_file);
	RUN_TEST(test_remove_dir_empty_check);
	RUN_TEST(test_rename_same_dir);
	RUN_TEST(test_rename_cross_dir);
	RUN_TEST(test_rename_replace);
	RUN_TEST(test_setattr);
	RUN_TEST(test_link);
	RUN_TEST(test_link_dir_rejected);
	RUN_TEST(test_readdir_basic);
	RUN_TEST(test_readdir_pagination);
	RUN_TEST(test_readdir_pagination_difflen);
	RUN_TEST(test_readdir_notdir);
	RUN_TEST(test_concurrent_mkdir);

	fprintf(stdout, "\n%d/%d tests passed.\n", tests_passed, tests_run);
	return (tests_passed == tests_run) ? 0 : 1;
}
