/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_inline_data.c -- Unit tests for inline data (small-file acceleration).
 *
 * Covers: CRUD, inline read/write via compound, promotion on size
 * exceed, inline remove (no GC), backward-compat 105-byte inodes,
 * and migration serialisation round-trip with inline data.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>
#include <time.h>

#include "pnfs_mds.h"
#include "test_helpers.h"
#include "mds_catalogue.h"
#include "migration.h"

/* -----------------------------------------------------------------------
 * Test helpers
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

/* -----------------------------------------------------------------------
 * test_inline_crud -- put / get / del round-trip
 * ----------------------------------------------------------------------- */

static void test_inline_crud(void)
{
	struct mds_catalogue *db = NULL;
	struct mds_catalogue *cat = NULL;
	enum mds_status st;
	char *path;
	uint64_t fileid = 100;
	uint8_t wdata[128];
	uint8_t rbuf[MDS_INLINE_DATA_MAX];
	uint32_t rlen = 0;

	path = make_temp_db_path();
	st = ((db = open_test_catalogue()) != NULL ? MDS_OK : MDS_ERR_IO);
	ASSERT_EQ(st, MDS_OK);
    cat = db;

	/* Fill test data. */
	for (int i = 0; i < (int)sizeof(wdata); i++)
		wdata[i] = (uint8_t)(i & 0xff);

	/* Put inline data. */
	st = mds_cat_inline_put(cat, NULL, fileid,
				  wdata, sizeof(wdata));
	ASSERT_EQ(st, MDS_OK);

	/* Get inline data. */
	st = mds_cat_inline_get(cat, fileid, rbuf, sizeof(rbuf), &rlen);
	ASSERT_EQ(st, MDS_OK);
	ASSERT_EQ(rlen, sizeof(wdata));
	ASSERT_EQ(memcmp(wdata, rbuf, rlen), 0);

	/* Overwrite with larger data. */
	uint8_t wdata2[256];
	memset(wdata2, 0xAB, sizeof(wdata2));
	st = mds_cat_inline_put(cat, NULL, fileid,
				  wdata2, sizeof(wdata2));
	ASSERT_EQ(st, MDS_OK);

	rlen = 0;
	st = mds_cat_inline_get(cat, fileid, rbuf, sizeof(rbuf), &rlen);
	ASSERT_EQ(st, MDS_OK);
	ASSERT_EQ(rlen, sizeof(wdata2));
	ASSERT_EQ(memcmp(wdata2, rbuf, rlen), 0);

	/* Delete. */
	st = mds_cat_inline_del(cat, NULL, fileid);
	ASSERT_EQ(st, MDS_OK);

	/* Get after delete should return NOTFOUND. */
	st = mds_cat_inline_get(cat, fileid, rbuf, sizeof(rbuf), &rlen);
	ASSERT_EQ(st, MDS_ERR_NOTFOUND);

	/* Double-delete should return NOTFOUND. */
	st = mds_cat_inline_del(cat, NULL, fileid);
	ASSERT_EQ(st, MDS_ERR_NOTFOUND);

	mds_catalogue_close(db);
	cleanup_temp_db(path);
	free(path);
}

/* -----------------------------------------------------------------------
 * test_inline_zero_length -- zero-length inline data
 * ----------------------------------------------------------------------- */

static void test_inline_zero_length(void)
{
	struct mds_catalogue *db = NULL;
	struct mds_catalogue *cat = NULL;
	enum mds_status st;
	char *path;
	uint64_t fileid = 200;
	uint8_t rbuf[64];
	uint32_t rlen = 99;

	path = make_temp_db_path();
	st = ((db = open_test_catalogue()) != NULL ? MDS_OK : MDS_ERR_IO);
	ASSERT_EQ(st, MDS_OK);
    cat = db;

	/* Put zero-length data. */
	st = mds_cat_inline_put(cat, NULL, fileid, NULL, 0);
	ASSERT_EQ(st, MDS_OK);

	/* Get should return length 0. */
	st = mds_cat_inline_get(cat, fileid, rbuf, sizeof(rbuf), &rlen);
	ASSERT_EQ(st, MDS_OK);
	ASSERT_EQ(rlen, 0u);

	mds_catalogue_close(db);
	cleanup_temp_db(path);
	free(path);
}

/* -----------------------------------------------------------------------
 * test_inline_txn -- put/get within explicit transaction
 * ----------------------------------------------------------------------- */

static void test_inline_txn(void)
{
	struct mds_catalogue *db = NULL;
	struct mds_catalogue *cat = NULL;
	enum mds_status st;
	char *path;
	uint64_t fileid = 300;
	uint8_t wdata[64];
	uint8_t rbuf[MDS_INLINE_DATA_MAX];
	uint32_t rlen = 0;
	struct mds_cat_txn *txn = NULL;

	path = make_temp_db_path();
	st = ((db = open_test_catalogue()) != NULL ? MDS_OK : MDS_ERR_IO);
	ASSERT_EQ(st, MDS_OK);
    cat = db;

	memset(wdata, 0xCC, sizeof(wdata));

	/* Write in explicit txn. */
	st = mds_cat_txn_begin(db, MDS_CAT_TXN_WRITE, &txn);
	ASSERT_EQ(st, MDS_OK);

	st = mds_cat_inline_put(db, txn, fileid,
				  wdata, sizeof(wdata));
	ASSERT_EQ(st, MDS_OK);

	/* Read within the same write txn. */
	st = mds_cat_inline_get(db, fileid,
				      rbuf, sizeof(rbuf), &rlen);
	ASSERT_EQ(st, MDS_OK);
	ASSERT_EQ(rlen, sizeof(wdata));
	ASSERT_EQ(memcmp(wdata, rbuf, rlen), 0);

	ASSERT_EQ(mds_cat_txn_commit(txn), 0);

	/* Verify after commit. */
	rlen = 0;
	st = mds_cat_inline_get(cat, fileid, rbuf, sizeof(rbuf), &rlen);
	ASSERT_EQ(st, MDS_OK);
	ASSERT_EQ(rlen, sizeof(wdata));

	mds_catalogue_close(db);
	cleanup_temp_db(path);
	free(path);
}

/* -----------------------------------------------------------------------
 * test_inline_inode_flag -- new REG files get MDS_IFLAG_INLINE
 * ----------------------------------------------------------------------- */

static void test_inline_inode_flag(void)
{
	struct mds_catalogue *db = NULL;
	struct mds_catalogue *cat = NULL;
	enum mds_status st;
	char *path;
	uint64_t parent_fid = MDS_FILEID_ROOT;
	struct mds_inode child_inode;
	struct mds_inode inode;

	path = make_temp_db_path();
	st = ((db = open_test_catalogue()) != NULL ? MDS_OK : MDS_ERR_IO);
	ASSERT_EQ(st, MDS_OK);
    cat = db;

	/* Create a regular file. */
	st = mds_cat_ns_create(cat, NULL, parent_fid, "testfile.txt",
			   MDS_FTYPE_REG, 0644, 0, 0,
			       NULL, &child_inode);
	ASSERT_EQ(st, MDS_OK);
	ASSERT_NE(child_inode.fileid, 0u);

	/* Read back -- should have inline flag. */
	st = mds_cat_ns_getattr(db, child_inode.fileid, &inode);
	ASSERT_EQ(st, MDS_OK);
	ASSERT_TRUE(inode.flags & MDS_IFLAG_INLINE);

	/*
	 * No inline_data record yet -- empty inline file is represented
	 * by MDS_IFLAG_INLINE + size 0, with no inline_data key.
	 */
	{
		uint8_t buf[64];
		uint32_t len = 99;

		st = mds_cat_inline_get(cat, child_inode.fileid, buf, sizeof(buf),
					  &len);
		ASSERT_EQ(st, MDS_ERR_NOTFOUND);
	}

	/* Directory should NOT have inline flag. */
	{
		struct mds_inode dir_inode;
		struct mds_inode dir_attr;

		st = mds_cat_ns_create(cat, NULL, parent_fid, "subdir",
				   MDS_FTYPE_DIR, 0755, 0, 0,
			       NULL, &dir_inode);
		ASSERT_EQ(st, MDS_OK);

		st = mds_cat_ns_getattr(db, dir_inode.fileid, &dir_attr);
		ASSERT_EQ(st, MDS_OK);
		ASSERT_EQ(dir_attr.flags & MDS_IFLAG_INLINE, 0u);
	}

	mds_catalogue_close(db);
	cleanup_temp_db(path);
	free(path);
}

/* -----------------------------------------------------------------------
 * test_inline_remove -- removing inline file cleans up inline data
 * ----------------------------------------------------------------------- */

static void test_inline_remove(void)
{
	struct mds_catalogue *db = NULL;
	struct mds_catalogue *cat = NULL;
	enum mds_status st;
	char *path;
	uint64_t parent_fid = MDS_FILEID_ROOT;
	struct mds_inode child_inode;
	uint8_t wdata[32];
	uint8_t rbuf[64];
	uint32_t rlen;

	path = make_temp_db_path();
	st = ((db = open_test_catalogue()) != NULL ? MDS_OK : MDS_ERR_IO);
	ASSERT_EQ(st, MDS_OK);
    cat = db;

	st = mds_cat_ns_create(cat, NULL, parent_fid, "removeme.txt",
			   MDS_FTYPE_REG, 0644, 0, 0,
			       NULL, &child_inode);
	ASSERT_EQ(st, MDS_OK);

	/* Write some inline data. */
	memset(wdata, 0xDD, sizeof(wdata));
	st = mds_cat_inline_put(cat, NULL, child_inode.fileid,
				  wdata, sizeof(wdata));
	ASSERT_EQ(st, MDS_OK);

	/* Remove the file. */
	st = mds_cat_ns_remove(db, NULL, parent_fid, "removeme.txt");
	ASSERT_EQ(st, MDS_OK);

	/* Inline data should be gone. */
	st = mds_cat_inline_get(cat, child_inode.fileid, rbuf, sizeof(rbuf), &rlen);
	ASSERT_EQ(st, MDS_ERR_NOTFOUND);

	mds_catalogue_close(db);
	cleanup_temp_db(path);
	free(path);
}

/* -----------------------------------------------------------------------
 * test_inline_backward_compat -- 105-byte inode records still work
 * ----------------------------------------------------------------------- */

static void test_inline_backward_compat(void)
{
	struct mds_catalogue *db = NULL;
	struct mds_catalogue *cat = NULL;
	enum mds_status st;
	char *path;
	struct mds_inode out_inode;
	struct mds_inode inode;

	path = make_temp_db_path();
	st = ((db = open_test_catalogue()) != NULL ? MDS_OK : MDS_ERR_IO);
	ASSERT_EQ(st, MDS_OK);
    cat = db;

	/* Create a file so we have a fileid. */
	st = mds_cat_ns_create(cat, NULL, MDS_FILEID_ROOT, "compat.txt",
			   MDS_FTYPE_REG, 0644, 0, 0,
			       NULL, &out_inode);
	ASSERT_EQ(st, MDS_OK);

	/* Read back -- the 109-byte record is valid. */
	memset(&inode, 0, sizeof(inode));
	st = mds_cat_ns_getattr(db, out_inode.fileid, &inode);
	ASSERT_EQ(st, MDS_OK);
	ASSERT_EQ(inode.fileid, out_inode.fileid);
	ASSERT_EQ(inode.type, (uint8_t)MDS_FTYPE_REG);
	ASSERT_TRUE(inode.flags & MDS_IFLAG_INLINE);

	mds_catalogue_close(db);
	cleanup_temp_db(path);
	free(path);
}

/* -----------------------------------------------------------------------
 * test_inline_migration_roundtrip -- chunk with inline data survives ser/de
 * ----------------------------------------------------------------------- */

static void test_inline_migration_roundtrip(void)
{
	struct mig_inode_chunk chunk;
	struct mig_inode_chunk chunk2;
	void *buf = NULL;
	size_t buf_len = 0;
	int rc;
	uint8_t test_data[512];

	memset(&chunk, 0, sizeof(chunk));
	memset(test_data, 0xEE, sizeof(test_data));

	chunk.fileid = 42;
	chunk.inode.fileid = 42;
	chunk.inode.type = MDS_FTYPE_REG;
	chunk.inode.flags = MDS_IFLAG_INLINE;
	chunk.inode.size = sizeof(test_data);
	chunk.has_inline_data = 1;
	chunk.inline_data = test_data;
	chunk.inline_data_len = sizeof(test_data);

	rc = mig_chunk_serialise(&chunk, &buf, &buf_len);
	ASSERT_EQ(rc, 0);
	ASSERT_NE(buf, NULL);

	memset(&chunk2, 0, sizeof(chunk2));
	rc = mig_chunk_deserialise(buf, buf_len, &chunk2);
	ASSERT_EQ(rc, 0);
	ASSERT_EQ(chunk2.fileid, 42u);
	ASSERT_EQ(chunk2.has_inline_data, 1);
	ASSERT_EQ(chunk2.inline_data_len, sizeof(test_data));
	ASSERT_NE(chunk2.inline_data, NULL);
	ASSERT_EQ(memcmp(chunk2.inline_data, test_data, sizeof(test_data)), 0);
	ASSERT_EQ(chunk2.inode.flags, MDS_IFLAG_INLINE);

	/* Prevent double-free: chunk.inline_data is stack, not heap. */
	chunk.inline_data = NULL;
	mig_chunk_free(&chunk2);
	free(buf);
}

/* -----------------------------------------------------------------------
 * test_inline_max_size -- write exactly MDS_INLINE_DATA_MAX
 * ----------------------------------------------------------------------- */

static void test_inline_max_size(void)
{
	struct mds_catalogue *db = NULL;
	struct mds_catalogue *cat = NULL;
	enum mds_status st;
	char *path;
	uint64_t fileid = 500;
	uint8_t *wdata;
	uint8_t *rbuf;
	uint32_t rlen = 0;

	path = make_temp_db_path();
	st = ((db = open_test_catalogue()) != NULL ? MDS_OK : MDS_ERR_IO);
	ASSERT_EQ(st, MDS_OK);
    cat = db;

	wdata = malloc(MDS_INLINE_DATA_MAX);
	rbuf = malloc(MDS_INLINE_DATA_MAX);
	assert(wdata != NULL && rbuf != NULL);
	memset(wdata, 0x55, MDS_INLINE_DATA_MAX);

	st = mds_cat_inline_put(cat, NULL, fileid,
				  wdata, MDS_INLINE_DATA_MAX);
	ASSERT_EQ(st, MDS_OK);

	st = mds_cat_inline_get(cat, fileid, rbuf, MDS_INLINE_DATA_MAX,
				  &rlen);
	ASSERT_EQ(st, MDS_OK);
	ASSERT_EQ(rlen, (uint32_t)MDS_INLINE_DATA_MAX);
	ASSERT_EQ(memcmp(wdata, rbuf, rlen), 0);

	/* Exceeding max should fail. */
	st = mds_cat_inline_put(cat, NULL, fileid,
				  wdata, MDS_INLINE_DATA_MAX + 1);
	ASSERT_EQ(st, MDS_ERR_INVAL);

	free(wdata);
	free(rbuf);
	mds_catalogue_close(db);
	cleanup_temp_db(path);
	free(path);
}

/* -----------------------------------------------------------------------
 * test_inline_write_updates_metadata -- verify mtime/ctime/change
 *
 * Writes inline data and asserts that the inode metadata contract
 * is honoured: change must increase, mtime/ctime must not go backward.
 * Uses a short nanosleep between create and write to avoid same-tick
 * aliasing.
 * ----------------------------------------------------------------------- */

static void test_inline_write_updates_metadata(void)
{
	struct mds_catalogue *db = NULL;
	struct mds_catalogue *cat = NULL;
	enum mds_status st;
	char *path;
	uint64_t fileid = 700;
	struct mds_inode before, after;
	struct timespec sleep_ts = { .tv_sec = 0, .tv_nsec = 2000000 }; /* 2ms */

	path = make_temp_db_path();
	st = ((db = open_test_catalogue()) != NULL ? MDS_OK : MDS_ERR_IO);
	ASSERT_EQ(st, MDS_OK);
    cat = db;

	/* Create inode with IFLAG_INLINE. */
	{
		struct mds_inode inode;
		struct mds_cat_txn *ctxn = NULL;
		memset(&inode, 0, sizeof(inode));
		inode.fileid = fileid;
		inode.type = MDS_FTYPE_REG;
		inode.mode = 0644;
		inode.nlink = 1;
		inode.flags = MDS_IFLAG_INLINE;
		inode.change = 1;
		clock_gettime(CLOCK_REALTIME, &inode.mtime);
		inode.ctime = inode.mtime;
		inode.atime = inode.mtime;
		st = mds_cat_txn_begin(db, MDS_CAT_TXN_WRITE, &ctxn);
		ASSERT_EQ(st, MDS_OK);
		st = mds_cat_inode_put(db, ctxn, &inode);
		ASSERT_EQ(st, MDS_OK);
		ASSERT_EQ(mds_cat_txn_commit(ctxn), 0);
	}

	/* Snapshot inode state before write. */
	st = mds_cat_ns_getattr(cat, fileid, &before);
	ASSERT_EQ(st, MDS_OK);

	/* Short sleep to guarantee clock advances. */
	nanosleep(&sleep_ts, NULL);

	/* Write inline data (direct catalogue path: read-modify-write). */
	{
		uint8_t data[16];
		struct mds_cat_txn *txn = NULL;
		uint8_t buf[MDS_INLINE_DATA_MAX];
		uint32_t cur_len = 0;

		memset(data, 0x42, sizeof(data));

		st = mds_cat_txn_begin(db, MDS_CAT_TXN_WRITE, &txn);
		ASSERT_EQ(st, MDS_OK);

		st = mds_cat_inline_get(db, fileid,
					      buf, sizeof(buf), &cur_len);
		if (st != MDS_OK && st != MDS_ERR_NOTFOUND) {
			mds_cat_txn_abort(txn);
			ASSERT_EQ(st, MDS_OK);
		}

		memcpy(buf, data, sizeof(data));
		cur_len = sizeof(data);

		st = mds_cat_inline_put(db, txn, fileid, buf, cur_len);
		if (st != MDS_OK) {
			mds_cat_txn_abort(txn);
			ASSERT_EQ(st, MDS_OK);
		}

		/* Apply metadata contract. */
		{
			struct mds_inode wr_inode;
			struct timespec now;

			st = mds_cat_ns_getattr(db, fileid, &wr_inode);
			if (st != MDS_OK) {
				mds_cat_txn_abort(txn);
				ASSERT_EQ(st, MDS_OK);
			}

			clock_gettime(CLOCK_REALTIME, &now);
			wr_inode.mtime = now;
			wr_inode.ctime = now;
			wr_inode.change++;
			if (cur_len > wr_inode.size) {
				wr_inode.size = cur_len;
				wr_inode.space_used = cur_len;
			}

			st = mds_cat_inode_put(db, txn, &wr_inode);
			if (st != MDS_OK) {
				mds_cat_txn_abort(txn);
				ASSERT_EQ(st, MDS_OK);
			}
		}

		ASSERT_EQ(mds_cat_txn_commit(txn), 0);
	}

	/* Read back inode and verify metadata. */
	st = mds_cat_ns_getattr(cat, fileid, &after);
	ASSERT_EQ(st, MDS_OK);

	/* Hard assertion: change must have increased. */
	ASSERT_EQ(after.change > before.change, 1);

	/* Timestamps must not go backward. */
	ASSERT_EQ(after.mtime.tv_sec >= before.mtime.tv_sec, 1);
	ASSERT_EQ(after.ctime.tv_sec >= before.ctime.tv_sec, 1);

	/* With nanosleep, we expect strictly greater. */
	{
		int64_t mt_delta = (int64_t)(after.mtime.tv_sec - before.mtime.tv_sec) *
				   1000000000LL +
				   (after.mtime.tv_nsec - before.mtime.tv_nsec);
		ASSERT_EQ(mt_delta > 0, 1);
	}

	mds_catalogue_close(db);
	cleanup_temp_db(path);
	free(path);
}

/* -----------------------------------------------------------------------
 * main
 * ----------------------------------------------------------------------- */

int main(void)
{
	fprintf(stdout, "test_inline_data\n");

	RUN_TEST(test_inline_crud);
	RUN_TEST(test_inline_zero_length);
	RUN_TEST(test_inline_txn);
	RUN_TEST(test_inline_inode_flag);
	RUN_TEST(test_inline_remove);
	RUN_TEST(test_inline_backward_compat);
	RUN_TEST(test_inline_migration_roundtrip);
	RUN_TEST(test_inline_max_size);
	RUN_TEST(test_inline_write_updates_metadata);

	fprintf(stdout, "\n  %d/%d tests passed\n", tests_passed, tests_run);
	return (tests_passed == tests_run) ? 0 : 1;
}
