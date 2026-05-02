/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_open_state.c — Unit tests for OPEN/CLOSE state management.
 *
 * Part 1: Direct open_state.h API tests (stateid alloc, share conflict,
 *          close, double-close, stateid lookup).
 * Part 2: Compound integration tests (OPEN create, OPEN existing,
 *          OPEN+GETATTR+CLOSE flow, share conflict via compound,
 *          OPEN+CLOSE+reopen).
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
#include "test_helpers.h"
#include "compound.h"
#include "session.h"
#include "open_state.h"

/* -----------------------------------------------------------------------
 * Test helpers
 * ----------------------------------------------------------------------- */

#define TEST_MAP_SIZE (16ULL * 1024 * 1024)
#define TEST_MDS_ID   0

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
	fprintf(stdout, "  %-44s", #fn);				\
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

/* Zero stateid for comparison. */
static const uint8_t zero_other[NFS4_OTHER_SIZE] = {0};

/* catalogue temp DB helpers (reused from test_compound.c). */
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
		const char *slash = strrchr(path, '/');

		if (slash != NULL) {
			size_t plen = (size_t)(slash - path);

			memcpy(dir, path, plen);
			dir[plen] = '\0';
			rmdir(dir);
		}
	}
}

/* Op builder helpers. */
static struct nfs4_op mk_sequence(void)
{
	struct nfs4_op op;

	memset(&op, 0, sizeof(op));
	op.opnum = OP_SEQUENCE;
	return op;
}

static struct nfs4_op mk_putrootfh(void)
{
	struct nfs4_op op;

	memset(&op, 0, sizeof(op));
	op.opnum = OP_PUTROOTFH;
	return op;
}

static struct nfs4_op mk_putfh(uint64_t fileid)
{
	struct nfs4_op op;

	memset(&op, 0, sizeof(op));
	op.opnum = OP_PUTFH;
	op.arg.putfh.fh.fileid = fileid;
	return op;
}

static struct nfs4_op mk_getattr(void)
{
	struct nfs4_op op;

	memset(&op, 0, sizeof(op));
	op.opnum = OP_GETATTR;
	return op;
}

static struct nfs4_op mk_create(const char *name, enum mds_file_type type,
				uint32_t mode)
{
	struct nfs4_op op;

	memset(&op, 0, sizeof(op));
	op.opnum = OP_CREATE;
	snprintf(op.arg.create.name, sizeof(op.arg.create.name), "%s", name);
	op.arg.create.type = type;
	op.arg.create.mode = mode;
	return op;
}

static struct nfs4_op mk_open_create(const char *name, uint32_t mode,
				     uint32_t share_access,
				     uint32_t share_deny)
{
	struct nfs4_op op;

	memset(&op, 0, sizeof(op));
	op.opnum = OP_OPEN;
	op.arg.open.claim = CLAIM_NULL;
	snprintf(op.arg.open.name, sizeof(op.arg.open.name), "%s", name);
	op.arg.open.share_access = share_access;
	op.arg.open.share_deny = share_deny;
	op.arg.open.create = true;
	op.arg.open.createmode = CREATEMODE_UNCHECKED4;
	op.arg.open.mode = mode;
	return op;
}

static struct nfs4_op mk_open_existing(const char *name,
				       uint32_t share_access,
				       uint32_t share_deny)
{
	struct nfs4_op op;

	memset(&op, 0, sizeof(op));
	op.opnum = OP_OPEN;
	op.arg.open.claim = CLAIM_NULL;
	snprintf(op.arg.open.name, sizeof(op.arg.open.name), "%s", name);
	op.arg.open.share_access = share_access;
	op.arg.open.share_deny = share_deny;
	op.arg.open.create = false;
	return op;
}

static struct nfs4_op mk_open_fh(uint32_t share_access, uint32_t share_deny)
{
	struct nfs4_op op;

	memset(&op, 0, sizeof(op));
	op.opnum = OP_OPEN;
	op.arg.open.claim = CLAIM_FH;
	op.arg.open.share_access = share_access;
	op.arg.open.share_deny = share_deny;
	return op;
}

static struct nfs4_op mk_close(const struct nfs4_stateid *sid)
{
	struct nfs4_op op;

	memset(&op, 0, sizeof(op));
	op.opnum = OP_CLOSE;
	op.arg.close.stateid = *sid;
	return op;
}

/* -----------------------------------------------------------------------
 * Part 1: Direct open_state.h API tests
 * ----------------------------------------------------------------------- */

/** Basic open — allocates a valid stateid. */
static void test_api_open_basic(void)
{
	struct open_state_table *ot = NULL;
	struct nfs4_stateid sid;
	int rc;

	ASSERT_EQ(open_state_table_init(TEST_MDS_ID, &ot), 0);

	rc = open_state_open(ot, 100 /* clientid */, NULL, 0, 42 /* fileid */,
			     OPEN4_SHARE_ACCESS_READ,
			     OPEN4_SHARE_DENY_NONE, &sid);
	ASSERT_EQ(rc, 0);
	ASSERT_EQ(sid.seqid, 1);
	ASSERT_NE(memcmp(sid.other, zero_other, NFS4_OTHER_SIZE), 0);

	open_state_table_destroy(ot);
}

/** Two opens on the same file with DENY_NONE — no conflict. */
static void test_api_open_no_conflict(void)
{
	struct open_state_table *ot = NULL;
	struct nfs4_stateid sid1, sid2;
	int rc;

	ASSERT_EQ(open_state_table_init(TEST_MDS_ID, &ot), 0);

	rc = open_state_open(ot, 100, NULL, 0, 42,
			     OPEN4_SHARE_ACCESS_READ,
			     OPEN4_SHARE_DENY_NONE, &sid1);
	ASSERT_EQ(rc, 0);

	rc = open_state_open(ot, 200, NULL, 0, 42,
			     OPEN4_SHARE_ACCESS_WRITE,
			     OPEN4_SHARE_DENY_NONE, &sid2);
	ASSERT_EQ(rc, 0);

	/* Different stateids. */
	ASSERT_NE(memcmp(sid1.other, sid2.other, NFS4_OTHER_SIZE), 0);

	open_state_table_destroy(ot);
}

/** Share conflict: first open denies write, second requests write. */
static void test_api_share_conflict_deny_write(void)
{
	struct open_state_table *ot = NULL;
	struct nfs4_stateid sid1, sid2;
	int rc;

	ASSERT_EQ(open_state_table_init(TEST_MDS_ID, &ot), 0);

	rc = open_state_open(ot, 100, NULL, 0, 42,
			     OPEN4_SHARE_ACCESS_READ,
			     OPEN4_SHARE_DENY_WRITE, &sid1);
	ASSERT_EQ(rc, 0);

	/* Second open tries to write — conflicts with deny_write. */
	rc = open_state_open(ot, 200, NULL, 0, 42,
			     OPEN4_SHARE_ACCESS_WRITE,
			     OPEN4_SHARE_DENY_NONE, &sid2);
	ASSERT_EQ(rc, -1); /* NFS4ERR_SHARE_DENIED */

	open_state_table_destroy(ot);
}

/** Share conflict: first open reads, second denies read. */
static void test_api_share_conflict_deny_read(void)
{
	struct open_state_table *ot = NULL;
	struct nfs4_stateid sid1, sid2;
	int rc;

	ASSERT_EQ(open_state_table_init(TEST_MDS_ID, &ot), 0);

	rc = open_state_open(ot, 100, NULL, 0, 42,
			     OPEN4_SHARE_ACCESS_READ,
			     OPEN4_SHARE_DENY_NONE, &sid1);
	ASSERT_EQ(rc, 0);

	/* Second open denies read — conflicts with first's access_read. */
	rc = open_state_open(ot, 200, NULL, 0, 42,
			     OPEN4_SHARE_ACCESS_WRITE,
			     OPEN4_SHARE_DENY_READ, &sid2);
	ASSERT_EQ(rc, -1);

	open_state_table_destroy(ot);
}

/** Different files — no conflict even with DENY_BOTH. */
static void test_api_no_conflict_different_files(void)
{
	struct open_state_table *ot = NULL;
	struct nfs4_stateid sid1, sid2;
	int rc;

	ASSERT_EQ(open_state_table_init(TEST_MDS_ID, &ot), 0);

	rc = open_state_open(ot, 100, NULL, 0, 42,
			     OPEN4_SHARE_ACCESS_BOTH,
			     OPEN4_SHARE_DENY_BOTH, &sid1);
	ASSERT_EQ(rc, 0);

	/* Different fileid — no conflict. */
	rc = open_state_open(ot, 200, NULL, 0, 99,
			     OPEN4_SHARE_ACCESS_BOTH,
			     OPEN4_SHARE_DENY_BOTH, &sid2);
	ASSERT_EQ(rc, 0);

	open_state_table_destroy(ot);
}

/** Close — returns seqid+1 stateid. */
static void test_api_close_basic(void)
{
	struct open_state_table *ot = NULL;
	struct nfs4_stateid sid, close_sid;
	int rc;

	ASSERT_EQ(open_state_table_init(TEST_MDS_ID, &ot), 0);

	rc = open_state_open(ot, 100, NULL, 0, 42,
			     OPEN4_SHARE_ACCESS_READ,
			     OPEN4_SHARE_DENY_NONE, &sid);
	ASSERT_EQ(rc, 0);

	rc = open_state_close(ot, 100, &sid, &close_sid);
	ASSERT_EQ(rc, 0);
	ASSERT_EQ(close_sid.seqid, sid.seqid + 1);
	ASSERT_EQ(memcmp(close_sid.other, sid.other, NFS4_OTHER_SIZE), 0);

	open_state_table_destroy(ot);
}

/** Close invalid stateid — returns error. */
static void test_api_close_invalid_stateid(void)
{
	struct open_state_table *ot = NULL;
	struct nfs4_stateid bogus, close_sid;
	int rc;

	ASSERT_EQ(open_state_table_init(TEST_MDS_ID, &ot), 0);

	memset(&bogus, 0xFF, sizeof(bogus));
	rc = open_state_close(ot, 0, &bogus, &close_sid);
	ASSERT_EQ(rc, -1); /* NFS4ERR_BAD_STATEID */

	open_state_table_destroy(ot);
}

/** Double close — second close fails. */
static void test_api_double_close(void)
{
	struct open_state_table *ot = NULL;
	struct nfs4_stateid sid, close_sid;
	int rc;

	ASSERT_EQ(open_state_table_init(TEST_MDS_ID, &ot), 0);

	rc = open_state_open(ot, 100, NULL, 0, 42,
			     OPEN4_SHARE_ACCESS_READ,
			     OPEN4_SHARE_DENY_NONE, &sid);
	ASSERT_EQ(rc, 0);

	rc = open_state_close(ot, 100, &sid, &close_sid);
	ASSERT_EQ(rc, 0);

	/* Second close with original stateid — state gone. */
	rc = open_state_close(ot, 100, &sid, &close_sid);
	ASSERT_EQ(rc, -1);

	open_state_table_destroy(ot);
}

/** Close with wrong seqid — returns error. */
static void test_api_close_wrong_seqid(void)
{
	struct open_state_table *ot = NULL;
	struct nfs4_stateid sid, bad_sid, close_sid;
	int rc;

	ASSERT_EQ(open_state_table_init(TEST_MDS_ID, &ot), 0);

	rc = open_state_open(ot, 100, NULL, 0, 42,
			     OPEN4_SHARE_ACCESS_READ,
			     OPEN4_SHARE_DENY_NONE, &sid);
	ASSERT_EQ(rc, 0);

	/* Tamper with seqid. */
	bad_sid = sid;
	bad_sid.seqid = 99;
	rc = open_state_close(ot, 100, &bad_sid, &close_sid);
	ASSERT_EQ(rc, -1); /* NFS4ERR_BAD_STATEID */

	/* Correct seqid should still work. */
	rc = open_state_close(ot, 100, &sid, &close_sid);
	ASSERT_EQ(rc, 0);

	open_state_table_destroy(ot);
}

/** Close removes share reservation — re-open should succeed. */
static void test_api_close_releases_share(void)
{
	struct open_state_table *ot = NULL;
	struct nfs4_stateid sid1, sid2, close_sid;
	int rc;

	ASSERT_EQ(open_state_table_init(TEST_MDS_ID, &ot), 0);

	/* Open with DENY_BOTH. */
	rc = open_state_open(ot, 100, NULL, 0, 42,
			     OPEN4_SHARE_ACCESS_BOTH,
			     OPEN4_SHARE_DENY_BOTH, &sid1);
	ASSERT_EQ(rc, 0);

	/* Second open — blocked. */
	rc = open_state_open(ot, 200, NULL, 0, 42,
			     OPEN4_SHARE_ACCESS_READ,
			     OPEN4_SHARE_DENY_NONE, &sid2);
	ASSERT_EQ(rc, -1);

	/* Close first open. */
	rc = open_state_close(ot, 100, &sid1, &close_sid);
	ASSERT_EQ(rc, 0);

	/* Now second open should succeed. */
	rc = open_state_open(ot, 200, NULL, 0, 42,
			     OPEN4_SHARE_ACCESS_READ,
			     OPEN4_SHARE_DENY_NONE, &sid2);
	ASSERT_EQ(rc, 0);

	open_state_table_destroy(ot);
}

/** open_state_find — copies state for open, returns -1 after close. */
static void test_api_find(void)
{
	struct open_state_table *ot = NULL;
	struct nfs4_stateid sid, close_sid;
	struct nfs4_open_state found;
	int rc;

	ASSERT_EQ(open_state_table_init(TEST_MDS_ID, &ot), 0);

	rc = open_state_open(ot, 100, NULL, 0, 42,
			     OPEN4_SHARE_ACCESS_READ,
			     OPEN4_SHARE_DENY_NONE, &sid);
	ASSERT_EQ(rc, 0);

	rc = open_state_find(ot, &sid, &found);
	ASSERT_EQ(rc, 0);
	ASSERT_EQ(found.fileid, 42);

	rc = open_state_close(ot, 100, &sid, &close_sid);
	ASSERT_EQ(rc, 0);

	rc = open_state_find(ot, &sid, &found);
	ASSERT_EQ(rc, -1);

	open_state_table_destroy(ot);
}

/** Close with wrong clientid (different owner) — returns error. */
static void test_api_close_wrong_owner(void)
{
	struct open_state_table *ot = NULL;
	struct nfs4_stateid sid, close_sid;
	int rc;

	ASSERT_EQ(open_state_table_init(TEST_MDS_ID, &ot), 0);

	rc = open_state_open(ot, 100 /* clientid */, NULL, 0, 42 /* fileid */,
			     OPEN4_SHARE_ACCESS_READ,
			     OPEN4_SHARE_DENY_NONE, &sid);
	ASSERT_EQ(rc, 0);

	/* Close with different clientid — wrong owner. */
	rc = open_state_close(ot, 999, &sid, &close_sid);
	ASSERT_EQ(rc, -1); /* NFS4ERR_BAD_STATEID */

	/* Correct owner should still work. */
	rc = open_state_close(ot, 100, &sid, &close_sid);
	ASSERT_EQ(rc, 0);

	open_state_table_destroy(ot);
}

/** Two open-owners under the same client get separate stateids. */
static void test_api_different_open_owners(void)
{
	struct open_state_table *ot = NULL;
	struct nfs4_stateid sid1, sid2;
	struct nfs4_open_state found;
	int rc;

	static const uint8_t owner_a[] = "process-A";
	static const uint8_t owner_b[] = "process-B";

	ASSERT_EQ(open_state_table_init(TEST_MDS_ID, &ot), 0);

	/* Same clientid, different owners, same file, DENY_NONE. */
	rc = open_state_open(ot, 100,
			     owner_a, sizeof(owner_a) - 1,
			     42,
			     OPEN4_SHARE_ACCESS_READ,
			     OPEN4_SHARE_DENY_NONE, &sid1);
	ASSERT_EQ(rc, 0);

	rc = open_state_open(ot, 100,
			     owner_b, sizeof(owner_b) - 1,
			     42,
			     OPEN4_SHARE_ACCESS_READ,
			     OPEN4_SHARE_DENY_NONE, &sid2);
	ASSERT_EQ(rc, 0);

	/* Different stateids. */
	ASSERT_NE(memcmp(sid1.other, sid2.other, NFS4_OTHER_SIZE), 0);

	/* Verify owner stored correctly. */
	rc = open_state_find(ot, &sid1, &found);
	ASSERT_EQ(rc, 0);
	ASSERT_EQ(found.open_owner_len, sizeof(owner_a) - 1);
	ASSERT_EQ(memcmp(found.open_owner, owner_a,
			  sizeof(owner_a) - 1), 0);

	rc = open_state_find(ot, &sid2, &found);
	ASSERT_EQ(rc, 0);
	ASSERT_EQ(found.open_owner_len, sizeof(owner_b) - 1);
	ASSERT_EQ(memcmp(found.open_owner, owner_b,
			  sizeof(owner_b) - 1), 0);

	open_state_table_destroy(ot);
}

/* -----------------------------------------------------------------------
 * Part 2: Compound integration tests
 * ----------------------------------------------------------------------- */

/** OPEN (CLAIM_NULL, create) + GETATTR + CLOSE — full flow. */
static void test_compound_open_create_close(void)
{
	struct mds_catalogue *db = NULL;
	struct open_state_table *ot = NULL;
	struct compound_data cd;
	struct nfs4_op ops[6];
	struct nfs4_result res[6];
	uint32_t n;
	char *path;

	path = make_temp_db_path();
	db = open_test_catalogue(); VERIFY(db != NULL);
	ASSERT_EQ(open_state_table_init(TEST_MDS_ID, &ot), 0);

	/* SEQUENCE + PUTROOTFH + OPEN(create "doc.txt") + GETATTR + CLOSE */
	compound_init(&cd);
	cd.cat = db;
	cd.ot = ot;

	ops[0] = mk_sequence();
	ops[1] = mk_putrootfh();
	ops[2] = mk_open_create("doc.txt", 0644,
				OPEN4_SHARE_ACCESS_BOTH,
				OPEN4_SHARE_DENY_NONE);

	n = compound_process(&cd, ops, res, 3);
	ASSERT_EQ(n, (uint32_t)3);
	ASSERT_EQ(res[2].status, NFS4_OK);
	ASSERT_EQ(res[2].res.open.stateid.seqid, 1);
	ASSERT_NE(memcmp(res[2].res.open.stateid.other,
			 zero_other, NFS4_OTHER_SIZE), 0);
	ASSERT_EQ(res[2].res.open.inode.type, MDS_FTYPE_REG);
	ASSERT_EQ(res[2].res.open.inode.mode, (uint32_t)0644);

	{
		struct nfs4_stateid open_sid = res[2].res.open.stateid;
		uint64_t file_fid = res[2].res.open.inode.fileid;

		/* Current FH should now be the opened file. */
		/* GETATTR should return the file. */
		ops[0] = mk_getattr();
		n = compound_process(&cd, ops, res, 1);
		ASSERT_EQ(n, (uint32_t)1);
		ASSERT_EQ(res[0].status, NFS4_OK);
		ASSERT_EQ(res[0].res.getattr.inode.fileid, file_fid);

		/* CLOSE */
		ops[0] = mk_close(&open_sid);
		n = compound_process(&cd, ops, res, 1);
		ASSERT_EQ(n, (uint32_t)1);
		ASSERT_EQ(res[0].status, NFS4_OK);
		ASSERT_EQ(res[0].res.close.stateid.seqid,
			  open_sid.seqid + 1);
	}

	open_state_table_destroy(ot);
	mds_catalogue_close(db);
	cleanup_temp_db(path);
	free(path);
}

/** OPEN existing file (CLAIM_NULL, no create). */
static void test_compound_open_existing(void)
{
	struct mds_catalogue *db = NULL;
	struct open_state_table *ot = NULL;
	struct compound_data cd;
	struct nfs4_op ops[6];
	struct nfs4_result res[6];
	uint32_t n;
	char *path;

	path = make_temp_db_path();
	db = open_test_catalogue(); VERIFY(db != NULL);
	ASSERT_EQ(open_state_table_init(TEST_MDS_ID, &ot), 0);

	/* First: create the file via CREATE. */
	compound_init(&cd);
	cd.cat = db;
	cd.ot = ot;
	ops[0] = mk_sequence();
	ops[1] = mk_putrootfh();
	ops[2] = mk_create("existing.txt", MDS_FTYPE_REG, 0644);
	n = compound_process(&cd, ops, res, 3);
	ASSERT_EQ(n, (uint32_t)3);

	/* Now open it without create. */
	compound_init(&cd);
	cd.cat = db;
	cd.ot = ot;
	ops[0] = mk_sequence();
	ops[1] = mk_putrootfh();
	ops[2] = mk_open_existing("existing.txt",
				  OPEN4_SHARE_ACCESS_READ,
				  OPEN4_SHARE_DENY_NONE);

	n = compound_process(&cd, ops, res, 3);
	ASSERT_EQ(n, (uint32_t)3);
	ASSERT_EQ(res[2].status, NFS4_OK);
	ASSERT_EQ(res[2].res.open.inode.type, MDS_FTYPE_REG);

	/* Close it. */
	{
		struct nfs4_stateid sid = res[2].res.open.stateid;

		ops[0] = mk_close(&sid);
		n = compound_process(&cd, ops, res, 1);
		ASSERT_EQ(n, (uint32_t)1);
		ASSERT_EQ(res[0].status, NFS4_OK);
	}

	open_state_table_destroy(ot);
	mds_catalogue_close(db);
	cleanup_temp_db(path);
	free(path);
}

/** OPEN nonexistent file without create — NFS4ERR_NOENT. */
static void test_compound_open_noent(void)
{
	struct mds_catalogue *db = NULL;
	struct open_state_table *ot = NULL;
	struct compound_data cd;
	struct nfs4_op ops[4];
	struct nfs4_result res[4];
	uint32_t n;
	char *path;

	path = make_temp_db_path();
	db = open_test_catalogue(); VERIFY(db != NULL);
	ASSERT_EQ(open_state_table_init(TEST_MDS_ID, &ot), 0);

	compound_init(&cd);
	cd.cat = db;
	cd.ot = ot;
	ops[0] = mk_sequence();
	ops[1] = mk_putrootfh();
	ops[2] = mk_open_existing("ghost.txt",
				  OPEN4_SHARE_ACCESS_READ,
				  OPEN4_SHARE_DENY_NONE);

	n = compound_process(&cd, ops, res, 3);
	ASSERT_EQ(n, (uint32_t)3);
	ASSERT_EQ(res[2].status, NFS4ERR_NOENT);

	open_state_table_destroy(ot);
	mds_catalogue_close(db);
	cleanup_temp_db(path);
	free(path);
}

/** OPEN guarded create on existing file — NFS4ERR_EXIST. */
static void test_compound_open_guarded_exist(void)
{
	struct mds_catalogue *db = NULL;
	struct open_state_table *ot = NULL;
	struct compound_data cd;
	struct nfs4_op ops[4];
	struct nfs4_result res[4];
	uint32_t n;
	char *path;

	path = make_temp_db_path();
	db = open_test_catalogue(); VERIFY(db != NULL);
	ASSERT_EQ(open_state_table_init(TEST_MDS_ID, &ot), 0);

	/* Create the file first. */
	compound_init(&cd);
	cd.cat = db;
	cd.ot = ot;
	ops[0] = mk_sequence();
	ops[1] = mk_putrootfh();
	ops[2] = mk_open_create("guard.txt", 0644,
				OPEN4_SHARE_ACCESS_READ,
				OPEN4_SHARE_DENY_NONE);
	n = compound_process(&cd, ops, res, 3);
	ASSERT_EQ(n, (uint32_t)3);
	ASSERT_EQ(res[2].status, NFS4_OK);

	/* Close first open. */
	{
		struct nfs4_stateid sid = res[2].res.open.stateid;

		compound_init(&cd);
		cd.cat = db;
		cd.ot = ot;
		ops[0] = mk_close(&sid);
		n = compound_process(&cd, ops, res, 1);
		ASSERT_EQ(n, (uint32_t)1);
	}

	/* Guarded create on same name — should fail. */
	compound_init(&cd);
	cd.cat = db;
	cd.ot = ot;
	ops[0] = mk_sequence();
	ops[1] = mk_putrootfh();
	{
		struct nfs4_op guarded_op;

		memset(&guarded_op, 0, sizeof(guarded_op));
		guarded_op.opnum = OP_OPEN;
		guarded_op.arg.open.claim = CLAIM_NULL;
		snprintf(guarded_op.arg.open.name,
			 sizeof(guarded_op.arg.open.name), "guard.txt");
		guarded_op.arg.open.share_access = OPEN4_SHARE_ACCESS_READ;
		guarded_op.arg.open.share_deny = OPEN4_SHARE_DENY_NONE;
		guarded_op.arg.open.create = true;
		guarded_op.arg.open.createmode = CREATEMODE_GUARDED4;
		guarded_op.arg.open.mode = 0644;
		ops[2] = guarded_op;
	}

	n = compound_process(&cd, ops, res, 3);
	ASSERT_EQ(n, (uint32_t)3);
	ASSERT_EQ(res[2].status, NFS4ERR_EXIST);

	open_state_table_destroy(ot);
	mds_catalogue_close(db);
	cleanup_temp_db(path);
	free(path);
}

/** Share conflict via compound — deny_write blocks write access. */
static void test_compound_share_conflict(void)
{
	struct mds_catalogue *db = NULL;
	struct open_state_table *ot = NULL;
	struct compound_data cd;
	struct nfs4_op ops[4];
	struct nfs4_result res[4];
	uint32_t n;
	char *path;

	path = make_temp_db_path();
	db = open_test_catalogue(); VERIFY(db != NULL);
	ASSERT_EQ(open_state_table_init(TEST_MDS_ID, &ot), 0);

	/* First open: read, deny_write. */
	compound_init(&cd);
	cd.cat = db;
	cd.ot = ot;
	ops[0] = mk_sequence();
	ops[1] = mk_putrootfh();
	ops[2] = mk_open_create("shared.txt", 0644,
				OPEN4_SHARE_ACCESS_READ,
				OPEN4_SHARE_DENY_WRITE);
	n = compound_process(&cd, ops, res, 3);
	ASSERT_EQ(n, (uint32_t)3);
	ASSERT_EQ(res[2].status, NFS4_OK);

	/* Second open: write, deny_none — should conflict. */
	compound_init(&cd);
	cd.cat = db;
	cd.ot = ot;
	ops[0] = mk_sequence();
	ops[1] = mk_putrootfh();
	ops[2] = mk_open_existing("shared.txt",
				  OPEN4_SHARE_ACCESS_WRITE,
				  OPEN4_SHARE_DENY_NONE);
	n = compound_process(&cd, ops, res, 3);
	ASSERT_EQ(n, (uint32_t)3);
	ASSERT_EQ(res[2].status, NFS4ERR_SHARE_DENIED);

	open_state_table_destroy(ot);
	mds_catalogue_close(db);
	cleanup_temp_db(path);
	free(path);
}

/** OPEN CLAIM_FH — open by current file handle. */
static void test_compound_open_claim_fh(void)
{
	struct mds_catalogue *db = NULL;
	struct open_state_table *ot = NULL;
	struct compound_data cd;
	struct nfs4_op ops[6];
	struct nfs4_result res[6];
	uint64_t file_fid;
	uint32_t n;
	char *path;

	path = make_temp_db_path();
	db = open_test_catalogue(); VERIFY(db != NULL);
	ASSERT_EQ(open_state_table_init(TEST_MDS_ID, &ot), 0);

	/* Create file via namespace. */
	compound_init(&cd);
	cd.cat = db;
	cd.ot = ot;
	ops[0] = mk_sequence();
	ops[1] = mk_putrootfh();
	ops[2] = mk_create("fhfile.txt", MDS_FTYPE_REG, 0644);
	n = compound_process(&cd, ops, res, 3);
	ASSERT_EQ(n, (uint32_t)3);
	file_fid = res[2].res.create.inode.fileid;

	/* Open via CLAIM_FH: PUTFH(file) + OPEN(CLAIM_FH). */
	compound_init(&cd);
	cd.cat = db;
	cd.ot = ot;
	ops[0] = mk_sequence();
	ops[1] = mk_putfh(file_fid);
	ops[2] = mk_open_fh(OPEN4_SHARE_ACCESS_READ,
			     OPEN4_SHARE_DENY_NONE);

	n = compound_process(&cd, ops, res, 3);
	ASSERT_EQ(n, (uint32_t)3);
	ASSERT_EQ(res[2].status, NFS4_OK);
	ASSERT_EQ(res[2].res.open.inode.fileid, file_fid);

	/* Close. */
	{
		struct nfs4_stateid sid = res[2].res.open.stateid;

		ops[0] = mk_close(&sid);
		n = compound_process(&cd, ops, res, 1);
		ASSERT_EQ(n, (uint32_t)1);
		ASSERT_EQ(res[0].status, NFS4_OK);
	}

	open_state_table_destroy(ot);
	mds_catalogue_close(db);
	cleanup_temp_db(path);
	free(path);
}

/** CLOSE with bad stateid — NFS4ERR_BAD_STATEID. */
static void test_compound_close_bad_stateid(void)
{
	struct mds_catalogue *db = NULL;
	struct open_state_table *ot = NULL;
	struct compound_data cd;
	struct nfs4_op ops[3];
	struct nfs4_result res[3];
	struct nfs4_stateid bogus;
	uint32_t n;
	char *path;

	path = make_temp_db_path();
	db = open_test_catalogue(); VERIFY(db != NULL);
	ASSERT_EQ(open_state_table_init(TEST_MDS_ID, &ot), 0);

	compound_init(&cd);
	cd.cat = db;
	cd.ot = ot;
	ops[0] = mk_sequence();
	ops[1] = mk_putrootfh();

	memset(&bogus, 0xBB, sizeof(bogus));
	ops[2] = mk_close(&bogus);

	n = compound_process(&cd, ops, res, 3);
	ASSERT_EQ(n, (uint32_t)3);
	ASSERT_EQ(res[2].status, NFS4ERR_BAD_STATEID);

	open_state_table_destroy(ot);
	mds_catalogue_close(db);
	cleanup_temp_db(path);
	free(path);
}

/** OPEN + CLOSE + re-OPEN — share released, new open succeeds. */
static void test_compound_reopen_after_close(void)
{
	struct mds_catalogue *db = NULL;
	struct open_state_table *ot = NULL;
	struct compound_data cd;
	struct nfs4_op ops[4];
	struct nfs4_result res[4];
	uint32_t n;
	char *path;

	path = make_temp_db_path();
	db = open_test_catalogue(); VERIFY(db != NULL);
	ASSERT_EQ(open_state_table_init(TEST_MDS_ID, &ot), 0);

	/* Open with DENY_BOTH. */
	compound_init(&cd);
	cd.cat = db;
	cd.ot = ot;
	ops[0] = mk_sequence();
	ops[1] = mk_putrootfh();
	ops[2] = mk_open_create("reopen.txt", 0644,
				OPEN4_SHARE_ACCESS_BOTH,
				OPEN4_SHARE_DENY_BOTH);
	n = compound_process(&cd, ops, res, 3);
	ASSERT_EQ(n, (uint32_t)3);
	ASSERT_EQ(res[2].status, NFS4_OK);

	/* Close. */
	{
		struct nfs4_stateid sid = res[2].res.open.stateid;

		compound_init(&cd);
		cd.cat = db;
		cd.ot = ot;
		ops[0] = mk_putrootfh(); /* need a valid FH for CLOSE */
		ops[1] = mk_close(&sid);
		n = compound_process(&cd, ops, res, 2);
		ASSERT_EQ(n, (uint32_t)2);
		ASSERT_EQ(res[1].status, NFS4_OK);
	}

	/* Re-open should succeed (share released). */
	compound_init(&cd);
	cd.cat = db;
	cd.ot = ot;
	ops[0] = mk_sequence();
	ops[1] = mk_putrootfh();
	ops[2] = mk_open_existing("reopen.txt",
				  OPEN4_SHARE_ACCESS_READ,
				  OPEN4_SHARE_DENY_NONE);
	n = compound_process(&cd, ops, res, 3);
	ASSERT_EQ(n, (uint32_t)3);
	ASSERT_EQ(res[2].status, NFS4_OK);

	open_state_table_destroy(ot);
	mds_catalogue_close(db);
	cleanup_temp_db(path);
	free(path);
}

/** OPEN on directory — NFS4ERR_ISDIR. */
static void test_compound_open_directory(void)
{
	struct mds_catalogue *db = NULL;
	struct open_state_table *ot = NULL;
	struct compound_data cd;
	struct nfs4_op ops[4];
	struct nfs4_result res[4];
	uint32_t n;
	char *path;

	path = make_temp_db_path();
	db = open_test_catalogue(); VERIFY(db != NULL);
	ASSERT_EQ(open_state_table_init(TEST_MDS_ID, &ot), 0);

	/* Create a directory. */
	compound_init(&cd);
	cd.cat = db;
	cd.ot = ot;
	ops[0] = mk_sequence();
	ops[1] = mk_putrootfh();
	ops[2] = mk_create("subdir", MDS_FTYPE_DIR, 0755);
	n = compound_process(&cd, ops, res, 3);
	ASSERT_EQ(n, (uint32_t)3);

	/* Try to OPEN the directory — should fail. */
	compound_init(&cd);
	cd.cat = db;
	cd.ot = ot;
	ops[0] = mk_sequence();
	ops[1] = mk_putrootfh();
	ops[2] = mk_open_existing("subdir",
				  OPEN4_SHARE_ACCESS_READ,
				  OPEN4_SHARE_DENY_NONE);
	n = compound_process(&cd, ops, res, 3);
	ASSERT_EQ(n, (uint32_t)3);
	ASSERT_EQ(res[2].status, NFS4ERR_ISDIR);

	open_state_table_destroy(ot);
	mds_catalogue_close(db);
	cleanup_temp_db(path);
	free(path);
}

/* -----------------------------------------------------------------------
 * main
 * ----------------------------------------------------------------------- */

int main(void)
{
	fprintf(stdout, "Running open state tests:\n");

	/* Part 1: Direct API tests */
	RUN_TEST(test_api_open_basic);
	RUN_TEST(test_api_open_no_conflict);
	RUN_TEST(test_api_share_conflict_deny_write);
	RUN_TEST(test_api_share_conflict_deny_read);
	RUN_TEST(test_api_no_conflict_different_files);
	RUN_TEST(test_api_close_basic);
	RUN_TEST(test_api_close_invalid_stateid);
	RUN_TEST(test_api_double_close);
	RUN_TEST(test_api_close_wrong_seqid);
	RUN_TEST(test_api_close_releases_share);
	RUN_TEST(test_api_find);
	RUN_TEST(test_api_close_wrong_owner);
	RUN_TEST(test_api_different_open_owners);

	/* Part 2: Compound integration tests */
	RUN_TEST(test_compound_open_create_close);
	RUN_TEST(test_compound_open_existing);
	RUN_TEST(test_compound_open_noent);
	RUN_TEST(test_compound_open_guarded_exist);
	RUN_TEST(test_compound_share_conflict);
	RUN_TEST(test_compound_open_claim_fh);
	RUN_TEST(test_compound_close_bad_stateid);
	RUN_TEST(test_compound_reopen_after_close);
	RUN_TEST(test_compound_open_directory);

	fprintf(stdout, "\n%d/%d tests passed.\n", tests_passed, tests_run);
	return (tests_passed == tests_run) ? 0 : 1;
}
