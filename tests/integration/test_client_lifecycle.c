/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_client_lifecycle.c -- Full client lifecycle integration test.
 *
 * Exercises the entire Phase 1 stack end-to-end in a single realistic
 * client workflow:
 *
 *   EXCHANGE_ID -> CREATE_SESSION -> SEQUENCE
 *   -> mkdir, touch (CREATE), OPEN/CLOSE, READDIR, RENAME, REMOVE
 *   -> DESTROY_SESSION
 *
 * All subsystems are wired together:
 *   - catalogue backend (metadata persistence)
 *   - Namespace layer (create/lookup/remove/rename/readdir)
 *   - Session/clientid (EXCHANGE_ID, CREATE_SESSION, SEQUENCE)
 *   - Open state (OPEN, CLOSE, share reservations)
 *   - Compound dispatch (operation sequencing, FH state)
 *
 * This is the simulated equivalent of Phase 1 item 9:
 *   "mount from Linux NFS client, ls, mkdir, touch, rm"
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>

#include "pnfs_mds.h"
#include "test_helpers.h"
#include "compound.h"
#include "session.h"
#include "open_state.h"

/* -----------------------------------------------------------------------
 * Test infrastructure
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

/* -----------------------------------------------------------------------
 * Temp database helpers
 * ----------------------------------------------------------------------- */

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

/* -----------------------------------------------------------------------
 * Op builder helpers
 * ----------------------------------------------------------------------- */

static struct nfs4_op mk_exchange_id(const char *owner, uint32_t flags)
{
	struct nfs4_op op;

	memset(&op, 0, sizeof(op));
	op.opnum = OP_EXCHANGE_ID;
	snprintf((char *)op.arg.exchange_id.co_ownerid,
		 sizeof(op.arg.exchange_id.co_ownerid), "%s", owner);
	op.arg.exchange_id.co_ownerid_len = (uint32_t)strlen(owner);
	memset(op.arg.exchange_id.verifier, 0x42, NFS4_VERIFIER_SIZE);
	op.arg.exchange_id.eia_flags = flags;
	return op;
}

static struct nfs4_op mk_create_session(uint64_t clientid, uint32_t seqid,
					uint32_t slots)
{
	struct nfs4_op op;

	memset(&op, 0, sizeof(op));
	op.opnum = OP_CREATE_SESSION;
	op.arg.create_session.clientid = clientid;
	op.arg.create_session.seqid = seqid;
	op.arg.create_session.csa_flags = 0;
	op.arg.create_session.fore_slots = slots;
	op.arg.create_session.back_slots = slots;
	/*
	 * RFC 8881 S18.36 channel attributes -- op_create_session now
	 * enforces NFS4ERR_TOOSMALL on too-small ca_max{request,response}
	 * size and zero ca_maxoperations / ca_maxrequests, so populate
	 * realistic values here so the integration test exercises a
	 * fully-formed CREATE_SESSION rather than an XDR shell.
	 */
	op.arg.create_session.fore_max_request_size  = 8192;
	op.arg.create_session.fore_max_response_size = 8192;
	op.arg.create_session.fore_max_operations    = 64;
	op.arg.create_session.back_max_request_size  = 4096;
	op.arg.create_session.back_max_response_size = 4096;
	op.arg.create_session.back_max_operations    = 2;
	op.arg.create_session.back_max_requests      = slots;
	return op;
}

static struct nfs4_op mk_destroy_session(const uint8_t sid[SESSION_ID_SIZE])
{
	struct nfs4_op op;

	memset(&op, 0, sizeof(op));
	op.opnum = OP_DESTROY_SESSION;
	memcpy(op.arg.destroy_session.session_id, sid, SESSION_ID_SIZE);
	return op;
}

static struct nfs4_op mk_sequence(const uint8_t sid[SESSION_ID_SIZE],
				  uint32_t slot_id, uint32_t seq_id)
{
	struct nfs4_op op;

	memset(&op, 0, sizeof(op));
	op.opnum = OP_SEQUENCE;
	memcpy(op.arg.sequence.session_id, sid, SESSION_ID_SIZE);
	op.arg.sequence.slot_id = slot_id;
	op.arg.sequence.seq_id = seq_id;
	op.arg.sequence.highest_slot_id = slot_id;
	op.arg.sequence.cache_this = false;
	return op;
}

static struct nfs4_op mk_putrootfh(void)
{
	struct nfs4_op op;

	memset(&op, 0, sizeof(op));
	op.opnum = OP_PUTROOTFH;
	return op;
}

static struct nfs4_op mk_lookup(const char *name)
{
	struct nfs4_op op;

	memset(&op, 0, sizeof(op));
	op.opnum = OP_LOOKUP;
	snprintf(op.arg.lookup.name, sizeof(op.arg.lookup.name), "%s", name);
	return op;
}

static struct nfs4_op mk_getattr(void)
{
	struct nfs4_op op;

	memset(&op, 0, sizeof(op));
	op.opnum = OP_GETATTR;
	return op;
}

static struct nfs4_op mk_savefh(void)
{
	struct nfs4_op op;

	memset(&op, 0, sizeof(op));
	op.opnum = OP_SAVEFH;
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

static struct nfs4_op mk_remove(const char *name)
{
	struct nfs4_op op;

	memset(&op, 0, sizeof(op));
	op.opnum = OP_REMOVE;
	snprintf(op.arg.remove.name, sizeof(op.arg.remove.name), "%s", name);
	return op;
}

static struct nfs4_op mk_rename(const char *src, const char *dst)
{
	struct nfs4_op op;

	memset(&op, 0, sizeof(op));
	op.opnum = OP_RENAME;
	snprintf(op.arg.rename.src_name, sizeof(op.arg.rename.src_name),
		 "%s", src);
	snprintf(op.arg.rename.dst_name, sizeof(op.arg.rename.dst_name),
		 "%s", dst);
	return op;
}

static struct nfs4_op mk_readdir(void)
{
	struct nfs4_op op;

	memset(&op, 0, sizeof(op));
	op.opnum = OP_READDIR;
	op.arg.readdir.cookie = 0;
	return op;
}

static struct nfs4_op mk_open_create(const char *name, uint32_t mode,
				     uint32_t access, uint32_t deny)
{
	struct nfs4_op op;

	memset(&op, 0, sizeof(op));
	op.opnum = OP_OPEN;
	op.arg.open.claim = CLAIM_NULL;
	snprintf(op.arg.open.name, sizeof(op.arg.open.name), "%s", name);
	op.arg.open.share_access = access;
	op.arg.open.share_deny = deny;
	op.arg.open.create = true;
	op.arg.open.createmode = CREATEMODE_UNCHECKED4;
	op.arg.open.mode = mode;
	return op;
}

static struct nfs4_op mk_open_existing(const char *name,
				       uint32_t access, uint32_t deny)
{
	struct nfs4_op op;

	memset(&op, 0, sizeof(op));
	op.opnum = OP_OPEN;
	op.arg.open.claim = CLAIM_NULL;
	snprintf(op.arg.open.name, sizeof(op.arg.open.name), "%s", name);
	op.arg.open.share_access = access;
	op.arg.open.share_deny = deny;
	op.arg.open.create = false;
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

static struct nfs4_op mk_reclaim_complete(void)
{
	struct nfs4_op op;

	memset(&op, 0, sizeof(op));
	op.opnum = OP_RECLAIM_COMPLETE;
	op.arg.reclaim_complete.rca_one_fs = false;
	return op;
}

static struct nfs4_op mk_setattr(uint32_t mask, uint32_t mode)
{
	struct nfs4_op op;

	memset(&op, 0, sizeof(op));
	op.opnum = OP_SETATTR;
	op.arg.setattr.mask = mask;
	op.arg.setattr.attrs.mode = mode;
	return op;
}

/* -----------------------------------------------------------------------
 * Test 1: Full client lifecycle
 *
 * Simulates: mount -> mkdir -> touch -> ls -> chmod -> rename -> rm -> umount
 * ----------------------------------------------------------------------- */

static void test_full_client_lifecycle(void)
{
	struct mds_catalogue *db = NULL;
	struct session_table *st = NULL;
	struct open_state_table *ot = NULL;
	struct compound_data cd;
	struct nfs4_op *ops = calloc(8, sizeof(struct nfs4_op));
	struct nfs4_result *res = calloc(8, sizeof(struct nfs4_result));
	char *path;
	uint32_t n;

	assert(ops != NULL);
	assert(res != NULL);

	/* Session tracking state. */
	uint64_t clientid;
	uint32_t create_seqid;
	uint8_t  session_id[SESSION_ID_SIZE];
	uint32_t slot_seq = 1; /* SEQUENCE seq_id starts at 1 */

	path = make_temp_db_path();
	db = open_test_catalogue(); assert(db != NULL);
	ASSERT_EQ(session_table_init(TEST_MDS_ID, 90, &st), 0);
	ASSERT_EQ(open_state_table_init(TEST_MDS_ID, &ot), 0);

	/* ===== Phase 1: Session establishment ===== */

	/* EXCHANGE_ID */
	compound_init(&cd);
	cd.cat = db;
	cd.st = st;
	cd.ot = ot;
	ops[0] = mk_exchange_id("test-client-01",
				EXCHGID4_FLAG_USE_PNFS_MDS);

	n = compound_process(&cd, ops, res, 1);
	ASSERT_EQ(n, (uint32_t)1);
	ASSERT_EQ(res[0].status, NFS4_OK);
	clientid = res[0].res.exchange_id.clientid;
	create_seqid = res[0].res.exchange_id.seqid;
	ASSERT_NE(clientid, (uint64_t)0);

	/* CREATE_SESSION */
	compound_init(&cd);
	cd.cat = db;
	cd.st = st;
	cd.ot = ot;
	ops[0] = mk_create_session(clientid, create_seqid, 16);

	n = compound_process(&cd, ops, res, 1);
	ASSERT_EQ(n, (uint32_t)1);
	ASSERT_EQ(res[0].status, NFS4_OK);
	memcpy(session_id, res[0].res.create_session.session_id,
	       SESSION_ID_SIZE);
	ASSERT_TRUE(res[0].res.create_session.fore_slots > 0);

	/* ===== Phase 1b: RECLAIM_COMPLETE (required before non-reclaim ops) ===== */

	compound_init(&cd);
	cd.cat = db;
	cd.st = st;
	cd.ot = ot;
	ops[0] = mk_sequence(session_id, 0, slot_seq++);
	ops[1] = mk_reclaim_complete();

	n = compound_process(&cd, ops, res, 2);
	ASSERT_EQ(n, (uint32_t)2);
	ASSERT_EQ(res[0].status, NFS4_OK);
	ASSERT_EQ(res[1].status, NFS4_OK);

	/* ===== Phase 2: mkdir /workspace ===== */

	compound_init(&cd);
	cd.cat = db;
	cd.st = st;
	cd.ot = ot;
	ops[0] = mk_sequence(session_id, 0, slot_seq++);
	ops[1] = mk_putrootfh();
	ops[2] = mk_create("workspace", MDS_FTYPE_DIR, 0755);
	ops[3] = mk_getattr();

	n = compound_process(&cd, ops, res, 4);
	ASSERT_EQ(n, (uint32_t)4);
	ASSERT_EQ(res[0].status, NFS4_OK);
	ASSERT_EQ(res[2].status, NFS4_OK);
	ASSERT_EQ(res[3].status, NFS4_OK);
	ASSERT_EQ(res[3].res.getattr.inode.type, MDS_FTYPE_DIR);
	ASSERT_EQ(res[3].res.getattr.inode.mode, (uint32_t)0755);

	/* ===== Phase 3: mkdir /workspace/subdir ===== */

	compound_init(&cd);
	cd.cat = db;
	cd.st = st;
	cd.ot = ot;
	ops[0] = mk_sequence(session_id, 0, slot_seq++);
	ops[1] = mk_putrootfh();
	ops[2] = mk_lookup("workspace");
	ops[3] = mk_create("subdir", MDS_FTYPE_DIR, 0750);

	n = compound_process(&cd, ops, res, 4);
	ASSERT_EQ(n, (uint32_t)4);
	ASSERT_EQ(res[3].status, NFS4_OK);
	ASSERT_EQ(res[3].res.create.inode.type, MDS_FTYPE_DIR);

	/* ===== Phase 4: touch /workspace/file1.txt (via OPEN create) ===== */

	{
		struct nfs4_stateid sid_file1;

		compound_init(&cd);
		cd.cat = db;
		cd.st = st;
		cd.ot = ot;
		ops[0] = mk_sequence(session_id, 0, slot_seq++);
		ops[1] = mk_putrootfh();
		ops[2] = mk_lookup("workspace");
		ops[3] = mk_open_create("file1.txt", 0644,
					OPEN4_SHARE_ACCESS_BOTH,
					OPEN4_SHARE_DENY_NONE);
		ops[4] = mk_getattr();

		n = compound_process(&cd, ops, res, 5);
		ASSERT_EQ(n, (uint32_t)5);
		ASSERT_EQ(res[3].status, NFS4_OK);
		ASSERT_EQ(res[3].res.open.stateid.seqid, 1);
		ASSERT_EQ(res[4].status, NFS4_OK);
		ASSERT_EQ(res[4].res.getattr.inode.type, MDS_FTYPE_REG);
		ASSERT_EQ(res[4].res.getattr.inode.mode, (uint32_t)0644);

		sid_file1 = res[3].res.open.stateid;

		/* CLOSE file1.txt */
		ops[0] = mk_close(&sid_file1);
		n = compound_process(&cd, ops, res, 1);
		ASSERT_EQ(n, (uint32_t)1);
		ASSERT_EQ(res[0].status, NFS4_OK);
		ASSERT_EQ(res[0].res.close.stateid.seqid, sid_file1.seqid + 1);
	}

	/* ===== Phase 5: touch /workspace/file2.txt ===== */

	{
		struct nfs4_stateid sid_file2;

		compound_init(&cd);
		cd.cat = db;
		cd.st = st;
		cd.ot = ot;
		ops[0] = mk_sequence(session_id, 0, slot_seq++);
		ops[1] = mk_putrootfh();
		ops[2] = mk_lookup("workspace");
		ops[3] = mk_open_create("file2.txt", 0600,
					OPEN4_SHARE_ACCESS_WRITE,
					OPEN4_SHARE_DENY_NONE);

		n = compound_process(&cd, ops, res, 4);
		ASSERT_EQ(n, (uint32_t)4);
		ASSERT_EQ(res[3].status, NFS4_OK);

		sid_file2 = res[3].res.open.stateid;

		/* CLOSE */
		ops[0] = mk_close(&sid_file2);
		n = compound_process(&cd, ops, res, 1);
		ASSERT_EQ(n, (uint32_t)1);
		ASSERT_EQ(res[0].status, NFS4_OK);
	}

	/* ===== Phase 6: ls /workspace (READDIR) ===== */

	compound_init(&cd);
	cd.cat = db;
	cd.st = st;
	cd.ot = ot;
	ops[0] = mk_sequence(session_id, 0, slot_seq++);
	ops[1] = mk_putrootfh();
	ops[2] = mk_lookup("workspace");
	ops[3] = mk_readdir();

	n = compound_process(&cd, ops, res, 4);
	ASSERT_EQ(n, (uint32_t)4);
	ASSERT_EQ(res[3].status, NFS4_OK);
	ASSERT_EQ(res[3].res.readdir.count, (uint32_t)3);
	ASSERT_TRUE(res[3].res.readdir.eof);
	/* Catalogue ordering across backends is intentionally
	 * unspecified -- RonDB returns rows in NDB scan order, the
	 * memdb test backend returns them in hash-bucket order, and
	 * neither matches the lexicographic ordering this test
	 * originally assumed.  Verify the entry set instead of the
	 * sequence; downstream tests already cover sort-on-the-client. */
	{
		bool saw_file1 = false, saw_file2 = false, saw_subdir = false;
		uint32_t i;

		for (i = 0; i < res[3].res.readdir.count; i++) {
			const char *name = res[3].res.readdir.entries[i].name;

			if (strcmp(name, "file1.txt") == 0) {
				saw_file1 = true;
			} else if (strcmp(name, "file2.txt") == 0) {
				saw_file2 = true;
			} else if (strcmp(name, "subdir") == 0) {
				saw_subdir = true;
			}
		}
		ASSERT_TRUE(saw_file1);
		ASSERT_TRUE(saw_file2);
		ASSERT_TRUE(saw_subdir);
	}

	/* ===== Phase 7: chmod /workspace/file1.txt (reopen + setattr) ===== */

	{
		struct nfs4_stateid sid;

		compound_init(&cd);
		cd.cat = db;
		cd.st = st;
		cd.ot = ot;
		ops[0] = mk_sequence(session_id, 0, slot_seq++);
		ops[1] = mk_putrootfh();
		ops[2] = mk_lookup("workspace");
		ops[3] = mk_open_existing("file1.txt",
					  OPEN4_SHARE_ACCESS_WRITE,
					  OPEN4_SHARE_DENY_NONE);
		ops[4] = mk_setattr(MDS_ATTR_MODE, 0600);
		ops[5] = mk_getattr();

		n = compound_process(&cd, ops, res, 6);
		ASSERT_EQ(n, (uint32_t)6);
		ASSERT_EQ(res[3].status, NFS4_OK);
		ASSERT_EQ(res[4].status, NFS4_OK);
		ASSERT_EQ(res[5].status, NFS4_OK);
		ASSERT_EQ(res[5].res.getattr.inode.mode, (uint32_t)0600);

		sid = res[3].res.open.stateid;

		/* CLOSE */
		ops[0] = mk_close(&sid);
		n = compound_process(&cd, ops, res, 1);
		ASSERT_EQ(n, (uint32_t)1);
		ASSERT_EQ(res[0].status, NFS4_OK);
	}

	/* ===== Phase 8: mv /workspace/file2.txt -> /workspace/renamed.txt ===== */

	compound_init(&cd);
	cd.cat = db;
	cd.st = st;
	cd.ot = ot;
	ops[0] = mk_sequence(session_id, 0, slot_seq++);
	ops[1] = mk_putrootfh();
	ops[2] = mk_lookup("workspace");
	ops[3] = mk_savefh();       /* saved_fh = /workspace (source dir) */
	ops[4] = mk_putrootfh();
	ops[5] = mk_lookup("workspace");
	/* current_fh = /workspace (dest dir), saved_fh = /workspace */
	ops[6] = mk_rename("file2.txt", "renamed.txt");

	n = compound_process(&cd, ops, res, 7);
	ASSERT_EQ(n, (uint32_t)7);
	ASSERT_EQ(res[6].status, NFS4_OK);

	/* Verify rename: file2.txt gone, renamed.txt exists. */
	compound_init(&cd);
	cd.cat = db;
	cd.st = st;
	cd.ot = ot;
	ops[0] = mk_sequence(session_id, 0, slot_seq++);
	ops[1] = mk_putrootfh();
	ops[2] = mk_lookup("workspace");
	ops[3] = mk_lookup("file2.txt");

	n = compound_process(&cd, ops, res, 4);
	ASSERT_EQ(n, (uint32_t)4);
	ASSERT_EQ(res[3].status, NFS4ERR_NOENT);

	compound_init(&cd);
	cd.cat = db;
	cd.st = st;
	cd.ot = ot;
	ops[0] = mk_sequence(session_id, 0, slot_seq++);
	ops[1] = mk_putrootfh();
	ops[2] = mk_lookup("workspace");
	ops[3] = mk_lookup("renamed.txt");
	ops[4] = mk_getattr();

	n = compound_process(&cd, ops, res, 5);
	ASSERT_EQ(n, (uint32_t)5);
	ASSERT_EQ(res[3].status, NFS4_OK);
	ASSERT_EQ(res[4].res.getattr.inode.type, MDS_FTYPE_REG);

	/* ===== Phase 9: rm -- remove all files and dirs ===== */

	/* rm /workspace/file1.txt */
	compound_init(&cd);
	cd.cat = db;
	cd.st = st;
	cd.ot = ot;
	ops[0] = mk_sequence(session_id, 0, slot_seq++);
	ops[1] = mk_putrootfh();
	ops[2] = mk_lookup("workspace");
	ops[3] = mk_remove("file1.txt");

	n = compound_process(&cd, ops, res, 4);
	ASSERT_EQ(n, (uint32_t)4);
	ASSERT_EQ(res[3].status, NFS4_OK);

	/* rm /workspace/renamed.txt */
	compound_init(&cd);
	cd.cat = db;
	cd.st = st;
	cd.ot = ot;
	ops[0] = mk_sequence(session_id, 0, slot_seq++);
	ops[1] = mk_putrootfh();
	ops[2] = mk_lookup("workspace");
	ops[3] = mk_remove("renamed.txt");

	n = compound_process(&cd, ops, res, 4);
	ASSERT_EQ(n, (uint32_t)4);
	ASSERT_EQ(res[3].status, NFS4_OK);

	/* rmdir /workspace/subdir */
	compound_init(&cd);
	cd.cat = db;
	cd.st = st;
	cd.ot = ot;
	ops[0] = mk_sequence(session_id, 0, slot_seq++);
	ops[1] = mk_putrootfh();
	ops[2] = mk_lookup("workspace");
	ops[3] = mk_remove("subdir");

	n = compound_process(&cd, ops, res, 4);
	ASSERT_EQ(n, (uint32_t)4);
	ASSERT_EQ(res[3].status, NFS4_OK);

	/* rmdir /workspace */
	compound_init(&cd);
	cd.cat = db;
	cd.st = st;
	cd.ot = ot;
	ops[0] = mk_sequence(session_id, 0, slot_seq++);
	ops[1] = mk_putrootfh();
	ops[2] = mk_remove("workspace");

	n = compound_process(&cd, ops, res, 3);
	ASSERT_EQ(n, (uint32_t)3);
	ASSERT_EQ(res[2].status, NFS4_OK);

	/* ===== Phase 10: Verify empty root ===== */

	compound_init(&cd);
	cd.cat = db;
	cd.st = st;
	cd.ot = ot;
	ops[0] = mk_sequence(session_id, 0, slot_seq++);
	ops[1] = mk_putrootfh();
	ops[2] = mk_readdir();

	n = compound_process(&cd, ops, res, 3);
	ASSERT_EQ(n, (uint32_t)3);
	ASSERT_EQ(res[2].status, NFS4_OK);
	ASSERT_EQ(res[2].res.readdir.count, (uint32_t)0);
	ASSERT_TRUE(res[2].res.readdir.eof);

	/* ===== Phase 11: Session teardown ===== */

	compound_init(&cd);
	cd.cat = db;
	cd.st = st;
	cd.ot = ot;
	ops[0] = mk_destroy_session(session_id);

	n = compound_process(&cd, ops, res, 1);
	ASSERT_EQ(n, (uint32_t)1);
	ASSERT_EQ(res[0].status, NFS4_OK);

	/* Cleanup. */
	open_state_table_destroy(ot);
	session_table_destroy(st);
	mds_catalogue_close(db);
	cleanup_temp_db(path);
	free(path);
	free(ops);
	free(res);
}

/* -----------------------------------------------------------------------
 * Test 2: Share conflict across opens
 *
 * Simulates: client A opens file with DENY_WRITE, client B tries to
 * open for write -> conflict. Client A closes -> client B retries -> OK.
 * ----------------------------------------------------------------------- */

static void test_share_conflict_lifecycle(void)
{
	struct mds_catalogue *db = NULL;
	struct session_table *st = NULL;
	struct open_state_table *ot = NULL;
	struct compound_data cd;
	struct nfs4_op *ops = calloc(6, sizeof(struct nfs4_op));
	struct nfs4_result *res = calloc(6, sizeof(struct nfs4_result));
	char *path;
	uint32_t n;
	uint64_t clientid;
	uint32_t seqid;
	uint8_t session_id[SESSION_ID_SIZE];
	uint32_t slot_seq = 1;

	assert(ops != NULL);
	assert(res != NULL);

	path = make_temp_db_path();
	db = open_test_catalogue(); assert(db != NULL);
	ASSERT_EQ(session_table_init(TEST_MDS_ID, 90, &st), 0);
	ASSERT_EQ(open_state_table_init(TEST_MDS_ID, &ot), 0);

	/* Quick session setup. */
	compound_init(&cd);
	cd.cat = db;
	cd.st = st;
	cd.ot = ot;
	ops[0] = mk_exchange_id("share-test-client",
				EXCHGID4_FLAG_USE_PNFS_MDS);
	n = compound_process(&cd, ops, res, 1);
	ASSERT_EQ(n, (uint32_t)1);
	ASSERT_EQ(res[0].status, NFS4_OK);
	clientid = res[0].res.exchange_id.clientid;
	seqid = res[0].res.exchange_id.seqid;

	compound_init(&cd);
	cd.cat = db;
	cd.st = st;
	cd.ot = ot;
	ops[0] = mk_create_session(clientid, seqid, 8);
	n = compound_process(&cd, ops, res, 1);
	ASSERT_EQ(n, (uint32_t)1);
	ASSERT_EQ(res[0].status, NFS4_OK);
	memcpy(session_id, res[0].res.create_session.session_id,
	       SESSION_ID_SIZE);

	/* RECLAIM_COMPLETE (required before OPEN). */
	compound_init(&cd);
	cd.cat = db;
	cd.st = st;
	cd.ot = ot;
	ops[0] = mk_sequence(session_id, 0, slot_seq++);
	ops[1] = mk_reclaim_complete();
	n = compound_process(&cd, ops, res, 2);
	ASSERT_EQ(n, (uint32_t)2);
	ASSERT_EQ(res[0].status, NFS4_OK);
	ASSERT_EQ(res[1].status, NFS4_OK);

	/* Create and OPEN file with DENY_WRITE.
	 *
	 * RFC 8881 S8.2.2 + S9.1.4: same-owner re-OPEN merges share modes
	 * rather than conflicting.  We therefore tag the two OPENs with
	 * distinct open_owner byte strings so the second one represents
	 * a different opener under the same NFS client and the file's
	 * existing DENY_WRITE applies. */
	{
		struct nfs4_stateid sid_a;
		static const uint8_t owner_a[] = { 'a', 'a', 'a', 'a' };
		static const uint8_t owner_b[] = { 'b', 'b', 'b', 'b' };

		compound_init(&cd);
		cd.cat = db;
		cd.st = st;
		cd.ot = ot;
		ops[0] = mk_sequence(session_id, 0, slot_seq++);
		ops[1] = mk_putrootfh();
		ops[2] = mk_open_create("shared.dat", 0644,
					OPEN4_SHARE_ACCESS_READ,
					OPEN4_SHARE_DENY_WRITE);
		memcpy(ops[2].arg.open.open_owner, owner_a, sizeof(owner_a));
		ops[2].arg.open.open_owner_len = (uint32_t)sizeof(owner_a);

		n = compound_process(&cd, ops, res, 3);
		ASSERT_EQ(n, (uint32_t)3);
		ASSERT_EQ(res[2].status, NFS4_OK);
		sid_a = res[2].res.open.stateid;

		/* Second OPEN by a *different* opener for write -- should fail
		 * with SHARE_DENIED because owner A still holds DENY_WRITE. */
		compound_init(&cd);
		cd.cat = db;
		cd.st = st;
		cd.ot = ot;
		ops[0] = mk_sequence(session_id, 0, slot_seq++);
		ops[1] = mk_putrootfh();
		ops[2] = mk_open_existing("shared.dat",
					  OPEN4_SHARE_ACCESS_WRITE,
					  OPEN4_SHARE_DENY_NONE);
		memcpy(ops[2].arg.open.open_owner, owner_b, sizeof(owner_b));
		ops[2].arg.open.open_owner_len = (uint32_t)sizeof(owner_b);

		n = compound_process(&cd, ops, res, 3);
		ASSERT_EQ(n, (uint32_t)3);
		ASSERT_EQ(res[2].status, NFS4ERR_SHARE_DENIED);

		/* Close client A's open. */
		compound_init(&cd);
		cd.cat = db;
		cd.st = st;
		cd.ot = ot;
		ops[0] = mk_sequence(session_id, 0, slot_seq++);
		ops[1] = mk_putrootfh();
		ops[2] = mk_lookup("shared.dat");
		ops[3] = mk_close(&sid_a);

		n = compound_process(&cd, ops, res, 4);
		ASSERT_EQ(n, (uint32_t)4);
		ASSERT_EQ(res[3].status, NFS4_OK);

		/* Retry as owner B -- should succeed now. */
		compound_init(&cd);
		cd.cat = db;
		cd.st = st;
		cd.ot = ot;
		ops[0] = mk_sequence(session_id, 0, slot_seq++);
		ops[1] = mk_putrootfh();
		ops[2] = mk_open_existing("shared.dat",
					  OPEN4_SHARE_ACCESS_WRITE,
					  OPEN4_SHARE_DENY_NONE);
		memcpy(ops[2].arg.open.open_owner, owner_b, sizeof(owner_b));
		ops[2].arg.open.open_owner_len = (uint32_t)sizeof(owner_b);

		n = compound_process(&cd, ops, res, 3);
		ASSERT_EQ(n, (uint32_t)3);
		ASSERT_EQ(res[2].status, NFS4_OK);

		/* Close and clean up. */
		{
			struct nfs4_stateid sid_b = res[2].res.open.stateid;

			ops[0] = mk_close(&sid_b);
			n = compound_process(&cd, ops, res, 1);
			ASSERT_EQ(n, (uint32_t)1);
			ASSERT_EQ(res[0].status, NFS4_OK);
		}
	}

	/* Remove the file. */
	compound_init(&cd);
	cd.cat = db;
	cd.st = st;
	cd.ot = ot;
	ops[0] = mk_sequence(session_id, 0, slot_seq++);
	ops[1] = mk_putrootfh();
	ops[2] = mk_remove("shared.dat");

	n = compound_process(&cd, ops, res, 3);
	ASSERT_EQ(n, (uint32_t)3);
	ASSERT_EQ(res[2].status, NFS4_OK);

	/* Teardown. */
	compound_init(&cd);
	cd.cat = db;
	cd.st = st;
	cd.ot = ot;
	ops[0] = mk_destroy_session(session_id);
	n = compound_process(&cd, ops, res, 1);
	ASSERT_EQ(n, (uint32_t)1);
	ASSERT_EQ(res[0].status, NFS4_OK);

	open_state_table_destroy(ot);
	session_table_destroy(st);
	mds_catalogue_close(db);
	cleanup_temp_db(path);
	free(path);
	free(ops);
	free(res);
}

/* -----------------------------------------------------------------------
 * Test 3: Nested directory operations
 *
 * Simulates: mkdir -p /a/b/c, touch files at each level, rm -rf /a
 * ----------------------------------------------------------------------- */

static void test_nested_directory_lifecycle(void)
{
	struct mds_catalogue *db = NULL;
	struct session_table *st = NULL;
	struct open_state_table *ot = NULL;
	struct compound_data cd;
	struct nfs4_op *ops = calloc(8, sizeof(struct nfs4_op));
	struct nfs4_result *res = calloc(8, sizeof(struct nfs4_result));
	char *path;
	uint32_t n;
	uint64_t clientid;
	uint32_t seqid;
	uint8_t session_id[SESSION_ID_SIZE];
	uint32_t slot_seq = 1;

	assert(ops != NULL);
	assert(res != NULL);

	path = make_temp_db_path();
	db = open_test_catalogue(); assert(db != NULL);
	ASSERT_EQ(session_table_init(TEST_MDS_ID, 90, &st), 0);
	ASSERT_EQ(open_state_table_init(TEST_MDS_ID, &ot), 0);

	/* Session setup. */
	compound_init(&cd);
	cd.cat = db;
	cd.st = st;
	cd.ot = ot;
	ops[0] = mk_exchange_id("nested-test",
				EXCHGID4_FLAG_USE_PNFS_MDS);
	n = compound_process(&cd, ops, res, 1);
	ASSERT_EQ(res[0].status, NFS4_OK);
	clientid = res[0].res.exchange_id.clientid;
	seqid = res[0].res.exchange_id.seqid;

	compound_init(&cd);
	cd.cat = db;
	cd.st = st;
	cd.ot = ot;
	ops[0] = mk_create_session(clientid, seqid, 4);
	n = compound_process(&cd, ops, res, 1);
	ASSERT_EQ(res[0].status, NFS4_OK);
	memcpy(session_id, res[0].res.create_session.session_id,
	       SESSION_ID_SIZE);

	/* mkdir /a */
	compound_init(&cd);
	cd.cat = db;
	cd.st = st;
	cd.ot = ot;
	ops[0] = mk_sequence(session_id, 0, slot_seq++);
	ops[1] = mk_putrootfh();
	ops[2] = mk_create("a", MDS_FTYPE_DIR, 0755);
	n = compound_process(&cd, ops, res, 3);
	ASSERT_EQ(res[2].status, NFS4_OK);

	/* mkdir /a/b */
	compound_init(&cd);
	cd.cat = db;
	cd.st = st;
	cd.ot = ot;
	ops[0] = mk_sequence(session_id, 0, slot_seq++);
	ops[1] = mk_putrootfh();
	ops[2] = mk_lookup("a");
	ops[3] = mk_create("b", MDS_FTYPE_DIR, 0755);
	n = compound_process(&cd, ops, res, 4);
	ASSERT_EQ(res[3].status, NFS4_OK);

	/* mkdir /a/b/c */
	compound_init(&cd);
	cd.cat = db;
	cd.st = st;
	cd.ot = ot;
	ops[0] = mk_sequence(session_id, 0, slot_seq++);
	ops[1] = mk_putrootfh();
	ops[2] = mk_lookup("a");
	ops[3] = mk_lookup("b");
	ops[4] = mk_create("c", MDS_FTYPE_DIR, 0755);
	n = compound_process(&cd, ops, res, 5);
	ASSERT_EQ(res[4].status, NFS4_OK);

	/* touch /a/b/c/deep.txt (via CREATE, not OPEN) */
	compound_init(&cd);
	cd.cat = db;
	cd.st = st;
	cd.ot = ot;
	ops[0] = mk_sequence(session_id, 0, slot_seq++);
	ops[1] = mk_putrootfh();
	ops[2] = mk_lookup("a");
	ops[3] = mk_lookup("b");
	ops[4] = mk_lookup("c");
	ops[5] = mk_create("deep.txt", MDS_FTYPE_REG, 0644);
	n = compound_process(&cd, ops, res, 6);
	ASSERT_EQ(res[5].status, NFS4_OK);

	/* Verify deep path traversal: LOOKUP a -> b -> c -> deep.txt */
	compound_init(&cd);
	cd.cat = db;
	cd.st = st;
	cd.ot = ot;
	ops[0] = mk_sequence(session_id, 0, slot_seq++);
	ops[1] = mk_putrootfh();
	ops[2] = mk_lookup("a");
	ops[3] = mk_lookup("b");
	ops[4] = mk_lookup("c");
	ops[5] = mk_lookup("deep.txt");
	ops[6] = mk_getattr();
	n = compound_process(&cd, ops, res, 7);
	ASSERT_EQ(n, (uint32_t)7);
	ASSERT_EQ(res[6].status, NFS4_OK);
	ASSERT_EQ(res[6].res.getattr.inode.type, MDS_FTYPE_REG);

	/* rm -rf /a (bottom-up) */
	/* rm /a/b/c/deep.txt */
	compound_init(&cd);
	cd.cat = db;
	cd.st = st;
	cd.ot = ot;
	ops[0] = mk_sequence(session_id, 0, slot_seq++);
	ops[1] = mk_putrootfh();
	ops[2] = mk_lookup("a");
	ops[3] = mk_lookup("b");
	ops[4] = mk_lookup("c");
	ops[5] = mk_remove("deep.txt");
	n = compound_process(&cd, ops, res, 6);
	ASSERT_EQ(res[5].status, NFS4_OK);

	/* rmdir /a/b/c */
	compound_init(&cd);
	cd.cat = db;
	cd.st = st;
	cd.ot = ot;
	ops[0] = mk_sequence(session_id, 0, slot_seq++);
	ops[1] = mk_putrootfh();
	ops[2] = mk_lookup("a");
	ops[3] = mk_lookup("b");
	ops[4] = mk_remove("c");
	n = compound_process(&cd, ops, res, 5);
	ASSERT_EQ(res[4].status, NFS4_OK);

	/* rmdir /a/b */
	compound_init(&cd);
	cd.cat = db;
	cd.st = st;
	cd.ot = ot;
	ops[0] = mk_sequence(session_id, 0, slot_seq++);
	ops[1] = mk_putrootfh();
	ops[2] = mk_lookup("a");
	ops[3] = mk_remove("b");
	n = compound_process(&cd, ops, res, 4);
	ASSERT_EQ(res[3].status, NFS4_OK);

	/* rmdir /a */
	compound_init(&cd);
	cd.cat = db;
	cd.st = st;
	cd.ot = ot;
	ops[0] = mk_sequence(session_id, 0, slot_seq++);
	ops[1] = mk_putrootfh();
	ops[2] = mk_remove("a");
	n = compound_process(&cd, ops, res, 3);
	ASSERT_EQ(res[2].status, NFS4_OK);

	/* Verify root is clean. */
	compound_init(&cd);
	cd.cat = db;
	cd.st = st;
	cd.ot = ot;
	ops[0] = mk_sequence(session_id, 0, slot_seq++);
	ops[1] = mk_putrootfh();
	ops[2] = mk_readdir();
	n = compound_process(&cd, ops, res, 3);
	ASSERT_EQ(res[2].status, NFS4_OK);
	ASSERT_EQ(res[2].res.readdir.count, (uint32_t)0);

	/* Teardown. */
	compound_init(&cd);
	cd.cat = db;
	cd.st = st;
	cd.ot = ot;
	ops[0] = mk_destroy_session(session_id);
	n = compound_process(&cd, ops, res, 1);
	ASSERT_EQ(res[0].status, NFS4_OK);

	open_state_table_destroy(ot);
	session_table_destroy(st);
	mds_catalogue_close(db);
	cleanup_temp_db(path);
	free(path);
	free(ops);
	free(res);
}

/* -----------------------------------------------------------------------
 * main
 * ----------------------------------------------------------------------- */

int main(void)
{
	fprintf(stdout, "Running client lifecycle integration tests:\n");

	RUN_TEST(test_full_client_lifecycle);
	RUN_TEST(test_share_conflict_lifecycle);
	RUN_TEST(test_nested_directory_lifecycle);

	fprintf(stdout, "\n%d/%d tests passed.\n", tests_passed, tests_run);
	return (tests_passed == tests_run) ? 0 : 1;
}
