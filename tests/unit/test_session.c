/*
 * Copyright (c) 2026 PeakAIO. All rights reserved.
 * SPDX-License-Identifier: LicenseRef-PeakAIO-Proprietary
 *
 * test_session.c — Unit tests for session/clientid management.
 *
 * Tests the session.h API directly (EXCHANGE_ID, CREATE_SESSION,
 * DESTROY_SESSION, SEQUENCE), plus a compound integration test
 * that exercises the full EXCHANGE_ID → CREATE_SESSION → SEQUENCE
 * → PUTROOTFH → GETATTR flow.
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
	fprintf(stdout, "  %-40s", #fn);				\
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

/* Fixed test verifiers. */
static const uint8_t verifier_a[NFS4_VERIFIER_SIZE] = {
	0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08
};
static const uint8_t verifier_b[NFS4_VERIFIER_SIZE] = {
	0xA1, 0xA2, 0xA3, 0xA4, 0xA5, 0xA6, 0xA7, 0xA8
};

static const uint8_t owner_alice[] = "alice-linux-client";
static const uint32_t owner_alice_len = sizeof(owner_alice) - 1;

static const uint8_t owner_bob[] = "bob-mac-client";
static const uint32_t owner_bob_len = sizeof(owner_bob) - 1;

/* -----------------------------------------------------------------------
 * Test: EXCHANGE_ID — new client
 * ----------------------------------------------------------------------- */

static void test_exchange_id_new_client(void)
{
	struct session_table *st = NULL;
	uint64_t clientid = 0;
	uint32_t seqid = 0;
	uint32_t flags = 0;

	ASSERT_EQ(session_table_init(TEST_MDS_ID, 0, &st), 0);

	ASSERT_EQ(session_exchange_id(st, owner_alice, owner_alice_len,
				      verifier_a, 0,
				      &clientid, &seqid, &flags, 0, 0, 0), 0);

	ASSERT_NE(clientid, 0);
	ASSERT_EQ(seqid, 1);
	ASSERT_TRUE(flags & EXCHGID4_FLAG_USE_PNFS_MDS);

	session_table_destroy(st);
}

/* -----------------------------------------------------------------------
 * Test: EXCHANGE_ID — confirmed record + same params returns same clientid
 *
 * RFC 8881 §18.35.4 case 2 ("renewal"): a confirmed record matched on
 * co_ownerid + verifier + principal returns the same clientid.
 *
 * Note: prior to commit "compound: EXCHANGE_ID conformance ..." this
 * test exercised the bogus "two unconfirmed EXCHANGE_IDs return the
 * same clientid" path, which violated case 4 ("unconfirmed records are
 * replaced unconditionally").  We now CREATE_SESSION between the two
 * EID calls to confirm the record before re-asserting renewal.
 * ----------------------------------------------------------------------- */

static void test_exchange_id_same_verifier(void)
{
	struct session_table *st = NULL;
	uint64_t cid1 = 0, cid2 = 0;
	uint32_t seqid = 0;
	uint8_t session_id[SESSION_ID_SIZE];
	uint32_t fore = 0, back = 0;

	ASSERT_EQ(session_table_init(TEST_MDS_ID, 0, &st), 0);

	ASSERT_EQ(session_exchange_id(st, owner_alice, owner_alice_len,
				      verifier_a, 0,
				      &cid1, &seqid, NULL, 0, 0, 0), 0);

	/* Confirm the client so the second EID hits case 2 (renewal),
	 * not case 4 (unconfirmed-record replacement). */
	ASSERT_EQ(session_create_session(st, cid1, seqid,
					 32, 4,
					 0, 0,
					 0, 0,
					 0, 0,
					 1,
					 0, 0, 0,
					 session_id, &fore, &back, NULL, NULL), 0);

	ASSERT_EQ(session_exchange_id(st, owner_alice, owner_alice_len,
				      verifier_a, 0,
				      &cid2, &seqid, NULL, 0, 0, 0), 0);

	/* Confirmed record + same verifier + same (default) principal →
	 * same clientid (case 2). */
	ASSERT_EQ(cid1, cid2);

	session_table_destroy(st);
}

/* -----------------------------------------------------------------------
 * Test: EXCHANGE_ID — different verifier → new clientid
 * ----------------------------------------------------------------------- */

static void test_exchange_id_new_verifier(void)
{
	struct session_table *st = NULL;
	uint64_t cid1 = 0, cid2 = 0;
	uint32_t seqid = 0;

	ASSERT_EQ(session_table_init(TEST_MDS_ID, 0, &st), 0);

	ASSERT_EQ(session_exchange_id(st, owner_alice, owner_alice_len,
				      verifier_a, 0,
				      &cid1, &seqid, NULL, 0, 0, 0), 0);
	ASSERT_EQ(session_exchange_id(st, owner_alice, owner_alice_len,
				      verifier_b, 0,
				      &cid2, &seqid, NULL, 0, 0, 0), 0);

	/* Different verifier → new incarnation → different clientid. */
	ASSERT_NE(cid1, cid2);

	session_table_destroy(st);
}

/* -----------------------------------------------------------------------
 * Test: EXCHANGE_ID — two distinct clients
 * ----------------------------------------------------------------------- */

static void test_exchange_id_two_clients(void)
{
	struct session_table *st = NULL;
	uint64_t cid_alice = 0, cid_bob = 0;
	uint32_t seqid = 0;

	ASSERT_EQ(session_table_init(TEST_MDS_ID, 0, &st), 0);

	ASSERT_EQ(session_exchange_id(st, owner_alice, owner_alice_len,
				      verifier_a, 0,
				      &cid_alice, &seqid, NULL, 0, 0, 0), 0);
	ASSERT_EQ(session_exchange_id(st, owner_bob, owner_bob_len,
				      verifier_a, 0,
				      &cid_bob, &seqid, NULL, 0, 0, 0), 0);

	ASSERT_NE(cid_alice, cid_bob);

	session_table_destroy(st);
}

/* -----------------------------------------------------------------------
 * Test: CREATE_SESSION — basic success
 * ----------------------------------------------------------------------- */

static void test_create_session_basic(void)
{
	struct session_table *st = NULL;
	uint64_t clientid = 0;
	uint32_t seqid = 0;
	uint8_t session_id[SESSION_ID_SIZE];
	uint32_t fore = 0, back = 0;

	ASSERT_EQ(session_table_init(TEST_MDS_ID, 0, &st), 0);

	ASSERT_EQ(session_exchange_id(st, owner_alice, owner_alice_len,
				      verifier_a, 0,
				      &clientid, &seqid, NULL, 0, 0, 0), 0);

	ASSERT_EQ(session_create_session(st, clientid, seqid,
					 32, 4,
					 0, 0,
					 0, 0,
					 0, 0,
					 1, /* minorversion */
					 0, 0, 0,
					 session_id, &fore, &back, NULL, NULL), 0);

	ASSERT_EQ(fore, 32);
	ASSERT_EQ(back, 4);

	/* Session ID should be non-zero. */
	{
		uint8_t zero[SESSION_ID_SIZE];

		memset(zero, 0, SESSION_ID_SIZE);
		ASSERT_NE(memcmp(session_id, zero, SESSION_ID_SIZE), 0);
	}

	session_table_destroy(st);
}

/* -----------------------------------------------------------------------
 * Test: CREATE_SESSION — invalid clientid
 * ----------------------------------------------------------------------- */

static void test_create_session_stale_clientid(void)
{
	struct session_table *st = NULL;
	uint8_t session_id[SESSION_ID_SIZE];
	uint32_t fore = 0, back = 0;

	ASSERT_EQ(session_table_init(TEST_MDS_ID, 0, &st), 0);

	/* Bogus clientid → NFS4ERR_STALE_CLIENTID (rc = -1). */
	ASSERT_EQ(session_create_session(st, 0xDEADBEEF, 1,
					 32, 4,
					 0, 0,
					 0, 0,
					 0, 0,
					 1, /* minorversion */
					 0, 0, 0,
					 session_id, &fore, &back, NULL, NULL), -1);

	session_table_destroy(st);
}

/* -----------------------------------------------------------------------
 * Test: CREATE_SESSION — bad seqid
 * ----------------------------------------------------------------------- */

static void test_create_session_bad_seqid(void)
{
	struct session_table *st = NULL;
	uint64_t clientid = 0;
	uint32_t seqid = 0;
	uint8_t session_id[SESSION_ID_SIZE];
	uint32_t fore = 0, back = 0;

	ASSERT_EQ(session_table_init(TEST_MDS_ID, 0, &st), 0);

	ASSERT_EQ(session_exchange_id(st, owner_alice, owner_alice_len,
				      verifier_a, 0,
				      &clientid, &seqid, NULL, 0, 0, 0), 0);

	/* seqid should be 1; use 99 → NFS4ERR_SEQ_MISORDERED (rc = -2). */
	ASSERT_EQ(session_create_session(st, clientid, 99,
					 32, 4,
					 0, 0,
					 0, 0,
					 0, 0,
					 1, /* minorversion */
					 0, 0, 0,
					 session_id, &fore, &back, NULL, NULL), -2);

	session_table_destroy(st);
}

/* -----------------------------------------------------------------------
 * Test: CREATE_SESSION — slot count negotiation
 * ----------------------------------------------------------------------- */

static void test_create_session_slot_cap(void)
{
	struct session_table *st = NULL;
	uint64_t clientid = 0;
	uint32_t seqid = 0;
	uint8_t session_id[SESSION_ID_SIZE];
	uint32_t fore = 0, back = 0;

	ASSERT_EQ(session_table_init(TEST_MDS_ID, 0, &st), 0);

	ASSERT_EQ(session_exchange_id(st, owner_alice, owner_alice_len,
				      verifier_a, 0,
				      &clientid, &seqid, NULL, 0, 0, 0), 0);

	/* Request 9999 slots → capped at SESSION_MAX_SLOTS (64). */
	ASSERT_EQ(session_create_session(st, clientid, seqid,
					 9999, 4,
					 0, 0,
					 0, 0,
					 0, 0,
					 1, /* minorversion */
					 0, 0, 0,
					 session_id, &fore, &back, NULL, NULL), 0);

	ASSERT_EQ(fore, SESSION_MAX_SLOTS);

	session_table_destroy(st);
}

/* -----------------------------------------------------------------------
 * Test: DESTROY_SESSION — success
 * ----------------------------------------------------------------------- */

static void test_destroy_session(void)
{
	struct session_table *st = NULL;
	uint64_t clientid = 0;
	uint32_t seqid = 0;
	uint8_t session_id[SESSION_ID_SIZE];
	uint32_t fore = 0, back = 0;

	ASSERT_EQ(session_table_init(TEST_MDS_ID, 0, &st), 0);

	ASSERT_EQ(session_exchange_id(st, owner_alice, owner_alice_len,
				      verifier_a, 0,
				      &clientid, &seqid, NULL, 0, 0, 0), 0);
	ASSERT_EQ(session_create_session(st, clientid, seqid,
					 16, 4,
					 0, 0,
					 0, 0,
					 0, 0,
					 1, /* minorversion */
					 0, 0, 0,
					 session_id, &fore, &back, NULL, NULL), 0);

	/* Destroy the session. */
	ASSERT_EQ(session_destroy_session(st, session_id), 0);

	/* Destroying again should fail (NFS4ERR_BADSESSION). */
	ASSERT_EQ(session_destroy_session(st, session_id), -1);

	session_table_destroy(st);
}

/* -----------------------------------------------------------------------
 * Test: SEQUENCE — valid new request
 * ----------------------------------------------------------------------- */

static void test_sequence_valid(void)
{
	struct session_table *st = NULL;
	uint64_t clientid = 0;
	uint32_t seqid = 0;
	uint8_t session_id[SESSION_ID_SIZE];
	uint32_t fore = 0, back = 0;
	uint32_t highest = 0, target = 0, flags = 0;
	int rc;

	ASSERT_EQ(session_table_init(TEST_MDS_ID, 0, &st), 0);

	ASSERT_EQ(session_exchange_id(st, owner_alice, owner_alice_len,
				      verifier_a, 0,
				      &clientid, &seqid, NULL, 0, 0, 0), 0);
	ASSERT_EQ(session_create_session(st, clientid, seqid,
					 16, 4,
					 0, 0,
					 0, 0,
					 0, 0,
					 1, /* minorversion */
					 0, 0, 0,
					 session_id, &fore, &back, NULL, NULL), 0);

	/* Slot starts at seq_id = 0; first new request uses 1. */
	rc = session_sequence_check(st, session_id, 0, 1, 15,
				    &highest, &target, &flags, NULL);
	ASSERT_EQ(rc, 0);  /* new request */
	ASSERT_EQ(highest, 15);
	ASSERT_EQ(flags, 0);

	/* Next request on same slot: seq_id = 2. */
	rc = session_sequence_check(st, session_id, 0, 2, 15,
				    &highest, &target, &flags, NULL);
	ASSERT_EQ(rc, 0);

	session_table_destroy(st);
}

/* -----------------------------------------------------------------------
 * Test: SEQUENCE — replay detection
 * ----------------------------------------------------------------------- */

static void test_sequence_replay(void)
{
	struct session_table *st = NULL;
	uint64_t clientid = 0;
	uint32_t seqid = 0;
	uint8_t session_id[SESSION_ID_SIZE];
	uint32_t fore = 0, back = 0;
	int rc;

	ASSERT_EQ(session_table_init(TEST_MDS_ID, 0, &st), 0);

	ASSERT_EQ(session_exchange_id(st, owner_alice, owner_alice_len,
				      verifier_a, 0,
				      &clientid, &seqid, NULL, 0, 0, 0), 0);
	ASSERT_EQ(session_create_session(st, clientid, seqid,
					 16, 4,
					 0, 0,
					 0, 0,
					 0, 0,
					 1, /* minorversion */
					 0, 0, 0,
					 session_id, &fore, &back, NULL, NULL), 0);

	/* First request: seq_id = 1. */
	rc = session_sequence_check(st, session_id, 0, 1, 15,
				    NULL, NULL, NULL, NULL);
	ASSERT_EQ(rc, 0);

	/* Replay: seq_id = 1 again (same as last completed). */
	rc = session_sequence_check(st, session_id, 0, 1, 15,
				    NULL, NULL, NULL, NULL);
	ASSERT_EQ(rc, 1);  /* replay detected */

	session_table_destroy(st);
}

/* -----------------------------------------------------------------------
 * Test: SEQUENCE — misordered
 * ----------------------------------------------------------------------- */

static void test_sequence_misordered(void)
{
	struct session_table *st = NULL;
	uint64_t clientid = 0;
	uint32_t seqid = 0;
	uint8_t session_id[SESSION_ID_SIZE];
	uint32_t fore = 0, back = 0;
	int rc;

	ASSERT_EQ(session_table_init(TEST_MDS_ID, 0, &st), 0);

	ASSERT_EQ(session_exchange_id(st, owner_alice, owner_alice_len,
				      verifier_a, 0,
				      &clientid, &seqid, NULL, 0, 0, 0), 0);
	ASSERT_EQ(session_create_session(st, clientid, seqid,
					 16, 4,
					 0, 0,
					 0, 0,
					 0, 0,
					 1, /* minorversion */
					 0, 0, 0,
					 session_id, &fore, &back, NULL, NULL), 0);

	/* First request: seq_id = 1. */
	rc = session_sequence_check(st, session_id, 0, 1, 15,
				    NULL, NULL, NULL, NULL);
	ASSERT_EQ(rc, 0);

	/* Skip to seq_id = 5 → misordered. */
	rc = session_sequence_check(st, session_id, 0, 5, 15,
				    NULL, NULL, NULL, NULL);
	ASSERT_EQ(rc, -3);  /* NFS4ERR_SEQ_MISORDERED */

	session_table_destroy(st);
}

/* -----------------------------------------------------------------------
 * Test: SEQUENCE — bad session ID
 * ----------------------------------------------------------------------- */

static void test_sequence_bad_session(void)
{
	struct session_table *st = NULL;
	uint8_t bogus[SESSION_ID_SIZE];
	int rc;

	ASSERT_EQ(session_table_init(TEST_MDS_ID, 0, &st), 0);

	memset(bogus, 0xFF, SESSION_ID_SIZE);
	rc = session_sequence_check(st, bogus, 0, 1, 0,
				    NULL, NULL, NULL, NULL);
	ASSERT_EQ(rc, -1);  /* NFS4ERR_BADSESSION */

	session_table_destroy(st);
}

/* -----------------------------------------------------------------------
 * Test: SEQUENCE — bad slot ID
 * ----------------------------------------------------------------------- */

static void test_sequence_bad_slot(void)
{
	struct session_table *st = NULL;
	uint64_t clientid = 0;
	uint32_t seqid = 0;
	uint8_t session_id[SESSION_ID_SIZE];
	uint32_t fore = 0, back = 0;
	int rc;

	ASSERT_EQ(session_table_init(TEST_MDS_ID, 0, &st), 0);

	ASSERT_EQ(session_exchange_id(st, owner_alice, owner_alice_len,
				      verifier_a, 0,
				      &clientid, &seqid, NULL, 0, 0, 0), 0);
	ASSERT_EQ(session_create_session(st, clientid, seqid,
					 4, 0,
					 0, 0,
					 0, 0,
					 0, 0,
					 1, /* minorversion */
					 0, 0, 0,
					 session_id, &fore, &back, NULL, NULL), 0);

	/* 4 slots → valid 0-3; slot 99 is out of range. */
	rc = session_sequence_check(st, session_id, 99, 1, 3,
				    NULL, NULL, NULL, NULL);
	ASSERT_EQ(rc, -2);  /* NFS4ERR_BADSLOT */

	session_table_destroy(st);
}

/* -----------------------------------------------------------------------
 * Test: Compound integration — full EXCHANGE_ID → CREATE_SESSION →
 *       SEQUENCE → PUTROOTFH → GETATTR flow
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

static void test_compound_full_flow(void)
{
	struct mds_catalogue *db = NULL;
	struct session_table *st = NULL;
	struct compound_data cd;
	char *path;
	struct nfs4_op ops[5];
	struct nfs4_result res[5];
	uint32_t nres;

	path = make_temp_db_path();
	db = open_test_catalogue(); VERIFY(db != NULL);
	ASSERT_EQ(session_table_init(TEST_MDS_ID, 0, &st), 0);

	compound_init(&cd);
	cd.cat = db;
	cd.st = st;

	/* Op 0: EXCHANGE_ID */
	memset(&ops[0], 0, sizeof(ops[0]));
	ops[0].opnum = OP_EXCHANGE_ID;
	memcpy(ops[0].arg.exchange_id.co_ownerid, owner_alice,
	       owner_alice_len);
	ops[0].arg.exchange_id.co_ownerid_len = owner_alice_len;
	memcpy(ops[0].arg.exchange_id.verifier, verifier_a,
	       NFS4_VERIFIER_SIZE);
	ops[0].arg.exchange_id.eia_flags = 0;

	/* Process EXCHANGE_ID alone to get clientid. */
	memset(res, 0, sizeof(res));
	nres = compound_process(&cd, ops, res, 1);
	ASSERT_EQ(nres, 1);
	ASSERT_EQ(res[0].status, NFS4_OK);

	{
		uint64_t clientid = res[0].res.exchange_id.clientid;
		uint32_t seqid = res[0].res.exchange_id.seqid;

		ASSERT_NE(clientid, 0);

		/* Op 0: CREATE_SESSION (RFC 8881 §18.36 — channel attrs
		 * must be sized so op_create_session doesn't reject the
		 * request with NFS4ERR_TOOSMALL).  Use the same defaults
		 * as a healthy Linux client. */
		memset(&ops[0], 0, sizeof(ops[0]));
		ops[0].opnum = OP_CREATE_SESSION;
		ops[0].arg.create_session.clientid = clientid;
		ops[0].arg.create_session.seqid = seqid;
		ops[0].arg.create_session.fore_slots = 16;
		ops[0].arg.create_session.back_slots = 4;
		ops[0].arg.create_session.fore_max_request_size  = 8192;
		ops[0].arg.create_session.fore_max_response_size = 8192;
		ops[0].arg.create_session.fore_max_operations    = 64;
		ops[0].arg.create_session.back_max_request_size  = 4096;
		ops[0].arg.create_session.back_max_response_size = 4096;
		ops[0].arg.create_session.back_max_operations    = 2;
		ops[0].arg.create_session.back_max_requests      = 4;

		/* Process CREATE_SESSION. */
		compound_init(&cd);
		cd.cat = db;
		cd.st = st;
		memset(res, 0, sizeof(res));
		nres = compound_process(&cd, ops, res, 1);
		ASSERT_EQ(nres, 1);
		ASSERT_EQ(res[0].status, NFS4_OK);
		ASSERT_EQ(res[0].res.create_session.fore_slots, 16);

		{
			uint8_t sid[SESSION_ID_SIZE];

			memcpy(sid, res[0].res.create_session.session_id,
			       SESSION_ID_SIZE);

			/* Now: SEQUENCE + PUTROOTFH + GETATTR */
			memset(ops, 0, sizeof(ops));

			ops[0].opnum = OP_SEQUENCE;
			memcpy(ops[0].arg.sequence.session_id, sid,
			       SESSION_ID_SIZE);
			ops[0].arg.sequence.slot_id = 0;
			ops[0].arg.sequence.seq_id = 1;
			ops[0].arg.sequence.highest_slot_id = 15;

			ops[1].opnum = OP_PUTROOTFH;

			ops[2].opnum = OP_GETATTR;

			compound_init(&cd);
			cd.cat = db;
			cd.st = st;
			memset(res, 0, sizeof(res));
			nres = compound_process(&cd, ops, res, 3);
			ASSERT_EQ(nres, 3);
			ASSERT_EQ(res[0].status, NFS4_OK);
			ASSERT_EQ(res[1].status, NFS4_OK);
			ASSERT_EQ(res[2].status, NFS4_OK);

			/* Root inode should be fileid 2. */
			ASSERT_EQ(res[2].res.getattr.inode.fileid,
				  MDS_FILEID_ROOT);
			ASSERT_EQ(res[2].res.getattr.inode.type,
				  MDS_FTYPE_DIR);
		}
	}

	session_table_destroy(st);
	mds_catalogue_close(db);
	cleanup_temp_db(path);
	free(path);
}

/* -----------------------------------------------------------------------
 * Test: Compound — SEQUENCE with bad session stops compound
 * ----------------------------------------------------------------------- */

static void test_compound_sequence_bad_session(void)
{
	struct mds_catalogue *db = NULL;
	struct session_table *st = NULL;
	struct compound_data cd;
	char *path;
	struct nfs4_op ops[3];
	struct nfs4_result res[3];
	uint32_t nres;

	path = make_temp_db_path();
	db = open_test_catalogue(); VERIFY(db != NULL);
	ASSERT_EQ(session_table_init(TEST_MDS_ID, 0, &st), 0);

	compound_init(&cd);
	cd.cat = db;
	cd.st = st;

	/* SEQUENCE with bogus session → error, compound stops. */
	memset(ops, 0, sizeof(ops));
	ops[0].opnum = OP_SEQUENCE;
	memset(ops[0].arg.sequence.session_id, 0xFF, SESSION_ID_SIZE);
	ops[0].arg.sequence.slot_id = 0;
	ops[0].arg.sequence.seq_id = 1;

	ops[1].opnum = OP_PUTROOTFH;
	ops[2].opnum = OP_GETATTR;

	memset(res, 0, sizeof(res));
	nres = compound_process(&cd, ops, res, 3);

	/* Should stop at op 0 with NFS4ERR_BADSESSION. */
	ASSERT_EQ(nres, 1);
	ASSERT_EQ(res[0].status, NFS4ERR_BADSESSION);

	session_table_destroy(st);
	mds_catalogue_close(db);
	cleanup_temp_db(path);
	free(path);
}

/* -----------------------------------------------------------------------
 * Test: Compound — replay returns NFS4ERR_RETRY_UNCACHED_REP
 *
 * RFC 5661 §15.1.10.2 / RFC 8881 §2.10.6.2: when the slot's reply is
 * not cached, the server may either (a) reconstruct the reply or (b)
 * return NFS4ERR_RETRY_UNCACHED_REP.  The pnfs-mds session_sequence
 * path takes (b) (driven by pynfs SEQ10b in op_sequence's case 1).
 * NFS4ERR_SEQ_FALSE_RETRY (§15.1.10.6) is reserved for the case where
 * the replay's op-array differs from the cached one; we don't track
 * that distinction so we conservatively always return
 * NFS4ERR_RETRY_UNCACHED_REP for the no-cache replay path.
 * ----------------------------------------------------------------------- */

static void test_compound_replay_seq_false_retry(void)
{
	struct mds_catalogue *db = NULL;
	struct session_table *st = NULL;
	struct compound_data cd;
	char *path;
	struct nfs4_op ops[3];
	struct nfs4_result res[3];
	uint32_t nres;
	uint64_t clientid = 0;
	uint32_t seqid = 0;
	uint8_t session_id[SESSION_ID_SIZE];
	uint32_t fore = 0, back = 0;

	path = make_temp_db_path();
	db = open_test_catalogue(); VERIFY(db != NULL);
	ASSERT_EQ(session_table_init(TEST_MDS_ID, 0, &st), 0);

	/* Set up client + session. */
	ASSERT_EQ(session_exchange_id(st, owner_alice, owner_alice_len,
				      verifier_a, 0,
				      &clientid, &seqid, NULL, 0, 0, 0), 0);
	ASSERT_EQ(session_create_session(st, clientid, seqid,
					 16, 4,
					 0, 0,
					 0, 0,
					 0, 0,
					 1, /* minorversion */
					 0, 0, 0,
					 session_id, &fore, &back, NULL, NULL), 0);

	/* First compound: SEQUENCE(seq_id=1) + PUTROOTFH + GETATTR. */
	memset(ops, 0, sizeof(ops));
	ops[0].opnum = OP_SEQUENCE;
	memcpy(ops[0].arg.sequence.session_id, session_id, SESSION_ID_SIZE);
	ops[0].arg.sequence.slot_id = 0;
	ops[0].arg.sequence.seq_id = 1;
	ops[0].arg.sequence.highest_slot_id = 15;
	ops[1].opnum = OP_PUTROOTFH;
	ops[2].opnum = OP_GETATTR;

	compound_init(&cd);
	cd.cat = db;
	cd.st = st;
	memset(res, 0, sizeof(res));
	nres = compound_process(&cd, ops, res, 3);
	ASSERT_EQ(nres, 3);
	ASSERT_EQ(res[0].status, NFS4_OK);

	/* Replay: same SEQUENCE(seq_id=1) — compound should stop. */
	compound_init(&cd);
	cd.cat = db;
	cd.st = st;
	memset(res, 0, sizeof(res));
	nres = compound_process(&cd, ops, res, 3);
	ASSERT_EQ(nres, 1);  /* stopped at SEQUENCE */
	ASSERT_EQ(res[0].status, NFS4ERR_RETRY_UNCACHED_REP);

	session_table_destroy(st);
	mds_catalogue_close(db);
	cleanup_temp_db(path);
	free(path);
}

/* -----------------------------------------------------------------------
 * Commit 2 — session_destroy_client (RFC 8881 §18.50)
 * ----------------------------------------------------------------------- */

/** DESTROY_CLIENTID on an unknown clientid → -1 (STALE_CLIENTID). */
static void test_destroy_client_stale(void)
{
	struct session_table *st = NULL;

	ASSERT_EQ(session_table_init(TEST_MDS_ID, 0, &st), 0);
	/* No EXCHANGE_ID; clientid 0xDEAD doesn't exist. */
	ASSERT_EQ(session_destroy_client(st, 0xDEAD), -1);
	/* NULL session table also returns -1 (defensive). */
	ASSERT_EQ(session_destroy_client(NULL, 0xDEAD), -1);
	session_table_destroy(st);
}

/** DESTROY_CLIENTID on an unconfirmed client (EXCHANGE_ID only) → 0. */
static void test_destroy_client_unconfirmed(void)
{
	struct session_table *st = NULL;
	uint64_t clientid = 0;
	uint32_t seqid = 0;

	ASSERT_EQ(session_table_init(TEST_MDS_ID, 0, &st), 0);
	ASSERT_EQ(session_exchange_id(st, owner_alice, owner_alice_len,
				      verifier_a, 0,
				      &clientid, &seqid, NULL, 0, 0, 0), 0);
	/* Client exists but has no sessions — destroy is allowed. */
	ASSERT_EQ(session_destroy_client(st, clientid), 0);
	/* Second destroy → STALE_CLIENTID (DESCID8 semantics). */
	ASSERT_EQ(session_destroy_client(st, clientid), -1);
	session_table_destroy(st);
}

/** DESTROY_CLIENTID with a confirmed session → -2 (CLIENTID_BUSY). */
static void test_destroy_client_busy(void)
{
	struct session_table *st = NULL;
	uint64_t clientid = 0;
	uint32_t seqid = 0;
	uint8_t session_id[SESSION_ID_SIZE];
	uint32_t fore = 0, back = 0;

	ASSERT_EQ(session_table_init(TEST_MDS_ID, 0, &st), 0);
	ASSERT_EQ(session_exchange_id(st, owner_alice, owner_alice_len,
				      verifier_a, 0,
				      &clientid, &seqid, NULL, 0, 0, 0), 0);
	ASSERT_EQ(session_create_session(st, clientid, seqid,
					 16, 4,
					 0, 0,
					 0, 0,
					 0, 0,
					 1,
					 0, 0, 0,
					 session_id, &fore, &back,
					 NULL, NULL), 0);
	/* Confirmed client owns the session → BUSY (DESCID5/6). */
	ASSERT_EQ(session_destroy_client(st, clientid), -2);
	/* Tear down the session, then destroy succeeds. */
	ASSERT_EQ(session_destroy_session(st, session_id), 0);
	ASSERT_EQ(session_destroy_client(st, clientid), 0);
	ASSERT_EQ(session_destroy_client(st, clientid), -1);
	session_table_destroy(st);
}

/* -----------------------------------------------------------------------
 * Commit 3 — op_create_session argument validation (RFC 8881 §18.36.3)
 *
 * The compound-level handler must reject:
 *   - csa_flags reserved bits (§18.36.1)        → NFS4ERR_INVAL
 *   - too-small ca_max{request,response}size  → NFS4ERR_TOOSMALL
 *   - zero ca_max{operations,requests}        → NFS4ERR_TOOSMALL
 * BEFORE calling session_create_session, so a rejected request leaves
 * the slot table untouched (CSESS29 sends 10000 of these in a row).
 * ----------------------------------------------------------------------- */

static void seed_create_session_args(struct nfs4_op *op,
				     uint64_t clientid,
				     uint32_t seqid)
{
	memset(op, 0, sizeof(*op));
	op->opnum = OP_CREATE_SESSION;
	op->arg.create_session.clientid = clientid;
	op->arg.create_session.seqid = seqid;
	op->arg.create_session.csa_flags = 0;
	op->arg.create_session.fore_slots = 16;
	op->arg.create_session.back_slots = 4;
	op->arg.create_session.fore_max_request_size  = 8192;
	op->arg.create_session.fore_max_response_size = 8192;
	op->arg.create_session.fore_max_operations    = 64;
	op->arg.create_session.back_max_request_size  = 4096;
	op->arg.create_session.back_max_response_size = 4096;
	op->arg.create_session.back_max_operations    = 2;
	op->arg.create_session.back_max_requests      = 4;
}

static void run_create_session_with_mutator(
	void (*mutate)(struct nfs4_arg_create_session *),
	enum nfs4_status expected)
{
	struct mds_catalogue *db = NULL;
	struct session_table *st = NULL;
	struct compound_data cd;
	char *path;
	struct nfs4_op op;
	struct nfs4_result res;
	uint64_t clientid = 0;
	uint32_t seqid = 0;
	uint32_t nres;

	path = make_temp_db_path();
	db = open_test_catalogue(); VERIFY(db != NULL);
	ASSERT_EQ(session_table_init(TEST_MDS_ID, 0, &st), 0);

	ASSERT_EQ(session_exchange_id(st, owner_alice, owner_alice_len,
				      verifier_a, 0,
				      &clientid, &seqid, NULL, 0, 0, 0), 0);

	seed_create_session_args(&op, clientid, seqid);
	mutate(&op.arg.create_session);

	compound_init(&cd);
	cd.cat = db;
	cd.st = st;
	memset(&res, 0, sizeof(res));
	nres = compound_process(&cd, &op, &res, 1);
	ASSERT_EQ(nres, 1);
	ASSERT_EQ(res.status, expected);

	session_table_destroy(st);
	mds_catalogue_close(db);
	cleanup_temp_db(path);
	free(path);
}

static void mut_csa_flags_reserved(struct nfs4_arg_create_session *a)
{
	a->csa_flags = 0x80;  /* outside 0x7 valid mask */
}

static void mut_fore_maxreq_too_small(struct nfs4_arg_create_session *a)
{
	a->fore_max_request_size = 20;  /* CSESS28 case */
}

static void mut_back_maxreq_too_small(struct nfs4_arg_create_session *a)
{
	a->back_max_request_size = 10;  /* CSESS29 case */
}

static void mut_fore_maxops_zero(struct nfs4_arg_create_session *a)
{
	a->fore_max_operations = 0;
}

static void test_create_session_reserved_flags(void)
{
	run_create_session_with_mutator(mut_csa_flags_reserved,
					NFS4ERR_INVAL);
}

static void test_create_session_fore_maxreq_too_small(void)
{
	run_create_session_with_mutator(mut_fore_maxreq_too_small,
					NFS4ERR_TOOSMALL);
}

static void test_create_session_back_maxreq_too_small(void)
{
	run_create_session_with_mutator(mut_back_maxreq_too_small,
					NFS4ERR_TOOSMALL);
}

static void test_create_session_fore_maxops_zero(void)
{
	run_create_session_with_mutator(mut_fore_maxops_zero,
					NFS4ERR_TOOSMALL);
}

/* -----------------------------------------------------------------------
 * RFC 8881 §18.51 RECLAIM_COMPLETE — per-client one-shot.
 *
 * Pynfs CALLBACK1 testCbNotifyLockExpiredClient regression: a client
 * that arrives AFTER grace ends still must be allowed to send
 * RECLAIM_COMPLETE exactly once and receive NFS4_OK; the second call
 * returns NFS4ERR_COMPLETE_ALREADY.  We validate both:
 *
 *   1. The session.h API directly (test_session_client_reclaim_*).
 *   2. The compound dispatch end-to-end through op_reclaim_complete
 *      (test_compound_reclaim_complete_one_shot) with cd->st set, so
 *      the new per-client path is exercised, not the legacy compat
 *      path.
 * ----------------------------------------------------------------------- */

static void test_session_client_reclaim_null_st(void)
{
	ASSERT_EQ(session_client_reclaim_complete(NULL, 0xDEAD), -1);
}

static void test_session_client_reclaim_unknown_clientid(void)
{
	struct session_table *st = NULL;

	ASSERT_EQ(session_table_init(TEST_MDS_ID, 0, &st), 0);
	/* No EXCHANGE_ID was issued — 0xDEAD is not in the client hash. */
	ASSERT_EQ(session_client_reclaim_complete(st, 0xDEAD), -1);
	session_table_destroy(st);
}

static void test_session_client_reclaim_first_then_already(void)
{
	struct session_table *st = NULL;
	uint64_t clientid = 0;
	uint32_t seqid = 0;

	ASSERT_EQ(session_table_init(TEST_MDS_ID, 0, &st), 0);
	ASSERT_EQ(session_exchange_id(st, owner_alice, owner_alice_len,
				      verifier_a, 0,
				      &clientid, &seqid, NULL, 0, 0, 0), 0);
	/* First call — first-time success. */
	ASSERT_EQ(session_client_reclaim_complete(st, clientid), 0);
	/* Second call — flag already set, COMPLETE_ALREADY signal. */
	ASSERT_EQ(session_client_reclaim_complete(st, clientid), 1);
	/* Third call — still COMPLETE_ALREADY. */
	ASSERT_EQ(session_client_reclaim_complete(st, clientid), 1);
	session_table_destroy(st);
}

static void test_session_client_reclaim_independent_per_client(void)
{
	struct session_table *st = NULL;
	uint64_t cid_alice = 0;
	uint64_t cid_bob = 0;
	uint32_t seqid = 0;

	ASSERT_EQ(session_table_init(TEST_MDS_ID, 0, &st), 0);
	ASSERT_EQ(session_exchange_id(st, owner_alice, owner_alice_len,
				      verifier_a, 0,
				      &cid_alice, &seqid, NULL, 0, 0, 0), 0);
	ASSERT_EQ(session_exchange_id(st, owner_bob, owner_bob_len,
				      verifier_b, 0,
				      &cid_bob, &seqid, NULL, 0, 0, 0), 0);
	ASSERT_NE(cid_alice, cid_bob);

	/* Marking alice does not affect bob (per-client flag). */
	ASSERT_EQ(session_client_reclaim_complete(st, cid_alice), 0);
	ASSERT_EQ(session_client_reclaim_complete(st, cid_bob), 0);
	ASSERT_EQ(session_client_reclaim_complete(st, cid_alice), 1);
	ASSERT_EQ(session_client_reclaim_complete(st, cid_bob), 1);
	session_table_destroy(st);
}

/*
 * End-to-end through op_reclaim_complete.  The compound is
 * [SEQUENCE, RECLAIM_COMPLETE] on the freshly-created session of a
 * confirmed client; we are NOT in grace (grace_init is the default,
 * grace_is_active() returns false).  This is exactly the pynfs
 * CALLBACK1 testCbNotifyLockExpiredClient flow: the very first
 * RECLAIM_COMPLETE must succeed (NFS4_OK), the second must return
 * NFS4ERR_COMPLETE_ALREADY.
 */
static void test_compound_reclaim_complete_one_shot(void)
{
	struct mds_catalogue *db = NULL;
	struct session_table *st = NULL;
	struct compound_data cd;
	char *path;
	struct nfs4_op ops[2];
	struct nfs4_result res[2];
	uint32_t nres;
	uint64_t clientid = 0;
	uint32_t seqid = 0;
	uint8_t session_id[SESSION_ID_SIZE];
	uint32_t fore = 0, back = 0;

	path = make_temp_db_path();
	db = open_test_catalogue(); VERIFY(db != NULL);
	ASSERT_EQ(session_table_init(TEST_MDS_ID, 0, &st), 0);

	ASSERT_EQ(session_exchange_id(st, owner_alice, owner_alice_len,
				      verifier_a, 0,
				      &clientid, &seqid, NULL, 0, 0, 0), 0);
	ASSERT_EQ(session_create_session(st, clientid, seqid,
					 16, 4,
					 0, 0,
					 0, 0,
					 0, 0,
					 1,
					 0, 0, 0,
					 session_id, &fore, &back,
					 NULL, NULL), 0);

	/* First [SEQUENCE, RECLAIM_COMPLETE] — expect NFS4_OK. */
	memset(ops, 0, sizeof(ops));
	ops[0].opnum = OP_SEQUENCE;
	memcpy(ops[0].arg.sequence.session_id, session_id, SESSION_ID_SIZE);
	ops[0].arg.sequence.slot_id = 0;
	ops[0].arg.sequence.seq_id = 1;
	ops[0].arg.sequence.highest_slot_id = 15;
	ops[1].opnum = OP_RECLAIM_COMPLETE;

	compound_init(&cd);
	cd.cat = db;
	cd.st = st;
	cd.minorversion = 1;
	memset(res, 0, sizeof(res));
	nres = compound_process(&cd, ops, res, 2);
	ASSERT_EQ(nres, 2);
	ASSERT_EQ(res[0].status, NFS4_OK);
	ASSERT_EQ(res[1].status, NFS4_OK);

	/* Second [SEQUENCE, RECLAIM_COMPLETE] (bump seq_id) — expect
	 * NFS4ERR_COMPLETE_ALREADY on the RECLAIM_COMPLETE step.  The
	 * SEQUENCE itself still succeeds. */
	ops[0].arg.sequence.seq_id = 2;

	compound_init(&cd);
	cd.cat = db;
	cd.st = st;
	cd.minorversion = 1;
	memset(res, 0, sizeof(res));
	nres = compound_process(&cd, ops, res, 2);
	ASSERT_EQ(nres, 2);
	ASSERT_EQ(res[0].status, NFS4_OK);
	ASSERT_EQ(res[1].status, NFS4ERR_COMPLETE_ALREADY);

	session_table_destroy(st);
	mds_catalogue_close(db);
	cleanup_temp_db(path);
	free(path);
}

/*
 * RFC 5661 §18.51.4 — rca_one_fs=TRUE is per-filesystem and is NOT a
 * one-shot.  Repeated invocations MUST keep returning NFS4_OK and MUST
 * NOT consume the global (rca_one_fs=FALSE) one-shot.  Pynfs RECC1
 * testSupported drives [RECLAIM_COMPLETE(TRUE), RECLAIM_COMPLETE(FALSE)]
 * and expects both to succeed.
 */
static void test_compound_reclaim_complete_one_fs_true(void)
{
	struct mds_catalogue *db = NULL;
	struct session_table *st = NULL;
	struct compound_data cd;
	char *path;
	struct nfs4_op ops[2];
	struct nfs4_result res[2];
	uint32_t nres;
	uint64_t clientid = 0;
	uint32_t seqid = 0;
	uint8_t session_id[SESSION_ID_SIZE];
	uint32_t fore = 0, back = 0;

	path = make_temp_db_path();
	db = open_test_catalogue(); VERIFY(db != NULL);
	ASSERT_EQ(session_table_init(TEST_MDS_ID, 0, &st), 0);

	ASSERT_EQ(session_exchange_id(st, owner_alice, owner_alice_len,
				      verifier_a, 0,
				      &clientid, &seqid, NULL, 0, 0, 0), 0);
	ASSERT_EQ(session_create_session(st, clientid, seqid,
					 16, 4,
					 0, 0,
					 0, 0,
					 0, 0,
					 1,
					 0, 0, 0,
					 session_id, &fore, &back,
					 NULL, NULL), 0);

	memset(ops, 0, sizeof(ops));
	ops[0].opnum = OP_SEQUENCE;
	memcpy(ops[0].arg.sequence.session_id, session_id, SESSION_ID_SIZE);
	ops[0].arg.sequence.slot_id = 0;
	ops[0].arg.sequence.seq_id = 1;
	ops[0].arg.sequence.highest_slot_id = 15;
	ops[1].opnum = OP_RECLAIM_COMPLETE;
	ops[1].arg.reclaim_complete.rca_one_fs = true;

	/* Pass 1: rca_one_fs=TRUE — NFS4_OK. */
	compound_init(&cd);
	cd.cat = db;
	cd.st = st;
	cd.minorversion = 1;
	memset(res, 0, sizeof(res));
	nres = compound_process(&cd, ops, res, 2);
	ASSERT_EQ(nres, 2);
	ASSERT_EQ(res[0].status, NFS4_OK);
	ASSERT_EQ(res[1].status, NFS4_OK);

	/* Pass 2: rca_one_fs=TRUE again — still NFS4_OK
	 * (per-fs reclaim is NOT a one-shot). */
	ops[0].arg.sequence.seq_id = 2;
	compound_init(&cd);
	cd.cat = db;
	cd.st = st;
	cd.minorversion = 1;
	memset(res, 0, sizeof(res));
	nres = compound_process(&cd, ops, res, 2);
	ASSERT_EQ(nres, 2);
	ASSERT_EQ(res[0].status, NFS4_OK);
	ASSERT_EQ(res[1].status, NFS4_OK);

	/* Pass 3: rca_one_fs=FALSE (global) — still NFS4_OK because
	 * the per-fs calls above did NOT consume the global one-shot. */
	ops[0].arg.sequence.seq_id = 3;
	ops[1].arg.reclaim_complete.rca_one_fs = false;
	compound_init(&cd);
	cd.cat = db;
	cd.st = st;
	cd.minorversion = 1;
	memset(res, 0, sizeof(res));
	nres = compound_process(&cd, ops, res, 2);
	ASSERT_EQ(nres, 2);
	ASSERT_EQ(res[0].status, NFS4_OK);
	ASSERT_EQ(res[1].status, NFS4_OK);

	/* Pass 4: rca_one_fs=FALSE again — NFS4ERR_COMPLETE_ALREADY
	 * (the global one-shot has now been consumed by pass 3). */
	ops[0].arg.sequence.seq_id = 4;
	compound_init(&cd);
	cd.cat = db;
	cd.st = st;
	cd.minorversion = 1;
	memset(res, 0, sizeof(res));
	nres = compound_process(&cd, ops, res, 2);
	ASSERT_EQ(nres, 2);
	ASSERT_EQ(res[0].status, NFS4_OK);
	ASSERT_EQ(res[1].status, NFS4ERR_COMPLETE_ALREADY);

	session_table_destroy(st);
	mds_catalogue_close(db);
	cleanup_temp_db(path);
	free(path);
}

/* -----------------------------------------------------------------------
 * Test: Compound — session-bound op without SEQUENCE
 * ----------------------------------------------------------------------- */

static void test_compound_op_without_sequence(void)
{
	struct mds_catalogue *db = NULL;
	struct session_table *st = NULL;
	struct compound_data cd;
	char *path;
	struct nfs4_op ops[2];
	struct nfs4_result res[2];
	uint32_t nres;

	path = make_temp_db_path();
	db = open_test_catalogue(); VERIFY(db != NULL);
	ASSERT_EQ(session_table_init(TEST_MDS_ID, 0, &st), 0);

	/* Send PUTROOTFH without preceding SEQUENCE — should be rejected.
	 * The dispatch's session-binding rule fires only when
	 * minorversion >= 1 (RFC 8881 §2.10.6.2 is v4.1-only); set it
	 * explicitly since compound_init zero-initialises minorversion. */
	compound_init(&cd);
	cd.cat = db;
	cd.st = st;
	cd.minorversion = 1;

	memset(ops, 0, sizeof(ops));
	ops[0].opnum = OP_PUTROOTFH;
	ops[1].opnum = OP_GETATTR;

	memset(res, 0, sizeof(res));
	nres = compound_process(&cd, ops, res, 2);
	ASSERT_EQ(nres, 1);  /* stopped at first op */
	ASSERT_EQ(res[0].status, NFS4ERR_OP_NOT_IN_SESSION);

	session_table_destroy(st);
	mds_catalogue_close(db);
	cleanup_temp_db(path);
	free(path);
}

/* -----------------------------------------------------------------------
 * main
 * ----------------------------------------------------------------------- */

int main(void)
{
	fprintf(stdout, "Running session/clientid tests:\n");

	/* Direct session.h API tests */
	RUN_TEST(test_exchange_id_new_client);
	RUN_TEST(test_exchange_id_same_verifier);
	RUN_TEST(test_exchange_id_new_verifier);
	RUN_TEST(test_exchange_id_two_clients);
	RUN_TEST(test_create_session_basic);
	RUN_TEST(test_create_session_stale_clientid);
	RUN_TEST(test_create_session_bad_seqid);
	RUN_TEST(test_create_session_slot_cap);
	RUN_TEST(test_destroy_session);
	RUN_TEST(test_sequence_valid);
	RUN_TEST(test_sequence_replay);
	RUN_TEST(test_sequence_misordered);
	RUN_TEST(test_sequence_bad_session);
	RUN_TEST(test_sequence_bad_slot);

	/* Compound integration tests */
	RUN_TEST(test_compound_full_flow);
	RUN_TEST(test_compound_sequence_bad_session);
	RUN_TEST(test_compound_replay_seq_false_retry);
	RUN_TEST(test_compound_op_without_sequence);

	/* Commit 2 — session_destroy_client (RFC 8881 §18.50) */
	RUN_TEST(test_destroy_client_stale);
	RUN_TEST(test_destroy_client_unconfirmed);
	RUN_TEST(test_destroy_client_busy);

	/* Commit 3 — op_create_session validation (RFC 8881 §18.36.3) */
	RUN_TEST(test_create_session_reserved_flags);
	RUN_TEST(test_create_session_fore_maxreq_too_small);
	RUN_TEST(test_create_session_back_maxreq_too_small);
	RUN_TEST(test_create_session_fore_maxops_zero);

	/* RFC 8881 §18.51 RECLAIM_COMPLETE per-client one-shot */
	RUN_TEST(test_session_client_reclaim_null_st);
	RUN_TEST(test_session_client_reclaim_unknown_clientid);
	RUN_TEST(test_session_client_reclaim_first_then_already);
	RUN_TEST(test_session_client_reclaim_independent_per_client);
	RUN_TEST(test_compound_reclaim_complete_one_shot);
	RUN_TEST(test_compound_reclaim_complete_one_fs_true);

	fprintf(stdout, "\n%d/%d tests passed.\n", tests_passed, tests_run);
	return (tests_passed == tests_run) ? 0 : 1;
}
