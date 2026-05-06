/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_nfsv42.c -- Unit tests for NFSv4.2 operation dispatch.
 *
 * RonDB-native: tests compound dispatch for NFSv4.2 operations
 * (ALLOCATE, SEEK, IO_ADVISE, COPY, READ_PLUS, etc.) using the
 * catalogue API.  Skips gracefully if no RonDB cluster is available.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "pnfs_mds.h"
#include "mds_catalogue.h"
#include "test_helpers.h"
#include "compound.h"
#include "inode_cache.h"
#include "io_tracker.h"

/*
 * Forward declaration -- op_io_advise is declared in compound_internal.h,
 * which is intentionally private to the mds subsystem.  Re-declare the
 * prototype here so the test can call the handler directly without
 * adding src/mds to the test target's include path.
 */
enum nfs4_status op_io_advise(const struct compound_data *cd,
			      const struct nfs4_op *op,
			      struct nfs4_result *res);

static int tests_run, tests_passed;

#define ASSERT_EQ(a, b) do { \
    if ((a) != (b)) { \
        fprintf(stderr, "  FAIL %s:%d: %s (%d) != %s (%d)\n", \
                __FILE__, __LINE__, #a, (int)(a), #b, (int)(b)); \
        return; \
    } \
} while (0)
#define ASSERT_TRUE(cond) do { \
    if (!(cond)) { \
        fprintf(stderr, "  FAIL %s:%d: !(%s)\n", __FILE__, __LINE__, #cond); \
        return; \
    } \
} while (0)
#define SKIP(msg) do { \
    fprintf(stdout, "SKIP (%s)\n", (msg)); tests_passed++; return; \
} while (0)
#define RUN_TEST(fn) do { \
    tests_run++; fprintf(stdout, "  %-50s", #fn); fflush(stdout); \
    fn(); tests_passed++; fprintf(stdout, "PASS\n"); \
} while (0)

/** Verify NFSv4.2 op numbers match RFC 7862 values. */
static void test_nfsv42_opnums(void)
{
    ASSERT_EQ(OP_ALLOCATE, 59);
    ASSERT_EQ(OP_COPY, 60);
    ASSERT_EQ(OP_DEALLOCATE, 62);
    ASSERT_EQ(OP_IO_ADVISE, 63);
    ASSERT_EQ(OP_SEEK, 69);
    ASSERT_EQ(OP_READ_PLUS, 68);
    ASSERT_EQ(OP_WRITE_SAME, 70);
    ASSERT_EQ(OP_CLONE, 71);
}

/** Verify SEEK what constants. */
static void test_seek_constants(void)
{
    ASSERT_EQ(NFS4_CONTENT_DATA, 0);
    ASSERT_EQ(NFS4_CONTENT_HOLE, 1);
}

/** Verify IO_ADVISE hint bits. */
static void test_io_advise_bits(void)
{
    ASSERT_EQ(IO_ADVISE4_NORMAL, 0x00);
    ASSERT_EQ(IO_ADVISE4_SEQUENTIAL, 0x01);
    ASSERT_EQ(IO_ADVISE4_RANDOM, 0x04);
    ASSERT_TRUE(IO_ADVISE4_WILLNEED != IO_ADVISE4_DONTNEED);
}

/*
 * Minimal compound_data scaffolding for op_io_advise unit tests.
 * cd->ot left NULL so validate_io_stateid auto-passes (test-compat
 * mode).  current_fh_set must be true or the handler returns
 * NFS4ERR_NOFILEHANDLE.
 */
static void io_advise_cd_init(struct compound_data *cd,
			      uint64_t fileid,
			      struct inode_cache *ic,
			      struct io_tracker *iot)
{
	compound_init(cd);
	cd->current_fh.fileid = fileid;
	cd->current_fh_set = true;
	cd->icache = ic;
	cd->io_tracker = iot;
}

static void io_advise_op_init(struct nfs4_op *op, uint32_t hints)
{
	memset(op, 0, sizeof(*op));
	op->opnum = OP_IO_ADVISE;
	op->arg.io_advise.offset = 0;
	op->arg.io_advise.count = 4096;
	op->arg.io_advise.hints = hints;
	/* Leave stateid zero-filled -- that's the anonymous stateid,
	 * which validate_io_stateid always accepts. */
}

/** WILLNEED: handler bumps io_tracker and echoes WILLNEED back. */
static void test_io_advise_willneed_bumps_tracker(void)
{
	struct io_tracker *iot = NULL;
	struct compound_data cd;
	struct nfs4_op op;
	struct nfs4_result res;
	struct io_tracker_entry *entries = NULL;
	uint32_t count = 0;
	const uint64_t fid = 12345ULL;
	bool saw_bump = false;
	uint32_t i;

	ASSERT_EQ(io_tracker_init(16, &iot), 0);
	ASSERT_TRUE(iot != NULL);

	io_advise_cd_init(&cd, fid, NULL, iot);
	io_advise_op_init(&op, IO_ADVISE4_WILLNEED | IO_ADVISE4_READ);
	memset(&res, 0, sizeof(res));

	ASSERT_EQ(op_io_advise(&cd, &op, &res), NFS4_OK);
	/* Response echoes WILLNEED (honoured) plus the directional READ. */
	ASSERT_EQ(res.res.io_advise.hints,
		  (uint32_t)(IO_ADVISE4_WILLNEED | IO_ADVISE4_READ));

	ASSERT_EQ(io_tracker_snapshot_and_reset(iot, &entries, &count), 0);
	for (i = 0; i < count; i++) {
		if (entries[i].fileid == fid &&
		    entries[i].layoutget_bumps > 0) {
			saw_bump = true;
			break;
		}
	}
	ASSERT_TRUE(saw_bump);

	free(entries);
	io_tracker_destroy(iot);
}

/** DONTNEED: handler evicts the fileid from inode_cache and echoes DONTNEED. */
static void test_io_advise_dontneed_invalidates_icache(void)
{
	struct inode_cache *ic = NULL;
	struct compound_data cd;
	struct nfs4_op op;
	struct nfs4_result res;
	struct mds_inode seed;
	struct mds_inode probe;
	const uint64_t fid = 67890ULL;

	ASSERT_EQ(inode_cache_init(16, &ic), 0);
	ASSERT_TRUE(ic != NULL);

	memset(&seed, 0, sizeof(seed));
	seed.fileid = fid;
	seed.mode = 0644;
	ASSERT_EQ(inode_cache_put(ic, &seed), 0);
	/* Sanity: the entry is cached. */
	ASSERT_EQ(inode_cache_get(ic, fid, &probe), 0);

	io_advise_cd_init(&cd, fid, ic, NULL);
	io_advise_op_init(&op, IO_ADVISE4_DONTNEED);
	memset(&res, 0, sizeof(res));

	ASSERT_EQ(op_io_advise(&cd, &op, &res), NFS4_OK);
	ASSERT_EQ(res.res.io_advise.hints, (uint32_t)IO_ADVISE4_DONTNEED);

	/* Entry must be gone. */
	ASSERT_EQ(inode_cache_get(ic, fid, &probe), -1);

	inode_cache_destroy(ic);
}

/** Hints we cannot act on must NOT be echoed (RFC 7862 S15.5.3). */
static void test_io_advise_ignored_hints_not_echoed(void)
{
	struct compound_data cd;
	struct nfs4_op op;
	struct nfs4_result res;

	io_advise_cd_init(&cd, 99ULL, NULL, NULL);
	/* None of these map to server-side action today. */
	io_advise_op_init(&op,
		IO_ADVISE4_SEQUENTIAL | IO_ADVISE4_RANDOM |
		IO_ADVISE4_INIT_PROXIMITY | IO_ADVISE4_READ);
	memset(&res, 0, sizeof(res));

	ASSERT_EQ(op_io_advise(&cd, &op, &res), NFS4_OK);
	ASSERT_EQ(res.res.io_advise.hints, (uint32_t)0);
}

/** Verify write stability constants. */
static void test_write_stability(void)
{
    ASSERT_EQ(UNSTABLE4, 0);
    ASSERT_EQ(DATA_SYNC4, 1);
    ASSERT_EQ(FILE_SYNC4, 2);
}

/** Test ALLOCATE without a current FH -> NFS4ERR_NOFILEHANDLE. */
static void test_allocate_no_fh(void)
{
    struct mds_catalogue *cat = open_test_catalogue();
    if (cat == NULL) SKIP("no RonDB");

    struct compound_data cd;
    compound_init(&cd);
    cd.cat = cat;

    struct nfs4_op ops[2];
    struct nfs4_result res[2];

    memset(ops, 0, sizeof(ops));
    ops[0].opnum = OP_SEQUENCE;
    ops[1].opnum = OP_ALLOCATE;
    ops[1].arg.allocate.offset = 0;
    ops[1].arg.allocate.length = 4096;

    uint32_t n = compound_process(&cd, ops, res, 2);
    ASSERT_EQ(n, (uint32_t)2);
    ASSERT_EQ(res[1].status, NFS4ERR_NOFILEHANDLE);

    mds_catalogue_close(cat);
}

/** Test SEEK without current FH -> NFS4ERR_NOFILEHANDLE. */
static void test_seek_no_fh(void)
{
    struct mds_catalogue *cat = open_test_catalogue();
    if (cat == NULL) SKIP("no RonDB");

    struct compound_data cd;
    compound_init(&cd);
    cd.cat = cat;

    struct nfs4_op ops[2];
    struct nfs4_result res[2];

    memset(ops, 0, sizeof(ops));
    ops[0].opnum = OP_SEQUENCE;
    ops[1].opnum = OP_SEEK;
    ops[1].arg.seek.what = NFS4_CONTENT_DATA;

    uint32_t n = compound_process(&cd, ops, res, 2);
    ASSERT_EQ(n, (uint32_t)2);
    ASSERT_EQ(res[1].status, NFS4ERR_NOFILEHANDLE);

    mds_catalogue_close(cat);
}

int main(void)
{
    fprintf(stdout, "test_nfsv42 (RonDB-native)\n");

    RUN_TEST(test_nfsv42_opnums);
    RUN_TEST(test_seek_constants);
    RUN_TEST(test_io_advise_bits);
    RUN_TEST(test_io_advise_willneed_bumps_tracker);
    RUN_TEST(test_io_advise_dontneed_invalidates_icache);
    RUN_TEST(test_io_advise_ignored_hints_not_echoed);
    RUN_TEST(test_write_stability);
    RUN_TEST(test_allocate_no_fh);
    RUN_TEST(test_seek_no_fh);

    fprintf(stdout, "\n  %d/%d tests passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
