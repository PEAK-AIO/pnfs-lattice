/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_inode_cache.c — Unit tests for the in-memory inode LRU cache.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "pnfs_mds.h"
#include "inode_cache.h"

/* -----------------------------------------------------------------------
 * Test helpers (catalogue test helpers)
 * ----------------------------------------------------------------------- */

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

/** Build a minimal test inode with the given fileid and size. */
static struct mds_inode make_inode(uint64_t fileid, uint64_t size)
{
	struct mds_inode inode;

	memset(&inode, 0, sizeof(inode));
	inode.fileid = fileid;
	inode.type   = MDS_FTYPE_REG;
	inode.mode   = 0644;
	inode.nlink  = 1;
	inode.size   = size;
	inode.change = 1;
	return inode;
}

/* -----------------------------------------------------------------------
 * test_init_destroy — basic lifecycle
 * ----------------------------------------------------------------------- */

static void test_init_destroy(void)
{
	struct inode_cache *ic = NULL;

	ASSERT_EQ(inode_cache_init(128, &ic), 0);
	ASSERT_NE(ic, NULL);
	ASSERT_EQ(inode_cache_count(ic), (uint32_t)0);

	inode_cache_destroy(ic);

	/* NULL-safe destroy. */
	inode_cache_destroy(NULL);

	/* Invalid args. */
	ASSERT_EQ(inode_cache_init(0, &ic), -1);
	ASSERT_EQ(inode_cache_init(10, NULL), -1);
}

/* -----------------------------------------------------------------------
 * test_put_get — round-trip insert and retrieval
 * ----------------------------------------------------------------------- */

static void test_put_get(void)
{
	struct inode_cache *ic = NULL;
	struct mds_inode in, out;

	ASSERT_EQ(inode_cache_init(64, &ic), 0);

	in = make_inode(100, 4096);
	ASSERT_EQ(inode_cache_put(ic, &in), 0);
	ASSERT_EQ(inode_cache_count(ic), (uint32_t)1);

	memset(&out, 0xff, sizeof(out));
	ASSERT_EQ(inode_cache_get(ic, 100, &out), 0);
	ASSERT_EQ(out.fileid, (uint64_t)100);
	ASSERT_EQ(out.size, (uint64_t)4096);
	ASSERT_EQ(out.mode, (uint32_t)0644);

	/* Miss on non-existent fileid. */
	ASSERT_EQ(inode_cache_get(ic, 999, &out), -1);

	inode_cache_destroy(ic);
}

/* -----------------------------------------------------------------------
 * test_update — put same fileid twice updates data
 * ----------------------------------------------------------------------- */

static void test_update(void)
{
	struct inode_cache *ic = NULL;
	struct mds_inode in, out;

	ASSERT_EQ(inode_cache_init(64, &ic), 0);

	in = make_inode(42, 100);
	ASSERT_EQ(inode_cache_put(ic, &in), 0);

	/* Update size. */
	in.size = 999;
	in.change = 2;
	ASSERT_EQ(inode_cache_put(ic, &in), 0);

	/* Count stays 1 (update, not second insert). */
	ASSERT_EQ(inode_cache_count(ic), (uint32_t)1);

	ASSERT_EQ(inode_cache_get(ic, 42, &out), 0);
	ASSERT_EQ(out.size, (uint64_t)999);
	ASSERT_EQ(out.change, (uint64_t)2);

	inode_cache_destroy(ic);
}

/* -----------------------------------------------------------------------
 * test_invalidate — remove an entry
 * ----------------------------------------------------------------------- */

static void test_invalidate(void)
{
	struct inode_cache *ic = NULL;
	struct mds_inode in, out;

	ASSERT_EQ(inode_cache_init(64, &ic), 0);

	in = make_inode(10, 0);
	ASSERT_EQ(inode_cache_put(ic, &in), 0);
	in = make_inode(20, 0);
	ASSERT_EQ(inode_cache_put(ic, &in), 0);
	ASSERT_EQ(inode_cache_count(ic), (uint32_t)2);

	inode_cache_invalidate(ic, 10);
	ASSERT_EQ(inode_cache_count(ic), (uint32_t)1);
	ASSERT_EQ(inode_cache_get(ic, 10, &out), -1); /* gone */
	ASSERT_EQ(inode_cache_get(ic, 20, &out), 0);  /* still there */

	/* Invalidating non-existent fileid is a no-op. */
	inode_cache_invalidate(ic, 9999);
	ASSERT_EQ(inode_cache_count(ic), (uint32_t)1);

	inode_cache_destroy(ic);
}

/* -----------------------------------------------------------------------
 * test_lru_eviction — exceeding capacity evicts oldest
 * ----------------------------------------------------------------------- */

static void test_lru_eviction(void)
{
	struct inode_cache *ic = NULL;
	struct mds_inode in, out;

	/* Cache holds only 3 entries. */
	ASSERT_EQ(inode_cache_init(3, &ic), 0);

	/* Insert fileids 1, 2, 3 (LRU order: 3=MRU, 1=LRU). */
	in = make_inode(1, 0);
	ASSERT_EQ(inode_cache_put(ic, &in), 0);
	in = make_inode(2, 0);
	ASSERT_EQ(inode_cache_put(ic, &in), 0);
	in = make_inode(3, 0);
	ASSERT_EQ(inode_cache_put(ic, &in), 0);
	ASSERT_EQ(inode_cache_count(ic), (uint32_t)3);

	/* Insert fileid 4 — should evict fileid 1 (LRU tail). */
	in = make_inode(4, 0);
	ASSERT_EQ(inode_cache_put(ic, &in), 0);
	ASSERT_EQ(inode_cache_count(ic), (uint32_t)3);

	ASSERT_EQ(inode_cache_get(ic, 1, &out), -1); /* evicted */
	ASSERT_EQ(inode_cache_get(ic, 2, &out), 0);  /* still here */
	ASSERT_EQ(inode_cache_get(ic, 3, &out), 0);
	ASSERT_EQ(inode_cache_get(ic, 4, &out), 0);

	inode_cache_destroy(ic);
}

/* -----------------------------------------------------------------------
 * test_lru_promote_on_get — get promotes entry, saving it from eviction
 * ----------------------------------------------------------------------- */

static void test_lru_promote_on_get(void)
{
	struct inode_cache *ic = NULL;
	struct mds_inode in, out;

	ASSERT_EQ(inode_cache_init(3, &ic), 0);

	/* Insert 1, 2, 3. LRU: 3-MRU ... 1-LRU. */
	in = make_inode(1, 0);
	ASSERT_EQ(inode_cache_put(ic, &in), 0);
	in = make_inode(2, 0);
	ASSERT_EQ(inode_cache_put(ic, &in), 0);
	in = make_inode(3, 0);
	ASSERT_EQ(inode_cache_put(ic, &in), 0);

	/* Touch fileid 1 — promotes it to MRU. LRU: 1-MRU ... 2-LRU. */
	ASSERT_EQ(inode_cache_get(ic, 1, &out), 0);

	/* Insert fileid 4 — now fileid 2 is LRU and should be evicted. */
	in = make_inode(4, 0);
	ASSERT_EQ(inode_cache_put(ic, &in), 0);

	ASSERT_EQ(inode_cache_get(ic, 1, &out), 0);  /* promoted, safe */
	ASSERT_EQ(inode_cache_get(ic, 2, &out), -1); /* evicted */
	ASSERT_EQ(inode_cache_get(ic, 3, &out), 0);
	ASSERT_EQ(inode_cache_get(ic, 4, &out), 0);

	inode_cache_destroy(ic);
}

/* -----------------------------------------------------------------------
 * test_lru_promote_on_put — updating an entry promotes it
 * ----------------------------------------------------------------------- */

static void test_lru_promote_on_put(void)
{
	struct inode_cache *ic = NULL;
	struct mds_inode in, out;

	ASSERT_EQ(inode_cache_init(3, &ic), 0);

	in = make_inode(1, 10);
	ASSERT_EQ(inode_cache_put(ic, &in), 0);
	in = make_inode(2, 20);
	ASSERT_EQ(inode_cache_put(ic, &in), 0);
	in = make_inode(3, 30);
	ASSERT_EQ(inode_cache_put(ic, &in), 0);

	/* Update fileid 1 (promotes to MRU). */
	in = make_inode(1, 99);
	ASSERT_EQ(inode_cache_put(ic, &in), 0);

	/* Insert fileid 4 — fileid 2 is now LRU tail. */
	in = make_inode(4, 40);
	ASSERT_EQ(inode_cache_put(ic, &in), 0);

	ASSERT_EQ(inode_cache_get(ic, 2, &out), -1); /* evicted */
	ASSERT_EQ(inode_cache_get(ic, 1, &out), 0);
	ASSERT_EQ(out.size, (uint64_t)99); /* updated value */

	inode_cache_destroy(ic);
}

/* -----------------------------------------------------------------------
 * test_many_entries — bulk insert and retrieval
 * ----------------------------------------------------------------------- */

static void test_many_entries(void)
{
	struct inode_cache *ic = NULL;
	struct mds_inode in, out;
	uint32_t i;
	uint32_t cap = 256;

	ASSERT_EQ(inode_cache_init(cap, &ic), 0);

	for (i = 0; i < cap; i++) {
		in = make_inode(1000 + i, i * 10);
		ASSERT_EQ(inode_cache_put(ic, &in), 0);
	}
	ASSERT_EQ(inode_cache_count(ic), cap);

	/* Every entry should be retrievable. */
	for (i = 0; i < cap; i++) {
		ASSERT_EQ(inode_cache_get(ic, 1000 + i, &out), 0);
		ASSERT_EQ(out.fileid, (uint64_t)(1000 + i));
		ASSERT_EQ(out.size, (uint64_t)(i * 10));
	}

	/* One more evicts the LRU tail (fileid 1000). */
	in = make_inode(9999, 0);
	ASSERT_EQ(inode_cache_put(ic, &in), 0);
	ASSERT_EQ(inode_cache_count(ic), cap);
	ASSERT_EQ(inode_cache_get(ic, 1000, &out), -1);

	inode_cache_destroy(ic);
}

/* -----------------------------------------------------------------------
 * test_single_entry_cache — edge case: max_entries = 1
 * ----------------------------------------------------------------------- */

static void test_single_entry_cache(void)
{
	struct inode_cache *ic = NULL;
	struct mds_inode in, out;

	ASSERT_EQ(inode_cache_init(1, &ic), 0);

	in = make_inode(10, 100);
	ASSERT_EQ(inode_cache_put(ic, &in), 0);
	ASSERT_EQ(inode_cache_get(ic, 10, &out), 0);

	/* Second insert evicts the only entry. */
	in = make_inode(20, 200);
	ASSERT_EQ(inode_cache_put(ic, &in), 0);
	ASSERT_EQ(inode_cache_count(ic), (uint32_t)1);
	ASSERT_EQ(inode_cache_get(ic, 10, &out), -1);
	ASSERT_EQ(inode_cache_get(ic, 20, &out), 0);

	inode_cache_destroy(ic);
}

/* -----------------------------------------------------------------------
 * main
 * ----------------------------------------------------------------------- */

int main(void)
{
	fprintf(stdout, "Running inode cache tests:\n");

	RUN_TEST(test_init_destroy);
	RUN_TEST(test_put_get);
	RUN_TEST(test_update);
	RUN_TEST(test_invalidate);
	RUN_TEST(test_lru_eviction);
	RUN_TEST(test_lru_promote_on_get);
	RUN_TEST(test_lru_promote_on_put);
	RUN_TEST(test_many_entries);
	RUN_TEST(test_single_entry_cache);

	fprintf(stdout, "\n%d/%d tests passed.\n", tests_passed, tests_run);
	return (tests_passed == tests_run) ? 0 : 1;
}
