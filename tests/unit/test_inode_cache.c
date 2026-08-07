/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_inode_cache.c -- Unit tests for the in-memory inode LRU cache.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>  /* usleep() for TTL tests */

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
 * Stripe-aware fileid helpers (Wave 3 T3.2)
 *
 * The cache is 16-way striped by splitmix64(fileid) % 16 with the
 * capacity divided across stripes, so LRU eviction is stripe-local.
 * Eviction-order tests must therefore use fileids that collide into
 * the SAME stripe.  These helpers mirror the stripe selection in
 * src/mds/inode_cache.c (IC_STRIPES / ic_stripe_idx).
 * ----------------------------------------------------------------------- */

#define IC_TEST_STRIPES 16

static uint64_t test_splitmix64(uint64_t x)
{
	x ^= x >> 30;
	x *= 0xbf58476d1ce4e5b9ULL;
	x ^= x >> 27;
	x *= 0x94d049bb133111ebULL;
	x ^= x >> 31;
	return x;
}

static uint32_t test_stripe_of(uint64_t fileid)
{
	return (uint32_t)(test_splitmix64(fileid) % IC_TEST_STRIPES);
}

/**
 * Fill @ids with @n distinct fileids >= @base that all map to the
 * same stripe as @base.
 */
static void pick_same_stripe_ids(uint64_t base, uint64_t *ids, uint32_t n)
{
	uint32_t stripe = test_stripe_of(base);
	uint32_t found = 0;
	uint64_t cand = base;

	while (found < n) {
		if (test_stripe_of(cand) == stripe) {
			ids[found++] = cand;
		}
		cand++;
	}
}

/* -----------------------------------------------------------------------
 * test_init_destroy -- basic lifecycle
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
 * test_put_get -- round-trip insert and retrieval
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
 * test_update -- put same fileid twice updates data
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
 * test_invalidate -- remove an entry
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
 * test_lru_eviction -- exceeding a stripe's capacity evicts its oldest
 * ----------------------------------------------------------------------- */

static void test_lru_eviction(void)
{
	struct inode_cache *ic = NULL;
	struct mds_inode in, out;
	uint64_t ids[4];

	/* 48 total -> 3 entries per stripe; all four test fileids land
	 * in ONE stripe so the eviction order is deterministic. */
	ASSERT_EQ(inode_cache_init(48, &ic), 0);
	pick_same_stripe_ids(1, ids, 4);

	/* Insert ids[0..2] (stripe LRU order: [2]=MRU, [0]=LRU). */
	in = make_inode(ids[0], 0);
	ASSERT_EQ(inode_cache_put(ic, &in), 0);
	in = make_inode(ids[1], 0);
	ASSERT_EQ(inode_cache_put(ic, &in), 0);
	in = make_inode(ids[2], 0);
	ASSERT_EQ(inode_cache_put(ic, &in), 0);
	ASSERT_EQ(inode_cache_count(ic), (uint32_t)3);

	/* Insert ids[3] -- should evict ids[0] (stripe LRU tail). */
	in = make_inode(ids[3], 0);
	ASSERT_EQ(inode_cache_put(ic, &in), 0);
	ASSERT_EQ(inode_cache_count(ic), (uint32_t)3);

	ASSERT_EQ(inode_cache_get(ic, ids[0], &out), -1); /* evicted */
	ASSERT_EQ(inode_cache_get(ic, ids[1], &out), 0);  /* still here */
	ASSERT_EQ(inode_cache_get(ic, ids[2], &out), 0);
	ASSERT_EQ(inode_cache_get(ic, ids[3], &out), 0);

	inode_cache_destroy(ic);
}

/* -----------------------------------------------------------------------
 * test_lru_promote_on_get -- get promotes entry, saving it from eviction
 * ----------------------------------------------------------------------- */

static void test_lru_promote_on_get(void)
{
	struct inode_cache *ic = NULL;
	struct mds_inode in, out;
	uint64_t ids[4];

	/* 48 total -> 3 per stripe; same-stripe ids (see T3.2 note). */
	ASSERT_EQ(inode_cache_init(48, &ic), 0);
	pick_same_stripe_ids(1, ids, 4);

	/* Insert ids[0..2]. Stripe LRU: [2]-MRU ... [0]-LRU. */
	in = make_inode(ids[0], 0);
	ASSERT_EQ(inode_cache_put(ic, &in), 0);
	in = make_inode(ids[1], 0);
	ASSERT_EQ(inode_cache_put(ic, &in), 0);
	in = make_inode(ids[2], 0);
	ASSERT_EQ(inode_cache_put(ic, &in), 0);

	/* Touch ids[0] -- promotes it to MRU. LRU: [0]-MRU ... [1]-LRU. */
	ASSERT_EQ(inode_cache_get(ic, ids[0], &out), 0);

	/* Insert ids[3] -- now ids[1] is LRU and should be evicted. */
	in = make_inode(ids[3], 0);
	ASSERT_EQ(inode_cache_put(ic, &in), 0);

	ASSERT_EQ(inode_cache_get(ic, ids[0], &out), 0);  /* promoted, safe */
	ASSERT_EQ(inode_cache_get(ic, ids[1], &out), -1); /* evicted */
	ASSERT_EQ(inode_cache_get(ic, ids[2], &out), 0);
	ASSERT_EQ(inode_cache_get(ic, ids[3], &out), 0);

	inode_cache_destroy(ic);
}

/* -----------------------------------------------------------------------
 * test_lru_promote_on_put -- updating an entry promotes it
 * ----------------------------------------------------------------------- */

static void test_lru_promote_on_put(void)
{
	struct inode_cache *ic = NULL;
	struct mds_inode in, out;
	uint64_t ids[4];

	/* 48 total -> 3 per stripe; same-stripe ids (see T3.2 note). */
	ASSERT_EQ(inode_cache_init(48, &ic), 0);
	pick_same_stripe_ids(1, ids, 4);

	in = make_inode(ids[0], 10);
	ASSERT_EQ(inode_cache_put(ic, &in), 0);
	in = make_inode(ids[1], 20);
	ASSERT_EQ(inode_cache_put(ic, &in), 0);
	in = make_inode(ids[2], 30);
	ASSERT_EQ(inode_cache_put(ic, &in), 0);

	/* Update ids[0] (promotes to MRU). */
	in = make_inode(ids[0], 99);
	ASSERT_EQ(inode_cache_put(ic, &in), 0);

	/* Insert ids[3] -- ids[1] is now the stripe's LRU tail. */
	in = make_inode(ids[3], 40);
	ASSERT_EQ(inode_cache_put(ic, &in), 0);

	ASSERT_EQ(inode_cache_get(ic, ids[1], &out), -1); /* evicted */
	ASSERT_EQ(inode_cache_get(ic, ids[0], &out), 0);
	ASSERT_EQ(out.size, (uint64_t)99); /* updated value */

	inode_cache_destroy(ic);
}

/* -----------------------------------------------------------------------
 * test_many_entries -- bulk insert and retrieval
 * ----------------------------------------------------------------------- */

static void test_many_entries(void)
{
	struct inode_cache *ic = NULL;
	struct mds_inode in, out;
	uint64_t ids[256];
	uint64_t extra;
	uint32_t fill[IC_TEST_STRIPES];
	uint32_t per_stripe;
	uint32_t i;
	uint32_t cap = 256;
	uint64_t cand;

	ASSERT_EQ(inode_cache_init(cap, &ic), 0);
	per_stripe = cap / IC_TEST_STRIPES; /* 16 */

	/* Pick exactly per_stripe fileids for EVERY stripe so the total
	 * budget is filled without any stripe overflowing (capacity is
	 * enforced per stripe, not globally). */
	memset(fill, 0, sizeof(fill));
	i = 0;
	cand = 1000;
	while (i < cap) {
		uint32_t s = test_stripe_of(cand);

		if (fill[s] < per_stripe) {
			fill[s]++;
			ids[i++] = cand;
		}
		cand++;
	}

	for (i = 0; i < cap; i++) {
		in = make_inode(ids[i], i * 10);
		ASSERT_EQ(inode_cache_put(ic, &in), 0);
	}
	ASSERT_EQ(inode_cache_count(ic), cap);

	/* Every entry should be retrievable.  Gets run in insertion
	 * order, so each stripe's relative LRU order is preserved. */
	for (i = 0; i < cap; i++) {
		ASSERT_EQ(inode_cache_get(ic, ids[i], &out), 0);
		ASSERT_EQ(out.fileid, ids[i]);
		ASSERT_EQ(out.size, (uint64_t)(i * 10));
	}

	/* One more id in ids[0]'s stripe evicts that stripe's LRU tail,
	 * which is ids[0] (the first insert into that stripe). */
	extra = cand;
	while (test_stripe_of(extra) != test_stripe_of(ids[0])) {
		extra++;
	}
	in = make_inode(extra, 0);
	ASSERT_EQ(inode_cache_put(ic, &in), 0);
	ASSERT_EQ(inode_cache_count(ic), cap);
	ASSERT_EQ(inode_cache_get(ic, ids[0], &out), -1);

	inode_cache_destroy(ic);
}

/* -----------------------------------------------------------------------
 * test_single_entry_cache -- edge case: max_entries = 1
 * ----------------------------------------------------------------------- */

static void test_single_entry_cache(void)
{
	struct inode_cache *ic = NULL;
	struct mds_inode in, out;
	uint64_t ids[2];

	/* max_entries=1 -> one entry per stripe; use two ids in the SAME
	 * stripe so the second insert must evict the first. */
	ASSERT_EQ(inode_cache_init(1, &ic), 0);
	pick_same_stripe_ids(10, ids, 2);

	in = make_inode(ids[0], 100);
	ASSERT_EQ(inode_cache_put(ic, &in), 0);
	ASSERT_EQ(inode_cache_get(ic, ids[0], &out), 0);

	/* Second insert evicts the stripe's only entry. */
	in = make_inode(ids[1], 200);
	ASSERT_EQ(inode_cache_put(ic, &in), 0);
	ASSERT_EQ(inode_cache_count(ic), (uint32_t)1);
	ASSERT_EQ(inode_cache_get(ic, ids[0], &out), -1);
	ASSERT_EQ(inode_cache_get(ic, ids[1], &out), 0);

	inode_cache_destroy(ic);
}

/* -----------------------------------------------------------------------
 * test_ttl_expiry -- a positive entry older than ttl_ms is a miss
 * ----------------------------------------------------------------------- */

static void test_ttl_expiry(void)
{
	struct inode_cache *ic = NULL;
	struct mds_inode in, out;

	ASSERT_EQ(inode_cache_init(64, &ic), 0);
	/* 10 ms TTL so the test stays fast. */
	inode_cache_set_ttl_ms(ic, 10);

	in = make_inode(100, 4096);
	ASSERT_EQ(inode_cache_put(ic, &in), 0);

	/* Immediate get is a hit (within TTL). */
	ASSERT_EQ(inode_cache_get(ic, 100, &out), 0);

	/* Sleep past the TTL; the next get must miss AND evict so the
	 * stale entry cannot be served again. */
	usleep(25 * 1000);
	ASSERT_EQ(inode_cache_get(ic, 100, &out), -1);
	ASSERT_EQ(inode_cache_count(ic), (uint32_t)0);

	inode_cache_destroy(ic);
}

/* -----------------------------------------------------------------------
 * test_ttl_disabled -- ttl_ms == 0 keeps entries until LRU eviction
 * ----------------------------------------------------------------------- */

static void test_ttl_disabled(void)
{
	struct inode_cache *ic = NULL;
	struct mds_inode in, out;

	ASSERT_EQ(inode_cache_init(64, &ic), 0);
	/* Default is 0 = disabled; set explicitly for clarity. */
	inode_cache_set_ttl_ms(ic, 0);

	in = make_inode(100, 4096);
	ASSERT_EQ(inode_cache_put(ic, &in), 0);

	usleep(25 * 1000);
	/* Still a hit -- no TTL in effect. */
	ASSERT_EQ(inode_cache_get(ic, 100, &out), 0);
	ASSERT_EQ(out.fileid, (uint64_t)100);

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
	RUN_TEST(test_ttl_expiry);
	RUN_TEST(test_ttl_disabled);

	fprintf(stdout, "\n%d/%d tests passed.\n", tests_passed, tests_run);
	return (tests_passed == tests_run) ? 0 : 1;
}
