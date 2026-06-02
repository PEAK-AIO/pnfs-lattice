/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_dirent_cache.c -- Unit tests for the in-memory dirent LRU cache,
 * focused on the positive- and negative-entry TTLs used to bound
 * cross-MDS staleness in active-active deployments.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>  /* usleep() for TTL tests */

#include "pnfs_mds.h"
#include "dirent_cache.h"

static int tests_run;
static int tests_passed;

#define ASSERT_EQ(a, b) do {						\
	if ((a) != (b)) {						\
		fprintf(stderr, "  FAIL %s:%d: %s != %s\n",		\
			__FILE__, __LINE__, #a, #b);			\
		return;							\
	}								\
} while (0)

#define RUN_TEST(fn) do {						\
	tests_run++;							\
	fprintf(stdout, "  %-40s", #fn);				\
	fflush(stdout);							\
	fn();								\
	tests_passed++;							\
	fprintf(stdout, "PASS\n");					\
} while (0)

/* -----------------------------------------------------------------------
 * test_put_get -- positive entry round-trip + miss
 * ----------------------------------------------------------------------- */

static void test_put_get(void)
{
	struct dirent_cache *dc = NULL;
	uint64_t child = 0;
	uint8_t type = 0;

	ASSERT_EQ(dirent_cache_init(64, 0, &dc), 0);

	ASSERT_EQ(dirent_cache_put(dc, 2, "file", 100,
				   (uint8_t)MDS_FTYPE_REG), 0);
	ASSERT_EQ(dirent_cache_get(dc, 2, "file", &child, &type), 0);
	ASSERT_EQ(child, (uint64_t)100);
	ASSERT_EQ(type, (uint8_t)MDS_FTYPE_REG);

	/* Miss on an un-cached name. */
	ASSERT_EQ(dirent_cache_get(dc, 2, "absent", &child, &type), -1);

	dirent_cache_destroy(dc);
}

/* -----------------------------------------------------------------------
 * test_negative_hit -- a negative entry reports return code 1
 * ----------------------------------------------------------------------- */

static void test_negative_hit(void)
{
	struct dirent_cache *dc = NULL;

	ASSERT_EQ(dirent_cache_init(64, 5000, &dc), 0);

	ASSERT_EQ(dirent_cache_put_negative(dc, 2, "missing"), 0);
	/* Negative hit within TTL -> return 1. */
	ASSERT_EQ(dirent_cache_get(dc, 2, "missing", NULL, NULL), 1);

	dirent_cache_destroy(dc);
}

/* -----------------------------------------------------------------------
 * test_invalidate -- explicit invalidate drops a positive entry
 * ----------------------------------------------------------------------- */

static void test_invalidate(void)
{
	struct dirent_cache *dc = NULL;
	uint64_t child = 0;
	uint8_t type = 0;

	ASSERT_EQ(dirent_cache_init(64, 0, &dc), 0);

	ASSERT_EQ(dirent_cache_put(dc, 2, "file", 100,
				   (uint8_t)MDS_FTYPE_REG), 0);
	dirent_cache_invalidate(dc, 2, "file");
	ASSERT_EQ(dirent_cache_get(dc, 2, "file", &child, &type), -1);

	dirent_cache_destroy(dc);
}

/* -----------------------------------------------------------------------
 * test_pos_ttl_expiry -- a positive entry older than pos_ttl is a miss
 *
 * This is the active-active coherence guard: a stale name->fileid
 * mapping left behind by a peer MDS's delete+recreate must age out so
 * the next LOOKUP re-reads the authoritative catalogue.
 * ----------------------------------------------------------------------- */

static void test_pos_ttl_expiry(void)
{
	struct dirent_cache *dc = NULL;
	uint64_t child = 0;
	uint8_t type = 0;

	ASSERT_EQ(dirent_cache_init(64, 5000, &dc), 0);
	dirent_cache_set_pos_ttl_ms(dc, 10); /* 10 ms positive TTL */

	ASSERT_EQ(dirent_cache_put(dc, 2, "file", 100,
				   (uint8_t)MDS_FTYPE_REG), 0);
	/* Immediate get is a hit (within TTL). */
	ASSERT_EQ(dirent_cache_get(dc, 2, "file", &child, &type), 0);

	/* Sleep past the TTL; the next get must miss and evict. */
	usleep(25 * 1000);
	ASSERT_EQ(dirent_cache_get(dc, 2, "file", &child, &type), -1);
	ASSERT_EQ(dirent_cache_count(dc), (uint32_t)0);

	dirent_cache_destroy(dc);
}

/* -----------------------------------------------------------------------
 * test_pos_ttl_disabled -- pos_ttl == 0 keeps positive entries
 * ----------------------------------------------------------------------- */

static void test_pos_ttl_disabled(void)
{
	struct dirent_cache *dc = NULL;
	uint64_t child = 0;
	uint8_t type = 0;

	ASSERT_EQ(dirent_cache_init(64, 5000, &dc), 0);
	/* pos_ttl defaults to 0 (disabled); do not set it. */

	ASSERT_EQ(dirent_cache_put(dc, 2, "file", 100,
				   (uint8_t)MDS_FTYPE_REG), 0);
	usleep(25 * 1000);
	/* Still a hit -- no positive TTL in effect. */
	ASSERT_EQ(dirent_cache_get(dc, 2, "file", &child, &type), 0);
	ASSERT_EQ(child, (uint64_t)100);

	dirent_cache_destroy(dc);
}

/* -----------------------------------------------------------------------
 * test_neg_ttl_expiry -- negative TTL still expires (unchanged behaviour)
 * ----------------------------------------------------------------------- */

static void test_neg_ttl_expiry(void)
{
	struct dirent_cache *dc = NULL;

	ASSERT_EQ(dirent_cache_init(64, 10, &dc), 0); /* 10 ms negative TTL */

	ASSERT_EQ(dirent_cache_put_negative(dc, 2, "missing"), 0);
	/* Within TTL -> negative hit. */
	ASSERT_EQ(dirent_cache_get(dc, 2, "missing", NULL, NULL), 1);

	/* Past TTL -> miss (entry evicted). */
	usleep(25 * 1000);
	ASSERT_EQ(dirent_cache_get(dc, 2, "missing", NULL, NULL), -1);

	dirent_cache_destroy(dc);
}

/* -----------------------------------------------------------------------
 * main
 * ----------------------------------------------------------------------- */

int main(void)
{
	fprintf(stdout, "Running dirent cache tests:\n");

	RUN_TEST(test_put_get);
	RUN_TEST(test_negative_hit);
	RUN_TEST(test_invalidate);
	RUN_TEST(test_pos_ttl_expiry);
	RUN_TEST(test_pos_ttl_disabled);
	RUN_TEST(test_neg_ttl_expiry);

	fprintf(stdout, "\n%d/%d tests passed.\n", tests_passed, tests_run);
	return (tests_passed == tests_run) ? 0 : 1;
}
