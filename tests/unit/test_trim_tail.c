/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_trim_tail.c -- Unit tests for the trim module state: the
 * striped per-inode lock (L(F)), the quiescing-flag table, and the
 * trim_max_wait config key (0 = 3 x lease_time_sec).
 */

#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "pnfs_mds.h"
#include "trim_tail.h"

/* -----------------------------------------------------------------------
 * Minimal test framework (mirrors test_ds_gc.c).
 * ----------------------------------------------------------------------- */

static int tests_run    = 0;
static int tests_passed = 0;

#define RUN_TEST(fn) do {                                              \
    tests_run++;                                                        \
    fprintf(stdout, "  %-40s ", #fn);                                   \
    fn();                                                               \
    tests_passed++;                                                     \
    fprintf(stdout, "PASS\n");                                          \
} while (0)

#define ASSERT_EQ(a, b) do {                                           \
    long long _av = (long long)(a);                                     \
    long long _bv = (long long)(b);                                     \
    if (_av != _bv) {                                                   \
        fprintf(stderr, "FAIL at %s:%d: %s (=%lld) != %s (=%lld)\n",    \
                __FILE__, __LINE__, #a, _av, #b, _bv);                  \
        exit(1);                                                        \
    }                                                                   \
} while (0)

#define ASSERT_TRUE(cond) do {                                         \
    if (!(cond)) {                                                      \
        fprintf(stderr, "FAIL at %s:%d: !(%s)\n",                       \
                __FILE__, __LINE__, #cond);                             \
        exit(1);                                                        \
    }                                                                   \
} while (0)

/* -----------------------------------------------------------------------
 * Stripe-fold mirror
 * ----------------------------------------------------------------------- */

/** Mirrors trim_tail.c's fold (splitmix64 % 16) so the lock tests can
 *  pick fileids that live on different stripes. */
static uint64_t test_splitmix64(uint64_t x)
{
	x ^= x >> 30;
	x *= 0xbf58476d1ce4e5b9ULL;
	x ^= x >> 27;
	x *= 0x94d049bb133111ebULL;
	x ^= x >> 31;
	return x;
}

static uint32_t test_stripe(uint64_t fileid)
{
	return (uint32_t)(test_splitmix64(fileid) % 16);
}

/* -----------------------------------------------------------------------
 * Tests
 * ----------------------------------------------------------------------- */

/**
 * Two fileids on different stripes must lock without deadlocking, and
 * A must be re-lockable after both are released.  (Fileids on the SAME
 * stripe share one mutex by design, so a cross-stripe pair is what
 * makes the no-deadlock assertion meaningful.)
 */
static void test_trim_lock_stripped(void)
{
	uint64_t a = 0;
	uint64_t b = 1;

	while (test_stripe(b) == test_stripe(a)) {
		b++;
	}
	trim_inode_lock(a);
	trim_inode_lock(b);
	trim_inode_unlock(b);
	trim_inode_unlock(a);
	trim_inode_lock(a);
	trim_inode_unlock(a);
}

struct lock_worker_args {
	uint64_t        fileid;
	volatile bool   acquired;
};

static void *lock_worker(void *arg)
{
	struct lock_worker_args *wa = (struct lock_worker_args *)arg;

	trim_inode_lock(wa->fileid);
	wa->acquired = true;
	trim_inode_unlock(wa->fileid);
	return NULL;
}

/**
 * A second thread must stay blocked on L(F) while the first holds it,
 * and must acquire it once it is released (real serialisation, not
 * just "two locks happen to coexist").
 */
static void test_trim_lock_serializes(void)
{
	pthread_t              th;
	struct lock_worker_args wa;
	const uint64_t         f = 0x9e3779b97f4a7c15ULL;
	int                    i;

	wa.fileid = f;
	wa.acquired = false;
	trim_inode_lock(f);
	ASSERT_EQ(pthread_create(&th, NULL, lock_worker, &wa), 0);
	/* 2 s bound: generous on every CI worker class. */
	for (i = 0; i < 200 && !wa.acquired; i++) {
		usleep(10 * 1000);
	}
	ASSERT_TRUE(!wa.acquired);
	trim_inode_unlock(f);
	for (i = 0; i < 200 && !wa.acquired; i++) {
		usleep(10 * 1000);
	}
	ASSERT_TRUE(wa.acquired);
	ASSERT_EQ(pthread_join(th, NULL), 0);
}

/**
 * Flag lifecycle: unset -> begin -> set -> end -> unset, an unrelated
 * fileid is never affected, begin is idempotent, and end on a clear
 * flag is a no-op.
 */
static void test_trim_flag(void)
{
	const uint64_t f = 0xdeadbeefcafeULL;
	const uint64_t g = 0x0123456789abULL;

	ASSERT_TRUE(!trim_quiescing(f));
	trim_quiesce_begin(f);
	ASSERT_TRUE(trim_quiescing(f));
	/* Other fileids are unaffected. */
	ASSERT_TRUE(!trim_quiescing(g));
	/* begin is idempotent: one end() must clear it. */
	trim_quiesce_begin(f);
	ASSERT_TRUE(trim_quiescing(f));
	trim_quiesce_end(f);
	ASSERT_TRUE(!trim_quiescing(f));
	/* end() on a clear flag is a no-op. */
	trim_quiesce_end(f);
	ASSERT_TRUE(!trim_quiescing(f));
}

/**
 * 0 in config means "3 x lease_time_sec"; an explicit value is used
 * verbatim.
 */
static void test_trim_max_wait_default(void)
{
	trim_module_fini();
	ASSERT_EQ(trim_module_init(90, 0), 0);
	ASSERT_EQ(trim_max_wait_sec(), 270);
	ASSERT_EQ(trim_module_init(90, 45), 0);
	ASSERT_EQ(trim_max_wait_sec(), 45);
	/* Restore the module for any subsequent tests. */
	trim_module_fini();
	ASSERT_EQ(trim_module_init(90, 0), 0);
}

/* -----------------------------------------------------------------------
 * Config parsing (same end-to-end pattern as test_config.c: write a
 * temp INI, run it through mds_config_load, assert on the struct).
 * ----------------------------------------------------------------------- */

static int write_tmp_ini(const char *content, char path_out[128])
{
	FILE *f;

	(void)snprintf(path_out, 128, "/tmp/pnfs-trim-cfg-%d.conf",
		       (int)getpid());
	f = fopen(path_out, "w");
	if (f == NULL) {
		return -1;
	}
	fputs(content, f);
	fclose(f);
	return 0;
}

static void test_trim_config_parse(void)
{
	char            path[128];
	struct mds_config cfg;

	/* Explicit key is parsed verbatim. */
	ASSERT_EQ(write_tmp_ini("trim_max_wait = 42\n", path), 0);
	ASSERT_EQ(mds_config_load(path, &cfg), MDS_OK);
	ASSERT_EQ((uint32_t)cfg.trim_max_wait_sec, 42);

	/* Absent key lands on the documented default (0 = 3 x lease),
	 * and the daemon's startup call passes the pair straight to
	 * trim_module_init. */
	ASSERT_EQ(write_tmp_ini("", path), 0);
	ASSERT_EQ(mds_config_load(path, &cfg), MDS_OK);
	ASSERT_EQ((uint32_t)cfg.trim_max_wait_sec, 0);
	ASSERT_EQ(trim_module_init(cfg.lease_time_sec,
				   cfg.trim_max_wait_sec), 0);
	ASSERT_EQ(trim_max_wait_sec(), 3 * cfg.lease_time_sec);

	/* Restore the module for any subsequent tests. */
	trim_module_fini();
	ASSERT_EQ(trim_module_init(90, 0), 0);
	unlink(path);
}

/* -----------------------------------------------------------------------
 * Entry point
 * ----------------------------------------------------------------------- */

int main(void)
{
	fprintf(stdout, "trim module state tests\n");

	ASSERT_EQ(trim_module_init(90, 0), 0);
	RUN_TEST(test_trim_lock_stripped);
	RUN_TEST(test_trim_lock_serializes);
	RUN_TEST(test_trim_flag);
	RUN_TEST(test_trim_max_wait_default);
	RUN_TEST(test_trim_config_parse);
	trim_module_fini();

	fprintf(stdout, "%d/%d tests passed\n", tests_passed, tests_run);
	return (tests_passed == tests_run) ? 0 : 1;
}
