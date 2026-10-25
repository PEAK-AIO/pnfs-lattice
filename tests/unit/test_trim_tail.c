/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_trim_tail.c -- Unit tests for the trim module: the striped
 * per-inode lock (L(F)), the quiescing-flag table, the trim_max_wait
 * config key (0 = 3 x lease_time_sec), and the trim execution core
 * (trim_process_entry): ownership guard, vanish check, quiesced
 * locked DS-tail truncate, backoff bound, and restart safety.
 *
 * The execution core runs against the in-memory memdb catalogue and
 * temp directories as fake DS mounts (test_ds_gc.c /
 * test_proxy_io.c fixture pattern), so no RonDB or NFS dependency is
 * needed.  No layout-recall coordinator is bound in these tests, so
 * the quiescence step is a no-op: the fixtures hold no pNFS layouts,
 * which is the documented unit-test path.
 */

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "pnfs_mds.h"
#include "trim_tail.h"
#include "test_helpers.h"
#include "mds_catalogue.h"
#include "proxy_io.h"
#include "subtree_map.h"
#include "catalogue_internal.h"
#include "catalog_image.h"

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
 * Execution-core fixtures
 *
 * One temp directory per fake DS mount; the on-disk file of a
 * (fileid, stripe, mirror) slot lives at
 *   {mount}/data/{fileid}_{stripe}_{mirror}
 * with the deterministic pattern byte(i) = i & 0xFF, so "intact" is
 * checkable byte-for-byte without re-running the writer.
 * ----------------------------------------------------------------------- */

#define KIB(n)       ((uint64_t)(n) * 1024ULL)
#define STRIPE_UNIT  KIB(64)
#define WRITE_BYTE   0xA5U

static uint8_t pat_byte(uint64_t off)
{
	return (uint8_t)(off & 0xFFU);
}

static const uint8_t zeros_[4096];

static char *make_ds_dir(void)
{
	char tpl[64];
	char *dir;

	(void)snprintf(tpl, sizeof(tpl), "/tmp/test_trim_tail_XXXXXX");
	dir = strdup(tpl);
	ASSERT_TRUE(dir != NULL);
	ASSERT_TRUE(mkdtemp(dir) != NULL);
	return dir;
}

static void ds_path(char *out, size_t cap, const char *mount,
                    uint64_t fileid, uint32_t stripe, uint32_t mirror)
{
	(void)snprintf(out, cap, "%s/data/%llu_%u_%u",
		       mount, (unsigned long long)fileid, stripe, mirror);
}

static void ds_data_dir(const char *mount)
{
	char path[4200];

	(void)snprintf(path, sizeof(path), "%s/data", mount);
	(void)mkdir(path, 0755);
}

/** Create the DS-side data file filled with the pattern (byte i = i&0xFF). */
static void ds_write_pattern(const char *mount, uint64_t fileid,
                             uint32_t stripe, uint32_t mirror,
                             uint64_t size)
{
	char    path[4200];
	FILE   *fp;
	uint8_t buf[4096];
	uint64_t off = 0;

	ds_path(path, sizeof(path), mount, fileid, stripe, mirror);
	fp = fopen(path, "wb");
	ASSERT_TRUE(fp != NULL);
	while (off < size) {
		uint64_t n = size - off;
		uint64_t i;

		if (n > sizeof(buf)) {
			n = sizeof(buf);
		}
		for (i = 0; i < n; i++) {
			buf[i] = pat_byte(off + i);
		}
		if (fwrite(buf, 1, n, fp) != n) {
			ASSERT_TRUE(0);
		}
		off += n;
	}
	(void)fclose(fp);
}

static bool ds_is_reg(const char *mount, uint64_t fileid, uint32_t s,
                      uint32_t m)
{
	char       path[4200];
	struct stat st;

	ds_path(path, sizeof(path), mount, fileid, s, m);
	if (stat(path, &st) != 0) {
		return false;
	}
	return S_ISREG(st.st_mode);
}

/** -1 when the backing file is not a regular file. */
static int64_t ds_len(const char *mount, uint64_t fileid,
                      uint32_t stripe, uint32_t mirror)
{
	char path[4200];
	int  fd;
	off_t len;

	ds_path(path, sizeof(path), mount, fileid, stripe, mirror);
	fd = open(path, O_RDONLY);
	if (fd < 0) {
		return -1;
	}
	len = lseek(fd, 0, SEEK_END);
	(void)close(fd);
	if (len < 0) {
		return -1;
	}
	return (int64_t)len;
}

static bool ds_read(const char *mount, uint64_t fileid, uint32_t stripe,
                    uint32_t mirror, uint64_t off, void *buf, uint32_t len)
{
	char path[4200];
	int  fd;
	ssize_t n;

	ds_path(path, sizeof(path), mount, fileid, stripe, mirror);
	fd = open(path, O_RDONLY);
	if (fd < 0) {
		return false;
	}
	n = pread(fd, buf, len, (off_t)off);
	(void)close(fd);
	return n == (ssize_t)len;
}

/** Every byte of [off, off+len) matches the pattern. */
static bool ds_is_pattern(const char *mount, uint64_t fileid, uint32_t s,
                          uint32_t m, uint64_t off, uint64_t len)
{
	uint8_t buf[4096];

	while (len > 0) {
		uint64_t n = (len > sizeof(buf)) ? sizeof(buf) : len;
		uint64_t i;

		if (!ds_read(mount, fileid, s, m, off, buf, (uint32_t)n)) {
			return false;
		}
		for (i = 0; i < n; i++) {
			if (buf[i] != pat_byte(off + i)) {
				return false;
			}
		}
		off += n;
		len -= n;
	}
	return true;
}

/** Every byte of [off, off+len) is zero (a trimmed/extended hole). */
static bool ds_is_zeros(const char *mount, uint64_t fileid, uint32_t s,
                        uint32_t m, uint64_t off, uint64_t len)
{
	uint8_t buf[4096];

	while (len > 0) {
		uint64_t n = (len > sizeof(buf)) ? sizeof(buf) : len;

		if (!ds_read(mount, fileid, s, m, off, buf, (uint32_t)n)) {
			return false;
		}
		if (memcmp(buf, zeros_, n) != 0) {
			return false;
		}
		off += n;
		len -= n;
	}
	return true;
}

/** Every byte of [off, off+len) equals @c b. */
static bool ds_is_byte(const char *mount, uint64_t fileid, uint32_t s,
                       uint32_t m, uint64_t off, uint64_t len, uint8_t b)
{
	uint8_t buf[4096];

	while (len > 0) {
		uint64_t n = (len > sizeof(buf)) ? sizeof(buf) : len;
		uint64_t i;

		if (!ds_read(mount, fileid, s, m, off, buf, (uint32_t)n)) {
			return false;
		}
		for (i = 0; i < n; i++) {
			if (buf[i] != b) {
				return false;
			}
		}
		off += n;
		len -= n;
	}
	return true;
}

/* Catalogue-side helpers. */

static uint64_t cat_size_of(struct mds_catalogue *cat, uint64_t fileid)
{
	struct mds_inode in;

	ASSERT_EQ(mds_cat_ns_getattr(cat, fileid, &in), MDS_OK);
	return in.size;
}

static void cat_set_size(struct mds_catalogue *cat, uint64_t fileid,
                         uint64_t size)
{
	struct mds_inode in;

	ASSERT_EQ(mds_cat_ns_getattr(cat, fileid, &in), MDS_OK);
	in.size = size;
	ASSERT_EQ(mds_cat_ns_setattr(cat, NULL, fileid, &in,
				     MDS_ATTR_SIZE), MDS_OK);
}

/** Enqueue the TRIM row the shrink path writes (owner 0 = global). */
static void enqueue_trim_row(struct mds_catalogue *cat, uint64_t fileid,
                             uint32_t ds_id)
{
	uint8_t fh[4] = { 't', 'r', 'm', 0 };

	ASSERT_EQ(mds_cat_gc_enqueue_hint_owner(cat, NULL, fileid, ds_id,
			fh, sizeof(fh), MDS_GC_SWEEP_TRIM,
			MDS_GC_OWNER_GLOBAL),
		MDS_OK);
}

static uint32_t gc_count_of(struct mds_catalogue *cat)
{
	uint32_t n = 0;

	ASSERT_EQ(mds_cat_gc_count(cat, &n), MDS_OK);
	return n;
}

/**
 * Peek the oldest GC row; true (and *out filled) when it belongs to
 * @a fileid.
 */
static bool peek_row(struct mds_catalogue *cat, uint64_t fileid,
                     struct mds_gc_entry *out)
{
	struct mds_gc_entry e;

	if (mds_cat_gc_peek(cat, &e) != MDS_OK) {
		return false;
	}
	if (e.fileid != fileid) {
		return false;
	}
	*out = e;
	return true;
}

/** Regular file @a name (under root) with a 1x1 stripe map on @a ds_id. */
static uint64_t new_file_1x1(struct mds_catalogue *cat, const char *name,
                             uint64_t size, uint32_t ds_id)
{
	struct mds_inode       file;
	struct mds_ds_map_entry e;

	ASSERT_EQ(test_create_file(cat, MDS_FILEID_ROOT, name, 0644, &file),
		MDS_OK);
	cat_set_size(cat, file.fileid, size);
	memset(&e, 0, sizeof(e));
	e.ds_id = ds_id;
	ASSERT_EQ(mds_cat_stripe_map_put(cat, NULL, file.fileid,
		/*sc*/ 1U, STRIPE_UNIT, /*mc*/ 1U, &e),
		MDS_OK);
	return file.fileid;
}

/** Regular file @a name (under root) with a 1x2 stripe map: mirror 0
 *  on @a ds_id0, mirror 1 on @a ds_id1 (slot i: stripe = i % 1,
 *  mirror = i / 1). */
static uint64_t new_file_1x2(struct mds_catalogue *cat, const char *name,
                             uint64_t size, uint32_t ds_id0,
                             uint32_t ds_id1)
{
	struct mds_inode       file;
	struct mds_ds_map_entry e[2];

	ASSERT_EQ(test_create_file(cat, MDS_FILEID_ROOT, name, 0644, &file),
		MDS_OK);
	cat_set_size(cat, file.fileid, size);
	memset(e, 0, sizeof(e));
	e[0].ds_id = ds_id0;
	e[1].ds_id = ds_id1;
	ASSERT_EQ(mds_cat_stripe_map_put(cat, NULL, file.fileid,
		/*sc*/ 1U, STRIPE_UNIT, /*mc*/ 2U, e),
		MDS_OK);
	return file.fileid;
}

/* Standard environment: one memdb catalogue, one proxy context with
 * ds_id 1 mounted at a temp dir. */
struct trim_env {
	struct mds_catalogue *cat;
	struct mds_proxy_ctx *proxy;
	char *ds_dir;
	char *ds_dir2;
};

static struct trim_env env_open(void)
{
	struct trim_env env;

	memset(&env, 0, sizeof(env));
	env.cat = open_test_catalogue();
	ASSERT_TRUE(env.cat != NULL);
	ASSERT_EQ(mds_proxy_ctx_create(&env.proxy), MDS_OK);
	env.ds_dir = make_ds_dir();
	ASSERT_EQ(mds_proxy_mount_set(env.proxy, /*ds_id*/ 1U, env.ds_dir),
		MDS_OK);
	return env;
}

static void env_close(struct trim_env *env)
{
	test_rm_rf(env->ds_dir);
	free(env->ds_dir);
	if (env->ds_dir2 != NULL) {
		test_rm_rf(env->ds_dir2);
		free(env->ds_dir2);
	}
	mds_proxy_ctx_destroy(env->proxy);
	mds_catalogue_close(env->cat);
	memset(env, 0, sizeof(*env));
}

/* -----------------------------------------------------------------------
 * test_trim_p1_proxy_fence (T1, both orderings)
 * ----------------------------------------------------------------------- */

struct fence_worker_args {
	struct mds_catalogue *cat;
	struct mds_proxy_ctx *proxy;
	struct mds_gc_entry  *row;
	enum mds_status       rc;
};

static void *fence_worker(void *arg)
{
	struct fence_worker_args *wa = (struct fence_worker_args *)arg;

	wa->rc = trim_process_entry(wa->cat, wa->proxy, wa->row);
	return NULL;
}

/**
 * P1-1 proxy fence.  The worker re-reads the authoritative size under
 * L(F) (Rule R); the proxy write path brackets DS write + size commit
 * with the same lock, so the two brackets cannot overlap:
 *
 *  (a) write bracket before the worker's S read: the re-read sees the
 *      grown size, the truncate is a no-op, all bytes survive;
 *  (b) worker's bracket serialised behind a write bracket held by the
 *      test thread (real L(F) blocking on a second thread);
 *  (c) worker's S read before the write bracket: the truncate to the
 *      shrunken size runs, then the write re-extends the file -- the
 *      written bytes survive, the shrink window is a legitimate hole.
 */
static void test_trim_p1_proxy_fence(void)
{
	struct trim_env         env;
	struct fence_worker_args wa;
	pthread_t               th;
	uint64_t                fid;
	struct mds_gc_entry     row;
	uint8_t                 data[4096];
	uint32_t                written = 0;
	int                     i;

	for (i = 0; i < (int)sizeof(data); i++) {
		data[i] = WRITE_BYTE;
	}

	/* (a) write commits before the worker's S read. */
	env = env_open();
	fid = new_file_1x1(env.cat, "fence_a", KIB(12), 1);
	ds_data_dir(env.ds_dir);
	ds_write_pattern(env.ds_dir, fid, 0, 0, KIB(12));
	cat_set_size(env.cat, fid, KIB(4));
	enqueue_trim_row(env.cat, fid, 1);
	ASSERT_EQ(mds_proxy_write(env.proxy, env.cat, fid, KIB(8),
		data, sizeof(data), &written), MDS_OK);
	ASSERT_EQ(written, sizeof(data));
	cat_set_size(env.cat, fid, KIB(12));
	ASSERT_TRUE(peek_row(env.cat, fid, &row));
	ASSERT_EQ(trim_process_entry(env.cat, env.proxy, &row), MDS_OK);
	ASSERT_EQ(gc_count_of(env.cat), 0);
	ASSERT_EQ(cat_size_of(env.cat, fid), KIB(12));
	ASSERT_EQ(ds_len(env.ds_dir, fid, 0, 0), KIB(12));
	ASSERT_TRUE(ds_is_pattern(env.ds_dir, fid, 0, 0, 0, KIB(8)));
	ASSERT_TRUE(ds_is_byte(env.ds_dir, fid, 0, 0, KIB(8),
			       KIB(4), WRITE_BYTE));
	ASSERT_TRUE(!trim_quiescing(fid));
	env_close(&env);

	/* (b) the worker blocks on L(F) while the write bracket runs. */
	env = env_open();
	fid = new_file_1x1(env.cat, "fence_b", KIB(12), 1);
	ds_data_dir(env.ds_dir);
	ds_write_pattern(env.ds_dir, fid, 0, 0, KIB(12));
	cat_set_size(env.cat, fid, KIB(4));
	enqueue_trim_row(env.cat, fid, 1);
	ASSERT_TRUE(peek_row(env.cat, fid, &row));

	wa.cat = env.cat;
	wa.proxy = env.proxy;
	wa.row = &row;
	wa.rc = (enum mds_status)MDS_ERR_INVAL;
	trim_inode_lock(fid);
	ASSERT_EQ(pthread_create(&th, NULL, fence_worker, &wa), 0);
	/* The worker must have reached the quiesce phase; it cannot
	 * pass step 6 while the test thread holds L(F). */
	for (i = 0; i < 200 && !trim_quiescing(fid); i++) {
		usleep(10 * 1000);
	}
	ASSERT_TRUE(trim_quiescing(fid));
	/* Write bracket, under the held L(F) (as in production). */
	ASSERT_EQ(mds_proxy_write(env.proxy, env.cat, fid, KIB(8),
		data, sizeof(data), &written), MDS_OK);
	cat_set_size(env.cat, fid, KIB(12));
	trim_inode_unlock(fid);
	ASSERT_EQ(pthread_join(th, NULL), 0);
	ASSERT_EQ(wa.rc, MDS_OK);
	ASSERT_EQ(gc_count_of(env.cat), 0);
	ASSERT_EQ(cat_size_of(env.cat, fid), KIB(12));
	ASSERT_EQ(ds_len(env.ds_dir, fid, 0, 0), KIB(12));
	ASSERT_TRUE(ds_is_pattern(env.ds_dir, fid, 0, 0, 0, KIB(8)));
	ASSERT_TRUE(ds_is_byte(env.ds_dir, fid, 0, 0, KIB(8),
			       KIB(4), WRITE_BYTE));
	ASSERT_TRUE(!trim_quiescing(fid));
	env_close(&env);

	/* (c) the write commits after the worker's S read. */
	env = env_open();
	fid = new_file_1x1(env.cat, "fence_c", KIB(12), 1);
	ds_data_dir(env.ds_dir);
	ds_write_pattern(env.ds_dir, fid, 0, 0, KIB(12));
	cat_set_size(env.cat, fid, KIB(4));
	enqueue_trim_row(env.cat, fid, 1);
	ASSERT_TRUE(peek_row(env.cat, fid, &row));
	ASSERT_EQ(trim_process_entry(env.cat, env.proxy, &row), MDS_OK);
	ASSERT_EQ(ds_len(env.ds_dir, fid, 0, 0), KIB(4));
	/* The write bracket runs after the worker's bracket: it
	 * re-extends the file and lands the new tail. */
	ASSERT_EQ(mds_proxy_write(env.proxy, env.cat, fid, KIB(8),
		data, sizeof(data), &written), MDS_OK);
	cat_set_size(env.cat, fid, KIB(12));
	ASSERT_EQ(cat_size_of(env.cat, fid), KIB(12));
	ASSERT_EQ(ds_len(env.ds_dir, fid, 0, 0), KIB(12));
	ASSERT_TRUE(ds_is_pattern(env.ds_dir, fid, 0, 0, 0, KIB(4)));
	ASSERT_TRUE(ds_is_zeros(env.ds_dir, fid, 0, 0, KIB(4), KIB(4)));
	ASSERT_TRUE(ds_is_byte(env.ds_dir, fid, 0, 0, KIB(8),
			       KIB(4), WRITE_BYTE));
	env_close(&env);
}

/* -----------------------------------------------------------------------
 * test_trim_recoverable_failure (T2)
 * ----------------------------------------------------------------------- */

/**
 * P1-2 recoverable failure: a truncate that fails keeps the row
 * queued (and the flag set); once the fault clears, the retry drains;
 * a later grow reads zeros, never stale tail bytes.
 */
static void test_trim_recoverable_failure(void)
{
	struct trim_env     env;
	uint64_t            fid;
	struct mds_gc_entry row;
	char                bad[4200];
	uint8_t             buf[4096];
	uint32_t            read = 0;
	bool                eof = false;

	env = env_open();
	fid = new_file_1x1(env.cat, "recover", KIB(12), 1);
	ds_data_dir(env.ds_dir);
	ds_write_pattern(env.ds_dir, fid, 0, 0, KIB(12));
	cat_set_size(env.cat, fid, KIB(4));
	enqueue_trim_row(env.cat, fid, 1);
	ASSERT_TRUE(peek_row(env.cat, fid, &row));

	/* Fault: replace the DS file with a directory so truncate(2)
	 * fails EISDIR regardless of who runs the test. */
	ds_path(bad, sizeof(bad), env.ds_dir, fid, 0, 0);
	ASSERT_EQ(unlink(bad), 0);
	ASSERT_EQ(mkdir(bad, 0755), 0);

	ASSERT_EQ(trim_process_entry(env.cat, env.proxy, &row),
		MDS_ERR_DELAY);
	ASSERT_EQ(gc_count_of(env.cat), 1);
	ASSERT_TRUE(trim_quiescing(fid));
	ASSERT_TRUE(!ds_is_reg(env.ds_dir, fid, 0, 0));

	/* Clear the fault: restore the real 12K backing file. */
	ASSERT_EQ(rmdir(bad), 0);
	ds_write_pattern(env.ds_dir, fid, 0, 0, KIB(12));

	ASSERT_TRUE(peek_row(env.cat, fid, &row));
	ASSERT_EQ(trim_process_entry(env.cat, env.proxy, &row), MDS_OK);
	ASSERT_EQ(gc_count_of(env.cat), 0);
	ASSERT_TRUE(!trim_quiescing(fid));
	ASSERT_EQ(ds_len(env.ds_dir, fid, 0, 0), KIB(4));
	ASSERT_TRUE(ds_is_pattern(env.ds_dir, fid, 0, 0, 0, KIB(4)));

	/* A later grow reads zeros, not the stale bytes. */
	cat_set_size(env.cat, fid, KIB(16));
	ASSERT_EQ(mds_proxy_read(env.proxy, env.cat, fid, KIB(16),
		KIB(4), KIB(4), buf, &read, &eof), MDS_OK);
	ASSERT_EQ(read, KIB(4));
	ASSERT_TRUE(memcmp(buf, zeros_, KIB(4)) == 0);
	env_close(&env);
}

/* -----------------------------------------------------------------------
 * test_trim_backoff_release_inflight (fix round 1, finding 1)
 * ----------------------------------------------------------------------- */

/**
 * The backoff-bound release must NOT clear the flag of an in-flight
 * drain: while worker A is mid-attempt (flag set by its own step 5,
 * parked under L(F)), a second drain of the same fileid whose backoff
 * bound is due must skip the release (a drain is in flight); once A
 * finishes, the next cycle's backoff bound clears the flag a failed
 * attempt kept set (no permanent DELAY wedge).
 *
 * Driven deterministically: the test thread holds L(F) so the worker
 * parks mid-attempt, and the test thread itself issues the probe
 * drain.  The 1-second max_wait plus ~1.1-second sleeps make the
 * probe's backoff bound due in both phases.
 */
static void test_trim_backoff_release_inflight(void)
{
	struct trim_env          env;
	struct fence_worker_args wa;
	pthread_t                th;
	uint64_t                 fid;
	struct mds_gc_entry      row;
	char                     bad[4200];
	int                      i;

	/* 1s max_wait so the backoff bound can fire across a sleep. */
	ASSERT_EQ(trim_module_init(90, 1), 0);

	env = env_open();
	fid = new_file_1x1(env.cat, "backoff", KIB(8), 1);
	ds_data_dir(env.ds_dir);
	ds_write_pattern(env.ds_dir, fid, 0, 0, KIB(8));
	cat_set_size(env.cat, fid, KIB(4));
	enqueue_trim_row(env.cat, fid, 1);

	/* Plant the kept-set flag the way a truncate failure does
	 * (EISDIR fault), then retire it the way a prior cycle's
	 * backoff release would: the seen entry's age keeps running,
	 * the in-flight count is zero, the flag is clear. */
	ds_path(bad, sizeof(bad), env.ds_dir, fid, 0, 0);
	ASSERT_EQ(unlink(bad), 0);
	ASSERT_EQ(mkdir(bad, 0755), 0);
	ASSERT_TRUE(peek_row(env.cat, fid, &row));
	ASSERT_EQ(trim_process_entry(env.cat, env.proxy, &row),
		MDS_ERR_DELAY);
	ASSERT_TRUE(trim_quiescing(fid));
	ASSERT_EQ(rmdir(bad), 0);
	ds_write_pattern(env.ds_dir, fid, 0, 0, KIB(8));
	trim_quiesce_end(fid);
	ASSERT_TRUE(!trim_quiescing(fid));

	/* Phase 1: worker A mid-attempt owns the flag; the probe's
	 * due backoff bound must NOT release it. */
	wa.cat = env.cat;
	wa.proxy = env.proxy;
	wa.row = &row;
	wa.rc = (enum mds_status)MDS_ERR_INVAL;
	trim_inode_lock(fid);
	ASSERT_EQ(pthread_create(&th, NULL, fence_worker, &wa), 0);
	for (i = 0; i < 200 && !trim_quiescing(fid); i++) {
		usleep(10 * 1000);
	}
	ASSERT_TRUE(trim_quiescing(fid));
	/* The seen entry is now older than the 1s max_wait. */
	usleep(1100 * 1000);
	ASSERT_TRUE(peek_row(env.cat, fid, &row));
	ASSERT_EQ(trim_process_entry(env.cat, env.proxy, &row),
		MDS_ERR_DELAY);
	/* A is still in flight (parked on L(F)): the flag survives
	 * the probe's due backoff bound. */
	ASSERT_TRUE(trim_quiescing(fid));
	ASSERT_EQ(gc_count_of(env.cat), 1);

	/* Release A: it drains the row and clears the flag itself. */
	trim_inode_unlock(fid);
	ASSERT_EQ(pthread_join(th, NULL), 0);
	ASSERT_EQ(wa.rc, MDS_OK);
	ASSERT_EQ(gc_count_of(env.cat), 0);
	ASSERT_TRUE(!trim_quiescing(fid));
	ASSERT_EQ(ds_len(env.ds_dir, fid, 0, 0), KIB(4));
	ASSERT_TRUE(ds_is_pattern(env.ds_dir, fid, 0, 0, 0, KIB(4)));

	/* Phase 2: with no drain in flight, the next cycle's backoff
	 * bound clears the flag a failed attempt kept set. */
	ds_path(bad, sizeof(bad), env.ds_dir, fid, 0, 0);
	ASSERT_EQ(unlink(bad), 0);
	ASSERT_EQ(mkdir(bad, 0755), 0);
	enqueue_trim_row(env.cat, fid, 1);
	ASSERT_TRUE(peek_row(env.cat, fid, &row));
	ASSERT_EQ(trim_process_entry(env.cat, env.proxy, &row),
		MDS_ERR_DELAY);
	ASSERT_TRUE(trim_quiescing(fid));
	usleep(1100 * 1000);
	ASSERT_TRUE(peek_row(env.cat, fid, &row));
	ASSERT_EQ(trim_process_entry(env.cat, env.proxy, &row),
		MDS_ERR_DELAY);
	ASSERT_TRUE(!trim_quiescing(fid));
	ASSERT_EQ(gc_count_of(env.cat), 1);

	/* Restore the resolved max_wait for the remaining tests. */
	ASSERT_EQ(trim_module_init(90, 0), 0);
	env_close(&env);
}

/* -----------------------------------------------------------------------
 * test_trim_superseding_grow (T3 + multi-mirror variant)
 * ----------------------------------------------------------------------- */

/**
 * A grow past the old tail that commits before the worker runs makes
 * the trim a no-op (Rule R: truncate to the current size).  The 1x2
 * variant asserts BOTH mirrors land at S.
 */
static void test_trim_superseding_grow(void)
{
	struct trim_env     env;
	uint64_t            fid;
	struct mds_gc_entry row;
	uint8_t             data[4096];
	uint32_t            written = 0;
	int                 i;

	for (i = 0; i < (int)sizeof(data); i++) {
		data[i] = WRITE_BYTE;
	}

	/* Single mirror: shrink, then grow past the old tail. */
	env = env_open();
	fid = new_file_1x1(env.cat, "grow", KIB(12), 1);
	ds_data_dir(env.ds_dir);
	ds_write_pattern(env.ds_dir, fid, 0, 0, KIB(12));
	cat_set_size(env.cat, fid, KIB(4));
	enqueue_trim_row(env.cat, fid, 1);
	ASSERT_EQ(mds_proxy_write(env.proxy, env.cat, fid, KIB(12),
		data, sizeof(data), &written), MDS_OK);
	cat_set_size(env.cat, fid, KIB(16));
	ASSERT_TRUE(peek_row(env.cat, fid, &row));
	ASSERT_EQ(trim_process_entry(env.cat, env.proxy, &row), MDS_OK);
	ASSERT_EQ(gc_count_of(env.cat), 0);
	ASSERT_EQ(ds_len(env.ds_dir, fid, 0, 0), KIB(16));
	ASSERT_TRUE(ds_is_pattern(env.ds_dir, fid, 0, 0, 0, KIB(12)));
	ASSERT_TRUE(ds_is_byte(env.ds_dir, fid, 0, 0, KIB(12),
			       KIB(4), WRITE_BYTE));
	env_close(&env);

	/* 1x2 mirrors: both backing files truncate to S. */
	env = env_open();
	env.ds_dir2 = make_ds_dir();
	ASSERT_EQ(mds_proxy_mount_set(env.proxy, /*ds_id*/ 2U,
		env.ds_dir2), MDS_OK);
	fid = new_file_1x2(env.cat, "mirror", KIB(12), 1, 2);
	ds_data_dir(env.ds_dir);
	ds_data_dir(env.ds_dir2);
	ds_write_pattern(env.ds_dir, fid, 0, 0, KIB(12));
	ds_write_pattern(env.ds_dir2, fid, 0, 1, KIB(12));
	cat_set_size(env.cat, fid, KIB(4));
	enqueue_trim_row(env.cat, fid, 1);
	ASSERT_TRUE(peek_row(env.cat, fid, &row));
	ASSERT_EQ(trim_process_entry(env.cat, env.proxy, &row), MDS_OK);
	ASSERT_EQ(gc_count_of(env.cat), 0);
	ASSERT_EQ(ds_len(env.ds_dir, fid, 0, 0), KIB(4));
	ASSERT_EQ(ds_len(env.ds_dir2, fid, 0, 1), KIB(4));
	ASSERT_TRUE(ds_is_pattern(env.ds_dir, fid, 0, 0, 0, KIB(4)));
	ASSERT_TRUE(ds_is_pattern(env.ds_dir2, fid, 0, 1, 0, KIB(4)));
	env_close(&env);
}

/* -----------------------------------------------------------------------
 * test_trim_double_shrink (T4)
 * ----------------------------------------------------------------------- */

/**
 * Two shrunken sizes in flight: the first row drains the DS tail to
 * the CURRENT size (2K), the second row drains idempotently.  No row
 * is dropped, the flag clears, and the final state is one.
 */
static void test_trim_double_shrink(void)
{
	struct trim_env     env;
	uint64_t            fid;
	struct mds_gc_entry row;

	env = env_open();
	fid = new_file_1x1(env.cat, "dbl", KIB(12), 1);
	ds_data_dir(env.ds_dir);
	ds_write_pattern(env.ds_dir, fid, 0, 0, KIB(12));
	cat_set_size(env.cat, fid, KIB(4));
	enqueue_trim_row(env.cat, fid, 1);
	cat_set_size(env.cat, fid, KIB(2));
	enqueue_trim_row(env.cat, fid, 1);
	ASSERT_EQ(gc_count_of(env.cat), 2);

	ASSERT_TRUE(peek_row(env.cat, fid, &row));
	ASSERT_EQ(trim_process_entry(env.cat, env.proxy, &row), MDS_OK);
	ASSERT_EQ(gc_count_of(env.cat), 1);
	ASSERT_EQ(ds_len(env.ds_dir, fid, 0, 0), KIB(2));

	ASSERT_TRUE(peek_row(env.cat, fid, &row));
	ASSERT_EQ(trim_process_entry(env.cat, env.proxy, &row), MDS_OK);
	ASSERT_EQ(gc_count_of(env.cat), 0);
	ASSERT_EQ(ds_len(env.ds_dir, fid, 0, 0), KIB(2));
	ASSERT_TRUE(ds_is_pattern(env.ds_dir, fid, 0, 0, 0, KIB(2)));
	ASSERT_TRUE(!trim_quiescing(fid));
	env_close(&env);
}

/* -----------------------------------------------------------------------
 * test_trim_missing_mount (T6)
 * ----------------------------------------------------------------------- */

/**
 * A stripe map pointing at a DS with no mount on this MDS: the entry
 * stays queued on every cycle (never a silent OK), the row is never
 * dropped, and the flag stays set while the work is pending.
 */
static void test_trim_missing_mount(void)
{
	struct trim_env     env;
	uint64_t            fid;
	struct mds_gc_entry row;
	int                 i;

	env = env_open();
	/* ds_id 99 is never registered on the proxy. */
	fid = new_file_1x1(env.cat, "nomount", KIB(8), /*ds_id*/ 99U);
	cat_set_size(env.cat, fid, KIB(4));
	enqueue_trim_row(env.cat, fid, /*ds_id*/ 99U);
	ASSERT_TRUE(peek_row(env.cat, fid, &row));

	for (i = 0; i < 3; i++) {
		ASSERT_EQ(trim_process_entry(env.cat, env.proxy, &row),
			MDS_ERR_DELAY);
		ASSERT_EQ(gc_count_of(env.cat), 1);
	}
	ASSERT_TRUE(trim_quiescing(fid));
	env_close(&env);
}

/* -----------------------------------------------------------------------
 * test_trim_refuse_without_recall (fix round 1, finding 2)
 * ----------------------------------------------------------------------- */

/**
 * A layout-serving deployment whose layout_recall_init() failed has
 * no recall coordinator bound: the drain must REFUSE (row stays
 * queued, MDS_ERR_DELAY, rate-limited WARN) instead of skipping the
 * quiescence step and truncating under live client layouts.  The DS
 * file is untouched, and no quiescing flag is left set.
 */
static void test_trim_refuse_without_recall(void)
{
	struct trim_env     env;
	uint64_t            fid;
	struct mds_gc_entry row;
	int                 i;

	ASSERT_EQ(trim_module_init(90, 0), 0);
	/* Layout-serving deployment, no recall coordinator bound
	 * (g_lr NULL, as in every unit test). */
	trim_set_quiescence_required(true);

	env = env_open();
	fid = new_file_1x1(env.cat, "norecall", KIB(8), 1);
	ds_data_dir(env.ds_dir);
	ds_write_pattern(env.ds_dir, fid, 0, 0, KIB(8));
	cat_set_size(env.cat, fid, KIB(4));
	enqueue_trim_row(env.cat, fid, 1);
	ASSERT_TRUE(peek_row(env.cat, fid, &row));

	for (i = 0; i < 3; i++) {
		ASSERT_EQ(trim_process_entry(env.cat, env.proxy, &row),
			MDS_ERR_DELAY);
		ASSERT_EQ(gc_count_of(env.cat), 1);
	}

	/* No truncate (DS length unchanged, tail bytes intact), and
	 * the gate was never closed. */
	ASSERT_EQ(ds_len(env.ds_dir, fid, 0, 0), KIB(8));
	ASSERT_TRUE(ds_is_pattern(env.ds_dir, fid, 0, 0, 0, KIB(8)));
	ASSERT_TRUE(!trim_quiescing(fid));

	trim_set_quiescence_required(false);
	env_close(&env);
}

/* -----------------------------------------------------------------------
 * test_trim_owner_guard (T7)
 * ----------------------------------------------------------------------- */

/**
 * Global visibility + ownership guard: a non-owner MDS's drainer sees
 * the (owner_mds_id = 0) row and leaves it queued, without even
 * quiescing; the owner's drainer drains it; after the ownership moves
 * to the non-owner, ITS drainer drains the same row (successor
 * hand-off).
 */
static void test_trim_owner_guard(void)
{
	struct subtree_map  *smap = NULL;
	struct mds_config   cfg;
	struct mds_catalogue *catA = NULL;
	struct mds_catalogue *catB = NULL;
	struct mds_proxy_ctx *proxy = NULL;
	struct mds_gc_entry  row;
	struct subtree_entry se;
	struct mds_inode     scratch;
	char                *ds_dir = make_ds_dir();
	uint64_t             fidA;
	uint64_t             fidB;

	ASSERT_EQ(subtree_map_init(NULL, NULL, /*self*/ 7U, "hostA", NULL,
		&smap), MDS_OK);
	memset(&cfg, 0, sizeof(cfg));
	cfg.self.id = 7U;
	ASSERT_EQ(catalogue_memdb_open_cfg(&cfg, &catA), MDS_OK);
	cfg.self.id = 8U;
	ASSERT_EQ(catalogue_memdb_open_cfg(&cfg, &catB), MDS_OK);
	ASSERT_EQ(mds_proxy_ctx_create(&proxy), MDS_OK);
	ASSERT_EQ(mds_proxy_mount_set(proxy, /*ds_id*/ 1U, ds_dir), MDS_OK);

	trim_set_subtree_map(smap);

	/* A (self 7) owns the subtree containing its file: it drains. */
	fidA = new_file_1x1(catA, "fa", KIB(8), 1);
	ds_data_dir(ds_dir);
	ds_write_pattern(ds_dir, fidA, 0, 0, KIB(8));
	cat_set_size(catA, fidA, KIB(4));
	enqueue_trim_row(catA, fidA, 1);
	ASSERT_TRUE(peek_row(catA, fidA, &row));
	ASSERT_EQ(trim_process_entry(catA, proxy, &row), MDS_OK);
	ASSERT_EQ(gc_count_of(catA), 0);
	ASSERT_EQ(ds_len(ds_dir, fidA, 0, 0), KIB(4));
	ASSERT_TRUE(!trim_quiescing(fidA));

	/* B (self 8) does not own it: the row stays queued for the
	 * owner, the guard fires before quiescing, and the DS file is
	 * untouched. */
	(void)test_create_file(catB, MDS_FILEID_ROOT, "scratch", 0644,
				&scratch);
	fidB = new_file_1x1(catB, "fb", KIB(8), 1);
	ds_write_pattern(ds_dir, fidB, 0, 0, KIB(8));
	cat_set_size(catB, fidB, KIB(4));
	enqueue_trim_row(catB, fidB, 1);
	ASSERT_TRUE(peek_row(catB, fidB, &row));
	ASSERT_EQ(trim_process_entry(catB, proxy, &row), MDS_ERR_DELAY);
	ASSERT_EQ(gc_count_of(catB), 1);
	ASSERT_TRUE(!trim_quiescing(fidB));
	ASSERT_EQ(ds_len(ds_dir, fidB, 0, 0), KIB(8));

	/* Ownership transfer: B takes over the subtree, then B's
	 * drainer can drain the row (successor hand-off). */
	ASSERT_EQ(subtree_map_lookup(smap, "/", &se), MDS_OK);
	ASSERT_EQ(se.owner_mds_id, 7U);
	ASSERT_EQ(subtree_map_set_owner(smap, "/", 8U, se.version), MDS_OK);
	ASSERT_TRUE(peek_row(catB, fidB, &row));
	ASSERT_EQ(trim_process_entry(catB, proxy, &row), MDS_OK);
	ASSERT_EQ(gc_count_of(catB), 0);
	ASSERT_EQ(ds_len(ds_dir, fidB, 0, 0), KIB(4));
	ASSERT_TRUE(!trim_quiescing(fidB));

	trim_set_subtree_map(NULL);
	subtree_map_destroy(smap);
	test_rm_rf(ds_dir);
	free(ds_dir);
	mds_proxy_ctx_destroy(proxy);
	mds_catalogue_close(catA);
	mds_catalogue_close(catB);
}

/* -----------------------------------------------------------------------
 * test_trim_vanish
 * ----------------------------------------------------------------------- */

/**
 * The file is finally unlinked before the drainer gets to it: the
 * trim dequeues the row as success (nothing to clear), never
 * quiesces, and leaks no rows.
 */
static void test_trim_vanish(void)
{
	struct trim_env     env;
	struct mds_inode    in;
	struct mds_cat_txn *txn = NULL;
	uint64_t            fid;
	struct mds_gc_entry row;

	env = env_open();
	fid = new_file_1x1(env.cat, "vanish", KIB(8), 1);
	ds_data_dir(env.ds_dir);
	ds_write_pattern(env.ds_dir, fid, 0, 0, KIB(8));
	cat_set_size(env.cat, fid, KIB(4));
	enqueue_trim_row(env.cat, fid, 1);
	ASSERT_TRUE(peek_row(env.cat, fid, &row));

	/* Final unlink: dirent + inode gone. */
	ASSERT_EQ(mds_cat_txn_begin(env.cat, MDS_CAT_TXN_WRITE, &txn),
		MDS_OK);
	ASSERT_EQ(mds_cat_dirent_del(env.cat, txn, MDS_FILEID_ROOT,
		"vanish"), MDS_OK);
	ASSERT_EQ(mds_cat_inode_del(env.cat, txn, fid), MDS_OK);
	ASSERT_EQ(mds_cat_txn_commit(txn), MDS_OK);
	ASSERT_EQ(mds_cat_ns_getattr(env.cat, fid, &in), MDS_ERR_NOTFOUND);

	ASSERT_EQ(trim_process_entry(env.cat, env.proxy, &row), MDS_OK);
	ASSERT_EQ(gc_count_of(env.cat), 0);
	ASSERT_TRUE(!trim_quiescing(fid));
	env_close(&env);
}

/* -----------------------------------------------------------------------
 * test_trim_restart
 * ----------------------------------------------------------------------- */

/**
 * A restart mid-trim (flag set, row still queued): the module re-init
 * clears the stale flag, and the new incarnation drains the row from
 * scratch.
 */
static void test_trim_restart(void)
{
	struct trim_env     env;
	uint64_t            fid;
	struct mds_gc_entry row;

	env = env_open();
	fid = new_file_1x1(env.cat, "restart", KIB(8), 1);
	ds_data_dir(env.ds_dir);
	ds_write_pattern(env.ds_dir, fid, 0, 0, KIB(8));
	cat_set_size(env.cat, fid, KIB(4));
	enqueue_trim_row(env.cat, fid, 1);
	ASSERT_TRUE(peek_row(env.cat, fid, &row));

	/* A previous incarnation died mid-trim: flag set, row queued. */
	trim_quiesce_begin(fid);
	ASSERT_TRUE(trim_quiescing(fid));

	/* Simulated restart: tear the module down and bring it back. */
	trim_module_fini();
	ASSERT_EQ(trim_module_init(90, 0), 0);
	ASSERT_TRUE(!trim_quiescing(fid));

	/* Fresh drain from scratch. */
	ASSERT_TRUE(peek_row(env.cat, fid, &row));
	ASSERT_EQ(trim_process_entry(env.cat, env.proxy, &row), MDS_OK);
	ASSERT_EQ(gc_count_of(env.cat), 0);
	ASSERT_EQ(ds_len(env.ds_dir, fid, 0, 0), KIB(4));
	ASSERT_TRUE(ds_is_pattern(env.ds_dir, fid, 0, 0, 0, KIB(4)));
	ASSERT_TRUE(!trim_quiescing(fid));
	env_close(&env);
}

/* -----------------------------------------------------------------------
 * test_trim_ready_gate (fix round 2, C1)
 * ----------------------------------------------------------------------- */

/**
 * A not-ready module (fini'd: the L(F) and flag tables are down, as
 * in the daemon startup window between the drainer thread starting
 * and trim_module_init running) must never execute a drain: the row
 * stays queued, the DS file is untouched, and no quiescing flag is
 * set.  Once the module is initialized again, the same row drains
 * normally.
 */
static void test_trim_ready_gate(void)
{
	struct trim_env     env;
	uint64_t            fid;
	struct mds_gc_entry row;

	ASSERT_EQ(trim_module_init(90, 0), 0);
	env = env_open();
	fid = new_file_1x1(env.cat, "ready", KIB(8), 1);
	ds_data_dir(env.ds_dir);
	ds_write_pattern(env.ds_dir, fid, 0, 0, KIB(8));
	cat_set_size(env.cat, fid, KIB(4));
	enqueue_trim_row(env.cat, fid, 1);
	ASSERT_TRUE(peek_row(env.cat, fid, &row));

	/* Not ready: fini'd, lock/flag tables destroyed. */
	trim_module_fini();
	ASSERT_EQ(trim_process_entry(env.cat, env.proxy, &row),
		MDS_ERR_DELAY);
	ASSERT_EQ(gc_count_of(env.cat), 1);
	/* No truncate, no quiescing flag, no L(F) work. */
	ASSERT_EQ(ds_len(env.ds_dir, fid, 0, 0), KIB(8));
	ASSERT_TRUE(ds_is_pattern(env.ds_dir, fid, 0, 0, 0, KIB(8)));
	ASSERT_TRUE(!trim_quiescing(fid));

	/* Ready again: the same row drains from scratch. */
	ASSERT_EQ(trim_module_init(90, 0), 0);
	ASSERT_EQ(trim_process_entry(env.cat, env.proxy, &row), MDS_OK);
	ASSERT_EQ(gc_count_of(env.cat), 0);
	ASSERT_EQ(ds_len(env.ds_dir, fid, 0, 0), KIB(4));
	ASSERT_TRUE(ds_is_pattern(env.ds_dir, fid, 0, 0, 0, KIB(4)));
	ASSERT_TRUE(!trim_quiescing(fid));
	env_close(&env);
}

/* -----------------------------------------------------------------------
 * test_trim_authoritative_read (T8)
 * ----------------------------------------------------------------------- */

/**
 * With a deliberately lagging hot-read image attached to the
 * catalogue, the trim must truncate to the AUTHORITATIVE size, never
 * the image's.  memdb has no changefeed to lag deterministically, so
 * the image is injected directly (spec V9 fallback): the file is
 * grown so the image still shows the old size, then shrunk to a size
 * LARGER than the image's, and the drain must land on the
 * authoritative value.
 */
static void test_trim_authoritative_read(void)
{
	struct trim_env       env;
	struct catalog_image *img = NULL;
	struct mds_inode      lag;
	struct mds_inode      hot;
	struct mds_inode      auth;
	uint64_t              fid;
	struct mds_gc_entry   row;

	env = env_open();
	fid = new_file_1x1(env.cat, "img", KIB(4), 1);
	ds_data_dir(env.ds_dir);
	ds_write_pattern(env.ds_dir, fid, 0, 0, KIB(4));

	/* Grow to 16K; the image (injected below) keeps the old 4K. */
	cat_set_size(env.cat, fid, KIB(16));
	ds_write_pattern(env.ds_dir, fid, 0, 0, KIB(16));

	ASSERT_EQ(catalog_image_create(&img), 0);
	ASSERT_EQ(mds_cat_ns_getattr(env.cat, fid, &lag), MDS_OK);
	lag.size = KIB(4);
	ASSERT_EQ(catalog_image_inode_put(img, &lag), 0);
	env.cat->image = img;

	/* The image and the authority diverge. */
	ASSERT_EQ(catalog_image_inode_get(img, fid, &hot), 0);
	ASSERT_EQ(mds_cat_ns_getattr(env.cat, fid, &auth), MDS_OK);
	ASSERT_EQ(hot.size, KIB(4));
	ASSERT_EQ(auth.size, KIB(16));

	/* Shrink to 8K (larger than the image's 4K) and drain. */
	cat_set_size(env.cat, fid, KIB(8));
	enqueue_trim_row(env.cat, fid, 1);
	ASSERT_TRUE(peek_row(env.cat, fid, &row));
	ASSERT_EQ(trim_process_entry(env.cat, env.proxy, &row), MDS_OK);

	/* The truncate used the authoritative 8K, not the image's 4K. */
	ASSERT_EQ(gc_count_of(env.cat), 0);
	ASSERT_EQ(ds_len(env.ds_dir, fid, 0, 0), KIB(8));
	ASSERT_TRUE(ds_is_pattern(env.ds_dir, fid, 0, 0, 0, KIB(8)));
	ASSERT_TRUE(!trim_quiescing(fid));

	env.cat->image = NULL;
	catalog_image_destroy(img);
	env_close(&env);
}

/* -----------------------------------------------------------------------
 * Entry point
 * ----------------------------------------------------------------------- */

int main(void)
{
	fprintf(stdout, "trim module state + execution core tests\n");

	ASSERT_EQ(trim_module_init(90, 0), 0);
	RUN_TEST(test_trim_lock_stripped);
	RUN_TEST(test_trim_lock_serializes);
	RUN_TEST(test_trim_flag);
	RUN_TEST(test_trim_max_wait_default);
	RUN_TEST(test_trim_config_parse);

	RUN_TEST(test_trim_p1_proxy_fence);
	RUN_TEST(test_trim_recoverable_failure);
	RUN_TEST(test_trim_backoff_release_inflight);
	RUN_TEST(test_trim_superseding_grow);
	RUN_TEST(test_trim_double_shrink);
	RUN_TEST(test_trim_missing_mount);
	RUN_TEST(test_trim_refuse_without_recall);
	RUN_TEST(test_trim_owner_guard);
	RUN_TEST(test_trim_vanish);
	RUN_TEST(test_trim_restart);
	RUN_TEST(test_trim_ready_gate);
	RUN_TEST(test_trim_authoritative_read);
	trim_module_fini();

	fprintf(stdout, "%d/%d tests passed\n", tests_passed, tests_run);
	return (tests_passed == tests_run) ? 0 : 1;
}
