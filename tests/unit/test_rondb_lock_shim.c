/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_rondb_lock_shim.c — Stage E RonDB lock shim behaviour tests.
 *
 * Exercises shared/exclusive compatibility, ownership-safe release,
 * and fencing-epoch propagation against a live RonDB cluster.
 *
 * Requires:
 *   - Built with ENABLE_RONDB=ON (HAVE_RONDB defined)
 *   - Runtime: PNFS_MDS_RONDB_TEST_CONF env var pointing to a valid
 *     RonDB backend config file (connect_string + schema_name)
 *
 * If the env var is not set, all tests are skipped (exit 0).
 */

#ifdef HAVE_RONDB

#include <stdio.h>
#include <stdlib.h>

#include "catalogue_rondb.h"
#include "rondb_schema.h"

static int tests_run;
static int tests_passed;
static int g_test_failed;
static const char *g_rondb_conf_path;

#define ASSERT_EQ(a, b) do {						\
	if ((a) != (b)) {						\
		fprintf(stderr, "  FAIL %s:%d: %s (%d) != %s (%d)\n",	\
			__FILE__, __LINE__, #a, (int)(a), #b, (int)(b));\
		g_test_failed = 1;					\
		return;							\
	}								\
} while (0)

#define ASSERT_EQ_U64(a, b) do {					\
	if ((a) != (b)) {						\
		fprintf(stderr, "  FAIL %s:%d: %s != %s\n",		\
			__FILE__, __LINE__, #a, #b);			\
		g_test_failed = 1;					\
		return;							\
	}								\
} while (0)

#define ASSERT_NE(a, b) do {						\
	if ((a) == (b)) {						\
		fprintf(stderr, "  FAIL %s:%d: %s == %s\n",		\
			__FILE__, __LINE__, #a, #b);			\
		g_test_failed = 1;					\
		return;							\
	}								\
} while (0)

#define ASSERT_TRUE(x)  ASSERT_NE((x), 0)

#define RUN_TEST(fn) do {						\
	tests_run++;							\
	g_test_failed = 0;						\
	fprintf(stdout, "  %-50s", #fn);				\
	fflush(stdout);							\
	fn();								\
	if (g_test_failed) {						\
		fprintf(stdout, "FAIL\n");				\
	} else {							\
		tests_passed++;						\
		fprintf(stdout, "PASS\n");				\
	}								\
} while (0)

static void *open_rondb_test_handle(struct mds_rondb_config *cfg)
{
	void *handle = NULL;

	if (mds_rondb_config_load(g_rondb_conf_path, cfg) != MDS_OK) {
		fprintf(stderr, "FATAL: mds_rondb_config_load failed\n");
		exit(1);
	}
	if (rondb_shim_connect(cfg->connect_string, cfg->schema_name,
			       &handle) != 0 || handle == NULL) {
		fprintf(stderr, "FATAL: rondb_shim_connect failed\n");
		exit(1);
	}
	if (rondb_shim_bootstrap_metadata(handle, cfg->schema_name) != 0) {
		fprintf(stderr, "FATAL: rondb_shim_bootstrap_metadata failed\n");
		rondb_shim_disconnect(handle);
		exit(1);
	}

	return handle;
}

static void close_rondb_test_handle(void *handle,
				    const struct mds_rondb_config *cfg)
{
	(void)rondb_shim_cleanup_metadata(handle, cfg->schema_name);
	rondb_shim_disconnect(handle);
}

static void test_shared_lock_allows_multiple_holders(void)
{
	struct mds_rondb_config cfg;
	uint8_t key[RONDB_LOCK_KEY_MAX];
	uint8_t mode = 0;
	uint64_t part_hint = 0;
	uint64_t fence = 0;
	uint64_t owner_epoch = 1234;
	uint32_t holder_count = 0;
	uint32_t owner_mds = 99;
	int len;
	void *handle;

	handle = open_rondb_test_handle(&cfg);

	len = rondb_lock_res_topology(key, sizeof(key), &part_hint);
	ASSERT_TRUE(len > 0);

	ASSERT_EQ(rondb_shim_lock_acquire(handle, part_hint,
					  RONDB_LOCK_CLASS_TOPOLOGY,
					  key, (uint32_t)len,
					  RONDB_LOCK_MODE_SHARED,
					  1, 10, 100, 5000), 0);
	ASSERT_EQ(rondb_shim_lock_acquire(handle, part_hint,
					  RONDB_LOCK_CLASS_TOPOLOGY,
					  key, (uint32_t)len,
					  RONDB_LOCK_MODE_SHARED,
					  2, 20, 200, 4000), 0);

	ASSERT_EQ(rondb_shim_lock_test(handle, RONDB_LOCK_CLASS_TOPOLOGY,
				       key, (uint32_t)len,
				       &mode, &holder_count, &fence,
				       &owner_mds, &owner_epoch), 0);
	ASSERT_EQ(mode, RONDB_LOCK_MODE_SHARED);
	ASSERT_EQ(holder_count, 2);
	ASSERT_EQ_U64(fence, (uint64_t)200);
	ASSERT_EQ(owner_mds, 0);
	ASSERT_EQ_U64(owner_epoch, (uint64_t)0);

	ASSERT_EQ(rondb_shim_lock_release(handle, RONDB_LOCK_CLASS_TOPOLOGY,
					  key, (uint32_t)len,
					  1, 10), 0);
	ASSERT_EQ(rondb_shim_lock_test(handle, RONDB_LOCK_CLASS_TOPOLOGY,
				       key, (uint32_t)len,
				       &mode, &holder_count, &fence,
				       &owner_mds, &owner_epoch), 0);
	ASSERT_EQ(mode, RONDB_LOCK_MODE_SHARED);
	ASSERT_EQ(holder_count, 1);
	ASSERT_EQ_U64(fence, (uint64_t)200);

	ASSERT_EQ(rondb_shim_lock_release(handle, RONDB_LOCK_CLASS_TOPOLOGY,
					  key, (uint32_t)len,
					  2, 20), 0);
	ASSERT_EQ(rondb_shim_lock_test(handle, RONDB_LOCK_CLASS_TOPOLOGY,
				       key, (uint32_t)len,
				       &mode, &holder_count, &fence,
				       &owner_mds, &owner_epoch), 1);

	close_rondb_test_handle(handle, &cfg);
}

static void test_release_requires_owner_identity(void)
{
	struct mds_rondb_config cfg;
	uint8_t key[RONDB_LOCK_KEY_MAX];
	uint8_t mode = 0;
	uint64_t part_hint = 0;
	uint64_t fence = 0;
	uint64_t owner_epoch = 0;
	uint32_t holder_count = 0;
	uint32_t owner_mds = 0;
	int len;
	void *handle;

	handle = open_rondb_test_handle(&cfg);

	len = rondb_lock_res_dir(77, key, sizeof(key), &part_hint);
	ASSERT_TRUE(len > 0);

	ASSERT_EQ(rondb_shim_lock_acquire(handle, part_hint,
					  RONDB_LOCK_CLASS_DIR_MUTATION,
					  key, (uint32_t)len,
					  RONDB_LOCK_MODE_EXCLUSIVE,
					  11, 110, 900, 5000), 0);

	ASSERT_EQ(rondb_shim_lock_release(handle,
					  RONDB_LOCK_CLASS_DIR_MUTATION,
					  key, (uint32_t)len,
					  11, 111), 0);
	ASSERT_EQ(rondb_shim_lock_test(handle,
				       RONDB_LOCK_CLASS_DIR_MUTATION,
				       key, (uint32_t)len,
				       &mode, &holder_count, &fence,
				       &owner_mds, &owner_epoch), 0);
	ASSERT_EQ(mode, RONDB_LOCK_MODE_EXCLUSIVE);
	ASSERT_EQ(holder_count, 1);
	ASSERT_EQ(owner_mds, 11);
	ASSERT_EQ_U64(owner_epoch, (uint64_t)110);
	ASSERT_EQ_U64(fence, (uint64_t)900);

	ASSERT_EQ(rondb_shim_lock_release(handle,
					  RONDB_LOCK_CLASS_DIR_MUTATION,
					  key, (uint32_t)len,
					  11, 110), 0);
	ASSERT_EQ(rondb_shim_lock_test(handle,
				       RONDB_LOCK_CLASS_DIR_MUTATION,
				       key, (uint32_t)len,
				       &mode, &holder_count, &fence,
				       &owner_mds, &owner_epoch), 1);

	close_rondb_test_handle(handle, &cfg);
}

static void test_exclusive_and_shared_conflict_rules(void)
{
	struct mds_rondb_config cfg;
	uint8_t key[RONDB_LOCK_KEY_MAX];
	uint8_t mode = 0;
	uint64_t part_hint = 0;
	uint64_t fence = 0;
	uint64_t owner_epoch = 0;
	uint32_t holder_count = 0;
	uint32_t owner_mds = 0;
	int len;
	void *handle;

	handle = open_rondb_test_handle(&cfg);

	len = rondb_lock_res_inode(42, key, sizeof(key), &part_hint);
	ASSERT_TRUE(len > 0);

	ASSERT_EQ(rondb_shim_lock_acquire(handle, part_hint,
					  RONDB_LOCK_CLASS_INODE_ATTR,
					  key, (uint32_t)len,
					  RONDB_LOCK_MODE_SHARED,
					  1, 1, 101, 5000), 0);
	ASSERT_EQ(rondb_shim_lock_acquire(handle, part_hint,
					  RONDB_LOCK_CLASS_INODE_ATTR,
					  key, (uint32_t)len,
					  RONDB_LOCK_MODE_EXCLUSIVE,
					  2, 2, 202, 5000), 1);
	ASSERT_EQ(rondb_shim_lock_release(handle,
					  RONDB_LOCK_CLASS_INODE_ATTR,
					  key, (uint32_t)len,
					  1, 1), 0);

	ASSERT_EQ(rondb_shim_lock_acquire(handle, part_hint,
					  RONDB_LOCK_CLASS_INODE_ATTR,
					  key, (uint32_t)len,
					  RONDB_LOCK_MODE_EXCLUSIVE,
					  2, 2, 202, 5000), 0);
	ASSERT_EQ(rondb_shim_lock_test(handle,
				       RONDB_LOCK_CLASS_INODE_ATTR,
				       key, (uint32_t)len,
				       &mode, &holder_count, &fence,
				       &owner_mds, &owner_epoch), 0);
	ASSERT_EQ(mode, RONDB_LOCK_MODE_EXCLUSIVE);
	ASSERT_EQ(holder_count, 1);
	ASSERT_EQ(owner_mds, 2);
	ASSERT_EQ_U64(owner_epoch, (uint64_t)2);
	ASSERT_EQ_U64(fence, (uint64_t)202);

	ASSERT_EQ(rondb_shim_lock_acquire(handle, part_hint,
					  RONDB_LOCK_CLASS_INODE_ATTR,
					  key, (uint32_t)len,
					  RONDB_LOCK_MODE_SHARED,
					  3, 3, 303, 5000), 1);

	ASSERT_EQ(rondb_shim_lock_acquire(handle, part_hint,
					  RONDB_LOCK_CLASS_INODE_ATTR,
					  key, (uint32_t)len,
					  RONDB_LOCK_MODE_EXCLUSIVE,
					  2, 2, 777, 7000), 0);
	ASSERT_EQ(rondb_shim_lock_test(handle,
				       RONDB_LOCK_CLASS_INODE_ATTR,
				       key, (uint32_t)len,
				       &mode, &holder_count, &fence,
				       &owner_mds, &owner_epoch), 0);
	ASSERT_EQ(mode, RONDB_LOCK_MODE_EXCLUSIVE);
	ASSERT_EQ(holder_count, 1);
	ASSERT_EQ(owner_mds, 2);
	ASSERT_EQ_U64(owner_epoch, (uint64_t)2);
	ASSERT_EQ_U64(fence, (uint64_t)777);

	ASSERT_EQ(rondb_shim_lock_release(handle,
					  RONDB_LOCK_CLASS_INODE_ATTR,
					  key, (uint32_t)len,
					  2, 2), 0);
	ASSERT_EQ(rondb_shim_lock_test(handle,
				       RONDB_LOCK_CLASS_INODE_ATTR,
				       key, (uint32_t)len,
				       &mode, &holder_count, &fence,
				       &owner_mds, &owner_epoch), 1);

	close_rondb_test_handle(handle, &cfg);
}

int main(void)
{
	g_rondb_conf_path = getenv("PNFS_MDS_RONDB_TEST_CONF");
	if (g_rondb_conf_path == NULL || g_rondb_conf_path[0] == '\0') {
		fprintf(stdout,
			"test_rondb_lock_shim: SKIP (set "
			"PNFS_MDS_RONDB_TEST_CONF to enable)\n");
		return 0;
	}

	fprintf(stdout, "test_rondb_lock_shim (conf=%s):\n",
		g_rondb_conf_path);

	RUN_TEST(test_shared_lock_allows_multiple_holders);
	RUN_TEST(test_release_requires_owner_identity);
	RUN_TEST(test_exclusive_and_shared_conflict_rules);

	fprintf(stdout, "\ntest_rondb_lock_shim: %d/%d passed\n",
		tests_passed, tests_run);
	return (tests_passed == tests_run) ? 0 : 1;
}

#else /* !HAVE_RONDB */

#include <stdio.h>

int main(void)
{
	fprintf(stdout,
		"test_rondb_lock_shim: SKIP (built without ENABLE_RONDB)\n");
	return 0;
}

#endif /* HAVE_RONDB */
