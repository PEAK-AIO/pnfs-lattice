/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_repl_health.c -- Unit tests for replication health monitoring
 *                      and compound write refusal (S23 item 38a).
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "pnfs_mds.h"
#include "health.h"

/* -------------------------------------------------------------------
 * Helpers
 * ------------------------------------------------------------------- */

static int passed;
static int failed;

#define ASSERT_EQ(a, b) do { \
	if ((a) != (b)) { \
		fprintf(stderr, "  FAIL %s:%d: %s != %s\n", \
			__FILE__, __LINE__, #a, #b); \
		failed++; \
		return; \
	} \
} while (0)

#define ASSERT_NE(a, b) do { \
	if ((a) == (b)) { \
		fprintf(stderr, "  FAIL %s:%d: %s == %s\n", \
			__FILE__, __LINE__, #a, #b); \
		failed++; \
		return; \
	} \
} while (0)

#define ASSERT_TRUE(cond) do { \
	if (!(cond)) { \
		fprintf(stderr, "  FAIL %s:%d: !(%s)\n", \
			__FILE__, __LINE__, #cond); \
		failed++; \
		return; \
	} \
} while (0)

#define PASS() do { \
	passed++; \
	fprintf(stdout, "  PASS\n"); \
} while (0)

/* -------------------------------------------------------------------
 * test_health_init_destroy
 *
 * Verify lifecycle with NULL conn (no standby).  Must not crash
 * and must report healthy.
 * ------------------------------------------------------------------- */

static void test_health_init_destroy(void)
{
	fprintf(stdout, "  test_health_init_destroy: ");

	struct health_monitor *hm = NULL;
	int rc;

	/* NULL out pointer -> error. */
	rc = health_monitor_init(NULL, 5000, false, NULL);
	ASSERT_EQ(rc, -1);

	/* Normal init with no conn. */
	rc = health_monitor_init(NULL, 1000, false, &hm);
	ASSERT_EQ(rc, 0);
	ASSERT_NE(hm, NULL);

	/* Should be healthy (no conn = always healthy). */
	ASSERT_TRUE(health_repl_is_ok(hm));
	ASSERT_TRUE(!health_writes_blocked(hm));

	/* Destroy without start -- must be safe. */
	health_monitor_destroy(hm);

	/* Destroy NULL -- must be safe. */
	health_monitor_destroy(NULL);

	PASS();
}

/* -------------------------------------------------------------------
 * test_health_repl_ok_default
 *
 * Start the monitor with NULL conn and verify it stays healthy
 * after a polling cycle.
 * ------------------------------------------------------------------- */

static void test_health_repl_ok_default(void)
{
	fprintf(stdout, "  test_health_repl_ok_default: ");

	struct health_monitor *hm = NULL;
	int rc;

	rc = health_monitor_init(NULL, 100, false, &hm);
	ASSERT_EQ(rc, 0);

	rc = health_monitor_start(hm);
	ASSERT_EQ(rc, 0);

	/* Let the thread run a few cycles. */
	usleep(350000);

	/* Still healthy -- NULL conn never sets resync. */
	ASSERT_TRUE(health_repl_is_ok(hm));
	ASSERT_TRUE(!health_writes_blocked(hm));

	health_monitor_destroy(hm);

	PASS();
}

/* -------------------------------------------------------------------
 * test_health_detects_resync
 *
 * Use test_set_repl_ok() to simulate the resync flag transition
 * and verify health_repl_is_ok() / health_writes_blocked() respond.
 * ------------------------------------------------------------------- */

static void test_health_detects_resync(void)
{
	fprintf(stdout, "  test_health_detects_resync: ");

	struct health_monitor *hm = NULL;
	int rc;

	/* refuse_writes = true so health_writes_blocked() can fire. */
	rc = health_monitor_init(NULL, 100, true, &hm);
	ASSERT_EQ(rc, 0);

	/* Default: healthy. */
	ASSERT_TRUE(health_repl_is_ok(hm));
	ASSERT_TRUE(!health_writes_blocked(hm));

	/* Simulate resync needed. */
	health_monitor_test_set_repl_ok(hm, 0);
	ASSERT_TRUE(!health_repl_is_ok(hm));
	ASSERT_TRUE(health_writes_blocked(hm));

	/* Simulate recovery. */
	health_monitor_test_set_repl_ok(hm, 1);
	ASSERT_TRUE(health_repl_is_ok(hm));
	ASSERT_TRUE(!health_writes_blocked(hm));

	health_monitor_destroy(hm);

	PASS();
}

/* -------------------------------------------------------------------
 * test_health_refuse_writes_config
 *
 * Verify that refuse_writes=false prevents health_writes_blocked()
 * from returning true even when replication is unhealthy.
 * ------------------------------------------------------------------- */

static void test_health_refuse_writes_config(void)
{
	fprintf(stdout, "  test_health_refuse_writes_config: ");

	struct health_monitor *hm = NULL;
	int rc;

	/* refuse_writes = false. */
	rc = health_monitor_init(NULL, 100, false, &hm);
	ASSERT_EQ(rc, 0);

	/* Even if repl is unhealthy, writes are NOT blocked. */
	health_monitor_test_set_repl_ok(hm, 0);
	ASSERT_TRUE(!health_repl_is_ok(hm));
	ASSERT_TRUE(!health_writes_blocked(hm));  /* key assertion */

	health_monitor_destroy(hm);

	PASS();
}

/* -------------------------------------------------------------------
 * test_compound_write_refused
 *
 * Verify that health_writes_blocked() returns true when replication
 * is in resync state and refuse_writes is enabled -- this is the
 * function compound.c calls to gate mutating ops.
 * ------------------------------------------------------------------- */

static void test_compound_write_refused(void)
{
	fprintf(stdout, "  test_compound_write_refused: ");

	struct health_monitor *hm = NULL;
	int rc = health_monitor_init(NULL, 100, true, &hm);
	ASSERT_EQ(rc, 0);

	/* Force resync state. */
	health_monitor_test_set_repl_ok(hm, 0);

	/* Writes must be blocked. */
	ASSERT_EQ(health_writes_blocked(hm), true);

	health_monitor_destroy(hm);
	PASS();
}

/* -------------------------------------------------------------------
 * test_compound_write_allowed
 *
 * Verify that health_writes_blocked() returns false when replication
 * is healthy, even with refuse_writes enabled.
 * ------------------------------------------------------------------- */

static void test_compound_write_allowed(void)
{
	fprintf(stdout, "  test_compound_write_allowed: ");

	struct health_monitor *hm = NULL;
	int rc = health_monitor_init(NULL, 100, true, &hm);
	ASSERT_EQ(rc, 0);

	/* Default: repl_ok=1 -> healthy -> writes allowed. */
	ASSERT_EQ(health_writes_blocked(hm), false);

	health_monitor_destroy(hm);
	PASS();
}


/* -------------------------------------------------------------------
 * main
 * ------------------------------------------------------------------- */

int main(void)
{
	fprintf(stdout, "Running replication health tests:\n");
	test_health_init_destroy();
	test_health_repl_ok_default();
	test_health_detects_resync();
	test_health_refuse_writes_config();
	test_compound_write_refused();
	test_compound_write_allowed();
	fprintf(stdout, "\nReplication health tests: %d passed, %d failed\n",
		passed, failed);
	return failed > 0 ? 1 : 0;
}
