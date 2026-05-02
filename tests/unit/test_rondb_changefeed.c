/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_rondb_changefeed.c — Live RonDB test for Phase 9C changefeed.
 *
 * Tests delta_insert, delta_poll, delta_trim, delta_seqno_load/save
 * against a real RonDB cluster.  Set PNFS_MDS_RONDB_TEST_CONF to enable.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "pnfs_mds.h"

#ifndef HAVE_RONDB
int main(void)
{
    fprintf(stdout, "test_rondb_changefeed: SKIP (HAVE_RONDB not defined)\n");
    return 0;
}
#else

#include "catalogue_rondb.h"

static int passed, failed;

#define ASSERT_EQ(a, b) do { \
    if ((a) != (b)) { \
        fprintf(stderr, "  FAIL %s:%d: %s (%d) != %s (%d)\n", \
                __FILE__, __LINE__, #a, (int)(a), #b, (int)(b)); \
        failed++; return; \
    } \
} while (0)

#define ASSERT_EQ_U64(a, b) do { \
    if ((a) != (b)) { \
        fprintf(stderr, "  FAIL %s:%d: %s (%llu) != %s (%llu)\n", \
                __FILE__, __LINE__, #a, (unsigned long long)(a), \
                #b, (unsigned long long)(b)); \
        failed++; return; \
    } \
} while (0)

#define ASSERT_TRUE(c) do { \
    if (!(c)) { \
        fprintf(stderr, "  FAIL %s:%d: !(%s)\n", \
                __FILE__, __LINE__, #c); \
        failed++; return; \
    } \
} while (0)

#define PASS() do { passed++; fprintf(stdout, "  PASS\n"); } while (0)

static void *g_handle;

/* -----------------------------------------------------------------------
 * test_delta_insert_and_poll
 *
 * Insert 3 delta records for MDS 99, then poll and verify all 3 come
 * back in seqno order.
 * ----------------------------------------------------------------------- */

struct poll_ctx {
    uint64_t seqnos[16];
    uint8_t  types[16];
    uint32_t count;
};

static int poll_cb(uint64_t seqno, uint64_t boot_epoch,
                   uint8_t delta_type,
                   const void *payload, uint32_t payload_len,
                   uint64_t timestamp_ns, void *ctx)
{
    struct poll_ctx *pc = ctx;
    (void)boot_epoch;
    (void)payload;
    (void)payload_len;
    (void)timestamp_ns;

    if (pc->count < 16) {
        pc->seqnos[pc->count] = seqno;
        pc->types[pc->count] = delta_type;
    }
    pc->count++;
    return 0;
}

static void test_delta_insert_and_poll(void)
{
    fprintf(stdout, "  test_delta_insert_and_poll:      ");
    fflush(stdout);

    uint8_t payload[] = { 0xCA, 0xFE };

    /* Insert 3 records for source_mds_id=99. */
    ASSERT_EQ(rondb_shim_delta_insert(g_handle, 99, 1, 1000, 1,
                                       payload, 2, 100), 0);
    ASSERT_EQ(rondb_shim_delta_insert(g_handle, 99, 2, 1000, 3,
                                       payload, 2, 200), 0);
    ASSERT_EQ(rondb_shim_delta_insert(g_handle, 99, 3, 1000, 5,
                                       NULL, 0, 300), 0);

    /* Poll: seqno > 0, should get all 3. */
    struct poll_ctx pc;
    memset(&pc, 0, sizeof(pc));
    ASSERT_EQ(rondb_shim_delta_poll(g_handle, 99, 0, 100, poll_cb, &pc), 0);
    ASSERT_EQ(pc.count, 3);
    ASSERT_EQ_U64(pc.seqnos[0], 1);
    ASSERT_EQ_U64(pc.seqnos[1], 2);
    ASSERT_EQ_U64(pc.seqnos[2], 3);
    ASSERT_EQ(pc.types[0], 1);
    ASSERT_EQ(pc.types[1], 3);
    ASSERT_EQ(pc.types[2], 5);

    /* Poll: seqno > 2, should get only seqno 3. */
    memset(&pc, 0, sizeof(pc));
    ASSERT_EQ(rondb_shim_delta_poll(g_handle, 99, 2, 100, poll_cb, &pc), 0);
    ASSERT_EQ(pc.count, 1);
    ASSERT_EQ_U64(pc.seqnos[0], 3);

    /* Poll: different source MDS, should get 0. */
    memset(&pc, 0, sizeof(pc));
    ASSERT_EQ(rondb_shim_delta_poll(g_handle, 88, 0, 100, poll_cb, &pc), 0);
    ASSERT_EQ(pc.count, 0);

    PASS();
}

/* -----------------------------------------------------------------------
 * test_delta_trim
 *
 * Trim seqno <= 2 for MDS 99, then verify only seqno 3 remains.
 * ----------------------------------------------------------------------- */

static void test_delta_trim(void)
{
    fprintf(stdout, "  test_delta_trim:                ");
    fflush(stdout);

    ASSERT_EQ(rondb_shim_delta_trim(g_handle, 99, 2), 0);

    struct poll_ctx pc;
    memset(&pc, 0, sizeof(pc));
    ASSERT_EQ(rondb_shim_delta_poll(g_handle, 99, 0, 100, poll_cb, &pc), 0);
    ASSERT_EQ(pc.count, 1);
    ASSERT_EQ_U64(pc.seqnos[0], 3);

    /* Clean up remaining row. */
    ASSERT_EQ(rondb_shim_delta_trim(g_handle, 99, 3), 0);

    memset(&pc, 0, sizeof(pc));
    ASSERT_EQ(rondb_shim_delta_poll(g_handle, 99, 0, 100, poll_cb, &pc), 0);
    ASSERT_EQ(pc.count, 0);

    PASS();
}

/* -----------------------------------------------------------------------
 * test_delta_seqno_save_load
 *
 * Save a seqno counter, load it back, verify round-trip.
 * ----------------------------------------------------------------------- */

static void test_delta_seqno_save_load(void)
{
    fprintf(stdout, "  test_delta_seqno_save_load:     ");
    fflush(stdout);

    ASSERT_EQ(rondb_shim_delta_seqno_save(g_handle, 42, 12345), 0);

    uint64_t loaded = 0;
    ASSERT_EQ(rondb_shim_delta_seqno_load(g_handle, 42, &loaded), 0);
    ASSERT_EQ_U64(loaded, 12345);

    /* Overwrite with higher value. */
    ASSERT_EQ(rondb_shim_delta_seqno_save(g_handle, 42, 99999), 0);
    ASSERT_EQ(rondb_shim_delta_seqno_load(g_handle, 42, &loaded), 0);
    ASSERT_EQ_U64(loaded, 99999);

    PASS();
}

/* -----------------------------------------------------------------------
 * test_delta_duplicate_insert_is_idempotent
 *
 * Insert the same (source_mds_id, seqno) twice — should succeed
 * (constraint violation treated as success).
 * ----------------------------------------------------------------------- */

static void test_delta_duplicate_insert(void)
{
    fprintf(stdout, "  test_delta_duplicate_insert:    ");
    fflush(stdout);

    uint8_t payload[] = { 0x01 };
    ASSERT_EQ(rondb_shim_delta_insert(g_handle, 77, 1, 2000, 1,
                                       payload, 1, 100), 0);
    /* Duplicate — should return 0 (idempotent). */
    ASSERT_EQ(rondb_shim_delta_insert(g_handle, 77, 1, 2000, 1,
                                       payload, 1, 100), 0);

    /* Clean up. */
    ASSERT_EQ(rondb_shim_delta_trim(g_handle, 77, 1), 0);
    PASS();
}

/* -----------------------------------------------------------------------
 * main
 * ----------------------------------------------------------------------- */

int main(void)
{
    const char *conf = getenv("PNFS_MDS_RONDB_TEST_CONF");
    if (conf == NULL || conf[0] == '\0') {
        fprintf(stdout,
                "test_rondb_changefeed: SKIP "
                "(set PNFS_MDS_RONDB_TEST_CONF to enable)\n");
        return 0;
    }

    struct mds_rondb_config cfg;
    if (mds_rondb_config_load(conf, &cfg) != MDS_OK) {
        fprintf(stderr, "FATAL: failed to load %s\n", conf);
        return 1;
    }

    if (rondb_shim_connect(cfg.connect_string, cfg.schema_name,
                           &g_handle) != 0) {
        fprintf(stderr, "FATAL: rondb_shim_connect failed\n");
        return 1;
    }

    /* Bootstrap ensures delta_broadcast table exists. */
    if (rondb_shim_bootstrap_metadata(g_handle, cfg.schema_name) != 0) {
        fprintf(stderr, "FATAL: bootstrap failed\n");
        rondb_shim_disconnect(g_handle);
        return 1;
    }

    fprintf(stdout, "test_rondb_changefeed (conf=%s):\n", conf);

    test_delta_insert_and_poll();
    test_delta_trim();
    test_delta_seqno_save_load();
    test_delta_duplicate_insert();

    rondb_shim_cleanup_metadata(g_handle, cfg.schema_name);
    rondb_shim_disconnect(g_handle);

    fprintf(stdout, "\ntest_rondb_changefeed: %d/%d passed\n",
            passed, passed + failed);
    return failed > 0 ? 1 : 0;
}

#endif /* HAVE_RONDB */
