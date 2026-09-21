/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_proxy_seek_hole.c -- mds_proxy_seek() SEEK_HOLE regression.
 *
 * A stripe unit that is written end to end contains no hole.  The
 * proxy SEEK_HOLE path used to report a hole at the START of such a
 * unit (the last mirror's "no zero byte found" outcome was mapped to
 * "hole at current"); RFC 7862 S15.11 / lseek(2) SEEK_HOLE require
 * the search to advance to the next unit and, when every unit up to
 * logical EOF is data, to report the implicit hole at end-of-file
 * with sr_eof set.  SEEK_DATA is unchanged and covered as a guard.
 *
 * Uses temporary directories as simulated DS mounts, exactly like
 * test_proxy_io.c, with a 2-stripe / 1-mirror map and a 16-byte
 * stripe unit so every case fits in a few bytes.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <inttypes.h>

#include "pnfs_mds.h"
#include "test_helpers.h"
#include "proxy_io.h"

/* ----------------------------------------------------------------------- */
/* Minimal test framework (same shape as test_proxy_io.c). */
/* ----------------------------------------------------------------------- */

static int tests_run    = 0;
static int tests_passed = 0;

#define RUN_TEST(fn) do {                                           \
    tests_run++;                                                     \
    fprintf(stdout, "  %-40s ", #fn);                                \
    fn();                                                            \
    tests_passed++;                                                  \
    fprintf(stdout, "PASS\n");                                       \
} while (0)

#define ASSERT_EQ(a, b) do {                                        \
    if ((a) != (b)) {                                                \
        fprintf(stderr, "FAIL at %s:%d: %s != %s\n",                \
                __FILE__, __LINE__, #a, #b);                         \
        exit(1);                                                     \
    }                                                                \
} while (0)

#define ASSERT_TRUE(cond) do {                                      \
    if (!(cond)) {                                                   \
        fprintf(stderr, "FAIL at %s:%d: !(%s)\n",                   \
                __FILE__, __LINE__, #cond);                          \
        exit(1);                                                     \
    }                                                                \
} while (0)

/* -----------------------------------------------------------------------
 * Fixture: memdb catalogue + two temp-dir DS mounts + 2-stripe map
 * ----------------------------------------------------------------------- */

#define SEEK_TEST_UNIT   16U
#define SEEK_TEST_DS0    1U
#define SEEK_TEST_DS1    2U
#define SEEK_WHAT_DATA   0U
#define SEEK_WHAT_HOLE   1U

struct seek_fixture {
    struct mds_catalogue *db;
    struct mds_proxy_ctx *proxy;
    char *ds0_path;
    char *ds1_path;
    uint64_t fileid;
};

/** Create a temp dir to act as a simulated DS mount. */
static char *make_ds_dir(void)
{
    char *tpl = strdup("/tmp/test_seek_ds_XXXXXX");

    ASSERT_TRUE(tpl != NULL);
    ASSERT_TRUE(mkdtemp(tpl) != NULL);
    return tpl;
}

static void rm_ds_dir(char *path)
{
    char cmd[4200];

    (void)snprintf(cmd, sizeof(cmd), "rm -rf '%s'", path);
    ASSERT_EQ(system(cmd), 0);
    free(path);
}

static void register_ds(struct mds_catalogue *db, uint32_t ds_id)
{
    struct mds_cat_txn *txn = NULL;
    struct mds_ds_info info;

    memset(&info, 0, sizeof(info));
    info.ds_id = ds_id;
    info.state = DS_ONLINE;
    info.port = 2049;
    (void)snprintf(info.addr, sizeof(info.addr), "10.0.0.%u:/ds", ds_id);
    ASSERT_EQ(mds_cat_txn_begin(db, MDS_CAT_TXN_WRITE, &txn), MDS_OK);
    ASSERT_EQ(mds_cat_ds_put(db, txn, &info), MDS_OK);
    ASSERT_EQ(mds_cat_txn_commit(txn), 0);
}

/* 2 stripes x 1 mirror: stripe 0 on DS0, stripe 1 on DS1, both DS
 * backing files created so an unwritten stripe reads as a hole. */
static void fixture_open(struct seek_fixture *fx, uint64_t fileid)
{
    struct mds_cat_txn *txn = NULL;
    struct mds_ds_map_entry entries[2];

    memset(fx, 0, sizeof(*fx));
    fx->fileid = fileid;
    fx->db = open_test_catalogue();
    ASSERT_TRUE(fx->db != NULL);
    fx->ds0_path = make_ds_dir();
    fx->ds1_path = make_ds_dir();

    ASSERT_EQ(mds_proxy_ctx_create(&fx->proxy), MDS_OK);
    ASSERT_EQ(mds_proxy_mount_set(fx->proxy, SEEK_TEST_DS0, fx->ds0_path),
              MDS_OK);
    ASSERT_EQ(mds_proxy_mount_set(fx->proxy, SEEK_TEST_DS1, fx->ds1_path),
              MDS_OK);

    register_ds(fx->db, SEEK_TEST_DS0);
    register_ds(fx->db, SEEK_TEST_DS1);

    memset(entries, 0, sizeof(entries));
    entries[0].ds_id = SEEK_TEST_DS0;
    entries[1].ds_id = SEEK_TEST_DS1;
    ASSERT_EQ(mds_cat_txn_begin(fx->db, MDS_CAT_TXN_WRITE, &txn), MDS_OK);
    ASSERT_EQ(mds_cat_stripe_map_put(fx->db, txn, fileid, 2, SEEK_TEST_UNIT,
                                     1, entries), MDS_OK);
    ASSERT_EQ(mds_cat_txn_commit(txn), 0);

    ASSERT_EQ(mds_proxy_ensure_ds_file(fx->proxy, SEEK_TEST_DS0, fileid,
                                       0, 0), MDS_OK);
    ASSERT_EQ(mds_proxy_ensure_ds_file(fx->proxy, SEEK_TEST_DS1, fileid,
                                       1, 0), MDS_OK);
}

static void fixture_close(struct seek_fixture *fx)
{
    mds_proxy_ctx_destroy(fx->proxy);
    mds_catalogue_close(fx->db);
    rm_ds_dir(fx->ds0_path);
    rm_ds_dir(fx->ds1_path);
}

/** Write @len copies of @fill at logical @offset through the proxy. */
static void write_fill(const struct seek_fixture *fx, uint64_t offset,
                       uint32_t len, char fill)
{
    char buf[64];
    uint32_t bytes = 0;

    ASSERT_TRUE(len <= sizeof(buf));
    memset(buf, fill, len);
    ASSERT_EQ(mds_proxy_write(fx->proxy, fx->db, fx->fileid, offset,
                              buf, len, &bytes), MDS_OK);
    ASSERT_EQ(bytes, len);
}

/** Run one SEEK and check (offset, eof). */
static void expect_seek(const struct seek_fixture *fx, uint64_t logical_size,
                        uint64_t from, uint32_t what,
                        uint64_t want_offset, bool want_eof)
{
    uint64_t got = UINT64_MAX;
    bool eof = !want_eof;

    ASSERT_EQ(mds_proxy_seek(fx->proxy, fx->db, fx->fileid, logical_size,
                             from, what, &got, &eof), MDS_OK);
    ASSERT_EQ(got, want_offset);
    ASSERT_EQ(eof, want_eof);
}

/* -----------------------------------------------------------------------
 * test_seek_hole_after_full_unit -- unit [0,16) fully written, unit
 * [16,32) never written, data again at [32,36).  The first hole is at
 * 16, not at the start of the written unit.
 * ----------------------------------------------------------------------- */

static void test_seek_hole_after_full_unit(void)
{
    struct seek_fixture fx;
    const uint64_t logical_size = 36;

    fixture_open(&fx, 500);
    write_fill(&fx, 0, SEEK_TEST_UNIT, 'A');
    write_fill(&fx, 32, 4, 'B');

    expect_seek(&fx, logical_size, 0, SEEK_WHAT_HOLE, 16, false);
    expect_seek(&fx, logical_size, 5, SEEK_WHAT_HOLE, 16, false);
    expect_seek(&fx, logical_size, 16, SEEK_WHAT_HOLE, 16, false);
    /* [32,36) is data up to logical EOF: the hole is the EOF one. */
    expect_seek(&fx, logical_size, 32, SEEK_WHAT_HOLE, 36, true);

    /* SEEK_DATA guard: the hole unit is skipped, data resumes at 32. */
    expect_seek(&fx, logical_size, 16, SEEK_WHAT_DATA, 32, false);
    expect_seek(&fx, logical_size, 0, SEEK_WHAT_DATA, 0, false);

    fixture_close(&fx);
}

/* -----------------------------------------------------------------------
 * test_seek_hole_all_data_eof -- both units written end to end: no hole
 * before EOF, so SEEK_HOLE reports the implicit hole at logical_size
 * with eof set (lseek(2) SEEK_HOLE / knfsd behaviour).
 * ----------------------------------------------------------------------- */

static void test_seek_hole_all_data_eof(void)
{
    struct seek_fixture fx;
    const uint64_t logical_size = 32;

    fixture_open(&fx, 501);
    write_fill(&fx, 0, SEEK_TEST_UNIT, 'A');
    write_fill(&fx, 16, SEEK_TEST_UNIT, 'C');

    expect_seek(&fx, logical_size, 0, SEEK_WHAT_HOLE, 32, true);
    expect_seek(&fx, logical_size, 20, SEEK_WHAT_HOLE, 32, true);
    expect_seek(&fx, logical_size, 31, SEEK_WHAT_HOLE, 32, true);

    /* SEEK_DATA guard. */
    expect_seek(&fx, logical_size, 0, SEEK_WHAT_DATA, 0, false);
    expect_seek(&fx, logical_size, 20, SEEK_WHAT_DATA, 20, false);

    fixture_close(&fx);
}

/* -----------------------------------------------------------------------
 * test_seek_hole_in_first_unit -- unchanged behaviour: data [0,4) then
 * the DS file ends, so the hole starts at 4 (short-read path); a seek
 * from inside the hole answers its own offset.
 * ----------------------------------------------------------------------- */

static void test_seek_hole_in_first_unit(void)
{
    struct seek_fixture fx;
    const uint64_t logical_size = 20;

    fixture_open(&fx, 502);
    write_fill(&fx, 0, 4, 'D');
    write_fill(&fx, 16, 4, 'E');

    expect_seek(&fx, logical_size, 0, SEEK_WHAT_HOLE, 4, false);
    expect_seek(&fx, logical_size, 2, SEEK_WHAT_HOLE, 4, false);
    expect_seek(&fx, logical_size, 4, SEEK_WHAT_HOLE, 4, false);

    /* SEEK_DATA guard: from inside the hole, data resumes at 16. */
    expect_seek(&fx, logical_size, 4, SEEK_WHAT_DATA, 16, false);

    fixture_close(&fx);
}

/* -----------------------------------------------------------------------
 * main
 * ----------------------------------------------------------------------- */

int main(void)
{
    fprintf(stdout, "Running proxy SEEK_HOLE tests:\n");

    RUN_TEST(test_seek_hole_after_full_unit);
    RUN_TEST(test_seek_hole_all_data_eof);
    RUN_TEST(test_seek_hole_in_first_unit);

    fprintf(stdout, "\n%d/%d tests passed.\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
