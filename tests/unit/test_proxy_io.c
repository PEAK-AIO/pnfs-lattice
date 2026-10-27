/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_proxy_io.c -- Unit tests for non-pNFS client proxy I/O.
 *
 * Uses temporary directories as simulated DS mounts so that the
 * proxy_read / proxy_write code exercises the full stripe-aware
 * and mirror-aware path without real NFS.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <inttypes.h>
#include <pthread.h>

#include "pnfs_mds.h"
#include "test_helpers.h"
#include "proxy_io.h"
#include "compound.h"
#include "open_state.h"
#include "trim_tail.h"

/* ----------------------------------------------------------------------- */
/* Minimal test framework (same as other test files). */
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
 * DB + temp dir helpers
 * ----------------------------------------------------------------------- */

static struct mds_catalogue *open_test_db(char **path_out)
{
    char *tpl = strdup("/tmp/test_proxy_XXXXXX");
    struct mds_catalogue *db = NULL;
    char db_path[4096];

    ASSERT_TRUE(tpl != NULL);
    ASSERT_TRUE(mkdtemp(tpl) != NULL);

    snprintf(db_path, sizeof(db_path), "%s/metadata.db", tpl);
    db = open_test_catalogue(); ASSERT_TRUE(db != NULL);

    *path_out = tpl;
    return db;
}

static void close_test_db(struct mds_catalogue *db, char *path)
{
    mds_catalogue_close(db);
    test_rm_rf(path);
    free(path);
}

/** Create a temp dir to act as a simulated DS mount. */
static char *make_ds_dir(void)
{
    char *tpl = strdup("/tmp/test_ds_XXXXXX");

    ASSERT_TRUE(tpl != NULL);
    ASSERT_TRUE(mkdtemp(tpl) != NULL);
    return tpl;
}

static void rm_ds_dir(char *path)
{
    test_rm_rf(path);
    free(path);
}

/** Register a DS in the catalogue and create a stripe_map for fileid. */
static void setup_single_stripe(struct mds_catalogue *db,
                                uint32_t ds_id,
                                uint64_t fileid,
                                uint32_t stripe_unit)
{
    struct mds_cat_txn *txn = NULL;
    struct mds_ds_info info;
    struct mds_ds_map_entry entry;

    memset(&info, 0, sizeof(info));
    info.ds_id = ds_id;
    info.state = DS_ONLINE;
    info.port = 2049;
    snprintf(info.addr, sizeof(info.addr), "10.0.0.%u:/ds", ds_id);

    ASSERT_EQ(mds_cat_txn_begin(db, MDS_CAT_TXN_WRITE, &txn), MDS_OK);
    ASSERT_EQ(mds_cat_ds_put(db, txn, &info), MDS_OK);
    ASSERT_EQ(mds_cat_txn_commit(txn), 0);

    memset(&entry, 0, sizeof(entry));
    entry.ds_id = ds_id;
    entry.nfs_fh_len = 4;
    memcpy(entry.nfs_fh, "fake", 4);

    ASSERT_EQ(mds_cat_txn_begin(db, MDS_CAT_TXN_WRITE, &txn), MDS_OK);
    ASSERT_EQ(mds_cat_stripe_map_put(db, txn,
              fileid, 1, stripe_unit, 1, &entry), MDS_OK);
    ASSERT_EQ(mds_cat_txn_commit(txn), 0);
}

/** Register a 2-mirror stripe_map. */
static void setup_mirrored_stripe(struct mds_catalogue *db,
                                  uint32_t ds_id_a,
                                  uint32_t ds_id_b,
                                  uint64_t fileid,
                                  uint32_t stripe_unit)
{
    struct mds_cat_txn *txn = NULL;
    struct mds_ds_info info;
    struct mds_ds_map_entry entries[2];

    /* Register both DSs. */
    memset(&info, 0, sizeof(info));
    info.ds_id = ds_id_a;
    info.state = DS_ONLINE;
    info.port = 2049;
    snprintf(info.addr, sizeof(info.addr), "10.0.0.%u:/ds", ds_id_a);

    ASSERT_EQ(mds_cat_txn_begin(db, MDS_CAT_TXN_WRITE, &txn), MDS_OK);
    ASSERT_EQ(mds_cat_ds_put(db, txn, &info), MDS_OK);
    ASSERT_EQ(mds_cat_txn_commit(txn), 0);

    info.ds_id = ds_id_b;
    snprintf(info.addr, sizeof(info.addr), "10.0.0.%u:/ds", ds_id_b);

    ASSERT_EQ(mds_cat_txn_begin(db, MDS_CAT_TXN_WRITE, &txn), MDS_OK);
    ASSERT_EQ(mds_cat_ds_put(db, txn, &info), MDS_OK);
    ASSERT_EQ(mds_cat_txn_commit(txn), 0);

    /* 1 stripe, 2 mirrors. */
    memset(entries, 0, sizeof(entries));
    entries[0].ds_id = ds_id_a;
    entries[0].nfs_fh_len = 4;
    memcpy(entries[0].nfs_fh, "fk_a", 4);
    entries[1].ds_id = ds_id_b;
    entries[1].nfs_fh_len = 4;
    memcpy(entries[1].nfs_fh, "fk_b", 4);

    ASSERT_EQ(mds_cat_txn_begin(db, MDS_CAT_TXN_WRITE, &txn), MDS_OK);
    ASSERT_EQ(mds_cat_stripe_map_put(db, txn,
              fileid, 1, stripe_unit, 2, entries), MDS_OK);
    ASSERT_EQ(mds_cat_txn_commit(txn), 0);
}

/** Register a 2-stripe (1 mirror each) map on two DS mounts. */
static void setup_wide_stripe(struct mds_catalogue *db,
                              uint32_t ds0, uint32_t ds1,
                              uint64_t fileid,
                              uint32_t stripe_unit)
{
    struct mds_cat_txn *txn = NULL;
    struct mds_ds_info info;
    struct mds_ds_map_entry entries[2];

    memset(&info, 0, sizeof(info));
    info.ds_id = ds0;
    info.state = DS_ONLINE;
    info.port = 2049;
    snprintf(info.addr, sizeof(info.addr), "10.0.0.%u:/ds", ds0);
    ASSERT_EQ(mds_cat_txn_begin(db, MDS_CAT_TXN_WRITE, &txn), MDS_OK);
    ASSERT_EQ(mds_cat_ds_put(db, txn, &info), MDS_OK);
    ASSERT_EQ(mds_cat_txn_commit(txn), 0);

    memset(&info, 0, sizeof(info));
    info.ds_id = ds1;
    info.state = DS_ONLINE;
    info.port = 2049;
    snprintf(info.addr, sizeof(info.addr), "10.0.0.%u:/ds", ds1);
    ASSERT_EQ(mds_cat_txn_begin(db, MDS_CAT_TXN_WRITE, &txn), MDS_OK);
    ASSERT_EQ(mds_cat_ds_put(db, txn, &info), MDS_OK);
    ASSERT_EQ(mds_cat_txn_commit(txn), 0);

    memset(entries, 0, sizeof(entries));
    entries[0].ds_id = ds0;
    entries[1].ds_id = ds1;

    ASSERT_EQ(mds_cat_txn_begin(db, MDS_CAT_TXN_WRITE, &txn), MDS_OK);
    ASSERT_EQ(mds_cat_stripe_map_put(db, txn,
              fileid, 2, stripe_unit, 1, entries), MDS_OK);
    ASSERT_EQ(mds_cat_txn_commit(txn), 0);
}

/* -----------------------------------------------------------------------
 * test_proxy_read_write -- basic write then read round-trip
 * ----------------------------------------------------------------------- */

static void test_proxy_read_write(void)
{
    struct mds_catalogue *db;
    struct mds_proxy_ctx *proxy;
    char *db_path, *ds_path;
    uint64_t fileid = 100;
    uint32_t stripe_unit = 65536;
    const char *test_data = "hello proxy I/O!";
    uint32_t data_len = (uint32_t)strlen(test_data);
    uint8_t buf[256];
    uint32_t bytes;
    bool eof;

    db = open_test_db(&db_path);
    ds_path = make_ds_dir();

    ASSERT_EQ(mds_proxy_ctx_create(&proxy), MDS_OK);
    ASSERT_EQ(mds_proxy_mount_set(proxy, 1, ds_path), MDS_OK);

    setup_single_stripe(db, 1, fileid, stripe_unit);

    /* Ensure DS data file exists. */
    ASSERT_EQ(mds_proxy_ensure_ds_file(proxy, 1, fileid, 0, 0), MDS_OK);

    /* Write. */
    ASSERT_EQ(mds_proxy_write(proxy, (struct mds_catalogue *)db, fileid, 0,
              test_data, data_len, &bytes), MDS_OK);
    ASSERT_EQ(bytes, data_len);

    /* Read back. */
    memset(buf, 0, sizeof(buf));
    ASSERT_EQ(mds_proxy_read(proxy, (struct mds_catalogue *)db, fileid,
              data_len, 0, data_len, buf, &bytes, &eof), MDS_OK);
    ASSERT_EQ(bytes, data_len);
    ASSERT_EQ(memcmp(buf, test_data, data_len), 0);

    mds_proxy_ctx_destroy(proxy);
    close_test_db(db, db_path);
    rm_ds_dir(ds_path);
}

/* -----------------------------------------------------------------------
 * test_proxy_read_eof -- read past end of file
 * ----------------------------------------------------------------------- */

static void test_proxy_read_eof(void)
{
    struct mds_catalogue *db;
    struct mds_proxy_ctx *proxy;
    char *db_path, *ds_path;
    uint64_t fileid = 200;
    uint32_t stripe_unit = 65536;
    const char *test_data = "short";
    uint32_t data_len = (uint32_t)strlen(test_data);
    uint8_t buf[256];
    uint32_t bytes;
    bool eof;

    db = open_test_db(&db_path);
    ds_path = make_ds_dir();

    ASSERT_EQ(mds_proxy_ctx_create(&proxy), MDS_OK);
    ASSERT_EQ(mds_proxy_mount_set(proxy, 1, ds_path), MDS_OK);

    setup_single_stripe(db, 1, fileid, stripe_unit);
    ASSERT_EQ(mds_proxy_ensure_ds_file(proxy, 1, fileid, 0, 0), MDS_OK);

    /* Write 5 bytes. */
    ASSERT_EQ(mds_proxy_write(proxy, (struct mds_catalogue *)db, fileid, 0,
              test_data, data_len, &bytes), MDS_OK);

    /* Read 256 bytes -- should get 5 + eof. */
    memset(buf, 0, sizeof(buf));
    ASSERT_EQ(mds_proxy_read(proxy, (struct mds_catalogue *)db, fileid,
              data_len, 0, 256, buf, &bytes, &eof), MDS_OK);
    ASSERT_EQ(bytes, data_len);
    ASSERT_TRUE(eof);
    ASSERT_EQ(memcmp(buf, test_data, data_len), 0);

    /* Read at offset past end -- 0 bytes + eof. */
    ASSERT_EQ(mds_proxy_read(proxy, (struct mds_catalogue *)db, fileid,
              data_len, 1000, 64, buf, &bytes, &eof), MDS_OK);
    ASSERT_EQ(bytes, (uint32_t)0);
    ASSERT_TRUE(eof);

    mds_proxy_ctx_destroy(proxy);
    close_test_db(db, db_path);
    rm_ds_dir(ds_path);
}

/* -----------------------------------------------------------------------
 * test_proxy_mirror_write -- 2-mirror write, both files get data
 * ----------------------------------------------------------------------- */

static void test_proxy_mirror_write(void)
{
    struct mds_catalogue *db;
    struct mds_proxy_ctx *proxy;
    char *db_path, *ds_a_path, *ds_b_path;
    uint64_t fileid = 300;
    uint32_t stripe_unit = 65536;
    const char *test_data = "mirrored data 42";
    uint32_t data_len = (uint32_t)strlen(test_data);
    uint32_t bytes;
    char file_a[4200], file_b[4200];
    uint8_t buf_a[256], buf_b[256];
    FILE *f;

    db = open_test_db(&db_path);
    ds_a_path = make_ds_dir();
    ds_b_path = make_ds_dir();

    ASSERT_EQ(mds_proxy_ctx_create(&proxy), MDS_OK);
    ASSERT_EQ(mds_proxy_mount_set(proxy, 10, ds_a_path), MDS_OK);
    ASSERT_EQ(mds_proxy_mount_set(proxy, 20, ds_b_path), MDS_OK);

    setup_mirrored_stripe(db, 10, 20, fileid, stripe_unit);

    /* Ensure both mirror data files exist. */
    ASSERT_EQ(mds_proxy_ensure_ds_file(proxy, 10, fileid, 0, 0), MDS_OK);
    ASSERT_EQ(mds_proxy_ensure_ds_file(proxy, 20, fileid, 0, 1), MDS_OK);

    /* Write -- should go to both mirrors. */
    ASSERT_EQ(mds_proxy_write(proxy, (struct mds_catalogue *)db, fileid, 0,
              test_data, data_len, &bytes), MDS_OK);
    ASSERT_EQ(bytes, data_len);

    /* Verify mirror 0 (DS A) directly via file I/O. */
    snprintf(file_a, sizeof(file_a),
             "%s/data/%" PRIu64 "_0_0", ds_a_path, fileid);
    f = fopen(file_a, "rb");
    ASSERT_TRUE(f != NULL);
    memset(buf_a, 0, sizeof(buf_a));
    ASSERT_EQ(fread(buf_a, 1, data_len, f), (size_t)data_len);
    fclose(f);
    ASSERT_EQ(memcmp(buf_a, test_data, data_len), 0);

    /* Verify mirror 1 (DS B). */
    snprintf(file_b, sizeof(file_b),
             "%s/data/%" PRIu64 "_0_1", ds_b_path, fileid);
    f = fopen(file_b, "rb");
    ASSERT_TRUE(f != NULL);
    memset(buf_b, 0, sizeof(buf_b));
    ASSERT_EQ(fread(buf_b, 1, data_len, f), (size_t)data_len);
    fclose(f);
    ASSERT_EQ(memcmp(buf_b, test_data, data_len), 0);

    mds_proxy_ctx_destroy(proxy);
    close_test_db(db, db_path);
    rm_ds_dir(ds_a_path);
    rm_ds_dir(ds_b_path);
}

/* -----------------------------------------------------------------------
 * test_proxy_sparse_stripe_addr -- wide-stripe proxy write lands at the
 * LOGICAL offset in the stripe DS file (not densely packed at 0).
 * ----------------------------------------------------------------------- */

static void test_proxy_sparse_stripe_addr(void)
{
    struct mds_catalogue *db;
    struct mds_proxy_ctx *proxy;
    char *db_path, *ds0_path, *ds1_path;
    uint64_t fileid = 400;
    uint32_t stripe_unit = 4096;
    const char *payload = "sparse-stripe";
    uint32_t data_len = (uint32_t)strlen(payload);
    uint64_t off = (uint64_t)stripe_unit; /* stripe 1 of 2 */
    uint32_t bytes;
    char path[4200];
    uint8_t at_logical[32], at_dense[32];
    FILE *f;
    size_t n;

    db = open_test_db(&db_path);
    ds0_path = make_ds_dir();
    ds1_path = make_ds_dir();

    ASSERT_EQ(mds_proxy_ctx_create(&proxy), MDS_OK);
    ASSERT_EQ(mds_proxy_mount_set(proxy, 1, ds0_path), MDS_OK);
    ASSERT_EQ(mds_proxy_mount_set(proxy, 2, ds1_path), MDS_OK);

    setup_wide_stripe(db, 1, 2, fileid, stripe_unit);
    ASSERT_EQ(mds_proxy_ensure_ds_file(proxy, 1, fileid, 0, 0), MDS_OK);
    ASSERT_EQ(mds_proxy_ensure_ds_file(proxy, 2, fileid, 1, 0), MDS_OK);

    ASSERT_EQ(mds_proxy_write(proxy, (struct mds_catalogue *)db, fileid, off,
              payload, data_len, &bytes), MDS_OK);
    ASSERT_EQ(bytes, data_len);

    /* Stripe 1 lives on DS 2 as {fileid}_1_0. */
    snprintf(path, sizeof(path), "%s/data/%" PRIu64 "_1_0", ds1_path, fileid);
    f = fopen(path, "rb");
    ASSERT_TRUE(f != NULL);

    /* Sparse: data at logical offset. */
    ASSERT_EQ(fseek(f, (long)off, SEEK_SET), 0);
    memset(at_logical, 0, sizeof(at_logical));
    n = fread(at_logical, 1, data_len, f);
    ASSERT_EQ(n, (size_t)data_len);
    ASSERT_EQ(memcmp(at_logical, payload, data_len), 0);

    /* Dense packing would have put the first stripe-1 unit at local 0. */
    ASSERT_EQ(fseek(f, 0, SEEK_SET), 0);
    memset(at_dense, 0xFF, sizeof(at_dense));
    n = fread(at_dense, 1, data_len, f);
    ASSERT_TRUE(n == 0 || memcmp(at_dense, payload, data_len) != 0);
    fclose(f);

    mds_proxy_ctx_destroy(proxy);
    close_test_db(db, db_path);
    rm_ds_dir(ds0_path);
    rm_ds_dir(ds1_path);
}

/* ----------------------------------------------------------------------- */
/* test_proxy_read_sparse_holes -- short DS reads are in-range holes that
 * do not hide data stored in a later stripe unit. */
/* ----------------------------------------------------------------------- */

static void test_proxy_read_sparse_holes(void)
{
    struct mds_catalogue *db;
    struct mds_proxy_ctx *proxy;
    char *db_path, *ds0_path, *ds1_path;
    const uint64_t fileid = 450;
    const uint32_t stripe_unit = 16;
    const uint64_t read_offset = 8;
    const uint64_t data_offset = (uint64_t)stripe_unit * 3;
    const char *payload = "later";
    const uint32_t payload_len = (uint32_t)strlen(payload);
    const uint64_t logical_size = data_offset + payload_len;
    uint8_t buf[64];
    uint32_t bytes;
    uint64_t index;
    bool eof;

    db = open_test_db(&db_path);
    ds0_path = make_ds_dir();
    ds1_path = make_ds_dir();

    ASSERT_EQ(mds_proxy_ctx_create(&proxy), MDS_OK);
    ASSERT_EQ(mds_proxy_mount_set(proxy, 1, ds0_path), MDS_OK);
    ASSERT_EQ(mds_proxy_mount_set(proxy, 2, ds1_path), MDS_OK);
    setup_wide_stripe(db, 1, 2, fileid, stripe_unit);
    ASSERT_EQ(mds_proxy_ensure_ds_file(proxy, 1, fileid, 0, 0), MDS_OK);
    ASSERT_EQ(mds_proxy_ensure_ds_file(proxy, 2, fileid, 1, 0), MDS_OK);
    ASSERT_EQ(mds_proxy_write(proxy, db, fileid, data_offset,
              payload, payload_len, &bytes), MDS_OK);

    memset(buf, 0xFF, sizeof(buf));
    ASSERT_EQ(mds_proxy_read(proxy, db, fileid, logical_size, read_offset,
              (uint32_t)(logical_size - read_offset), buf, &bytes, &eof),
              MDS_OK);
    ASSERT_EQ(bytes, (uint32_t)(logical_size - read_offset));
    ASSERT_TRUE(eof);
    for (index = 0; index < data_offset - read_offset; index++) {
        ASSERT_EQ(buf[index], (uint8_t)0);
    }
    ASSERT_EQ(memcmp(buf + data_offset - read_offset, payload, payload_len), 0);

    mds_proxy_ctx_destroy(proxy);
    close_test_db(db, db_path);
    rm_ds_dir(ds0_path);
    rm_ds_dir(ds1_path);
}

/* ----------------------------------------------------------------------- */
/* test_proxy_read_mirror_failover -- an unreadable primary mirror does not
 * prevent a logical-size-bounded read from a healthy mirror. */
/* ----------------------------------------------------------------------- */

static void test_proxy_read_mirror_failover(void)
{
    struct mds_catalogue *db;
    struct mds_proxy_ctx *proxy;
    char *db_path, *ds_a_path, *ds_b_path;
    const uint64_t fileid = 460;
    const uint32_t stripe_unit = 4096;
    const char *payload = "healthy mirror";
    const uint32_t payload_len = (uint32_t)strlen(payload);
    uint8_t buf[32];
    uint32_t bytes;
    bool eof;

    db = open_test_db(&db_path);
    ds_a_path = make_ds_dir();
    ds_b_path = make_ds_dir();

    ASSERT_EQ(mds_proxy_ctx_create(&proxy), MDS_OK);
    ASSERT_EQ(mds_proxy_mount_set(proxy, 10, ds_a_path), MDS_OK);
    ASSERT_EQ(mds_proxy_mount_set(proxy, 20, ds_b_path), MDS_OK);
    setup_mirrored_stripe(db, 10, 20, fileid, stripe_unit);
    ASSERT_EQ(mds_proxy_ensure_ds_file(proxy, 20, fileid, 0, 1), MDS_OK);
    ASSERT_EQ(mds_proxy_write_direct(proxy, 20, fileid, 0, 1, 0,
              payload, payload_len), MDS_OK);

    memset(buf, 0, sizeof(buf));
    ASSERT_EQ(mds_proxy_read(proxy, db, fileid, payload_len, 0, payload_len,
              buf, &bytes, &eof), MDS_OK);
    ASSERT_EQ(bytes, payload_len);
    ASSERT_TRUE(eof);
    ASSERT_EQ(memcmp(buf, payload, payload_len), 0);

    mds_proxy_ctx_destroy(proxy);
    close_test_db(db, db_path);
    rm_ds_dir(ds_a_path);
    rm_ds_dir(ds_b_path);
}

/* ----------------------------------------------------------------------- */
/* test_extract_server_fh_formats -- the opaque default accepts knfsd
 * AND NetApp ONTAP server FHs (RFC 8435: DS FHs are opaque); strict
 * knfsd mode pins the legacy 0x01 first byte.
 * ----------------------------------------------------------------------- */

/** Build a synthetic VFS f_handle payload: 12-byte NFS wrapper,
 *  uint16 LE server-FH size at offset 12, raw server FH from 14. */
static uint32_t mk_vfs_fh(uint8_t *buf, uint32_t cap,
                          const uint8_t *server_fh, uint16_t fh_size)
{
    ASSERT_TRUE((uint32_t)(14 + fh_size) <= cap);
    memset(buf, 0xAA, 12);            /* opaque NFS wrapper bytes */
    buf[12] = (uint8_t)(fh_size & 0xFF);
    buf[13] = (uint8_t)(fh_size >> 8);
    memcpy(buf + 14, server_fh, fh_size);
    return (uint32_t)(14 + fh_size);
}

static void test_extract_server_fh_formats(void)
{
    uint8_t vfs[160];
    uint8_t out[128];
    uint8_t knfsd_fh[36];
    uint8_t ontap_fh[60];
    uint32_t vlen;
    uint32_t olen = 0;

    /* Linux knfsd FH: version byte 0x01 first. */
    memset(knfsd_fh, 0x5C, sizeof(knfsd_fh));
    knfsd_fh[0] = 0x01;

    /* NetApp ONTAP NFSv3 FH (rev2, 60 bytes): n3_utility carries
     * the FH version in bits[3:0] (0x02) plus flag bits on top
     * (NFS3NG_FH_NFLEXCACHE 0x20 here) -- NOT knfsd's 0x01. */
    memset(ontap_fh, 0x7E, sizeof(ontap_fh));
    ontap_fh[0] = 0x22;

    /* knfsd FH accepted in both modes, both VFS handle types. */
    vlen = mk_vfs_fh(vfs, sizeof(vfs), knfsd_fh,
                     (uint16_t)sizeof(knfsd_fh));
    ASSERT_EQ(mds_proxy_extract_server_fh(vfs, vlen, 13, false,
                                          out, sizeof(out), &olen), 0);
    ASSERT_EQ(olen, (uint32_t)sizeof(knfsd_fh));
    ASSERT_EQ(memcmp(out, knfsd_fh, sizeof(knfsd_fh)), 0);
    ASSERT_EQ(mds_proxy_extract_server_fh(vfs, vlen, 12, true,
                                          out, sizeof(out), &olen), 0);

    /* NetApp FH: accepted by the opaque default... */
    vlen = mk_vfs_fh(vfs, sizeof(vfs), ontap_fh,
                     (uint16_t)sizeof(ontap_fh));
    ASSERT_EQ(mds_proxy_extract_server_fh(vfs, vlen, 13, false,
                                          out, sizeof(out), &olen), 0);
    ASSERT_EQ(olen, (uint32_t)sizeof(ontap_fh));
    ASSERT_EQ(memcmp(out, ontap_fh, sizeof(ontap_fh)), 0);

    /* ...but rejected by the legacy strict-knfsd mode. */
    ASSERT_EQ(mds_proxy_extract_server_fh(vfs, vlen, 13, true,
                                          out, sizeof(out), &olen), -1);

    /* Structural rejects apply in every mode: non-NFS handle type,
     * zero size, size beyond the payload, size beyond capacity. */
    ASSERT_EQ(mds_proxy_extract_server_fh(vfs, vlen, 1, false,
                                          out, sizeof(out), &olen), -1);
    vfs[12] = 0; vfs[13] = 0;                      /* size = 0 */
    ASSERT_EQ(mds_proxy_extract_server_fh(vfs, vlen, 13, false,
                                          out, sizeof(out), &olen), -1);
    vfs[12] = (uint8_t)(sizeof(ontap_fh) + 1);     /* beyond payload */
    ASSERT_EQ(mds_proxy_extract_server_fh(vfs, vlen, 13, false,
                                          out, sizeof(out), &olen), -1);
    vfs[12] = (uint8_t)sizeof(ontap_fh);           /* restore size */
    ASSERT_EQ(mds_proxy_extract_server_fh(vfs, vlen, 13, false,
                                          out, 32, &olen), -1);
}

/* ----------------------------------------------------------------------- */
/* Compound op builders (minimal subset for driving op_write through the  */
/* dispatcher in the L(F) fence test below).                               */
/* ----------------------------------------------------------------------- */

static struct nfs4_op mk_sequence(void)
{
    struct nfs4_op op;

    memset(&op, 0, sizeof(op));
    op.opnum = OP_SEQUENCE;
    return op;
}

static struct nfs4_op mk_putfh(uint64_t fileid)
{
    struct nfs4_op op;

    memset(&op, 0, sizeof(op));
    op.opnum = OP_PUTFH;
    op.arg.putfh.fh.fileid = fileid;
    return op;
}

static struct nfs4_op mk_write_op_ex(const struct nfs4_stateid *sid,
                                     uint64_t offset,
                                     const void *data, uint32_t len)
{
    struct nfs4_op op;

    memset(&op, 0, sizeof(op));
    op.opnum = OP_WRITE;
    if (sid != NULL) {
        op.arg.write.stateid = *sid;
    }
    op.arg.write.offset = offset;
    op.arg.write.data_len = len;
    op.arg.write.data = data;
    return op;
}

/* -----------------------------------------------------------------------
 * test_write_serialised_by_trim_lock -- L(F) fence on proxy WRITE
 * (trim-tail rework, spec S3.3).
 *
 * While the test thread holds the per-inode trim lock L(F), a worker
 * thread issues a proxy WRITE that extends the file.  The committed
 * catalogue size must NOT advance until L(F) is released: the DS write
 * and the MDS_ATTR_SIZE_EXTEND commit both run inside the lock, so a
 * committed size always pairs with a complete DS write (and the
 * background trim worker can never truncate between them).
 *
 * Fails pre-change because nothing serialises the write against the
 * lock: the size commits while the test still holds L(F).
 * ----------------------------------------------------------------------- */

struct write_worker_args {
    struct compound_data cd;
    struct nfs4_op       ops[3];
    struct nfs4_result   res[3];
    volatile int         status;
    volatile bool        done;
};

static void *write_worker(void *arg)
{
    struct write_worker_args *wa = (struct write_worker_args *)arg;
    uint32_t n;

    n = compound_process(&wa->cd, wa->ops, wa->res, 3);
    wa->status = (n == 3) ? (int)wa->res[2].status : -1;
    wa->done = true;
    return NULL;
}

/** Read the committed catalogue size of @fileid. */
static uint64_t cat_committed_size(struct mds_catalogue *db, uint64_t fileid)
{
    struct mds_inode inode;

    if (mds_cat_ns_getattr(db, fileid, &inode) != MDS_OK) {
        return (uint64_t)-1;
    }
    return inode.size;
}

static void test_write_serialised_by_trim_lock(void)
{
    struct mds_catalogue *db;
    struct mds_proxy_ctx *proxy;
    struct open_state_table *ot;
    struct nfs4_stateid sid;
    char *db_path, *ds_path;
    struct write_worker_args wa;
    pthread_t th;
    struct mds_inode inode;
    struct mds_cat_txn *txn = NULL;
    const uint64_t seed_size = 4096;
    const uint32_t write_len = 128;
    uint8_t seed[4096];
    uint8_t payload[128];
    uint32_t bytes;
    uint64_t fileid;
    uint64_t size;
    int i;

    db = open_test_db(&db_path);
    ds_path = make_ds_dir();

    ASSERT_EQ(mds_proxy_ctx_create(&proxy), MDS_OK);
    ASSERT_EQ(mds_proxy_mount_set(proxy, 1, ds_path), MDS_OK);

    /* 1x1 simulated-DS file seeded with seed_size bytes, size committed
     * to the catalogue. */
    ASSERT_EQ(test_create_file(db, MDS_FILEID_ROOT, "trimwrite", 0644,
                               &inode), MDS_OK);
    fileid = inode.fileid;
    setup_single_stripe(db, 1, fileid, 65536);
    ASSERT_EQ(mds_proxy_ensure_ds_file(proxy, 1, fileid, 0, 0), MDS_OK);

    for (i = 0; i < (int)seed_size; i++) {
        seed[i] = (uint8_t)i;
    }
    ASSERT_EQ(mds_proxy_write(proxy, db, fileid, 0, seed, seed_size,
                              &bytes), MDS_OK);
    ASSERT_EQ(bytes, seed_size);
    ASSERT_EQ(mds_cat_ns_getattr(db, fileid, &inode), MDS_OK);
    inode.size = seed_size;
    ASSERT_EQ(mds_cat_txn_begin(db, MDS_CAT_TXN_WRITE, &txn), MDS_OK);
    ASSERT_EQ(mds_cat_inode_put(db, txn, &inode), MDS_OK);
    ASSERT_EQ(mds_cat_txn_commit(txn), 0);

    /* A real open stateid so the write takes the full proxy path. */
    ASSERT_EQ(open_state_table_init(1, &ot), 0);
    ASSERT_EQ(open_state_open(ot, 0x100, NULL, 0, fileid,
                              OPEN4_SHARE_ACCESS_BOTH,
                              OPEN4_SHARE_DENY_NONE, &sid), 0);

    for (i = 0; i < (int)write_len; i++) {
        payload[i] = (uint8_t)(0x5a + i);
    }

    memset(&wa, 0, sizeof(wa));
    compound_init(&wa.cd);
    wa.cd.cat = db;
    wa.cd.proxy = proxy;
    wa.cd.ot = ot;
    wa.cd.clientid = 0x100;
    wa.ops[0] = mk_sequence();
    wa.ops[1] = mk_putfh(fileid);
    wa.ops[2] = mk_write_op_ex(&sid, seed_size, payload, write_len);
    wa.status = -1;
    wa.done = false;

    /* Hold L(F): the extension commit must not be visible until it is
     * released. */
    trim_inode_lock(fileid);
    ASSERT_EQ(pthread_create(&th, NULL, write_worker, &wa), 0);

    /* Generous 2 s hold window: a local write commits in milliseconds,
     * so any size advance in here escaped the fence. */
    for (i = 0; i < 200; i++) {
        size = cat_committed_size(db, fileid);
        if (size != seed_size) {
            fprintf(stderr,
                    "FAIL at %s:%d: size advanced to %llu while L(F) held\n",
                    __FILE__, __LINE__,
                    (unsigned long long)size);
            exit(1);
        }
        if (wa.done) {
            /* The worker finished without committing the size: a fence
             * that swallows a successful write is as broken as one
             * that lets it through. */
            fprintf(stderr,
                    "FAIL at %s:%d: worker done (st=%d) but size still %llu\n",
                    __FILE__, __LINE__, wa.status,
                    (unsigned long long)size);
            exit(1);
        }
        usleep(10 * 1000);
    }

    trim_inode_unlock(fileid);

    for (i = 0; i < 200 && !wa.done; i++) {
        usleep(10 * 1000);
    }
    ASSERT_TRUE(wa.done);
    ASSERT_EQ(wa.status, (int)NFS4_OK);
    ASSERT_EQ(cat_committed_size(db, fileid), seed_size + write_len);
    ASSERT_EQ(pthread_join(th, NULL), 0);

    open_state_table_destroy(ot);
    mds_proxy_ctx_destroy(proxy);
    close_test_db(db, db_path);
    rm_ds_dir(ds_path);
}

/* -----------------------------------------------------------------------
 * test_proxy_truncate_to -- DS tail-trim truncate semantics
 *
 * mds_proxy_truncate_ds_file_to() is the per-stripe truncate the trim
 * drainer uses.  Its error mapping decides whether a broken DS mount
 * drops the pending TRIM row (stale tail survives a later grow) or keeps
 * it queued for retry:
 *
 *   - a detached DS mount resolves nothing beneath the mountpoint, so
 *     {mount}/data is absent while the mount is still "registered";
 *     that must surface as MDS_ERR_IO, never as "slot missing";
 *   - a missing slot under a healthy mount (data dir present) stays a
 *     no-op success;
 *   - a healthy slot is resized exactly to the requested size (shrink
 *     and sparse extend).
 * ----------------------------------------------------------------------- */

/** Length of a regular file, or -1 on error. */
static long file_len(const char *path)
{
    struct stat st;

    if (stat(path, &st) != 0) {
        return -1;
    }
    return (long)st.st_size;
}

static void test_proxy_truncate_to(void)
{
    struct mds_proxy_ctx *proxy;
    char *ds_path;

    /* Detached mount: the mountpoint dir exists and is registered, but
     * nothing beneath it resolves -- no data/ subdir, no slot file. */
    ds_path = make_ds_dir();
    ASSERT_EQ(mds_proxy_ctx_create(&proxy), MDS_OK);
    ASSERT_EQ(mds_proxy_mount_set(proxy, 1, ds_path), MDS_OK);
    ASSERT_EQ(mds_proxy_truncate_ds_file_to(proxy, 1, 100, 0, 0, 4096),
              MDS_ERR_IO);

    /* Healthy mount, missing slot: the data dir exists; "slot never
     * existed" remains a no-op success. */
    char data_dir[4096];
    snprintf(data_dir, sizeof(data_dir), "%s/data", ds_path);
    ASSERT_EQ(mkdir(data_dir, 0755), 0);
    ASSERT_EQ(mds_proxy_truncate_ds_file_to(proxy, 1, 100, 0, 0, 4096),
              MDS_OK);

    /* Healthy mount, existing slot: shrink and sparse extend land
     * exactly on the requested size. */
    char slot[8192];
    char tmp[8192];
    snprintf(slot, sizeof(slot), "%s/data/100_0_0", ds_path);
    memset(tmp, 0x5a, sizeof(tmp));
    int fd = open(slot, O_WRONLY | O_CREAT, 0644);
    ASSERT_TRUE(fd >= 0);
    ASSERT_EQ(write(fd, tmp, sizeof(tmp)), (ssize_t)sizeof(tmp));
    ASSERT_EQ(close(fd), 0);
    ASSERT_EQ(mds_proxy_truncate_ds_file_to(proxy, 1, 100, 0, 0, 4096),
              MDS_OK);
    ASSERT_EQ(file_len(slot), 4096);
    ASSERT_EQ(mds_proxy_truncate_ds_file_to(proxy, 1, 100, 0, 0, 8192),
              MDS_OK);
    ASSERT_EQ(file_len(slot), 8192);

    /* NULL ctx is rejected like every other proxy op. */
    ASSERT_EQ(mds_proxy_truncate_ds_file_to(NULL, 1, 100, 0, 0, 4096),
              MDS_ERR_INVAL);

    mds_proxy_ctx_destroy(proxy);
    rm_ds_dir(ds_path);
}

/* -----------------------------------------------------------------------
 * main
 * ----------------------------------------------------------------------- */

int main(void)
{
    fprintf(stdout, "Running proxy I/O tests:\n");

    ASSERT_EQ(trim_module_init(90, 0), 0);
    RUN_TEST(test_extract_server_fh_formats);
    RUN_TEST(test_proxy_read_write);
    RUN_TEST(test_proxy_read_eof);
    RUN_TEST(test_proxy_mirror_write);
    RUN_TEST(test_proxy_sparse_stripe_addr);
    RUN_TEST(test_proxy_read_sparse_holes);
    RUN_TEST(test_proxy_read_mirror_failover);
    RUN_TEST(test_write_serialised_by_trim_lock);
    RUN_TEST(test_proxy_truncate_to);
    trim_module_fini();

    fprintf(stdout, "\n%d/%d tests passed.\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
