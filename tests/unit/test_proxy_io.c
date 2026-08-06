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
#include <unistd.h>
#include <sys/stat.h>
#include <inttypes.h>

#include "pnfs_mds.h"
#include "test_helpers.h"
#include "proxy_io.h"

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
    char cmd[4200];

    mds_catalogue_close(db);
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", path);
    (void)system(cmd);
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
    char cmd[4200];

    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", path);
    (void)system(cmd);
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

/* -----------------------------------------------------------------------
 * main
 * ----------------------------------------------------------------------- */

int main(void)
{
    fprintf(stdout, "Running proxy I/O tests:\n");

    RUN_TEST(test_extract_server_fh_formats);
    RUN_TEST(test_proxy_read_write);
    RUN_TEST(test_proxy_read_eof);
    RUN_TEST(test_proxy_mirror_write);
    RUN_TEST(test_proxy_sparse_stripe_addr);
    RUN_TEST(test_proxy_read_sparse_holes);
    RUN_TEST(test_proxy_read_mirror_failover);

    fprintf(stdout, "\n%d/%d tests passed.\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
