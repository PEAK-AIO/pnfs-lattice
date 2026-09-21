/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_fdb_ext.c -- FoundationDB extended authority slots
 * (src/catalogue/catalogue_fdb_ext.c) against the local cluster.
 *
 * Every test goes through the public dispatcher (mds_cat_*), so the
 * registration into the fdb authority table is exercised too.  What is
 * pinned per slot: the happy path, NOTFOUND / INVAL / STALE statuses,
 * the size limits, and the properties the design relies on --
 *   xattr_list pages (name count and byte target) with every name once,
 *   in key order, a callback that re-enters the handle and early stop;
 *   xattr mutations bump the inode's change / ctime (file blob and
 *   directory side key);
 *   a stripe map of MDS_MAX_STRIPES entries round-trips, a narrower
 *   re-put leaves no stale ordinals, and the whole map stays under the
 *   10 MB transaction limit with the real key lengths;
 *   stripe_map_scan pages by header count and by entry budget;
 *   GC rows come back in ascending sequence, gc_count matches the
 *   number enqueued (below the saturation cap) and the owner filter
 *   hides another MDS's rows while legacy (owner 0) rows stay visible;
 *   the owner filter is the GC_BY_OWNER index, not a scan: a dead
 *   peer's backlog larger than the examine cap ahead of this MDS's
 *   rows leaves peek, peek_batch and count exact, legacy rows
 *   interleave by seq, dequeue clears row and index key together and
 *   the fused remove writes the index too;
 *   remove_pending claims are exclusive until they expire,
 *   enqueue_unlink is guarded by (child, generation), scan_all pages;
 *   the DS registry holds exactly MDS_MAX_DS_NODES ids and refuses more.
 * The binary exits 77 when the backend is not built or no cluster is
 * reachable.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "pnfs_mds.h"

#ifdef HAVE_FDB

#include "catalogue_fdb.h"
#include "catalogue_fdb_internal.h"
#include "catalogue_internal.h"
#include "fdb_codec.h"
#include "fdb_keys.h"
#include "fdb_txn.h"
#include "mds_catalogue.h"
#include "quota.h"

static int tests_run;
static int tests_passed;
static int test_failed;

#define ASSERT_TRUE(cond) do { \
    if (!(cond)) { \
        fprintf(stderr, "  FAIL %s:%d: !(%s)\n", __FILE__, __LINE__, #cond); \
        test_failed = 1; \
        return; \
    } \
} while (0)

#define ASSERT_EQ(a, b) do { \
    long long _a = (long long)(a); \
    long long _b = (long long)(b); \
    if (_a != _b) { \
        fprintf(stderr, "  FAIL %s:%d: %s (%lld) != %s (%lld)\n", \
                __FILE__, __LINE__, #a, _a, #b, _b); \
        test_failed = 1; \
        return; \
    } \
} while (0)

#define RUN_TEST(fn) do { \
    fprintf(stdout, "  %-48s", #fn); \
    fflush(stdout); \
    tests_run++; \
    test_failed = 0; \
    fn(); \
    if (test_failed == 0) { \
        tests_passed++; \
        fprintf(stdout, "ok\n"); \
    } else { \
        fprintf(stdout, "FAILED\n"); \
    } \
} while (0)

/* Fileids of rows that need no inode behind them. */
#define FID_INLINE      900001ULL
#define FID_XATTR       900002ULL
#define FID_XATTR_PAGE  900003ULL
#define FID_STRIPE      900004ULL
#define FID_STRIPE_SCAN 900100ULL
#define FID_SHARD       900500ULL

/* This process's MDS id on the handle under test. */
#define TEST_MDS_ID 3U

static struct mds_catalogue *g_cat;
static char g_prefix[32];

/* Open a second handle on the same keyspace as MDS @p mds_id. */
static struct mds_catalogue *open_as(uint32_t mds_id)
{
    struct mds_config *cfg = calloc(1, sizeof(*cfg));
    struct mds_catalogue *cat = NULL;
    /* NOLINTNEXTLINE(concurrency-mt-unsafe) */
    const char *cluster = getenv("FDB_CLUSTER_FILE");

    if (cfg == NULL) {
        return NULL;
    }
    cfg->catalogue_backend = MDS_BACKEND_FDB;
    cfg->self.id = mds_id;
    if (cluster != NULL && cluster[0] != '\0') {
        (void)snprintf(cfg->fdb_cluster_file, sizeof(cfg->fdb_cluster_file), "%s", cluster);
    }
    (void)snprintf(cfg->fdb_key_prefix, sizeof(cfg->fdb_key_prefix), "%s", g_prefix);
    if (mds_catalogue_open(cfg, &cat) != MDS_OK) {
        cat = NULL;
    }
    free(cfg);
    return cat;
}

static uint64_t now_realtime_ns(void)
{
    struct timespec ts;

    (void)clock_gettime(CLOCK_REALTIME, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

/* -----------------------------------------------------------------------
 * Inline data
 * ----------------------------------------------------------------------- */

static void test_inline(void)
{
    uint8_t *big = malloc(MDS_INLINE_DATA_MAX);
    uint8_t *back = malloc(MDS_INLINE_DATA_MAX);
    uint8_t small[16];
    uint32_t outlen = 0;
    uint32_t i;

    ASSERT_TRUE(big != NULL && back != NULL);
    ASSERT_EQ(mds_cat_inline_get(g_cat, FID_INLINE, small, sizeof(small), &outlen),
              MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_inline_del(g_cat, NULL, FID_INLINE), MDS_ERR_NOTFOUND);

    for (i = 0; i < MDS_INLINE_DATA_MAX; i++) {
        big[i] = (uint8_t)(i * 7U);
    }
    ASSERT_EQ(mds_cat_inline_put(g_cat, NULL, FID_INLINE, big, MDS_INLINE_DATA_MAX), MDS_OK);
    memset(back, 0, MDS_INLINE_DATA_MAX);
    ASSERT_EQ(mds_cat_inline_get(g_cat, FID_INLINE, back, MDS_INLINE_DATA_MAX, &outlen), MDS_OK);
    ASSERT_EQ(outlen, MDS_INLINE_DATA_MAX);
    ASSERT_EQ(memcmp(big, back, MDS_INLINE_DATA_MAX), 0);

    /* A short buffer gets a truncated copy (memdb semantics). */
    ASSERT_EQ(mds_cat_inline_get(g_cat, FID_INLINE, small, sizeof(small), &outlen), MDS_OK);
    ASSERT_EQ(outlen, sizeof(small));
    ASSERT_EQ(memcmp(big, small, sizeof(small)), 0);

    /* One byte over the limit is refused; the stored value is intact. */
    ASSERT_EQ(mds_cat_inline_put(g_cat, NULL, FID_INLINE, big, MDS_INLINE_DATA_MAX + 1U),
              MDS_ERR_INVAL);
    ASSERT_EQ(mds_cat_inline_put(g_cat, NULL, FID_INLINE, NULL, 4), MDS_ERR_INVAL);
    ASSERT_EQ(mds_cat_inline_get(g_cat, FID_INLINE, back, MDS_INLINE_DATA_MAX, &outlen), MDS_OK);
    ASSERT_EQ(outlen, MDS_INLINE_DATA_MAX);

    /* Overwrite with an empty value: present, zero bytes. */
    ASSERT_EQ(mds_cat_inline_put(g_cat, NULL, FID_INLINE, NULL, 0), MDS_OK);
    outlen = 99;
    ASSERT_EQ(mds_cat_inline_get(g_cat, FID_INLINE, small, sizeof(small), &outlen), MDS_OK);
    ASSERT_EQ(outlen, 0);

    ASSERT_EQ(mds_cat_inline_del(g_cat, NULL, FID_INLINE), MDS_OK);
    ASSERT_EQ(mds_cat_inline_get(g_cat, FID_INLINE, small, sizeof(small), &outlen),
              MDS_ERR_NOTFOUND);
    free(big);
    free(back);
}

/* -----------------------------------------------------------------------
 * Extended attributes
 * ----------------------------------------------------------------------- */

static void test_xattr_basic(void)
{
    void *val = NULL;
    uint32_t vallen = 0;
    char long_name[MDS_XATTR_NAME_MAX + 2];
    uint8_t *big = malloc(MDS_XATTR_VAL_MAX + 1U);

    ASSERT_TRUE(big != NULL);
    memset(big, 0x5A, MDS_XATTR_VAL_MAX + 1U);
    memset(long_name, 'n', sizeof(long_name) - 1);
    long_name[sizeof(long_name) - 1] = '\0';

    ASSERT_EQ(mds_cat_xattr_get(g_cat, FID_XATTR, "user.a", &val, &vallen), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_xattr_exists(g_cat, FID_XATTR, "user.a"), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_xattr_del(g_cat, NULL, FID_XATTR, "user.a"), MDS_ERR_NOTFOUND);

    ASSERT_EQ(mds_cat_xattr_put(g_cat, NULL, FID_XATTR, "user.a", "hello", 5), MDS_OK);
    ASSERT_EQ(mds_cat_xattr_exists(g_cat, FID_XATTR, "user.a"), MDS_OK);
    ASSERT_EQ(mds_cat_xattr_get(g_cat, FID_XATTR, "user.a", &val, &vallen), MDS_OK);
    ASSERT_EQ(vallen, 5);
    ASSERT_EQ(memcmp(val, "hello", 5), 0);
    free(val);
    val = NULL;

    /* Overwrite, then an empty value (still a freeable buffer). */
    ASSERT_EQ(mds_cat_xattr_put(g_cat, NULL, FID_XATTR, "user.a", "hi", 2), MDS_OK);
    ASSERT_EQ(mds_cat_xattr_get(g_cat, FID_XATTR, "user.a", &val, &vallen), MDS_OK);
    ASSERT_EQ(vallen, 2);
    free(val);
    val = NULL;
    ASSERT_EQ(mds_cat_xattr_put(g_cat, NULL, FID_XATTR, "user.empty", NULL, 0), MDS_OK);
    ASSERT_EQ(mds_cat_xattr_get(g_cat, FID_XATTR, "user.empty", &val, &vallen), MDS_OK);
    ASSERT_EQ(vallen, 0);
    ASSERT_TRUE(val != NULL);
    free(val);
    val = NULL;

    /* Limits: the largest value fits, one more byte does not; a name
     * longer than MDS_XATTR_NAME_MAX or empty is refused. */
    ASSERT_EQ(mds_cat_xattr_put(g_cat, NULL, FID_XATTR, "user.big", big, MDS_XATTR_VAL_MAX),
              MDS_OK);
    ASSERT_EQ(mds_cat_xattr_get(g_cat, FID_XATTR, "user.big", &val, &vallen), MDS_OK);
    ASSERT_EQ(vallen, MDS_XATTR_VAL_MAX);
    ASSERT_EQ(memcmp(val, big, MDS_XATTR_VAL_MAX), 0);
    free(val);
    val = NULL;
    ASSERT_EQ(mds_cat_xattr_put(g_cat, NULL, FID_XATTR, "user.big", big, MDS_XATTR_VAL_MAX + 1U),
              MDS_ERR_INVAL);
    ASSERT_EQ(mds_cat_xattr_put(g_cat, NULL, FID_XATTR, long_name, "x", 1), MDS_ERR_INVAL);
    ASSERT_EQ(mds_cat_xattr_put(g_cat, NULL, FID_XATTR, "", "x", 1), MDS_ERR_INVAL);
    ASSERT_EQ(mds_cat_xattr_put(g_cat, NULL, FID_XATTR, "user.x", NULL, 1), MDS_ERR_INVAL);
    ASSERT_EQ(mds_cat_xattr_get(g_cat, FID_XATTR, long_name, &val, &vallen), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_xattr_exists(g_cat, FID_XATTR, ""), MDS_ERR_NOTFOUND);

    ASSERT_EQ(mds_cat_xattr_del(g_cat, NULL, FID_XATTR, "user.a"), MDS_OK);
    ASSERT_EQ(mds_cat_xattr_exists(g_cat, FID_XATTR, "user.a"), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_xattr_del(g_cat, NULL, FID_XATTR, "user.a"), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_xattr_del(g_cat, NULL, FID_XATTR, "user.big"), MDS_OK);
    ASSERT_EQ(mds_cat_xattr_del(g_cat, NULL, FID_XATTR, "user.empty"), MDS_OK);
    free(big);
}

/* Every xattr mutation bumps the owning inode's change and ctime: the
 * blob of a regular file, the side keys of a directory. */
static void test_xattr_touches_inode(void)
{
    struct mds_inode f;
    struct mds_inode before;
    struct mds_inode after;
    struct mds_inode dir_before;
    struct mds_inode dir_after;

    ASSERT_EQ(mds_cat_ns_create(g_cat, NULL, MDS_FILEID_ROOT, "xattr-touch", MDS_FTYPE_REG,
                                0644, 0, 0, NULL, &f), MDS_OK);
    ASSERT_EQ(mds_cat_ns_getattr(g_cat, f.fileid, &before), MDS_OK);
    ASSERT_EQ(mds_cat_xattr_put(g_cat, NULL, f.fileid, "user.t", "1", 1), MDS_OK);
    ASSERT_EQ(mds_cat_ns_getattr(g_cat, f.fileid, &after), MDS_OK);
    ASSERT_EQ(after.change, before.change + 1);
    ASSERT_TRUE(after.ctime.tv_sec > before.ctime.tv_sec ||
                (after.ctime.tv_sec == before.ctime.tv_sec &&
                 after.ctime.tv_nsec >= before.ctime.tv_nsec));
    ASSERT_EQ(mds_cat_xattr_del(g_cat, NULL, f.fileid, "user.t"), MDS_OK);
    ASSERT_EQ(mds_cat_ns_getattr(g_cat, f.fileid, &after), MDS_OK);
    ASSERT_EQ(after.change, before.change + 2);
    /* Nothing else of the blob moved. */
    ASSERT_EQ(after.size, before.size);
    ASSERT_EQ(after.nlink, before.nlink);
    ASSERT_EQ(after.mode, before.mode);

    ASSERT_EQ(mds_cat_ns_getattr(g_cat, MDS_FILEID_ROOT, &dir_before), MDS_OK);
    ASSERT_EQ(mds_cat_xattr_put(g_cat, NULL, MDS_FILEID_ROOT, "user.d", "1", 1), MDS_OK);
    ASSERT_EQ(mds_cat_ns_getattr(g_cat, MDS_FILEID_ROOT, &dir_after), MDS_OK);
    ASSERT_EQ(dir_after.change, dir_before.change + 1);
    ASSERT_EQ(dir_after.nlink, dir_before.nlink);
    ASSERT_EQ(mds_cat_xattr_del(g_cat, NULL, MDS_FILEID_ROOT, "user.d"), MDS_OK);

    ASSERT_EQ(mds_cat_ns_remove(g_cat, NULL, MDS_FILEID_ROOT, "xattr-touch"), MDS_OK);
    /* The final unlink purged the file's xattr range too. */
    ASSERT_EQ(mds_cat_xattr_exists(g_cat, f.fileid, "user.t"), MDS_ERR_NOTFOUND);
}

#define XATTR_PAGE_TEST_N 600U
#define XATTR_PAGE_TEST_VAL 4096U

struct list_ctx {
    struct mds_catalogue *cat;
    uint64_t fileid;
    uint32_t seen;
    uint32_t values_ok;
    uint32_t stop_after;      /* 0 = never */
    bool order_ok;
    bool lengths_ok;
    char last[MDS_XATTR_NAME_MAX + 1];
    uint8_t hits[XATTR_PAGE_TEST_N];
};

/* Re-enters the handle (C1) and checks key order and uniqueness. */
static int list_cb(const char *name, size_t name_len, void *arg)
{
    struct list_ctx *c = arg;
    void *val = NULL;
    uint32_t vallen = 0;

    if (name_len != strlen(name)) {
        c->lengths_ok = false;
    }
    if (c->seen > 0 && strcmp(c->last, name) >= 0) {
        c->order_ok = false;
    }
    (void)snprintf(c->last, sizeof(c->last), "%s", name);
    if (strncmp(name, "user.k", 6) == 0) {
        char *end = NULL;
        unsigned long idx = strtoul(name + 6, &end, 10);

        if (end != name + 6 && *end == '\0' && idx < XATTR_PAGE_TEST_N) {
            c->hits[idx]++;
        }
    }
    if (mds_cat_xattr_get(c->cat, c->fileid, name, &val, &vallen) == MDS_OK) {
        if (vallen == XATTR_PAGE_TEST_VAL) {
            c->values_ok++;
        }
        free(val);
    }
    c->seen++;
    return (c->stop_after != 0 && c->seen >= c->stop_after) ? 1 : 0;
}

/* 600 names of 4 KiB values: 2.4 MB, which crosses both the 256-name
 * page and the 1 MiB byte target several times. */
static void test_xattr_list_paging(void)
{
    struct list_ctx *c = calloc(1, sizeof(*c));
    uint8_t *val = malloc(XATTR_PAGE_TEST_VAL);
    uint32_t i;

    ASSERT_TRUE(c != NULL && val != NULL);
    memset(val, 0xC3, XATTR_PAGE_TEST_VAL);
    for (i = 0; i < XATTR_PAGE_TEST_N; i++) {
        char name[32];

        (void)snprintf(name, sizeof(name), "user.k%03u", i);
        ASSERT_EQ(mds_cat_xattr_put(g_cat, NULL, FID_XATTR_PAGE, name, val,
                                    XATTR_PAGE_TEST_VAL), MDS_OK);
    }
    c->cat = g_cat;
    c->fileid = FID_XATTR_PAGE;
    c->order_ok = true;
    c->lengths_ok = true;
    ASSERT_EQ(mds_cat_xattr_list(g_cat, FID_XATTR_PAGE, list_cb, c), MDS_OK);
    ASSERT_EQ(c->seen, XATTR_PAGE_TEST_N);
    ASSERT_EQ(c->values_ok, XATTR_PAGE_TEST_N);
    ASSERT_TRUE(c->order_ok);
    ASSERT_TRUE(c->lengths_ok);
    for (i = 0; i < XATTR_PAGE_TEST_N; i++) {
        ASSERT_EQ(c->hits[i], 1);
    }

    /* Early stop ends delivery with MDS_OK. */
    memset(c, 0, sizeof(*c));
    c->cat = g_cat;
    c->fileid = FID_XATTR_PAGE;
    c->order_ok = true;
    c->lengths_ok = true;
    c->stop_after = 300;
    ASSERT_EQ(mds_cat_xattr_list(g_cat, FID_XATTR_PAGE, list_cb, c), MDS_OK);
    ASSERT_EQ(c->seen, 300);

    /* An inode without xattrs lists nothing. */
    memset(c, 0, sizeof(*c));
    c->cat = g_cat;
    c->fileid = FID_XATTR_PAGE + 1U;
    ASSERT_EQ(mds_cat_xattr_list(g_cat, FID_XATTR_PAGE + 1U, list_cb, c), MDS_OK);
    ASSERT_EQ(c->seen, 0);

    for (i = 0; i < XATTR_PAGE_TEST_N; i++) {
        char name[32];

        (void)snprintf(name, sizeof(name), "user.k%03u", i);
        ASSERT_EQ(mds_cat_xattr_del(g_cat, NULL, FID_XATTR_PAGE, name), MDS_OK);
    }
    free(val);
    free(c);
}

/* -----------------------------------------------------------------------
 * Stripe maps
 * ----------------------------------------------------------------------- */

static void fill_entries(struct mds_ds_map_entry *e, uint32_t n, uint32_t fh_len)
{
    uint32_t i;

    memset(e, 0, (size_t)n * sizeof(*e));
    for (i = 0; i < n; i++) {
        uint32_t j;

        e[i].ds_id = i + 1U;
        e[i].nfs_fh_len = fh_len;
        for (j = 0; j < fh_len; j++) {
            e[i].nfs_fh[j] = (uint8_t)(i + j);
        }
        e[i].synth_suid = 0x1000U + i;
        e[i].synth_sgid = 0x2000U + i;
    }
}

static void check_entries(const struct mds_ds_map_entry *got, const struct mds_ds_map_entry *want,
                          uint32_t n)
{
    uint32_t i;

    for (i = 0; i < n; i++) {
        ASSERT_EQ(got[i].ds_id, want[i].ds_id);
        ASSERT_EQ(got[i].nfs_fh_len, want[i].nfs_fh_len);
        ASSERT_EQ(memcmp(got[i].nfs_fh, want[i].nfs_fh, want[i].nfs_fh_len), 0);
        ASSERT_EQ(got[i].synth_suid, want[i].synth_suid);
        ASSERT_EQ(got[i].synth_sgid, want[i].synth_sgid);
    }
}

static void test_stripe_map_wide(void)
{
    const uint32_t n = MDS_MAX_STRIPES;
    struct mds_ds_map_entry *in = calloc(n, sizeof(*in));
    struct mds_ds_map_entry *out = NULL;
    struct fdb_backend *b = g_cat->backend_private;
    struct fdb_key k;
    uint8_t enc[FDB_STRIPE_ENT_ENC_MAX];
    size_t enc_len = 0;
    uint64_t txn_bytes;
    uint32_t sc = 0;
    uint32_t su = 0;
    uint32_t mc = 0;

    ASSERT_TRUE(in != NULL && b != NULL);
    fill_entries(in, n, MDS_NFS_FH_MAX);

    /* The full map, with the longest file handles and the real key
     * lengths of this keyspace, stays far below the 10 MB transaction
     * limit (the header also carries a compile-time bound). */
    ASSERT_TRUE(fdb_stripe_ent_encode(&in[0], enc, sizeof(enc), &enc_len));
    fdb_key_stripe_ent(&k, &b->prefix, FID_STRIPE, n - 1U);
    txn_bytes = (uint64_t)MDS_MAX_STRIPES * MDS_MAX_MIRRORS * (k.len + enc_len);
    fdb_key_stripe_hdr(&k, &b->prefix, FID_STRIPE);
    txn_bytes += k.len + FDB_STRIPE_HDR_ENC_SIZE;
    ASSERT_TRUE(txn_bytes < 10000000ULL);
    (void)printf("[%llu B for %u entries] ", (unsigned long long)txn_bytes,
                 (unsigned)(MDS_MAX_STRIPES * MDS_MAX_MIRRORS));

    ASSERT_EQ(mds_cat_stripe_map_get(g_cat, FID_STRIPE, &sc, &su, &mc, &out), MDS_ERR_NOTFOUND);
    ASSERT_TRUE(out == NULL);
    ASSERT_EQ(mds_cat_stripe_map_put(g_cat, NULL, FID_STRIPE, n, 1U << 20, 1, in), MDS_OK);
    ASSERT_EQ(mds_cat_stripe_map_get(g_cat, FID_STRIPE, &sc, &su, &mc, &out), MDS_OK);
    ASSERT_EQ(sc, n);
    ASSERT_EQ(su, 1U << 20);
    ASSERT_EQ(mc, 1);
    ASSERT_TRUE(out != NULL);
    check_entries(out, in, n);
    free(out);
    out = NULL;

    /* Header only: no entries wanted, none allocated. */
    sc = su = mc = 0;
    ASSERT_EQ(mds_cat_stripe_map_get(g_cat, FID_STRIPE, &sc, &su, &mc, NULL), MDS_OK);
    ASSERT_EQ(sc, n);
    ASSERT_EQ(mds_cat_stripe_map_get(g_cat, FID_STRIPE, NULL, NULL, NULL, NULL), MDS_OK);

    /* A narrower re-put leaves no stale ordinals behind: the read
     * validates the row count against the header. */
    ASSERT_EQ(mds_cat_stripe_map_put(g_cat, NULL, FID_STRIPE, 2, 65536, 2, in), MDS_OK);
    ASSERT_EQ(mds_cat_stripe_map_get(g_cat, FID_STRIPE, &sc, &su, &mc, &out), MDS_OK);
    ASSERT_EQ(sc, 2);
    ASSERT_EQ(mc, 2);
    ASSERT_EQ(su, 65536);
    check_entries(out, in, 4);
    free(out);
    out = NULL;

    /* Zero geometry is refused, and the stored map is untouched. */
    ASSERT_EQ(mds_cat_stripe_map_put(g_cat, NULL, FID_STRIPE, 0, 65536, 1, in), MDS_ERR_INVAL);
    ASSERT_EQ(mds_cat_stripe_map_put(g_cat, NULL, FID_STRIPE, 1, 0, 1, in), MDS_ERR_INVAL);
    ASSERT_EQ(mds_cat_stripe_map_put(g_cat, NULL, FID_STRIPE, 1, 65536, 0, in), MDS_ERR_INVAL);
    ASSERT_EQ(mds_cat_stripe_map_put(g_cat, NULL, FID_STRIPE, MDS_MAX_STRIPES + 1U, 65536, 1,
                                     in), MDS_ERR_INVAL);
    ASSERT_EQ(mds_cat_stripe_map_put(g_cat, NULL, FID_STRIPE, 1, 65536, 1, NULL), MDS_ERR_INVAL);
    ASSERT_EQ(mds_cat_stripe_map_get(g_cat, FID_STRIPE, &sc, NULL, &mc, NULL), MDS_OK);
    ASSERT_EQ(sc, 2);
    ASSERT_EQ(mc, 2);

    /* Delete is idempotent. */
    ASSERT_EQ(mds_cat_stripe_map_del(g_cat, NULL, FID_STRIPE), MDS_OK);
    ASSERT_EQ(mds_cat_stripe_map_get(g_cat, FID_STRIPE, &sc, &su, &mc, &out), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_stripe_map_del(g_cat, NULL, FID_STRIPE), MDS_OK);
    free(in);
}

#define SCAN_SMALL_N 70U
#define SCAN_WIDE_N  9U

struct scan_ctx {
    uint32_t files;
    uint32_t entries_total;
    uint32_t bad;
    uint32_t stop_after;
    uint8_t  seen[SCAN_SMALL_N + SCAN_WIDE_N];
};

static int scan_cb(uint64_t fileid, uint32_t sc, uint32_t su, uint32_t mc,
                   const struct mds_ds_map_entry *entries, void *arg)
{
    struct scan_ctx *c = arg;
    uint32_t idx;
    uint32_t i;

    if (fileid < FID_STRIPE_SCAN || fileid >= FID_STRIPE_SCAN + SCAN_SMALL_N + SCAN_WIDE_N ||
        entries == NULL || su != 65536U || mc != 1U) {
        c->bad++;
        return 0;
    }
    idx = (uint32_t)(fileid - FID_STRIPE_SCAN);
    if ((idx < SCAN_SMALL_N && sc != 1U) || (idx >= SCAN_SMALL_N && sc != MDS_MAX_STRIPES)) {
        c->bad++;
    }
    for (i = 0; i < sc * mc; i++) {
        if (entries[i].ds_id != i + 1U) {
            c->bad++;
        }
    }
    c->seen[idx]++;
    c->files++;
    c->entries_total += sc * mc;
    return (c->stop_after != 0 && c->files >= c->stop_after) ? 1 : 0;
}

/* 70 one-stripe maps cross the 64-header page; 9 maps of
 * MDS_MAX_STRIPES entries cross the 8192-entry page budget. */
static void test_stripe_map_scan(void)
{
    struct mds_ds_map_entry *wide = calloc(MDS_MAX_STRIPES, sizeof(*wide));
    struct scan_ctx *c = calloc(1, sizeof(*c));
    uint32_t i;

    ASSERT_TRUE(wide != NULL && c != NULL);
    fill_entries(wide, MDS_MAX_STRIPES, 8);

    ASSERT_EQ(mds_cat_stripe_map_scan(g_cat, scan_cb, c), MDS_OK);
    ASSERT_EQ(c->files, 0);

    for (i = 0; i < SCAN_SMALL_N; i++) {
        ASSERT_EQ(mds_cat_stripe_map_put(g_cat, NULL, FID_STRIPE_SCAN + i, 1, 65536, 1, wide),
                  MDS_OK);
    }
    for (i = 0; i < SCAN_WIDE_N; i++) {
        ASSERT_EQ(mds_cat_stripe_map_put(g_cat, NULL, FID_STRIPE_SCAN + SCAN_SMALL_N + i,
                                         MDS_MAX_STRIPES, 65536, 1, wide), MDS_OK);
    }
    memset(c, 0, sizeof(*c));
    ASSERT_EQ(mds_cat_stripe_map_scan(g_cat, scan_cb, c), MDS_OK);
    ASSERT_EQ(c->bad, 0);
    ASSERT_EQ(c->files, SCAN_SMALL_N + SCAN_WIDE_N);
    ASSERT_EQ(c->entries_total, SCAN_SMALL_N + SCAN_WIDE_N * MDS_MAX_STRIPES);
    for (i = 0; i < SCAN_SMALL_N + SCAN_WIDE_N; i++) {
        ASSERT_EQ(c->seen[i], 1);
    }

    memset(c, 0, sizeof(*c));
    c->stop_after = 3;
    ASSERT_EQ(mds_cat_stripe_map_scan(g_cat, scan_cb, c), MDS_OK);
    ASSERT_EQ(c->files, 3);

    for (i = 0; i < SCAN_SMALL_N + SCAN_WIDE_N; i++) {
        ASSERT_EQ(mds_cat_stripe_map_del(g_cat, NULL, FID_STRIPE_SCAN + i), MDS_OK);
    }
    memset(c, 0, sizeof(*c));
    ASSERT_EQ(mds_cat_stripe_map_scan(g_cat, scan_cb, c), MDS_OK);
    ASSERT_EQ(c->files, 0);
    free(wide);
    free(c);
}

/* -----------------------------------------------------------------------
 * DS registry and provisioning
 * ----------------------------------------------------------------------- */

static void test_ds_registry(void)
{
    struct mds_ds_info info;
    struct mds_ds_info got;
    struct mds_ds_info *list = NULL;
    uint32_t count = 0;
    uint32_t i;

    ASSERT_EQ(mds_cat_ds_list(g_cat, &list, &count), MDS_OK);
    ASSERT_EQ(count, 0);
    ASSERT_TRUE(list != NULL);
    free(list);
    list = NULL;
    ASSERT_EQ(mds_cat_ds_get(g_cat, 7, &got), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_ds_del(g_cat, NULL, 7), MDS_ERR_NOTFOUND);

    memset(&info, 0, sizeof(info));
    info.ds_id = 7;
    info.state = DS_ONLINE;
    info.tier = 1;
    info.total_bytes = 1ULL << 40;
    info.used_bytes = 1ULL << 30;
    info.port = 2049;
    info.mode = DS_MODE_GENERIC;
    info.transport = DS_TRANSPORT_TCP | DS_TRANSPORT_RDMA;
    info.tcp_port = 2049;
    info.rdma_port = 20049;
    info.capabilities = DS_CAP_GPUDIRECT;
    info.weight = 5;
    (void)snprintf(info.addr, sizeof(info.addr), "10.0.0.7:/export7");
    (void)snprintf(info.host, sizeof(info.host), "ds7.example");
    (void)snprintf(info.export_path, sizeof(info.export_path), "/export7");
    ASSERT_EQ(mds_cat_ds_put(g_cat, NULL, &info), MDS_OK);
    memset(&got, 0xFF, sizeof(got));
    ASSERT_EQ(mds_cat_ds_get(g_cat, 7, &got), MDS_OK);
    ASSERT_EQ(got.ds_id, info.ds_id);
    ASSERT_EQ(got.state, info.state);
    ASSERT_EQ(got.tier, info.tier);
    ASSERT_EQ(got.total_bytes, info.total_bytes);
    ASSERT_EQ(got.used_bytes, info.used_bytes);
    ASSERT_EQ(got.port, info.port);
    ASSERT_EQ(got.mode, info.mode);
    ASSERT_EQ(got.transport, info.transport);
    ASSERT_EQ(got.tcp_port, info.tcp_port);
    ASSERT_EQ(got.rdma_port, info.rdma_port);
    ASSERT_EQ(got.capabilities, info.capabilities);
    ASSERT_EQ(got.weight, info.weight);
    ASSERT_EQ(strcmp(got.addr, info.addr), 0);
    ASSERT_EQ(strcmp(got.host, info.host), 0);
    ASSERT_EQ(strcmp(got.export_path, info.export_path), 0);

    /* The id space is bounded: MDS_MAX_DS_NODES ids fit, the next is refused. */
    for (i = 0; i < MDS_MAX_DS_NODES; i++) {
        info.ds_id = i;
        ASSERT_EQ(mds_cat_ds_put(g_cat, NULL, &info), MDS_OK);
    }
    info.ds_id = MDS_MAX_DS_NODES;
    ASSERT_EQ(mds_cat_ds_put(g_cat, NULL, &info), MDS_ERR_INVAL);
    ASSERT_EQ(mds_cat_ds_list(g_cat, &list, &count), MDS_OK);
    ASSERT_EQ(count, MDS_MAX_DS_NODES);
    for (i = 0; i < count; i++) {
        ASSERT_EQ(list[i].ds_id, i);
    }
    free(list);
    list = NULL;
    for (i = 0; i < MDS_MAX_DS_NODES; i++) {
        ASSERT_EQ(mds_cat_ds_del(g_cat, NULL, i), MDS_OK);
    }
    ASSERT_EQ(mds_cat_ds_get(g_cat, 7, &got), MDS_ERR_NOTFOUND);
}

static void test_ds_provision(void)
{
    uint8_t secret[FDB_DS_SECRET_MAX + 1U];
    uint8_t back[FDB_DS_SECRET_MAX];
    uint64_t epoch = 0;
    uint32_t i;

    for (i = 0; i < sizeof(secret); i++) {
        secret[i] = (uint8_t)(0xA0 + i);
    }
    ASSERT_EQ(mds_cat_ds_provision_get(g_cat, 9, back, sizeof(back), &epoch), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_ds_provision_del(g_cat, NULL, 9), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_ds_provision_put(g_cat, NULL, 9, secret, FDB_DS_SECRET_MAX + 1U, 1),
              MDS_ERR_INVAL);
    ASSERT_EQ(mds_cat_ds_provision_put(g_cat, NULL, MDS_MAX_DS_NODES, secret, 32, 1),
              MDS_ERR_INVAL);
    ASSERT_EQ(mds_cat_ds_provision_put(g_cat, NULL, 9, secret, 32, 77), MDS_OK);
    memset(back, 0, sizeof(back));
    ASSERT_EQ(mds_cat_ds_provision_get(g_cat, 9, back, sizeof(back), &epoch), MDS_OK);
    ASSERT_EQ(epoch, 77);
    ASSERT_EQ(memcmp(back, secret, 32), 0);
    ASSERT_EQ(back[32], 0);
    /* A short buffer gets a truncated copy. */
    memset(back, 0, sizeof(back));
    ASSERT_EQ(mds_cat_ds_provision_get(g_cat, 9, back, 8, &epoch), MDS_OK);
    ASSERT_EQ(memcmp(back, secret, 8), 0);
    ASSERT_EQ(back[8], 0);
    /* Rotate: the full 64-byte secret, new epoch. */
    ASSERT_EQ(mds_cat_ds_provision_put(g_cat, NULL, 9, secret, FDB_DS_SECRET_MAX, 78), MDS_OK);
    ASSERT_EQ(mds_cat_ds_provision_get(g_cat, 9, back, sizeof(back), &epoch), MDS_OK);
    ASSERT_EQ(epoch, 78);
    ASSERT_EQ(memcmp(back, secret, FDB_DS_SECRET_MAX), 0);
    ASSERT_EQ(mds_cat_ds_provision_del(g_cat, NULL, 9), MDS_OK);
    ASSERT_EQ(mds_cat_ds_provision_get(g_cat, 9, back, sizeof(back), &epoch), MDS_ERR_NOTFOUND);
}

/* -----------------------------------------------------------------------
 * Quota
 * ----------------------------------------------------------------------- */

static void test_quota(void)
{
    struct mds_quota_rule rule;
    struct mds_quota_rule got_rule;
    struct mds_quota_usage usage;
    struct mds_quota_usage got_usage;

    ASSERT_EQ(mds_cat_quota_rule_get(g_cat, MDS_QUOTA_USER, 1000, &got_rule), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_quota_usage_get(g_cat, MDS_QUOTA_USER_USAGE, 1000, &got_usage),
              MDS_ERR_NOTFOUND);
    memset(&rule, 0, sizeof(rule));
    rule.hard_bytes = 1ULL << 40;
    rule.soft_bytes = 1ULL << 39;
    rule.hard_inodes = 1000000;
    rule.soft_inodes = 900000;
    rule.grace_sec = MDS_QUOTA_DEFAULT_GRACE_SEC;
    ASSERT_EQ(mds_cat_quota_rule_put(g_cat, NULL, MDS_QUOTA_USER, 1000, &rule), MDS_OK);
    ASSERT_EQ(mds_cat_quota_rule_get(g_cat, MDS_QUOTA_USER, 1000, &got_rule), MDS_OK);
    ASSERT_EQ(got_rule.hard_bytes, rule.hard_bytes);
    ASSERT_EQ(got_rule.soft_bytes, rule.soft_bytes);
    ASSERT_EQ(got_rule.hard_inodes, rule.hard_inodes);
    ASSERT_EQ(got_rule.soft_inodes, rule.soft_inodes);
    ASSERT_EQ(got_rule.grace_sec, rule.grace_sec);
    /* Same scope id under another scope type is a different row. */
    ASSERT_EQ(mds_cat_quota_rule_get(g_cat, MDS_QUOTA_GROUP, 1000, &got_rule), MDS_ERR_NOTFOUND);

    memset(&usage, 0, sizeof(usage));
    usage.used_bytes = 4096;
    usage.used_inodes = 3;
    usage.grace_start_bytes = -1;
    usage.grace_start_inodes = 1234567;
    ASSERT_EQ(mds_cat_quota_usage_put(g_cat, NULL, MDS_QUOTA_USER_USAGE, 1000, &usage), MDS_OK);
    ASSERT_EQ(mds_cat_quota_usage_get(g_cat, MDS_QUOTA_USER_USAGE, 1000, &got_usage), MDS_OK);
    ASSERT_EQ(got_usage.used_bytes, usage.used_bytes);
    ASSERT_EQ(got_usage.used_inodes, usage.used_inodes);
    ASSERT_EQ(got_usage.grace_start_bytes, usage.grace_start_bytes);
    ASSERT_EQ(got_usage.grace_start_inodes, usage.grace_start_inodes);
    /* Absolute upsert: the second put replaces, not adds. */
    usage.used_bytes = 8192;
    ASSERT_EQ(mds_cat_quota_usage_put(g_cat, NULL, MDS_QUOTA_USER_USAGE, 1000, &usage), MDS_OK);
    ASSERT_EQ(mds_cat_quota_usage_get(g_cat, MDS_QUOTA_USER_USAGE, 1000, &got_usage), MDS_OK);
    ASSERT_EQ(got_usage.used_bytes, 8192);
}

/* -----------------------------------------------------------------------
 * GC queue
 * ----------------------------------------------------------------------- */

static void test_gc_queue(void)
{
    struct mds_gc_entry entry;
    struct mds_gc_entry batch[8];
    struct mds_catalogue *other = NULL;
    struct mds_catalogue *legacy = NULL;
    uint8_t fh[3] = { 1, 2, 3 };
    uint32_t n = 0;
    uint32_t i;

    ASSERT_EQ(mds_cat_gc_count(g_cat, &n), MDS_OK);
    ASSERT_EQ(n, 0);
    ASSERT_EQ(mds_cat_gc_peek(g_cat, &entry), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_gc_peek_batch(g_cat, batch, 8, &n), MDS_OK);
    ASSERT_EQ(n, 0);
    ASSERT_EQ(mds_cat_gc_dequeue(g_cat, NULL, 1), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_gc_enqueue(g_cat, NULL, 10, 1, NULL, 3), MDS_ERR_INVAL);
    ASSERT_EQ(mds_cat_gc_enqueue(g_cat, NULL, 10, 1, fh, MDS_NFS_FH_MAX + 1U), MDS_ERR_INVAL);

    for (i = 0; i < 5; i++) {
        ASSERT_EQ(mds_cat_gc_enqueue_hint(g_cat, NULL, 100 + i, 1 + i, fh, 3,
                                          MDS_GC_SWEEP_GEOM(1, 1)), MDS_OK);
    }
    ASSERT_EQ(mds_cat_gc_count(g_cat, &n), MDS_OK);
    ASSERT_EQ(n, 5);

    /* FIFO by sequence within one thread; the peek is the head. */
    ASSERT_EQ(mds_cat_gc_peek(g_cat, &entry), MDS_OK);
    ASSERT_EQ(entry.fileid, 100);
    ASSERT_EQ(entry.ds_id, 1);
    ASSERT_EQ(entry.nfs_fh_len, 3);
    ASSERT_EQ(memcmp(entry.nfs_fh, fh, 3), 0);
    ASSERT_EQ(entry.owner_mds_id, TEST_MDS_ID);
    ASSERT_EQ(entry.sweep_hint, MDS_GC_SWEEP_GEOM(1, 1));
    ASSERT_EQ(mds_cat_gc_peek_batch(g_cat, batch, 3, &n), MDS_OK);
    ASSERT_EQ(n, 3);
    ASSERT_EQ(batch[0].fileid, 100);
    ASSERT_EQ(batch[1].fileid, 101);
    ASSERT_EQ(batch[2].fileid, 102);
    ASSERT_TRUE(batch[0].gc_seq < batch[1].gc_seq && batch[1].gc_seq < batch[2].gc_seq);
    ASSERT_EQ(mds_cat_gc_peek_batch(g_cat, batch, 8, &n), MDS_OK);
    ASSERT_EQ(n, 5);

    /* Dequeue the head: gone once, NOTFOUND the second time. */
    ASSERT_EQ(mds_cat_gc_dequeue(g_cat, NULL, batch[0].gc_seq), MDS_OK);
    ASSERT_EQ(mds_cat_gc_dequeue(g_cat, NULL, batch[0].gc_seq), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_gc_peek(g_cat, &entry), MDS_OK);
    ASSERT_EQ(entry.fileid, 101);
    ASSERT_EQ(mds_cat_gc_count(g_cat, &n), MDS_OK);
    ASSERT_EQ(n, 4);

    /* Owner filter: another MDS's rows are invisible here and vice
     * versa; an MDS id of 0 writes legacy rows everyone sees and itself
     * sees everything. */
    other = open_as(4);
    legacy = open_as(0);
    ASSERT_TRUE(other != NULL && legacy != NULL);
    ASSERT_EQ(mds_cat_gc_enqueue(other, NULL, 200, 9, fh, 3), MDS_OK);
    ASSERT_EQ(mds_cat_gc_enqueue(legacy, NULL, 300, 9, fh, 3), MDS_OK);
    ASSERT_EQ(mds_cat_gc_count(g_cat, &n), MDS_OK);
    ASSERT_EQ(n, 5); /* 4 own + 1 legacy */
    ASSERT_EQ(mds_cat_gc_peek_batch(g_cat, batch, 8, &n), MDS_OK);
    ASSERT_EQ(n, 5);
    for (i = 0; i < n; i++) {
        ASSERT_TRUE(batch[i].fileid != 200);
    }
    ASSERT_EQ(batch[4].fileid, 300);
    ASSERT_EQ(mds_cat_gc_count(other, &n), MDS_OK);
    ASSERT_EQ(n, 2); /* its own + legacy */
    ASSERT_EQ(mds_cat_gc_peek(other, &entry), MDS_OK);
    ASSERT_EQ(entry.fileid, 200);
    ASSERT_EQ(mds_cat_gc_count(legacy, &n), MDS_OK);
    ASSERT_EQ(n, 6);
    ASSERT_EQ(mds_cat_gc_peek_batch(legacy, batch, 8, &n), MDS_OK);
    ASSERT_EQ(n, 6);

    /* Drain everything through the unfiltered handle. */
    for (i = 0; i < n; i++) {
        ASSERT_EQ(mds_cat_gc_dequeue(legacy, NULL, batch[i].gc_seq), MDS_OK);
    }
    ASSERT_EQ(mds_cat_gc_count(legacy, &n), MDS_OK);
    ASSERT_EQ(n, 0);
    mds_catalogue_close(other);
    mds_catalogue_close(legacy);
}

/* -----------------------------------------------------------------------
 * GC owner index (GC_BY_OWNER)
 * ----------------------------------------------------------------------- */

/* A dead peer's backlog ahead of this MDS's rows, larger than what a
 * peek walking the GC table with a row filter would examine before
 * giving up: FDB_EXT_EXAMINE_CAP(1) = 1 * 256 + 4096 rows, checked only
 * between pages of FDB_EXT_QUEUE_PAGE = 1024 rows (catalogue_fdb_ext.c),
 * so such a walk reads 5 pages = 5120 rows and stops.  Any backlog
 * beyond that would hide our rows from it for ever. */
#define GC_FOREIGN_N       (5U * 1024U + 200U)
/* Foreign rows written per raw transaction. */
#define GC_FOREIGN_TXN     512U
/* The dead peer and its fileids. */
#define GC_FOREIGN_MDS_ID  2U
#define GC_FOREIGN_FILEID  910000ULL
/* This handle's rows behind the backlog, and the interleave test's. */
#define GC_OWN_N           3U
#define GC_OWN_FILEID      920000ULL
#define GC_MIX_FILEID      930000ULL
/* Raw index scans read at most this many keys (above every count). */
#define GC_RAW_SCAN_LIMIT  (GC_FOREIGN_N + 64U)

/* One raw batch of GC rows of a foreign owner, written the way a
 * peer's gc_enqueue writes them (row + GC_BY_OWNER key). */
struct gc_raw_write {
    struct fdb_backend *b;
    uint32_t            owner;
    uint32_t            n;
    uint64_t            seqs[GC_FOREIGN_TXN];
    uint64_t            fileid_base;
};

static int gc_raw_write_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct gc_raw_write *c = arg;
    uint32_t i;

    for (i = 0; i < c->n; i++) {
        struct mds_gc_entry e;
        int rc;

        memset(&e, 0, sizeof(e));
        e.gc_seq = c->seqs[i];
        e.fileid = c->fileid_base + i;
        e.ds_id = 1;
        e.owner_mds_id = c->owner;
        e.nfs_fh_len = 1;
        e.nfs_fh[0] = 0xF0;
        rc = fdb_gc_row_set(tr, &c->b->prefix, &e);
        if (rc != 0) {
            return rc;
        }
    }
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

/* Write GC_FOREIGN_N rows of GC_FOREIGN_MDS_ID; seqs are minted from
 * this handle's allocator, so they precede everything enqueued after. */
static enum mds_status gc_write_foreign_backlog(struct fdb_backend *b)
{
    struct gc_raw_write *c = calloc(1, sizeof(*c));
    uint32_t done;
    enum mds_status st = MDS_OK;

    if (c == NULL) {
        return MDS_ERR_NOMEM;
    }
    c->b = b;
    c->owner = GC_FOREIGN_MDS_ID;
    for (done = 0; done < GC_FOREIGN_N && st == MDS_OK; done += c->n) {
        uint32_t i;

        c->n = GC_FOREIGN_N - done;
        if (c->n > GC_FOREIGN_TXN) {
            c->n = GC_FOREIGN_TXN;
        }
        c->fileid_base = GC_FOREIGN_FILEID + done;
        for (i = 0; i < c->n && st == MDS_OK; i++) {
            st = fdb_backend_alloc_id(b, FDB_META_GC_SEQ, &c->seqs[i]);
        }
        if (st == MDS_OK) {
            st = fdb_run_txn(b, FDB_TXN_MUTATING, "test_gc_raw_write", gc_raw_write_body, c);
        }
    }
    free(c);
    return st;
}

/* Raw read of one owner's GC_BY_OWNER prefix: key count and first seq. */
struct gc_raw_scan {
    struct fdb_backend *b;
    uint32_t            owner;
    int                 count;
    uint64_t            first_seq;
};

static int gc_raw_scan_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct gc_raw_scan *c = arg;
    struct fdb_key_range r;
    const FDBKeyValue *kvs = NULL;
    FDBFuture *f;
    bool more = false;
    fdb_error_t err;

    c->count = 0;
    c->first_seq = 0;
    fdb_key_gc_by_owner_prefix(&r.begin, &c->b->prefix, c->owner);
    if (!fdb_key_range_prefix(&r, &r.begin)) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    f = fdb_txn_get_range_start(tr, &r, (int)GC_RAW_SCAN_LIMIT, false, false);
    err = fdb_txn_get_range_wait(f, &kvs, &c->count, &more);
    if (err == 0 && c->count > 0) {
        FDBKeyValue kv;

        fdb_kv_at(kvs, 0, &kv);
        c->first_seq = fdb_get_u64(kv.key + r.begin.len);
    }
    if (f != NULL) {
        fdb_future_destroy(f);
    }
    if (err != 0) {
        return (int)err;
    }
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

/* Number of index keys under @p owner, -1 on a failed read. */
static int gc_index_count(struct fdb_backend *b, uint32_t owner, uint64_t *first_seq)
{
    struct gc_raw_scan c;

    memset(&c, 0, sizeof(c));
    c.b = b;
    c.owner = owner;
    if (fdb_run_txn(b, FDB_TXN_READONLY, "test_gc_raw_scan", gc_raw_scan_body, &c) != MDS_OK) {
        return -1;
    }
    if (first_seq != NULL) {
        *first_seq = c.first_seq;
    }
    return c.count;
}

/* Clear the GC table and the whole GC_BY_OWNER index (the foreign
 * backlog is not drainable through any handle of this test). */
static int gc_raw_wipe_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct fdb_backend *b = arg;
    struct fdb_key_range r;

    fdb_key_gc_prefix(&r.begin, &b->prefix);
    if (!fdb_key_range_prefix(&r, &r.begin)) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    fdb_txn_clear_range(tr, &r);
    fdb_key_init(&r.begin, &b->prefix, FDB_KT_GC_BY_OWNER);
    if (!fdb_key_range_prefix(&r, &r.begin)) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    fdb_txn_clear_range(tr, &r);
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

/* (a) A backlog of a dead peer, older than everything of ours and
 * larger than the examine cap, sits ahead of our rows: peek, peek_batch
 * and count (e) answer exactly our rows; an id-0 handle sees every row
 * (d); dequeue clears the row with its index key and repeats as
 * NOTFOUND without touching anything (c). */
static void test_gc_owner_index_starvation(void)
{
    struct fdb_backend *b = g_cat->backend_private;
    struct mds_gc_entry entry;
    struct mds_gc_entry batch[8];
    struct mds_catalogue *legacy = NULL;
    struct mds_catalogue *foreign = NULL;
    uint8_t fh[3] = { 7, 8, 9 };
    uint64_t first = 0;
    uint32_t n = 0;
    uint32_t i;

    ASSERT_TRUE(b != NULL);
    ASSERT_EQ(mds_cat_gc_count(g_cat, &n), MDS_OK);
    ASSERT_EQ(n, 0);
    ASSERT_EQ(gc_write_foreign_backlog(b), MDS_OK);
    for (i = 0; i < GC_OWN_N; i++) {
        ASSERT_EQ(mds_cat_gc_enqueue_hint(g_cat, NULL, GC_OWN_FILEID + i, 1 + i, fh, 3,
                                          MDS_GC_SWEEP_GEOM(1, 1)), MDS_OK);
    }

    /* Ours, in seq order, whatever sits in front of them. */
    ASSERT_EQ(mds_cat_gc_peek(g_cat, &entry), MDS_OK);
    ASSERT_EQ(entry.fileid, GC_OWN_FILEID);
    ASSERT_EQ(entry.owner_mds_id, TEST_MDS_ID);
    ASSERT_EQ(mds_cat_gc_peek_batch(g_cat, batch, 8, &n), MDS_OK);
    ASSERT_EQ(n, GC_OWN_N);
    for (i = 0; i < n; i++) {
        ASSERT_EQ(batch[i].fileid, GC_OWN_FILEID + i);
        ASSERT_EQ(batch[i].ds_id, 1 + i);
        ASSERT_EQ(batch[i].owner_mds_id, TEST_MDS_ID);
        ASSERT_EQ(batch[i].sweep_hint, MDS_GC_SWEEP_GEOM(1, 1));
        ASSERT_TRUE(i == 0 || batch[i - 1].gc_seq < batch[i].gc_seq);
    }
    ASSERT_EQ(mds_cat_gc_count(g_cat, &n), MDS_OK);
    ASSERT_EQ(n, GC_OWN_N);
    ASSERT_EQ(gc_index_count(b, TEST_MDS_ID, &first), (int)GC_OWN_N);
    ASSERT_EQ(first, batch[0].gc_seq);
    ASSERT_EQ(gc_index_count(b, GC_FOREIGN_MDS_ID, NULL), (int)GC_FOREIGN_N);
    ASSERT_EQ(gc_index_count(b, 0, NULL), 0);

    /* The peer itself and an id-0 handle see the backlog. */
    legacy = open_as(0);
    foreign = open_as(GC_FOREIGN_MDS_ID);
    ASSERT_TRUE(legacy != NULL && foreign != NULL);
    ASSERT_EQ(mds_cat_gc_count(foreign, &n), MDS_OK);
    ASSERT_EQ(n, GC_FOREIGN_N);
    ASSERT_EQ(mds_cat_gc_peek(foreign, &entry), MDS_OK);
    ASSERT_EQ(entry.fileid, GC_FOREIGN_FILEID);
    ASSERT_EQ(entry.owner_mds_id, GC_FOREIGN_MDS_ID);
    ASSERT_EQ(mds_cat_gc_count(legacy, &n), MDS_OK);
    ASSERT_EQ(n, GC_FOREIGN_N + GC_OWN_N);
    ASSERT_EQ(mds_cat_gc_peek_batch(legacy, batch, 8, &n), MDS_OK);
    ASSERT_EQ(n, 8);
    for (i = 0; i < n; i++) {
        ASSERT_EQ(batch[i].fileid, GC_FOREIGN_FILEID + i);
    }

    /* Dequeue: row and index key go together; a repeat changes nothing. */
    ASSERT_EQ(mds_cat_gc_peek_batch(g_cat, batch, 8, &n), MDS_OK);
    ASSERT_EQ(n, GC_OWN_N);
    ASSERT_EQ(mds_cat_gc_dequeue(g_cat, NULL, batch[0].gc_seq), MDS_OK);
    ASSERT_EQ(gc_index_count(b, TEST_MDS_ID, &first), (int)GC_OWN_N - 1);
    ASSERT_EQ(first, batch[1].gc_seq);
    ASSERT_EQ(mds_cat_gc_peek(g_cat, &entry), MDS_OK);
    ASSERT_EQ(entry.fileid, GC_OWN_FILEID + 1U);
    ASSERT_EQ(mds_cat_gc_dequeue(g_cat, NULL, batch[0].gc_seq), MDS_ERR_NOTFOUND);
    ASSERT_EQ(gc_index_count(b, TEST_MDS_ID, &first), (int)GC_OWN_N - 1);
    ASSERT_EQ(first, batch[1].gc_seq);
    ASSERT_EQ(mds_cat_gc_count(g_cat, &n), MDS_OK);
    ASSERT_EQ(n, GC_OWN_N - 1);
    for (i = 1; i < GC_OWN_N; i++) {
        ASSERT_EQ(mds_cat_gc_dequeue(g_cat, NULL, batch[i].gc_seq), MDS_OK);
    }
    ASSERT_EQ(gc_index_count(b, TEST_MDS_ID, NULL), 0);
    ASSERT_EQ(mds_cat_gc_peek(g_cat, &entry), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_gc_count(g_cat, &n), MDS_OK);
    ASSERT_EQ(n, 0);
    ASSERT_EQ(mds_cat_gc_count(foreign, &n), MDS_OK);
    ASSERT_EQ(n, GC_FOREIGN_N);

    /* The backlog belongs to nobody here: wipe it raw. */
    ASSERT_EQ(fdb_run_txn(b, FDB_TXN_MUTATING, "test_gc_raw_wipe", gc_raw_wipe_body, b),
              MDS_OK);
    ASSERT_EQ(mds_cat_gc_count(legacy, &n), MDS_OK);
    ASSERT_EQ(n, 0);
    ASSERT_EQ(gc_index_count(b, GC_FOREIGN_MDS_ID, NULL), 0);
    mds_catalogue_close(legacy);
    mds_catalogue_close(foreign);
}

/* (b) Legacy rows (owner 0) are visible to every owner and take their
 * place by seq among the owner's rows. */
static void test_gc_owner_index_legacy_interleave(void)
{
    struct fdb_backend *b = g_cat->backend_private;
    struct mds_gc_entry batch[8];
    struct mds_catalogue *legacy = open_as(0);
    struct mds_catalogue *other = open_as(GC_FOREIGN_MDS_ID);
    uint8_t fh[2] = { 1, 2 };
    uint64_t first = 0;
    uint32_t n = 0;
    uint32_t i;

    ASSERT_TRUE(b != NULL && legacy != NULL && other != NULL);
    ASSERT_EQ(mds_cat_gc_enqueue(g_cat, NULL, GC_MIX_FILEID, 1, fh, 2), MDS_OK);
    ASSERT_EQ(mds_cat_gc_enqueue(legacy, NULL, GC_MIX_FILEID + 1U, 1, fh, 2), MDS_OK);
    ASSERT_EQ(mds_cat_gc_enqueue(g_cat, NULL, GC_MIX_FILEID + 2U, 1, fh, 2), MDS_OK);

    ASSERT_EQ(mds_cat_gc_peek_batch(g_cat, batch, 8, &n), MDS_OK);
    ASSERT_EQ(n, 3);
    for (i = 0; i < n; i++) {
        ASSERT_EQ(batch[i].fileid, GC_MIX_FILEID + i);
        ASSERT_TRUE(i == 0 || batch[i - 1].gc_seq < batch[i].gc_seq);
    }
    ASSERT_EQ(batch[0].owner_mds_id, TEST_MDS_ID);
    ASSERT_EQ(batch[1].owner_mds_id, 0);
    ASSERT_EQ(batch[2].owner_mds_id, TEST_MDS_ID);
    ASSERT_EQ(mds_cat_gc_count(g_cat, &n), MDS_OK);
    ASSERT_EQ(n, 3);
    /* Another owner sees the legacy row alone. */
    ASSERT_EQ(mds_cat_gc_peek_batch(other, batch, 8, &n), MDS_OK);
    ASSERT_EQ(n, 1);
    ASSERT_EQ(batch[0].fileid, GC_MIX_FILEID + 1U);
    ASSERT_EQ(mds_cat_gc_count(other, &n), MDS_OK);
    ASSERT_EQ(n, 1);
    ASSERT_EQ(gc_index_count(b, 0, &first), 1);
    ASSERT_EQ(first, batch[0].gc_seq);

    /* Draining through one owner's handle clears the legacy row's own
     * index key: the owner comes from the row, not from the handle. */
    ASSERT_EQ(mds_cat_gc_peek_batch(g_cat, batch, 8, &n), MDS_OK);
    ASSERT_EQ(n, 3);
    for (i = 0; i < n; i++) {
        ASSERT_EQ(mds_cat_gc_dequeue(g_cat, NULL, batch[i].gc_seq), MDS_OK);
    }
    ASSERT_EQ(gc_index_count(b, 0, NULL), 0);
    ASSERT_EQ(gc_index_count(b, TEST_MDS_ID, NULL), 0);
    ASSERT_EQ(mds_cat_gc_count(legacy, &n), MDS_OK);
    ASSERT_EQ(n, 0);
    mds_catalogue_close(legacy);
    mds_catalogue_close(other);
}

/* (f) The fused final unlink (catalogue_fdb_ns.c) writes the index
 * with its GC rows: an owner-scoped peek finds them. */
static void test_gc_fused_remove_indexed(void)
{
    struct fdb_backend *b = g_cat->backend_private;
    struct mds_ds_map_entry entries[2];
    struct mds_gc_entry batch[8];
    struct mds_inode child;
    struct mds_inode seen;
    uint32_t sc = 0;
    uint32_t n = 0;
    uint32_t i;
    bool safe = true;
    bool folded = false;

    ASSERT_TRUE(b != NULL);
    memset(&child, 0, sizeof(child));
    ASSERT_EQ(mds_cat_alloc_fileid(g_cat, NULL, &child.fileid), MDS_OK);
    child.type = MDS_FTYPE_REG;
    child.mode = 0644;
    child.nlink = 1;
    child.change = 1;
    child.generation = 1;
    child.parent_fileid = MDS_FILEID_ROOT;
    child.stripe_count = 2;
    child.stripe_unit = 65536;
    child.mirror_count = 1;
    fill_entries(entries, 2, 4);
    ASSERT_EQ(mds_cat_ns_create_wide(g_cat, MDS_FILEID_ROOT, "gc-wide", &child, 2, 65536, 1,
                                     entries, &safe), MDS_OK);
    ASSERT_TRUE(!safe);
    ASSERT_EQ(mds_cat_ns_getattr(g_cat, child.fileid, &seen), MDS_OK);
    ASSERT_EQ(mds_cat_ns_remove_known_gc(g_cat, NULL, MDS_FILEID_ROOT, "gc-wide", &seen, 2,
                                         entries, 2, MDS_GC_SWEEP_GEOM(2, 1), &folded), MDS_OK);
    ASSERT_TRUE(folded);
    /* REMOVE purged the map with the inode (op_remove snapshots it first). */
    ASSERT_EQ(mds_cat_ns_getattr(g_cat, child.fileid, &seen), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_stripe_map_get(g_cat, child.fileid, &sc, NULL, NULL, NULL),
              MDS_ERR_NOTFOUND);

    ASSERT_EQ(mds_cat_gc_peek_batch(g_cat, batch, 8, &n), MDS_OK);
    ASSERT_EQ(n, 2);
    for (i = 0; i < n; i++) {
        ASSERT_EQ(batch[i].fileid, child.fileid);
        ASSERT_EQ(batch[i].ds_id, entries[i].ds_id);
        ASSERT_EQ(batch[i].owner_mds_id, TEST_MDS_ID);
        ASSERT_EQ(batch[i].sweep_hint, MDS_GC_SWEEP_GEOM(2, 1));
        ASSERT_EQ(batch[i].nfs_fh_len, entries[i].nfs_fh_len);
        ASSERT_EQ(memcmp(batch[i].nfs_fh, entries[i].nfs_fh, entries[i].nfs_fh_len), 0);
    }
    ASSERT_EQ(gc_index_count(b, TEST_MDS_ID, NULL), 2);
    for (i = 0; i < n; i++) {
        ASSERT_EQ(mds_cat_gc_dequeue(g_cat, NULL, batch[i].gc_seq), MDS_OK);
    }
    ASSERT_EQ(gc_index_count(b, TEST_MDS_ID, NULL), 0);
    ASSERT_EQ(mds_cat_gc_count(g_cat, &n), MDS_OK);
    ASSERT_EQ(n, 0);
}

/* -----------------------------------------------------------------------
 * Async-REMOVE delete manifest
 * ----------------------------------------------------------------------- */

static void test_remove_pending_claims(void)
{
    struct mds_remove_pending_entry batch[4];
    uint64_t now = now_realtime_ns();
    uint64_t seq = 0;
    uint32_t n = 0;

    ASSERT_EQ(mds_cat_remove_pending_count(g_cat, &n), MDS_OK);
    ASSERT_EQ(n, 0);
    ASSERT_EQ(mds_cat_remove_pending_claim(g_cat, 12345, TEST_MDS_ID, 1, now, 1000),
              MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_remove_pending_complete(g_cat, 12345), MDS_OK);   /* idempotent */
    ASSERT_EQ(mds_cat_remove_pending_bump_retry(g_cat, 12345), MDS_OK); /* absent: no-op */

    ASSERT_EQ(mds_cat_remove_pending_enqueue(g_cat, NULL, MDS_FILEID_ROOT, "victim", 4242, 1,
                                             &seq), MDS_OK);
    ASSERT_TRUE(seq > 0);
    ASSERT_EQ(mds_cat_remove_pending_count(g_cat, &n), MDS_OK);
    ASSERT_EQ(n, 1);
    ASSERT_EQ(mds_cat_remove_pending_peek_batch(g_cat, now, batch, 4, &n), MDS_OK);
    ASSERT_EQ(n, 1);
    ASSERT_EQ(batch[0].remove_seq, seq);
    ASSERT_EQ(batch[0].dir_fileid, MDS_FILEID_ROOT);
    ASSERT_EQ(batch[0].child_fileid, 4242);
    ASSERT_EQ(batch[0].child_generation, 1);
    ASSERT_EQ(batch[0].claim_mds_id, 0);
    ASSERT_EQ(batch[0].retries, 0);
    ASSERT_TRUE(batch[0].enqueued_ns >= now - 60000000000ULL);
    ASSERT_EQ(strcmp(batch[0].name, "victim"), 0);

    /* Claim for 10 s: a second claimer is refused and the peek skips
     * the row until the lease expires. */
    ASSERT_EQ(mds_cat_remove_pending_claim(g_cat, seq, TEST_MDS_ID, 77, now, 10000000000ULL),
              MDS_OK);
    ASSERT_EQ(mds_cat_remove_pending_claim(g_cat, seq, 4, 78, now + 1, 10000000000ULL),
              MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_remove_pending_peek_batch(g_cat, now + 1, batch, 4, &n), MDS_OK);
    ASSERT_EQ(n, 0);
    ASSERT_EQ(mds_cat_remove_pending_count(g_cat, &n), MDS_OK);
    ASSERT_EQ(n, 1); /* claimed rows still count */
    /* After the lease: visible again and claimable by the other MDS. */
    ASSERT_EQ(mds_cat_remove_pending_peek_batch(g_cat, now + 10000000001ULL, batch, 4, &n),
              MDS_OK);
    ASSERT_EQ(n, 1);
    ASSERT_EQ(batch[0].claim_mds_id, TEST_MDS_ID);
    ASSERT_EQ(batch[0].claim_boot, 77);
    ASSERT_EQ(batch[0].claim_expires_ns, now + 10000000000ULL);
    ASSERT_EQ(mds_cat_remove_pending_claim(g_cat, seq, 4, 78, now + 10000000001ULL,
                                           UINT64_MAX), MDS_OK);
    ASSERT_EQ(mds_cat_remove_pending_peek_batch(g_cat, now + 10000000002ULL, batch, 4, &n),
              MDS_OK);
    ASSERT_EQ(n, 0); /* saturated lease never expires */

    ASSERT_EQ(mds_cat_remove_pending_bump_retry(g_cat, seq), MDS_OK);
    ASSERT_EQ(mds_cat_remove_pending_bump_retry(g_cat, seq), MDS_OK);
    ASSERT_EQ(mds_cat_remove_pending_peek_batch(g_cat, UINT64_MAX, batch, 4, &n), MDS_OK);
    ASSERT_EQ(n, 0);
    ASSERT_EQ(mds_cat_remove_pending_complete(g_cat, seq), MDS_OK);
    ASSERT_EQ(mds_cat_remove_pending_complete(g_cat, seq), MDS_OK);
    ASSERT_EQ(mds_cat_remove_pending_count(g_cat, &n), MDS_OK);
    ASSERT_EQ(n, 0);
}

static void test_remove_pending_enqueue_unlink(void)
{
    struct mds_inode dir;
    struct mds_inode f;
    struct mds_inode got;
    uint64_t seq = 0;
    uint64_t committed_seq = 0;

    ASSERT_EQ(mds_cat_ns_create(g_cat, NULL, MDS_FILEID_ROOT, "rp-dir", MDS_FTYPE_DIR, 0755, 0,
                                0, NULL, &dir), MDS_OK);
    ASSERT_EQ(mds_cat_ns_create(g_cat, NULL, dir.fileid, "f", MDS_FTYPE_REG, 0644, 0, 0, NULL,
                                &f), MDS_OK);

    /* Guard mismatches change nothing. */
    ASSERT_EQ(mds_cat_remove_pending_enqueue_unlink(g_cat, NULL, dir.fileid, "f", f.fileid + 1,
                                                    f.generation, &seq), MDS_ERR_STALE);
    ASSERT_EQ(mds_cat_remove_pending_enqueue_unlink(g_cat, NULL, dir.fileid, "f", f.fileid,
                                                    f.generation + 1, &seq), MDS_ERR_STALE);
    ASSERT_EQ(mds_cat_remove_pending_enqueue_unlink(g_cat, NULL, dir.fileid, "nope", f.fileid,
                                                    f.generation, &seq), MDS_ERR_STALE);
    ASSERT_EQ(mds_cat_ns_lookup(g_cat, dir.fileid, "f", &got), MDS_OK);
    ASSERT_EQ(got.flags & MDS_IFLAG_DELETE_PENDING, 0);
    {
        uint32_t pending = 99;

        ASSERT_EQ(mds_cat_remove_pending_count(g_cat, &pending), MDS_OK);
        ASSERT_EQ(pending, 0);
    }

    /* The real thing: dirent gone, inode flagged, manifest row present. */
    ASSERT_EQ(mds_cat_remove_pending_enqueue_unlink(g_cat, NULL, dir.fileid, "f", f.fileid,
                                                    f.generation, &seq), MDS_OK);
    ASSERT_TRUE(seq > 0);
    committed_seq = seq; /* the dispatcher zeroes seq_out on every call */
    ASSERT_EQ(mds_cat_ns_lookup(g_cat, dir.fileid, "f", &got), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_ns_getattr(g_cat, f.fileid, &got), MDS_OK);
    ASSERT_TRUE((got.flags & MDS_IFLAG_DELETE_PENDING) != 0);
    ASSERT_EQ(got.nlink, 1); /* only the flag moved */
    {
        struct mds_remove_pending_entry e[2];
        uint32_t n = 0;

        ASSERT_EQ(mds_cat_remove_pending_peek_batch(g_cat, now_realtime_ns(), e, 2, &n), MDS_OK);
        ASSERT_EQ(n, 1);
        ASSERT_EQ(e[0].remove_seq, committed_seq);
        ASSERT_EQ(e[0].child_fileid, f.fileid);
        ASSERT_EQ(e[0].dir_fileid, dir.fileid);
        ASSERT_EQ(strcmp(e[0].name, "f"), 0);
    }
    /* A replay finds no dirent: STALE, nothing enqueued twice. */
    ASSERT_EQ(mds_cat_remove_pending_enqueue_unlink(g_cat, NULL, dir.fileid, "f", f.fileid,
                                                    f.generation, &seq), MDS_ERR_STALE);
    ASSERT_EQ(seq, 0);
    ASSERT_EQ(mds_cat_remove_pending_complete(g_cat, committed_seq), MDS_OK);
    ASSERT_EQ(mds_cat_inode_del(g_cat, NULL, f.fileid), MDS_OK);
    ASSERT_EQ(mds_cat_ns_remove(g_cat, NULL, MDS_FILEID_ROOT, "rp-dir"), MDS_OK);
}

#define RP_SCAN_N 300U

struct rp_scan_ctx {
    uint32_t seen;
    uint64_t last_seq;
    bool     order_ok;
    uint32_t retries_seen;
    uint32_t stop_after;
};

static int rp_dump_cb(const struct mds_remove_pending_entry *e, void *arg)
{
    (void)arg;
    (void)fprintf(stderr, "  leftover manifest row seq=%llu dir=%llu child=%llu name=%s\n",
                  (unsigned long long)e->remove_seq, (unsigned long long)e->dir_fileid,
                  (unsigned long long)e->child_fileid, e->name);
    return 0;
}

static int rp_scan_cb(const struct mds_remove_pending_entry *e, void *arg)
{
    struct rp_scan_ctx *c = arg;

    if (c->seen > 0 && e->remove_seq <= c->last_seq) {
        c->order_ok = false;
    }
    c->last_seq = e->remove_seq;
    c->retries_seen += e->retries;
    c->seen++;
    return (c->stop_after != 0 && c->seen >= c->stop_after) ? 1 : 0;
}

/* 300 rows cross the 256-row scan page; every row once, ascending. */
static void test_remove_pending_scan_paging(void)
{
    struct rp_scan_ctx c;
    uint64_t first = 0;
    uint64_t seq = 0;
    uint32_t n = 0;
    uint32_t i;

    /* The earlier manifest tests must have left the queue empty. */
    ASSERT_EQ(mds_cat_remove_pending_count(g_cat, &n), MDS_OK);
    if (n != 0) {
        (void)mds_cat_remove_pending_scan_all(g_cat, rp_dump_cb, NULL);
    }
    ASSERT_EQ(n, 0);

    for (i = 0; i < RP_SCAN_N; i++) {
        char name[32];

        (void)snprintf(name, sizeof(name), "v%u", i);
        ASSERT_EQ(mds_cat_remove_pending_enqueue(g_cat, NULL, MDS_FILEID_ROOT, name, 5000 + i,
                                                 1, &seq), MDS_OK);
        if (i == 0) {
            first = seq;
        }
    }
    ASSERT_EQ(mds_cat_remove_pending_bump_retry(g_cat, first), MDS_OK);
    ASSERT_EQ(mds_cat_remove_pending_count(g_cat, &n), MDS_OK);
    ASSERT_EQ(n, RP_SCAN_N);

    memset(&c, 0, sizeof(c));
    c.order_ok = true;
    ASSERT_EQ(mds_cat_remove_pending_scan_all(g_cat, rp_scan_cb, &c), MDS_OK);
    ASSERT_EQ(c.seen, RP_SCAN_N);
    ASSERT_TRUE(c.order_ok);
    ASSERT_EQ(c.retries_seen, 1);

    memset(&c, 0, sizeof(c));
    c.order_ok = true;
    c.stop_after = 10;
    ASSERT_EQ(mds_cat_remove_pending_scan_all(g_cat, rp_scan_cb, &c), MDS_OK);
    ASSERT_EQ(c.seen, 10);

    /* peek_batch honours its cap across pages and stays ordered. */
    {
        struct mds_remove_pending_entry *e = calloc(RP_SCAN_N, sizeof(*e));

        ASSERT_TRUE(e != NULL);
        ASSERT_EQ(mds_cat_remove_pending_peek_batch(g_cat, now_realtime_ns(), e, RP_SCAN_N, &n),
                  MDS_OK);
        ASSERT_EQ(n, RP_SCAN_N);
        for (i = 1; i < n; i++) {
            ASSERT_TRUE(e[i - 1].remove_seq < e[i].remove_seq);
        }
        for (i = 0; i < n; i++) {
            ASSERT_EQ(mds_cat_remove_pending_complete(g_cat, e[i].remove_seq), MDS_OK);
        }
        free(e);
    }
    ASSERT_EQ(mds_cat_remove_pending_count(g_cat, &n), MDS_OK);
    ASSERT_EQ(n, 0);
}

/* -----------------------------------------------------------------------
 * Shard routing, cross-shard dirents, link anchors
 * ----------------------------------------------------------------------- */

static void test_shard_ext_dirent_link_anchor(void)
{
    uint32_t shard = 0;
    uint32_t owner = 0;
    uint64_t target = 0;
    uint8_t type = 0;
    uint64_t anchor = 0;
    char long_name[MDS_MAX_NAME + 2];

    memset(long_name, 'l', sizeof(long_name) - 1);
    long_name[sizeof(long_name) - 1] = '\0';

    ASSERT_EQ(mds_cat_shard_fileid_get(g_cat, FID_SHARD, &shard), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_shard_fileid_del(g_cat, NULL, FID_SHARD), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_shard_fileid_put(g_cat, NULL, FID_SHARD, 17), MDS_OK);
    ASSERT_EQ(mds_cat_shard_fileid_get(g_cat, FID_SHARD, &shard), MDS_OK);
    ASSERT_EQ(shard, 17);
    ASSERT_EQ(mds_cat_shard_fileid_put(g_cat, NULL, FID_SHARD, 18), MDS_OK);
    ASSERT_EQ(mds_cat_shard_fileid_get(g_cat, FID_SHARD, &shard), MDS_OK);
    ASSERT_EQ(shard, 18);
    ASSERT_EQ(mds_cat_shard_fileid_del(g_cat, NULL, FID_SHARD), MDS_OK);
    ASSERT_EQ(mds_cat_shard_fileid_get(g_cat, FID_SHARD, &shard), MDS_ERR_NOTFOUND);

    ASSERT_EQ(mds_cat_ext_dirent_get(g_cat, MDS_FILEID_ROOT, "ext", &owner, &target, &type,
                                     &anchor), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_ext_dirent_del(g_cat, NULL, MDS_FILEID_ROOT, "ext"), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_ext_dirent_put(g_cat, NULL, MDS_FILEID_ROOT, "", 2, 77, 1, 9),
              MDS_ERR_INVAL);
    ASSERT_EQ(mds_cat_ext_dirent_put(g_cat, NULL, MDS_FILEID_ROOT, long_name, 2, 77, 1, 9),
              MDS_ERR_INVAL);
    ASSERT_EQ(mds_cat_ext_dirent_put(g_cat, NULL, MDS_FILEID_ROOT, "ext", 2, 77,
                                     (uint8_t)MDS_FTYPE_DIR, 9), MDS_OK);
    ASSERT_EQ(mds_cat_ext_dirent_get(g_cat, MDS_FILEID_ROOT, "ext", &owner, &target, &type,
                                     &anchor), MDS_OK);
    ASSERT_EQ(owner, 2);
    ASSERT_EQ(target, 77);
    ASSERT_EQ(type, (uint8_t)MDS_FTYPE_DIR);
    ASSERT_EQ(anchor, 9);
    /* Optional outputs may be NULL; upsert replaces. */
    ASSERT_EQ(mds_cat_ext_dirent_put(g_cat, NULL, MDS_FILEID_ROOT, "ext", 3, 78,
                                     (uint8_t)MDS_FTYPE_REG, 10), MDS_OK);
    ASSERT_EQ(mds_cat_ext_dirent_get(g_cat, MDS_FILEID_ROOT, "ext", NULL, &target, NULL, NULL),
              MDS_OK);
    ASSERT_EQ(target, 78);
    ASSERT_EQ(mds_cat_ext_dirent_del(g_cat, NULL, MDS_FILEID_ROOT, "ext"), MDS_OK);
    ASSERT_EQ(mds_cat_ext_dirent_get(g_cat, MDS_FILEID_ROOT, "ext", &owner, &target, &type,
                                     &anchor), MDS_ERR_NOTFOUND);

    ASSERT_EQ(mds_cat_link_anchor_del(g_cat, NULL, 555), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_link_anchor_put(g_cat, NULL, 555, 2, MDS_FILEID_ROOT, long_name),
              MDS_ERR_INVAL);
    ASSERT_EQ(mds_cat_link_anchor_put(g_cat, NULL, 555, 2, MDS_FILEID_ROOT, "anchored"), MDS_OK);
    ASSERT_EQ(mds_cat_link_anchor_put(g_cat, NULL, 556, 2, MDS_FILEID_ROOT, ""), MDS_OK);
    ASSERT_EQ(mds_cat_link_anchor_del(g_cat, NULL, 555), MDS_OK);
    ASSERT_EQ(mds_cat_link_anchor_del(g_cat, NULL, 556), MDS_OK);
    ASSERT_EQ(mds_cat_link_anchor_del(g_cat, NULL, 555), MDS_ERR_NOTFOUND);
}

/* ----------------------------------------------------------------------- */

static int real_open(void)
{
    const char *cluster = getenv("FDB_CLUSTER_FILE");
    struct timespec ts;

    if (cluster == NULL || cluster[0] == '\0') {
        cluster = "/etc/foundationdb/fdb.cluster";
    }
    if (access(cluster, R_OK) != 0) {
        return -1;
    }
    (void)clock_gettime(CLOCK_REALTIME, &ts);
    (void)snprintf(g_prefix, sizeof(g_prefix), "te-%ld-%08lx", (long)getpid(),
                   (unsigned long)ts.tv_nsec);
    g_cat = open_as(TEST_MDS_ID);
    if (g_cat == NULL) {
        return -1;
    }
    if (catalogue_fdb_keyspace_clear(g_cat) != MDS_OK ||
        mds_catalogue_bootstrap(g_cat) != MDS_OK) {
        mds_catalogue_close(g_cat);
        g_cat = NULL;
        return -1;
    }
    return 0;
}

int main(void)
{
    printf("test_fdb_ext:\n");
    if (real_open() != 0) {
        printf("SKIP: FoundationDB cluster not reachable\n");
        mds_catalogue_process_shutdown();
        return 77;
    }

    RUN_TEST(test_inline);
    RUN_TEST(test_xattr_basic);
    RUN_TEST(test_xattr_touches_inode);
    RUN_TEST(test_xattr_list_paging);
    RUN_TEST(test_stripe_map_wide);
    RUN_TEST(test_stripe_map_scan);
    RUN_TEST(test_ds_registry);
    RUN_TEST(test_ds_provision);
    RUN_TEST(test_quota);
    RUN_TEST(test_gc_queue);
    RUN_TEST(test_gc_owner_index_starvation);
    RUN_TEST(test_gc_owner_index_legacy_interleave);
    RUN_TEST(test_gc_fused_remove_indexed);
    RUN_TEST(test_remove_pending_claims);
    RUN_TEST(test_remove_pending_enqueue_unlink);
    RUN_TEST(test_remove_pending_scan_paging);
    RUN_TEST(test_shard_ext_dirent_link_anchor);

    (void)catalogue_fdb_keyspace_clear(g_cat);
    mds_catalogue_close(g_cat);
    g_cat = NULL;
    mds_catalogue_process_shutdown();

    printf("\n%d/%d tests passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}

#else /* !HAVE_FDB */

int main(void)
{
    printf("test_fdb_ext: SKIP (fdb backend not built)\n");
    return 77;
}

#endif
