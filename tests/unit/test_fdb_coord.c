/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_fdb_coord.c -- FoundationDB coordination and cluster slots
 *                     (catalogue_fdb_coord.c, catalogue_fdb_cluster.c).
 *
 * Runs against the local cluster under an isolated key prefix; exits 77
 * when the backend is not built or no readable cluster file exists.
 * Every check goes through the public dispatchers (mds_coord_*,
 * mds_cluster_*), so the semantics pinned here are the ones memdb
 * (catalogue_memdb.c) implements as the parity reference:
 *
 *   layout_indexes     grant -> by-file / by-client / DS index scans see
 *                      it; two stateids on one file collapse to one DS
 *                      pair; return -> gone; fileid mismatch -> NOTFOUND
 *   layout_union       widening union, monotonic seqid, RW dominance,
 *                      to-EOF sentinel, clientid move re-keys the indexes
 *   layout_del_all     bulk delete of one client leaves the others
 *   layout_rebind      a blind re-grant under a stateid bound to another
 *                      file: the row follows, index scans validate rows
 *   layoutget_fused    stripe map + grant in one transaction; NOTFOUND
 *                      writes nothing
 *   recovery           upsert, idempotent delete, owner / epoch stamping,
 *                      the owner-f-or-0 listing filter
 *   journal            upsert on (txn_id, role), oldest-first scan,
 *                      payload round trip
 *   shared_state       open / deleg / client / session / DRC round trips,
 *                      index scans, NOTFOUND after delete, reply bound
 *   locks              LOCKT conventions, owner scan, reap
 *   cluster            the Phase 1b registry / partition contract,
 *                      partition_cas, the capability predicates
 *   witness_sweep      the open-time witness sweep of an mds_id clears
 *                      only rows below the epoch its registry row
 *                      carries; register stamps it, heartbeat keeps it
 *   codecs             a few malformed-value rejections
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "pnfs_mds.h"

#ifdef HAVE_FDB

#include "catalogue_fdb.h"
#include "catalogue_internal.h"
#include "fdb_codec.h"
#include "fdb_txn.h"
#include "mds_cluster.h"
#include "mds_coordination.h"
#include "open_state.h"

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
    fprintf(stdout, "  %-40s", #fn); \
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

/* LAYOUTIOMODE4_* and nfs_lock_type4 (compound.h / lock_state.h drag in
 * the RPC headers). */
#define IOMODE_READ 1U
#define IOMODE_RW   2U
#define LT_READ     1U
#define LT_WRITE    2U
#define LT_READW    3U

/* The handle under test (self.id 3) and a second handle of the same
 * keyspace opened with self.id 0 (an identity-less writer). */
static struct mds_catalogue *g_cat;
static struct mds_catalogue *g_cat0;
static char g_prefix[32];

/* A further handle of the same keyspace under @p self_id (NULL when the
 * cluster is unreachable); defined with main() below. */
static struct mds_catalogue *open_handle(uint32_t self_id);

static struct nfs4_stateid mk_sid(uint32_t seqid, uint8_t fill)
{
    struct nfs4_stateid s;

    memset(&s, 0, sizeof(s));
    s.seqid = seqid;
    memset(s.other, fill, sizeof(s.other));
    return s;
}

/* --- collectors ---------------------------------------------------------- */

struct holders {
    unsigned n;
    struct nfs4_stateid sid[8];
    uint64_t clientid[8];
    uint32_t iomode[8];
};

static int holder_cb(uint64_t clientid, const struct nfs4_stateid *sid, uint32_t iomode,
                     void *arg)
{
    struct holders *h = arg;

    if (h->n < 8) {
        h->sid[h->n] = *sid;
        h->clientid[h->n] = clientid;
        h->iomode[h->n] = iomode;
    }
    h->n++;
    return 0;
}

struct pairs {
    unsigned n;
    uint64_t clientid[8];
    uint64_t fileid[8];
};

static int pair_cb(uint64_t clientid, uint64_t fileid, void *arg)
{
    struct pairs *p = arg;

    if (p->n < 8) {
        p->clientid[p->n] = clientid;
        p->fileid[p->n] = fileid;
    }
    p->n++;
    return 0;
}

static int stop_after_one_cb(uint64_t clientid, uint64_t fileid, void *arg)
{
    unsigned *n = arg;

    (void)clientid;
    (void)fileid;
    (*n)++;
    return 1;
}

static unsigned count_pairs(uint32_t ds_id)
{
    struct pairs p;

    memset(&p, 0, sizeof(p));
    if (mds_coord_ds_layout_idx_scan(g_cat, ds_id, pair_cb, &p) != MDS_OK) {
        return 999;
    }
    return p.n;
}

static unsigned count_holders(uint64_t fileid)
{
    struct holders h;

    memset(&h, 0, sizeof(h));
    if (mds_coord_layout_iter_file(g_cat, fileid, holder_cb, &h) != MDS_OK) {
        return 999;
    }
    return h.n;
}

/* --- layout state ---------------------------------------------------------- */

static void test_layout_indexes(void)
{
    struct nfs4_stateid a = mk_sid(1, 0xA1);
    struct nfs4_stateid b = mk_sid(4, 0xB2);
    uint32_t ds_ab[2] = { 1, 2 };
    uint32_t ds_b[1] = { 1 };
    struct holders h;
    struct pairs p;
    uint64_t cid = 0;
    uint64_t fid = 0;
    uint64_t off = 0;
    uint64_t len = 0;
    uint32_t iomode = 0;
    uint32_t seqid = 0;
    bool has = true;
    unsigned stopped = 0;

    ASSERT_EQ(mds_coord_layout_grant(g_cat, NULL, 7, 500, IOMODE_RW, 0, 4096, &a, ds_ab, 2),
              MDS_OK);
    ASSERT_EQ(mds_coord_layout_get_by_stateid(g_cat, a.other, &cid, &fid, &iomode, &off, &len,
                                              &seqid), MDS_OK);
    ASSERT_EQ(cid, 7);
    ASSERT_EQ(fid, 500);
    ASSERT_EQ(iomode, IOMODE_RW);
    ASSERT_EQ(off, 0);
    ASSERT_EQ(len, 4096);
    ASSERT_EQ(seqid, 1);
    ASSERT_EQ(mds_coord_layout_scan_for_file(g_cat, 500, &has), MDS_OK);
    ASSERT_TRUE(has);
    ASSERT_EQ(mds_coord_layout_scan_for_file(g_cat, 501, &has), MDS_OK);
    ASSERT_TRUE(!has);

    memset(&h, 0, sizeof(h));
    ASSERT_EQ(mds_coord_layout_iter_file(g_cat, 500, holder_cb, &h), MDS_OK);
    ASSERT_EQ(h.n, 1);
    ASSERT_EQ(h.clientid[0], 7);
    ASSERT_EQ(h.iomode[0], IOMODE_RW);
    ASSERT_EQ(h.sid[0].seqid, 1);
    ASSERT_EQ(memcmp(h.sid[0].other, a.other, sizeof(a.other)), 0);

    /* One (clientid, fileid) pair per DS named by the row. */
    memset(&p, 0, sizeof(p));
    ASSERT_EQ(mds_coord_ds_layout_idx_scan(g_cat, 1, pair_cb, &p), MDS_OK);
    ASSERT_EQ(p.n, 1);
    ASSERT_EQ(p.clientid[0], 7);
    ASSERT_EQ(p.fileid[0], 500);
    ASSERT_EQ(count_pairs(2), 1);
    ASSERT_EQ(count_pairs(3), 0);

    /* A second stateid of the same client on the same file: two holders,
     * still one DS pair (the index is keyed per DS / client / file). */
    ASSERT_EQ(mds_coord_layout_grant(g_cat, NULL, 7, 500, IOMODE_READ, 0, UINT64_MAX, &b,
                                     ds_b, 1), MDS_OK);
    ASSERT_EQ(count_holders(500), 2);
    ASSERT_EQ(count_pairs(1), 1);
    ASSERT_EQ(count_pairs(2), 1);

    /* An early stop is honoured and still MDS_OK. */
    ASSERT_EQ(mds_coord_ds_layout_idx_scan(g_cat, 1, stop_after_one_cb, &stopped), MDS_OK);
    ASSERT_EQ(stopped, 1);

    /* Return by (stateid, fileid): index rows go with the row. */
    ASSERT_EQ(mds_coord_layout_return(g_cat, NULL, a.other, 7, 500, ds_ab, 2), MDS_OK);
    ASSERT_EQ(mds_coord_layout_get_by_stateid(g_cat, a.other, NULL, NULL, NULL, NULL, NULL,
                                              NULL), MDS_ERR_NOTFOUND);
    ASSERT_EQ(count_holders(500), 1);
    ASSERT_EQ(count_pairs(2), 0); /* only a named DS 2 */
    ASSERT_EQ(count_pairs(1), 1); /* b still names DS 1 */
    /* A fileid that does not match the row: NOTFOUND, nothing changes. */
    ASSERT_EQ(mds_coord_layout_return(g_cat, NULL, b.other, 7, 999, NULL, 0),
              MDS_ERR_NOTFOUND);
    ASSERT_EQ(count_holders(500), 1);
    /* fileid 0 matches the stateid alone. */
    ASSERT_EQ(mds_coord_layout_return(g_cat, NULL, b.other, 7, 0, NULL, 0), MDS_OK);
    ASSERT_EQ(mds_coord_layout_scan_for_file(g_cat, 500, &has), MDS_OK);
    ASSERT_TRUE(!has);
    ASSERT_EQ(count_pairs(1), 0);
    ASSERT_EQ(count_holders(500), 0);
    ASSERT_EQ(mds_coord_layout_return(g_cat, NULL, a.other, 7, 500, NULL, 0),
              MDS_ERR_NOTFOUND);
    /* Argument checks reach the slot unchanged. */
    ASSERT_EQ(mds_coord_layout_get_by_stateid(g_cat, NULL, NULL, NULL, NULL, NULL, NULL,
                                              NULL), MDS_ERR_INVAL);
    ASSERT_EQ(mds_coord_layout_return(g_cat, NULL, NULL, 7, 500, NULL, 0), MDS_ERR_INVAL);
}

static void test_layout_union(void)
{
    struct nfs4_stateid s = mk_sid(1, 0x51);
    uint32_t ds[1] = { 4 };
    uint64_t cid = 0;
    uint64_t off = 0;
    uint64_t len = 0;
    uint32_t iomode = 0;
    uint32_t seqid = 0;

    /* Absent: a full insert with its DS index. */
    ASSERT_EQ(mds_coord_layout_grant_union(g_cat, NULL, 11, 600, IOMODE_READ, 0, 4096, &s,
                                           ds, 1), MDS_OK);
    ASSERT_EQ(count_pairs(4), 1);
    /* Widening: [0,4096) U [8192,4096) = [0,12288); seqid advances. */
    s.seqid = 2;
    ASSERT_EQ(mds_coord_layout_grant_union(g_cat, NULL, 11, 600, IOMODE_READ, 8192, 4096, &s,
                                           ds, 1), MDS_OK);
    ASSERT_EQ(mds_coord_layout_get_by_stateid(g_cat, s.other, &cid, NULL, &iomode, &off, &len,
                                              &seqid), MDS_OK);
    ASSERT_EQ(off, 0);
    ASSERT_EQ(len, 12288);
    ASSERT_EQ(seqid, 2);
    ASSERT_EQ(iomode, IOMODE_READ);
    /* An older seqid never regresses the row; RW dominates for good. */
    s.seqid = 1;
    ASSERT_EQ(mds_coord_layout_grant_union(g_cat, NULL, 11, 600, IOMODE_RW, 100, 10, &s, ds,
                                           1), MDS_OK);
    ASSERT_EQ(mds_coord_layout_get_by_stateid(g_cat, s.other, NULL, NULL, &iomode, &off, &len,
                                              &seqid), MDS_OK);
    ASSERT_EQ(seqid, 2);
    ASSERT_EQ(iomode, IOMODE_RW);
    ASSERT_EQ(len, 12288);
    s.seqid = 3;
    ASSERT_EQ(mds_coord_layout_grant_union(g_cat, NULL, 11, 600, IOMODE_READ, 0, 1, &s, ds,
                                           1), MDS_OK);
    ASSERT_EQ(mds_coord_layout_get_by_stateid(g_cat, s.other, NULL, NULL, &iomode, NULL, NULL,
                                              &seqid), MDS_OK);
    ASSERT_EQ(iomode, IOMODE_RW);
    ASSERT_EQ(seqid, 3);
    /* The to-EOF sentinel dominates. */
    ASSERT_EQ(mds_coord_layout_grant_union(g_cat, NULL, 11, 600, IOMODE_RW, 1 << 20,
                                           UINT64_MAX, &s, ds, 1), MDS_OK);
    ASSERT_EQ(mds_coord_layout_get_by_stateid(g_cat, s.other, NULL, NULL, NULL, &off, &len,
                                              NULL), MDS_OK);
    ASSERT_EQ(off, 0);
    ASSERT_TRUE(len == UINT64_MAX);
    /* Still one holder row, one DS pair. */
    ASSERT_EQ(count_holders(600), 1);
    ASSERT_EQ(count_pairs(4), 1);

    /* The clientid moves with the newest grant and so do the
     * client-keyed index rows: a bulk delete of the old client leaves
     * the row, one of the new client removes it. */
    ASSERT_EQ(mds_coord_layout_grant_union(g_cat, NULL, 12, 600, IOMODE_RW, 0, 1, &s, ds, 1),
              MDS_OK);
    ASSERT_EQ(mds_coord_layout_get_by_stateid(g_cat, s.other, &cid, NULL, NULL, NULL, NULL,
                                              NULL), MDS_OK);
    ASSERT_EQ(cid, 12);
    ASSERT_EQ(mds_coord_layout_del_all_for_client(g_cat, 11), MDS_OK);
    ASSERT_EQ(count_holders(600), 1);
    ASSERT_EQ(count_pairs(4), 1);
    ASSERT_EQ(mds_coord_layout_del_all_for_client(g_cat, 12), MDS_OK);
    ASSERT_EQ(count_holders(600), 0);
    ASSERT_EQ(count_pairs(4), 0);
    ASSERT_EQ(mds_coord_layout_grant_union(g_cat, NULL, 11, 600, IOMODE_RW, 0, 1, NULL, ds, 1),
              MDS_ERR_INVAL);
}

static void test_layout_del_all(void)
{
    struct nfs4_stateid s1 = mk_sid(1, 0xD1);
    struct nfs4_stateid s2 = mk_sid(1, 0xD2);
    struct nfs4_stateid s3 = mk_sid(1, 0xD3);
    struct nfs4_stateid s4 = mk_sid(1, 0xD4);
    uint32_t ds[1] = { 9 };
    bool has = false;

    ASSERT_EQ(mds_coord_layout_grant(g_cat, NULL, 42, 700, IOMODE_RW, 0, 1, &s1, ds, 1),
              MDS_OK);
    ASSERT_EQ(mds_coord_layout_grant(g_cat, NULL, 42, 700, IOMODE_RW, 0, 1, &s2, ds, 1),
              MDS_OK);
    ASSERT_EQ(mds_coord_layout_grant(g_cat, NULL, 42, 701, IOMODE_RW, 0, 1, &s3, ds, 1),
              MDS_OK);
    ASSERT_EQ(mds_coord_layout_grant(g_cat, NULL, 43, 701, IOMODE_RW, 0, 1, &s4, ds, 1),
              MDS_OK);
    ASSERT_EQ(count_pairs(9), 3); /* (42,700) (42,701) (43,701) */
    ASSERT_EQ(mds_coord_layout_del_all_for_client(g_cat, 42), MDS_OK);
    ASSERT_EQ(mds_coord_layout_get_by_stateid(g_cat, s1.other, NULL, NULL, NULL, NULL, NULL,
                                              NULL), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_coord_layout_get_by_stateid(g_cat, s3.other, NULL, NULL, NULL, NULL, NULL,
                                              NULL), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_coord_layout_get_by_stateid(g_cat, s4.other, NULL, NULL, NULL, NULL, NULL,
                                              NULL), MDS_OK);
    ASSERT_EQ(count_holders(700), 0);
    ASSERT_EQ(count_holders(701), 1);
    ASSERT_EQ(count_pairs(9), 1);
    ASSERT_EQ(mds_coord_layout_scan_for_file(g_cat, 700, &has), MDS_OK);
    ASSERT_TRUE(!has);
    /* A client without rows is fine; repeated deletes are fine. */
    ASSERT_EQ(mds_coord_layout_del_all_for_client(g_cat, 42), MDS_OK);
    ASSERT_EQ(mds_coord_layout_del_all_for_client(g_cat, 4242), MDS_OK);
    ASSERT_EQ(mds_coord_layout_del_all_for_client(g_cat, 43), MDS_OK);
    ASSERT_EQ(count_pairs(9), 0);
}

/* A blind grant under a stateid bound to another file: the row follows
 * the newest grant, the index-driven scans validate against the row, so
 * the old file shows no holder and the return clears everything. */
static void test_layout_rebind(void)
{
    struct nfs4_stateid x = mk_sid(1, 0xE1);
    uint32_t ds1[1] = { 21 };
    uint32_t ds2[1] = { 22 };
    uint64_t fid = 0;

    ASSERT_EQ(mds_coord_layout_grant(g_cat, NULL, 5, 800, IOMODE_RW, 0, 1, &x, ds1, 1), MDS_OK);
    ASSERT_EQ(mds_coord_layout_grant(g_cat, NULL, 5, 801, IOMODE_RW, 0, 1, &x, ds2, 1), MDS_OK);
    ASSERT_EQ(mds_coord_layout_get_by_stateid(g_cat, x.other, NULL, &fid, NULL, NULL, NULL,
                                              NULL), MDS_OK);
    ASSERT_EQ(fid, 801);
    ASSERT_EQ(count_holders(800), 0);
    ASSERT_EQ(count_holders(801), 1);
    ASSERT_EQ(count_pairs(21), 0); /* the row no longer names DS 21 */
    ASSERT_EQ(count_pairs(22), 1);
    ASSERT_EQ(mds_coord_layout_return(g_cat, NULL, x.other, 5, 800, NULL, 0),
              MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_coord_layout_return(g_cat, NULL, x.other, 5, 801, NULL, 0), MDS_OK);
    ASSERT_EQ(count_holders(801), 0);
    ASSERT_EQ(count_pairs(22), 0);
    /* The union path clears the old binding's keys itself. */
    ASSERT_EQ(mds_coord_layout_grant(g_cat, NULL, 5, 800, IOMODE_RW, 0, 1, &x, ds1, 1), MDS_OK);
    ASSERT_EQ(mds_coord_layout_grant_union(g_cat, NULL, 5, 801, IOMODE_RW, 0, 1, &x, ds2, 1),
              MDS_OK);
    ASSERT_EQ(count_pairs(21), 0);
    ASSERT_EQ(count_pairs(22), 1);
    ASSERT_EQ(mds_coord_layout_del_all_for_client(g_cat, 5), MDS_OK);
    ASSERT_EQ(count_pairs(22), 0);
}

static void test_layoutget_fused(void)
{
    struct nfs4_stateid f = mk_sid(2, 0xF1);
    struct mds_inode child;
    struct mds_ds_map_entry in[2];
    struct mds_ds_map_entry *out = NULL;
    uint32_t sc = 0;
    uint32_t su = 0;
    uint32_t mc = 0;
    uint64_t fileid = 0;
    uint64_t cid = 0;
    uint64_t fid = 0;
    bool safe = false;
    unsigned i;

    ASSERT_TRUE(mds_coord_layoutget_fused_supported(g_cat));
    ASSERT_EQ(mds_cat_alloc_fileid(g_cat, NULL, &fileid), MDS_OK);
    memset(&child, 0, sizeof(child));
    child.fileid = fileid;
    child.type = MDS_FTYPE_REG;
    child.mode = 0644;
    child.nlink = 1;
    child.parent_fileid = MDS_FILEID_ROOT;
    child.generation = 1;
    memset(in, 0, sizeof(in));
    for (i = 0; i < 2; i++) {
        in[i].ds_id = 5 + i;
        in[i].nfs_fh_len = 8;
        memset(in[i].nfs_fh, (int)(0x30 + i), 8);
    }
    ASSERT_EQ(mds_cat_ns_create_wide(g_cat, MDS_FILEID_ROOT, "fused-wide", &child, 2, 65536, 1,
                                     in, &safe), MDS_OK);

    ASSERT_EQ(mds_coord_layoutget_fused(g_cat, fileid, &sc, &su, &mc, &out, &f, 9, IOMODE_RW,
                                        0, UINT64_MAX, 3), MDS_OK);
    ASSERT_EQ(sc, 2);
    ASSERT_EQ(su, 65536);
    ASSERT_EQ(mc, 1);
    ASSERT_TRUE(out != NULL);
    ASSERT_EQ(out[0].ds_id, 5);
    ASSERT_EQ(out[1].ds_id, 6);
    ASSERT_EQ(out[1].nfs_fh_len, 8);
    ASSERT_EQ(out[1].nfs_fh[0], 0x31);
    free(out);
    out = NULL;
    /* The grant is persisted with its indexes. */
    ASSERT_EQ(mds_coord_layout_get_by_stateid(g_cat, f.other, &cid, &fid, NULL, NULL, NULL,
                                              NULL), MDS_OK);
    ASSERT_EQ(cid, 9);
    ASSERT_EQ(fid, fileid);
    ASSERT_EQ(count_pairs(5), 1);
    ASSERT_EQ(count_pairs(6), 1);
    ASSERT_EQ(count_holders(fileid), 1);
    ASSERT_EQ(mds_coord_layout_return(g_cat, NULL, f.other, 9, fileid, NULL, 0), MDS_OK);
    ASSERT_EQ(count_pairs(5), 0);

    /* No stripe map (the root directory): NOTFOUND and nothing written. */
    f = mk_sid(1, 0xF2);
    ASSERT_EQ(mds_coord_layoutget_fused(g_cat, MDS_FILEID_ROOT, &sc, &su, &mc, &out, &f, 9,
                                        IOMODE_RW, 0, UINT64_MAX, 3), MDS_ERR_NOTFOUND);
    ASSERT_TRUE(out == NULL);
    ASSERT_EQ(mds_coord_layout_get_by_stateid(g_cat, f.other, NULL, NULL, NULL, NULL, NULL,
                                              NULL), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cat_ns_remove(g_cat, NULL, MDS_FILEID_ROOT, "fused-wide"), MDS_OK);
}

/* --- client recovery --------------------------------------------------------- */

struct rec_rows {
    unsigned n;
    uint64_t clientid[8];
    uint32_t owner[8];
    uint64_t epoch[8];
};

static int rec_cb(uint64_t clientid, uint32_t owner_mds_id, uint64_t owner_boot_epoch,
                  void *arg)
{
    struct rec_rows *r = arg;

    if (r->n < 8) {
        r->clientid[r->n] = clientid;
        r->owner[r->n] = owner_mds_id;
        r->epoch[r->n] = owner_boot_epoch;
    }
    r->n++;
    return 0;
}

static bool rec_has(const struct rec_rows *r, uint64_t clientid, uint32_t owner,
                    uint64_t epoch)
{
    unsigned i;

    for (i = 0; i < r->n && i < 8; i++) {
        if (r->clientid[i] == clientid && r->owner[i] == owner && r->epoch[i] == epoch) {
            return true;
        }
    }
    return false;
}

static void test_recovery(void)
{
    uint8_t verf[8] = { 1, 2, 3, 4, 5, 6, 7, 8 };
    uint8_t owner_out[16];
    uint8_t verf_out[8];
    uint32_t owner_len = 0;
    struct rec_rows r;

    /* Owner 3 (this handle), epoch 0 until the registry knows better. */
    ASSERT_EQ(mds_coord_recovery_put(g_cat, NULL, 100, (const uint8_t *)"own", 3, verf), MDS_OK);
    ASSERT_EQ(mds_coord_recovery_put(g_cat, NULL, 100, (const uint8_t *)"own2", 4, verf),
              MDS_OK);
    memset(owner_out, 0, sizeof(owner_out));
    ASSERT_EQ(mds_coord_recovery_get(g_cat, 100, owner_out, &owner_len, verf_out), MDS_OK);
    ASSERT_EQ(owner_len, 4);
    ASSERT_EQ(memcmp(owner_out, "own2", 4), 0);
    ASSERT_EQ(memcmp(verf_out, verf, 8), 0);
    /* An identity-less writer stamps owner 0. */
    ASSERT_EQ(mds_coord_recovery_put(g_cat0, NULL, 200, (const uint8_t *)"zero", 4, verf),
              MDS_OK);

    /* Owner 3 lists its rows plus the unassigned ones. */
    memset(&r, 0, sizeof(r));
    ASSERT_EQ(mds_coord_recovery_list(g_cat, 3, rec_cb, &r), MDS_OK);
    ASSERT_EQ(r.n, 2);
    ASSERT_TRUE(rec_has(&r, 100, 3, 0));
    ASSERT_TRUE(rec_has(&r, 200, 0, 0));
    /* Another owner sees only the unassigned row. */
    memset(&r, 0, sizeof(r));
    ASSERT_EQ(mds_coord_recovery_list(g_cat, 99, rec_cb, &r), MDS_OK);
    ASSERT_EQ(r.n, 1);
    ASSERT_TRUE(rec_has(&r, 200, 0, 0));
    /* Filter 0 lists everything. */
    memset(&r, 0, sizeof(r));
    ASSERT_EQ(mds_coord_recovery_list(g_cat, 0, rec_cb, &r), MDS_OK);
    ASSERT_EQ(r.n, 2);

    /* Once registered, the epoch is stamped; a re-put by another owner
     * moves the row (and its index entry) to that owner. */
    ASSERT_EQ(mds_cluster_node_register(g_cat, 3, 77, "mds3", 2049, 9401), MDS_OK);
    ASSERT_EQ(mds_coord_recovery_put(g_cat, NULL, 200, (const uint8_t *)"taken", 5, verf),
              MDS_OK);
    memset(&r, 0, sizeof(r));
    ASSERT_EQ(mds_coord_recovery_list(g_cat, 99, rec_cb, &r), MDS_OK);
    ASSERT_EQ(r.n, 0);
    memset(&r, 0, sizeof(r));
    ASSERT_EQ(mds_coord_recovery_list(g_cat, 3, rec_cb, &r), MDS_OK);
    ASSERT_EQ(r.n, 2);
    ASSERT_TRUE(rec_has(&r, 200, 3, 77));
    ASSERT_TRUE(rec_has(&r, 100, 3, 0));
    ASSERT_EQ(mds_cluster_node_deregister(g_cat, 3, 77), MDS_OK);

    /* Idempotent delete. */
    ASSERT_EQ(mds_coord_recovery_del(g_cat, NULL, 100), MDS_OK);
    ASSERT_EQ(mds_coord_recovery_del(g_cat, NULL, 100), MDS_OK);
    ASSERT_EQ(mds_coord_recovery_get(g_cat, 100, owner_out, &owner_len, verf_out),
              MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_coord_recovery_del(g_cat, NULL, 200), MDS_OK);
    memset(&r, 0, sizeof(r));
    ASSERT_EQ(mds_coord_recovery_list(g_cat, 0, rec_cb, &r), MDS_OK);
    ASSERT_EQ(r.n, 0);
    ASSERT_EQ(mds_coord_recovery_put(g_cat, NULL, 100, NULL, 3, verf), MDS_ERR_INVAL);
    ASSERT_EQ(mds_coord_recovery_put(g_cat, NULL, 100, (const uint8_t *)"x", 1, NULL),
              MDS_ERR_INVAL);
}

/* --- 2PC journal -------------------------------------------------------------- */

struct jscan {
    unsigned n;
    uint64_t txn_id[8];
    uint8_t  role[8];
    uint8_t  state[8];
};

static int jscan_cb(const struct mds_coord_journal_record *rec, void *arg)
{
    struct jscan *j = arg;

    if (j->n < 8) {
        j->txn_id[j->n] = rec->txn_id;
        j->role[j->n] = rec->role;
        j->state[j->n] = rec->state;
    }
    j->n++;
    return 0;
}

static void test_journal(void)
{
    struct mds_coord_journal_record rec;
    struct mds_coord_journal_record got;
    struct jscan j;
    unsigned i;

    memset(&rec, 0, sizeof(rec));
    rec.txn_id = 9;
    rec.role = 0;
    rec.state = 1;
    rec.remote_mds_id = 4;
    rec.src_parent_fileid = 10;
    rec.dst_parent_fileid = 11;
    rec.src_child_fileid = 12;
    (void)snprintf(rec.src_name, sizeof(rec.src_name), "src-name");
    (void)snprintf(rec.dst_name, sizeof(rec.dst_name), "dst-name");
    rec.payload_len = 20;
    for (i = 0; i < rec.payload_len; i++) {
        rec.payload[i] = (uint8_t)(0xA0 + i);
    }
    rec.created_at_ns = 100;
    ASSERT_EQ(mds_coord_journal_put(g_cat, NULL, &rec), MDS_OK);
    rec.role = 1;
    rec.created_at_ns = 200;
    ASSERT_EQ(mds_coord_journal_put(g_cat, NULL, &rec), MDS_OK);
    rec.txn_id = 8;
    rec.role = 0;
    rec.created_at_ns = 50;
    ASSERT_EQ(mds_coord_journal_put(g_cat, NULL, &rec), MDS_OK);
    /* Upsert on (txn_id, role): the COMMITTED record replaces PREPARED. */
    rec.txn_id = 9;
    rec.state = 2;
    rec.created_at_ns = 300;
    ASSERT_EQ(mds_coord_journal_put(g_cat, NULL, &rec), MDS_OK);

    memset(&got, 0, sizeof(got));
    ASSERT_EQ(mds_coord_journal_get(g_cat, NULL, 9, 0, &got), MDS_OK);
    ASSERT_EQ(got.txn_id, 9);
    ASSERT_EQ(got.role, 0);
    ASSERT_EQ(got.state, 2);
    ASSERT_EQ(got.remote_mds_id, 4);
    ASSERT_EQ(got.src_child_fileid, 12);
    ASSERT_EQ(strcmp(got.src_name, "src-name"), 0);
    ASSERT_EQ(strcmp(got.dst_name, "dst-name"), 0);
    ASSERT_EQ(got.payload_len, 20);
    ASSERT_EQ(memcmp(got.payload, rec.payload, 20), 0);
    ASSERT_EQ(got.created_at_ns, 300);

    /* Oldest first: (8,0)@50, (9,1)@200, (9,0)@300. */
    memset(&j, 0, sizeof(j));
    ASSERT_EQ(mds_coord_journal_scan(g_cat, jscan_cb, &j), MDS_OK);
    ASSERT_EQ(j.n, 3);
    ASSERT_EQ(j.txn_id[0], 8);
    ASSERT_EQ(j.txn_id[1], 9);
    ASSERT_EQ(j.role[1], 1);
    ASSERT_EQ(j.txn_id[2], 9);
    ASSERT_EQ(j.role[2], 0);
    ASSERT_EQ(j.state[2], 2);

    ASSERT_EQ(mds_coord_journal_del(g_cat, NULL, 9, 0), MDS_OK);
    ASSERT_EQ(mds_coord_journal_del(g_cat, NULL, 9, 0), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_coord_journal_get(g_cat, NULL, 9, 0, &got), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_coord_journal_get(g_cat, NULL, 9, 1, &got), MDS_OK);
    ASSERT_EQ(mds_coord_journal_del(g_cat, NULL, 9, 1), MDS_OK);
    ASSERT_EQ(mds_coord_journal_del(g_cat, NULL, 8, 0), MDS_OK);
    memset(&j, 0, sizeof(j));
    ASSERT_EQ(mds_coord_journal_scan(g_cat, jscan_cb, &j), MDS_OK);
    ASSERT_EQ(j.n, 0);
    /* The slot refuses a NULL record itself (the dispatcher turns it
     * into NOSUPPORT before the slot is reached). */
    ASSERT_EQ(g_cat->coord_ops->journal_put(g_cat, NULL, NULL), MDS_ERR_INVAL);
    rec.payload_len = (uint32_t)MDS_COORD_JOURNAL_PAYLOAD_MAX + 1U;
    ASSERT_EQ(mds_coord_journal_put(g_cat, NULL, &rec), MDS_ERR_IO);
}

/* --- open / deleg / client / session / DRC ------------------------------------- */

static int open_count_cb(const struct mds_coord_open_row *row, void *arg)
{
    unsigned *n = arg;

    (void)row;
    (*n)++;
    return 0;
}

static int deleg_count_cb(const struct mds_coord_deleg_row *row, void *arg)
{
    unsigned *n = arg;

    (void)row;
    (*n)++;
    return 0;
}

static int session_count_cb(const struct mds_coord_session_row *row, void *arg)
{
    unsigned *n = arg;

    (void)row;
    (*n)++;
    return 0;
}

static void test_shared_state(void)
{
    struct mds_coord_open_row orow;
    struct mds_coord_open_row oget;
    struct mds_coord_deleg_row drow;
    struct mds_coord_deleg_row dget;
    struct mds_coord_client_row *crow = calloc(1, sizeof(*crow));
    struct mds_coord_client_row *cget = calloc(1, sizeof(*cget));
    struct mds_coord_session_row srow;
    struct mds_coord_session_row sget;
    struct mds_coord_drc_slot_row slot;
    uint8_t sid_a[12];
    uint8_t sid_b[12];
    uint8_t sess[16];
    uint8_t *big;
    unsigned n = 0;

    ASSERT_TRUE(crow != NULL && cget != NULL);
    ASSERT_TRUE(mds_coord_shared_state_supported(g_cat));
    memset(sid_a, 0xA1, sizeof(sid_a));
    memset(sid_b, 0xB2, sizeof(sid_b));
    memset(sess, 0x5E, sizeof(sess));

    /* Open state: upsert on the stateid, both index scans, delete. */
    memset(&orow, 0, sizeof(orow));
    memcpy(orow.stateid_other, sid_a, 12);
    orow.seqid = 1;
    orow.clientid = 100;
    orow.fileid = 500;
    orow.share_access = 3;
    orow.share_deny = 1;
    orow.open_owner_len = 5;
    memcpy(orow.open_owner, "owner", 5);
    orow.owner_mds_id = 3;
    orow.owner_boot_epoch = 77;
    ASSERT_EQ(mds_coord_open_put(g_cat, &orow), MDS_OK);
    orow.seqid = 2;
    ASSERT_EQ(mds_coord_open_put(g_cat, &orow), MDS_OK);
    memcpy(orow.stateid_other, sid_b, 12);
    orow.clientid = 101;
    ASSERT_EQ(mds_coord_open_put(g_cat, &orow), MDS_OK);
    memset(&oget, 0, sizeof(oget));
    ASSERT_EQ(mds_coord_open_get(g_cat, sid_a, &oget), MDS_OK);
    ASSERT_EQ(oget.seqid, 2);
    ASSERT_EQ(oget.clientid, 100);
    ASSERT_EQ(oget.fileid, 500);
    ASSERT_EQ(oget.share_access, 3);
    ASSERT_EQ(oget.share_deny, 1);
    ASSERT_EQ(oget.open_owner_len, 5);
    ASSERT_EQ(memcmp(oget.open_owner, "owner", 5), 0);
    ASSERT_EQ(oget.owner_mds_id, 3);
    ASSERT_EQ(oget.owner_boot_epoch, 77);
    ASSERT_EQ(memcmp(oget.stateid_other, sid_a, 12), 0);
    n = 0;
    ASSERT_EQ(mds_coord_open_scan_file(g_cat, 500, open_count_cb, &n), MDS_OK);
    ASSERT_EQ(n, 2);
    n = 0;
    ASSERT_EQ(mds_coord_open_scan_client(g_cat, 101, open_count_cb, &n), MDS_OK);
    ASSERT_EQ(n, 1);
    n = 0;
    ASSERT_EQ(mds_coord_open_scan_client(g_cat, 102, open_count_cb, &n), MDS_OK);
    ASSERT_EQ(n, 0);
    ASSERT_EQ(mds_coord_open_del(g_cat, sid_a), MDS_OK);
    ASSERT_EQ(mds_coord_open_del(g_cat, sid_a), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_coord_open_get(g_cat, sid_a, &oget), MDS_ERR_NOTFOUND);
    n = 0;
    ASSERT_EQ(mds_coord_open_scan_file(g_cat, 500, open_count_cb, &n), MDS_OK);
    ASSERT_EQ(n, 1);
    ASSERT_EQ(mds_coord_open_del(g_cat, sid_b), MDS_OK);
    orow.open_owner_len = 129;
    ASSERT_EQ(mds_coord_open_put(g_cat, &orow), MDS_ERR_INVAL);
    ASSERT_EQ(mds_coord_open_put(g_cat, NULL), MDS_ERR_INVAL);

    /* Delegations. */
    memset(&drow, 0, sizeof(drow));
    memcpy(drow.stateid_other, sid_a, 12);
    drow.seqid = 1;
    drow.clientid = 100;
    drow.fileid = 500;
    drow.deleg_type = 1;
    drow.owner_mds_id = 3;
    drow.owner_boot_epoch = 77;
    drow.grant_time_ns = 123456789ULL;
    drow.recall_pending = 1;
    ASSERT_EQ(mds_coord_deleg_put(g_cat, &drow), MDS_OK);
    memset(&dget, 0, sizeof(dget));
    ASSERT_EQ(mds_coord_deleg_get(g_cat, sid_a, &dget), MDS_OK);
    ASSERT_EQ(dget.deleg_type, 1);
    ASSERT_EQ(dget.grant_time_ns, 123456789ULL);
    ASSERT_EQ(dget.recall_pending, 1);
    ASSERT_EQ(dget.clientid, 100);
    n = 0;
    ASSERT_EQ(mds_coord_deleg_scan_file(g_cat, 500, deleg_count_cb, &n), MDS_OK);
    ASSERT_EQ(n, 1);
    n = 0;
    ASSERT_EQ(mds_coord_deleg_scan_client(g_cat, 100, deleg_count_cb, &n), MDS_OK);
    ASSERT_EQ(n, 1);
    n = 0;
    ASSERT_EQ(mds_coord_deleg_scan_client(g_cat, 999, deleg_count_cb, &n), MDS_OK);
    ASSERT_EQ(n, 0);
    ASSERT_EQ(mds_coord_deleg_del(g_cat, sid_a), MDS_OK);
    ASSERT_EQ(mds_coord_deleg_del(g_cat, sid_a), MDS_ERR_NOTFOUND);
    n = 0;
    ASSERT_EQ(mds_coord_deleg_scan_file(g_cat, 500, deleg_count_cb, &n), MDS_OK);
    ASSERT_EQ(n, 0);

    /* Clients. */
    crow->clientid = 100;
    crow->co_ownerid_len = 3;
    memcpy(crow->co_ownerid, "abc", 3);
    memset(crow->verifier, 0x7, 8);
    crow->confirmed = true;
    crow->owner_mds_id = 3;
    crow->lease_renewed_ns = 42;
    ASSERT_EQ(mds_coord_client_put(g_cat, crow), MDS_OK);
    ASSERT_EQ(mds_coord_client_get(g_cat, 100, cget), MDS_OK);
    ASSERT_EQ(cget->confirmed, true);
    ASSERT_EQ(cget->co_ownerid_len, 3);
    ASSERT_EQ(memcmp(cget->co_ownerid, "abc", 3), 0);
    ASSERT_EQ(cget->verifier[7], 0x7);
    ASSERT_EQ(cget->lease_renewed_ns, 42);
    ASSERT_EQ(cget->clientid, 100);
    ASSERT_EQ(mds_coord_client_del(g_cat, 100), MDS_OK);
    ASSERT_EQ(mds_coord_client_get(g_cat, 100, cget), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_coord_client_del(g_cat, 100), MDS_ERR_NOTFOUND);
    crow->co_ownerid_len = 1025;
    ASSERT_EQ(mds_coord_client_put(g_cat, crow), MDS_ERR_INVAL);

    /* Sessions. */
    memset(&srow, 0, sizeof(srow));
    memcpy(srow.session_id, sess, 16);
    srow.clientid = 100;
    srow.num_slots = 8;
    srow.cb_prog = 0x40000000;
    srow.owner_mds_id = 3;
    srow.created_ns = 99;
    ASSERT_EQ(mds_coord_session_put(g_cat, &srow), MDS_OK);
    memset(&sget, 0, sizeof(sget));
    ASSERT_EQ(mds_coord_session_get(g_cat, sess, &sget), MDS_OK);
    ASSERT_EQ(sget.num_slots, 8);
    ASSERT_EQ(sget.cb_prog, 0x40000000);
    ASSERT_EQ(sget.created_ns, 99);
    ASSERT_EQ(memcmp(sget.session_id, sess, 16), 0);
    n = 0;
    ASSERT_EQ(mds_coord_session_scan_client(g_cat, 100, session_count_cb, &n), MDS_OK);
    ASSERT_EQ(n, 1);
    n = 0;
    ASSERT_EQ(mds_coord_session_scan_client(g_cat, 101, session_count_cb, &n), MDS_OK);
    ASSERT_EQ(n, 0);
    ASSERT_EQ(mds_coord_session_del(g_cat, sess), MDS_OK);
    ASSERT_EQ(mds_coord_session_del(g_cat, sess), MDS_ERR_NOTFOUND);
    n = 0;
    ASSERT_EQ(mds_coord_session_scan_client(g_cat, 100, session_count_cb, &n), MDS_OK);
    ASSERT_EQ(n, 0);

    /* DRC slots: the cached reply comes back as a heap copy. */
    ASSERT_EQ(mds_coord_slot_put(g_cat, sess, 3, 7, "reply", 5), MDS_OK);
    memset(&slot, 0, sizeof(slot));
    ASSERT_EQ(mds_coord_slot_get(g_cat, sess, 3, &slot), MDS_OK);
    ASSERT_EQ(slot.seq_id, 7);
    ASSERT_EQ(slot.reply_len, 5);
    ASSERT_EQ(slot.slot_id, 3);
    ASSERT_TRUE(slot.cached_reply != NULL);
    ASSERT_EQ(memcmp(slot.cached_reply, "reply", 5), 0);
    ASSERT_TRUE(slot.last_used_ns != 0);
    free(slot.cached_reply);
    ASSERT_EQ(mds_coord_slot_put(g_cat, sess, 3, 8, NULL, 0), MDS_OK);
    ASSERT_EQ(mds_coord_slot_get(g_cat, sess, 3, &slot), MDS_OK);
    ASSERT_EQ(slot.seq_id, 8);
    ASSERT_EQ(slot.reply_len, 0);
    ASSERT_TRUE(slot.cached_reply == NULL);
    ASSERT_EQ(mds_coord_slot_get(g_cat, sess, 4, &slot), MDS_ERR_NOTFOUND);
    /* A reply at the bound is stored; one byte more is refused. */
    big = malloc(FDB_SLOT_REPLY_MAX + 1U);
    ASSERT_TRUE(big != NULL);
    memset(big, 0x5A, FDB_SLOT_REPLY_MAX + 1U);
    ASSERT_EQ(mds_coord_slot_put(g_cat, sess, 5, 1, big, FDB_SLOT_REPLY_MAX), MDS_OK);
    ASSERT_EQ(mds_coord_slot_get(g_cat, sess, 5, &slot), MDS_OK);
    ASSERT_EQ(slot.reply_len, FDB_SLOT_REPLY_MAX);
    ASSERT_TRUE(slot.cached_reply != NULL);
    ASSERT_EQ(slot.cached_reply[FDB_SLOT_REPLY_MAX - 1], 0x5A);
    free(slot.cached_reply);
    ASSERT_EQ(mds_coord_slot_put(g_cat, sess, 6, 1, big, FDB_SLOT_REPLY_MAX + 1U),
              MDS_ERR_NOSPC);
    ASSERT_EQ(mds_coord_slot_put(g_cat, sess, 6, 1, NULL, 1), MDS_ERR_INVAL);
    free(big);
    free(crow);
    free(cget);
}

/* --- byte-range locks ----------------------------------------------------------- */

struct lock_scan_ctx {
    unsigned n;
    uint64_t lock_ids[8];
};

static int lock_scan_cb(const struct mds_coord_lock_row *row, void *arg)
{
    struct lock_scan_ctx *c = arg;

    if (c->n < 8) {
        c->lock_ids[c->n] = row->lock_id;
    }
    c->n++;
    return 0;
}

static struct mds_coord_lock_row mk_lock(uint64_t fileid, uint64_t lock_id, uint32_t type,
                                         uint64_t off, uint64_t len, uint64_t clientid,
                                         const char *owner)
{
    struct mds_coord_lock_row r;

    memset(&r, 0, sizeof(r));
    r.fileid = fileid;
    r.lock_id = lock_id;
    r.lock_type = type;
    r.offset = off;
    r.length = len;
    r.clientid = clientid;
    r.owner_len = (uint32_t)strlen(owner);
    memcpy(r.owner, owner, r.owner_len);
    memset(r.stateid_other, 0x33, sizeof(r.stateid_other));
    r.seqid = 5;
    r.owner_mds_id = 3;
    return r;
}

/* The memdb LOCKT conventions: OK = free, EXISTS = the conflicting row,
 * same owner never conflicts, READ/READ never conflicts, length 0 or
 * UINT64_MAX = to EOF. */
static void test_locks(void)
{
    struct mds_coord_lock_row row = mk_lock(7, 1, LT_WRITE, 0, 100, 1, "o1");
    struct mds_coord_lock_row conflict;
    struct lock_scan_ctx sc;
    const uint8_t *o1 = (const uint8_t *)"o1";
    const uint8_t *o2 = (const uint8_t *)"o2";

    ASSERT_EQ(mds_coord_lock_put(g_cat, &row), MDS_OK);
    row = mk_lock(7, 2, LT_READ, 1000, 0, 1, "o1");   /* to EOF */
    ASSERT_EQ(mds_coord_lock_put(g_cat, &row), MDS_OK);
    row = mk_lock(8, 3, LT_READ, 0, 10, 1, "o1");
    ASSERT_EQ(mds_coord_lock_put(g_cat, &row), MDS_OK);
    row = mk_lock(7, 4, LT_READ, 500, 10, 2, "o2");
    ASSERT_EQ(mds_coord_lock_put(g_cat, &row), MDS_OK);

    ASSERT_EQ(mds_coord_lock_test(g_cat, 7, LT_READ, 50, 10, 2, o2, 2, &conflict),
              MDS_ERR_EXISTS);
    ASSERT_EQ(conflict.lock_id, 1);
    ASSERT_EQ(conflict.clientid, 1);
    ASSERT_EQ(conflict.fileid, 7);
    ASSERT_EQ(conflict.owner_len, 2);
    ASSERT_EQ(conflict.seqid, 5);
    ASSERT_EQ(conflict.stateid_other[0], 0x33);
    ASSERT_EQ(mds_coord_lock_test(g_cat, 7, LT_WRITE, 50, 10, 1, o1, 2, &conflict), MDS_OK);
    ASSERT_EQ(conflict.lock_id, 0);
    ASSERT_EQ(mds_coord_lock_test(g_cat, 7, LT_WRITE, 200, 100, 2, o2, 2, &conflict), MDS_OK);
    ASSERT_EQ(mds_coord_lock_test(g_cat, 7, LT_READW, 2000, 10, 2, o2, 2, &conflict), MDS_OK);
    ASSERT_EQ(mds_coord_lock_test(g_cat, 7, LT_WRITE, 5000, 1, 2, o2, 2, &conflict),
              MDS_ERR_EXISTS);
    ASSERT_EQ(conflict.lock_id, 2);
    ASSERT_EQ(mds_coord_lock_test(g_cat, 7, LT_WRITE, 505, 1, 1, o2, 2, &conflict),
              MDS_ERR_EXISTS);
    ASSERT_EQ(conflict.lock_id, 4);
    ASSERT_EQ(mds_coord_lock_test(g_cat, 9, LT_WRITE, 0, 0, 2, o2, 2, &conflict), MDS_OK);
    ASSERT_EQ(mds_coord_lock_test(g_cat, 7, LT_WRITE, 0, 0, 2, NULL, 2, &conflict),
              MDS_ERR_INVAL);
    ASSERT_EQ(mds_coord_lock_test(g_cat, 7, LT_WRITE, 0, 0, 2, o2, 2, NULL), MDS_ERR_INVAL);

    /* Owner scan: (client 1, "o1") holds rows 1, 2 and 3 across two files. */
    memset(&sc, 0, sizeof(sc));
    ASSERT_EQ(mds_coord_lock_scan_owner(g_cat, 1, o1, 2, lock_scan_cb, &sc), MDS_OK);
    ASSERT_EQ(sc.n, 3);
    memset(&sc, 0, sizeof(sc));
    ASSERT_EQ(mds_coord_lock_scan_owner(g_cat, 2, o2, 2, lock_scan_cb, &sc), MDS_OK);
    ASSERT_EQ(sc.n, 1);
    ASSERT_EQ(sc.lock_ids[0], 4);
    memset(&sc, 0, sizeof(sc));
    ASSERT_EQ(mds_coord_lock_scan_owner(g_cat, 1, o2, 2, lock_scan_cb, &sc), MDS_OK);
    ASSERT_EQ(sc.n, 0);
    memset(&sc, 0, sizeof(sc));
    ASSERT_EQ(mds_coord_lock_scan_file(g_cat, 7, lock_scan_cb, &sc), MDS_OK);
    ASSERT_EQ(sc.n, 3);

    /* Upsert keyed (fileid, lock_id); delete; reap. */
    row = mk_lock(7, 1, LT_READ, 0, 100, 1, "o1");
    ASSERT_EQ(mds_coord_lock_put(g_cat, &row), MDS_OK);
    ASSERT_EQ(mds_coord_lock_test(g_cat, 7, LT_READ, 50, 10, 2, o2, 2, &conflict), MDS_OK);
    memset(&sc, 0, sizeof(sc));
    ASSERT_EQ(mds_coord_lock_scan_owner(g_cat, 1, o1, 2, lock_scan_cb, &sc), MDS_OK);
    ASSERT_EQ(sc.n, 3);
    ASSERT_EQ(mds_coord_lock_del(g_cat, 7, 1), MDS_OK);
    ASSERT_EQ(mds_coord_lock_del(g_cat, 7, 1), MDS_ERR_NOTFOUND);
    memset(&sc, 0, sizeof(sc));
    ASSERT_EQ(mds_coord_lock_scan_owner(g_cat, 1, o1, 2, lock_scan_cb, &sc), MDS_OK);
    ASSERT_EQ(sc.n, 2);
    ASSERT_EQ(mds_coord_lock_reap_client(g_cat, 1), MDS_OK);
    memset(&sc, 0, sizeof(sc));
    ASSERT_EQ(mds_coord_lock_scan_owner(g_cat, 1, o1, 2, lock_scan_cb, &sc), MDS_OK);
    ASSERT_EQ(sc.n, 0);
    memset(&sc, 0, sizeof(sc));
    ASSERT_EQ(mds_coord_lock_scan_file(g_cat, 7, lock_scan_cb, &sc), MDS_OK);
    ASSERT_EQ(sc.n, 1);
    ASSERT_EQ(sc.lock_ids[0], 4);
    memset(&sc, 0, sizeof(sc));
    ASSERT_EQ(mds_coord_lock_scan_file(g_cat, 8, lock_scan_cb, &sc), MDS_OK);
    ASSERT_EQ(sc.n, 0);
    ASSERT_EQ(mds_coord_lock_reap_client(g_cat, 1), MDS_OK);
    ASSERT_EQ(mds_coord_lock_reap_client(g_cat, 2), MDS_OK);
    memset(&sc, 0, sizeof(sc));
    ASSERT_EQ(mds_coord_lock_scan_file(g_cat, 7, lock_scan_cb, &sc), MDS_OK);
    ASSERT_EQ(sc.n, 0);
    row = mk_lock(7, 9, LT_READ, 0, 1, 1, "o1");
    row.owner_len = 129;
    ASSERT_EQ(mds_coord_lock_put(g_cat, &row), MDS_ERR_INVAL);
}

/* --- cluster ------------------------------------------------------------------- */

struct node_row {
    uint32_t mds_id;
    uint64_t boot_epoch;
    uint64_t last_heartbeat_ns;
    uint16_t nfs_port;
    uint16_t grpc_port;
    char     hostname[64];
};

struct node_list_ctx {
    unsigned n;
    struct node_row rows[4];
};

static int node_list_cb(uint32_t mds_id, uint64_t boot_epoch, const char *hostname,
                        uint16_t nfs_port, uint16_t grpc_port, uint64_t last_heartbeat_ns,
                        void *arg)
{
    struct node_list_ctx *c = arg;

    if (c->n < 4) {
        c->rows[c->n].mds_id = mds_id;
        c->rows[c->n].boot_epoch = boot_epoch;
        c->rows[c->n].last_heartbeat_ns = last_heartbeat_ns;
        c->rows[c->n].nfs_port = nfs_port;
        c->rows[c->n].grpc_port = grpc_port;
        (void)snprintf(c->rows[c->n].hostname, sizeof(c->rows[c->n].hostname), "%s", hostname);
    }
    c->n++;
    return 0;
}

static int stale_cb(uint32_t mds_id, uint64_t boot_epoch, uint64_t last_heartbeat_ns, void *arg)
{
    struct node_list_ctx *c = arg;

    if (c->n < 4) {
        c->rows[c->n].mds_id = mds_id;
        c->rows[c->n].boot_epoch = boot_epoch;
        c->rows[c->n].last_heartbeat_ns = last_heartbeat_ns;
    }
    c->n++;
    return 0;
}

struct part_row {
    uint32_t id;
    uint32_t owner;
    uint8_t  state;
    char     path[64];
};

struct part_list_ctx {
    unsigned n;
    struct part_row rows[4];
};

static int part_list_cb(uint32_t partition_id, uint32_t owner_mds_id, uint8_t state,
                        const char *subtree_path, void *arg)
{
    struct part_list_ctx *c = arg;

    if (c->n < 4) {
        c->rows[c->n].id = partition_id;
        c->rows[c->n].owner = owner_mds_id;
        c->rows[c->n].state = state;
        (void)snprintf(c->rows[c->n].path, sizeof(c->rows[c->n].path), "%s", subtree_path);
    }
    c->n++;
    return 0;
}

static void test_cluster_contract(void)
{
    struct node_list_ctx nl;
    struct part_list_ctx pl;
    uint64_t hb1;

    ASSERT_TRUE(mds_cluster_supported(g_cat));
    ASSERT_TRUE(mds_cluster_stale_scan_supported(g_cat));

    ASSERT_EQ(mds_cluster_node_register(g_cat, 1, 10, "mds1", 2049, 9401), MDS_OK);
    /* Duplicate live registration (equal epoch) and an older incarnation
     * are refused; the row is untouched. */
    ASSERT_EQ(mds_cluster_node_register(g_cat, 1, 10, "impostor", 1, 1), MDS_ERR_EXISTS);
    ASSERT_EQ(mds_cluster_node_register(g_cat, 1, 9, "older", 1, 1), MDS_ERR_EXISTS);
    memset(&nl, 0, sizeof(nl));
    ASSERT_EQ(mds_cluster_node_list(g_cat, node_list_cb, &nl), MDS_OK);
    ASSERT_EQ(nl.n, 1);
    ASSERT_EQ(nl.rows[0].mds_id, 1);
    ASSERT_EQ(nl.rows[0].boot_epoch, 10);
    ASSERT_EQ(nl.rows[0].nfs_port, 2049);
    ASSERT_EQ(nl.rows[0].grpc_port, 9401);
    ASSERT_EQ(strcmp(nl.rows[0].hostname, "mds1"), 0);
    ASSERT_TRUE(nl.rows[0].last_heartbeat_ns != 0);
    hb1 = nl.rows[0].last_heartbeat_ns;

    /* Old-epoch heartbeat: STALE and the row is unchanged. */
    ASSERT_EQ(mds_cluster_node_heartbeat(g_cat, 1, 9), MDS_ERR_STALE);
    memset(&nl, 0, sizeof(nl));
    ASSERT_EQ(mds_cluster_node_list(g_cat, node_list_cb, &nl), MDS_OK);
    ASSERT_EQ(nl.rows[0].boot_epoch, 10);
    ASSERT_EQ(nl.rows[0].last_heartbeat_ns, hb1);
    /* Matching epoch: the timestamp advances (CLOCK_REALTIME ns). */
    ASSERT_EQ(mds_cluster_node_heartbeat(g_cat, 1, 10), MDS_OK);
    memset(&nl, 0, sizeof(nl));
    ASSERT_EQ(mds_cluster_node_list(g_cat, node_list_cb, &nl), MDS_OK);
    ASSERT_TRUE(nl.rows[0].last_heartbeat_ns >= hb1);
    ASSERT_EQ(strcmp(nl.rows[0].hostname, "mds1"), 0);
    /* Unknown node: NOTFOUND passes through unchanged. */
    ASSERT_EQ(mds_cluster_node_heartbeat(g_cat, 2, 1), MDS_ERR_NOTFOUND);

    /* A newer incarnation replaces the row (restart with a higher epoch). */
    ASSERT_EQ(mds_cluster_node_register(g_cat, 1, 11, "mds1b", 2050, 9402), MDS_OK);
    memset(&nl, 0, sizeof(nl));
    ASSERT_EQ(mds_cluster_node_list(g_cat, node_list_cb, &nl), MDS_OK);
    ASSERT_EQ(nl.n, 1);
    ASSERT_EQ(nl.rows[0].boot_epoch, 11);
    ASSERT_EQ(strcmp(nl.rows[0].hostname, "mds1b"), 0);
    /* The old incarnation can neither heartbeat nor deregister it. */
    ASSERT_EQ(mds_cluster_node_heartbeat(g_cat, 1, 10), MDS_ERR_STALE);
    ASSERT_EQ(mds_cluster_node_deregister(g_cat, 1, 10), MDS_ERR_STALE);
    memset(&nl, 0, sizeof(nl));
    ASSERT_EQ(mds_cluster_node_list(g_cat, node_list_cb, &nl), MDS_OK);
    ASSERT_EQ(nl.n, 1);

    /* Stale scan: everything is older than a threshold in the future,
     * nothing is older than the epoch. */
    memset(&nl, 0, sizeof(nl));
    ASSERT_EQ(mds_cluster_node_scan_stale(g_cat, UINT64_MAX, stale_cb, &nl), MDS_OK);
    ASSERT_EQ(nl.n, 1);
    ASSERT_EQ(nl.rows[0].mds_id, 1);
    ASSERT_EQ(nl.rows[0].boot_epoch, 11);
    memset(&nl, 0, sizeof(nl));
    ASSERT_EQ(mds_cluster_node_scan_stale(g_cat, 1, stale_cb, &nl), MDS_OK);
    ASSERT_EQ(nl.n, 0);
    memset(&nl, 0, sizeof(nl));
    ASSERT_EQ(mds_cluster_node_scan_stale(g_cat, 0, stale_cb, &nl), MDS_OK);
    ASSERT_EQ(nl.n, 0);
    /* A second node: both listed, ascending id. */
    ASSERT_EQ(mds_cluster_node_register(g_cat, 2, 5, "mds2", 2049, 9401), MDS_OK);
    memset(&nl, 0, sizeof(nl));
    ASSERT_EQ(mds_cluster_node_list(g_cat, node_list_cb, &nl), MDS_OK);
    ASSERT_EQ(nl.n, 2);
    ASSERT_EQ(nl.rows[0].mds_id, 1);
    ASSERT_EQ(nl.rows[1].mds_id, 2);
    ASSERT_EQ(mds_cluster_node_deregister(g_cat, 2, 5), MDS_OK);

    /* Deregister: epoch match deletes, a retry of the delete is MDS_OK. */
    ASSERT_EQ(mds_cluster_node_deregister(g_cat, 1, 11), MDS_OK);
    ASSERT_EQ(mds_cluster_node_deregister(g_cat, 1, 11), MDS_OK);
    ASSERT_EQ(mds_cluster_node_heartbeat(g_cat, 1, 11), MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cluster_node_register(g_cat, 1, 11, "back", 2049, 9401), MDS_OK);
    ASSERT_EQ(mds_cluster_node_register(g_cat, 1, 12, "restarted", 1, 1), MDS_OK);
    ASSERT_EQ(mds_cluster_node_register(g_cat, 3, 1, NULL, 1, 1), MDS_ERR_INVAL);
    ASSERT_EQ(mds_cluster_node_register(g_cat, 3, 1, "", 1, 1), MDS_ERR_INVAL);
    ASSERT_EQ(mds_cluster_node_deregister(g_cat, 1, 12), MDS_OK);

    /* Partition map: the insert-only root claim, then upsert seeding. */
    ASSERT_EQ(mds_cluster_partition_put(g_cat, 0, 1, MDS_PARTITION_STATE_ACTIVE, "/", true),
              MDS_OK);
    ASSERT_EQ(mds_cluster_partition_put(g_cat, 0, 2, MDS_PARTITION_STATE_ACTIVE, "/", true),
              MDS_ERR_EXISTS);
    memset(&pl, 0, sizeof(pl));
    ASSERT_EQ(mds_cluster_partition_list(g_cat, part_list_cb, &pl), MDS_OK);
    ASSERT_EQ(pl.n, 1);
    ASSERT_EQ(pl.rows[0].owner, 1);
    ASSERT_EQ(pl.rows[0].state, MDS_PARTITION_STATE_ACTIVE);
    ASSERT_EQ(strcmp(pl.rows[0].path, "/"), 0);
    ASSERT_EQ(mds_cluster_partition_put(g_cat, 0, 2, MDS_PARTITION_STATE_MIGRATING, "/",
                                        false), MDS_OK);
    ASSERT_EQ(mds_cluster_partition_put(g_cat, 1, 2, MDS_PARTITION_STATE_ACTIVE, "/data",
                                        false), MDS_OK);
    memset(&pl, 0, sizeof(pl));
    ASSERT_EQ(mds_cluster_partition_list(g_cat, part_list_cb, &pl), MDS_OK);
    ASSERT_EQ(pl.n, 2);
    ASSERT_EQ(pl.rows[0].id, 0);
    ASSERT_EQ(pl.rows[0].owner, 2);
    ASSERT_EQ(pl.rows[0].state, MDS_PARTITION_STATE_MIGRATING);
    ASSERT_EQ(pl.rows[1].id, 1);
    ASSERT_EQ(strcmp(pl.rows[1].path, "/data"), 0);
    ASSERT_EQ(mds_cluster_partition_put(g_cat, 2, 2, MDS_PARTITION_STATE_ACTIVE, NULL, false),
              MDS_ERR_INVAL);
    ASSERT_EQ(mds_cluster_partition_put(g_cat, 2, 2, MDS_PARTITION_STATE_ACTIVE, "", false),
              MDS_ERR_INVAL);

    /* partition_cas: expected owner checked inside the transaction. */
    ASSERT_EQ(mds_cluster_partition_cas(g_cat, 0, 1, 3, MDS_PARTITION_STATE_ACTIVE),
              MDS_ERR_STALE);
    memset(&pl, 0, sizeof(pl));
    ASSERT_EQ(mds_cluster_partition_list(g_cat, part_list_cb, &pl), MDS_OK);
    ASSERT_EQ(pl.rows[0].owner, 2);
    ASSERT_EQ(pl.rows[0].state, MDS_PARTITION_STATE_MIGRATING);
    ASSERT_EQ(mds_cluster_partition_cas(g_cat, 0, 2, 3, MDS_PARTITION_STATE_ACTIVE), MDS_OK);
    memset(&pl, 0, sizeof(pl));
    ASSERT_EQ(mds_cluster_partition_list(g_cat, part_list_cb, &pl), MDS_OK);
    ASSERT_EQ(pl.rows[0].owner, 3);
    ASSERT_EQ(pl.rows[0].state, MDS_PARTITION_STATE_ACTIVE);
    ASSERT_EQ(strcmp(pl.rows[0].path, "/"), 0);
    ASSERT_EQ(mds_cluster_partition_cas(g_cat, 7, 0, 3, MDS_PARTITION_STATE_ACTIVE),
              MDS_ERR_NOTFOUND);
    ASSERT_EQ(mds_cluster_partition_cas(NULL, 0, 3, 4, MDS_PARTITION_STATE_ACTIVE),
              MDS_ERR_INVAL);
}

/* --- witness sweep bound ------------------------------------------------------- */

/* Raw single-key bodies over the handle's prefix: the sweep test writes
 * registry and witness rows of OTHER mds_ids by hand. */
struct raw_ctx {
    struct fdb_key key;
    uint8_t        val[FDB_NODE_ENC_MAX];
    size_t         len;
    bool           found;
    uint64_t       last_epoch;   /**< witness_rows_clear_body: highest epoch cleared. */
};

static int raw_set_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct raw_ctx *c = arg;

    fdb_txn_set(tr, &c->key, c->val, c->len);
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static int raw_get_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct raw_ctx *c = arg;
    fdb_error_t err;

    err = fdb_txn_get(tr, &c->key, false, c->val, sizeof(c->val), &c->len, &c->found);
    if (err != 0) {
        return (int)err;
    }
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

/* Clear WITNESS + mds_id + [0, last_epoch + 1): the rows the test wrote
 * by hand under an mds_id no handle of this process owns any more. */
static int witness_rows_clear_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct raw_ctx *c = arg;
    struct fdb_key_range r;

    r.begin = c->key;
    fdb_key_be64(&r.begin, 0);
    r.end = c->key;
    fdb_key_be64(&r.end, c->last_epoch + 1U);
    if (!fdb_key_ok(&r.begin) || !fdb_key_ok(&r.end)) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    fdb_txn_clear_range(tr, &r);
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static enum mds_status witness_row_set(struct fdb_backend *b, uint32_t mds_id, uint64_t epoch)
{
    struct raw_ctx c;

    memset(&c, 0, sizeof(c));
    fdb_key_witness(&c.key, &b->prefix, mds_id, epoch, 0);
    fdb_le64_put(c.val, 1);
    c.len = 8;
    return fdb_run_txn(b, FDB_TXN_MUTATING, "wit_set", raw_set_body, &c);
}

/* 1 present, 0 absent, -1 when the read failed. */
static int witness_row_present(struct fdb_backend *b, uint32_t mds_id, uint64_t epoch)
{
    struct raw_ctx c;

    memset(&c, 0, sizeof(c));
    fdb_key_witness(&c.key, &b->prefix, mds_id, epoch, 0);
    if (fdb_run_txn(b, FDB_TXN_READONLY, "wit_get", raw_get_body, &c) != MDS_OK) {
        return -1;
    }
    return c.found ? 1 : 0;
}

static enum mds_status witness_rows_clear(struct fdb_backend *b, uint32_t mds_id,
                                          uint64_t last_epoch)
{
    struct raw_ctx c;

    memset(&c, 0, sizeof(c));
    fdb_key_witness_mds_prefix(&c.key, &b->prefix, mds_id);
    c.last_epoch = last_epoch;
    return fdb_run_txn(b, FDB_TXN_MUTATING, "wit_clear", witness_rows_clear_body, &c);
}

/* A registry row for @p mds_id carrying @p witness_epoch, written by
 * hand: the slot would stamp this process's own epoch. */
static enum mds_status node_row_set(struct fdb_backend *b, uint32_t mds_id,
                                    uint64_t witness_epoch)
{
    struct raw_ctx c;
    struct fdb_node_val v;

    memset(&c, 0, sizeof(c));
    memset(&v, 0, sizeof(v));
    v.boot_epoch = 1;
    v.witness_epoch = witness_epoch;
    v.nfs_port = 2049;
    (void)snprintf(v.hostname, sizeof(v.hostname), "previous");
    fdb_key_init(&c.key, &b->prefix, FDB_KT_NODE_REGISTRY);
    fdb_key_be32(&c.key, mds_id);
    if (!fdb_node_encode(&v, c.val, sizeof(c.val), &c.len)) {
        return MDS_ERR_INVAL;
    }
    return fdb_run_txn(b, FDB_TXN_MUTATING, "node_set", raw_set_body, &c);
}

static bool node_row_get(struct fdb_backend *b, uint32_t mds_id, struct fdb_node_val *out)
{
    struct raw_ctx c;

    memset(&c, 0, sizeof(c));
    fdb_key_init(&c.key, &b->prefix, FDB_KT_NODE_REGISTRY);
    fdb_key_be32(&c.key, mds_id);
    if (fdb_run_txn(b, FDB_TXN_READONLY, "node_get", raw_get_body, &c) != MDS_OK || !c.found) {
        return false;
    }
    return fdb_node_decode(c.val, c.len, out);
}

/* The open-time witness sweep of an mds_id clears only the rows strictly
 * below the witness epoch its registry row carries (catalogue_fdb.c,
 * witness_clear_body): the registered incarnation's rows -- possibly a
 * still-running daemon's -- and the opener's own survive.  Every handle
 * of this process shares one epoch E, so the previous incarnation is
 * emulated by a hand-written registry row at W < E with witness rows at
 * W - 1, W and E.  Then: no row sweeps everything below E; a row
 * without an epoch sweeps nothing. */
static void test_witness_sweep_bound(void)
{
    struct fdb_backend *b = g_cat->backend_private;
    const uint64_t epoch = fdb_txn_witness_epoch();
    const uint64_t w = epoch - 1000U;
    const uint32_t id_reg = 41;
    const uint32_t id_none = 42;
    const uint32_t id_zero = 43;
    struct mds_catalogue *cat;
    struct fdb_node_val row;

    ASSERT_TRUE(b != NULL && epoch > 1000U);

    ASSERT_EQ(node_row_set(b, id_reg, w), MDS_OK);
    ASSERT_EQ(witness_row_set(b, id_reg, w - 1U), MDS_OK);
    ASSERT_EQ(witness_row_set(b, id_reg, w), MDS_OK);
    ASSERT_EQ(witness_row_set(b, id_reg, epoch), MDS_OK);
    cat = open_handle(id_reg);
    ASSERT_TRUE(cat != NULL);
    ASSERT_EQ(witness_row_present(b, id_reg, w - 1U), 0);
    ASSERT_EQ(witness_row_present(b, id_reg, w), 1);
    ASSERT_EQ(witness_row_present(b, id_reg, epoch), 1);
    /* Registering through the new handle stamps this process's epoch
     * and a heartbeat, which rewrites the row, keeps it. */
    ASSERT_EQ(mds_cluster_node_register(cat, id_reg, 2, "next", 2049, 9401), MDS_OK);
    ASSERT_TRUE(node_row_get(b, id_reg, &row));
    ASSERT_EQ(row.boot_epoch, 2);
    ASSERT_TRUE(row.witness_epoch == epoch);
    ASSERT_EQ(mds_cluster_node_heartbeat(cat, id_reg, 2), MDS_OK);
    ASSERT_TRUE(node_row_get(b, id_reg, &row));
    ASSERT_TRUE(row.witness_epoch == epoch);
    ASSERT_EQ(mds_cluster_node_deregister(cat, id_reg, 2), MDS_OK);
    mds_catalogue_close(cat);

    ASSERT_EQ(witness_row_set(b, id_none, epoch - 1U), MDS_OK);
    ASSERT_EQ(witness_row_set(b, id_none, epoch), MDS_OK);
    cat = open_handle(id_none);
    ASSERT_TRUE(cat != NULL);
    ASSERT_EQ(witness_row_present(b, id_none, epoch - 1U), 0);
    ASSERT_EQ(witness_row_present(b, id_none, epoch), 1);
    mds_catalogue_close(cat);

    ASSERT_EQ(node_row_set(b, id_zero, 0), MDS_OK);
    ASSERT_EQ(witness_row_set(b, id_zero, epoch - 1U), MDS_OK);
    cat = open_handle(id_zero);
    ASSERT_TRUE(cat != NULL);
    ASSERT_EQ(witness_row_present(b, id_zero, epoch - 1U), 1);
    mds_catalogue_close(cat);

    ASSERT_EQ(witness_rows_clear(b, id_reg, epoch), MDS_OK);
    ASSERT_EQ(witness_rows_clear(b, id_none, epoch), MDS_OK);
    ASSERT_EQ(witness_rows_clear(b, id_zero, epoch), MDS_OK);
    ASSERT_EQ(witness_row_present(b, id_zero, epoch - 1U), 0);
}

/* --- codecs: malformed values are rejected, never defaulted ------------------- */

static void test_codecs_reject(void)
{
    struct fdb_layout_hdr h;
    struct fdb_node_val nv;
    struct fdb_node_val nv2;
    struct fdb_partition_val pv;
    struct mds_coord_journal_record rec;
    uint32_t ids[3] = { 4, 5, 6 };
    uint32_t got[3];
    uint8_t buf[FDB_NODE_ENC_MAX];
    size_t len = 0;

    memset(&h, 0, sizeof(h));
    h.clientid = 1;
    h.fileid = 2;
    h.ds_count = 3;
    ASSERT_TRUE(fdb_layout_encode(&h, ids, buf, sizeof(buf), &len));
    ASSERT_EQ(len, FDB_LAYOUT_ENC_FIXED + 12);
    ASSERT_TRUE(fdb_layout_decode(buf, len, &h, got, 3));
    ASSERT_EQ(got[2], 6);
    ASSERT_TRUE(!fdb_layout_decode(buf, len, &h, got, 2));      /* list does not fit */
    ASSERT_TRUE(!fdb_layout_decode(buf, len - 1, &h, NULL, 0)); /* truncated */
    ASSERT_TRUE(!fdb_layout_decode(buf, len + 1, &h, NULL, 0)); /* padded */
    ASSERT_TRUE(fdb_layout_ds_contains(buf, len, 5));
    ASSERT_TRUE(!fdb_layout_ds_contains(buf, len, 7));
    h.ds_count = FDB_LAYOUT_DS_MAX + 1U;
    ASSERT_TRUE(!fdb_layout_encode(&h, ids, buf, sizeof(buf), &len));
    h.ds_count = 1;
    ASSERT_TRUE(!fdb_layout_encode(&h, NULL, buf, sizeof(buf), &len));

    memset(&nv, 0, sizeof(nv));
    nv.boot_epoch = 10;
    nv.nfs_port = 2049;
    nv.grpc_port = 9401;
    nv.last_heartbeat_ns = 12345;
    (void)snprintf(nv.sw_version, sizeof(nv.sw_version), "0.1.0");
    (void)snprintf(nv.hostname, sizeof(nv.hostname), "host");
    ASSERT_TRUE(fdb_node_encode(&nv, buf, sizeof(buf), &len));
    ASSERT_TRUE(fdb_node_decode(buf, len, &nv2));
    ASSERT_EQ(nv2.boot_epoch, 10);
    ASSERT_EQ(nv2.nfs_port, 2049);
    ASSERT_EQ(strcmp(nv2.hostname, "host"), 0);
    ASSERT_EQ(strcmp(nv2.sw_version, "0.1.0"), 0);
    ASSERT_TRUE(!fdb_node_decode(buf, len - 1, &nv2));
    buf[0] = 99; /* unknown version */
    ASSERT_TRUE(!fdb_node_decode(buf, len, &nv2));
    nv.hostname[0] = '\0';
    ASSERT_TRUE(!fdb_node_encode(&nv, buf, sizeof(buf), &len));

    memset(&pv, 0, sizeof(pv));
    pv.owner_mds_id = 1;
    ASSERT_TRUE(!fdb_partition_encode(&pv, buf, sizeof(buf), &len)); /* empty path */
    (void)snprintf(pv.subtree_path, sizeof(pv.subtree_path), "/a");
    ASSERT_TRUE(fdb_partition_encode(&pv, buf, sizeof(buf), &len));
    ASSERT_EQ(len, FDB_PARTITION_ENC_FIXED + 2);
    ASSERT_TRUE(!fdb_partition_decode(buf, len + 1, &pv));

    memset(&rec, 0, sizeof(rec));
    memset(rec.src_name, 'x', sizeof(rec.src_name)); /* not NUL-terminated */
    ASSERT_TRUE(!fdb_journal_encode(&rec, buf, sizeof(buf), &len));
}

/* ----------------------------------------------------------------------- */

static struct mds_catalogue *open_handle(uint32_t self_id)
{
    struct mds_config *cfg;
    struct mds_catalogue *cat = NULL;
    const char *cluster = getenv("FDB_CLUSTER_FILE");

    if (cluster == NULL || cluster[0] == '\0') {
        cluster = "/etc/foundationdb/fdb.cluster";
    }
    if (access(cluster, R_OK) != 0) {
        return NULL;
    }
    cfg = calloc(1, sizeof(*cfg));
    if (cfg == NULL) {
        return NULL;
    }
    cfg->catalogue_backend = MDS_BACKEND_FDB;
    cfg->self.id = self_id;
    (void)snprintf(cfg->fdb_key_prefix, sizeof(cfg->fdb_key_prefix), "%s", g_prefix);
    if (mds_catalogue_open(cfg, &cat) != MDS_OK) {
        cat = NULL;
    }
    free(cfg);
    return cat;
}

int main(void)
{
    printf("test_fdb_coord:\n");
    (void)snprintf(g_prefix, sizeof(g_prefix), "tc-%ld", (long)getpid());
    g_cat = open_handle(3);
    if (g_cat == NULL) {
        printf("SKIP: FoundationDB cluster not reachable\n");
        mds_catalogue_process_shutdown();
        return 77;
    }
    if (catalogue_fdb_keyspace_clear(g_cat) != MDS_OK ||
        mds_catalogue_bootstrap(g_cat) != MDS_OK) {
        printf("SKIP: FoundationDB keyspace not usable\n");
        mds_catalogue_close(g_cat);
        mds_catalogue_process_shutdown();
        return 77;
    }
    g_cat0 = open_handle(0);
    if (g_cat0 == NULL) {
        printf("FAIL: second handle cannot be opened\n");
        mds_catalogue_close(g_cat);
        mds_catalogue_process_shutdown();
        return 1;
    }

    RUN_TEST(test_codecs_reject);
    RUN_TEST(test_layout_indexes);
    RUN_TEST(test_layout_union);
    RUN_TEST(test_layout_del_all);
    RUN_TEST(test_layout_rebind);
    RUN_TEST(test_layoutget_fused);
    RUN_TEST(test_recovery);
    RUN_TEST(test_journal);
    RUN_TEST(test_shared_state);
    RUN_TEST(test_locks);
    RUN_TEST(test_cluster_contract);
    RUN_TEST(test_witness_sweep_bound);

    (void)catalogue_fdb_keyspace_clear(g_cat);
    mds_catalogue_close(g_cat0);
    mds_catalogue_close(g_cat);
    mds_catalogue_process_shutdown();

    printf("\n%d/%d tests passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}

#else /* !HAVE_FDB */

int main(void)
{
    printf("test_fdb_coord: SKIP (fdb backend not built)\n");
    return 77;
}

#endif
