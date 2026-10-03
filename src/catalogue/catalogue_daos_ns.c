/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * catalogue_daos_ns.c -- DAOS backend: namespace authority slots and the
 * shared inode / dirent / shard / marker primitives of
 * catalogue_daos_internal.h.
 *
 * Every slot is ONE daos_run_txn (daos_txn.h); the mds_cat_txn token is
 * never enlisted (C6).  Reads that can be issued together are started
 * as one parallel wave on the transaction's event queue and finished
 * afterwards, so a slot pays one round-trip latency per dependent wave,
 * not per object.  Enumerating slots materialise a bounded page inside
 * the transaction and deliver it after daos_run_txn returned (C1/C2):
 * a callback may re-enter this handle and a retried attempt never
 * redelivers.
 *
 * Transaction shapes (F = fetch RPC, L = key listing, U = buffered
 * update, P = buffered punch; every mutating transaction ends in one
 * commit RPC):
 *   ns_getattr           F 'a' (blob + K shards)                  (no transaction)
 *   ns_lookup            F dirent -> F child 'a' (blob + K shards) (read-only)
 *   ns_create            F parent blob+shard || F dirent [|| F child blob];
 *                        U child blob [+ shard 0], U 'N' akey (COND_AKEY_INSERT),
 *                        U 'S'+seq, [U stripe blob + STRIPE_IDX marker],
 *                        U parent shard
 *   ns_create_wide       the same with the child-absence read and stripe rows
 *   ns_link              F parent blob+shard || F target blob || F dirent;
 *                        U 'N' (COND_AKEY_INSERT) + 'S'+seq, U target blob,
 *                        U parent shard
 *   ns_setattr           F 'a' (blob + K shards); file: U blob;
 *                        directory: U blob + shard 0, P shards 1..K-1
 *   ns_nlink_adjust      F blob+shard; file: U blob / directory: U shard
 *   ns_remove*           F dirent || F parent blob+shard -> F child blob
 *                        [|| L child 'N' nr=1]; P 'N' akey + 'S'+seq dkey;
 *                        final: P 'a','x','i','h' + stripe blob + STRIPE_IDX
 *                        marker [+ U GC rows with their GC_BY_OWNER markers] /
 *                        non-final: U child blob; U parent shard
 *   ns_rename*           F src dirent || F dst dirent || F src parent blob+shard
 *                        || F dst parent blob+shard -> F src child blob
 *                        [|| F victim blob || L victim 'N' nr=1]; P src 'N' +
 *                        'S', [P victim 'S'; victim purge (stripes kept) /
 *                        U victim blob], U dst 'N' (overwrite) + 'S',
 *                        [U src child blob (parent_fileid)], U parent shards
 *   ns_readdir           per page: L 'N' akeys from key2anchor(name) -> ONE F
 *                        of the page's akeys                        (read-only)
 *   dirent_name_for_child  the same pages                           (read-only)
 *   ns_readdir_plus_from per page: L dkeys from key2anchor('S'+be64 cookie)
 *                        -> N parallel F of 'S' rows -> N parallel F of child
 *                        inodes                                     (read-only)
 *   dirent_put/insert/del F 'N' akey; U 'N' + 'S' / P 'N' + 'S'
 *   inode_put            U blob [+ shard 0, P shards 1..K-1]
 *   inode_del            F blob; P 'a'
 *   alloc_fileid         thread-local batch; refill: F counter, U counter
 *   ns_parent_touch      F blob+shard; U shard
 *
 * Enumeration order.  ns_readdir and dirent_name_for_child walk the 'N'
 * akeys of the directory in the store's hash order: a stable total
 * order, every name delivered exactly once, a start_after cursor
 * resuming strictly after that name in the same order -- but NOT
 * byte-wise name order (the names are hashed akeys; a sorted akey could
 * not hold a 255-byte name, daos_keys.h).  Every in-tree caller of
 * ns_readdir passes a NULL cursor and consumes the whole listing.  NFS
 * READDIR uses the cookie order of ns_readdir_plus_from ('S'+be64
 * sorted dkeys), which is strictly ascending within and across pages.
 *
 * READDIR cookies.  Each dirent carries a sequence minted from the
 * container-global META cookie counter in thread-local batches
 * (daos_backend_alloc_id): unique container-wide, hence unique per
 * directory, never below 16 (0/1/2 are reserved) and stable for the
 * life of the binding -- a rename or dirent_put rebinding gets a new
 * one.  A directory's `change` counter is NOT used: it is the sum of K
 * shards that concurrent creates write independently.
 *
 * Directory counters.  nlink, change, mtime and ctime of a directory are
 * K shards under dkey 'a' (daos_codec.h).  A mutation reads the shard
 * this thread owns (hash(thread) % K) in its first wave together with
 * the blob -- the same dkey, so one RPC -- and writes it back with the
 * deltas applied; two mutations of one directory conflict (and one
 * restarts) only when they pick the same shard.
 */

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "catalogue_daos.h"
#include "catalogue_daos_internal.h"
#include "catalogue_internal.h"
#include "daos_codec.h"
#include "daos_keys.h"
#include "daos_txn.h"
#include "ds_prealloc.h"
#include "fdb_codec.h"

/* Entries materialised per read-only page (one transaction each). */
#define DAOS_READDIR_PAGE      256U
/* Entries per readdir_plus page: each carries two parallel fetches. */
#define DAOS_READDIR_PLUS_PAGE 128U
/* Upper bound of GC rows one fused remove may fold. */
#define DAOS_GC_ROWS_MAX       4096U
/* Compact 'c' + be32 shard akey. */
#define DAOS_SHARD_AKEY_LEN    (1U + DAOS_KEY_BE32_LEN)

/* -----------------------------------------------------------------------
 * Small helpers
 * ----------------------------------------------------------------------- */

static struct daos_backend *be_of(const struct mds_catalogue *cat)
{
    return cat != NULL ? cat->backend_private : NULL;
}

static bool name_ok(const char *name)
{
    return name != NULL && name[0] != '\0' && strnlen(name, MDS_MAX_NAME + 1U) <= MDS_MAX_NAME;
}

static struct timespec now_ts(void)
{
    struct timespec ts;

    if (clock_gettime(CLOCK_REALTIME, &ts) != 0) {
        ts.tv_sec = 0;
        ts.tv_nsec = 0;
    }
    return ts;
}

/* Fresh inode record for a create (memdb / RonDB field parity). */
static void build_child(struct mds_inode *c, uint64_t fileid, uint64_t parent,
                        enum mds_file_type type, uint32_t mode, uint64_t uid, uint64_t gid,
                        struct timespec now)
{
    memset(c, 0, sizeof(*c));
    c->fileid = fileid;
    c->type = type;
    c->mode = mode;
    c->uid = uid;
    c->gid = gid;
    c->nlink = (type == MDS_FTYPE_DIR) ? 2U : 1U;
    c->atime = now;
    c->mtime = now;
    c->ctime = now;
    c->change = 1;
    c->generation = 1;
    c->parent_fileid = parent;
}

static size_t align16(size_t n)
{
    return (n + 15U) & ~(size_t)15U;
}

/* -----------------------------------------------------------------------
 * Inode read requests (catalogue_daos_internal.h)
 * ----------------------------------------------------------------------- */

static int req_alloc(struct daos_inode_req *r, uint32_t nshards, uint32_t first, bool all)
{
    size_t n = 1U + (size_t)nshards;
    size_t off_sgls = align16(n * sizeof(daos_iod_t));
    size_t off_iovs = align16(off_sgls + n * sizeof(d_sg_list_t));
    size_t off_shards = align16(off_iovs + n * sizeof(d_iov_t));
    size_t off_keys = align16(off_shards + (size_t)nshards * sizeof(struct daos_dir_shard));
    size_t off_vals = align16(off_keys + (size_t)nshards * DAOS_SHARD_AKEY_LEN);
    size_t total = off_vals + (size_t)nshards * DAOS_DIR_SHARD_ENC_SIZE;
    uint8_t *blk;
    uint32_t i;

    memset(r, 0, sizeof(*r));
    blk = calloc(1, total);
    if (blk == NULL) {
        return -DER_NOMEM;
    }
    r->block = blk;
    r->nshards = nshards;
    r->first_shard = first;
    r->all = all;
    r->f.iods = (daos_iod_t *)(void *)blk;
    r->f.sgls = (d_sg_list_t *)(void *)(blk + off_sgls);
    r->f.iovs = (d_iov_t *)(void *)(blk + off_iovs);
    r->shards = (struct daos_dir_shard *)(void *)(blk + off_shards);
    r->shard_keys = blk + off_keys;
    r->shard_buf = blk + off_vals;
    for (i = 0; i < nshards; i++) {
        uint8_t *k = r->shard_keys + (size_t)i * DAOS_SHARD_AKEY_LEN;

        k[0] = (uint8_t)'c';
        fdb_put_u32(k + 1, first + i);
    }
    daos_inode_req_rearm(r);
    return 0;
}

void daos_inode_req_rearm(struct daos_inode_req *r)
{
    struct daos_key dk;
    uint32_t i;

    if (r->block == NULL) {
        return;
    }
    daos_dkey_inode(&dk, DAOS_INODE_DKEY_ATTRS);
    daos_fetch_init(&r->f, &dk, r->f.iods, r->f.sgls, r->f.iovs, 1U + r->nshards);
    daos_akey_blob(&r->ak_blob);
    (void)daos_fetch_add(&r->f, &r->ak_blob, r->blob, sizeof(r->blob));
    for (i = 0; i < r->nshards; i++) {
        (void)daos_fetch_add_raw(&r->f, r->shard_keys + (size_t)i * DAOS_SHARD_AKEY_LEN,
                                 DAOS_SHARD_AKEY_LEN,
                                 r->shard_buf + (size_t)i * DAOS_DIR_SHARD_ENC_SIZE,
                                 DAOS_DIR_SHARD_ENC_SIZE);
    }
}

int daos_inode_req_init(struct daos_inode_req *r, const struct daos_backend *b, int32_t shard)
{
    if (r == NULL || b == NULL) {
        return -DER_INVAL;
    }
    if (shard == DAOS_INODE_ALL_SHARDS) {
        return req_alloc(r, b->dir_shards, 0, true);
    }
    if (shard < 0 || (uint32_t)shard >= b->dir_shards) {
        return -DER_INVAL;
    }
    return req_alloc(r, 1, (uint32_t)shard, false);
}

int daos_inode_req_init_blob(struct daos_inode_req *r)
{
    if (r == NULL) {
        return -DER_INVAL;
    }
    return req_alloc(r, 0, 0, false);
}

void daos_inode_req_fini(struct daos_inode_req *r)
{
    if (r != NULL) {
        free(r->block);
        r->block = NULL;
    }
}

int daos_inode_read_start(struct daos_txn *t, daos_handle_t oh, struct daos_inode_req *r)
{
    return daos_fetch_start(t, oh, &r->f);
}

int daos_inode_read_finish(struct daos_txn *t, struct daos_inode_req *r, struct mds_inode *out,
                           bool *found)
{
    size_t blen;
    uint32_t present = 0;
    uint32_t i;
    int rc;

    *found = false;
    rc = daos_fetch_finish(t, &r->f);
    if (rc == -DER_REC2BIG) {
        return -DER_IO; /* a value larger than its layout: corrupt row */
    }
    if (rc != 0) {
        return rc;
    }
    blen = daos_fetch_size(&r->f, 0);
    if (blen == 0) {
        return 0;
    }
    if (!fdb_inode_decode(r->blob, blen, out)) {
        return -DER_IO;
    }
    *found = true;
    if (!r->all || !fdb_inode_is_dir(out)) {
        return 0;
    }
    for (i = 0; i < r->nshards; i++) {
        size_t len = daos_fetch_size(&r->f, 1U + i);

        if (len == 0) {
            continue;
        }
        if (!daos_dir_shard_decode(r->shard_buf + (size_t)i * DAOS_DIR_SHARD_ENC_SIZE, len,
                                   &r->shards[present])) {
            return -DER_IO;
        }
        present++;
    }
    daos_dir_shards_compose(out, r->shards, present);
    return 0;
}

bool daos_inode_req_shard(const struct daos_inode_req *r, struct daos_dir_shard *out,
                          bool *present)
{
    size_t len;

    memset(out, 0, sizeof(*out));
    *present = false;
    if (r->nshards != 1 || r->all) {
        return false;
    }
    len = daos_fetch_size(&r->f, 1);
    if (len == 0) {
        return true;
    }
    *present = true;
    return daos_dir_shard_decode(r->shard_buf, len, out);
}

int daos_inode_read_full(struct daos_txn *t, daos_handle_t oh, struct mds_inode *out,
                         bool *found)
{
    struct daos_inode_req r;
    int rc = daos_inode_req_init(&r, t->b, DAOS_INODE_ALL_SHARDS);

    if (rc != 0) {
        return rc;
    }
    rc = daos_inode_read_start(t, oh, &r);
    if (rc == 0) {
        rc = daos_inode_read_finish(t, &r, out, found);
    }
    daos_inode_req_fini(&r);
    return rc;
}

int daos_inode_read_blob(struct daos_txn *t, daos_handle_t oh, struct mds_inode *out,
                         bool *found)
{
    struct daos_inode_req r;
    int rc = daos_inode_req_init_blob(&r);

    if (rc != 0) {
        return rc;
    }
    rc = daos_inode_read_start(t, oh, &r);
    if (rc == 0) {
        rc = daos_inode_read_finish(t, &r, out, found);
    }
    daos_inode_req_fini(&r);
    return rc;
}

/* -----------------------------------------------------------------------
 * Inode writes
 * ----------------------------------------------------------------------- */

int daos_inode_write_blob(struct daos_txn *t, daos_handle_t oh, const struct mds_inode *ino)
{
    struct mds_inode blob_copy;
    struct fdb_dir_counters ctr;
    uint8_t enc[FDB_INODE_ENC_MAX];
    size_t len = 0;
    struct daos_key dk;
    struct daos_key ak;

    fdb_inode_split(ino, &blob_copy, &ctr);
    if (!fdb_inode_encode(&blob_copy, enc, sizeof(enc), &len)) {
        return -DER_INVAL;
    }
    daos_dkey_inode(&dk, DAOS_INODE_DKEY_ATTRS);
    daos_akey_blob(&ak);
    return daos_update_single(t, oh, &dk, &ak, enc, len, 0, 0);
}

/* Punch the counter shards [from, K) of the directory @p oh. */
static int shards_punch(struct daos_txn *t, daos_handle_t oh, uint32_t from)
{
    struct daos_key dk;
    struct daos_key aks[DAOS_PUNCH_MAX];
    uint32_t k = t->b->dir_shards;
    uint32_t i = from;

    daos_dkey_inode(&dk, DAOS_INODE_DKEY_ATTRS);
    while (i < k) {
        uint32_t n = 0;
        int rc;

        while (n < DAOS_PUNCH_MAX && i < k) {
            daos_akey_dir_shard(&aks[n], i);
            n++;
            i++;
        }
        rc = daos_punch_akeys(t, oh, &dk, aks, n, 0);
        if (rc != 0) {
            return rc;
        }
    }
    return 0;
}

int daos_inode_write_full(struct daos_txn *t, daos_handle_t oh, const struct mds_inode *ino,
                          bool fresh)
{
    struct mds_inode blob_copy;
    struct fdb_dir_counters ctr;
    struct daos_dir_shard s0;
    struct daos_update_small u;
    struct daos_key dk;
    struct daos_key ak_blob;
    struct daos_key ak_shard;
    uint8_t enc[FDB_INODE_ENC_MAX];
    uint8_t senc[DAOS_DIR_SHARD_ENC_SIZE];
    size_t len = 0;
    size_t slen = 0;
    bool dir = fdb_inode_is_dir(ino);
    int rc;

    fdb_inode_split(ino, &blob_copy, &ctr);
    if (!fdb_inode_encode(&blob_copy, enc, sizeof(enc), &len)) {
        return -DER_INVAL;
    }
    daos_dkey_inode(&dk, DAOS_INODE_DKEY_ATTRS);
    daos_akey_blob(&ak_blob);
    daos_update_small_init(&u, &dk, 0);
    if (!daos_update_add(&u.u, &ak_blob, enc, len, 0)) {
        return -DER_INVAL;
    }
    if (dir) {
        /* Shard 0 carries the whole directory state of this write. */
        s0.nlink_delta = (int64_t)ctr.nlink;
        s0.change = ctr.change;
        s0.mtime_ns = (int64_t)ctr.mtime_ns;
        s0.ctime_ns = (int64_t)ctr.ctime_ns;
        if (!daos_dir_shard_encode(&s0, senc, sizeof(senc), &slen)) {
            return -DER_INVAL;
        }
        daos_akey_dir_shard(&ak_shard, 0);
        if (!daos_update_add(&u.u, &ak_shard, senc, slen, 0)) {
            return -DER_INVAL;
        }
    }
    rc = daos_update_run(t, oh, &u.u);
    if (rc != 0 || fresh) {
        return rc;
    }
    return shards_punch(t, oh, dir ? 1U : 0U);
}

/* -----------------------------------------------------------------------
 * Directory counter shards
 * ----------------------------------------------------------------------- */

static _Thread_local uint32_t tl_shard_hash;
static _Thread_local bool     tl_shard_hash_set;

uint32_t daos_dir_shard_pick(const struct daos_backend *b)
{
    if (!tl_shard_hash_set) {
        uint64_t x = (uint64_t)(uintptr_t)pthread_self();

        x ^= x >> 33;
        x *= 0xff51afd7ed558ccdULL;
        x ^= x >> 33;
        x *= 0xc4ceb9fe1a85ec53ULL;
        x ^= x >> 33;
        tl_shard_hash = (uint32_t)x ^ (uint32_t)(x >> 32);
        tl_shard_hash_set = true;
    }
    if (b == NULL || b->dir_shards == 0) {
        return 0;
    }
    return tl_shard_hash % b->dir_shards;
}

void daos_dir_shard_apply(struct daos_dir_shard *s, int64_t nlink_delta, uint64_t change_delta,
                          const struct timespec *stamp)
{
    if (nlink_delta > 0 && s->nlink_delta > INT64_MAX - nlink_delta) {
        s->nlink_delta = INT64_MAX;
    } else if (nlink_delta < 0 && s->nlink_delta < INT64_MIN - nlink_delta) {
        s->nlink_delta = INT64_MIN;
    } else {
        s->nlink_delta += nlink_delta;
    }
    s->change += change_delta;
    if (stamp != NULL) {
        int64_t ns = fdb_ts_to_ns(*stamp);

        s->mtime_ns = ns;
        s->ctime_ns = ns;
    }
}

int daos_dir_shard_write(struct daos_txn *t, daos_handle_t oh, uint32_t idx,
                         const struct daos_dir_shard *s)
{
    uint8_t enc[DAOS_DIR_SHARD_ENC_SIZE];
    size_t len = 0;
    struct daos_key dk;
    struct daos_key ak;

    if (!daos_dir_shard_encode(s, enc, sizeof(enc), &len)) {
        return -DER_INVAL;
    }
    daos_dkey_inode(&dk, DAOS_INODE_DKEY_ATTRS);
    daos_akey_dir_shard(&ak, idx);
    return daos_update_single(t, oh, &dk, &ak, enc, len, 0, 0);
}

int daos_dir_touch(struct daos_txn *t, daos_handle_t oh, uint32_t idx, int64_t nlink_delta,
                   uint64_t change_delta, const struct timespec *stamp)
{
    struct daos_dir_shard s;
    struct daos_key dk;
    struct daos_key ak;
    uint8_t buf[DAOS_DIR_SHARD_ENC_SIZE];
    size_t len = 0;
    bool found = false;
    int rc;

    daos_dkey_inode(&dk, DAOS_INODE_DKEY_ATTRS);
    daos_akey_dir_shard(&ak, idx);
    rc = daos_fetch_single(t, oh, &dk, &ak, buf, sizeof(buf), &len, &found);
    if (rc == -DER_REC2BIG) {
        return -DER_IO;
    }
    if (rc != 0) {
        return rc;
    }
    memset(&s, 0, sizeof(s));
    if (found && !daos_dir_shard_decode(buf, len, &s)) {
        return -DER_IO;
    }
    daos_dir_shard_apply(&s, nlink_delta, change_delta, stamp);
    return daos_dir_shard_write(t, oh, idx, &s);
}

int daos_dir_nonempty(struct daos_txn *t, daos_handle_t oh, bool *nonempty)
{
    daos_key_desc_t kds[1];
    uint8_t buf[DAOS_KEY_MAX];
    struct daos_list l;
    struct daos_key dk;
    int rc;

    *nonempty = false;
    daos_dkey_inode(&dk, DAOS_INODE_DKEY_DIRENTS);
    daos_list_init(&l, kds, 1, buf, sizeof(buf));
    rc = daos_list_akeys(t, oh, &dk, NULL, &l);
    if (rc != 0) {
        return rc;
    }
    *nonempty = (l.nr > 0);
    return 0;
}

/* -----------------------------------------------------------------------
 * Dirents
 * ----------------------------------------------------------------------- */

int daos_dirent_read_start(struct daos_txn *t, daos_handle_t oh, const char *name,
                           struct daos_dirent_req *r)
{
    struct daos_key dk;

    daos_dkey_inode(&dk, DAOS_INODE_DKEY_DIRENTS);
    daos_fetch_small_init(&r->f, &dk);
    daos_akey_name(&r->ak, name);
    if (!daos_fetch_add(&r->f.f, &r->ak, r->buf, sizeof(r->buf))) {
        return -DER_INVAL;
    }
    return daos_fetch_start(t, oh, &r->f.f);
}

int daos_dirent_read_finish(struct daos_txn *t, struct daos_dirent_req *r,
                            struct fdb_dirent_val *out, bool *found)
{
    size_t len;
    int rc;

    *found = false;
    rc = daos_fetch_finish(t, &r->f.f);
    if (rc == -DER_REC2BIG) {
        return -DER_IO;
    }
    if (rc != 0) {
        return rc;
    }
    len = daos_fetch_size(&r->f.f, 0);
    if (len == 0) {
        return 0;
    }
    if (!fdb_dirent_decode(r->buf, len, out)) {
        return -DER_IO;
    }
    *found = true;
    return 0;
}

int daos_dirent_read(struct daos_txn *t, daos_handle_t oh, const char *name,
                     struct fdb_dirent_val *out, bool *found)
{
    struct daos_dirent_req r;
    int rc = daos_dirent_read_start(t, oh, name, &r);

    if (rc != 0) {
        *found = false;
        return rc;
    }
    return daos_dirent_read_finish(t, &r, out, found);
}

int daos_dirent_write(struct daos_txn *t, daos_handle_t oh, const char *name, uint64_t child,
                      uint8_t type, uint64_t seq, bool insert_only)
{
    struct fdb_dirent_val dv = { child, seq, type };
    struct fdb_dirent_seq_val sv;
    uint8_t enc[FDB_DIRENT_SEQ_ENC_MAX];
    size_t len = 0;
    struct daos_key dk;
    struct daos_key ak;
    int rc;

    if (!fdb_dirent_encode(&dv, enc, sizeof(enc), &len)) {
        return -DER_INVAL;
    }
    daos_dkey_inode(&dk, DAOS_INODE_DKEY_DIRENTS);
    daos_akey_name(&ak, name);
    rc = daos_update_single(t, oh, &dk, &ak, enc, len, 0,
                            insert_only ? (uint64_t)DAOS_COND_AKEY_INSERT : 0U);
    if (rc != 0) {
        return rc;
    }
    memset(&sv, 0, sizeof(sv));
    sv.child_fileid = child;
    sv.type = type;
    (void)snprintf(sv.name, sizeof(sv.name), "%s", name);
    if (!fdb_dirent_seq_encode(&sv, enc, sizeof(enc), &len)) {
        return -DER_INVAL;
    }
    daos_dkey_inode_dirent_seq(&dk, seq);
    daos_akey_value(&ak);
    return daos_update_single(t, oh, &dk, &ak, enc, len, 0, 0);
}

int daos_dirent_seq_clear(struct daos_txn *t, daos_handle_t oh, uint64_t seq)
{
    struct daos_key dk;

    daos_dkey_inode_dirent_seq(&dk, seq);
    return daos_punch_dkeys(t, oh, &dk, 1, 0);
}

int daos_dirent_clear(struct daos_txn *t, daos_handle_t oh, const char *name, uint64_t seq)
{
    struct daos_key dk;
    struct daos_key ak;
    int rc;

    daos_dkey_inode(&dk, DAOS_INODE_DKEY_DIRENTS);
    daos_akey_name(&ak, name);
    rc = daos_punch_akeys(t, oh, &dk, &ak, 1, 0);
    if (rc != 0) {
        return rc;
    }
    return daos_dirent_seq_clear(t, oh, seq);
}

/* -----------------------------------------------------------------------
 * Markers, GC rows, stripe map, purge
 * ----------------------------------------------------------------------- */

int daos_marker_set(struct daos_txn *t, daos_handle_t oh, const struct daos_key *dkey,
                    struct daos_key *akey)
{
    static const uint8_t marker = DAOS_MARKER_BYTE;

    return daos_update_single(t, oh, dkey, akey, &marker, sizeof(marker), 0, 0);
}

int daos_marker_clear(struct daos_txn *t, daos_handle_t oh, const struct daos_key *dkey,
                      struct daos_key *akey)
{
    return daos_punch_akeys(t, oh, dkey, akey, 1, 0);
}

int daos_gc_row_set(struct daos_txn *t, const struct mds_gc_entry *e)
{
    uint8_t enc[FDB_GC_ENC_MAX];
    size_t len = 0;
    struct daos_key dk;
    struct daos_key ak;
    daos_handle_t oh;
    int rc;

    if (!fdb_gc_encode(e, enc, sizeof(enc), &len)) {
        return -DER_INVAL;
    }
    rc = daos_txn_obj(t, DAOS_TAG_GC, 0, &oh);
    if (rc != 0) {
        return rc;
    }
    daos_dkey_be64(&dk, e->gc_seq);
    daos_akey_value(&ak);
    rc = daos_update_single(t, oh, &dk, &ak, enc, len, 0, 0);
    if (rc != 0) {
        return rc;
    }
    rc = daos_txn_obj(t, DAOS_TAG_GC_BY_OWNER, e->owner_mds_id, &oh);
    if (rc != 0) {
        return rc;
    }
    daos_dkey_u64(&dk, e->gc_seq);
    daos_akey_marker(&ak);
    return daos_marker_set(t, oh, &dk, &ak);
}

int daos_gc_row_clear(struct daos_txn *t, uint64_t gc_seq, uint32_t owner_mds_id)
{
    struct daos_key dk;
    daos_handle_t oh;
    int rc;

    rc = daos_txn_obj(t, DAOS_TAG_GC, 0, &oh);
    if (rc != 0) {
        return rc;
    }
    daos_dkey_be64(&dk, gc_seq);
    rc = daos_punch_dkeys(t, oh, &dk, 1, 0);
    if (rc != 0) {
        return rc;
    }
    rc = daos_txn_obj(t, DAOS_TAG_GC_BY_OWNER, owner_mds_id, &oh);
    if (rc != 0) {
        return rc;
    }
    daos_dkey_u64(&dk, gc_seq);
    return daos_punch_dkeys(t, oh, &dk, 1, 0);
}

int daos_stripe_map_write(struct daos_txn *t, daos_handle_t inode_oh, uint64_t fileid,
                          uint32_t sc, uint32_t su, uint32_t mc,
                          const struct mds_ds_map_entry *entries)
{
    size_t size = daos_stripe_map_enc_size(sc, mc, entries);
    size_t len = 0;
    uint8_t *enc;
    struct daos_key dk;
    struct daos_key ak;
    daos_handle_t idx_oh;
    int rc;

    if (size == 0 || su == 0) {
        return -DER_INVAL;
    }
    enc = malloc(size);
    if (enc == NULL) {
        return -DER_NOMEM;
    }
    if (!daos_stripe_map_encode(sc, su, mc, entries, enc, size, &len)) {
        free(enc);
        return -DER_INVAL;
    }
    daos_dkey_inode(&dk, DAOS_INODE_DKEY_STRIPE);
    daos_akey_stripe_map(&ak);
    rc = daos_update_single(t, inode_oh, &dk, &ak, enc, len, 0, 0);
    free(enc); /* the client copied the value when the update was issued */
    if (rc != 0) {
        return rc;
    }
    rc = daos_txn_obj(t, DAOS_TAG_STRIPE_IDX, 0, &idx_oh);
    if (rc != 0) {
        return rc;
    }
    daos_dkey_be64(&dk, fileid);
    daos_akey_marker(&ak);
    return daos_marker_set(t, idx_oh, &dk, &ak);
}

int daos_stripe_map_clear(struct daos_txn *t, daos_handle_t inode_oh, uint64_t fileid)
{
    struct daos_key dk;
    daos_handle_t idx_oh;
    int rc;

    daos_dkey_inode(&dk, DAOS_INODE_DKEY_STRIPE);
    rc = daos_punch_dkeys(t, inode_oh, &dk, 1, 0);
    if (rc != 0) {
        return rc;
    }
    rc = daos_txn_obj(t, DAOS_TAG_STRIPE_IDX, 0, &idx_oh);
    if (rc != 0) {
        return rc;
    }
    daos_dkey_be64(&dk, fileid);
    return daos_punch_dkeys(t, idx_oh, &dk, 1, 0);
}

int daos_inode_purge(struct daos_txn *t, daos_handle_t oh, uint64_t fileid, bool keep_stripes)
{
    struct daos_key dks[4];
    int rc;

    daos_dkey_inode(&dks[0], DAOS_INODE_DKEY_ATTRS);
    daos_dkey_inode(&dks[1], DAOS_INODE_DKEY_XATTRS);
    daos_dkey_inode(&dks[2], DAOS_INODE_DKEY_INLINE);
    daos_dkey_inode(&dks[3], DAOS_INODE_DKEY_SHARD);
    rc = daos_punch_dkeys(t, oh, dks, 4, 0);
    if (rc != 0 || keep_stripes) {
        return rc;
    }
    return daos_stripe_map_clear(t, oh, fileid);
}

/* -----------------------------------------------------------------------
 * The common first wave: parent blob + this thread's shard (one fetch of
 * dkey 'a'), the dirent, and optionally the blob of another inode.
 * ----------------------------------------------------------------------- */

struct ns_wave_out {
    struct mds_inode      parent;
    struct daos_dir_shard shard;        /**< Zero when absent. */
    struct fdb_dirent_val dv;
    struct mds_inode      extra;
    uint32_t              shard_idx;
    bool                  parent_found;
    bool                  shard_present;
    bool                  dirent_found;
    bool                  extra_found;
};

static int ns_wave_run(struct daos_txn *t, daos_handle_t oh_parent, const char *name,
                       bool want_extra, daos_handle_t oh_extra, struct ns_wave_out *o)
{
    struct daos_inode_req rp;
    struct daos_inode_req rx;
    struct daos_dirent_req rd;
    bool sp = false;
    bool sd = false;
    bool sx = false;
    int rc;
    int rc2;

    memset(o, 0, sizeof(*o));
    rx.block = NULL;
    o->shard_idx = daos_dir_shard_pick(t->b);
    rc = daos_inode_req_init(&rp, t->b, (int32_t)o->shard_idx);
    if (rc != 0) {
        return rc;
    }
    if (want_extra) {
        rc = daos_inode_req_init_blob(&rx);
        if (rc != 0) {
            daos_inode_req_fini(&rp);
            return rc;
        }
    }
    rc = daos_inode_read_start(t, oh_parent, &rp);
    sp = (rc == 0);
    if (rc == 0) {
        rc = daos_dirent_read_start(t, oh_parent, name, &rd);
        sd = (rc == 0);
    }
    if (rc == 0 && want_extra) {
        rc = daos_inode_read_start(t, oh_extra, &rx);
        sx = (rc == 0);
    }
    /* Finish every started request, keeping the first error. */
    if (sp) {
        rc2 = daos_inode_read_finish(t, &rp, &o->parent, &o->parent_found);
        rc = (rc == 0) ? rc2 : rc;
    }
    if (sd) {
        rc2 = daos_dirent_read_finish(t, &rd, &o->dv, &o->dirent_found);
        rc = (rc == 0) ? rc2 : rc;
    }
    if (sx) {
        rc2 = daos_inode_read_finish(t, &rx, &o->extra, &o->extra_found);
        rc = (rc == 0) ? rc2 : rc;
    }
    if (rc == 0 && !daos_inode_req_shard(&rp, &o->shard, &o->shard_present)) {
        rc = -DER_IO;
    }
    daos_inode_req_fini(&rp);
    daos_inode_req_fini(&rx);
    return rc;
}

/* Directory bookkeeping on the wave's shard: change += 1, mtime/ctime =
 * now, nlink += delta; one blind update of the shard read in the wave. */
static int parent_shard_commit(struct daos_txn *t, daos_handle_t oh, struct ns_wave_out *o,
                               int64_t nlink_delta, struct timespec now)
{
    daos_dir_shard_apply(&o->shard, nlink_delta, 1, &now);
    return daos_dir_shard_write(t, oh, o->shard_idx, &o->shard);
}

/* -----------------------------------------------------------------------
 * ns_lookup / ns_getattr
 * ----------------------------------------------------------------------- */

struct lookup_ctx {
    uint64_t         parent;
    const char      *name;
    struct mds_inode out;
};

static int lookup_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct lookup_ctx *c = arg;
    struct fdb_dirent_val dv;
    daos_handle_t oh;
    bool found = false;
    int rc;

    rc = daos_txn_obj(t, DAOS_TAG_INODE, c->parent, &oh);
    if (rc != 0) {
        return rc;
    }
    rc = daos_dirent_read(t, oh, c->name, &dv, &found);
    if (rc != 0) {
        return rc;
    }
    if (!found) {
        *st_out = MDS_ERR_NOTFOUND;
        return DAOS_BODY_DONE;
    }
    rc = daos_txn_obj(t, DAOS_TAG_INODE, dv.child_fileid, &oh);
    if (rc != 0) {
        return rc;
    }
    rc = daos_inode_read_full(t, oh, &c->out, &found);
    if (rc != 0) {
        return rc;
    }
    *st_out = found ? MDS_OK : MDS_ERR_NOTFOUND;
    return DAOS_BODY_COMMIT;
}

static enum mds_status daos_ns_lookup(struct mds_catalogue *cat, uint64_t parent,
                                      const char *name, struct mds_inode *child)
{
    struct lookup_ctx c;
    enum mds_status st;

    if (be_of(cat) == NULL || !name_ok(name) || child == NULL) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.parent = parent;
    c.name = name;
    st = daos_run_txn(be_of(cat), DAOS_TXN_READONLY, "ns_lookup", lookup_body, &c);
    if (st == MDS_OK) {
        *child = c.out;
    }
    return st;
}

struct getattr_ctx {
    uint64_t         fileid;
    struct mds_inode out;
};

static int getattr_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct getattr_ctx *c = arg;
    daos_handle_t oh;
    bool found = false;
    int rc;

    rc = daos_txn_obj(t, DAOS_TAG_INODE, c->fileid, &oh);
    if (rc != 0) {
        return rc;
    }
    rc = daos_inode_read_full(t, oh, &c->out, &found);
    if (rc != 0) {
        return rc;
    }
    *st_out = found ? MDS_OK : MDS_ERR_NOTFOUND;
    return DAOS_BODY_COMMIT;
}

static enum mds_status daos_ns_getattr(struct mds_catalogue *cat, uint64_t fileid,
                                       struct mds_inode *inode)
{
    struct getattr_ctx c;
    enum mds_status st;

    if (be_of(cat) == NULL || inode == NULL) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.fileid = fileid;
    st = daos_run_txn(be_of(cat), DAOS_TXN_NONE, "ns_getattr", getattr_body, &c);
    if (st == MDS_OK) {
        *inode = c.out;
    }
    return st;
}

/* -----------------------------------------------------------------------
 * ns_create / ns_create_wide
 * ----------------------------------------------------------------------- */

struct create_ctx {
    uint64_t                       parent;
    const char                    *name;
    struct mds_inode               child;   /**< Fully built before the body. */
    uint64_t                       seq;     /**< Dirent cookie, minted once. */
    bool                           check_child_absent; /**< create_wide: EXISTS on a live child. */
    /* Stripe rows written with the child (count 0 = none). */
    uint32_t                       sc;
    uint32_t                       su;
    uint32_t                       mc;
    const struct mds_ds_map_entry *entries;
};

static int create_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct create_ctx *c = arg;
    struct ns_wave_out w;
    daos_handle_t oh_p;
    daos_handle_t oh_c;
    int rc;

    rc = daos_txn_obj(t, DAOS_TAG_INODE, c->parent, &oh_p);
    if (rc != 0) {
        return rc;
    }
    rc = daos_txn_obj(t, DAOS_TAG_INODE, c->child.fileid, &oh_c);
    if (rc != 0) {
        return rc;
    }
    rc = ns_wave_run(t, oh_p, c->name, c->check_child_absent, oh_c, &w);
    if (rc != 0) {
        return rc;
    }
    if (!w.parent_found) {
        *st_out = MDS_ERR_NOTFOUND;
        return DAOS_BODY_DONE;
    }
    if (!fdb_inode_is_dir(&w.parent)) {
        *st_out = MDS_ERR_NOTDIR;
        return DAOS_BODY_DONE;
    }
    if (w.dirent_found || w.extra_found) {
        *st_out = MDS_ERR_EXISTS;
        return DAOS_BODY_DONE;
    }
    rc = daos_inode_write_full(t, oh_c, &c->child, true);
    if (rc != 0) {
        return rc;
    }
    rc = daos_dirent_write(t, oh_p, c->name, c->child.fileid, (uint8_t)c->child.type, c->seq,
                           true);
    if (rc != 0) {
        return rc;
    }
    if (c->sc > 0) {
        rc = daos_stripe_map_write(t, oh_c, c->child.fileid, c->sc, c->su, c->mc, c->entries);
        if (rc != 0) {
            return rc;
        }
    }
    rc = parent_shard_commit(t, oh_p, &w, fdb_inode_is_dir(&c->child) ? 1 : 0, c->child.ctime);
    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

/* Placement popped from the DS prealloc ring for a regular-file create
 * (same shape as the RonDB and FoundationDB backends): a captured FH
 * goes inline on the inode (MDS_IFLAG_INLINE_STRIPE); a pop without an
 * FH keeps a 1x1 stripe map with an empty entry and marks the inode
 * DS_PENDING so the late FH capture path lands the handle. */
struct create_pop {
    bool                    popped;
    struct mds_ds_map_entry entry;
    uint32_t                stripe_unit;
    uint64_t                fileid;
};

static void create_pop_prealloc(struct ds_prealloc_ctx *prealloc, enum mds_file_type type,
                                struct create_pop *pop)
{
    memset(pop, 0, sizeof(*pop));
    if (type != MDS_FTYPE_REG || prealloc == NULL) {
        return;
    }
    if (ds_prealloc_pop(prealloc, &pop->entry, &pop->stripe_unit, &pop->fileid) != 0) {
        memset(pop, 0, sizeof(*pop));
        return;
    }
    if (pop->entry.nfs_fh_len > MDS_NFS_FH_MAX) {
        pop->entry.nfs_fh_len = MDS_NFS_FH_MAX;
    }
    if (pop->stripe_unit == 0) {
        pop->stripe_unit = 65536U;
    }
    pop->popped = true;
}

static void create_apply_pop(struct create_ctx *c, const struct create_pop *pop)
{
    if (!pop->popped) {
        return;
    }
    c->child.synth_suid = pop->entry.synth_suid;
    c->child.synth_sgid = pop->entry.synth_sgid;
    if (pop->entry.nfs_fh_len > 0) {
        c->child.flags |= MDS_IFLAG_INLINE_STRIPE;
        c->child.stripe_count = 1;
        c->child.mirror_count = 1;
        c->child.stripe_unit = pop->stripe_unit;
        c->child.inline_ds_id = pop->entry.ds_id;
        c->child.inline_fh_len = pop->entry.nfs_fh_len;
        memcpy(c->child.inline_fh, pop->entry.nfs_fh, pop->entry.nfs_fh_len);
        return;
    }
    c->child.flags |= MDS_IFLAG_DS_PENDING;
    c->sc = 1;
    c->su = pop->stripe_unit;
    c->mc = 1;
    c->entries = &pop->entry;
}

static enum mds_status daos_ns_create(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                      uint64_t parent, const char *name,
                                      enum mds_file_type type, uint32_t mode, uint64_t uid,
                                      uint64_t gid, struct ds_prealloc_ctx *prealloc,
                                      struct mds_inode *out)
{
    struct daos_backend *b = be_of(cat);
    struct create_ctx c;
    struct create_pop pop;
    uint64_t fileid = 0;
    enum mds_status st;

    (void)txn;
    if (b == NULL || !name_ok(name) || out == NULL) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.parent = parent;
    c.name = name;

    /* Pop, fileid and cookie are minted ONCE per call, outside the
     * body: a retried attempt reuses them (nothing was published). */
    create_pop_prealloc(prealloc, type, &pop);
    fileid = pop.fileid;
    if (fileid == 0) {
        st = daos_backend_alloc_id(b, DAOS_META_FILEID, &fileid);
        if (st != MDS_OK) {
            return st;
        }
    }
    st = daos_backend_alloc_id(b, DAOS_META_COOKIE_SEQ, &c.seq);
    if (st != MDS_OK) {
        return st;
    }
    build_child(&c.child, fileid, parent, type, mode, uid, gid, now_ts());
    create_apply_pop(&c, &pop);

    st = daos_run_txn(b, DAOS_TXN_MUTATING, "ns_create", create_body, &c);
    if (st == MDS_OK) {
        *out = c.child;
    }
    return st;
}

static enum mds_status daos_ns_create_wide(struct mds_catalogue *cat, uint64_t parent,
                                           const char *name, const struct mds_inode *child,
                                           uint32_t stripe_count, uint32_t stripe_unit,
                                           uint32_t mirror_count,
                                           const struct mds_ds_map_entry *entries,
                                           bool *safe_to_discard)
{
    struct daos_backend *b = be_of(cat);
    struct create_ctx c;
    enum mds_status st;

    if (safe_to_discard != NULL) {
        *safe_to_discard = false;
    }
    if (b == NULL || !name_ok(name) || child == NULL || entries == NULL ||
        safe_to_discard == NULL || child->fileid == 0 || child->parent_fileid != parent ||
        child->type != MDS_FTYPE_REG || stripe_count == 0 || stripe_count > MDS_MAX_STRIPES ||
        stripe_unit == 0 || mirror_count == 0 || mirror_count > MDS_MAX_MIRRORS ||
        (child->flags & MDS_IFLAG_HPC_CREATE_PENDING) != 0) {
        if (safe_to_discard != NULL) {
            *safe_to_discard = true;
        }
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.parent = parent;
    c.name = name;
    c.child = *child;
    c.child.ds_map = NULL;
    c.check_child_absent = true;
    c.sc = stripe_count;
    c.su = stripe_unit;
    c.mc = mirror_count;
    c.entries = entries;
    st = daos_backend_alloc_id(b, DAOS_META_COOKIE_SEQ, &c.seq);
    if (st != MDS_OK) {
        *safe_to_discard = (st != MDS_ERR_INDOUBT && st != MDS_ERR_DELAY && st != MDS_ERR_IO);
        return st;
    }
    st = daos_run_txn(b, DAOS_TXN_MUTATING, "ns_create_wide", create_body, &c);
    /* Proven not published: a definitive refusal decided inside the
     * transaction.  Anything indeterminate (DELAY, IO, INDOUBT) keeps
     * the DS bundle. */
    *safe_to_discard = (st == MDS_ERR_EXISTS || st == MDS_ERR_NOTFOUND ||
                        st == MDS_ERR_NOTDIR || st == MDS_ERR_INVAL);
    return st;
}

/* -----------------------------------------------------------------------
 * ns_link
 * ----------------------------------------------------------------------- */

struct link_ctx {
    uint64_t    parent;
    const char *name;
    uint64_t    target;
    uint64_t    seq;
};

static int link_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct link_ctx *c = arg;
    struct ns_wave_out w;
    struct timespec now;
    daos_handle_t oh_p;
    daos_handle_t oh_t;
    int rc;

    rc = daos_txn_obj(t, DAOS_TAG_INODE, c->parent, &oh_p);
    if (rc != 0) {
        return rc;
    }
    rc = daos_txn_obj(t, DAOS_TAG_INODE, c->target, &oh_t);
    if (rc != 0) {
        return rc;
    }
    rc = ns_wave_run(t, oh_p, c->name, true, oh_t, &w);
    if (rc != 0) {
        return rc;
    }
    if (!w.parent_found || !w.extra_found) {
        *st_out = MDS_ERR_NOTFOUND;
        return DAOS_BODY_DONE;
    }
    if (!fdb_inode_is_dir(&w.parent)) {
        *st_out = MDS_ERR_NOTDIR;
        return DAOS_BODY_DONE;
    }
    if (fdb_inode_is_dir(&w.extra)) {
        *st_out = MDS_ERR_ISDIR;
        return DAOS_BODY_DONE;
    }
    if (w.dirent_found) {
        *st_out = MDS_ERR_EXISTS;
        return DAOS_BODY_DONE;
    }
    now = now_ts();
    rc = daos_dirent_write(t, oh_p, c->name, c->target, (uint8_t)w.extra.type, c->seq, true);
    if (rc != 0) {
        return rc;
    }
    w.extra.nlink++;
    w.extra.ctime = now;
    w.extra.change++;
    rc = daos_inode_write_blob(t, oh_t, &w.extra);
    if (rc != 0) {
        return rc;
    }
    rc = parent_shard_commit(t, oh_p, &w, 0, now);
    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

static enum mds_status daos_ns_link(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                    uint64_t parent, const char *name, uint64_t target)
{
    struct daos_backend *b = be_of(cat);
    struct link_ctx c;
    enum mds_status st;

    (void)txn;
    if (b == NULL || !name_ok(name)) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.parent = parent;
    c.name = name;
    c.target = target;
    st = daos_backend_alloc_id(b, DAOS_META_COOKIE_SEQ, &c.seq);
    if (st != MDS_OK) {
        return st;
    }
    return daos_run_txn(b, DAOS_TXN_MUTATING, "ns_link", link_body, &c);
}

/* -----------------------------------------------------------------------
 * ns_setattr / ns_nlink_adjust
 * ----------------------------------------------------------------------- */

struct setattr_ctx {
    uint64_t                fileid;
    const struct mds_inode *attrs;
    uint32_t                mask;
};

/* Same mask semantics as memdb / RonDB / FoundationDB, including the
 * grow-only SIZE_EXTEND. */
static void setattr_apply(struct mds_inode *i, const struct mds_inode *a, uint32_t mask,
                          struct timespec now)
{
    if ((mask & MDS_ATTR_MODE) != 0) {
        i->mode = a->mode;
    }
    if ((mask & MDS_ATTR_UID) != 0) {
        i->uid = a->uid;
    }
    if ((mask & MDS_ATTR_GID) != 0) {
        i->gid = a->gid;
    }
    if ((mask & MDS_ATTR_SIZE) != 0) {
        i->size = a->size;
    }
    if ((mask & MDS_ATTR_SIZE_EXTEND) != 0 && a->size > i->size) {
        i->size = a->size;
    }
    if ((mask & MDS_ATTR_ATIME) != 0) {
        i->atime = a->atime;
    }
    if ((mask & MDS_ATTR_ATIME_NOW) != 0) {
        i->atime = now;
    }
    if ((mask & MDS_ATTR_FLAGS) != 0) {
        i->flags = a->flags;
    }
    if ((mask & MDS_ATTR_MTIME) != 0) {
        i->mtime = a->mtime;
    }
    if ((mask & MDS_ATTR_MTIME_NOW) != 0) {
        i->mtime = now;
    }
    i->ctime = now;
    i->change++;
}

/* The composed read gives a directory its authoritative counters; the
 * full write puts them back as shard 0 and drops the other shards, so a
 * SETATTR of mtime is visible even when it moves the stamp backwards. */
static int setattr_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct setattr_ctx *c = arg;
    struct mds_inode ino;
    daos_handle_t oh;
    bool found = false;
    int rc;

    rc = daos_txn_obj(t, DAOS_TAG_INODE, c->fileid, &oh);
    if (rc != 0) {
        return rc;
    }
    rc = daos_inode_read_full(t, oh, &ino, &found);
    if (rc != 0) {
        return rc;
    }
    if (!found) {
        *st_out = MDS_ERR_NOTFOUND;
        return DAOS_BODY_DONE;
    }
    setattr_apply(&ino, c->attrs, c->mask, now_ts());
    if (fdb_inode_is_dir(&ino)) {
        rc = daos_inode_write_full(t, oh, &ino, false);
    } else {
        rc = daos_inode_write_blob(t, oh, &ino);
    }
    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

static enum mds_status daos_ns_setattr(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                       uint64_t fileid, const struct mds_inode *attrs,
                                       uint32_t mask)
{
    struct setattr_ctx c;

    (void)txn;
    if (be_of(cat) == NULL || attrs == NULL) {
        return MDS_ERR_INVAL;
    }
    c.fileid = fileid;
    c.attrs = attrs;
    c.mask = mask;
    return daos_run_txn(be_of(cat), DAOS_TXN_MUTATING, "ns_setattr", setattr_body, &c);
}

struct nlink_ctx {
    uint64_t fileid;
    int32_t  delta;
};

static int nlink_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct nlink_ctx *c = arg;
    struct daos_inode_req r;
    struct daos_dir_shard shard;
    struct mds_inode ino;
    daos_handle_t oh;
    uint32_t idx = daos_dir_shard_pick(t->b);
    bool found = false;
    bool present = false;
    int rc;

    rc = daos_txn_obj(t, DAOS_TAG_INODE, c->fileid, &oh);
    if (rc != 0) {
        return rc;
    }
    rc = daos_inode_req_init(&r, t->b, (int32_t)idx);
    if (rc != 0) {
        return rc;
    }
    rc = daos_inode_read_start(t, oh, &r);
    if (rc == 0) {
        rc = daos_inode_read_finish(t, &r, &ino, &found);
    }
    if (rc == 0 && found && !daos_inode_req_shard(&r, &shard, &present)) {
        rc = -DER_IO;
    }
    daos_inode_req_fini(&r);
    if (rc != 0) {
        return rc;
    }
    if (!found) {
        *st_out = MDS_ERR_NOTFOUND;
        return DAOS_BODY_DONE;
    }
    if (fdb_inode_is_dir(&ino)) {
        daos_dir_shard_apply(&shard, c->delta, 0, NULL);
        rc = daos_dir_shard_write(t, oh, idx, &shard);
    } else {
        int64_t n = (int64_t)ino.nlink + c->delta;

        ino.nlink = (n < 0) ? 0U : (uint32_t)n;
        rc = daos_inode_write_blob(t, oh, &ino);
    }
    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

static enum mds_status daos_ns_nlink_adjust(struct mds_catalogue *cat, uint64_t fileid,
                                            int32_t delta)
{
    struct nlink_ctx c;

    if (be_of(cat) == NULL) {
        return MDS_ERR_INVAL;
    }
    c.fileid = fileid;
    c.delta = delta;
    return daos_run_txn(be_of(cat), DAOS_TXN_MUTATING, "ns_nlink_adjust", nlink_body, &c);
}

/* -----------------------------------------------------------------------
 * ns_remove / ns_remove_known / ns_remove_known_gc
 * ----------------------------------------------------------------------- */

struct remove_ctx {
    uint64_t                       parent;
    const char                    *name;
    const struct mds_inode        *guard;     /**< NULL: remove by name. */
    const struct mds_ds_map_entry *gc_entries;
    const uint64_t                *gc_seqs;   /**< One minted seq per row. */
    uint32_t                       gc_count;
    uint32_t                       gc_sweep_hint;
    bool                           final;     /**< Out: the inode was deleted. */
};

/* Resolved operands of one attempt. */
struct remove_plan {
    struct ns_wave_out w;
    struct mds_inode   child;
    daos_handle_t      oh_c;
    bool               child_found;
    bool               is_dir;
    bool               final;
};

/* Wave 2: the child blob and, for a directory, its emptiness (the
 * listing waits while the fetch is in flight). */
static int remove_child_wave(struct daos_txn *t, struct remove_plan *pl, bool *nonempty)
{
    struct daos_inode_req r;
    bool started = false;
    int rc;
    int rc2;

    *nonempty = false;
    rc = daos_inode_req_init_blob(&r);
    if (rc != 0) {
        return rc;
    }
    rc = daos_inode_read_start(t, pl->oh_c, &r);
    started = (rc == 0);
    if (rc == 0 && pl->w.dv.type == (uint8_t)MDS_FTYPE_DIR) {
        rc = daos_dir_nonempty(t, pl->oh_c, nonempty);
    }
    if (started) {
        rc2 = daos_inode_read_finish(t, &r, &pl->child, &pl->child_found);
        rc = (rc == 0) ? rc2 : rc;
    }
    daos_inode_req_fini(&r);
    return rc;
}

/* Wave 1 (dirent || parent blob+shard) and wave 2 (child blob [|| child
 * emptiness]).  Decides everything; nothing is written here. */
static int remove_resolve(struct daos_txn *t, const struct remove_ctx *c, daos_handle_t oh_p,
                          struct remove_plan *pl, enum mds_status *st_out)
{
    bool nonempty = false;
    int rc;

    memset(pl, 0, sizeof(*pl));
    rc = ns_wave_run(t, oh_p, c->name, false, DAOS_HDL_INVAL, &pl->w);
    if (rc != 0) {
        return rc;
    }
    if (!pl->w.dirent_found) {
        *st_out = MDS_ERR_NOTFOUND;
        return DAOS_BODY_DONE;
    }
    if (c->guard != NULL && pl->w.dv.child_fileid != c->guard->fileid) {
        *st_out = MDS_ERR_STALE;
        return DAOS_BODY_DONE;
    }
    rc = daos_txn_obj(t, DAOS_TAG_INODE, pl->w.dv.child_fileid, &pl->oh_c);
    if (rc != 0) {
        return rc;
    }
    rc = remove_child_wave(t, pl, &nonempty);
    if (rc != 0) {
        return rc;
    }
    if (c->guard != NULL && pl->child_found && pl->child.generation != c->guard->generation) {
        *st_out = MDS_ERR_STALE;
        return DAOS_BODY_DONE;
    }
    pl->is_dir = pl->child_found ? fdb_inode_is_dir(&pl->child)
                                 : pl->w.dv.type == (uint8_t)MDS_FTYPE_DIR;
    if (pl->child_found && pl->is_dir) {
        if (nonempty) {
            *st_out = MDS_ERR_NOTEMPTY;
            return DAOS_BODY_DONE;
        }
        pl->final = true;
    } else if (pl->child_found) {
        pl->final = (pl->child.nlink <= 1U);
        /* The caller derived its plan -- GC rows, quota delta, layout
         * recall -- from the snapshot's link count.  A LINK or another
         * REMOVE that landed since makes that plan wrong for this inode;
         * refuse instead of silently doing the other shape
         * (mds_catalogue.h, ns_remove_known contract). */
        if (c->guard != NULL && (c->guard->nlink <= 1U) != pl->final) {
            *st_out = MDS_ERR_STALE;
            return DAOS_BODY_DONE;
        }
    }
    return DAOS_BODY_COMMIT;
}

/* The caller's GC rows, each with its GC_BY_OWNER marker, stamped with
 * this MDS's id (the same shape gc_enqueue writes). */
static int remove_write_gc_rows(struct daos_txn *t, const struct remove_ctx *c,
                                uint64_t child_fileid)
{
    uint32_t i;

    for (i = 0; i < c->gc_count; i++) {
        struct mds_gc_entry e;
        int rc;

        memset(&e, 0, sizeof(e));
        e.gc_seq = c->gc_seqs[i];
        e.fileid = child_fileid;
        e.ds_id = c->gc_entries[i].ds_id;
        e.owner_mds_id = t->b->mds_id;
        e.sweep_hint = c->gc_sweep_hint;
        e.nfs_fh_len = c->gc_entries[i].nfs_fh_len;
        if (e.nfs_fh_len > MDS_NFS_FH_MAX) {
            e.nfs_fh_len = MDS_NFS_FH_MAX;
        }
        memcpy(e.nfs_fh, c->gc_entries[i].nfs_fh, e.nfs_fh_len);
        rc = daos_gc_row_set(t, &e);
        if (rc != 0) {
            return rc;
        }
    }
    return 0;
}

static int remove_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct remove_ctx *c = arg;
    struct remove_plan pl;
    struct timespec now;
    daos_handle_t oh_p;
    int rc;

    c->final = false;
    rc = daos_txn_obj(t, DAOS_TAG_INODE, c->parent, &oh_p);
    if (rc != 0) {
        return rc;
    }
    rc = remove_resolve(t, c, oh_p, &pl, st_out);
    if (rc != DAOS_BODY_COMMIT) {
        return rc;
    }
    now = now_ts();
    rc = daos_dirent_clear(t, oh_p, c->name, pl.w.dv.seq);
    if (rc != 0) {
        return rc;
    }
    if (pl.child_found && pl.final) {
        /* A final unlink drops everything, the stripe map included: the
         * caller snapshotted the map before the mutation and the fused
         * variant carries the GC rows derived from it. */
        rc = daos_inode_purge(t, pl.oh_c, pl.w.dv.child_fileid, false);
        if (rc == 0) {
            rc = remove_write_gc_rows(t, c, pl.w.dv.child_fileid);
        }
    } else if (pl.child_found) {
        pl.child.nlink--;
        pl.child.ctime = now;
        pl.child.change++;
        rc = daos_inode_write_blob(t, pl.oh_c, &pl.child);
    }
    if (rc != 0) {
        return rc;
    }
    if (pl.w.parent_found) {
        rc = parent_shard_commit(t, oh_p, &pl.w, (pl.is_dir && pl.final) ? -1 : 0, now);
        if (rc != 0) {
            return rc;
        }
    }
    c->final = pl.final;
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

static enum mds_status daos_ns_remove(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                      uint64_t parent, const char *name)
{
    struct remove_ctx c;

    (void)txn;
    if (be_of(cat) == NULL || !name_ok(name)) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.parent = parent;
    c.name = name;
    return daos_run_txn(be_of(cat), DAOS_TXN_MUTATING, "ns_remove", remove_body, &c);
}

static enum mds_status daos_ns_remove_known(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                            uint64_t parent, const char *name,
                                            const struct mds_inode *child,
                                            uint32_t stripe_count)
{
    struct remove_ctx c;

    (void)txn;
    (void)stripe_count;
    if (be_of(cat) == NULL || !name_ok(name) || child == NULL) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.parent = parent;
    c.name = name;
    c.guard = child;
    return daos_run_txn(be_of(cat), DAOS_TXN_MUTATING, "ns_remove_known", remove_body, &c);
}

static enum mds_status daos_ns_remove_known_gc(struct mds_catalogue *cat,
                                               struct mds_cat_txn *txn, uint64_t parent,
                                               const char *name, const struct mds_inode *child,
                                               uint32_t stripe_count,
                                               const struct mds_ds_map_entry *gc_entries,
                                               uint32_t gc_entry_count, uint32_t gc_sweep_hint,
                                               bool *gc_folded)
{
    struct daos_backend *b = be_of(cat);
    struct remove_ctx c;
    uint64_t *seqs = NULL;
    enum mds_status st;

    (void)txn;
    (void)stripe_count;
    if (gc_folded != NULL) {
        *gc_folded = false;
    }
    if (b == NULL || !name_ok(name) || child == NULL || gc_folded == NULL ||
        (gc_entry_count > 0 && gc_entries == NULL) || gc_entry_count > DAOS_GC_ROWS_MAX) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.parent = parent;
    c.name = name;
    c.guard = child;
    c.gc_entries = gc_entries;
    c.gc_count = gc_entry_count;
    c.gc_sweep_hint = gc_sweep_hint;
    if (gc_entry_count > 0) {
        uint32_t i;

        /* GC sequences are minted once per call so a retried attempt
         * rewrites the same rows; a refused remove wastes them. */
        seqs = calloc(gc_entry_count, sizeof(*seqs));
        if (seqs == NULL) {
            return MDS_ERR_NOMEM;
        }
        for (i = 0; i < gc_entry_count; i++) {
            st = daos_backend_alloc_id(b, DAOS_META_GC_SEQ, &seqs[i]);
            if (st != MDS_OK) {
                free(seqs);
                return st;
            }
        }
        c.gc_seqs = seqs;
    }
    st = daos_run_txn(b, DAOS_TXN_MUTATING, "ns_remove_known_gc", remove_body, &c);
    if (st == MDS_OK) {
        *gc_folded = c.final && gc_entry_count > 0;
    }
    free(seqs);
    return st;
}

/* -----------------------------------------------------------------------
 * ns_rename / ns_rename_flags
 * ----------------------------------------------------------------------- */

struct rename_ctx {
    uint64_t    sp;
    const char *sn;
    uint64_t    dp;
    const char *dn;
    uint32_t    flags;
    uint64_t    seq;    /**< Cookie of the rebound destination entry. */
};

struct rename_plan {
    struct ns_wave_out s;         /**< Source parent blob+shard, source dirent. */
    struct ns_wave_out d;         /**< Destination parent blob+shard, destination dirent. */
    struct mds_inode   src_child;
    struct mds_inode   dst_child;
    daos_handle_t      oh_sp;
    daos_handle_t      oh_dp;
    daos_handle_t      oh_sc;
    daos_handle_t      oh_dc;
    bool               sc_found;
    bool               dc_found;
    bool               src_is_dir;
    bool               dst_is_dir;
};

/* Finish a started inode request into @p o, keeping the first error. */
static void wave_finish_inode(struct daos_txn *t, struct daos_inode_req *r, bool started,
                              struct mds_inode *out, bool *found, int *rc)
{
    int rc2;

    if (!started) {
        return;
    }
    rc2 = daos_inode_read_finish(t, r, out, found);
    if (*rc == 0) {
        *rc = rc2;
    }
}

static void wave_finish_dirent(struct daos_txn *t, struct daos_dirent_req *r, bool started,
                               struct fdb_dirent_val *out, bool *found, int *rc)
{
    int rc2;

    if (!started) {
        return;
    }
    rc2 = daos_dirent_read_finish(t, r, out, found);
    if (*rc == 0) {
        *rc = rc2;
    }
}

/* Wave 1: both dirents and both parents (blob + shard) in parallel. */
static int rename_wave1(struct daos_txn *t, const struct rename_ctx *c, struct rename_plan *pl)
{
    struct daos_inode_req rsp;
    struct daos_inode_req rdp;
    struct daos_dirent_req rsd;
    struct daos_dirent_req rdd;
    bool same = (c->sp == c->dp);
    bool s1 = false;
    bool s2 = false;
    bool s3 = false;
    bool s4 = false;
    int rc;

    pl->s.shard_idx = daos_dir_shard_pick(t->b);
    pl->d.shard_idx = pl->s.shard_idx;
    rdp.block = NULL;
    rc = daos_inode_req_init(&rsp, t->b, (int32_t)pl->s.shard_idx);
    if (rc != 0) {
        return rc;
    }
    if (!same) {
        rc = daos_inode_req_init(&rdp, t->b, (int32_t)pl->d.shard_idx);
        if (rc != 0) {
            daos_inode_req_fini(&rsp);
            return rc;
        }
    }
    rc = daos_inode_read_start(t, pl->oh_sp, &rsp);
    s1 = (rc == 0);
    if (rc == 0 && !same) {
        rc = daos_inode_read_start(t, pl->oh_dp, &rdp);
        s2 = (rc == 0);
    }
    if (rc == 0) {
        rc = daos_dirent_read_start(t, pl->oh_sp, c->sn, &rsd);
        s3 = (rc == 0);
    }
    if (rc == 0) {
        rc = daos_dirent_read_start(t, pl->oh_dp, c->dn, &rdd);
        s4 = (rc == 0);
    }
    wave_finish_inode(t, &rsp, s1, &pl->s.parent, &pl->s.parent_found, &rc);
    wave_finish_inode(t, &rdp, s2, &pl->d.parent, &pl->d.parent_found, &rc);
    wave_finish_dirent(t, &rsd, s3, &pl->s.dv, &pl->s.dirent_found, &rc);
    wave_finish_dirent(t, &rdd, s4, &pl->d.dv, &pl->d.dirent_found, &rc);
    if (rc == 0 && !daos_inode_req_shard(&rsp, &pl->s.shard, &pl->s.shard_present)) {
        rc = -DER_IO;
    }
    if (rc == 0 && !same && !daos_inode_req_shard(&rdp, &pl->d.shard, &pl->d.shard_present)) {
        rc = -DER_IO;
    }
    daos_inode_req_fini(&rsp);
    daos_inode_req_fini(&rdp);
    if (rc == 0 && same) {
        pl->d.parent = pl->s.parent;
        pl->d.parent_found = pl->s.parent_found;
        pl->d.shard = pl->s.shard;
        pl->d.shard_present = pl->s.shard_present;
    }
    return rc;
}

/* A parent that exists but is not a directory is NOTDIR (RFC 8881
 * 18.26.4: the saved or current filehandle is not a directory); only a
 * missing parent or a missing source name is NOTFOUND.  A non-directory
 * has no dirents, so the type has to be decided before the absent
 * source name is, or the answer would be NOENT. */
static int rename_check1(const struct rename_plan *pl, enum mds_status *st_out)
{
    if ((pl->s.parent_found && !fdb_inode_is_dir(&pl->s.parent)) ||
        (pl->d.parent_found && !fdb_inode_is_dir(&pl->d.parent))) {
        *st_out = MDS_ERR_NOTDIR;
        return DAOS_BODY_DONE;
    }
    if (!pl->s.dirent_found || !pl->d.parent_found) {
        *st_out = MDS_ERR_NOTFOUND;
        return DAOS_BODY_DONE;
    }
    return DAOS_BODY_COMMIT;
}

/* Wave 2: the source child, the victim and (directory victim) its
 * emptiness, in parallel. */
static int rename_wave2(struct daos_txn *t, struct rename_plan *pl, bool *nonempty)
{
    struct daos_inode_req rs;
    struct daos_inode_req rd;
    bool s1 = false;
    bool s2 = false;
    int rc;

    *nonempty = false;
    rd.block = NULL;
    rc = daos_inode_req_init_blob(&rs);
    if (rc != 0) {
        return rc;
    }
    if (pl->d.dirent_found) {
        rc = daos_inode_req_init_blob(&rd);
        if (rc != 0) {
            daos_inode_req_fini(&rs);
            return rc;
        }
    }
    rc = daos_inode_read_start(t, pl->oh_sc, &rs);
    s1 = (rc == 0);
    if (rc == 0 && pl->d.dirent_found) {
        rc = daos_inode_read_start(t, pl->oh_dc, &rd);
        s2 = (rc == 0);
    }
    if (rc == 0 && pl->d.dirent_found && pl->d.dv.type == (uint8_t)MDS_FTYPE_DIR) {
        rc = daos_dir_nonempty(t, pl->oh_dc, nonempty);
    }
    wave_finish_inode(t, &rs, s1, &pl->src_child, &pl->sc_found, &rc);
    wave_finish_inode(t, &rd, s2, &pl->dst_child, &pl->dc_found, &rc);
    daos_inode_req_fini(&rs);
    daos_inode_req_fini(&rd);
    return rc;
}

/* The POSIX type rules between the source and an existing destination. */
static int rename_check2(struct rename_plan *pl, bool nonempty, enum mds_status *st_out)
{
    pl->src_is_dir = pl->sc_found ? fdb_inode_is_dir(&pl->src_child)
                                  : pl->s.dv.type == (uint8_t)MDS_FTYPE_DIR;
    if (!pl->d.dirent_found) {
        return DAOS_BODY_COMMIT;
    }
    pl->dst_is_dir = pl->dc_found ? fdb_inode_is_dir(&pl->dst_child)
                                  : pl->d.dv.type == (uint8_t)MDS_FTYPE_DIR;
    if (pl->dst_is_dir) {
        if (!pl->src_is_dir) {
            *st_out = MDS_ERR_ISDIR;
            return DAOS_BODY_DONE;
        }
        if (nonempty) {
            *st_out = MDS_ERR_NOTEMPTY;
            return DAOS_BODY_DONE;
        }
    } else if (pl->src_is_dir) {
        *st_out = MDS_ERR_NOTDIR;
        return DAOS_BODY_DONE;
    }
    return DAOS_BODY_COMMIT;
}

/* Overwritten destination inode: a directory victim is deleted and the
 * destination parent loses its ".." link (applied to @p dp_shard); a
 * file victim loses a link and on its last one is deleted or kept as an
 * UNLINK_ORPHAN row.
 *
 * A deleted victim keeps its stripe map (and STRIPE_IDX marker): the
 * caller reads the map AFTER the rename committed to queue the DS
 * objects for GC and then drops the rows itself, and the orphan
 * finaliser does the same for a kept orphan.  Only the REMOVE paths
 * purge the stripes (daos_inode_purge). */
static int rename_drop_victim(struct daos_txn *t, const struct rename_ctx *c,
                              struct rename_plan *pl, struct daos_dir_shard *dp_shard,
                              struct timespec now)
{
    struct mds_inode *v = &pl->dst_child;

    if (!pl->dc_found) {
        return 0;
    }
    if (pl->dst_is_dir) {
        daos_dir_shard_apply(dp_shard, -1, 0, NULL);
        return daos_inode_purge(t, pl->oh_dc, v->fileid, true);
    }
    if (v->nlink > 0) {
        v->nlink--;
    }
    if (v->nlink > 0 ||
        (v->type == MDS_FTYPE_REG && (c->flags & MDS_CAT_RNF_KEEP_DST_ORPHAN) != 0U)) {
        if (v->nlink == 0) {
            v->flags |= MDS_IFLAG_UNLINK_ORPHAN;
        }
        v->ctime = now;
        v->change++;
        return daos_inode_write_blob(t, pl->oh_dc, v);
    }
    return daos_inode_purge(t, pl->oh_dc, v->fileid, true);
}

/* Every check passed: the writes. */
static int rename_mutate(struct daos_txn *t, const struct rename_ctx *c, struct rename_plan *pl)
{
    struct timespec now = now_ts();
    bool same = (c->sp == c->dp);
    struct daos_dir_shard *sps = &pl->s.shard;
    struct daos_dir_shard *dps = same ? &pl->s.shard : &pl->d.shard;
    int rc;

    rc = daos_dirent_clear(t, pl->oh_sp, c->sn, pl->s.dv.seq);
    if (rc == 0 && pl->d.dirent_found) {
        rc = daos_dirent_seq_clear(t, pl->oh_dp, pl->d.dv.seq);
        if (rc == 0) {
            rc = rename_drop_victim(t, c, pl, dps, now);
        }
    }
    if (rc == 0) {
        rc = daos_dirent_write(t, pl->oh_dp, c->dn, pl->s.dv.child_fileid, pl->s.dv.type,
                               c->seq, false);
    }
    if (rc != 0) {
        return rc;
    }
    if (!same) {
        if (pl->src_is_dir) {
            if (pl->s.parent_found) {
                daos_dir_shard_apply(sps, -1, 0, NULL);
            }
            daos_dir_shard_apply(dps, 1, 0, NULL);
        }
        if (pl->sc_found) {
            pl->src_child.parent_fileid = c->dp;
            rc = daos_inode_write_blob(t, pl->oh_sc, &pl->src_child);
            if (rc != 0) {
                return rc;
            }
        }
        if (pl->s.parent_found) {
            daos_dir_shard_apply(sps, 0, 1, &now);
            rc = daos_dir_shard_write(t, pl->oh_sp, pl->s.shard_idx, sps);
            if (rc != 0) {
                return rc;
            }
        }
    }
    daos_dir_shard_apply(dps, 0, 1, &now);
    return daos_dir_shard_write(t, pl->oh_dp, pl->d.shard_idx, dps);
}

static int rename_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct rename_ctx *c = arg;
    struct rename_plan pl;
    bool nonempty = false;
    int rc;

    memset(&pl, 0, sizeof(pl));
    rc = daos_txn_obj(t, DAOS_TAG_INODE, c->sp, &pl.oh_sp);
    if (rc == 0) {
        rc = daos_txn_obj(t, DAOS_TAG_INODE, c->dp, &pl.oh_dp);
    }
    if (rc == 0) {
        rc = rename_wave1(t, c, &pl);
    }
    if (rc != 0) {
        return rc;
    }
    rc = rename_check1(&pl, st_out);
    if (rc != DAOS_BODY_COMMIT) {
        return rc;
    }
    if (pl.d.dirent_found && pl.d.dv.child_fileid == pl.s.dv.child_fileid) {
        *st_out = MDS_OK; /* both names already resolve to the inode: POSIX no-op */
        return DAOS_BODY_DONE;
    }
    rc = daos_txn_obj(t, DAOS_TAG_INODE, pl.s.dv.child_fileid, &pl.oh_sc);
    if (rc == 0 && pl.d.dirent_found) {
        rc = daos_txn_obj(t, DAOS_TAG_INODE, pl.d.dv.child_fileid, &pl.oh_dc);
    }
    if (rc == 0) {
        rc = rename_wave2(t, &pl, &nonempty);
    }
    if (rc != 0) {
        return rc;
    }
    rc = rename_check2(&pl, nonempty, st_out);
    if (rc != DAOS_BODY_COMMIT) {
        return rc;
    }
    rc = rename_mutate(t, c, &pl);
    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

static enum mds_status daos_ns_rename_flags(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                            uint64_t src_parent, const char *src_name,
                                            uint64_t dst_parent, const char *dst_name,
                                            uint32_t ns_flags)
{
    struct daos_backend *b = be_of(cat);
    struct rename_ctx c;
    enum mds_status st;

    (void)txn;
    if (b == NULL || !name_ok(src_name) || !name_ok(dst_name)) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.sp = src_parent;
    c.sn = src_name;
    c.dp = dst_parent;
    c.dn = dst_name;
    c.flags = ns_flags;
    st = daos_backend_alloc_id(b, DAOS_META_COOKIE_SEQ, &c.seq);
    if (st != MDS_OK) {
        return st;
    }
    return daos_run_txn(b, DAOS_TXN_MUTATING, "ns_rename", rename_body, &c);
}

static enum mds_status daos_ns_rename(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                      uint64_t src_parent, const char *src_name,
                                      uint64_t dst_parent, const char *dst_name)
{
    return daos_ns_rename_flags(cat, txn, src_parent, src_name, dst_parent, dst_name, 0U);
}

/* -----------------------------------------------------------------------
 * READDIR (hash order of the 'N' akeys), dirent_name_for_child
 *
 * One read-only transaction per page: the 'N' akeys are listed from the
 * cursor, their values fetched in ONE RPC, the page materialised into a
 * caller-owned buffer, the transaction ends, then the entries are
 * delivered (C1/C2).  The cursor is the last name of the page.
 * ----------------------------------------------------------------------- */

struct rd_page_ctx {
    uint64_t               parent;
    const char            *after;   /**< NULL: from the start. */
    uint32_t               limit;
    struct mds_cat_dirent *page;    /**< DAOS_READDIR_PAGE entries. */
    daos_key_desc_t       *kds;
    uint8_t               *keybuf;  /**< DAOS_READDIR_PAGE * DAOS_KEY_MAX. */
    daos_iod_t            *iods;
    d_sg_list_t           *sgls;
    d_iov_t               *iovs;
    uint8_t               *vals;    /**< DAOS_READDIR_PAGE * FDB_DIRENT_ENC_SIZE. */
    uint32_t               n;
    bool                   more;
};

static void rd_page_free(struct rd_page_ctx *c)
{
    free(c->page);
    free(c->kds);
    free(c->keybuf);
    free(c->iods);
    free(c->sgls);
    free(c->iovs);
    free(c->vals);
}

static enum mds_status rd_page_alloc(struct rd_page_ctx *c)
{
    c->page = calloc(DAOS_READDIR_PAGE, sizeof(*c->page));
    c->kds = calloc(DAOS_READDIR_PAGE, sizeof(*c->kds));
    c->keybuf = calloc(DAOS_READDIR_PAGE, DAOS_KEY_MAX);
    c->iods = calloc(DAOS_READDIR_PAGE, sizeof(*c->iods));
    c->sgls = calloc(DAOS_READDIR_PAGE, sizeof(*c->sgls));
    c->iovs = calloc(DAOS_READDIR_PAGE, sizeof(*c->iovs));
    c->vals = calloc(DAOS_READDIR_PAGE, FDB_DIRENT_ENC_SIZE);
    if (c->page == NULL || c->kds == NULL || c->keybuf == NULL || c->iods == NULL ||
        c->sgls == NULL || c->iovs == NULL || c->vals == NULL) {
        rd_page_free(c);
        return MDS_ERR_NOMEM;
    }
    return MDS_OK;
}

/* Decode the fetched values into the page; the names come from the keys. */
static int rd_page_decode(struct rd_page_ctx *c, const struct daos_list *l,
                          const struct daos_fetch *f)
{
    size_t off = 0;
    uint32_t i;

    for (i = 0; i < l->nr; i++) {
        struct mds_cat_dirent *d = &c->page[c->n];
        struct fdb_dirent_val dv;
        size_t klen = (size_t)l->kds[i].kd_key_len;
        size_t vlen = daos_fetch_size(f, i);

        if (klen == 0 || klen > MDS_MAX_NAME || vlen == 0 ||
            !fdb_dirent_decode(c->vals + (size_t)i * FDB_DIRENT_ENC_SIZE, vlen, &dv)) {
            return -DER_IO; /* corrupt row */
        }
        memset(d, 0, sizeof(*d));
        d->fileid = dv.child_fileid;
        d->cookie = dv.seq;
        d->type = dv.type;
        memcpy(d->name, l->buf + off, klen);
        d->name[klen] = '\0';
        off += klen;
        c->n++;
    }
    return 0;
}

static int rd_page_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct rd_page_ctx *c = arg;
    struct daos_list l;
    struct daos_fetch f;
    struct daos_key dk;
    struct daos_key after;
    daos_handle_t oh;
    size_t off = 0;
    uint32_t i;
    int rc;

    c->n = 0;
    c->more = false;
    rc = daos_txn_obj(t, DAOS_TAG_INODE, c->parent, &oh);
    if (rc != 0) {
        return rc;
    }
    daos_dkey_inode(&dk, DAOS_INODE_DKEY_DIRENTS);
    daos_list_init(&l, c->kds, c->limit, c->keybuf, (size_t)DAOS_READDIR_PAGE * DAOS_KEY_MAX);
    if (c->after != NULL) {
        daos_akey_name(&after, c->after);
    }
    rc = daos_list_akeys(t, oh, &dk, c->after != NULL ? &after : NULL, &l);
    if (rc != 0) {
        return rc;
    }
    c->more = !l.eof;
    if (l.nr == 0) {
        *st_out = MDS_OK;
        return DAOS_BODY_COMMIT;
    }
    /* One fetch of every listed akey: the keys are referenced in place. */
    daos_fetch_init(&f, &dk, c->iods, c->sgls, c->iovs, c->limit);
    for (i = 0; i < l.nr; i++) {
        size_t klen = (size_t)l.kds[i].kd_key_len;

        if (!daos_fetch_add_raw(&f, l.buf + off, klen, c->vals + (size_t)i * FDB_DIRENT_ENC_SIZE,
                                FDB_DIRENT_ENC_SIZE)) {
            return -DER_IO;
        }
        off += klen;
    }
    rc = daos_fetch_run(t, oh, &f);
    if (rc == -DER_REC2BIG) {
        return -DER_IO;
    }
    if (rc != 0) {
        return rc;
    }
    rc = rd_page_decode(c, &l, &f);
    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

static enum mds_status daos_ns_readdir(struct mds_catalogue *cat, uint64_t parent,
                                       const char *start_after, uint32_t max_entries,
                                       struct mds_cat_txn *txn, mds_readdir_cb cb, void *ctx)
{
    struct rd_page_ctx c;
    char cursor[MDS_MAX_NAME + 1];
    uint32_t delivered = 0;
    enum mds_status st;

    (void)txn;
    if (be_of(cat) == NULL || cb == NULL ||
        (start_after != NULL && strnlen(start_after, MDS_MAX_NAME + 1U) > MDS_MAX_NAME)) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.parent = parent;
    c.after = (start_after != NULL && start_after[0] != '\0') ? start_after : NULL;
    st = rd_page_alloc(&c);
    if (st != MDS_OK) {
        return st;
    }
    for (;;) {
        uint32_t i;

        c.limit = DAOS_READDIR_PAGE;
        if (max_entries > 0 && max_entries - delivered < c.limit) {
            c.limit = max_entries - delivered;
        }
        st = daos_run_txn(be_of(cat), DAOS_TXN_READONLY, "ns_readdir", rd_page_body, &c);
        if (st != MDS_OK) {
            break;
        }
        for (i = 0; i < c.n; i++) {
            if (cb(&c.page[i], ctx) != 0) {
                goto out;
            }
            delivered++;
        }
        if (!c.more || (max_entries > 0 && delivered >= max_entries) || c.n == 0) {
            break;
        }
        (void)snprintf(cursor, sizeof(cursor), "%s", c.page[c.n - 1].name);
        c.after = cursor;
    }
out:
    rd_page_free(&c);
    return st;
}

static enum mds_status daos_dirent_name_for_child(struct mds_catalogue *cat, uint64_t parent,
                                                  uint64_t child_fileid, char *name_out,
                                                  size_t name_out_len)
{
    struct rd_page_ctx c;
    char cursor[MDS_MAX_NAME + 1];
    enum mds_status st = MDS_ERR_NOTFOUND;

    if (be_of(cat) == NULL || name_out == NULL || name_out_len == 0) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.parent = parent;
    if (rd_page_alloc(&c) != MDS_OK) {
        return MDS_ERR_NOMEM;
    }
    for (;;) {
        uint32_t i;
        enum mds_status pst;

        c.limit = DAOS_READDIR_PAGE;
        pst = daos_run_txn(be_of(cat), DAOS_TXN_READONLY, "dirent_name_for_child",
                           rd_page_body, &c);
        if (pst != MDS_OK) {
            st = pst;
            break;
        }
        for (i = 0; i < c.n; i++) {
            if (c.page[i].fileid == child_fileid) {
                (void)snprintf(name_out, name_out_len, "%s", c.page[i].name);
                st = MDS_OK;
                goto out;
            }
        }
        if (!c.more || c.n == 0) {
            break;
        }
        (void)snprintf(cursor, sizeof(cursor), "%s", c.page[c.n - 1].name);
        c.after = cursor;
    }
out:
    rd_page_free(&c);
    return st;
}

/* -----------------------------------------------------------------------
 * READDIR_PLUS resumed by cookie ('S'+be64 order)
 *
 * Per page: one listing of the cookie-ordered dkeys from the resume
 * key, one parallel wave fetching the 'S' rows, one parallel wave
 * fetching the child inodes, all in one read-only transaction; the page
 * is delivered after the transaction ended.
 * ----------------------------------------------------------------------- */

struct rdp_entry {
    struct mds_cat_dirent   d;
    struct mds_inode        ino;
    uint64_t                seq;
    struct daos_fetch_small sf;       /**< The 'S' row fetch. */
    struct daos_key         sv_ak;
    uint8_t                 sv_buf[FDB_DIRENT_SEQ_ENC_MAX];
    struct daos_inode_req   ireq;     /**< The child inode fetch (block kept per slot call). */
    daos_handle_t           oh;       /**< Untracked child handle, closed per page. */
    bool                    ino_valid;
    bool                    sf_started;
    bool                    ireq_started;
};

struct rdp_page_ctx {
    uint64_t          parent;
    uint64_t          after;   /**< Cookie strictly before the page. */
    uint32_t          limit;
    struct rdp_entry *page;
    daos_key_desc_t  *kds;
    uint8_t          *keybuf;
    uint32_t          n;
    bool              more;
};

static void rdp_page_free(struct rdp_page_ctx *c)
{
    uint32_t i;

    if (c->page != NULL) {
        for (i = 0; i < DAOS_READDIR_PLUS_PAGE; i++) {
            daos_inode_req_fini(&c->page[i].ireq);
        }
    }
    free(c->page);
    free(c->kds);
    free(c->keybuf);
}

static enum mds_status rdp_page_alloc(struct rdp_page_ctx *c, const struct daos_backend *b)
{
    uint32_t i;

    c->page = calloc(DAOS_READDIR_PLUS_PAGE, sizeof(*c->page));
    c->kds = calloc(DAOS_READDIR_PLUS_PAGE, sizeof(*c->kds));
    c->keybuf = calloc(DAOS_READDIR_PLUS_PAGE, DAOS_KEY_MAX);
    if (c->page == NULL || c->kds == NULL || c->keybuf == NULL) {
        rdp_page_free(c);
        return MDS_ERR_NOMEM;
    }
    for (i = 0; i < DAOS_READDIR_PLUS_PAGE; i++) {
        if (daos_inode_req_init(&c->page[i].ireq, b, DAOS_INODE_ALL_SHARDS) != 0) {
            rdp_page_free(c);
            return MDS_ERR_NOMEM;
        }
    }
    return MDS_OK;
}

/* List the 'S' dkeys after the cursor and keep their cookies.  The
 * listing is sorted, so the first key that is not an 'S'+be64 key ends
 * the directory's cookie range. */
static int rdp_collect(struct daos_txn *t, daos_handle_t oh, struct rdp_page_ctx *c)
{
    struct daos_list l;
    struct daos_key resume;
    uint32_t i;
    int rc;

    if (c->after == 0) {
        daos_key_init(&resume);
        daos_key_u8(&resume, DAOS_INODE_DKEY_DIRENT_SEQ_PREFIX);
    } else {
        daos_dkey_inode_dirent_seq(&resume, c->after);
    }
    daos_list_init(&l, c->kds, c->limit, c->keybuf, (size_t)DAOS_READDIR_PLUS_PAGE * DAOS_KEY_MAX);
    rc = daos_list_dkeys(t, oh, &resume, &l);
    if (rc != 0) {
        return rc;
    }
    for (i = 0; i < l.nr; i++) {
        const uint8_t *key;
        size_t klen;
        uint64_t seq;

        (void)daos_list_key(&l, i, &key, &klen);
        if (!daos_dkey_inode_dirent_seq_parse(key, klen, &seq)) {
            break;
        }
        c->page[c->n].seq = seq;
        c->n++;
    }
    c->more = (i == l.nr) && !l.eof;
    return 0;
}

/* Wave 1: the 'S' rows of the collected cookies. */
static int rdp_wave_rows(struct daos_txn *t, daos_handle_t oh, struct rdp_page_ctx *c)
{
    uint32_t i;
    int rc = 0;

    for (i = 0; i < c->n && rc == 0; i++) {
        struct rdp_entry *e = &c->page[i];
        struct daos_key dk;

        daos_dkey_inode_dirent_seq(&dk, e->seq);
        daos_fetch_small_init(&e->sf, &dk);
        daos_akey_value(&e->sv_ak);
        if (!daos_fetch_add(&e->sf.f, &e->sv_ak, e->sv_buf, sizeof(e->sv_buf))) {
            rc = -DER_INVAL;
            break;
        }
        rc = daos_fetch_start(t, oh, &e->sf.f);
        e->sf_started = (rc == 0);
    }
    for (i = 0; i < c->n; i++) {
        struct rdp_entry *e = &c->page[i];
        int rc2;

        if (!e->sf_started) {
            continue;
        }
        rc2 = daos_fetch_finish(t, &e->sf.f);
        e->sf_started = false;
        if (rc == 0) {
            rc = (rc2 == -DER_REC2BIG) ? -DER_IO : rc2;
        }
    }
    if (rc != 0) {
        return rc;
    }
    for (i = 0; i < c->n; i++) {
        struct rdp_entry *e = &c->page[i];
        struct fdb_dirent_seq_val sv;
        size_t len = daos_fetch_size(&e->sf.f, 0);

        if (len == 0 || !fdb_dirent_seq_decode(e->sv_buf, len, &sv)) {
            return -DER_IO; /* a listed cookie without its row: corrupt */
        }
        memset(&e->d, 0, sizeof(e->d));
        e->d.fileid = sv.child_fileid;
        e->d.cookie = e->seq;
        e->d.type = sv.type;
        (void)snprintf(e->d.name, sizeof(e->d.name), "%s", sv.name);
    }
    return 0;
}

/* Wave 2: the child inodes (blob + K shards each), composed. */
static int rdp_wave_inodes(struct daos_txn *t, struct rdp_page_ctx *c)
{
    uint32_t i;
    int rc = 0;

    for (i = 0; i < c->n && rc == 0; i++) {
        struct rdp_entry *e = &c->page[i];

        e->ino_valid = false;
        rc = daos_obj_open_tag(t->b, DAOS_TAG_INODE, e->d.fileid, &e->oh);
        if (rc != 0) {
            break;
        }
        daos_inode_req_rearm(&e->ireq);
        rc = daos_inode_read_start(t, e->oh, &e->ireq);
        e->ireq_started = (rc == 0);
    }
    for (i = 0; i < c->n; i++) {
        struct rdp_entry *e = &c->page[i];
        int rc2;

        if (e->ireq_started) {
            rc2 = daos_inode_read_finish(t, &e->ireq, &e->ino, &e->ino_valid);
            e->ireq_started = false;
            if (rc == 0) {
                rc = rc2;
            }
        }
        daos_obj_close_h(&e->oh);
    }
    return rc;
}

static int rdp_page_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct rdp_page_ctx *c = arg;
    daos_handle_t oh;
    int rc;

    c->n = 0;
    c->more = false;
    rc = daos_txn_obj(t, DAOS_TAG_INODE, c->parent, &oh);
    if (rc != 0) {
        return rc;
    }
    rc = rdp_collect(t, oh, c);
    if (rc == 0 && c->n > 0) {
        rc = rdp_wave_rows(t, oh, c);
    }
    if (rc == 0 && c->n > 0) {
        rc = rdp_wave_inodes(t, c);
    }
    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

static enum mds_status daos_ns_readdir_plus_from(struct mds_catalogue *cat, uint64_t parent,
                                                 uint64_t start_after_cookie,
                                                 uint32_t max_entries, struct mds_cat_txn *txn,
                                                 mds_readdir_plus_cb cb, void *ctx)
{
    struct daos_backend *b = be_of(cat);
    struct rdp_page_ctx c;
    uint32_t delivered = 0;
    enum mds_status st;

    (void)txn;
    if (b == NULL || cb == NULL) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.parent = parent;
    c.after = start_after_cookie;
    st = rdp_page_alloc(&c, b);
    if (st != MDS_OK) {
        return st;
    }
    for (;;) {
        uint32_t i;

        c.limit = DAOS_READDIR_PLUS_PAGE;
        if (max_entries > 0 && max_entries - delivered < c.limit) {
            c.limit = max_entries - delivered;
        }
        st = daos_run_txn(b, DAOS_TXN_READONLY, "ns_readdir_plus_from", rdp_page_body, &c);
        if (st != MDS_OK) {
            break;
        }
        for (i = 0; i < c.n; i++) {
            const struct rdp_entry *e = &c.page[i];

            if (cb(&e->d, e->ino_valid ? &e->ino : NULL, e->ino_valid, ctx) != 0) {
                goto out;
            }
            delivered++;
        }
        if (!c.more || (max_entries > 0 && delivered >= max_entries) || c.n == 0) {
            break;
        }
        c.after = c.page[c.n - 1].d.cookie;
    }
out:
    rdp_page_free(&c);
    return st;
}

/* -----------------------------------------------------------------------
 * Raw dirent rows: dirent_put / dirent_insert / dirent_del
 * ----------------------------------------------------------------------- */

enum dirent_op {
    DIRENT_OP_PUT,
    DIRENT_OP_INSERT,
    DIRENT_OP_DEL,
};

struct dirent_ctx {
    enum dirent_op op;
    uint64_t       parent;
    const char    *name;
    uint64_t       child;
    uint8_t        type;
    uint64_t       seq;
};

static int dirent_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct dirent_ctx *c = arg;
    struct fdb_dirent_val old;
    daos_handle_t oh;
    bool found = false;
    int rc;

    rc = daos_txn_obj(t, DAOS_TAG_INODE, c->parent, &oh);
    if (rc != 0) {
        return rc;
    }
    rc = daos_dirent_read(t, oh, c->name, &old, &found);
    if (rc != 0) {
        return rc;
    }
    if (c->op == DIRENT_OP_DEL) {
        if (!found) {
            *st_out = MDS_ERR_NOTFOUND;
            return DAOS_BODY_DONE;
        }
        rc = daos_dirent_clear(t, oh, c->name, old.seq);
        if (rc != 0) {
            return rc;
        }
        *st_out = MDS_OK;
        return DAOS_BODY_COMMIT;
    }
    if (found && c->op == DIRENT_OP_INSERT) {
        *st_out = MDS_ERR_EXISTS;
        return DAOS_BODY_DONE;
    }
    if (found) {
        /* Overwrite is a rebinding: the old cookie row goes. */
        rc = daos_dirent_seq_clear(t, oh, old.seq);
        if (rc != 0) {
            return rc;
        }
    }
    rc = daos_dirent_write(t, oh, c->name, c->child, c->type, c->seq, c->op == DIRENT_OP_INSERT);
    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

static enum mds_status dirent_run(const struct mds_catalogue *cat, enum dirent_op op,
                                  uint64_t parent, const char *name, uint64_t child,
                                  uint8_t type)
{
    struct daos_backend *b = be_of(cat);
    struct dirent_ctx c;

    if (b == NULL || !name_ok(name)) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.op = op;
    c.parent = parent;
    c.name = name;
    c.child = child;
    c.type = type;
    if (op != DIRENT_OP_DEL) {
        enum mds_status st;

        if (type < (uint8_t)MDS_FTYPE_REG || type > (uint8_t)MDS_FTYPE_SOCK) {
            return MDS_ERR_INVAL;
        }
        st = daos_backend_alloc_id(b, DAOS_META_COOKIE_SEQ, &c.seq);
        if (st != MDS_OK) {
            return st;
        }
    }
    return daos_run_txn(b, DAOS_TXN_MUTATING, "dirent_op", dirent_body, &c);
}

static enum mds_status daos_dirent_put(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                       uint64_t parent, const char *name, uint64_t child_fileid,
                                       uint8_t child_type)
{
    (void)txn;
    return dirent_run(cat, DIRENT_OP_PUT, parent, name, child_fileid, child_type);
}

static enum mds_status daos_dirent_insert(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                          uint64_t parent, const char *name,
                                          uint64_t child_fileid, uint8_t child_type)
{
    (void)txn;
    return dirent_run(cat, DIRENT_OP_INSERT, parent, name, child_fileid, child_type);
}

static enum mds_status daos_dirent_del(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                       uint64_t parent, const char *name)
{
    (void)txn;
    return dirent_run(cat, DIRENT_OP_DEL, parent, name, 0, 0);
}

/* -----------------------------------------------------------------------
 * Raw inode rows: inode_put / inode_del
 * ----------------------------------------------------------------------- */

struct inode_ctx {
    const struct mds_inode *inode;
    uint64_t                fileid;
};

static int inode_put_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct inode_ctx *c = arg;
    daos_handle_t oh;
    int rc;

    rc = daos_txn_obj(t, DAOS_TAG_INODE, c->fileid, &oh);
    if (rc != 0) {
        return rc;
    }
    rc = daos_inode_write_full(t, oh, c->inode, false);
    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

static enum mds_status daos_inode_put(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                      const struct mds_inode *inode)
{
    struct inode_ctx c;

    (void)txn;
    if (be_of(cat) == NULL || inode == NULL) {
        return MDS_ERR_INVAL;
    }
    c.inode = inode;
    c.fileid = inode->fileid;
    return daos_run_txn(be_of(cat), DAOS_TXN_MUTATING, "inode_put", inode_put_body, &c);
}

static int inode_del_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct inode_ctx *c = arg;
    struct mds_inode ino;
    struct daos_key dk;
    daos_handle_t oh;
    bool found = false;
    int rc;

    rc = daos_txn_obj(t, DAOS_TAG_INODE, c->fileid, &oh);
    if (rc != 0) {
        return rc;
    }
    rc = daos_inode_read_blob(t, oh, &ino, &found);
    if (rc != 0) {
        return rc;
    }
    if (!found) {
        *st_out = MDS_ERR_NOTFOUND;
        return DAOS_BODY_DONE;
    }
    daos_dkey_inode(&dk, DAOS_INODE_DKEY_ATTRS);
    rc = daos_punch_dkeys(t, oh, &dk, 1, 0);
    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

static enum mds_status daos_inode_del(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                      uint64_t fileid)
{
    struct inode_ctx c;

    (void)txn;
    if (be_of(cat) == NULL) {
        return MDS_ERR_INVAL;
    }
    c.inode = NULL;
    c.fileid = fileid;
    return daos_run_txn(be_of(cat), DAOS_TXN_MUTATING, "inode_del", inode_del_body, &c);
}

/* -----------------------------------------------------------------------
 * alloc_fileid / ns_parent_touch
 * ----------------------------------------------------------------------- */

static enum mds_status daos_alloc_fileid(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                         uint64_t *fileid)
{
    (void)txn;
    if (be_of(cat) == NULL || fileid == NULL) {
        return MDS_ERR_INVAL;
    }
    return daos_backend_alloc_id(be_of(cat), DAOS_META_FILEID, fileid);
}

struct touch_ctx {
    uint64_t        fileid;
    uint64_t        delta;
    struct timespec stamp;
};

/* The blob read keeps a blind shard write from materialising counters
 * for a directory that no longer exists; a missing row is MDS_OK. */
static int touch_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct touch_ctx *c = arg;
    struct daos_inode_req r;
    struct daos_dir_shard shard;
    struct mds_inode ino;
    daos_handle_t oh;
    uint32_t idx = daos_dir_shard_pick(t->b);
    bool found = false;
    bool present = false;
    int rc;

    rc = daos_txn_obj(t, DAOS_TAG_INODE, c->fileid, &oh);
    if (rc != 0) {
        return rc;
    }
    rc = daos_inode_req_init(&r, t->b, (int32_t)idx);
    if (rc != 0) {
        return rc;
    }
    rc = daos_inode_read_start(t, oh, &r);
    if (rc == 0) {
        rc = daos_inode_read_finish(t, &r, &ino, &found);
    }
    if (rc == 0 && found && !daos_inode_req_shard(&r, &shard, &present)) {
        rc = -DER_IO;
    }
    daos_inode_req_fini(&r);
    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    if (!found || !fdb_inode_is_dir(&ino)) {
        return DAOS_BODY_DONE;
    }
    daos_dir_shard_apply(&shard, 0, c->delta, &c->stamp);
    rc = daos_dir_shard_write(t, oh, idx, &shard);
    if (rc != 0) {
        return rc;
    }
    return DAOS_BODY_COMMIT;
}

static enum mds_status daos_ns_parent_touch(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                            uint64_t fileid, uint64_t change_delta,
                                            struct timespec stamp)
{
    struct touch_ctx c;

    (void)txn;
    if (be_of(cat) == NULL || change_delta == 0 || change_delta > (uint64_t)INT64_MAX) {
        return MDS_ERR_INVAL;
    }
    c.fileid = fileid;
    c.delta = change_delta;
    c.stamp = stamp;
    return daos_run_txn(be_of(cat), DAOS_TXN_MUTATING, "ns_parent_touch", touch_body, &c);
}

/* -----------------------------------------------------------------------
 * Registration
 * ----------------------------------------------------------------------- */

void catalogue_daos_ns_register(struct mds_authority_ops *ops)
{
    if (ops == NULL) {
        return;
    }
    ops->ns_create             = daos_ns_create;
    ops->ns_create_wide        = daos_ns_create_wide;
    ops->ns_remove             = daos_ns_remove;
    ops->ns_remove_known       = daos_ns_remove_known;
    ops->ns_remove_known_gc    = daos_ns_remove_known_gc;
    ops->ns_parent_touch       = daos_ns_parent_touch;
    ops->ns_rename             = daos_ns_rename;
    ops->ns_rename_flags       = daos_ns_rename_flags;
    ops->ns_link               = daos_ns_link;
    ops->ns_lookup             = daos_ns_lookup;
    ops->ns_getattr            = daos_ns_getattr;
    ops->ns_setattr            = daos_ns_setattr;
    ops->ns_readdir            = daos_ns_readdir;
    ops->dirent_name_for_child = daos_dirent_name_for_child;
    ops->ns_readdir_plus_from  = daos_ns_readdir_plus_from;
    ops->ns_nlink_adjust       = daos_ns_nlink_adjust;
    ops->alloc_fileid          = daos_alloc_fileid;
    ops->inode_put             = daos_inode_put;
    ops->inode_del             = daos_inode_del;
    ops->dirent_put            = daos_dirent_put;
    ops->dirent_insert         = daos_dirent_insert;
    ops->dirent_del            = daos_dirent_del;
    /* ns_create_with_layout and ns_readdir_plus stay NULL: the dispatcher
     * falls back to ns_create and to ns_readdir + ns_getattr, as on the
     * FoundationDB backend. */
}
