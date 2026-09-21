/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * catalogue_fdb_ns.c -- FoundationDB backend: namespace authority slots.
 *
 * Every slot is ONE fdb_run_txn (fdb_txn.h); the mds_cat_txn token is
 * never enlisted (C6).  Reads that can be issued together are started
 * as parallel futures and waited on afterwards, so a slot pays one
 * round trip per dependent wave, not per key.  Enumerating slots
 * materialise a bounded page inside the transaction and deliver it
 * after fdb_run_txn returned (C1/C2): a callback may re-enter this
 * handle and a retried attempt never redelivers.
 *
 * Transaction shapes (R = point read, RR = range read, W = set/clear,
 * A = atomic ADD, CR = conflict ranges beyond the reads; every mutating
 * transaction adds the witness set + READ conflict range and commits
 * once):
 *   ns_lookup            R dirent -> RR inode          (2 dependent waves)
 *   ns_getattr           RR inode                      (1 wave)
 *   ns_create            R parent blob || R dirent; W child (1 or 5 keys),
 *                        W dirent, W dirent_seq, A parent change [+ A
 *                        nlink], W parent mtime/ctime [+ stripe rows]
 *   ns_create_wide       R parent blob || R dirent || R child blob; W child,
 *                        W dirent, W dirent_seq, W stripe hdr + entries,
 *                        parent touch
 *   ns_link              R parent blob || R target blob || R dirent;
 *                        W dirent, W dirent_seq, W target blob, parent touch
 *   ns_setattr           R blob; W blob [dir: A change, W ctime/mtime]
 *   ns_nlink_adjust      R blob; dir: A nlink / file: W blob
 *   ns_remove*           R dirent || R parent blob -> RR child [|| RR child
 *                        dirents limit 1 for a directory]; W clear dirent +
 *                        dirent_seq, final: clear INODE/XATTR/INLINE/STRIPE
 *                        ranges [+ W GC rows with their GC_BY_OWNER keys] /
 *                        non-final: W blob; parent touch [A nlink -1 for a
 *                        directory child]
 *   ns_rename*           R src dirent || R dst dirent || R src parent blob
 *                        || R dst parent blob -> RR src child [|| RR victim
 *                        || RR victim dirents limit 1]; W clear src dirent +
 *                        seq, victim purge (INODE / XATTR / INLINE; the
 *                        STRIPE rows stay for the caller) / W victim blob,
 *                        W dst dirent + seq, [W src child blob
 *                        (parent_fileid)], A nlink moves, parent touches
 *   ns_readdir           per page: RR dirents (read-only)
 *   ns_readdir_plus_from per page: RR dirent_seq -> N parallel RR inodes
 *   dirent_name_for_child per page: RR dirents (read-only)
 *   dirent_put/insert/del R dirent; W dirent [+ seq] / clear
 *   inode_put            W blob [+ 4 counters]
 *   inode_del            R blob; clear INODE range
 *   alloc_fileid         thread-local batch; refill: R counter, W counter
 *   ns_parent_touch      R blob; A change, W mtime/ctime
 *
 * READDIR cookies.  Each dirent carries a sequence minted from the
 * cluster-global META cookie counter in thread-local batches
 * (fdb_backend_alloc_id): unique cluster-wide, hence unique per
 * directory, never below 16 (0/1/2 are reserved) and stable for the
 * life of the binding -- a rename or dirent_put rebinding gets a new
 * one.  The parent's `change` counter is NOT used: learning its
 * post-increment value inside the transaction would need a read of a
 * key every concurrent create in the directory writes, which would
 * make sibling creates conflict and defeat the counter split.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "catalogue_fdb.h"
#include "catalogue_fdb_internal.h"
#include "catalogue_internal.h"
#include "ds_prealloc.h"
#include "fdb_codec.h"
#include "fdb_keys.h"
#include "fdb_txn.h"

/* Entries materialised per read-only page (one transaction each). */
#define FDB_READDIR_PAGE      256U
/* Entries per readdir_plus page: each carries a parallel inode read. */
#define FDB_READDIR_PLUS_PAGE 128U
/* Upper bound of GC rows one fused remove may fold. */
#define FDB_GC_ROWS_MAX       4096U

/* -----------------------------------------------------------------------
 * Small helpers
 * ----------------------------------------------------------------------- */

static struct fdb_backend *be_of(const struct mds_catalogue *cat)
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

/* -----------------------------------------------------------------------
 * Inode read / write primitives
 * ----------------------------------------------------------------------- */

int catalogue_fdb_inode_write(FDBTransaction *tr, const struct fdb_key_prefix *p,
                              const struct mds_inode *ino)
{
    struct mds_inode blob_copy;
    struct fdb_dir_counters ctr;
    uint8_t enc[FDB_INODE_ENC_MAX];
    size_t len = 0;
    struct fdb_key k;

    fdb_inode_split(ino, &blob_copy, &ctr);
    if (!fdb_inode_encode(&blob_copy, enc, sizeof(enc), &len)) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    fdb_key_inode(&k, p, ino->fileid, FDB_INODE_PART_BLOB);
    fdb_txn_set(tr, &k, enc, len);
    if (!fdb_inode_is_dir(ino)) {
        return 0;
    }
    fdb_key_inode(&k, p, ino->fileid, FDB_INODE_PART_NLINK);
    fdb_txn_set_le64(tr, &k, ctr.nlink);
    fdb_key_inode(&k, p, ino->fileid, FDB_INODE_PART_CHANGE);
    fdb_txn_set_le64(tr, &k, ctr.change);
    fdb_key_inode(&k, p, ino->fileid, FDB_INODE_PART_MTIME);
    fdb_txn_set_le64(tr, &k, ctr.mtime_ns);
    fdb_key_inode(&k, p, ino->fileid, FDB_INODE_PART_CTIME);
    fdb_txn_set_le64(tr, &k, ctr.ctime_ns);
    return 0;
}

/* Start the range read of one logical inode (blob + side keys); the
 * blob-only read and write are fdb_blob_read_start / fdb_blob_write
 * (catalogue_fdb_internal.h). */
static FDBFuture *inode_read_start(FDBTransaction *tr, const struct fdb_key_prefix *p,
                                   uint64_t fileid)
{
    struct fdb_key_range r;

    fdb_key_inode_prefix(&r.begin, p, fileid);
    if (!fdb_key_range_prefix(&r, &r.begin)) {
        return NULL;
    }
    return fdb_txn_get_range_start(tr, &r, (int)FDB_INODE_PART_COUNT + 1, false, false);
}

/* Finish an inode range read: decode the blob, compose the side keys.
 * *found is false when the blob is absent. */
static fdb_error_t inode_read_finish(FDBFuture *f, struct mds_inode *out, bool *found)
{
    const FDBKeyValue *kvs = NULL;
    struct fdb_dir_counters ctr;
    bool have_blob = false;
    bool more = false;
    int count = 0;
    int i;
    fdb_error_t err;

    *found = false;
    memset(&ctr, 0, sizeof(ctr));
    err = fdb_txn_get_range_wait(f, &kvs, &count, &more);
    if (err != 0) {
        if (f != NULL) {
            fdb_future_destroy(f);
        }
        return err;
    }
    for (i = 0; i < count; i++) {
        FDBKeyValue kv;
        uint8_t part;
        uint64_t v;

        fdb_kv_at(kvs, i, &kv);
        if (kv.key_length < 1) {
            continue;
        }
        part = kv.key[kv.key_length - 1];
        if (part == FDB_INODE_PART_BLOB) {
            if (!fdb_inode_decode(kv.value, (size_t)kv.value_length, out)) {
                fdb_future_destroy(f);
                return FDB_ERR_PLATFORM_ERROR; /* corrupt blob */
            }
            have_blob = true;
            continue;
        }
        if (!fdb_le64_decode(kv.value, (size_t)kv.value_length, &v)) {
            fdb_future_destroy(f);
            return FDB_ERR_PLATFORM_ERROR;
        }
        if (part == FDB_INODE_PART_NLINK) {
            ctr.nlink = v;
        } else if (part == FDB_INODE_PART_CHANGE) {
            ctr.change = v;
        } else if (part == FDB_INODE_PART_MTIME) {
            ctr.mtime_ns = v;
        } else if (part == FDB_INODE_PART_CTIME) {
            ctr.ctime_ns = v;
        }
    }
    fdb_future_destroy(f);
    if (have_blob) {
        fdb_inode_compose(out, &ctr);
        *found = true;
    }
    return 0;
}

/* -----------------------------------------------------------------------
 * Dirent primitives (the reads and the clear are shared with the ext
 * unit: catalogue_fdb_internal.h)
 * ----------------------------------------------------------------------- */

/* Write the DIRENT row and its DIRENT_SEQ index row together. */
static int dirent_write(FDBTransaction *tr, const struct fdb_key_prefix *p, uint64_t parent,
                        const char *name, uint64_t child, uint8_t type, uint64_t seq)
{
    struct fdb_dirent_val dv = { child, seq, type };
    struct fdb_dirent_seq_val sv;
    uint8_t enc[FDB_DIRENT_SEQ_ENC_MAX];
    size_t len = 0;
    struct fdb_key k;

    if (!fdb_dirent_encode(&dv, enc, sizeof(enc), &len)) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    fdb_key_dirent(&k, p, parent, name);
    if (!fdb_key_ok(&k)) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    fdb_txn_set(tr, &k, enc, len);

    memset(&sv, 0, sizeof(sv));
    sv.child_fileid = child;
    sv.type = type;
    (void)snprintf(sv.name, sizeof(sv.name), "%s", name);
    if (!fdb_dirent_seq_encode(&sv, enc, sizeof(enc), &len)) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    fdb_key_dirent_seq(&k, p, parent, seq);
    fdb_txn_set(tr, &k, enc, len);
    return 0;
}

/* Directory bookkeeping: change += 1, mtime/ctime = now, nlink += delta
 * (all blind: no read of the hot counters). */
static void parent_touch(FDBTransaction *tr, const struct fdb_key_prefix *p, uint64_t dir,
                         int64_t nlink_delta, struct timespec now)
{
    struct fdb_key k;
    uint64_t ns = (uint64_t)fdb_ts_to_ns(now);

    fdb_key_inode(&k, p, dir, FDB_INODE_PART_CHANGE);
    fdb_txn_add_le64(tr, &k, 1);
    if (nlink_delta != 0) {
        fdb_key_inode(&k, p, dir, FDB_INODE_PART_NLINK);
        fdb_txn_add_le64(tr, &k, nlink_delta);
    }
    fdb_key_inode(&k, p, dir, FDB_INODE_PART_MTIME);
    fdb_txn_set_le64(tr, &k, ns);
    fdb_key_inode(&k, p, dir, FDB_INODE_PART_CTIME);
    fdb_txn_set_le64(tr, &k, ns);
}

/* Everything hanging off a deleted inode except its stripe map: the
 * INODE range, xattrs and inline data. */
static void inode_purge_keep_stripes(FDBTransaction *tr, const struct fdb_key_prefix *p,
                                     uint64_t fileid)
{
    struct fdb_key_range r;
    struct fdb_key k;

    fdb_key_inode_prefix(&r.begin, p, fileid);
    if (fdb_key_range_prefix(&r, &r.begin)) {
        fdb_txn_clear_range(tr, &r);
    }
    fdb_key_xattr_prefix(&r.begin, p, fileid);
    if (fdb_key_range_prefix(&r, &r.begin)) {
        fdb_txn_clear_range(tr, &r);
    }
    fdb_key_inline(&k, p, fileid);
    fdb_txn_clear(tr, &k);
}

/* The stripe map of @p fileid: the STRIPE_HDR row and the STRIPE_ENT range. */
static void stripe_rows_purge(FDBTransaction *tr, const struct fdb_key_prefix *p,
                              uint64_t fileid)
{
    struct fdb_key_range r;
    struct fdb_key k;

    fdb_key_stripe_ent_prefix(&r.begin, p, fileid);
    if (fdb_key_range_prefix(&r, &r.begin)) {
        fdb_txn_clear_range(tr, &r);
    }
    fdb_key_stripe_hdr(&k, p, fileid);
    fdb_txn_clear(tr, &k);
}

/* A final unlink through REMOVE drops everything, the stripe map
 * included: op_remove snapshots the map BEFORE the mutation
 * (compound_namespace.c op_remove_prepare_final_unlink; the fused
 * ns_remove_known_gc carries the GC rows derived from it), so nothing
 * reads the rows after the commit -- the same shape as memdb's unlink
 * and the RonDB ns_remove.  RENAME is different: see rename_drop_victim. */
static void inode_purge(FDBTransaction *tr, const struct fdb_key_prefix *p, uint64_t fileid)
{
    inode_purge_keep_stripes(tr, p, fileid);
    stripe_rows_purge(tr, p, fileid);
}

/* Start a "has at least one entry" read of directory @p dir. */
static FDBFuture *dir_nonempty_start(FDBTransaction *tr, const struct fdb_key_prefix *p,
                                     uint64_t dir)
{
    struct fdb_key_range r;

    fdb_key_dirent_prefix(&r.begin, p, dir);
    if (!fdb_key_range_prefix(&r, &r.begin)) {
        return NULL;
    }
    return fdb_txn_get_range_start(tr, &r, 1, false, false);
}

static fdb_error_t dir_nonempty_finish(FDBFuture *f, bool *nonempty)
{
    const FDBKeyValue *kvs = NULL;
    bool more = false;
    int count = 0;
    fdb_error_t err;

    *nonempty = false;
    err = fdb_txn_get_range_wait(f, &kvs, &count, &more);
    if (f != NULL) {
        fdb_future_destroy(f);
    }
    if (err == 0) {
        *nonempty = (count > 0);
    }
    return err;
}

/* Write a stripe map: header + stripe-major entries. */
static int stripe_map_write(FDBTransaction *tr, const struct fdb_key_prefix *p,
                            uint64_t fileid, uint32_t sc, uint32_t su, uint32_t mc,
                            const struct mds_ds_map_entry *entries)
{
    struct fdb_stripe_hdr_val hdr = { sc, su, mc };
    uint8_t enc[FDB_STRIPE_ENT_ENC_MAX];
    size_t len = 0;
    struct fdb_key k;
    uint64_t n = (uint64_t)sc * mc;
    uint64_t i;

    if (!fdb_stripe_hdr_encode(&hdr, enc, sizeof(enc), &len)) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    fdb_key_stripe_hdr(&k, p, fileid);
    fdb_txn_set(tr, &k, enc, len);
    for (i = 0; i < n; i++) {
        if (!fdb_stripe_ent_encode(&entries[i], enc, sizeof(enc), &len)) {
            return FDB_ERR_PLATFORM_ERROR;
        }
        fdb_key_stripe_ent(&k, p, fileid, (uint32_t)i);
        fdb_txn_set(tr, &k, enc, len);
    }
    return 0;
}

/* -----------------------------------------------------------------------
 * ns_lookup / ns_getattr
 * ----------------------------------------------------------------------- */

struct lookup_ctx {
    struct fdb_backend *b;
    uint64_t            parent;
    const char         *name;
    struct mds_inode    out;
};

static int lookup_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct lookup_ctx *c = arg;
    struct fdb_dirent_val dv;
    bool found = false;
    fdb_error_t err;

    err = fdb_dirent_read_finish(fdb_dirent_read_start(tr, &c->b->prefix, c->parent, c->name),
                             &dv, &found);
    if (err != 0) {
        return (int)err;
    }
    if (!found) {
        *st_out = MDS_ERR_NOTFOUND;
        return FDB_BODY_DONE;
    }
    err = inode_read_finish(inode_read_start(tr, &c->b->prefix, dv.child_fileid), &c->out,
                            &found);
    if (err != 0) {
        return (int)err;
    }
    *st_out = found ? MDS_OK : MDS_ERR_NOTFOUND;
    return FDB_BODY_COMMIT;
}

static enum mds_status fdb_ns_lookup(struct mds_catalogue *cat, uint64_t parent,
                                     const char *name, struct mds_inode *child)
{
    struct lookup_ctx c;
    enum mds_status st;

    if (be_of(cat) == NULL || !name_ok(name) || child == NULL) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.b = be_of(cat);
    c.parent = parent;
    c.name = name;
    st = fdb_run_txn(c.b, FDB_TXN_READONLY, "ns_lookup", lookup_body, &c);
    if (st == MDS_OK) {
        *child = c.out;
    }
    return st;
}

struct getattr_ctx {
    struct fdb_backend *b;
    uint64_t            fileid;
    struct mds_inode    out;
};

static int getattr_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct getattr_ctx *c = arg;
    bool found = false;
    fdb_error_t err;

    err = inode_read_finish(inode_read_start(tr, &c->b->prefix, c->fileid), &c->out, &found);
    if (err != 0) {
        return (int)err;
    }
    *st_out = found ? MDS_OK : MDS_ERR_NOTFOUND;
    return FDB_BODY_COMMIT;
}

static enum mds_status fdb_ns_getattr(struct mds_catalogue *cat, uint64_t fileid,
                                      struct mds_inode *inode)
{
    struct getattr_ctx c;
    enum mds_status st;

    if (be_of(cat) == NULL || inode == NULL) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.b = be_of(cat);
    c.fileid = fileid;
    st = fdb_run_txn(c.b, FDB_TXN_READONLY, "ns_getattr", getattr_body, &c);
    if (st == MDS_OK) {
        *inode = c.out;
    }
    return st;
}

/* -----------------------------------------------------------------------
 * ns_create / ns_create_wide
 * ----------------------------------------------------------------------- */

struct create_ctx {
    struct fdb_backend       *b;
    uint64_t                  parent;
    const char               *name;
    struct mds_inode          child;    /**< Fully built before the body. */
    uint64_t                  seq;      /**< Dirent cookie, minted once. */
    bool                      check_child_absent; /**< create_wide: EXISTS on a live child. */
    /* Stripe rows written with the child (count 0 = none). */
    uint32_t                  sc;
    uint32_t                  su;
    uint32_t                  mc;
    const struct mds_ds_map_entry *entries;
};

/* Validation half: parent is a live directory, the name is free and
 * (create_wide) the child fileid is unused.  Three parallel reads. */
static int create_check(FDBTransaction *tr, const struct create_ctx *c,
                        enum mds_status *st_out, bool *ok)
{
    FDBFuture *f_parent;
    FDBFuture *f_dirent;
    FDBFuture *f_child = NULL;
    struct mds_inode parent;
    struct fdb_dirent_val dv;
    bool parent_found = false;
    bool dirent_found = false;
    bool child_found = false;
    fdb_error_t err;
    fdb_error_t err2;

    *ok = false;
    f_parent = fdb_blob_read_start(tr, &c->b->prefix, c->parent);
    f_dirent = fdb_dirent_read_start(tr, &c->b->prefix, c->parent, c->name);
    if (c->check_child_absent) {
        f_child = fdb_blob_read_start(tr, &c->b->prefix, c->child.fileid);
    }
    err = fdb_blob_read_finish(f_parent, &parent, &parent_found);
    err2 = fdb_dirent_read_finish(f_dirent, &dv, &dirent_found);
    if (err == 0) {
        err = err2;
    }
    if (f_child != NULL) {
        struct mds_inode existing;

        err2 = fdb_blob_read_finish(f_child, &existing, &child_found);
        if (err == 0) {
            err = err2;
        }
    }
    if (err != 0) {
        return (int)err;
    }
    if (!parent_found) {
        *st_out = MDS_ERR_NOTFOUND;
        return FDB_BODY_DONE;
    }
    if (!fdb_inode_is_dir(&parent)) {
        *st_out = MDS_ERR_NOTDIR;
        return FDB_BODY_DONE;
    }
    if (dirent_found || child_found) {
        *st_out = MDS_ERR_EXISTS;
        return FDB_BODY_DONE;
    }
    *ok = true;
    return FDB_BODY_COMMIT;
}

static int create_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct create_ctx *c = arg;
    bool ok = false;
    int rc;

    rc = create_check(tr, c, st_out, &ok);
    if (rc != FDB_BODY_COMMIT || !ok) {
        return rc;
    }
    rc = catalogue_fdb_inode_write(tr, &c->b->prefix, &c->child);
    if (rc != 0) {
        return rc;
    }
    rc = dirent_write(tr, &c->b->prefix, c->parent, c->name, c->child.fileid,
                      (uint8_t)c->child.type, c->seq);
    if (rc != 0) {
        return rc;
    }
    if (c->sc > 0) {
        rc = stripe_map_write(tr, &c->b->prefix, c->child.fileid, c->sc, c->su, c->mc,
                              c->entries);
        if (rc != 0) {
            return rc;
        }
    }
    parent_touch(tr, &c->b->prefix, c->parent, fdb_inode_is_dir(&c->child) ? 1 : 0,
                 c->child.ctime);
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

/* Placement popped from the DS prealloc ring for a regular-file create
 * (same shape as the RonDB backend): a captured FH goes inline on the
 * inode (MDS_IFLAG_INLINE_STRIPE); a pop without an FH keeps a 1x1
 * stripe map with an empty entry and marks the inode DS_PENDING so the
 * late FH capture path lands the handle. */
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

static enum mds_status fdb_ns_create(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                     uint64_t parent, const char *name,
                                     enum mds_file_type type, uint32_t mode, uint64_t uid,
                                     uint64_t gid, struct ds_prealloc_ctx *prealloc,
                                     struct mds_inode *out)
{
    struct create_ctx c;
    struct create_pop pop;
    uint64_t fileid = 0;
    enum mds_status st;

    (void)txn;
    if (be_of(cat) == NULL || !name_ok(name) || out == NULL) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.b = be_of(cat);
    c.parent = parent;
    c.name = name;

    /* Pop, fileid and cookie are minted ONCE per call, outside the
     * body: a retried attempt reuses them (nothing was published). */
    create_pop_prealloc(prealloc, type, &pop);
    fileid = pop.fileid;
    if (fileid == 0) {
        st = fdb_backend_alloc_id(c.b, FDB_META_FILEID, &fileid);
        if (st != MDS_OK) {
            return st;
        }
    }
    st = fdb_backend_alloc_id(c.b, FDB_META_COOKIE_SEQ, &c.seq);
    if (st != MDS_OK) {
        return st;
    }
    build_child(&c.child, fileid, parent, type, mode, uid, gid, now_ts());
    create_apply_pop(&c, &pop);

    st = fdb_run_txn(c.b, FDB_TXN_MUTATING, "ns_create", create_body, &c);
    if (st == MDS_OK) {
        *out = c.child;
    }
    return st;
}

static enum mds_status fdb_ns_create_wide(struct mds_catalogue *cat, uint64_t parent,
                                          const char *name, const struct mds_inode *child,
                                          uint32_t stripe_count, uint32_t stripe_unit,
                                          uint32_t mirror_count,
                                          const struct mds_ds_map_entry *entries,
                                          bool *safe_to_discard)
{
    struct create_ctx c;
    enum mds_status st;

    if (safe_to_discard != NULL) {
        *safe_to_discard = false;
    }
    if (be_of(cat) == NULL || !name_ok(name) || child == NULL || entries == NULL ||
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
    c.b = be_of(cat);
    c.parent = parent;
    c.name = name;
    c.child = *child;
    c.child.ds_map = NULL;
    c.check_child_absent = true;
    c.sc = stripe_count;
    c.su = stripe_unit;
    c.mc = mirror_count;
    c.entries = entries;
    st = fdb_backend_alloc_id(c.b, FDB_META_COOKIE_SEQ, &c.seq);
    if (st != MDS_OK) {
        *safe_to_discard = (st != MDS_ERR_INDOUBT && st != MDS_ERR_DELAY && st != MDS_ERR_IO);
        return st;
    }
    st = fdb_run_txn(c.b, FDB_TXN_MUTATING, "ns_create_wide", create_body, &c);
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
    struct fdb_backend *b;
    uint64_t            parent;
    const char         *name;
    uint64_t            target;
    uint64_t            seq;
};

static int link_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct link_ctx *c = arg;
    FDBFuture *f_parent;
    FDBFuture *f_target;
    FDBFuture *f_dirent;
    struct mds_inode parent;
    struct mds_inode target;
    struct fdb_dirent_val dv;
    bool parent_found = false;
    bool target_found = false;
    bool dirent_found = false;
    struct timespec now;
    fdb_error_t err;
    fdb_error_t err2;
    int rc;

    f_parent = fdb_blob_read_start(tr, &c->b->prefix, c->parent);
    f_target = fdb_blob_read_start(tr, &c->b->prefix, c->target);
    f_dirent = fdb_dirent_read_start(tr, &c->b->prefix, c->parent, c->name);
    err = fdb_blob_read_finish(f_parent, &parent, &parent_found);
    err2 = fdb_blob_read_finish(f_target, &target, &target_found);
    if (err == 0) {
        err = err2;
    }
    err2 = fdb_dirent_read_finish(f_dirent, &dv, &dirent_found);
    if (err == 0) {
        err = err2;
    }
    if (err != 0) {
        return (int)err;
    }
    if (!parent_found || !target_found) {
        *st_out = MDS_ERR_NOTFOUND;
        return FDB_BODY_DONE;
    }
    if (!fdb_inode_is_dir(&parent)) {
        *st_out = MDS_ERR_NOTDIR;
        return FDB_BODY_DONE;
    }
    if (fdb_inode_is_dir(&target)) {
        *st_out = MDS_ERR_ISDIR;
        return FDB_BODY_DONE;
    }
    if (dirent_found) {
        *st_out = MDS_ERR_EXISTS;
        return FDB_BODY_DONE;
    }
    now = now_ts();
    rc = dirent_write(tr, &c->b->prefix, c->parent, c->name, c->target, (uint8_t)target.type,
                      c->seq);
    if (rc != 0) {
        return rc;
    }
    target.nlink++;
    target.ctime = now;
    target.change++;
    rc = fdb_blob_write(tr, &c->b->prefix, &target);
    if (rc != 0) {
        return rc;
    }
    parent_touch(tr, &c->b->prefix, c->parent, 0, now);
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static enum mds_status fdb_ns_link(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                   uint64_t parent, const char *name, uint64_t target)
{
    struct link_ctx c;
    enum mds_status st;

    (void)txn;
    if (be_of(cat) == NULL || !name_ok(name)) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.b = be_of(cat);
    c.parent = parent;
    c.name = name;
    c.target = target;
    st = fdb_backend_alloc_id(c.b, FDB_META_COOKIE_SEQ, &c.seq);
    if (st != MDS_OK) {
        return st;
    }
    return fdb_run_txn(c.b, FDB_TXN_MUTATING, "ns_link", link_body, &c);
}

/* -----------------------------------------------------------------------
 * ns_setattr / ns_nlink_adjust
 * ----------------------------------------------------------------------- */

struct setattr_ctx {
    struct fdb_backend     *b;
    uint64_t                fileid;
    const struct mds_inode *attrs;
    uint32_t                mask;
};

/* Blob-resident attributes (same mask semantics as memdb / RonDB, incl.
 * the grow-only SIZE_EXTEND).  Returns true when the blob changed. */
static bool setattr_apply_blob(struct mds_inode *i, const struct mds_inode *a, uint32_t mask,
                               struct timespec now)
{
    bool changed = false;

    if ((mask & MDS_ATTR_MODE) != 0) {
        i->mode = a->mode;
        changed = true;
    }
    if ((mask & MDS_ATTR_UID) != 0) {
        i->uid = a->uid;
        changed = true;
    }
    if ((mask & MDS_ATTR_GID) != 0) {
        i->gid = a->gid;
        changed = true;
    }
    if ((mask & MDS_ATTR_SIZE) != 0) {
        i->size = a->size;
        changed = true;
    }
    if ((mask & MDS_ATTR_SIZE_EXTEND) != 0 && a->size > i->size) {
        i->size = a->size;
        changed = true;
    }
    if ((mask & MDS_ATTR_ATIME) != 0) {
        i->atime = a->atime;
        changed = true;
    }
    if ((mask & MDS_ATTR_ATIME_NOW) != 0) {
        i->atime = now;
        changed = true;
    }
    if ((mask & MDS_ATTR_FLAGS) != 0) {
        i->flags = a->flags;
        changed = true;
    }
    return changed;
}

static int setattr_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct setattr_ctx *c = arg;
    struct mds_inode ino;
    struct timespec now;
    bool found = false;
    bool blob_changed;
    fdb_error_t err;
    int rc;

    err = fdb_blob_read_finish(fdb_blob_read_start(tr, &c->b->prefix, c->fileid), &ino, &found);
    if (err != 0) {
        return (int)err;
    }
    if (!found) {
        *st_out = MDS_ERR_NOTFOUND;
        return FDB_BODY_DONE;
    }
    now = now_ts();
    blob_changed = setattr_apply_blob(&ino, c->attrs, c->mask, now);
    if (fdb_inode_is_dir(&ino)) {
        struct fdb_key k;

        /* Side keys are authoritative for the directory's mtime, ctime
         * and change: blind writes, no read of the hot counters. */
        if (blob_changed) {
            rc = fdb_blob_write(tr, &c->b->prefix, &ino);
            if (rc != 0) {
                return rc;
            }
        }
        if ((c->mask & (MDS_ATTR_MTIME | MDS_ATTR_MTIME_NOW)) != 0) {
            struct timespec mt = ((c->mask & MDS_ATTR_MTIME_NOW) != 0) ? now : c->attrs->mtime;

            fdb_key_inode(&k, &c->b->prefix, c->fileid, FDB_INODE_PART_MTIME);
            fdb_txn_set_le64(tr, &k, (uint64_t)fdb_ts_to_ns(mt));
        }
        fdb_key_inode(&k, &c->b->prefix, c->fileid, FDB_INODE_PART_CTIME);
        fdb_txn_set_le64(tr, &k, (uint64_t)fdb_ts_to_ns(now));
        fdb_key_inode(&k, &c->b->prefix, c->fileid, FDB_INODE_PART_CHANGE);
        fdb_txn_add_le64(tr, &k, 1);
        *st_out = MDS_OK;
        return FDB_BODY_COMMIT;
    }
    if ((c->mask & MDS_ATTR_MTIME) != 0) {
        ino.mtime = c->attrs->mtime;
    }
    if ((c->mask & MDS_ATTR_MTIME_NOW) != 0) {
        ino.mtime = now;
    }
    ino.ctime = now;
    ino.change++;
    rc = fdb_blob_write(tr, &c->b->prefix, &ino);
    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static enum mds_status fdb_ns_setattr(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                      uint64_t fileid, const struct mds_inode *attrs,
                                      uint32_t mask)
{
    struct setattr_ctx c;

    (void)txn;
    if (be_of(cat) == NULL || attrs == NULL) {
        return MDS_ERR_INVAL;
    }
    c.b = be_of(cat);
    c.fileid = fileid;
    c.attrs = attrs;
    c.mask = mask;
    return fdb_run_txn(c.b, FDB_TXN_MUTATING, "ns_setattr", setattr_body, &c);
}

struct nlink_ctx {
    struct fdb_backend *b;
    uint64_t            fileid;
    int32_t             delta;
};

static int nlink_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct nlink_ctx *c = arg;
    struct mds_inode ino;
    bool found = false;
    fdb_error_t err;

    err = fdb_blob_read_finish(fdb_blob_read_start(tr, &c->b->prefix, c->fileid), &ino, &found);
    if (err != 0) {
        return (int)err;
    }
    if (!found) {
        *st_out = MDS_ERR_NOTFOUND;
        return FDB_BODY_DONE;
    }
    if (fdb_inode_is_dir(&ino)) {
        struct fdb_key k;

        fdb_key_inode(&k, &c->b->prefix, c->fileid, FDB_INODE_PART_NLINK);
        fdb_txn_add_le64(tr, &k, c->delta);
    } else {
        int64_t n = (int64_t)ino.nlink + c->delta;

        ino.nlink = (n < 0) ? 0U : (uint32_t)n;
        if (fdb_blob_write(tr, &c->b->prefix, &ino) != 0) {
            return FDB_ERR_PLATFORM_ERROR;
        }
    }
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static enum mds_status fdb_ns_nlink_adjust(struct mds_catalogue *cat, uint64_t fileid,
                                           int32_t delta)
{
    struct nlink_ctx c;

    if (be_of(cat) == NULL) {
        return MDS_ERR_INVAL;
    }
    c.b = be_of(cat);
    c.fileid = fileid;
    c.delta = delta;
    return fdb_run_txn(c.b, FDB_TXN_MUTATING, "ns_nlink_adjust", nlink_body, &c);
}

/* -----------------------------------------------------------------------
 * ns_remove / ns_remove_known / ns_remove_known_gc
 * ----------------------------------------------------------------------- */

struct remove_ctx {
    struct fdb_backend            *b;
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
    struct fdb_dirent_val dv;
    struct mds_inode      child;
    bool                  parent_found;
    bool                  child_found;
    bool                  is_dir;
    bool                  final;
};

/* Wave 1 (dirent || parent blob) and wave 2 (child inode [|| child
 * dirents]).  Decides everything; nothing is written here. */
static int remove_resolve(FDBTransaction *tr, const struct remove_ctx *c,
                          struct remove_plan *pl, enum mds_status *st_out)
{
    FDBFuture *f_dirent;
    FDBFuture *f_parent;
    FDBFuture *f_child;
    FDBFuture *f_empty = NULL;
    struct mds_inode parent;
    bool dirent_found = false;
    bool nonempty = false;
    fdb_error_t err;
    fdb_error_t err2;

    memset(pl, 0, sizeof(*pl));
    f_dirent = fdb_dirent_read_start(tr, &c->b->prefix, c->parent, c->name);
    f_parent = fdb_blob_read_start(tr, &c->b->prefix, c->parent);
    err = fdb_dirent_read_finish(f_dirent, &pl->dv, &dirent_found);
    err2 = fdb_blob_read_finish(f_parent, &parent, &pl->parent_found);
    if (err == 0) {
        err = err2;
    }
    if (err != 0) {
        return (int)err;
    }
    if (!dirent_found) {
        *st_out = MDS_ERR_NOTFOUND;
        return FDB_BODY_DONE;
    }
    if (c->guard != NULL && pl->dv.child_fileid != c->guard->fileid) {
        *st_out = MDS_ERR_STALE;
        return FDB_BODY_DONE;
    }
    f_child = inode_read_start(tr, &c->b->prefix, pl->dv.child_fileid);
    if (pl->dv.type == (uint8_t)MDS_FTYPE_DIR) {
        f_empty = dir_nonempty_start(tr, &c->b->prefix, pl->dv.child_fileid);
    }
    err = inode_read_finish(f_child, &pl->child, &pl->child_found);
    if (f_empty != NULL) {
        err2 = dir_nonempty_finish(f_empty, &nonempty);
        if (err == 0) {
            err = err2;
        }
    }
    if (err != 0) {
        return (int)err;
    }
    if (c->guard != NULL && pl->child_found && pl->child.generation != c->guard->generation) {
        *st_out = MDS_ERR_STALE;
        return FDB_BODY_DONE;
    }
    pl->is_dir = pl->child_found ? fdb_inode_is_dir(&pl->child)
                                 : pl->dv.type == (uint8_t)MDS_FTYPE_DIR;
    if (pl->child_found && pl->is_dir) {
        if (nonempty) {
            *st_out = MDS_ERR_NOTEMPTY;
            return FDB_BODY_DONE;
        }
        pl->final = true;
    } else if (pl->child_found) {
        pl->final = (pl->child.nlink <= 1U);
        /* The caller derived its plan -- GC rows, quota delta, layout
         * recall -- from the snapshot's link count.  A LINK or another
         * REMOVE that landed since makes that plan wrong for this inode
         * (a "final" snapshot with a live second link would have the
         * caller queue a live file's DS objects for collection).
         * Refuse instead of silently doing the other shape; the caller
         * re-resolves (mds_catalogue.h, ns_remove_known contract). */
        if (c->guard != NULL && (c->guard->nlink <= 1U) != pl->final) {
            *st_out = MDS_ERR_STALE;
            return FDB_BODY_DONE;
        }
    }
    return FDB_BODY_COMMIT;
}

/* The caller's GC rows, each with its GC_BY_OWNER key, stamped with
 * this MDS's id (the same shape gc_enqueue writes). */
static int remove_write_gc_rows(FDBTransaction *tr, const struct remove_ctx *c,
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
        e.owner_mds_id = c->b->mds_id;
        e.sweep_hint = c->gc_sweep_hint;
        e.nfs_fh_len = c->gc_entries[i].nfs_fh_len;
        if (e.nfs_fh_len > MDS_NFS_FH_MAX) {
            e.nfs_fh_len = MDS_NFS_FH_MAX;
        }
        memcpy(e.nfs_fh, c->gc_entries[i].nfs_fh, e.nfs_fh_len);
        rc = fdb_gc_row_set(tr, &c->b->prefix, &e);
        if (rc != 0) {
            return rc;
        }
    }
    return 0;
}

static int remove_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct remove_ctx *c = arg;
    struct remove_plan pl;
    struct timespec now;
    int rc;

    c->final = false;
    rc = remove_resolve(tr, c, &pl, st_out);
    if (rc != FDB_BODY_COMMIT) {
        return rc;
    }
    now = now_ts();
    fdb_dirent_clear(tr, &c->b->prefix, c->parent, c->name, pl.dv.seq);
    if (pl.child_found && pl.final) {
        inode_purge(tr, &c->b->prefix, pl.dv.child_fileid);
        rc = remove_write_gc_rows(tr, c, pl.dv.child_fileid);
        if (rc != 0) {
            return rc;
        }
    } else if (pl.child_found) {
        pl.child.nlink--;
        pl.child.ctime = now;
        pl.child.change++;
        rc = fdb_blob_write(tr, &c->b->prefix, &pl.child);
        if (rc != 0) {
            return rc;
        }
    }
    if (pl.parent_found) {
        parent_touch(tr, &c->b->prefix, c->parent, (pl.is_dir && pl.final) ? -1 : 0, now);
    }
    c->final = pl.final;
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static enum mds_status fdb_ns_remove(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                     uint64_t parent, const char *name)
{
    struct remove_ctx c;

    (void)txn;
    if (be_of(cat) == NULL || !name_ok(name)) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.b = be_of(cat);
    c.parent = parent;
    c.name = name;
    return fdb_run_txn(c.b, FDB_TXN_MUTATING, "ns_remove", remove_body, &c);
}

static enum mds_status fdb_ns_remove_known(struct mds_catalogue *cat, struct mds_cat_txn *txn,
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
    c.b = be_of(cat);
    c.parent = parent;
    c.name = name;
    c.guard = child;
    return fdb_run_txn(c.b, FDB_TXN_MUTATING, "ns_remove_known", remove_body, &c);
}

static enum mds_status fdb_ns_remove_known_gc(struct mds_catalogue *cat,
                                              struct mds_cat_txn *txn, uint64_t parent,
                                              const char *name, const struct mds_inode *child,
                                              uint32_t stripe_count,
                                              const struct mds_ds_map_entry *gc_entries,
                                              uint32_t gc_entry_count, uint32_t gc_sweep_hint,
                                              bool *gc_folded)
{
    struct remove_ctx c;
    uint64_t *seqs = NULL;
    enum mds_status st;

    (void)txn;
    (void)stripe_count;
    if (gc_folded != NULL) {
        *gc_folded = false;
    }
    if (be_of(cat) == NULL || !name_ok(name) || child == NULL || gc_folded == NULL ||
        (gc_entry_count > 0 && gc_entries == NULL) || gc_entry_count > FDB_GC_ROWS_MAX) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.b = be_of(cat);
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
            st = fdb_backend_alloc_id(c.b, FDB_META_GC_SEQ, &seqs[i]);
            if (st != MDS_OK) {
                free(seqs);
                return st;
            }
        }
        c.gc_seqs = seqs;
    }
    st = fdb_run_txn(c.b, FDB_TXN_MUTATING, "ns_remove_known_gc", remove_body, &c);
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
    struct fdb_backend *b;
    uint64_t            sp;
    const char         *sn;
    uint64_t            dp;
    const char         *dn;
    uint32_t            flags;
    uint64_t            seq;    /**< Cookie of the rebound destination entry. */
};

struct rename_plan {
    struct fdb_dirent_val src;
    struct fdb_dirent_val dst;
    struct mds_inode      src_child;
    struct mds_inode      dst_child;
    bool                  dst_found;
    bool                  sp_found;
    bool                  sc_found;
    bool                  dc_found;
    bool                  src_is_dir;
    bool                  dst_is_dir;
};

/* Wave 1: both dirents and both parent blobs in parallel. */
static int rename_wave1(FDBTransaction *tr, const struct rename_ctx *c, struct rename_plan *pl,
                        enum mds_status *st_out)
{
    FDBFuture *f_sd;
    FDBFuture *f_dd;
    FDBFuture *f_sp;
    FDBFuture *f_dp = NULL;
    struct mds_inode sp_ino;
    struct mds_inode dp_ino;
    bool src_found = false;
    bool dp_found = false;
    fdb_error_t err;
    fdb_error_t err2;

    f_sd = fdb_dirent_read_start(tr, &c->b->prefix, c->sp, c->sn);
    f_dd = fdb_dirent_read_start(tr, &c->b->prefix, c->dp, c->dn);
    f_sp = fdb_blob_read_start(tr, &c->b->prefix, c->sp);
    if (c->dp != c->sp) {
        f_dp = fdb_blob_read_start(tr, &c->b->prefix, c->dp);
    }
    err = fdb_dirent_read_finish(f_sd, &pl->src, &src_found);
    err2 = fdb_dirent_read_finish(f_dd, &pl->dst, &pl->dst_found);
    if (err == 0) {
        err = err2;
    }
    err2 = fdb_blob_read_finish(f_sp, &sp_ino, &pl->sp_found);
    if (err == 0) {
        err = err2;
    }
    if (f_dp != NULL) {
        err2 = fdb_blob_read_finish(f_dp, &dp_ino, &dp_found);
        if (err == 0) {
            err = err2;
        }
    } else {
        dp_ino = sp_ino;
        dp_found = pl->sp_found;
    }
    if (err != 0) {
        return (int)err;
    }
    /* A parent that exists but is not a directory is NOTDIR (RFC 8881
     * 18.26.4: the saved or current filehandle is not a directory);
     * only a missing parent or a missing source name is NOTFOUND.  A
     * non-directory has no dirents, so the type has to be decided
     * before the absent source name is, or the answer would be NOENT. */
    if ((pl->sp_found && !fdb_inode_is_dir(&sp_ino)) ||
        (dp_found && !fdb_inode_is_dir(&dp_ino))) {
        *st_out = MDS_ERR_NOTDIR;
        return FDB_BODY_DONE;
    }
    if (!src_found || !dp_found) {
        *st_out = MDS_ERR_NOTFOUND;
        return FDB_BODY_DONE;
    }
    return FDB_BODY_COMMIT;
}

/* Wave 2: the source child, the victim and (directory victim) its
 * emptiness, in parallel; then the POSIX type rules. */
static int rename_wave2(FDBTransaction *tr, const struct rename_ctx *c, struct rename_plan *pl,
                        enum mds_status *st_out)
{
    FDBFuture *f_sc;
    FDBFuture *f_dc = NULL;
    FDBFuture *f_de = NULL;
    bool nonempty = false;
    fdb_error_t err;
    fdb_error_t err2;

    f_sc = inode_read_start(tr, &c->b->prefix, pl->src.child_fileid);
    if (pl->dst_found) {
        f_dc = inode_read_start(tr, &c->b->prefix, pl->dst.child_fileid);
        if (pl->dst.type == (uint8_t)MDS_FTYPE_DIR) {
            f_de = dir_nonempty_start(tr, &c->b->prefix, pl->dst.child_fileid);
        }
    }
    err = inode_read_finish(f_sc, &pl->src_child, &pl->sc_found);
    if (f_dc != NULL) {
        err2 = inode_read_finish(f_dc, &pl->dst_child, &pl->dc_found);
        if (err == 0) {
            err = err2;
        }
    }
    if (f_de != NULL) {
        err2 = dir_nonempty_finish(f_de, &nonempty);
        if (err == 0) {
            err = err2;
        }
    }
    if (err != 0) {
        return (int)err;
    }
    pl->src_is_dir = pl->sc_found ? fdb_inode_is_dir(&pl->src_child)
                                  : pl->src.type == (uint8_t)MDS_FTYPE_DIR;
    if (!pl->dst_found) {
        return FDB_BODY_COMMIT;
    }
    pl->dst_is_dir = pl->dc_found ? fdb_inode_is_dir(&pl->dst_child)
                                  : pl->dst.type == (uint8_t)MDS_FTYPE_DIR;
    if (pl->dst_is_dir) {
        if (!pl->src_is_dir) {
            *st_out = MDS_ERR_ISDIR;
            return FDB_BODY_DONE;
        }
        if (nonempty) {
            *st_out = MDS_ERR_NOTEMPTY;
            return FDB_BODY_DONE;
        }
    } else if (pl->src_is_dir) {
        *st_out = MDS_ERR_NOTDIR;
        return FDB_BODY_DONE;
    }
    return FDB_BODY_COMMIT;
}

/* Overwritten destination inode: a directory victim is deleted and the
 * destination parent loses its ".." link; a file victim loses a link and
 * on its last one is deleted or kept as an UNLINK_ORPHAN row.
 *
 * A deleted victim keeps its stripe map.  op_rename
 * (compound_namespace.c) prefetches only the inline 1x1 binding of the
 * victim; for a wide file it reads the map AFTER the rename committed
 * to queue the DS objects for GC and then drops the rows itself, and
 * compound_orphan_finalize does the same for a kept orphan.  A rename
 * that purged the rows would leave the overwritten file's DS objects
 * uncollectable.  memdb and the RonDB rename leave them too; only the
 * REMOVE paths purge them (inode_purge). */
static int rename_drop_victim(FDBTransaction *tr, const struct rename_ctx *c,
                              struct rename_plan *pl, struct timespec now)
{
    struct mds_inode *v = &pl->dst_child;

    if (!pl->dc_found) {
        return 0;
    }
    if (pl->dst_is_dir) {
        inode_purge_keep_stripes(tr, &c->b->prefix, v->fileid);
        {
            struct fdb_key k;

            fdb_key_inode(&k, &c->b->prefix, c->dp, FDB_INODE_PART_NLINK);
            fdb_txn_add_le64(tr, &k, -1);
        }
        return 0;
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
        return fdb_blob_write(tr, &c->b->prefix, v);
    }
    inode_purge_keep_stripes(tr, &c->b->prefix, v->fileid);
    return 0;
}

static int rename_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct rename_ctx *c = arg;
    struct rename_plan pl;
    struct timespec now;
    int rc;

    memset(&pl, 0, sizeof(pl));
    rc = rename_wave1(tr, c, &pl, st_out);
    if (rc != FDB_BODY_COMMIT) {
        return rc;
    }
    if (pl.dst_found && pl.dst.child_fileid == pl.src.child_fileid) {
        *st_out = MDS_OK; /* both names already resolve to the inode: POSIX no-op */
        return FDB_BODY_DONE;
    }
    rc = rename_wave2(tr, c, &pl, st_out);
    if (rc != FDB_BODY_COMMIT) {
        return rc;
    }

    /* Every check passed: mutate. */
    now = now_ts();
    fdb_dirent_clear(tr, &c->b->prefix, c->sp, c->sn, pl.src.seq);
    if (pl.dst_found) {
        struct fdb_key k;

        fdb_key_dirent_seq(&k, &c->b->prefix, c->dp, pl.dst.seq);
        fdb_txn_clear(tr, &k);
        rc = rename_drop_victim(tr, c, &pl, now);
        if (rc != 0) {
            return rc;
        }
    }
    rc = dirent_write(tr, &c->b->prefix, c->dp, c->dn, pl.src.child_fileid, pl.src.type,
                      c->seq);
    if (rc != 0) {
        return rc;
    }
    if (c->sp != c->dp) {
        struct fdb_key k;

        if (pl.src_is_dir) {
            if (pl.sp_found) {
                fdb_key_inode(&k, &c->b->prefix, c->sp, FDB_INODE_PART_NLINK);
                fdb_txn_add_le64(tr, &k, -1);
            }
            fdb_key_inode(&k, &c->b->prefix, c->dp, FDB_INODE_PART_NLINK);
            fdb_txn_add_le64(tr, &k, 1);
        }
        if (pl.sc_found) {
            pl.src_child.parent_fileid = c->dp;
            rc = fdb_blob_write(tr, &c->b->prefix, &pl.src_child);
            if (rc != 0) {
                return rc;
            }
        }
        if (pl.sp_found) {
            parent_touch(tr, &c->b->prefix, c->sp, 0, now);
        }
    }
    parent_touch(tr, &c->b->prefix, c->dp, 0, now);
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static enum mds_status fdb_ns_rename_flags(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                           uint64_t src_parent, const char *src_name,
                                           uint64_t dst_parent, const char *dst_name,
                                           uint32_t ns_flags)
{
    struct rename_ctx c;
    enum mds_status st;

    (void)txn;
    if (be_of(cat) == NULL || !name_ok(src_name) || !name_ok(dst_name)) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.b = be_of(cat);
    c.sp = src_parent;
    c.sn = src_name;
    c.dp = dst_parent;
    c.dn = dst_name;
    c.flags = ns_flags;
    st = fdb_backend_alloc_id(c.b, FDB_META_COOKIE_SEQ, &c.seq);
    if (st != MDS_OK) {
        return st;
    }
    return fdb_run_txn(c.b, FDB_TXN_MUTATING, "ns_rename", rename_body, &c);
}

static enum mds_status fdb_ns_rename(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                     uint64_t src_parent, const char *src_name,
                                     uint64_t dst_parent, const char *dst_name)
{
    return fdb_ns_rename_flags(cat, txn, src_parent, src_name, dst_parent, dst_name, 0U);
}

/* -----------------------------------------------------------------------
 * READDIR (name order), dirent_name_for_child
 *
 * One read-only transaction per page: the page is materialised into a
 * caller-owned bounded buffer, the transaction ends, then the entries
 * are delivered (C1/C2).  The cursor is the last name of the page.
 * ----------------------------------------------------------------------- */

struct rd_entry {
    struct mds_cat_dirent d;
};

struct rd_page_ctx {
    struct fdb_backend *b;
    uint64_t            parent;
    const char         *after;   /**< NULL: from the start. */
    uint32_t            limit;
    struct rd_entry    *page;    /**< limit entries. */
    uint32_t            n;
    bool                more;
};

/* Key of the first entry strictly after @p after (NULL: the range base). */
static void rd_range_after(struct fdb_key_range *r, const struct fdb_key_prefix *p,
                           uint64_t parent, const char *after)
{
    struct fdb_key base;

    fdb_key_dirent_prefix(&base, p, parent);
    (void)fdb_key_range_prefix(r, &base);
    if (after != NULL) {
        fdb_key_dirent(&r->begin, p, parent, after);
        fdb_key_u8(&r->begin, 0); /* smallest key greater than the name */
    }
}

static int rd_page_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct rd_page_ctx *c = arg;
    struct fdb_key_range r;
    struct fdb_key base;
    const FDBKeyValue *kvs = NULL;
    FDBFuture *f;
    int count = 0;
    int i;
    fdb_error_t err;

    c->n = 0;
    c->more = false;
    fdb_key_dirent_prefix(&base, &c->b->prefix, c->parent);
    rd_range_after(&r, &c->b->prefix, c->parent, c->after);
    f = fdb_txn_get_range_start(tr, &r, (int)c->limit, false, false);
    err = fdb_txn_get_range_wait(f, &kvs, &count, &c->more);
    if (err != 0) {
        if (f != NULL) {
            fdb_future_destroy(f);
        }
        return (int)err;
    }
    for (i = 0; i < count && c->n < c->limit; i++) {
        FDBKeyValue kv;
        struct fdb_dirent_val dv;
        struct mds_cat_dirent *d = &c->page[c->n].d;
        int nlen;

        fdb_kv_at(kvs, i, &kv);
        nlen = kv.key_length - (int)base.len;
        if (nlen <= 0 || nlen > (int)MDS_MAX_NAME ||
            !fdb_dirent_decode(kv.value, (size_t)kv.value_length, &dv)) {
            fdb_future_destroy(f);
            return FDB_ERR_PLATFORM_ERROR; /* corrupt row */
        }
        memset(d, 0, sizeof(*d));
        d->fileid = dv.child_fileid;
        d->cookie = dv.seq;
        d->type = dv.type;
        memcpy(d->name, kv.key + base.len, (size_t)nlen);
        d->name[nlen] = '\0';
        c->n++;
    }
    fdb_future_destroy(f);
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static enum mds_status fdb_ns_readdir(struct mds_catalogue *cat, uint64_t parent,
                                      const char *start_after, uint32_t max_entries,
                                      struct mds_cat_txn *txn, mds_readdir_cb cb, void *ctx)
{
    struct rd_page_ctx c;
    char cursor[MDS_MAX_NAME + 1];
    uint32_t delivered = 0;
    enum mds_status st = MDS_OK;

    (void)txn;
    if (be_of(cat) == NULL || cb == NULL ||
        (start_after != NULL && strnlen(start_after, MDS_MAX_NAME + 1U) > MDS_MAX_NAME)) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.b = be_of(cat);
    c.parent = parent;
    c.after = (start_after != NULL && start_after[0] != '\0') ? start_after : NULL;
    c.page = calloc(FDB_READDIR_PAGE, sizeof(*c.page));
    if (c.page == NULL) {
        return MDS_ERR_NOMEM;
    }
    for (;;) {
        uint32_t i;

        c.limit = FDB_READDIR_PAGE;
        if (max_entries > 0 && max_entries - delivered < c.limit) {
            c.limit = max_entries - delivered;
        }
        st = fdb_run_txn(c.b, FDB_TXN_READONLY, "ns_readdir", rd_page_body, &c);
        if (st != MDS_OK) {
            break;
        }
        for (i = 0; i < c.n; i++) {
            if (cb(&c.page[i].d, ctx) != 0) {
                goto out;
            }
            delivered++;
        }
        if (!c.more || (max_entries > 0 && delivered >= max_entries) || c.n == 0) {
            break;
        }
        (void)snprintf(cursor, sizeof(cursor), "%s", c.page[c.n - 1].d.name);
        c.after = cursor;
    }
out:
    free(c.page);
    return st;
}

static enum mds_status fdb_dirent_name_for_child(struct mds_catalogue *cat, uint64_t parent,
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
    c.b = be_of(cat);
    c.parent = parent;
    c.page = calloc(FDB_READDIR_PAGE, sizeof(*c.page));
    if (c.page == NULL) {
        return MDS_ERR_NOMEM;
    }
    for (;;) {
        uint32_t i;
        enum mds_status pst;

        c.limit = FDB_READDIR_PAGE;
        pst = fdb_run_txn(c.b, FDB_TXN_READONLY, "dirent_name_for_child", rd_page_body, &c);
        if (pst != MDS_OK) {
            st = pst;
            break;
        }
        for (i = 0; i < c.n; i++) {
            if (c.page[i].d.fileid == child_fileid) {
                (void)snprintf(name_out, name_out_len, "%s", c.page[i].d.name);
                st = MDS_OK;
                goto out;
            }
        }
        if (!c.more || c.n == 0) {
            break;
        }
        (void)snprintf(cursor, sizeof(cursor), "%s", c.page[c.n - 1].d.name);
        c.after = cursor;
    }
out:
    free(c.page);
    return st;
}

/* -----------------------------------------------------------------------
 * READDIR_PLUS resumed by cookie (DIRENT_SEQ order)
 *
 * Per page: one range read of the cookie index, then one parallel
 * inode range read per entry, all in one read-only transaction; the
 * page is delivered after the transaction ended.
 * ----------------------------------------------------------------------- */

struct rdp_entry {
    struct mds_cat_dirent d;
    struct mds_inode      ino;
    bool                  ino_valid;
    FDBFuture            *f;
};

struct rdp_page_ctx {
    struct fdb_backend *b;
    uint64_t            parent;
    uint64_t            after;   /**< Cookie strictly before the page. */
    uint32_t            limit;
    struct rdp_entry   *page;
    uint32_t            n;
    bool                more;
};

/* Decode the cookie-index page into c->page and start the inode reads. */
static int rdp_collect(FDBTransaction *tr, struct rdp_page_ctx *c, const FDBKeyValue *kvs,
                       int count, uint32_t key_base_len)
{
    int i;

    for (i = 0; i < count && c->n < c->limit; i++) {
        FDBKeyValue kv;
        struct fdb_dirent_seq_val sv;
        struct rdp_entry *e = &c->page[c->n];

        fdb_kv_at(kvs, i, &kv);
        if (kv.key_length != (int)(key_base_len + FDB_KEY_BE64_LEN) ||
            !fdb_dirent_seq_decode(kv.value, (size_t)kv.value_length, &sv)) {
            return FDB_ERR_PLATFORM_ERROR;
        }
        memset(e, 0, sizeof(*e));
        e->d.fileid = sv.child_fileid;
        e->d.cookie = fdb_get_u64(kv.key + key_base_len);
        e->d.type = sv.type;
        (void)snprintf(e->d.name, sizeof(e->d.name), "%s", sv.name);
        e->f = inode_read_start(tr, &c->b->prefix, sv.child_fileid);
        c->n++;
    }
    return 0;
}

static int rdp_page_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct rdp_page_ctx *c = arg;
    struct fdb_key_range r;
    struct fdb_key base;
    const FDBKeyValue *kvs = NULL;
    FDBFuture *f;
    int count = 0;
    int rc;
    uint32_t i;
    fdb_error_t err = 0;

    c->n = 0;
    c->more = false;
    fdb_key_dirent_seq_prefix(&base, &c->b->prefix, c->parent);
    (void)fdb_key_range_prefix(&r, &base);
    fdb_key_dirent_seq(&r.begin, &c->b->prefix, c->parent, c->after);
    fdb_key_u8(&r.begin, 0); /* strictly greater than the resume cookie */
    f = fdb_txn_get_range_start(tr, &r, (int)c->limit, false, false);
    err = fdb_txn_get_range_wait(f, &kvs, &count, &c->more);
    if (err != 0) {
        if (f != NULL) {
            fdb_future_destroy(f);
        }
        return (int)err;
    }
    rc = rdp_collect(tr, c, kvs, count, base.len);
    fdb_future_destroy(f);
    /* Wait for every inode read, even after an error, so no future is
     * leaked. */
    for (i = 0; i < c->n; i++) {
        struct rdp_entry *e = &c->page[i];
        fdb_error_t e2;

        e2 = inode_read_finish(e->f, &e->ino, &e->ino_valid);
        e->f = NULL;
        if (e2 != 0 && err == 0) {
            err = e2;
        }
    }
    if (rc != 0) {
        return rc;
    }
    if (err != 0) {
        return (int)err;
    }
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static enum mds_status fdb_ns_readdir_plus_from(struct mds_catalogue *cat, uint64_t parent,
                                                uint64_t start_after_cookie,
                                                uint32_t max_entries, struct mds_cat_txn *txn,
                                                mds_readdir_plus_cb cb, void *ctx)
{
    struct rdp_page_ctx c;
    uint32_t delivered = 0;
    enum mds_status st = MDS_OK;

    (void)txn;
    if (be_of(cat) == NULL || cb == NULL) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.b = be_of(cat);
    c.parent = parent;
    c.after = start_after_cookie;
    c.page = calloc(FDB_READDIR_PLUS_PAGE, sizeof(*c.page));
    if (c.page == NULL) {
        return MDS_ERR_NOMEM;
    }
    for (;;) {
        uint32_t i;

        c.limit = FDB_READDIR_PLUS_PAGE;
        if (max_entries > 0 && max_entries - delivered < c.limit) {
            c.limit = max_entries - delivered;
        }
        st = fdb_run_txn(c.b, FDB_TXN_READONLY, "ns_readdir_plus_from", rdp_page_body, &c);
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
    free(c.page);
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
    struct fdb_backend *b;
    enum dirent_op      op;
    uint64_t            parent;
    const char         *name;
    uint64_t            child;
    uint8_t             type;
    uint64_t            seq;
};

static int dirent_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct dirent_ctx *c = arg;
    struct fdb_dirent_val old;
    bool found = false;
    fdb_error_t err;

    err = fdb_dirent_read_finish(fdb_dirent_read_start(tr, &c->b->prefix, c->parent, c->name), &old,
                             &found);
    if (err != 0) {
        return (int)err;
    }
    if (c->op == DIRENT_OP_DEL) {
        if (!found) {
            *st_out = MDS_ERR_NOTFOUND;
            return FDB_BODY_DONE;
        }
        fdb_dirent_clear(tr, &c->b->prefix, c->parent, c->name, old.seq);
        *st_out = MDS_OK;
        return FDB_BODY_COMMIT;
    }
    if (found && c->op == DIRENT_OP_INSERT) {
        *st_out = MDS_ERR_EXISTS;
        return FDB_BODY_DONE;
    }
    if (found) {
        struct fdb_key k;

        /* Overwrite is a rebinding: the old cookie row goes. */
        fdb_key_dirent_seq(&k, &c->b->prefix, c->parent, old.seq);
        fdb_txn_clear(tr, &k);
    }
    {
        int rc = dirent_write(tr, &c->b->prefix, c->parent, c->name, c->child, c->type, c->seq);

        if (rc != 0) {
            return rc;
        }
    }
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static enum mds_status dirent_run(const struct mds_catalogue *cat, enum dirent_op op,
                                  uint64_t parent, const char *name, uint64_t child,
                                  uint8_t type)
{
    struct dirent_ctx c;

    if (be_of(cat) == NULL || !name_ok(name)) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.b = be_of(cat);
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
        st = fdb_backend_alloc_id(c.b, FDB_META_COOKIE_SEQ, &c.seq);
        if (st != MDS_OK) {
            return st;
        }
    }
    return fdb_run_txn(c.b, FDB_TXN_MUTATING, "dirent_op", dirent_body, &c);
}

static enum mds_status fdb_dirent_put(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                      uint64_t parent, const char *name, uint64_t child_fileid,
                                      uint8_t child_type)
{
    (void)txn;
    return dirent_run(cat, DIRENT_OP_PUT, parent, name, child_fileid, child_type);
}

static enum mds_status fdb_dirent_insert(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                         uint64_t parent, const char *name,
                                         uint64_t child_fileid, uint8_t child_type)
{
    (void)txn;
    return dirent_run(cat, DIRENT_OP_INSERT, parent, name, child_fileid, child_type);
}

static enum mds_status fdb_dirent_del(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                      uint64_t parent, const char *name)
{
    (void)txn;
    return dirent_run(cat, DIRENT_OP_DEL, parent, name, 0, 0);
}

/* -----------------------------------------------------------------------
 * Raw inode rows: inode_put / inode_del
 * ----------------------------------------------------------------------- */

struct inode_ctx {
    struct fdb_backend     *b;
    const struct mds_inode *inode;
    uint64_t                fileid;
};

static int inode_put_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct inode_ctx *c = arg;
    int rc = catalogue_fdb_inode_write(tr, &c->b->prefix, c->inode);

    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static enum mds_status fdb_inode_put(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                     const struct mds_inode *inode)
{
    struct inode_ctx c;

    (void)txn;
    if (be_of(cat) == NULL || inode == NULL) {
        return MDS_ERR_INVAL;
    }
    c.b = be_of(cat);
    c.inode = inode;
    c.fileid = inode->fileid;
    return fdb_run_txn(c.b, FDB_TXN_MUTATING, "inode_put", inode_put_body, &c);
}

static int inode_del_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct inode_ctx *c = arg;
    struct mds_inode ino;
    struct fdb_key_range r;
    bool found = false;
    fdb_error_t err;

    err = fdb_blob_read_finish(fdb_blob_read_start(tr, &c->b->prefix, c->fileid), &ino, &found);
    if (err != 0) {
        return (int)err;
    }
    if (!found) {
        *st_out = MDS_ERR_NOTFOUND;
        return FDB_BODY_DONE;
    }
    fdb_key_inode_prefix(&r.begin, &c->b->prefix, c->fileid);
    if (!fdb_key_range_prefix(&r, &r.begin)) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    fdb_txn_clear_range(tr, &r);
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static enum mds_status fdb_inode_del(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                     uint64_t fileid)
{
    struct inode_ctx c;

    (void)txn;
    if (be_of(cat) == NULL) {
        return MDS_ERR_INVAL;
    }
    c.b = be_of(cat);
    c.inode = NULL;
    c.fileid = fileid;
    return fdb_run_txn(c.b, FDB_TXN_MUTATING, "inode_del", inode_del_body, &c);
}

/* -----------------------------------------------------------------------
 * alloc_fileid / ns_parent_touch
 * ----------------------------------------------------------------------- */

static enum mds_status fdb_alloc_fileid(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                        uint64_t *fileid)
{
    (void)txn;
    if (be_of(cat) == NULL || fileid == NULL) {
        return MDS_ERR_INVAL;
    }
    return fdb_backend_alloc_id(be_of(cat), FDB_META_FILEID, fileid);
}

struct touch_ctx {
    struct fdb_backend *b;
    uint64_t            fileid;
    uint64_t            delta;
    struct timespec     stamp;
};

static int touch_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct touch_ctx *c = arg;
    struct mds_inode ino;
    struct fdb_key k;
    uint64_t ns;
    bool found = false;
    fdb_error_t err;

    /* The blob read keeps a blind ADD from materialising counters for a
     * directory that no longer exists; a missing row is MDS_OK. */
    err = fdb_blob_read_finish(fdb_blob_read_start(tr, &c->b->prefix, c->fileid), &ino, &found);
    if (err != 0) {
        return (int)err;
    }
    *st_out = MDS_OK;
    if (!found || !fdb_inode_is_dir(&ino)) {
        return FDB_BODY_DONE;
    }
    ns = (uint64_t)fdb_ts_to_ns(c->stamp);
    fdb_key_inode(&k, &c->b->prefix, c->fileid, FDB_INODE_PART_CHANGE);
    fdb_txn_add_le64(tr, &k, (int64_t)c->delta);
    fdb_key_inode(&k, &c->b->prefix, c->fileid, FDB_INODE_PART_MTIME);
    fdb_txn_set_le64(tr, &k, ns);
    fdb_key_inode(&k, &c->b->prefix, c->fileid, FDB_INODE_PART_CTIME);
    fdb_txn_set_le64(tr, &k, ns);
    return FDB_BODY_COMMIT;
}

static enum mds_status fdb_ns_parent_touch(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                           uint64_t fileid, uint64_t change_delta,
                                           struct timespec stamp)
{
    struct touch_ctx c;

    (void)txn;
    if (be_of(cat) == NULL || change_delta == 0 || change_delta > (uint64_t)INT64_MAX) {
        return MDS_ERR_INVAL;
    }
    c.b = be_of(cat);
    c.fileid = fileid;
    c.delta = change_delta;
    c.stamp = stamp;
    return fdb_run_txn(c.b, FDB_TXN_MUTATING, "ns_parent_touch", touch_body, &c);
}

/* -----------------------------------------------------------------------
 * Registration
 * ----------------------------------------------------------------------- */

void catalogue_fdb_ns_register(struct mds_authority_ops *ops)
{
    if (ops == NULL) {
        return;
    }
    ops->ns_create             = fdb_ns_create;
    ops->ns_create_wide        = fdb_ns_create_wide;
    ops->ns_remove             = fdb_ns_remove;
    ops->ns_remove_known       = fdb_ns_remove_known;
    ops->ns_remove_known_gc    = fdb_ns_remove_known_gc;
    ops->ns_parent_touch       = fdb_ns_parent_touch;
    ops->ns_rename             = fdb_ns_rename;
    ops->ns_rename_flags       = fdb_ns_rename_flags;
    ops->ns_link               = fdb_ns_link;
    ops->ns_lookup             = fdb_ns_lookup;
    ops->ns_getattr            = fdb_ns_getattr;
    ops->ns_setattr            = fdb_ns_setattr;
    ops->ns_readdir            = fdb_ns_readdir;
    ops->dirent_name_for_child = fdb_dirent_name_for_child;
    ops->ns_readdir_plus_from  = fdb_ns_readdir_plus_from;
    ops->ns_nlink_adjust       = fdb_ns_nlink_adjust;
    ops->alloc_fileid          = fdb_alloc_fileid;
    ops->inode_put             = fdb_inode_put;
    ops->inode_del             = fdb_inode_del;
    ops->dirent_put            = fdb_dirent_put;
    ops->dirent_insert         = fdb_dirent_insert;
    ops->dirent_del            = fdb_dirent_del;
}
