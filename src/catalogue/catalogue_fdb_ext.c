/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * catalogue_fdb_ext.c -- FoundationDB backend: extended authority slots.
 *
 * The authority slots outside the namespace: inline data, extended
 * attributes, stripe maps, the DS registry and provisioning, quota,
 * the GC queue, the async-REMOVE delete manifest, shard routing,
 * cross-shard dirents and link anchors.  Same rules as
 * catalogue_fdb_ns.c: every slot is ONE fdb_run_txn (fdb_txn.h), the
 * mds_cat_txn token is never enlisted (C6), reads that do not depend
 * on each other are issued as parallel futures and waited on once,
 * every mutating body relies on the runner's witness and adds none of
 * its own, and an enumerating slot materialises a bounded page inside
 * a transaction and delivers it after fdb_run_txn returned (C1/C2).
 * Statuses follow the memdb reference implementation
 * (catalogue_memdb.c); the deviations are listed at the end.
 *
 * Transaction shapes (R = point read, RR = range read, W = set/clear,
 * A = atomic ADD, "page" = one read-only transaction per page; every
 * mutating transaction adds the witness set + READ conflict range and
 * commits once):
 *   inline_get                    R (copy truncated to the caller's buffer)
 *   inline_put                    W (blind)
 *   inline_del                    R presence -> W clear
 *   xattr_get / xattr_exists      R
 *   xattr_put                     R inode blob; W xattr, inode touch
 *                                 (directory: A change, W ctime side key;
 *                                 file: W blob with ctime/change bumped)
 *   xattr_del                     R xattr presence || R inode blob; W clear,
 *                                 inode touch
 *   xattr_list                    page: RR names (1 MiB byte target)
 *   stripe_map_get                R hdr || RR entries, one wave (C3)
 *   stripe_map_put                W clear entry range, W hdr, W entries
 *   stripe_map_del                W clear hdr + entry range (blind)
 *   stripe_map_scan               page: RR hdrs -> N parallel RR entries
 *   ds_get / ds_put / ds_del      R / W / R presence -> W clear
 *   ds_list                       RR (limit MDS_MAX_DS_NODES)
 *   ds_provision_get/put/del      R / W / R presence -> W clear
 *   quota_rule_get / _put         R / W
 *   quota_usage_get / _put        R / W (absolute upsert)
 *   gc_enqueue                    seq from the GC_SEQ batch allocator; W row,
 *                                 W GC_BY_OWNER key (blind)
 *   gc_peek / gc_peek_batch       mds_id != 0: RR GC_BY_OWNER + mds_id || RR
 *                                 GC_BY_OWNER + 0 (cap keys each) -> merge
 *                                 by seq -> R || ... || R rows (waves of
 *                                 FDB_EXT_QUEUE_PAGE); mds_id == 0: page: RR
 *                                 GC rows
 *   gc_dequeue                    R row -> W clear row + GC_BY_OWNER key
 *   gc_count                      mds_id != 0: page: RR GC_BY_OWNER + mds_id,
 *                                 then + 0, count keys; mds_id == 0: page:
 *                                 RR GC rows, count
 *   remove_pending_enqueue        seq from the REMOVE_SEQ allocator; W
 *   remove_pending_enqueue_unlink R dirent || R child blob; W child blob
 *                                 (DELETE_PENDING), W clear dirent +
 *                                 dirent_seq, W manifest row
 *   remove_pending_peek_batch     page: RR rows, claim filter
 *   remove_pending_claim          R -> W (claim decided and written in
 *                                 the same transaction, C3)
 *   remove_pending_complete       W clear (blind, idempotent)
 *   remove_pending_bump_retry     R -> W
 *   remove_pending_count          page: RR rows, count
 *   remove_pending_scan_all       page: RR rows
 *   shard_fileid_get/put/del      R / W / R presence -> W clear
 *   ext_dirent_get/put/del        R / W / R presence -> W clear
 *   link_anchor_put / _del        W / R presence -> W clear
 *
 * GC visibility.  gc_peek, gc_peek_batch and gc_count see the rows
 * whose owner_mds_id is 0 (legacy) or this MDS's id, exactly like the
 * RonDB backend (every row when this MDS's id is 0); gc_enqueue and
 * the fused remove (catalogue_fdb_ns.c) stamp the owner.  ds_gc.c
 * drops a peeked row whose ds_id is not mounted on the draining MDS,
 * so on a store shared by several daemons an unfiltered peek would let
 * a peer drain -- and drop -- rows it cannot service.  A dead MDS's
 * rows therefore wait for its restart under the same id; reaping
 * foreign rows is a recovery-time job (an epoch-aware sweep), not a
 * peek-time one.  The filter is structural, not a scan: every row has
 * a GC_BY_OWNER key (fdb_keys.h) written and cleared with it, and a
 * filtered peek pages the two owner prefixes it may drain (its own id
 * and 0), merges them by seq and point-reads the rows -- O(rows it may
 * drain) whatever a dead peer's backlog is.  A walk of the GC table
 * with a row filter cannot give that: the peer's rows are older, hence
 * sorted first, and any examine cap that keeps a peek prompt would
 * stop before this MDS's rows for ever.  FDB_EXT_EXAMINE_CAP(cap)
 * still bounds the walks in which every row is a candidate (the id-0
 * peek and the manifest peek).
 *
 * Counts.  gc_count and remove_pending_count are exact paged counts
 * (RonDB semantics) that saturate at FDB_EXT_COUNT_MAX examined keys so
 * the metrics gauge they feed stays bounded in time; a filtered
 * gc_count pages the two index prefixes, the others page the tables.
 * A maintained META counter is not an option: catalogue_fdb_ns.c's
 * fused remove writes GC rows directly and would drift it.
 *
 * Sequences.  GC and manifest sequences come from the META batch
 * allocators (fdb_backend_alloc_id), so their order is the order the
 * thread-local batches were reserved: strictly increasing within one
 * thread, not a global enqueue-time order across threads.
 *
 * Deviations from memdb, all deliberate: stripe_map_put refuses a zero
 * stripe_count, mirror_count or stripe_unit (MDS_ERR_INVAL; the header
 * codec cannot represent it and no caller passes it); ds_put and
 * ds_provision_put refuse ds_id >= MDS_MAX_DS_NODES (MDS_ERR_INVAL) so
 * ds_list is bounded by construction; gc rows carry this MDS's owner
 * id and the owner filter above applies.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "catalogue_fdb.h"
#include "catalogue_fdb_internal.h"
#include "catalogue_internal.h"
#include "fdb_codec.h"
#include "fdb_keys.h"
#include "fdb_txn.h"
#include "quota.h"

/* Names per xattr_list page and the byte target of one page's range
 * read: the values (up to MDS_XATTR_VAL_MAX each) ride along with the
 * keys, so the byte target, not the name count, bounds the page. */
#define FDB_EXT_XATTR_PAGE        256U
#define FDB_EXT_XATTR_PAGE_BYTES  (1U << 20)

/* Entry rows of one stripe map (stripe-major ordinals). */
#define FDB_EXT_STRIPE_ENT_MAX    ((uint32_t)MDS_MAX_STRIPES * (uint32_t)MDS_MAX_MIRRORS)

/* stripe_map_scan: headers read per page and the entry budget that
 * closes a page early (the first file of a page is always taken). */
#define FDB_EXT_SCAN_HDR_PAGE     64U
#define FDB_EXT_SCAN_ENT_BUDGET   8192U

/* GC / manifest queues: rows examined per page, the examine cap of a
 * filtered peek and the saturation point of the counting slots. */
#define FDB_EXT_QUEUE_PAGE        1024U
#define FDB_EXT_COUNT_PAGE        4096U
#define FDB_EXT_COUNT_MAX         (1U << 20)
#define FDB_EXT_EXAMINE_CAP(cap)  ((uint64_t)(cap) * 256U + 4096U)

/* remove_pending_scan_all rows materialised per page. */
#define FDB_EXT_RP_SCAN_PAGE      256U

/* FoundationDB hard limits the slots rely on (bytes). */
#define FDB_EXT_VALUE_LIMIT       100000U
#define FDB_EXT_TXN_LIMIT         10000000U

/* Body result for a failed heap allocation; translated to MDS_ERR_NOMEM
 * before it reaches the runner (which only knows FDB_BODY_* and
 * fdb_error_t values). */
#define FDB_EXT_RC_NOMEM          (-2)

_Static_assert(MDS_XATTR_NAME_MAX <= MDS_MAX_NAME, "xattr names must fit fdb_key_name");
_Static_assert(MDS_XATTR_VAL_MAX < FDB_EXT_VALUE_LIMIT &&
               MDS_INLINE_DATA_MAX < FDB_EXT_VALUE_LIMIT,
               "xattr and inline values must stay under the 100 KB value limit");
_Static_assert((uint64_t)FDB_EXT_STRIPE_ENT_MAX * (FDB_STRIPE_ENT_ENC_MAX + FDB_KEY_MAX) +
               FDB_STRIPE_HDR_ENC_SIZE + FDB_KEY_MAX < FDB_EXT_TXN_LIMIT,
               "a full stripe map must fit one transaction");
_Static_assert(FDB_EXT_COUNT_MAX <= UINT32_MAX, "counts are reported as uint32_t");

/* -----------------------------------------------------------------------
 * Small helpers
 * ----------------------------------------------------------------------- */

static struct fdb_backend *be_of(const struct mds_catalogue *cat)
{
    return cat != NULL ? cat->backend_private : NULL;
}

/* True when @p name is 1..@p max bytes long. */
static bool name_len_ok(const char *name, size_t max)
{
    size_t n;

    if (name == NULL) {
        return false;
    }
    n = strnlen(name, max + 1U);
    return n > 0 && n <= max;
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

static uint64_t realtime_ns(void)
{
    struct timespec ts = now_ts();

    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

/* Range read of at most @p limit rows or about @p target_bytes bytes
 * of [r->begin, r->end); *more reports either bound being hit. */
static FDBFuture *range_start_bytes(FDBTransaction *tr, const struct fdb_key_range *r,
                                    int limit, int target_bytes)
{
    if (!fdb_key_ok(&r->begin) || !fdb_key_ok(&r->end) || limit <= 0) {
        return NULL;
    }
    return fdb_transaction_get_range(tr,
                                     FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(r->begin.buf,
                                                                       (int)r->begin.len),
                                     FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(r->end.buf,
                                                                       (int)r->end.len),
                                     limit, target_bytes, FDB_STREAMING_MODE_EXACT, 1, 0, 0);
}

/* Finish a point read started with fdb_txn_get_start as a presence test
 * (the value is not copied). */
static fdb_error_t presence_finish(FDBFuture *f, bool *present)
{
    fdb_bool_t p = 0;
    const uint8_t *val = NULL;
    int vlen = 0;
    fdb_error_t err;

    *present = false;
    if (f == NULL) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    err = fdb_txn_wait(f);
    if (err == 0) {
        err = fdb_future_get_value(f, &p, &val, &vlen);
    }
    fdb_future_destroy(f);
    if (err == 0) {
        *present = (p != 0);
    }
    return err;
}

static fdb_error_t point_present(FDBTransaction *tr, const struct fdb_key *k, bool *present)
{
    return presence_finish(fdb_txn_get_start(tr, k, false), present);
}

/* -----------------------------------------------------------------------
 * Generic one-key bodies: blind set, bounded get, clear-if-present and
 * blind clear.  Most slots of this unit are one of these.
 * ----------------------------------------------------------------------- */

struct kv_ctx {
    struct fdb_key  key;
    const uint8_t  *val;   /**< set: value bytes (caller-owned, immutable). */
    size_t          len;   /**< set: value length / get: length read. */
    uint8_t        *buf;   /**< get: destination. */
    size_t          cap;   /**< get: capacity of buf. */
    bool            found; /**< get: presence. */
};

static int body_set(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct kv_ctx *c = arg;

    fdb_txn_set(tr, &c->key, c->val, c->len);
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static int body_get(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct kv_ctx *c = arg;
    fdb_error_t err;

    err = fdb_txn_get(tr, &c->key, false, c->buf, c->cap, &c->len, &c->found);
    if (err != 0) {
        return (int)err;
    }
    *st_out = c->found ? MDS_OK : MDS_ERR_NOTFOUND;
    return FDB_BODY_COMMIT;
}

static int body_clear_present(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct kv_ctx *c = arg;
    bool present = false;
    fdb_error_t err;

    err = point_present(tr, &c->key, &present);
    if (err != 0) {
        return (int)err;
    }
    if (!present) {
        *st_out = MDS_ERR_NOTFOUND;
        return FDB_BODY_DONE;
    }
    fdb_txn_clear(tr, &c->key);
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static int body_clear_blind(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct kv_ctx *c = arg;

    fdb_txn_clear(tr, &c->key);
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static enum mds_status run_set(struct fdb_backend *b, const char *op, const struct fdb_key *k,
                               const uint8_t *val, size_t len)
{
    struct kv_ctx c;

    if (!fdb_key_ok(k)) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.key = *k;
    c.val = val;
    c.len = len;
    return fdb_run_txn(b, FDB_TXN_MUTATING, op, body_set, &c);
}

/* Read @p k into @p buf; MDS_OK / MDS_ERR_NOTFOUND / the runner's status.
 * A value longer than @p cap is MDS_ERR_IO (corrupt row). */
static enum mds_status run_get(struct fdb_backend *b, const char *op, const struct fdb_key *k,
                               uint8_t *buf, size_t cap, size_t *len)
{
    struct kv_ctx c;
    enum mds_status st;

    *len = 0;
    if (!fdb_key_ok(k)) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.key = *k;
    c.buf = buf;
    c.cap = cap;
    st = fdb_run_txn(b, FDB_TXN_READONLY, op, body_get, &c);
    if (st == MDS_OK) {
        *len = c.len;
    }
    return st;
}

static enum mds_status run_clear(struct fdb_backend *b, const char *op, const struct fdb_key *k,
                                 bool require_present)
{
    struct kv_ctx c;

    if (!fdb_key_ok(k)) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.key = *k;
    return fdb_run_txn(b, FDB_TXN_MUTATING, op,
                       require_present ? body_clear_present : body_clear_blind, &c);
}

/* -----------------------------------------------------------------------
 * Inode touch (the blob and dirent primitives it builds on are shared
 * with the namespace unit: catalogue_fdb_internal.h)
 * ----------------------------------------------------------------------- */

/* change += 1, ctime = now: side keys for a directory (blind), the blob
 * for a file.  The xattr mutations use it like memdb's inode touch. */
static int inode_touch(FDBTransaction *tr, const struct fdb_key_prefix *p, struct mds_inode *ino,
                       struct timespec now)
{
    struct fdb_key k;

    if (fdb_inode_is_dir(ino)) {
        fdb_key_inode(&k, p, ino->fileid, FDB_INODE_PART_CHANGE);
        fdb_txn_add_le64(tr, &k, 1);
        fdb_key_inode(&k, p, ino->fileid, FDB_INODE_PART_CTIME);
        fdb_txn_set_le64(tr, &k, (uint64_t)fdb_ts_to_ns(now));
        return 0;
    }
    ino->ctime = now;
    ino->change++;
    return fdb_blob_write(tr, p, ino);
}

/* -----------------------------------------------------------------------
 * Inline data
 * ----------------------------------------------------------------------- */

struct inline_get_ctx {
    struct fdb_key key;
    uint8_t       *buf;
    size_t         cap;
    size_t         copied;
};

/* The caller's buffer may be shorter than the value: copy what fits
 * (memdb semantics; no caller needs the full length). */
static int inline_get_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct inline_get_ctx *c = arg;
    FDBFuture *f = fdb_txn_get_start(tr, &c->key, false);
    fdb_bool_t present = 0;
    const uint8_t *val = NULL;
    int vlen = 0;
    fdb_error_t err;

    c->copied = 0;
    if (f == NULL) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    err = fdb_txn_wait(f);
    if (err == 0) {
        err = fdb_future_get_value(f, &present, &val, &vlen);
    }
    if (err != 0 || vlen < 0) {
        fdb_future_destroy(f);
        return err != 0 ? (int)err : FDB_ERR_PLATFORM_ERROR;
    }
    if (present != 0) {
        size_t n = (size_t)vlen;

        if (n > c->cap) {
            n = c->cap;
        }
        if (n > 0) {
            memcpy(c->buf, val, n);
        }
        c->copied = n;
    }
    fdb_future_destroy(f);
    *st_out = (present != 0) ? MDS_OK : MDS_ERR_NOTFOUND;
    return FDB_BODY_COMMIT;
}

static enum mds_status fdb_inline_get(struct mds_catalogue *cat, uint64_t fileid, void *buf,
                                      uint32_t buflen, uint32_t *outlen)
{
    struct fdb_backend *b = be_of(cat);
    struct inline_get_ctx c;
    enum mds_status st;

    if (b == NULL || outlen == NULL || (buflen > 0 && buf == NULL)) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    fdb_key_inline(&c.key, &b->prefix, fileid);
    c.buf = buf;
    c.cap = buflen;
    st = fdb_run_txn(b, FDB_TXN_READONLY, "inline_get", inline_get_body, &c);
    if (st == MDS_OK) {
        *outlen = (uint32_t)c.copied;
    }
    return st;
}

static enum mds_status fdb_inline_put(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                      uint64_t fileid, const void *buf, uint32_t len)
{
    struct fdb_backend *b = be_of(cat);
    struct fdb_key k;

    (void)txn;
    if (b == NULL || len > MDS_INLINE_DATA_MAX || (len > 0 && buf == NULL)) {
        return MDS_ERR_INVAL;
    }
    fdb_key_inline(&k, &b->prefix, fileid);
    return run_set(b, "inline_put", &k, buf, len);
}

static enum mds_status fdb_inline_del(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                      uint64_t fileid)
{
    struct fdb_backend *b = be_of(cat);
    struct fdb_key k;

    (void)txn;
    if (b == NULL) {
        return MDS_ERR_INVAL;
    }
    fdb_key_inline(&k, &b->prefix, fileid);
    return run_clear(b, "inline_del", &k, true);
}

/* -----------------------------------------------------------------------
 * Extended attributes
 * ----------------------------------------------------------------------- */

struct xattr_get_ctx {
    struct fdb_key key;
    uint8_t       *val;    /**< Heap copy handed to the caller. */
    uint32_t       vallen;
};

static int xattr_get_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct xattr_get_ctx *c = arg;
    FDBFuture *f;
    fdb_bool_t present = 0;
    const uint8_t *val = NULL;
    int vlen = 0;
    uint8_t *copy;
    fdb_error_t err;

    /* A re-run attempt starts clean. */
    free(c->val);
    c->val = NULL;
    c->vallen = 0;
    f = fdb_txn_get_start(tr, &c->key, false);
    if (f == NULL) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    err = fdb_txn_wait(f);
    if (err == 0) {
        err = fdb_future_get_value(f, &present, &val, &vlen);
    }
    if (err != 0) {
        fdb_future_destroy(f);
        return (int)err;
    }
    if (present == 0) {
        fdb_future_destroy(f);
        *st_out = MDS_ERR_NOTFOUND;
        return FDB_BODY_DONE;
    }
    if (vlen < 0 || (size_t)vlen > MDS_XATTR_VAL_MAX) {
        fdb_future_destroy(f);
        return FDB_ERR_PLATFORM_ERROR; /* corrupt row */
    }
    /* A zero-length value still hands back a freeable buffer so the
     * caller's contract (free *val) holds uniformly, as on memdb. */
    copy = malloc(vlen > 0 ? (size_t)vlen : 1U);
    if (copy == NULL) {
        fdb_future_destroy(f);
        *st_out = MDS_ERR_NOMEM;
        return FDB_BODY_DONE;
    }
    if (vlen > 0) {
        memcpy(copy, val, (size_t)vlen);
    }
    fdb_future_destroy(f);
    c->val = copy;
    c->vallen = (uint32_t)vlen;
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static enum mds_status fdb_xattr_get(struct mds_catalogue *cat, uint64_t fileid,
                                     const char *name, void **val, uint32_t *vallen)
{
    struct fdb_backend *b = be_of(cat);
    struct xattr_get_ctx c;
    enum mds_status st;

    if (b == NULL || name == NULL || val == NULL || vallen == NULL) {
        return MDS_ERR_INVAL;
    }
    if (!name_len_ok(name, MDS_XATTR_NAME_MAX)) {
        return MDS_ERR_NOTFOUND; /* no such name can exist */
    }
    memset(&c, 0, sizeof(c));
    fdb_key_xattr(&c.key, &b->prefix, fileid, name);
    if (!fdb_key_ok(&c.key)) {
        return MDS_ERR_INVAL;
    }
    st = fdb_run_txn(b, FDB_TXN_READONLY, "xattr_get", xattr_get_body, &c);
    if (st != MDS_OK) {
        free(c.val);
        return st;
    }
    *val = c.val;
    *vallen = c.vallen;
    return MDS_OK;
}

struct xattr_mut_ctx {
    struct fdb_backend *b;
    uint64_t            fileid;
    struct fdb_key      key;
    const uint8_t      *val;
    uint32_t            vallen;
    bool                del;
};

/* put: R blob; W xattr, touch.  del: R xattr presence || R blob; W
 * clear, touch.  A missing inode is tolerated (the xattr row is still
 * written / cleared), as on memdb. */
static int xattr_mut_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct xattr_mut_ctx *c = arg;
    FDBFuture *f_blob;
    FDBFuture *f_x = NULL;
    struct mds_inode ino;
    bool ino_found = false;
    bool x_present = true;
    fdb_error_t err;

    f_blob = fdb_blob_read_start(tr, &c->b->prefix, c->fileid);
    if (c->del) {
        f_x = fdb_txn_get_start(tr, &c->key, false);
    }
    err = fdb_blob_read_finish(f_blob, &ino, &ino_found);
    if (f_x != NULL) {
        fdb_error_t err2 = presence_finish(f_x, &x_present);

        if (err == 0) {
            err = err2;
        }
    }
    if (err != 0) {
        return (int)err;
    }
    if (c->del) {
        if (!x_present) {
            *st_out = MDS_ERR_NOTFOUND;
            return FDB_BODY_DONE;
        }
        fdb_txn_clear(tr, &c->key);
    } else {
        fdb_txn_set(tr, &c->key, c->val, c->vallen);
    }
    if (ino_found) {
        int rc = inode_touch(tr, &c->b->prefix, &ino, now_ts());

        if (rc != 0) {
            return rc;
        }
    }
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static enum mds_status fdb_xattr_put(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                     uint64_t fileid, const char *name, const void *val,
                                     uint32_t vallen)
{
    struct fdb_backend *b = be_of(cat);
    struct xattr_mut_ctx c;

    (void)txn;
    if (b == NULL || !name_len_ok(name, MDS_XATTR_NAME_MAX) || vallen > MDS_XATTR_VAL_MAX ||
        (vallen > 0 && val == NULL)) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.b = b;
    c.fileid = fileid;
    fdb_key_xattr(&c.key, &b->prefix, fileid, name);
    if (!fdb_key_ok(&c.key)) {
        return MDS_ERR_INVAL;
    }
    c.val = val;
    c.vallen = vallen;
    return fdb_run_txn(b, FDB_TXN_MUTATING, "xattr_put", xattr_mut_body, &c);
}

static enum mds_status fdb_xattr_del(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                     uint64_t fileid, const char *name)
{
    struct fdb_backend *b = be_of(cat);
    struct xattr_mut_ctx c;

    (void)txn;
    if (b == NULL || name == NULL) {
        return MDS_ERR_INVAL;
    }
    if (!name_len_ok(name, MDS_XATTR_NAME_MAX)) {
        return MDS_ERR_NOTFOUND;
    }
    memset(&c, 0, sizeof(c));
    c.b = b;
    c.fileid = fileid;
    fdb_key_xattr(&c.key, &b->prefix, fileid, name);
    if (!fdb_key_ok(&c.key)) {
        return MDS_ERR_INVAL;
    }
    c.del = true;
    return fdb_run_txn(b, FDB_TXN_MUTATING, "xattr_del", xattr_mut_body, &c);
}

static int xattr_exists_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct kv_ctx *c = arg;
    bool present = false;
    fdb_error_t err;

    err = point_present(tr, &c->key, &present);
    if (err != 0) {
        return (int)err;
    }
    *st_out = present ? MDS_OK : MDS_ERR_NOTFOUND;
    return FDB_BODY_COMMIT;
}

static enum mds_status fdb_xattr_exists(struct mds_catalogue *cat, uint64_t fileid,
                                        const char *name)
{
    struct fdb_backend *b = be_of(cat);
    struct kv_ctx c;

    if (b == NULL || name == NULL) {
        return MDS_ERR_INVAL;
    }
    if (!name_len_ok(name, MDS_XATTR_NAME_MAX)) {
        return MDS_ERR_NOTFOUND;
    }
    memset(&c, 0, sizeof(c));
    fdb_key_xattr(&c.key, &b->prefix, fileid, name);
    if (!fdb_key_ok(&c.key)) {
        return MDS_ERR_INVAL;
    }
    return fdb_run_txn(b, FDB_TXN_READONLY, "xattr_exists", xattr_exists_body, &c);
}

struct xattr_list_ctx {
    struct fdb_backend *b;
    uint64_t            fileid;
    bool                have_after;
    char                after[MDS_XATTR_NAME_MAX + 1]; /**< Last name delivered. */
    char              (*names)[MDS_XATTR_NAME_MAX + 1];
    uint32_t            n;
    bool                more;
};

static int xattr_list_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct xattr_list_ctx *c = arg;
    struct fdb_key base;
    struct fdb_key_range r;
    const FDBKeyValue *kvs = NULL;
    FDBFuture *f;
    int count = 0;
    int i;
    fdb_error_t err;

    c->n = 0;
    c->more = false;
    fdb_key_xattr_prefix(&base, &c->b->prefix, c->fileid);
    if (!fdb_key_range_prefix(&r, &base)) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    if (c->have_after) {
        fdb_key_xattr(&r.begin, &c->b->prefix, c->fileid, c->after);
        fdb_key_u8(&r.begin, 0); /* strictly after the last delivered name */
    }
    f = range_start_bytes(tr, &r, (int)FDB_EXT_XATTR_PAGE, (int)FDB_EXT_XATTR_PAGE_BYTES);
    err = fdb_txn_get_range_wait(f, &kvs, &count, &c->more);
    if (err != 0) {
        if (f != NULL) {
            fdb_future_destroy(f);
        }
        return (int)err;
    }
    for (i = 0; i < count && c->n < FDB_EXT_XATTR_PAGE; i++) {
        FDBKeyValue kv;
        int nlen;

        fdb_kv_at(kvs, i, &kv);
        nlen = kv.key_length - (int)base.len;
        if (nlen <= 0 || nlen > (int)MDS_XATTR_NAME_MAX) {
            fdb_future_destroy(f);
            return FDB_ERR_PLATFORM_ERROR; /* corrupt key */
        }
        memcpy(c->names[c->n], kv.key + base.len, (size_t)nlen);
        c->names[c->n][nlen] = '\0';
        c->n++;
    }
    fdb_future_destroy(f);
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static enum mds_status fdb_xattr_list(struct mds_catalogue *cat, uint64_t fileid,
                                      mds_xattr_list_cb cb, void *ctx)
{
    struct fdb_backend *b = be_of(cat);
    struct xattr_list_ctx c;
    enum mds_status st = MDS_OK;

    if (b == NULL || cb == NULL) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.b = b;
    c.fileid = fileid;
    c.names = calloc(FDB_EXT_XATTR_PAGE, sizeof(*c.names));
    if (c.names == NULL) {
        return MDS_ERR_NOMEM;
    }
    for (;;) {
        uint32_t i;

        st = fdb_run_txn(b, FDB_TXN_READONLY, "xattr_list", xattr_list_body, &c);
        if (st != MDS_OK) {
            break;
        }
        for (i = 0; i < c.n; i++) {
            if (cb(c.names[i], strlen(c.names[i]), ctx) != 0) {
                goto out;
            }
        }
        if (!c.more || c.n == 0) {
            break;
        }
        memcpy(c.after, c.names[c.n - 1], sizeof(c.after));
        c.have_after = true;
    }
out:
    free(c.names);
    return st;
}

/* -----------------------------------------------------------------------
 * Stripe maps: STRIPE_HDR + fileid -> header, STRIPE_ENT + fileid +
 * be32 ordinal -> one entry per (stripe, mirror) in stripe-major order,
 * the rows catalogue_fdb_ns.c writes for ns_create_wide.
 * ----------------------------------------------------------------------- */

/* Start the entries range read of @p fileid: sc * mc rows expected, one
 * more requested so an extra row is detected as corruption. */
static FDBFuture *stripe_entries_start(FDBTransaction *tr, const struct fdb_key_prefix *p,
                                       uint64_t fileid, uint32_t expected)
{
    struct fdb_key_range r;

    fdb_key_stripe_ent_prefix(&r.begin, p, fileid);
    if (!fdb_key_range_prefix(&r, &r.begin)) {
        return NULL;
    }
    return fdb_txn_get_range_start(tr, &r, (int)expected + 1, false, false);
}

/*
 * Wait for an entries read and decode exactly hdr->stripe_count *
 * hdr->mirror_count rows with contiguous ordinals into a fresh heap
 * array (*out).  Returns 0, an fdb_error_t (FDB_ERR_PLATFORM_ERROR for
 * a map whose rows disagree with its header) or FDB_EXT_RC_NOMEM.
 */
static int stripe_entries_finish(FDBFuture *f, const struct fdb_stripe_hdr_val *hdr,
                                 uint32_t key_base_len, struct mds_ds_map_entry **out)
{
    const FDBKeyValue *kvs = NULL;
    struct mds_ds_map_entry *entries;
    uint32_t n = hdr->stripe_count * hdr->mirror_count;
    bool more = false;
    int count = 0;
    int i;
    fdb_error_t err;

    *out = NULL;
    err = fdb_txn_get_range_wait(f, &kvs, &count, &more);
    if (err != 0) {
        if (f != NULL) {
            fdb_future_destroy(f);
        }
        return (int)err;
    }
    /* The read asked for n + 1 rows, so an extra row shows up in the
     * count itself (FDB's `more` is set whenever the row limit is hit,
     * even with nothing beyond it, and is not a witness here). */
    if (count != (int)n) {
        fdb_future_destroy(f);
        return FDB_ERR_PLATFORM_ERROR; /* row count disagrees with the header */
    }
    entries = malloc((size_t)n * sizeof(*entries));
    if (entries == NULL) {
        fdb_future_destroy(f);
        return FDB_EXT_RC_NOMEM;
    }
    for (i = 0; i < count; i++) {
        FDBKeyValue kv;

        fdb_kv_at(kvs, i, &kv);
        if (kv.key_length != (int)(key_base_len + FDB_KEY_BE32_LEN) ||
            fdb_get_u32(kv.key + key_base_len) != (uint32_t)i ||
            !fdb_stripe_ent_decode(kv.value, (size_t)kv.value_length, &entries[i])) {
            fdb_future_destroy(f);
            free(entries);
            return FDB_ERR_PLATFORM_ERROR; /* ordinal gap or corrupt row */
        }
    }
    fdb_future_destroy(f);
    *out = entries;
    return 0;
}

/* Write a stripe map: clear the entry range (a narrower re-put must not
 * leave stale ordinals), then header + stripe-major entries. */
static int stripe_rows_write(FDBTransaction *tr, const struct fdb_key_prefix *p,
                             uint64_t fileid, const struct fdb_stripe_hdr_val *hdr,
                             const struct mds_ds_map_entry *entries)
{
    uint8_t enc[FDB_STRIPE_ENT_ENC_MAX];
    size_t len = 0;
    struct fdb_key_range r;
    struct fdb_key k;
    uint32_t n = hdr->stripe_count * hdr->mirror_count;
    uint32_t i;

    fdb_key_stripe_ent_prefix(&r.begin, p, fileid);
    if (!fdb_key_range_prefix(&r, &r.begin)) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    fdb_txn_clear_range(tr, &r);
    if (!fdb_stripe_hdr_encode(hdr, enc, sizeof(enc), &len)) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    fdb_key_stripe_hdr(&k, p, fileid);
    fdb_txn_set(tr, &k, enc, len);
    for (i = 0; i < n; i++) {
        if (!fdb_stripe_ent_encode(&entries[i], enc, sizeof(enc), &len)) {
            return FDB_ERR_PLATFORM_ERROR;
        }
        fdb_key_stripe_ent(&k, p, fileid, i);
        fdb_txn_set(tr, &k, enc, len);
    }
    return 0;
}

struct stripe_get_ctx {
    struct fdb_backend        *b;
    uint64_t                   fileid;
    bool                       want_entries;
    struct fdb_stripe_hdr_val  hdr;
    struct mds_ds_map_entry   *entries;
};

static int stripe_get_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct stripe_get_ctx *c = arg;
    struct fdb_key k;
    struct fdb_key ent_base;
    FDBFuture *f_hdr;
    FDBFuture *f_ent = NULL;
    uint8_t buf[FDB_STRIPE_HDR_ENC_SIZE];
    size_t len = 0;
    bool found = false;
    fdb_error_t err;
    int rc;

    free(c->entries);
    c->entries = NULL;
    fdb_key_stripe_hdr(&k, &c->b->prefix, c->fileid);
    fdb_key_stripe_ent_prefix(&ent_base, &c->b->prefix, c->fileid);
    f_hdr = fdb_txn_get_start(tr, &k, false);
    if (c->want_entries) {
        f_ent = stripe_entries_start(tr, &c->b->prefix, c->fileid, FDB_EXT_STRIPE_ENT_MAX);
    }
    err = fdb_txn_get_finish(f_hdr, buf, sizeof(buf), &len, &found);
    if (err != 0 || !found || !fdb_stripe_hdr_decode(buf, len, &c->hdr)) {
        if (f_ent != NULL) {
            fdb_future_destroy(f_ent); /* cancels the in-flight read */
        }
        if (err != 0) {
            return (int)err;
        }
        if (!found) {
            *st_out = MDS_ERR_NOTFOUND;
            return FDB_BODY_DONE;
        }
        return FDB_ERR_PLATFORM_ERROR; /* corrupt header */
    }
    if (f_ent == NULL) {
        *st_out = MDS_OK;
        return FDB_BODY_COMMIT;
    }
    rc = stripe_entries_finish(f_ent, &c->hdr, ent_base.len, &c->entries);
    if (rc == FDB_EXT_RC_NOMEM) {
        *st_out = MDS_ERR_NOMEM;
        return FDB_BODY_DONE;
    }
    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static enum mds_status fdb_stripe_map_get(struct mds_catalogue *cat, uint64_t fileid,
                                          uint32_t *sc, uint32_t *su, uint32_t *mc,
                                          struct mds_ds_map_entry **entries)
{
    struct fdb_backend *b = be_of(cat);
    struct stripe_get_ctx c;
    enum mds_status st;

    if (entries != NULL) {
        *entries = NULL;
    }
    if (b == NULL) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.b = b;
    c.fileid = fileid;
    c.want_entries = (entries != NULL);
    st = fdb_run_txn(b, FDB_TXN_READONLY, "stripe_map_get", stripe_get_body, &c);
    if (st != MDS_OK) {
        free(c.entries);
        return st;
    }
    if (sc != NULL) {
        *sc = c.hdr.stripe_count;
    }
    if (su != NULL) {
        *su = c.hdr.stripe_unit;
    }
    if (mc != NULL) {
        *mc = c.hdr.mirror_count;
    }
    if (entries != NULL) {
        *entries = c.entries;
    }
    return MDS_OK;
}

struct stripe_put_ctx {
    struct fdb_backend            *b;
    uint64_t                       fileid;
    struct fdb_stripe_hdr_val      hdr;
    const struct mds_ds_map_entry *entries;
};

static int stripe_put_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct stripe_put_ctx *c = arg;
    int rc = stripe_rows_write(tr, &c->b->prefix, c->fileid, &c->hdr, c->entries);

    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static enum mds_status fdb_stripe_map_put(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                          uint64_t fileid, uint32_t sc, uint32_t su, uint32_t mc,
                                          const struct mds_ds_map_entry *entries)
{
    struct fdb_backend *b = be_of(cat);
    struct stripe_put_ctx c;
    uint32_t i;

    (void)txn;
    if (b == NULL || sc == 0 || sc > MDS_MAX_STRIPES || mc == 0 || mc > MDS_MAX_MIRRORS ||
        su == 0 || entries == NULL) {
        return MDS_ERR_INVAL;
    }
    for (i = 0; i < sc * mc; i++) {
        if (entries[i].nfs_fh_len > MDS_NFS_FH_MAX) {
            return MDS_ERR_INVAL;
        }
    }
    memset(&c, 0, sizeof(c));
    c.b = b;
    c.fileid = fileid;
    c.hdr.stripe_count = sc;
    c.hdr.stripe_unit = su;
    c.hdr.mirror_count = mc;
    c.entries = entries;
    return fdb_run_txn(b, FDB_TXN_MUTATING, "stripe_map_put", stripe_put_body, &c);
}

struct stripe_del_ctx {
    struct fdb_backend *b;
    uint64_t            fileid;
};

/* Idempotent: deleting an absent map is MDS_OK (ds_gc repeats it). */
static int stripe_del_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct stripe_del_ctx *c = arg;
    struct fdb_key_range r;
    struct fdb_key k;

    fdb_key_stripe_ent_prefix(&r.begin, &c->b->prefix, c->fileid);
    if (!fdb_key_range_prefix(&r, &r.begin)) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    fdb_txn_clear_range(tr, &r);
    fdb_key_stripe_hdr(&k, &c->b->prefix, c->fileid);
    fdb_txn_clear(tr, &k);
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static enum mds_status fdb_stripe_map_del(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                          uint64_t fileid)
{
    struct fdb_backend *b = be_of(cat);
    struct stripe_del_ctx c;

    (void)txn;
    if (b == NULL) {
        return MDS_ERR_INVAL;
    }
    c.b = b;
    c.fileid = fileid;
    return fdb_run_txn(b, FDB_TXN_MUTATING, "stripe_map_del", stripe_del_body, &c);
}

/* One file of a stripe_map_scan page. */
struct scan_file {
    uint64_t                  fileid;
    struct fdb_stripe_hdr_val hdr;
    struct mds_ds_map_entry  *entries;
    FDBFuture                *f;
};

struct stripe_scan_ctx {
    struct fdb_backend *b;
    bool                have_cursor;
    uint64_t            cursor;  /**< Last fileid of the previous page. */
    struct scan_file   *files;   /**< FDB_EXT_SCAN_HDR_PAGE slots. */
    uint32_t            n;
    bool                more;
};

static void scan_page_reset(struct stripe_scan_ctx *c)
{
    uint32_t i;

    for (i = 0; i < c->n; i++) {
        free(c->files[i].entries);
        c->files[i].entries = NULL;
    }
    c->n = 0;
    c->more = false;
}

/* Phase 1: read a page of headers; accept files until the entry budget
 * is spent (the first file always fits).  Sets c->more when rows of
 * this page or of the range were left for the next page. */
static int scan_collect_headers(FDBTransaction *tr, struct stripe_scan_ctx *c)
{
    struct fdb_key base;
    struct fdb_key_range r;
    const FDBKeyValue *kvs = NULL;
    FDBFuture *f;
    uint32_t budget = 0;
    bool more = false;
    int count = 0;
    int i;
    fdb_error_t err;

    fdb_key_init(&base, &c->b->prefix, FDB_KT_STRIPE_HDR);
    if (!fdb_key_range_prefix(&r, &base)) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    if (c->have_cursor) {
        fdb_key_stripe_hdr(&r.begin, &c->b->prefix, c->cursor);
        fdb_key_u8(&r.begin, 0);
    }
    f = fdb_txn_get_range_start(tr, &r, (int)FDB_EXT_SCAN_HDR_PAGE, false, false);
    err = fdb_txn_get_range_wait(f, &kvs, &count, &more);
    if (err != 0) {
        if (f != NULL) {
            fdb_future_destroy(f);
        }
        return (int)err;
    }
    for (i = 0; i < count; i++) {
        FDBKeyValue kv;
        struct scan_file *sf = &c->files[c->n];
        uint32_t n_ent;

        fdb_kv_at(kvs, i, &kv);
        if (kv.key_length != (int)(base.len + FDB_KEY_BE64_LEN) ||
            !fdb_stripe_hdr_decode(kv.value, (size_t)kv.value_length, &sf->hdr)) {
            fdb_future_destroy(f);
            return FDB_ERR_PLATFORM_ERROR; /* corrupt header row */
        }
        n_ent = sf->hdr.stripe_count * sf->hdr.mirror_count;
        if (c->n > 0 && budget + n_ent > FDB_EXT_SCAN_ENT_BUDGET) {
            more = true; /* rows i.. of this page go to the next one */
            break;
        }
        sf->fileid = fdb_get_u64(kv.key + base.len);
        sf->entries = NULL;
        sf->f = NULL;
        budget += n_ent;
        c->n++;
    }
    fdb_future_destroy(f);
    c->more = more;
    return 0;
}

static int stripe_scan_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct stripe_scan_ctx *c = arg;
    uint32_t i;
    int rc;
    int first_rc = 0;

    scan_page_reset(c);
    rc = scan_collect_headers(tr, c);
    if (rc != 0) {
        return rc;
    }
    /* Phase 2: every accepted file's entries in parallel, one wave. */
    for (i = 0; i < c->n; i++) {
        struct scan_file *sf = &c->files[i];

        sf->f = stripe_entries_start(tr, &c->b->prefix, sf->fileid,
                                     sf->hdr.stripe_count * sf->hdr.mirror_count);
    }
    /* Phase 3: wait for all of them, even after an error, so no future
     * is leaked; the first error wins. */
    for (i = 0; i < c->n; i++) {
        struct scan_file *sf = &c->files[i];
        struct fdb_key ent_base;

        fdb_key_stripe_ent_prefix(&ent_base, &c->b->prefix, sf->fileid);
        rc = stripe_entries_finish(sf->f, &sf->hdr, ent_base.len, &sf->entries);
        sf->f = NULL;
        if (rc != 0 && first_rc == 0) {
            first_rc = rc;
        }
    }
    if (first_rc == FDB_EXT_RC_NOMEM) {
        *st_out = MDS_ERR_NOMEM;
        return FDB_BODY_DONE;
    }
    if (first_rc != 0) {
        return first_rc;
    }
    if (c->n > 0) {
        c->cursor = c->files[c->n - 1].fileid;
        c->have_cursor = true;
    }
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static enum mds_status fdb_stripe_map_scan(struct mds_catalogue *cat,
                                           mds_cat_stripe_map_scan_cb cb, void *ctx)
{
    struct fdb_backend *b = be_of(cat);
    struct stripe_scan_ctx c;
    enum mds_status st = MDS_OK;

    if (b == NULL || cb == NULL) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.b = b;
    c.files = calloc(FDB_EXT_SCAN_HDR_PAGE, sizeof(*c.files));
    if (c.files == NULL) {
        return MDS_ERR_NOMEM;
    }
    for (;;) {
        uint32_t i;

        st = fdb_run_txn(b, FDB_TXN_READONLY, "stripe_map_scan", stripe_scan_body, &c);
        if (st != MDS_OK) {
            break;
        }
        for (i = 0; i < c.n; i++) {
            const struct scan_file *sf = &c.files[i];

            if (cb(sf->fileid, sf->hdr.stripe_count, sf->hdr.stripe_unit,
                   sf->hdr.mirror_count, sf->entries, ctx) != 0) {
                goto out;
            }
        }
        if (!c.more || c.n == 0) {
            break;
        }
    }
out:
    scan_page_reset(&c);
    free(c.files);
    return st;
}

/* -----------------------------------------------------------------------
 * DS registry and provisioning
 * ----------------------------------------------------------------------- */

static enum mds_status fdb_ds_get(struct mds_catalogue *cat, uint32_t ds_id,
                                  struct mds_ds_info *info)
{
    struct fdb_backend *b = be_of(cat);
    uint8_t buf[FDB_DS_INFO_ENC_MAX];
    size_t len = 0;
    struct fdb_key k;
    enum mds_status st;

    if (b == NULL || info == NULL) {
        return MDS_ERR_INVAL;
    }
    fdb_key_ds(&k, &b->prefix, ds_id);
    st = run_get(b, "ds_get", &k, buf, sizeof(buf), &len);
    if (st != MDS_OK) {
        return st;
    }
    return fdb_ds_info_decode(buf, len, info) ? MDS_OK : MDS_ERR_IO;
}

static enum mds_status fdb_ds_put(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                  const struct mds_ds_info *info)
{
    struct fdb_backend *b = be_of(cat);
    uint8_t enc[FDB_DS_INFO_ENC_MAX];
    size_t len = 0;
    struct fdb_key k;

    (void)txn;
    if (b == NULL || info == NULL || info->ds_id >= MDS_MAX_DS_NODES ||
        !fdb_ds_info_encode(info, enc, sizeof(enc), &len)) {
        return MDS_ERR_INVAL;
    }
    fdb_key_ds(&k, &b->prefix, info->ds_id);
    return run_set(b, "ds_put", &k, enc, len);
}

static enum mds_status fdb_ds_del(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                  uint32_t ds_id)
{
    struct fdb_backend *b = be_of(cat);
    struct fdb_key k;

    (void)txn;
    if (b == NULL) {
        return MDS_ERR_INVAL;
    }
    fdb_key_ds(&k, &b->prefix, ds_id);
    return run_clear(b, "ds_del", &k, true);
}

struct ds_list_ctx {
    struct fdb_backend *b;
    struct mds_ds_info *list;
    uint32_t            count;
};

static int ds_list_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct ds_list_ctx *c = arg;
    struct fdb_key base;
    struct fdb_key_range r;
    const FDBKeyValue *kvs = NULL;
    FDBFuture *f;
    bool more = false;
    int count = 0;
    int i;
    fdb_error_t err;

    free(c->list);
    c->list = NULL;
    c->count = 0;
    fdb_key_ds_prefix(&base, &c->b->prefix);
    if (!fdb_key_range_prefix(&r, &base)) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    /* One row more than the bound: a registry beyond MDS_MAX_DS_NODES
     * (only a writer bypassing ds_put's id check could produce it) is
     * reported as corrupt rather than silently truncated. */
    f = fdb_txn_get_range_start(tr, &r, (int)MDS_MAX_DS_NODES + 1, false, false);
    err = fdb_txn_get_range_wait(f, &kvs, &count, &more);
    if (err != 0) {
        if (f != NULL) {
            fdb_future_destroy(f);
        }
        return (int)err;
    }
    if (count > (int)MDS_MAX_DS_NODES) {
        fdb_future_destroy(f);
        return FDB_ERR_PLATFORM_ERROR;
    }
    /* A non-NULL array even when empty (memdb hands one out too). */
    c->list = calloc(count > 0 ? (size_t)count : 1U, sizeof(*c->list));
    if (c->list == NULL) {
        fdb_future_destroy(f);
        *st_out = MDS_ERR_NOMEM;
        return FDB_BODY_DONE;
    }
    for (i = 0; i < count; i++) {
        FDBKeyValue kv;

        fdb_kv_at(kvs, i, &kv);
        if (!fdb_ds_info_decode(kv.value, (size_t)kv.value_length, &c->list[i])) {
            fdb_future_destroy(f);
            return FDB_ERR_PLATFORM_ERROR;
        }
    }
    fdb_future_destroy(f);
    c->count = (uint32_t)count;
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static enum mds_status fdb_ds_list(struct mds_catalogue *cat, struct mds_ds_info **list,
                                   uint32_t *count)
{
    struct fdb_backend *b = be_of(cat);
    struct ds_list_ctx c;
    enum mds_status st;

    if (b == NULL || list == NULL || count == NULL) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.b = b;
    st = fdb_run_txn(b, FDB_TXN_READONLY, "ds_list", ds_list_body, &c);
    if (st != MDS_OK) {
        free(c.list);
        return st;
    }
    *list = c.list;
    *count = c.count;
    return MDS_OK;
}

static enum mds_status fdb_ds_provision_get(struct mds_catalogue *cat, uint32_t ds_id,
                                            uint8_t *secret, uint32_t secret_len,
                                            uint64_t *epoch)
{
    struct fdb_backend *b = be_of(cat);
    struct fdb_ds_provision_val v;
    uint8_t buf[FDB_DS_PROVISION_ENC_MAX];
    size_t len = 0;
    struct fdb_key k;
    uint32_t copy;
    enum mds_status st;

    if (b == NULL || (secret_len > 0 && secret == NULL) || epoch == NULL) {
        return MDS_ERR_INVAL;
    }
    fdb_key_ds_provision(&k, &b->prefix, ds_id);
    st = run_get(b, "ds_provision_get", &k, buf, sizeof(buf), &len);
    if (st != MDS_OK) {
        return st;
    }
    if (!fdb_ds_provision_decode(buf, len, &v)) {
        return MDS_ERR_IO;
    }
    copy = v.secret_len;
    if (copy > secret_len) {
        copy = secret_len;
    }
    if (copy > 0) {
        memcpy(secret, v.secret, copy);
    }
    *epoch = v.epoch;
    return MDS_OK;
}

static enum mds_status fdb_ds_provision_put(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                            uint32_t ds_id, const uint8_t *secret,
                                            uint32_t secret_len, uint64_t epoch)
{
    struct fdb_backend *b = be_of(cat);
    struct fdb_ds_provision_val v;
    uint8_t enc[FDB_DS_PROVISION_ENC_MAX];
    size_t len = 0;
    struct fdb_key k;

    (void)txn;
    if (b == NULL || ds_id >= MDS_MAX_DS_NODES || secret_len > FDB_DS_SECRET_MAX ||
        (secret_len > 0 && secret == NULL)) {
        return MDS_ERR_INVAL;
    }
    memset(&v, 0, sizeof(v));
    v.epoch = epoch;
    v.secret_len = secret_len;
    if (secret_len > 0) {
        memcpy(v.secret, secret, secret_len);
    }
    if (!fdb_ds_provision_encode(&v, enc, sizeof(enc), &len)) {
        return MDS_ERR_INVAL;
    }
    fdb_key_ds_provision(&k, &b->prefix, ds_id);
    return run_set(b, "ds_provision_put", &k, enc, len);
}

static enum mds_status fdb_ds_provision_del(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                            uint32_t ds_id)
{
    struct fdb_backend *b = be_of(cat);
    struct fdb_key k;

    (void)txn;
    if (b == NULL) {
        return MDS_ERR_INVAL;
    }
    fdb_key_ds_provision(&k, &b->prefix, ds_id);
    return run_clear(b, "ds_provision_del", &k, true);
}

/* -----------------------------------------------------------------------
 * Quota rules and usage (keyed scope_type + scope_id; absolute upserts)
 * ----------------------------------------------------------------------- */

static enum mds_status fdb_quota_rule_get(struct mds_catalogue *cat, uint8_t scope_type,
                                          uint64_t scope_id, struct mds_quota_rule *rule)
{
    struct fdb_backend *b = be_of(cat);
    uint8_t buf[FDB_QUOTA_RULE_ENC_SIZE];
    size_t len = 0;
    struct fdb_key k;
    enum mds_status st;

    if (b == NULL || rule == NULL) {
        return MDS_ERR_INVAL;
    }
    fdb_key_quota_rule(&k, &b->prefix, scope_type, scope_id);
    st = run_get(b, "quota_rule_get", &k, buf, sizeof(buf), &len);
    if (st != MDS_OK) {
        return st;
    }
    return fdb_quota_rule_decode(buf, len, rule) ? MDS_OK : MDS_ERR_IO;
}

static enum mds_status fdb_quota_rule_put(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                          uint8_t scope_type, uint64_t scope_id,
                                          const struct mds_quota_rule *rule)
{
    struct fdb_backend *b = be_of(cat);
    uint8_t enc[FDB_QUOTA_RULE_ENC_SIZE];
    size_t len = 0;
    struct fdb_key k;

    (void)txn;
    if (b == NULL || rule == NULL || !fdb_quota_rule_encode(rule, enc, sizeof(enc), &len)) {
        return MDS_ERR_INVAL;
    }
    fdb_key_quota_rule(&k, &b->prefix, scope_type, scope_id);
    return run_set(b, "quota_rule_put", &k, enc, len);
}

static enum mds_status fdb_quota_usage_get(struct mds_catalogue *cat, uint8_t usage_type,
                                           uint64_t scope_id, struct mds_quota_usage *usage)
{
    struct fdb_backend *b = be_of(cat);
    uint8_t buf[FDB_QUOTA_USAGE_ENC_SIZE];
    size_t len = 0;
    struct fdb_key k;
    enum mds_status st;

    if (b == NULL || usage == NULL) {
        return MDS_ERR_INVAL;
    }
    fdb_key_quota_usage(&k, &b->prefix, usage_type, scope_id);
    st = run_get(b, "quota_usage_get", &k, buf, sizeof(buf), &len);
    if (st != MDS_OK) {
        return st;
    }
    return fdb_quota_usage_decode(buf, len, usage) ? MDS_OK : MDS_ERR_IO;
}

static enum mds_status fdb_quota_usage_put(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                           uint8_t usage_type, uint64_t scope_id,
                                           const struct mds_quota_usage *usage)
{
    struct fdb_backend *b = be_of(cat);
    uint8_t enc[FDB_QUOTA_USAGE_ENC_SIZE];
    size_t len = 0;
    struct fdb_key k;

    (void)txn;
    if (b == NULL || usage == NULL || !fdb_quota_usage_encode(usage, enc, sizeof(enc), &len)) {
        return MDS_ERR_INVAL;
    }
    fdb_key_quota_usage(&k, &b->prefix, usage_type, scope_id);
    return run_set(b, "quota_usage_put", &k, enc, len);
}

/* -----------------------------------------------------------------------
 * Sequence-keyed ranges (the GC and manifest tables, one owner's prefix
 * of the GC index): a paged, read-only walk in ascending sequence order
 * over the keys `base + be64 seq`.  Every key is handed to a visitor
 * with its value; the visitor filters, copies out and says when the
 * caller's capacity is reached.  A page is one transaction; a retried
 * page re-runs from the position saved at its start, so nothing is
 * counted or copied twice.
 * ----------------------------------------------------------------------- */

struct queue_pos {
    bool     have_cursor;
    uint64_t cursor;    /**< Last examined sequence. */
    uint64_t examined;
    uint32_t n;         /**< Rows accepted by the visitor so far. */
};

struct queue_ctx;

/** 0 = continue, 1 = capacity reached (stop), < 0 = corrupt row. */
typedef int (*queue_visit_fn)(struct queue_ctx *c, uint64_t seq, const uint8_t *val,
                              size_t vlen);

struct queue_ctx {
    struct fdb_backend *b;
    struct fdb_key      base;      /**< The range walked: base + be64 seq. */
    uint32_t            page;      /**< Rows per range read. */
    struct queue_pos    start;     /**< Position at the start of the page. */
    struct queue_pos    cur;       /**< Position reached by the page. */
    bool                more;
    bool                stop;
    queue_visit_fn      visit;
    void               *arg;
};

static int queue_page_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct queue_ctx *c = arg;
    struct fdb_key_range r;
    const FDBKeyValue *kvs = NULL;
    FDBFuture *f;
    bool more = false;
    int count = 0;
    int i;
    fdb_error_t err;

    c->cur = c->start;
    c->more = false;
    c->stop = false;
    if (!fdb_key_range_prefix(&r, &c->base)) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    if (c->cur.have_cursor) {
        r.begin = c->base;
        fdb_key_be64(&r.begin, c->cur.cursor);
        fdb_key_u8(&r.begin, 0); /* strictly after the last examined row */
    }
    f = fdb_txn_get_range_start(tr, &r, (int)c->page, false, false);
    err = fdb_txn_get_range_wait(f, &kvs, &count, &more);
    if (err != 0) {
        if (f != NULL) {
            fdb_future_destroy(f);
        }
        return (int)err;
    }
    for (i = 0; i < count; i++) {
        FDBKeyValue kv;
        uint64_t seq;
        int rc;

        fdb_kv_at(kvs, i, &kv);
        if (kv.key_length != (int)(c->base.len + FDB_KEY_BE64_LEN) || kv.value_length < 0) {
            fdb_future_destroy(f);
            return FDB_ERR_PLATFORM_ERROR; /* corrupt key */
        }
        seq = fdb_get_u64(kv.key + c->base.len);
        rc = c->visit(c, seq, kv.value, (size_t)kv.value_length);
        if (rc < 0) {
            fdb_future_destroy(f);
            return FDB_ERR_PLATFORM_ERROR; /* corrupt row */
        }
        c->cur.examined++;
        c->cur.cursor = seq;
        c->cur.have_cursor = true;
        if (rc > 0) {
            c->stop = true;
            break;
        }
    }
    fdb_future_destroy(f);
    c->more = more || c->stop;
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

/* Walk pages until the visitor stops, the range ends or @p examine_cap
 * rows were examined. */
static enum mds_status queue_walk(struct queue_ctx *c, const char *op, uint64_t examine_cap)
{
    for (;;) {
        enum mds_status st = fdb_run_txn(c->b, FDB_TXN_READONLY, op, queue_page_body, c);

        if (st != MDS_OK) {
            return st;
        }
        c->start = c->cur;
        if (c->stop || !c->more || c->cur.examined >= examine_cap) {
            return MDS_OK;
        }
    }
}

/* Walk the keys under @p base (a table, or one owner's index prefix). */
static void queue_init_base(struct queue_ctx *c, struct fdb_backend *b,
                            const struct fdb_key *base, uint32_t page, queue_visit_fn visit,
                            void *arg)
{
    memset(c, 0, sizeof(*c));
    c->b = b;
    c->base = *base;
    c->page = page;
    c->visit = visit;
    c->arg = arg;
}

/* Walk the whole table @p type. */
static void queue_init(struct queue_ctx *c, struct fdb_backend *b, enum fdb_key_type type,
                       uint32_t page, queue_visit_fn visit, void *arg)
{
    struct fdb_key base;

    fdb_key_init(&base, &b->prefix, type);
    queue_init_base(c, b, &base, page, visit, arg);
}

/* -----------------------------------------------------------------------
 * GC queue
 *
 * Two views (header comment, "GC visibility").  An MDS with an id
 * drains its own rows and the legacy rows (owner 0) and reaches them
 * through GC_BY_OWNER; an MDS with id 0 sees every row and walks the
 * GC table with the sequence walk above.  Both deliver ascending seq.
 * ----------------------------------------------------------------------- */

/* Largest batch one index peek materialises (ds_gc asks for at most
 * 4096, DS_GC_MAX_BATCH_SIZE).  A larger cap is served up to this
 * bound: fewer rows than asked for is always a legal answer, and the
 * bound keeps the per-call scratch (one seq per candidate) fixed. */
#define FDB_EXT_GC_PEEK_MAX       65536U

_Static_assert(FDB_EXT_GC_PEEK_MAX <= 0x7FFFFFFFU, "range limits are fdb_c ints");

struct gc_enqueue_ctx {
    struct fdb_backend  *b;
    struct mds_gc_entry  e;
};

static int gc_enqueue_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct gc_enqueue_ctx *c = arg;
    int rc = fdb_gc_row_set(tr, &c->b->prefix, &c->e);

    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static enum mds_status fdb_gc_enqueue(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                      uint64_t fileid, uint32_t ds_id, const uint8_t *nfs_fh,
                                      uint32_t fh_len, uint32_t sweep_hint)
{
    struct fdb_backend *b = be_of(cat);
    struct gc_enqueue_ctx c;
    uint64_t seq = 0;
    enum mds_status st;

    (void)txn;
    if (b == NULL || fh_len > MDS_NFS_FH_MAX || (fh_len > 0 && nfs_fh == NULL)) {
        return MDS_ERR_INVAL;
    }
    /* The sequence is minted once per call: a retried attempt rewrites
     * the same row and the same index key. */
    st = fdb_backend_alloc_id(b, FDB_META_GC_SEQ, &seq);
    if (st != MDS_OK) {
        return st;
    }
    memset(&c, 0, sizeof(c));
    c.b = b;
    c.e.gc_seq = seq;
    c.e.fileid = fileid;
    c.e.ds_id = ds_id;
    c.e.owner_mds_id = b->mds_id;
    c.e.sweep_hint = sweep_hint;
    c.e.nfs_fh_len = fh_len;
    if (fh_len > 0) {
        memcpy(c.e.nfs_fh, nfs_fh, fh_len);
    }
    return fdb_run_txn(b, FDB_TXN_MUTATING, "gc_enqueue", gc_enqueue_body, &c);
}

/* --- peek: the id-0 view, a walk of the GC table ------------------------ */

struct gc_peek_arg {
    struct mds_gc_entry *entries;
    uint32_t             cap;
};

static int gc_peek_visit(struct queue_ctx *c, uint64_t seq, const uint8_t *val, size_t vlen)
{
    struct gc_peek_arg *a = c->arg;
    struct mds_gc_entry e;

    if (!fdb_gc_decode(val, vlen, &e)) {
        return -1;
    }
    e.gc_seq = seq;
    a->entries[c->cur.n] = e;
    c->cur.n++;
    return (c->cur.n >= a->cap) ? 1 : 0;
}

static enum mds_status gc_walk_peek(struct fdb_backend *b, struct mds_gc_entry *entries,
                                    uint32_t cap, uint32_t *n_out)
{
    struct queue_ctx q;
    struct gc_peek_arg a;
    enum mds_status st;

    a.entries = entries;
    a.cap = cap;
    queue_init(&q, b, FDB_KT_GC, FDB_EXT_QUEUE_PAGE, gc_peek_visit, &a);
    st = queue_walk(&q, "gc_peek_batch", FDB_EXT_EXAMINE_CAP(cap));
    if (st == MDS_OK) {
        *n_out = q.cur.n;
    }
    return st;
}

/* --- peek: the owner view, through GC_BY_OWNER ------------------------- */

/* Heap allocated once per call (the wave of futures). */
struct gc_idx_ctx {
    struct fdb_backend  *b;
    struct mds_gc_entry *entries;   /**< Caller's array; want slots are used. */
    uint32_t             want;      /**< Rows asked for (<= FDB_EXT_GC_PEEK_MAX). */
    uint32_t             n;         /**< Rows delivered. */
    uint32_t             base_len;  /**< Bytes of an index key before its seq. */
    uint64_t            *seqs;      /**< want slots: the merged candidates (heap). */
    FDBFuture           *fs[FDB_EXT_QUEUE_PAGE]; /**< One wave of row reads. */
};

/* One owner's index page while it is merged. */
struct gc_idx_page {
    const FDBKeyValue *kvs;
    int                count;
    int                pos;
};

/* Start the range read of the first @p limit index keys of @p owner. */
static FDBFuture *gc_idx_start(FDBTransaction *tr, const struct fdb_key_prefix *p,
                               uint32_t owner, uint32_t limit)
{
    struct fdb_key_range r;

    fdb_key_gc_by_owner_prefix(&r.begin, p, owner);
    if (!fdb_key_range_prefix(&r, &r.begin)) {
        return NULL;
    }
    return fdb_txn_get_range_start(tr, &r, (int)limit, false, false);
}

/* The seq of the page's next key; false when the key is not
 * prefix + be64 (a corrupt index). */
static bool gc_idx_head(const struct gc_idx_page *pg, uint32_t base_len, uint64_t *seq)
{
    FDBKeyValue kv;

    fdb_kv_at(pg->kvs, pg->pos, &kv);
    if (kv.key_length != (int)(base_len + FDB_KEY_BE64_LEN)) {
        return false;
    }
    *seq = fdb_get_u64(kv.key + base_len);
    return true;
}

/* Merge the two owner pages by ascending seq into c->seqs, at most
 * c->want of them.  Each page holds the smallest seqs of its owner, so
 * the merged prefix is the smallest seqs of both together. */
static int gc_idx_merge(struct gc_idx_ctx *c, struct gc_idx_page *own,
                        struct gc_idx_page *legacy, uint32_t *n_out)
{
    uint32_t n = 0;

    while (n < c->want && (own->pos < own->count || legacy->pos < legacy->count)) {
        bool have_own = own->pos < own->count;
        bool have_legacy = legacy->pos < legacy->count;
        uint64_t so = 0;
        uint64_t sl = 0;

        if ((have_own && !gc_idx_head(own, c->base_len, &so)) ||
            (have_legacy && !gc_idx_head(legacy, c->base_len, &sl))) {
            return FDB_ERR_PLATFORM_ERROR; /* corrupt index key */
        }
        if (have_own && (!have_legacy || so <= sl)) {
            c->seqs[n] = so;
            own->pos++;
        } else {
            c->seqs[n] = sl;
            legacy->pos++;
        }
        n++;
    }
    *n_out = n;
    return 0;
}

/* Wave 1: the first c->want keys of this MDS's prefix and of the
 * legacy prefix, read in parallel and merged. */
static int gc_idx_collect(FDBTransaction *tr, struct gc_idx_ctx *c, uint32_t *n_out)
{
    const struct fdb_key_prefix *p = &c->b->prefix;
    struct gc_idx_page own;
    struct gc_idx_page legacy;
    FDBFuture *f_own;
    FDBFuture *f_legacy;
    bool more = false;
    fdb_error_t err;
    fdb_error_t err2;
    int rc;

    memset(&own, 0, sizeof(own));
    memset(&legacy, 0, sizeof(legacy));
    f_own = gc_idx_start(tr, p, c->b->mds_id, c->want);
    f_legacy = gc_idx_start(tr, p, 0, c->want);
    err = fdb_txn_get_range_wait(f_own, &own.kvs, &own.count, &more);
    err2 = fdb_txn_get_range_wait(f_legacy, &legacy.kvs, &legacy.count, &more);
    if (err == 0) {
        err = err2;
    }
    rc = (err == 0) ? gc_idx_merge(c, &own, &legacy, n_out) : (int)err;
    if (f_own != NULL) {
        fdb_future_destroy(f_own);
    }
    if (f_legacy != NULL) {
        fdb_future_destroy(f_legacy);
    }
    return rc;
}

/* Finish the row read @p f of @p seq into *e (gc_seq set from the key);
 * *found is false when the row is absent. */
static fdb_error_t gc_row_finish(FDBFuture *f, uint64_t seq, struct mds_gc_entry *e,
                                 bool *found)
{
    uint8_t buf[FDB_GC_ENC_MAX];
    size_t len = 0;
    fdb_error_t err;

    err = fdb_txn_get_finish(f, buf, sizeof(buf), &len, found);
    if (err != 0 || !*found) {
        return err;
    }
    if (!fdb_gc_decode(buf, len, e)) {
        return FDB_ERR_PLATFORM_ERROR; /* corrupt row */
    }
    e->gc_seq = seq;
    return 0;
}

/* Wave 2: the rows of c->seqs[0 .. n), FDB_EXT_QUEUE_PAGE parallel
 * point reads at a time, appended to c->entries in seq order.  A key
 * whose row is absent is skipped: row and key are written and cleared
 * in one transaction and this is one snapshot, so only a foreign
 * writer could produce one. */
static int gc_idx_rows(FDBTransaction *tr, struct gc_idx_ctx *c, uint32_t n)
{
    const struct fdb_key_prefix *p = &c->b->prefix;
    uint32_t done = 0;

    while (done < n) {
        uint32_t wave = n - done;
        fdb_error_t err = 0;
        uint32_t i;

        if (wave > FDB_EXT_QUEUE_PAGE) {
            wave = FDB_EXT_QUEUE_PAGE;
        }
        for (i = 0; i < wave; i++) {
            struct fdb_key k;

            fdb_key_gc(&k, p, c->seqs[done + i]);
            c->fs[i] = fdb_txn_get_start(tr, &k, false);
        }
        /* Every future is finished, even after an error, so none leaks. */
        for (i = 0; i < wave; i++) {
            bool found = false;
            fdb_error_t e2 = gc_row_finish(c->fs[i], c->seqs[done + i], &c->entries[c->n],
                                           &found);

            c->fs[i] = NULL;
            if (e2 != 0) {
                if (err == 0) {
                    err = e2;
                }
            } else if (err == 0 && found) {
                c->n++;
            }
        }
        if (err != 0) {
            return (int)err;
        }
        done += wave;
    }
    return 0;
}

static int gc_idx_peek_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct gc_idx_ctx *c = arg;
    uint32_t n = 0;
    int rc;

    c->n = 0;
    rc = gc_idx_collect(tr, c, &n);
    if (rc == 0) {
        rc = gc_idx_rows(tr, c, n);
    }
    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

/* One read-only transaction, two dependent waves (three or more only
 * above FDB_EXT_QUEUE_PAGE rows). */
static enum mds_status gc_idx_peek(struct fdb_backend *b, struct mds_gc_entry *entries,
                                   uint32_t cap, uint32_t *n_out)
{
    struct gc_idx_ctx *c;
    struct fdb_key base;
    enum mds_status st;

    c = calloc(1, sizeof(*c));
    if (c == NULL) {
        return MDS_ERR_NOMEM;
    }
    c->b = b;
    c->entries = entries;
    c->want = (cap < FDB_EXT_GC_PEEK_MAX) ? cap : FDB_EXT_GC_PEEK_MAX;
    fdb_key_gc_by_owner_prefix(&base, &b->prefix, 0);
    c->base_len = base.len;
    c->seqs = calloc(c->want, sizeof(*c->seqs));
    if (c->seqs == NULL) {
        free(c);
        return MDS_ERR_NOMEM;
    }
    st = fdb_run_txn(b, FDB_TXN_READONLY, "gc_peek_batch", gc_idx_peek_body, c);
    if (st == MDS_OK) {
        *n_out = c->n;
    }
    free(c->seqs);
    free(c);
    return st;
}

static enum mds_status fdb_gc_peek_batch(struct mds_catalogue *cat, struct mds_gc_entry *entries,
                                         uint32_t cap, uint32_t *n_out)
{
    struct fdb_backend *b = be_of(cat);

    if (n_out != NULL) {
        *n_out = 0;
    }
    if (b == NULL || entries == NULL || cap == 0 || n_out == NULL) {
        return MDS_ERR_INVAL;
    }
    if (b->mds_id == 0) {
        return gc_walk_peek(b, entries, cap, n_out);
    }
    return gc_idx_peek(b, entries, cap, n_out);
}

static enum mds_status fdb_gc_peek(struct mds_catalogue *cat, struct mds_gc_entry *entry)
{
    uint32_t n = 0;
    enum mds_status st;

    if (entry == NULL) {
        return MDS_ERR_INVAL;
    }
    st = fdb_gc_peek_batch(cat, entry, 1, &n);
    if (st != MDS_OK) {
        return st;
    }
    return (n == 1) ? MDS_OK : MDS_ERR_NOTFOUND;
}

/* --- dequeue ------------------------------------------------------------ */

struct gc_dequeue_ctx {
    struct fdb_backend *b;
    uint64_t            gc_seq;
};

/* The row is read for its owner, then row and index key are cleared
 * together.  An absent row is MDS_ERR_NOTFOUND with nothing written
 * (a repeated dequeue), as before the index existed; a row that does
 * not decode is corrupt (MDS_ERR_IO), as a peek over it reports. */
static int gc_dequeue_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct gc_dequeue_ctx *c = arg;
    struct mds_gc_entry e;
    struct fdb_key k;
    uint8_t buf[FDB_GC_ENC_MAX];
    size_t len = 0;
    bool found = false;
    fdb_error_t err;

    fdb_key_gc(&k, &c->b->prefix, c->gc_seq);
    err = fdb_txn_get(tr, &k, false, buf, sizeof(buf), &len, &found);
    if (err != 0) {
        return (int)err;
    }
    if (!found) {
        *st_out = MDS_ERR_NOTFOUND;
        return FDB_BODY_DONE;
    }
    if (!fdb_gc_decode(buf, len, &e)) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    fdb_gc_row_clear(tr, &c->b->prefix, c->gc_seq, e.owner_mds_id);
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static enum mds_status fdb_gc_dequeue(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                      uint64_t gc_seq)
{
    struct fdb_backend *b = be_of(cat);
    struct gc_dequeue_ctx c;

    (void)txn;
    if (b == NULL) {
        return MDS_ERR_INVAL;
    }
    c.b = b;
    c.gc_seq = gc_seq;
    return fdb_run_txn(b, FDB_TXN_MUTATING, "gc_dequeue", gc_dequeue_body, &c);
}

/* --- count -------------------------------------------------------------- */

struct count_arg {
    uint64_t count;
};

/* Every key counts (an index prefix, the manifest table). */
static int count_visit(struct queue_ctx *c, uint64_t seq, const uint8_t *val, size_t vlen)
{
    struct count_arg *a = c->arg;

    (void)seq;
    (void)val;
    (void)vlen;
    a->count++;
    return 0;
}

/* GC rows of the id-0 walk: decoded, so a corrupt row is reported. */
static int gc_count_visit(struct queue_ctx *c, uint64_t seq, const uint8_t *val, size_t vlen)
{
    struct count_arg *a = c->arg;
    struct mds_gc_entry e;

    (void)seq;
    if (!fdb_gc_decode(val, vlen, &e)) {
        return -1;
    }
    a->count++;
    return 0;
}

static uint32_t count_clamp(uint64_t n)
{
    return (n > UINT32_MAX) ? UINT32_MAX : (uint32_t)n;
}

/* Add the number of index keys of @p owner to *count, examining at
 * most @p budget of them. */
static enum mds_status gc_idx_count(struct fdb_backend *b, uint32_t owner, uint64_t budget,
                                    uint64_t *count)
{
    struct queue_ctx q;
    struct count_arg a = { 0 };
    struct fdb_key base;
    enum mds_status st;

    fdb_key_gc_by_owner_prefix(&base, &b->prefix, owner);
    queue_init_base(&q, b, &base, FDB_EXT_COUNT_PAGE, count_visit, &a);
    st = queue_walk(&q, "gc_count", budget);
    if (st == MDS_OK) {
        *count += a.count;
    }
    return st;
}

static enum mds_status fdb_gc_count(struct mds_catalogue *cat, uint32_t *count)
{
    struct fdb_backend *b = be_of(cat);
    struct queue_ctx q;
    struct count_arg a = { 0 };
    enum mds_status st;

    if (b == NULL || count == NULL) {
        return MDS_ERR_INVAL;
    }
    if (b->mds_id != 0) {
        uint64_t total = 0;

        /* Own keys first; the legacy keys get what is left of the
         * examine budget, so the two together saturate like one walk. */
        st = gc_idx_count(b, b->mds_id, FDB_EXT_COUNT_MAX, &total);
        if (st == MDS_OK && total < FDB_EXT_COUNT_MAX) {
            st = gc_idx_count(b, 0, FDB_EXT_COUNT_MAX - total, &total);
        }
        if (st == MDS_OK) {
            *count = count_clamp(total);
        }
        return st;
    }
    queue_init(&q, b, FDB_KT_GC, FDB_EXT_COUNT_PAGE, gc_count_visit, &a);
    st = queue_walk(&q, "gc_count", FDB_EXT_COUNT_MAX);
    if (st == MDS_OK) {
        *count = count_clamp(a.count);
    }
    return st;
}

/* -----------------------------------------------------------------------
 * Async-REMOVE delete manifest
 * ----------------------------------------------------------------------- */

/* Encode a fresh manifest row for @p seq into @p enc. */
static bool rp_row_build(uint64_t dir_fileid, const char *name, uint64_t child_fileid,
                         uint64_t child_generation, uint8_t *enc, size_t cap, size_t *len)
{
    struct mds_remove_pending_entry e;

    memset(&e, 0, sizeof(e));
    e.dir_fileid = dir_fileid;
    e.child_fileid = child_fileid;
    e.child_generation = child_generation;
    e.enqueued_ns = realtime_ns();
    (void)snprintf(e.name, sizeof(e.name), "%s", name);
    return fdb_remove_pending_encode(&e, enc, cap, len);
}

static enum mds_status fdb_remove_pending_enqueue(struct mds_catalogue *cat,
                                                  struct mds_cat_txn *txn, uint64_t dir_fileid,
                                                  const char *name, uint64_t child_fileid,
                                                  uint64_t child_generation, uint64_t *seq_out)
{
    struct fdb_backend *b = be_of(cat);
    uint8_t enc[FDB_REMOVE_PENDING_ENC_MAX];
    size_t len = 0;
    struct fdb_key k;
    uint64_t seq = 0;
    enum mds_status st;

    (void)txn;
    if (b == NULL || !name_len_ok(name, MDS_MAX_NAME) || seq_out == NULL) {
        return MDS_ERR_INVAL;
    }
    st = fdb_backend_alloc_id(b, FDB_META_REMOVE_SEQ, &seq);
    if (st != MDS_OK) {
        return st;
    }
    if (!rp_row_build(dir_fileid, name, child_fileid, child_generation, enc, sizeof(enc), &len)) {
        return MDS_ERR_INVAL;
    }
    fdb_key_remove_pending(&k, &b->prefix, seq);
    st = run_set(b, "remove_pending_enqueue", &k, enc, len);
    if (st == MDS_OK) {
        *seq_out = seq;
    }
    return st;
}

struct rp_unlink_ctx {
    struct fdb_backend *b;
    uint64_t            dir;
    const char         *name;
    uint64_t            child;
    uint64_t            generation;
    struct fdb_key      row_key;
    uint8_t             row[FDB_REMOVE_PENDING_ENC_MAX];
    size_t              row_len;
};

/* Delete-at-ack: manifest row + guarded dirent delete + DELETE_PENDING
 * flag in one transaction.  MDS_ERR_STALE when the dirent no longer
 * resolves to (child, generation).  The parent is deliberately not
 * touched: the ack path folds that delta into its aggregator (memdb
 * does the same). */
static int rp_unlink_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct rp_unlink_ctx *c = arg;
    FDBFuture *f_dirent;
    FDBFuture *f_child;
    struct fdb_dirent_val dv;
    struct mds_inode child;
    bool dirent_found = false;
    bool child_found = false;
    fdb_error_t err;
    fdb_error_t err2;
    int rc;

    f_dirent = fdb_dirent_read_start(tr, &c->b->prefix, c->dir, c->name);
    f_child = fdb_blob_read_start(tr, &c->b->prefix, c->child);
    err = fdb_dirent_read_finish(f_dirent, &dv, &dirent_found);
    err2 = fdb_blob_read_finish(f_child, &child, &child_found);
    if (err == 0) {
        err = err2;
    }
    if (err != 0) {
        return (int)err;
    }
    if (!dirent_found || dv.child_fileid != c->child || !child_found ||
        child.generation != c->generation) {
        *st_out = MDS_ERR_STALE;
        return FDB_BODY_DONE;
    }
    child.flags |= MDS_IFLAG_DELETE_PENDING;
    rc = fdb_blob_write(tr, &c->b->prefix, &child);
    if (rc != 0) {
        return rc;
    }
    fdb_dirent_clear(tr, &c->b->prefix, c->dir, c->name, dv.seq);
    fdb_txn_set(tr, &c->row_key, c->row, c->row_len);
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static enum mds_status fdb_remove_pending_enqueue_unlink(struct mds_catalogue *cat,
                                                         struct mds_cat_txn *txn,
                                                         uint64_t dir_fileid, const char *name,
                                                         uint64_t child_fileid,
                                                         uint64_t child_generation,
                                                         uint64_t *seq_out)
{
    struct fdb_backend *b = be_of(cat);
    struct rp_unlink_ctx c;
    uint64_t seq = 0;
    enum mds_status st;

    (void)txn;
    if (b == NULL || !name_len_ok(name, MDS_MAX_NAME) || seq_out == NULL) {
        return MDS_ERR_INVAL;
    }
    st = fdb_backend_alloc_id(b, FDB_META_REMOVE_SEQ, &seq);
    if (st != MDS_OK) {
        return st;
    }
    memset(&c, 0, sizeof(c));
    c.b = b;
    c.dir = dir_fileid;
    c.name = name;
    c.child = child_fileid;
    c.generation = child_generation;
    fdb_key_remove_pending(&c.row_key, &b->prefix, seq);
    if (!rp_row_build(dir_fileid, name, child_fileid, child_generation, c.row, sizeof(c.row),
                      &c.row_len)) {
        return MDS_ERR_INVAL;
    }
    st = fdb_run_txn(b, FDB_TXN_MUTATING, "remove_pending_enqueue_unlink", rp_unlink_body, &c);
    if (st == MDS_OK) {
        *seq_out = seq;
    }
    return st;
}

struct rp_peek_arg {
    struct mds_remove_pending_entry *entries;
    uint32_t                         cap;
    uint64_t                         now_ns;
};

/* Rows with no live claim, ascending seq (memdb's filter). */
static int rp_peek_visit(struct queue_ctx *c, uint64_t seq, const uint8_t *val, size_t vlen)
{
    struct rp_peek_arg *a = c->arg;
    struct mds_remove_pending_entry e;

    if (!fdb_remove_pending_decode(val, vlen, &e)) {
        return -1;
    }
    if (e.claim_mds_id != 0 && e.claim_expires_ns >= a->now_ns) {
        return 0; /* claimed by a live drainer */
    }
    e.remove_seq = seq;
    a->entries[c->cur.n] = e;
    c->cur.n++;
    return (c->cur.n >= a->cap) ? 1 : 0;
}

static enum mds_status fdb_remove_pending_peek_batch(struct mds_catalogue *cat,
                                                     uint64_t now_ns,
                                                     struct mds_remove_pending_entry *entries,
                                                     uint32_t cap, uint32_t *n_out)
{
    struct fdb_backend *b = be_of(cat);
    struct queue_ctx q;
    struct rp_peek_arg a;
    enum mds_status st;

    if (n_out != NULL) {
        *n_out = 0;
    }
    if (b == NULL || entries == NULL || cap == 0 || n_out == NULL) {
        return MDS_ERR_INVAL;
    }
    a.entries = entries;
    a.cap = cap;
    a.now_ns = now_ns;
    queue_init(&q, b, FDB_KT_REMOVE_PENDING, FDB_EXT_QUEUE_PAGE, rp_peek_visit, &a);
    st = queue_walk(&q, "remove_pending_peek_batch", FDB_EXT_EXAMINE_CAP(cap));
    if (st == MDS_OK) {
        *n_out = q.cur.n;
    }
    return st;
}

struct rp_claim_ctx {
    struct fdb_key key;
    uint32_t       mds_id;
    uint64_t       boot_epoch;
    uint64_t       now_ns;
    uint64_t       claim_ttl_ns;
};

/* The claim test and the claim write are one transaction (C3): two
 * drainers racing for a row conflict on its read, the loser re-runs and
 * sees the live claim. */
static int rp_claim_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct rp_claim_ctx *c = arg;
    struct mds_remove_pending_entry e;
    uint8_t buf[FDB_REMOVE_PENDING_ENC_MAX];
    size_t len = 0;
    bool found = false;
    fdb_error_t err;

    err = fdb_txn_get(tr, &c->key, false, buf, sizeof(buf), &len, &found);
    if (err != 0) {
        return (int)err;
    }
    if (!found) {
        *st_out = MDS_ERR_NOTFOUND;
        return FDB_BODY_DONE;
    }
    if (!fdb_remove_pending_decode(buf, len, &e)) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    if (e.claim_mds_id != 0 && e.claim_expires_ns >= c->now_ns) {
        *st_out = MDS_ERR_NOTFOUND; /* another drainer holds a live claim */
        return FDB_BODY_DONE;
    }
    e.claim_mds_id = c->mds_id;
    e.claim_boot = c->boot_epoch;
    if (c->claim_ttl_ns > UINT64_MAX - c->now_ns) {
        e.claim_expires_ns = UINT64_MAX; /* saturate like memdb */
    } else {
        e.claim_expires_ns = c->now_ns + c->claim_ttl_ns;
    }
    if (!fdb_remove_pending_encode(&e, buf, sizeof(buf), &len)) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    fdb_txn_set(tr, &c->key, buf, len);
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static enum mds_status fdb_remove_pending_claim(struct mds_catalogue *cat, uint64_t remove_seq,
                                                uint32_t mds_id, uint64_t boot_epoch,
                                                uint64_t now_ns, uint64_t claim_ttl_ns)
{
    struct fdb_backend *b = be_of(cat);
    struct rp_claim_ctx c;

    if (b == NULL) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    fdb_key_remove_pending(&c.key, &b->prefix, remove_seq);
    c.mds_id = mds_id;
    c.boot_epoch = boot_epoch;
    c.now_ns = now_ns;
    c.claim_ttl_ns = claim_ttl_ns;
    return fdb_run_txn(b, FDB_TXN_MUTATING, "remove_pending_claim", rp_claim_body, &c);
}

/* Idempotent: a completed row may be completed again (blind clear). */
static enum mds_status fdb_remove_pending_complete(struct mds_catalogue *cat,
                                                   uint64_t remove_seq)
{
    struct fdb_backend *b = be_of(cat);
    struct fdb_key k;

    if (b == NULL) {
        return MDS_ERR_INVAL;
    }
    fdb_key_remove_pending(&k, &b->prefix, remove_seq);
    return run_clear(b, "remove_pending_complete", &k, false);
}

/* retries += 1 when the row exists; MDS_OK either way (memdb). */
static int rp_bump_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct kv_ctx *c = arg;
    struct mds_remove_pending_entry e;
    uint8_t buf[FDB_REMOVE_PENDING_ENC_MAX];
    size_t len = 0;
    bool found = false;
    fdb_error_t err;

    err = fdb_txn_get(tr, &c->key, false, buf, sizeof(buf), &len, &found);
    if (err != 0) {
        return (int)err;
    }
    *st_out = MDS_OK;
    if (!found) {
        return FDB_BODY_DONE;
    }
    if (!fdb_remove_pending_decode(buf, len, &e)) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    e.retries++;
    if (!fdb_remove_pending_encode(&e, buf, sizeof(buf), &len)) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    fdb_txn_set(tr, &c->key, buf, len);
    return FDB_BODY_COMMIT;
}

static enum mds_status fdb_remove_pending_bump_retry(struct mds_catalogue *cat,
                                                     uint64_t remove_seq)
{
    struct fdb_backend *b = be_of(cat);
    struct kv_ctx c;

    if (b == NULL) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    fdb_key_remove_pending(&c.key, &b->prefix, remove_seq);
    return fdb_run_txn(b, FDB_TXN_MUTATING, "remove_pending_bump_retry", rp_bump_body, &c);
}

static enum mds_status fdb_remove_pending_count(struct mds_catalogue *cat, uint32_t *count)
{
    struct fdb_backend *b = be_of(cat);
    struct queue_ctx q;
    struct count_arg a = { 0 };
    enum mds_status st;

    if (b == NULL || count == NULL) {
        return MDS_ERR_INVAL;
    }
    queue_init(&q, b, FDB_KT_REMOVE_PENDING, FDB_EXT_COUNT_PAGE, count_visit, &a);
    st = queue_walk(&q, "remove_pending_count", FDB_EXT_COUNT_MAX);
    if (st == MDS_OK) {
        *count = count_clamp(a.count);
    }
    return st;
}

struct rp_scan_arg {
    struct mds_remove_pending_entry *page; /**< FDB_EXT_RP_SCAN_PAGE slots. */
};

static int rp_scan_visit(struct queue_ctx *c, uint64_t seq, const uint8_t *val, size_t vlen)
{
    struct rp_scan_arg *a = c->arg;

    if (c->cur.n >= FDB_EXT_RP_SCAN_PAGE ||
        !fdb_remove_pending_decode(val, vlen, &a->page[c->cur.n])) {
        return -1;
    }
    a->page[c->cur.n].remove_seq = seq;
    c->cur.n++;
    return 0;
}

/* One page per transaction, delivered after it ended (C1/C2). */
static enum mds_status fdb_remove_pending_scan_all(struct mds_catalogue *cat,
                                                   mds_cat_remove_pending_scan_cb cb, void *ctx)
{
    struct fdb_backend *b = be_of(cat);
    struct queue_ctx q;
    struct rp_scan_arg a;
    enum mds_status st = MDS_OK;

    if (b == NULL || cb == NULL) {
        return MDS_ERR_INVAL;
    }
    a.page = calloc(FDB_EXT_RP_SCAN_PAGE, sizeof(*a.page));
    if (a.page == NULL) {
        return MDS_ERR_NOMEM;
    }
    queue_init(&q, b, FDB_KT_REMOVE_PENDING, FDB_EXT_RP_SCAN_PAGE, rp_scan_visit, &a);
    for (;;) {
        uint32_t i;

        q.start.n = 0;
        st = fdb_run_txn(b, FDB_TXN_READONLY, "remove_pending_scan_all", queue_page_body, &q);
        if (st != MDS_OK) {
            break;
        }
        for (i = 0; i < q.cur.n; i++) {
            if (cb(&a.page[i], ctx) != 0) {
                goto out;
            }
        }
        if (!q.more || q.cur.n == 0) {
            break;
        }
        q.start = q.cur;
    }
out:
    free(a.page);
    return st;
}

/* -----------------------------------------------------------------------
 * Shard routing, cross-shard dirents, link anchors
 * ----------------------------------------------------------------------- */

static enum mds_status fdb_shard_fileid_get(struct mds_catalogue *cat, uint64_t fileid,
                                            uint32_t *shard_id)
{
    struct fdb_backend *b = be_of(cat);
    uint8_t buf[sizeof(uint32_t)]; /* SHARD_FILEID value: one LE u32 */
    size_t len = 0;
    struct fdb_key k;
    enum mds_status st;

    if (b == NULL || shard_id == NULL) {
        return MDS_ERR_INVAL;
    }
    fdb_key_shard_fileid(&k, &b->prefix, fileid);
    st = run_get(b, "shard_fileid_get", &k, buf, sizeof(buf), &len);
    if (st != MDS_OK) {
        return st;
    }
    return fdb_le32_decode(buf, len, shard_id) ? MDS_OK : MDS_ERR_IO;
}

static enum mds_status fdb_shard_fileid_put(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                            uint64_t fileid, uint32_t shard_id)
{
    struct fdb_backend *b = be_of(cat);
    uint8_t enc[sizeof(uint32_t)]; /* SHARD_FILEID value: one LE u32 */
    struct fdb_key k;

    (void)txn;
    if (b == NULL) {
        return MDS_ERR_INVAL;
    }
    fdb_le32_put(enc, shard_id);
    fdb_key_shard_fileid(&k, &b->prefix, fileid);
    return run_set(b, "shard_fileid_put", &k, enc, sizeof(enc));
}

static enum mds_status fdb_shard_fileid_del(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                            uint64_t fileid)
{
    struct fdb_backend *b = be_of(cat);
    struct fdb_key k;

    (void)txn;
    if (b == NULL) {
        return MDS_ERR_INVAL;
    }
    fdb_key_shard_fileid(&k, &b->prefix, fileid);
    return run_clear(b, "shard_fileid_del", &k, true);
}

static enum mds_status fdb_ext_dirent_get(struct mds_catalogue *cat, uint64_t parent,
                                          const char *name, uint32_t *owner_mds_id,
                                          uint64_t *target_fileid, uint8_t *target_type,
                                          uint64_t *anchor_id)
{
    struct fdb_backend *b = be_of(cat);
    struct fdb_ext_dirent_val v;
    uint8_t buf[FDB_EXT_DIRENT_ENC_SIZE];
    size_t len = 0;
    struct fdb_key k;
    enum mds_status st;

    if (b == NULL || name == NULL) {
        return MDS_ERR_INVAL;
    }
    if (!name_len_ok(name, MDS_MAX_NAME)) {
        return MDS_ERR_NOTFOUND;
    }
    fdb_key_ext_dirent(&k, &b->prefix, parent, name);
    st = run_get(b, "ext_dirent_get", &k, buf, sizeof(buf), &len);
    if (st != MDS_OK) {
        return st;
    }
    if (!fdb_ext_dirent_decode(buf, len, &v)) {
        return MDS_ERR_IO;
    }
    if (owner_mds_id != NULL) {
        *owner_mds_id = v.owner_mds_id;
    }
    if (target_fileid != NULL) {
        *target_fileid = v.target_fileid;
    }
    if (target_type != NULL) {
        *target_type = v.target_type;
    }
    if (anchor_id != NULL) {
        *anchor_id = v.anchor_id;
    }
    return MDS_OK;
}

/* Upsert keyed (parent, name). */
static enum mds_status fdb_ext_dirent_put(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                          uint64_t parent, const char *name,
                                          uint32_t owner_mds_id, uint64_t target_fileid,
                                          uint8_t target_type, uint64_t anchor_id)
{
    struct fdb_backend *b = be_of(cat);
    struct fdb_ext_dirent_val v;
    uint8_t enc[FDB_EXT_DIRENT_ENC_SIZE];
    size_t len = 0;
    struct fdb_key k;

    (void)txn;
    if (b == NULL || !name_len_ok(name, MDS_MAX_NAME)) {
        return MDS_ERR_INVAL;
    }
    v.target_fileid = target_fileid;
    v.anchor_id = anchor_id;
    v.owner_mds_id = owner_mds_id;
    v.target_type = target_type;
    if (!fdb_ext_dirent_encode(&v, enc, sizeof(enc), &len)) {
        return MDS_ERR_INVAL;
    }
    fdb_key_ext_dirent(&k, &b->prefix, parent, name);
    return run_set(b, "ext_dirent_put", &k, enc, len);
}

static enum mds_status fdb_ext_dirent_del(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                          uint64_t parent, const char *name)
{
    struct fdb_backend *b = be_of(cat);
    struct fdb_key k;

    (void)txn;
    if (b == NULL || name == NULL) {
        return MDS_ERR_INVAL;
    }
    if (!name_len_ok(name, MDS_MAX_NAME)) {
        return MDS_ERR_NOTFOUND;
    }
    fdb_key_ext_dirent(&k, &b->prefix, parent, name);
    return run_clear(b, "ext_dirent_del", &k, true);
}

/* Upsert keyed anchor_id; the name may be empty (memdb parity). */
static enum mds_status fdb_link_anchor_put(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                           uint64_t anchor_id, uint32_t remote_mds_id,
                                           uint64_t parent_fileid, const char *name)
{
    struct fdb_backend *b = be_of(cat);
    struct fdb_link_anchor_val v;
    uint8_t enc[FDB_LINK_ANCHOR_ENC_MAX];
    size_t len = 0;
    struct fdb_key k;

    (void)txn;
    if (b == NULL || name == NULL || strnlen(name, MDS_MAX_NAME + 1U) > MDS_MAX_NAME) {
        return MDS_ERR_INVAL;
    }
    memset(&v, 0, sizeof(v));
    v.parent_fileid = parent_fileid;
    v.remote_mds_id = remote_mds_id;
    (void)snprintf(v.name, sizeof(v.name), "%s", name);
    if (!fdb_link_anchor_encode(&v, enc, sizeof(enc), &len)) {
        return MDS_ERR_INVAL;
    }
    fdb_key_link_anchor(&k, &b->prefix, anchor_id);
    return run_set(b, "link_anchor_put", &k, enc, len);
}

static enum mds_status fdb_link_anchor_del(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                           uint64_t anchor_id)
{
    struct fdb_backend *b = be_of(cat);
    struct fdb_key k;

    (void)txn;
    if (b == NULL) {
        return MDS_ERR_INVAL;
    }
    fdb_key_link_anchor(&k, &b->prefix, anchor_id);
    return run_clear(b, "link_anchor_del", &k, true);
}

/* -----------------------------------------------------------------------
 * Registration.  prealloc_pool_* stay NULL (as on memdb: the DS
 * prealloc engine then runs in-memory only) and backend_client_stats
 * stays NULL (no client-side counters are exported yet).
 * ----------------------------------------------------------------------- */

void catalogue_fdb_ext_register(struct mds_authority_ops *ops)
{
    if (ops == NULL) {
        return;
    }
    ops->inline_get                    = fdb_inline_get;
    ops->inline_put                    = fdb_inline_put;
    ops->inline_del                    = fdb_inline_del;
    ops->xattr_get                     = fdb_xattr_get;
    ops->xattr_put                     = fdb_xattr_put;
    ops->xattr_del                     = fdb_xattr_del;
    ops->xattr_list                    = fdb_xattr_list;
    ops->xattr_exists                  = fdb_xattr_exists;
    ops->stripe_map_get                = fdb_stripe_map_get;
    ops->stripe_map_put                = fdb_stripe_map_put;
    ops->stripe_map_del                = fdb_stripe_map_del;
    ops->stripe_map_scan               = fdb_stripe_map_scan;
    ops->ds_get                        = fdb_ds_get;
    ops->ds_put                        = fdb_ds_put;
    ops->ds_del                        = fdb_ds_del;
    ops->ds_list                       = fdb_ds_list;
    ops->ds_provision_get              = fdb_ds_provision_get;
    ops->ds_provision_put              = fdb_ds_provision_put;
    ops->ds_provision_del              = fdb_ds_provision_del;
    ops->quota_rule_get                = fdb_quota_rule_get;
    ops->quota_rule_put                = fdb_quota_rule_put;
    ops->quota_usage_get               = fdb_quota_usage_get;
    ops->quota_usage_put               = fdb_quota_usage_put;
    ops->gc_enqueue                    = fdb_gc_enqueue;
    ops->gc_peek                       = fdb_gc_peek;
    ops->gc_dequeue                    = fdb_gc_dequeue;
    ops->gc_count                      = fdb_gc_count;
    ops->gc_peek_batch                 = fdb_gc_peek_batch;
    ops->remove_pending_enqueue        = fdb_remove_pending_enqueue;
    ops->remove_pending_enqueue_unlink = fdb_remove_pending_enqueue_unlink;
    ops->remove_pending_peek_batch     = fdb_remove_pending_peek_batch;
    ops->remove_pending_claim          = fdb_remove_pending_claim;
    ops->remove_pending_complete       = fdb_remove_pending_complete;
    ops->remove_pending_bump_retry     = fdb_remove_pending_bump_retry;
    ops->remove_pending_count          = fdb_remove_pending_count;
    ops->remove_pending_scan_all       = fdb_remove_pending_scan_all;
    ops->shard_fileid_get              = fdb_shard_fileid_get;
    ops->shard_fileid_put              = fdb_shard_fileid_put;
    ops->shard_fileid_del              = fdb_shard_fileid_del;
    ops->ext_dirent_get                = fdb_ext_dirent_get;
    ops->ext_dirent_put                = fdb_ext_dirent_put;
    ops->ext_dirent_del                = fdb_ext_dirent_del;
    ops->link_anchor_put               = fdb_link_anchor_put;
    ops->link_anchor_del               = fdb_link_anchor_del;
}
