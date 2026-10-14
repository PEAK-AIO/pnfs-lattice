/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * catalogue_fdb_coord.c -- FoundationDB backend: coordination slots.
 *
 * Every slot is ONE fdb_run_txn (fdb_txn.h); the mds_cat_txn token is
 * never enlisted (C6).  Reads that can be issued together are started
 * as parallel futures and waited on afterwards, so a slot pays one
 * round trip per dependent wave, not per key.  Enumerating slots
 * materialise a bounded page inside a read-only transaction and
 * deliver it after fdb_run_txn returned (C1/C2): a callback may
 * re-enter this handle and a retried attempt never redelivers.
 * Statuses follow the memdb reference implementation
 * (catalogue_memdb.c), the parity oracle for every semantic detail.
 *
 * Rows and their indexes (fdb_keys.h; values in fdb_codec.h):
 *   LAYOUT_STATE + stateid          LAYOUT_BY_FILE, LAYOUT_BY_CLIENT,
 *                                   DS_LAYOUT_IDX (one per DS in the row)
 *   OPEN + stateid                  OPEN_BY_FILE, OPEN_BY_CLIENT
 *   LOCK + fileid + lock_id         LOCK_BY_OWNER
 *   DELEG + stateid                 DELEG_BY_FILE, DELEG_BY_CLIENT
 *   CLIENT + clientid               --
 *   SESSION + session_id            SESSION_BY_CLIENT
 *   SLOT + session_id + slot_id     --
 *   RECOVERY + clientid             RECOVERY_BY_OWNER
 *   JOURNAL + txn_id + role         --
 *
 * Index keys are written and cleared in the SAME transaction as their
 * row and are authoritative only together with it: every index-driven
 * scan point-reads the rows in one parallel wave and skips a key whose
 * row is absent or names another (fileid, clientid) -- such an orphan
 * can only be left by a re-put that rebinds a key the protocol never
 * rebinds (a stateid belongs to one file and one client for its life).
 * layout_scan_for_file alone trusts LAYOUT_BY_FILE without the row
 * read: a false positive can only cause an over-recall, which is safe.
 * The hot-path puts (layout_grant, open/lock/deleg/client/session/slot
 * put) are therefore blind writes with no read, like RonDB's writeTuple.
 *
 * LAYOUT_STATE is keyed by stateid.other alone (memdb keys the row by
 * (fileid, stateid)): a grant or union under a stateid already bound to
 * a different fileid rebinds it, clearing the old file's index keys in
 * the same transaction; layout_return with fileid != 0 reports
 * MDS_ERR_NOTFOUND when the row names another file (memdb parity) and
 * matches the stateid alone when fileid == 0.
 *
 * Transaction shapes (R = point read, RR = range read, W = set/clear,
 * || = parallel in one wave, -> = dependent wave; every mutating
 * transaction adds the runner's witness set + READ conflict range and
 * commits once):
 *   journal_put            W row                                  (blind)
 *   journal_get            R row
 *   journal_del            R row; W clear                         (NOTFOUND if absent)
 *   journal_scan           per page: RR JOURNAL (sort keys); then per batch
 *                          of 16: R || ... || R rows -> deliver oldest first
 *   layout_grant           W row, W by_file, W by_client, W ds_idx x n (blind)
 *   layout_grant_union     R row; absent: as grant / present: W union row,
 *                          [clear + W client-keyed indexes when the clientid
 *                          moved] / other fileid: clear old indexes + as grant
 *   layoutget_fused        R stripe hdr || RR stripe entries; W row +
 *                          indexes (DS list derived from the entries)
 *   layout_return          R row; W clear row + by_file + by_client + ds_idx
 *   layout_get_by_stateid  R row
 *   layout_scan_for_file   RR by_file limit 1
 *   layout_del_all_for_client per batch of 64: RR by_client -> R || ... || R
 *                          rows; W clears (one commit per batch)
 *   ds_layout_idx_scan     per page: RR ds_idx -> R || ... || R rows (validate
 *                          clientid, fileid, DS membership; dedupe pairs)
 *   layout_iter_file       per page: RR by_file -> R || ... || R rows
 *   recovery_put           R row; W row, W by_owner [clear old by_owner]
 *   recovery_del           R row; W clear row + by_owner              (absent: OK)
 *   recovery_get           R row
 *   recovery_list          per page: RR by_owner(f) -> R || ... || R rows, then
 *                          the same for owner 0; f == 0: RR RECOVERY table
 *   open_put / deleg_put   W row, W by_file, W by_client             (blind)
 *   open_get / deleg_get   R row
 *   open_del / deleg_del   R row; W clear row + by_file + by_client
 *   open/deleg_scan_file, _scan_client
 *                          per page: RR index -> R || ... || R rows
 *   lock_put               W row, W by_owner                         (blind)
 *   lock_del               R row; W clear row + by_owner
 *   lock_test              RR LOCK + fileid (paged inside one read-only txn)
 *   lock_scan_file         per page: RR LOCK + fileid
 *   lock_scan_owner        per page: RR by_owner prefix -> R || ... || R rows
 *   lock_reap_client       per batch of 64: RR by_owner + clientid; W clears
 *   client_put             W row                                     (blind)
 *   client_get             R row
 *   client_del             R row; W clear
 *   session_put            W row, W by_client                        (blind)
 *   session_get            R row
 *   session_del            R row; W clear row + by_client
 *   session_scan_client    per page: RR by_client -> R || ... || R rows
 *   slot_put               W row                                     (blind)
 *   slot_get               R row
 */

#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "catalogue_fdb.h"
#include "catalogue_internal.h"
#include "fdb_codec.h"
#include "fdb_keys.h"
#include "fdb_txn.h"
#include "layout_ds_ids.h"
#include "layout_range.h"
#include "open_state.h"

/* Index entries materialised per read-only page (one transaction each). */
#define FDB_COORD_PAGE            256U
/* Rows cleared per mutating batch of the bulk deletes. */
#define FDB_COORD_DEL_BATCH       64U
/* Bulk deletes stop after this many batches: a store that gains rows
 * faster than a batch clears them (a grant storm on an expiring client)
 * must not turn the caller into an unbounded loop; MDS_ERR_DELAY then. */
#define FDB_COORD_DEL_MAX_BATCHES 1024U
/* Lock rows examined per LOCKT before the slot gives up (MDS_ERR_IO):
 * FDB_COORD_PAGE rows per range read. */
#define FDB_LOCK_TEST_MAX_PAGES   256U
/* Records a journal scan can order.  The scan sorts by created_at_ns
 * (memdb parity), which needs every sort key in memory before the first
 * delivery; the bound (16384 x 24 bytes) is far above any live 2PC
 * population and keeps that memory fixed.  Beyond it the scan refuses
 * with MDS_ERR_NOSPC rather than deliver an unordered subset. */
#define FDB_JOURNAL_SCAN_MAX      16384U
/* Journal records fetched per read-only batch after sorting. */
#define FDB_JOURNAL_FETCH         16U

/* LAYOUTIOMODE4_RW: the union keeps a stateid ever granted RW at RW
 * (compound.h drags in the RPC headers; same local constant as memdb). */
#define FDB_LAYOUTIOMODE_RW       2U

/* RFC 8881 nfs_lock_type4; blocking variants normalise to their plain
 * type for conflict purposes (lock_state.h drags in compound.h). */
#define FDB_READ_LT               1U
#define FDB_WRITE_LT              2U
#define FDB_READW_LT              3U
#define FDB_WRITEW_LT             4U

_Static_assert(FDB_LAYOUT_DS_MAX == MDS_LAYOUT_DS_ID_MAX,
               "layout row DS bound must equal the dispatcher ceiling");
_Static_assert(FDB_COORD_PAGE <= 0x7FFFFFFFU && FDB_COORD_DEL_BATCH <= FDB_COORD_PAGE,
               "page bounds must fit the fdb_c int limit");

/* Zero-length index values are written through this pointer so the
 * client never sees a NULL value pointer. */
static const uint8_t g_empty_val[1] = { 0 };

/* -----------------------------------------------------------------------
 * Small helpers
 * ----------------------------------------------------------------------- */

static struct fdb_backend *be_of(const struct mds_catalogue *cat)
{
    return cat != NULL ? cat->backend_private : NULL;
}

static uint64_t realtime_ns(void)
{
    struct timespec ts;

    if (clock_gettime(CLOCK_REALTIME, &ts) != 0) {
        return 0;
    }
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static void set_empty(FDBTransaction *tr, const struct fdb_key *k)
{
    fdb_txn_set(tr, k, g_empty_val, 0);
}

/* -----------------------------------------------------------------------
 * Key builders (layouts documented in fdb_keys.h)
 * ----------------------------------------------------------------------- */

static void key_sid(struct fdb_key *k, const struct fdb_key_prefix *p, enum fdb_key_type t,
                    const uint8_t other[NFS4_OTHER_SIZE])
{
    fdb_key_init(k, p, t);
    fdb_key_bytes(k, other, NFS4_OTHER_SIZE);
}

static void key_u64_sid(struct fdb_key *k, const struct fdb_key_prefix *p, enum fdb_key_type t,
                        uint64_t id, const uint8_t other[NFS4_OTHER_SIZE])
{
    fdb_key_init(k, p, t);
    fdb_key_be64(k, id);
    fdb_key_bytes(k, other, NFS4_OTHER_SIZE);
}

static void key_u64_prefix(struct fdb_key *k, const struct fdb_key_prefix *p,
                           enum fdb_key_type t, uint64_t id)
{
    fdb_key_init(k, p, t);
    fdb_key_be64(k, id);
}

/* Tail of a DS_LAYOUT_IDX key after the be32 ds_id: be64 clientid + be64
 * fileid + stateid_other. */
#define FDB_DS_LAYOUT_IDX_TAIL (2U * FDB_KEY_BE64_LEN + NFS4_OTHER_SIZE)

static void key_ds_layout_idx(struct fdb_key *k, const struct fdb_key_prefix *p, uint32_t ds_id,
                              uint64_t clientid, uint64_t fileid,
                              const uint8_t other[NFS4_OTHER_SIZE])
{
    fdb_key_init(k, p, FDB_KT_DS_LAYOUT_IDX);
    fdb_key_be32(k, ds_id);
    fdb_key_be64(k, clientid);
    fdb_key_be64(k, fileid);
    fdb_key_bytes(k, other, NFS4_OTHER_SIZE);
}

/* A LOCK row key after the type byte, and the tail of a LOCK_BY_OWNER
 * key after its owner: be64 fileid + be64 lock_id. */
#define FDB_LOCK_KEY_TAIL (2U * FDB_KEY_BE64_LEN)

static void key_lock(struct fdb_key *k, const struct fdb_key_prefix *p, uint64_t fileid,
                     uint64_t lock_id)
{
    fdb_key_init(k, p, FDB_KT_LOCK);
    fdb_key_be64(k, fileid);
    fdb_key_be64(k, lock_id);
}

/* LOCK_BY_OWNER + be64 clientid + be32 owner_len + owner: the per-owner
 * prefix; the row part (be64 fileid + be64 lock_id) is appended by the
 * caller when a full key is wanted. */
static void key_lock_owner_prefix(struct fdb_key *k, const struct fdb_key_prefix *p,
                                  uint64_t clientid, const uint8_t *owner, uint32_t owner_len)
{
    fdb_key_init(k, p, FDB_KT_LOCK_BY_OWNER);
    fdb_key_be64(k, clientid);
    fdb_key_be32(k, owner_len);
    if (owner_len > 0) {
        fdb_key_bytes(k, owner, owner_len);
    }
}

static void key_lock_by_owner(struct fdb_key *k, const struct fdb_key_prefix *p,
                              const struct mds_coord_lock_row *r)
{
    key_lock_owner_prefix(k, p, r->clientid, r->owner, r->owner_len);
    fdb_key_be64(k, r->fileid);
    fdb_key_be64(k, r->lock_id);
}

static void key_session(struct fdb_key *k, const struct fdb_key_prefix *p,
                        const uint8_t session_id[16])
{
    fdb_key_init(k, p, FDB_KT_SESSION);
    fdb_key_bytes(k, session_id, 16);
}

static void key_session_by_client(struct fdb_key *k, const struct fdb_key_prefix *p,
                                  uint64_t clientid, const uint8_t session_id[16])
{
    fdb_key_init(k, p, FDB_KT_SESSION_BY_CLIENT);
    fdb_key_be64(k, clientid);
    fdb_key_bytes(k, session_id, 16);
}

static void key_slot(struct fdb_key *k, const struct fdb_key_prefix *p,
                     const uint8_t session_id[16], uint32_t slot_id)
{
    fdb_key_init(k, p, FDB_KT_SLOT);
    fdb_key_bytes(k, session_id, 16);
    fdb_key_be32(k, slot_id);
}

static void key_recovery_by_owner(struct fdb_key *k, const struct fdb_key_prefix *p,
                                  uint32_t owner, uint64_t clientid)
{
    fdb_key_init(k, p, FDB_KT_RECOVERY_BY_OWNER);
    fdb_key_be32(k, owner);
    fdb_key_be64(k, clientid);
}

/* A JOURNAL key after the type byte: be64 txn_id + u8 role. */
#define FDB_JOURNAL_KEY_TAIL (FDB_KEY_BE64_LEN + FDB_KEY_U8_LEN)

static void key_journal(struct fdb_key *k, const struct fdb_key_prefix *p, uint64_t txn_id,
                        uint8_t role)
{
    fdb_key_init(k, p, FDB_KT_JOURNAL);
    fdb_key_be64(k, txn_id);
    fdb_key_u8(k, role);
}

/* -----------------------------------------------------------------------
 * Read primitives.  A point read is decoded straight from the client's
 * value buffer (valid until the future is destroyed), so no slot needs a
 * copy buffer sized for the largest possible value.
 * ----------------------------------------------------------------------- */

struct rval {
    FDBFuture     *f;
    const uint8_t *val;
    size_t         len;
    bool           found;
};

/* Wait for the point read @p f; on success *rv owns the future. */
static fdb_error_t raw_wait(FDBFuture *f, struct rval *rv)
{
    fdb_bool_t present = 0;
    const uint8_t *val = NULL;
    int vlen = 0;
    fdb_error_t err;

    rv->f = f;
    rv->val = NULL;
    rv->len = 0;
    rv->found = false;
    if (f == NULL) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    err = fdb_txn_wait(f);
    if (err == 0) {
        err = fdb_future_get_value(f, &present, &val, &vlen);
    }
    if (err != 0) {
        fdb_future_destroy(f);
        rv->f = NULL;
        return err;
    }
    if (present != 0 && vlen >= 0) {
        rv->val = val;
        rv->len = (size_t)vlen;
        rv->found = true;
    }
    return 0;
}

static fdb_error_t raw_get(FDBTransaction *tr, const struct fdb_key *k, struct rval *rv)
{
    return raw_wait(fdb_txn_get_start(tr, k, false), rv);
}

static void raw_release(struct rval *rv)
{
    if (rv->f != NULL) {
        fdb_future_destroy(rv->f);
        rv->f = NULL;
    }
    rv->val = NULL;
    rv->len = 0;
}

/* Destroy every started future of a parallel wave from @p first on. */
static void futures_drop(FDBFuture **fs, uint32_t first, uint32_t n)
{
    uint32_t i;

    for (i = first; i < n; i++) {
        if (fs[i] != NULL) {
            fdb_future_destroy(fs[i]);
            fs[i] = NULL;
        }
    }
}

struct kvs {
    FDBFuture         *f;
    const FDBKeyValue *kvs;
    int                count;
    bool               more;
};

static fdb_error_t range_read(FDBTransaction *tr, const struct fdb_key_range *r, int limit,
                              struct kvs *out)
{
    fdb_error_t err;

    out->f = fdb_txn_get_range_start(tr, r, limit, false, false);
    err = fdb_txn_get_range_wait(out->f, &out->kvs, &out->count, &out->more);
    if (err != 0 && out->f != NULL) {
        fdb_future_destroy(out->f);
        out->f = NULL;
    }
    return err;
}

static void kvs_release(struct kvs *k)
{
    if (k->f != NULL) {
        fdb_future_destroy(k->f);
        k->f = NULL;
    }
    k->kvs = NULL;
    k->count = 0;
}

/* Range read of every key under @p base that sorts after @p cursor (a
 * full key of the previous page; len 0 = from the start). */
static fdb_error_t idx_range_read(FDBTransaction *tr, const struct fdb_key *base,
                                  const struct fdb_key *cursor, int limit, struct kvs *out)
{
    struct fdb_key_range r;

    out->f = NULL;
    out->kvs = NULL;
    out->count = 0;
    out->more = false;
    if (!fdb_key_range_prefix(&r, base)) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    if (cursor->len > 0) {
        r.begin = *cursor;
        fdb_key_u8(&r.begin, 0); /* strictly after the cursor key */
    }
    return range_read(tr, &r, limit, out);
}

static void cursor_set(struct fdb_key *cursor, const FDBKeyValue *kv)
{
    cursor->len = 0;
    cursor->overflow = false;
    if (kv->key_length > 0) {
        fdb_key_bytes(cursor, kv->key, (size_t)kv->key_length);
    }
}

/* The @p want bytes that follow @p base in the key of @p kv; NULL when
 * the key has another length (a corrupt or foreign key). */
static const uint8_t *key_suffix(const struct fdb_key *base, const FDBKeyValue *kv, size_t want)
{
    if (kv->key_length < 0 || (size_t)kv->key_length != base->len + want) {
        return NULL;
    }
    return kv->key + base->len;
}

/* -----------------------------------------------------------------------
 * Layout state
 * ----------------------------------------------------------------------- */

/* Working state of the layout slots that read or write whole rows.  The
 * DS list of a row can carry FDB_LAYOUT_DS_MAX ids, so the struct is
 * heap allocated once per call (never on a worker stack). */
struct layout_ctx {
    struct fdb_backend   *b;
    uint8_t               other[NFS4_OTHER_SIZE];
    struct fdb_layout_hdr want;         /**< The grant being written. */
    const uint32_t       *want_ds;      /**< Caller's DS list (borrowed). */
    uint64_t              match_fileid; /**< layout_return: 0 = any file. */
    struct fdb_layout_hdr row;          /**< The stored row, once read. */
    uint32_t              row_ds[FDB_LAYOUT_DS_MAX];
    uint8_t               enc[FDB_LAYOUT_ENC_MAX];
};

/* Write the row and every index key of @p h. */
static int layout_write(FDBTransaction *tr, const struct fdb_key_prefix *p,
                        const uint8_t other[NFS4_OTHER_SIZE], const struct fdb_layout_hdr *h,
                        const uint32_t *ds_ids, uint8_t *enc, size_t enc_cap)
{
    struct fdb_key k;
    size_t len = 0;
    uint32_t i;

    if (!fdb_layout_encode(h, ds_ids, enc, enc_cap, &len)) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    key_sid(&k, p, FDB_KT_LAYOUT_STATE, other);
    fdb_txn_set(tr, &k, enc, len);
    key_u64_sid(&k, p, FDB_KT_LAYOUT_BY_FILE, h->fileid, other);
    set_empty(tr, &k);
    key_u64_sid(&k, p, FDB_KT_LAYOUT_BY_CLIENT, h->clientid, other);
    set_empty(tr, &k);
    for (i = 0; i < h->ds_count; i++) {
        key_ds_layout_idx(&k, p, ds_ids[i], h->clientid, h->fileid, other);
        set_empty(tr, &k);
    }
    return 0;
}

/* Clear the client-keyed index keys of the stored row @p h. */
static void layout_clear_client_keys(FDBTransaction *tr, const struct fdb_key_prefix *p,
                                     const uint8_t other[NFS4_OTHER_SIZE],
                                     const struct fdb_layout_hdr *h, const uint32_t *ds_ids)
{
    struct fdb_key k;
    uint32_t i;

    key_u64_sid(&k, p, FDB_KT_LAYOUT_BY_CLIENT, h->clientid, other);
    fdb_txn_clear(tr, &k);
    for (i = 0; i < h->ds_count; i++) {
        key_ds_layout_idx(&k, p, ds_ids[i], h->clientid, h->fileid, other);
        fdb_txn_clear(tr, &k);
    }
}

/* Clear the row and every index key of the stored row @p h. */
static void layout_clear(FDBTransaction *tr, const struct fdb_key_prefix *p,
                         const uint8_t other[NFS4_OTHER_SIZE], const struct fdb_layout_hdr *h,
                         const uint32_t *ds_ids)
{
    struct fdb_key k;

    key_sid(&k, p, FDB_KT_LAYOUT_STATE, other);
    fdb_txn_clear(tr, &k);
    key_u64_sid(&k, p, FDB_KT_LAYOUT_BY_FILE, h->fileid, other);
    fdb_txn_clear(tr, &k);
    layout_clear_client_keys(tr, p, other, h, ds_ids);
}

/* Read the stored row into c->row / c->row_ds; *found false when absent. */
static fdb_error_t layout_read_row(FDBTransaction *tr, struct layout_ctx *c, bool *found)
{
    struct fdb_key k;
    struct rval rv;
    fdb_error_t err;

    *found = false;
    key_sid(&k, &c->b->prefix, FDB_KT_LAYOUT_STATE, c->other);
    err = raw_get(tr, &k, &rv);
    if (err != 0) {
        return err;
    }
    if (rv.found) {
        if (!fdb_layout_decode(rv.val, rv.len, &c->row, c->row_ds, FDB_LAYOUT_DS_MAX)) {
            raw_release(&rv);
            return FDB_ERR_PLATFORM_ERROR; /* corrupt row */
        }
        *found = true;
    }
    raw_release(&rv);
    return 0;
}

static int layout_grant_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct layout_ctx *c = arg;
    int rc;

    rc = layout_write(tr, &c->b->prefix, c->other, &c->want, c->want_ds, c->enc,
                      sizeof(c->enc));
    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

/* Renewal union (C3: read, decide and write in one transaction).  The
 * present row keeps a SUPERSET of every range granted under the
 * stateid, a monotonic seqid, an RW-dominant iomode, the newest
 * clientid and the DS list of the first grant (memdb / RonDB parity). */
static int layout_union_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct layout_ctx *c = arg;
    const struct fdb_key_prefix *p = &c->b->prefix;
    struct fdb_layout_hdr merged;
    bool found = false;
    fdb_error_t err;
    int rc;

    err = layout_read_row(tr, c, &found);
    if (err != 0) {
        return (int)err;
    }
    if (!found || c->row.fileid != c->want.fileid) {
        if (found) {
            /* The stateid was bound to another file: rebind. */
            layout_clear(tr, p, c->other, &c->row, c->row_ds);
        }
        rc = layout_write(tr, p, c->other, &c->want, c->want_ds, c->enc, sizeof(c->enc));
        if (rc != 0) {
            return rc;
        }
        *st_out = MDS_OK;
        return FDB_BODY_COMMIT;
    }
    merged = c->row;
    layout_range_union_saturating(c->row.offset, c->row.length, c->want.offset,
                                  c->want.length, &merged.offset, &merged.length);
    if (c->want.seqid > merged.seqid) {
        merged.seqid = c->want.seqid;
    }
    merged.iomode = (c->row.iomode == FDB_LAYOUTIOMODE_RW ||
                     c->want.iomode == FDB_LAYOUTIOMODE_RW) ? FDB_LAYOUTIOMODE_RW
                                                            : c->want.iomode;
    merged.clientid = c->want.clientid;
    if (merged.clientid != c->row.clientid) {
        layout_clear_client_keys(tr, p, c->other, &c->row, c->row_ds);
    }
    rc = layout_write(tr, p, c->other, &merged, c->row_ds, c->enc, sizeof(c->enc));
    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static int layout_return_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct layout_ctx *c = arg;
    bool found = false;
    fdb_error_t err;

    err = layout_read_row(tr, c, &found);
    if (err != 0) {
        return (int)err;
    }
    if (!found || (c->match_fileid != 0 && c->row.fileid != c->match_fileid)) {
        *st_out = MDS_ERR_NOTFOUND;
        return FDB_BODY_DONE;
    }
    layout_clear(tr, &c->b->prefix, c->other, &c->row, c->row_ds);
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

/* Argument checks shared by the two grant slots; fills c->want. */
static enum mds_status layout_ctx_prepare(struct layout_ctx *c, struct fdb_backend *b,
                                          uint64_t clientid, uint64_t fileid, uint32_t iomode,
                                          uint64_t offset, uint64_t length,
                                          const struct nfs4_stateid *stateid,
                                          const uint32_t *ds_ids, uint32_t ds_count)
{
    if (stateid == NULL || ds_count > FDB_LAYOUT_DS_MAX || (ds_count > 0 && ds_ids == NULL)) {
        return MDS_ERR_INVAL;
    }
    c->b = b;
    memcpy(c->other, stateid->other, sizeof(c->other));
    c->want.clientid = clientid;
    c->want.fileid = fileid;
    c->want.offset = offset;
    c->want.length = length;
    c->want.iomode = iomode;
    c->want.seqid = stateid->seqid;
    c->want.grant_owner_mds_id = 0;
    c->want.ds_count = ds_count;
    c->want_ds = ds_ids;
    return MDS_OK;
}

static enum mds_status layout_grant_run(const struct mds_catalogue *cat, uint64_t clientid,
                                        uint64_t fileid, uint32_t iomode, uint64_t offset,
                                        uint64_t length, const struct nfs4_stateid *stateid,
                                        const uint32_t *ds_ids, uint32_t ds_count,
                                        fdb_txn_body body, const char *op)
{
    struct layout_ctx *c;
    enum mds_status st;

    if (be_of(cat) == NULL) {
        return MDS_ERR_INVAL;
    }
    c = calloc(1, sizeof(*c));
    if (c == NULL) {
        return MDS_ERR_NOMEM;
    }
    st = layout_ctx_prepare(c, be_of(cat), clientid, fileid, iomode, offset, length, stateid,
                            ds_ids, ds_count);
    if (st == MDS_OK) {
        st = fdb_run_txn(c->b, FDB_TXN_MUTATING, op, body, c);
    }
    free(c);
    return st;
}

static enum mds_status fdb_layout_grant(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                        uint64_t clientid, uint64_t fileid, uint32_t iomode,
                                        uint64_t offset, uint64_t length,
                                        const struct nfs4_stateid *stateid,
                                        const uint32_t *ds_ids, uint32_t ds_count)
{
    (void)txn;
    return layout_grant_run(cat, clientid, fileid, iomode, offset, length, stateid, ds_ids,
                            ds_count, layout_grant_body, "layout_grant");
}

static enum mds_status fdb_layout_grant_union(struct mds_catalogue *cat,
                                              struct mds_cat_txn *txn, uint64_t clientid,
                                              uint64_t fileid, uint32_t iomode,
                                              uint64_t offset, uint64_t length,
                                              const struct nfs4_stateid *stateid,
                                              const uint32_t *ds_ids, uint32_t ds_count)
{
    (void)txn;
    return layout_grant_run(cat, clientid, fileid, iomode, offset, length, stateid, ds_ids,
                            ds_count, layout_union_body, "layout_grant_union");
}

static enum mds_status fdb_layout_return(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                         const uint8_t stateid_other[12], uint64_t clientid,
                                         uint64_t fileid, const uint32_t *ds_ids,
                                         uint32_t ds_count)
{
    struct layout_ctx *c;
    enum mds_status st;

    (void)txn;
    (void)clientid;
    (void)ds_ids;
    (void)ds_count;
    if (be_of(cat) == NULL || stateid_other == NULL) {
        return MDS_ERR_INVAL;
    }
    c = calloc(1, sizeof(*c));
    if (c == NULL) {
        return MDS_ERR_NOMEM;
    }
    c->b = be_of(cat);
    memcpy(c->other, stateid_other, sizeof(c->other));
    c->match_fileid = fileid;
    st = fdb_run_txn(c->b, FDB_TXN_MUTATING, "layout_return", layout_return_body, c);
    free(c);
    return st;
}

/* --- layout_get_by_stateid --------------------------------------------- */

struct layout_get_ctx {
    struct fdb_backend   *b;
    const uint8_t        *other;
    struct fdb_layout_hdr hdr;
};

static int layout_get_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct layout_get_ctx *c = arg;
    struct fdb_key k;
    struct rval rv;
    fdb_error_t err;
    bool ok;

    key_sid(&k, &c->b->prefix, FDB_KT_LAYOUT_STATE, c->other);
    err = raw_get(tr, &k, &rv);
    if (err != 0) {
        return (int)err;
    }
    if (!rv.found) {
        raw_release(&rv);
        *st_out = MDS_ERR_NOTFOUND;
        return FDB_BODY_DONE;
    }
    ok = fdb_layout_decode(rv.val, rv.len, &c->hdr, NULL, 0);
    raw_release(&rv);
    if (!ok) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static enum mds_status fdb_layout_get_by_stateid(struct mds_catalogue *cat,
                                                 const uint8_t stateid_other[12],
                                                 uint64_t *clientid, uint64_t *fileid,
                                                 uint32_t *iomode, uint64_t *offset,
                                                 uint64_t *length, uint32_t *seqid)
{
    struct layout_get_ctx c;
    enum mds_status st;

    if (be_of(cat) == NULL || stateid_other == NULL) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.b = be_of(cat);
    c.other = stateid_other;
    st = fdb_run_txn(c.b, FDB_TXN_READONLY, "layout_get_by_stateid", layout_get_body, &c);
    if (st != MDS_OK) {
        return st;
    }
    if (clientid != NULL) {
        *clientid = c.hdr.clientid;
    }
    if (fileid != NULL) {
        *fileid = c.hdr.fileid;
    }
    if (iomode != NULL) {
        *iomode = c.hdr.iomode;
    }
    if (offset != NULL) {
        *offset = c.hdr.offset;
    }
    if (length != NULL) {
        *length = c.hdr.length;
    }
    if (seqid != NULL) {
        *seqid = c.hdr.seqid;
    }
    return MDS_OK;
}

/* --- layout_scan_for_file ---------------------------------------------- */

struct layout_scan_ctx {
    struct fdb_backend *b;
    uint64_t            fileid;
    bool                has_layout;
};

static int layout_scan_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct layout_scan_ctx *c = arg;
    struct fdb_key base;
    struct fdb_key cursor;
    struct kvs page;
    fdb_error_t err;

    cursor.len = 0;
    cursor.overflow = false;
    key_u64_prefix(&base, &c->b->prefix, FDB_KT_LAYOUT_BY_FILE, c->fileid);
    err = idx_range_read(tr, &base, &cursor, 1, &page);
    if (err != 0) {
        return (int)err;
    }
    c->has_layout = (page.count > 0);
    kvs_release(&page);
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static enum mds_status fdb_layout_scan_for_file(struct mds_catalogue *cat, uint64_t fileid,
                                                bool *has_layout)
{
    struct layout_scan_ctx c;
    enum mds_status st;

    if (be_of(cat) == NULL || has_layout == NULL) {
        return MDS_ERR_INVAL;
    }
    *has_layout = false;
    memset(&c, 0, sizeof(c));
    c.b = be_of(cat);
    c.fileid = fileid;
    st = fdb_run_txn(c.b, FDB_TXN_READONLY, "layout_scan_for_file", layout_scan_body, &c);
    if (st == MDS_OK) {
        *has_layout = c.has_layout;
    }
    return st;
}

/* --- layout_del_all_for_client ----------------------------------------- */

struct layout_del_ctx {
    struct layout_ctx  *lc;      /**< Row decode scratch (heap). */
    uint64_t            clientid;
    bool                more;
    FDBFuture          *fs[FDB_COORD_DEL_BATCH];
    uint8_t             others[FDB_COORD_DEL_BATCH][NFS4_OTHER_SIZE];
};

/* One batch: the first FDB_COORD_DEL_BATCH by-client index keys, their
 * rows in one parallel wave, then every clear.  Re-runnable: each
 * attempt re-reads the index. */
static int layout_del_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct layout_del_ctx *c = arg;
    const struct fdb_key_prefix *p = &c->lc->b->prefix;
    struct fdb_key base;
    struct fdb_key cursor;
    struct fdb_key k;
    struct kvs page;
    uint32_t n = 0;
    uint32_t i;
    fdb_error_t err = 0;

    cursor.len = 0;
    cursor.overflow = false;
    key_u64_prefix(&base, p, FDB_KT_LAYOUT_BY_CLIENT, c->clientid);
    err = idx_range_read(tr, &base, &cursor, (int)FDB_COORD_DEL_BATCH, &page);
    if (err != 0) {
        return (int)err;
    }
    c->more = page.more;
    for (i = 0; i < (uint32_t)page.count && n < FDB_COORD_DEL_BATCH; i++) {
        FDBKeyValue kv;
        const uint8_t *other;

        fdb_kv_at(page.kvs, (int)i, &kv);
        other = key_suffix(&base, &kv, NFS4_OTHER_SIZE);
        if (other == NULL) {
            kvs_release(&page);
            futures_drop(c->fs, 0, n);
            return FDB_ERR_PLATFORM_ERROR; /* corrupt index key */
        }
        memcpy(c->others[n], other, NFS4_OTHER_SIZE);
        key_sid(&k, p, FDB_KT_LAYOUT_STATE, other);
        c->fs[n] = fdb_txn_get_start(tr, &k, false);
        n++;
    }
    kvs_release(&page);
    for (i = 0; i < n; i++) {
        struct rval rv;
        fdb_error_t e2 = raw_wait(c->fs[i], &rv);

        c->fs[i] = NULL;
        if (e2 != 0) {
            if (err == 0) {
                err = e2;
            }
            continue;
        }
        if (err == 0) {
            memcpy(c->lc->other, c->others[i], NFS4_OTHER_SIZE);
            if (rv.found) {
                if (!fdb_layout_decode(rv.val, rv.len, &c->lc->row, c->lc->row_ds,
                                       FDB_LAYOUT_DS_MAX)) {
                    err = FDB_ERR_PLATFORM_ERROR;
                } else if (c->lc->row.clientid == c->clientid) {
                    layout_clear(tr, p, c->lc->other, &c->lc->row, c->lc->row_ds);
                }
                /* A row naming another client is that client's live
                 * layout: only the orphan key below goes. */
            }
            /* The scanned key itself, also when its row is gone or names
             * another client (an orphan). */
            key_u64_sid(&k, p, FDB_KT_LAYOUT_BY_CLIENT, c->clientid, c->lc->other);
            fdb_txn_clear(tr, &k);
        }
        raw_release(&rv);
    }
    if (err != 0) {
        return (int)err;
    }
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static enum mds_status fdb_layout_del_all_for_client(struct mds_catalogue *cat,
                                                     uint64_t clientid)
{
    struct layout_del_ctx *c;
    enum mds_status st = MDS_OK;
    uint32_t batches = 0;

    if (be_of(cat) == NULL) {
        return MDS_ERR_INVAL;
    }
    c = calloc(1, sizeof(*c));
    if (c == NULL) {
        return MDS_ERR_NOMEM;
    }
    c->lc = calloc(1, sizeof(*c->lc));
    if (c->lc == NULL) {
        free(c);
        return MDS_ERR_NOMEM;
    }
    c->lc->b = be_of(cat);
    c->clientid = clientid;
    do {
        c->more = false;
        st = fdb_run_txn(c->lc->b, FDB_TXN_MUTATING, "layout_del_all_for_client",
                         layout_del_body, c);
        batches++;
    } while (st == MDS_OK && c->more && batches < FDB_COORD_DEL_MAX_BATCHES);
    if (st == MDS_OK && c->more) {
        MDS_LOG_ERROR(LOG_COMP_CAT, "fdb layout_del_all_for_client: client %llu still has "
                      "layout rows after %u batches", (unsigned long long)clientid, batches);
        st = MDS_ERR_DELAY;
    }
    free(c->lc);
    free(c);
    return st;
}

/* --- ds_layout_idx_scan ------------------------------------------------ */

struct ds_pair {
    uint64_t clientid;
    uint64_t fileid;
};

/* Heap allocated once per call (the page arrays). */
struct ds_idx_ctx {
    struct fdb_backend *b;
    uint32_t            ds_id;
    struct fdb_key      cursor;
    bool                more;
    struct ds_pair      last;        /**< Last pair materialised (dedupe). */
    bool                have_last;
    uint32_t            n;
    struct ds_pair      page[FDB_COORD_PAGE];
    struct ds_pair      cand[FDB_COORD_PAGE]; /**< Per-key candidates of the wave. */
    FDBFuture          *fs[FDB_COORD_PAGE];
};

/* Every (clientid, fileid) with a layout row naming @p ds_id, delivered
 * once (keys of one pair are contiguous in DS_LAYOUT_IDX order). */
static int ds_idx_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct ds_idx_ctx *c = arg;
    const struct fdb_key_prefix *p = &c->b->prefix;
    struct fdb_key base;
    struct fdb_key k;
    struct kvs page;
    struct ds_pair last = c->last;
    bool have_last = c->have_last;
    uint32_t n = 0;
    uint32_t i;
    fdb_error_t err;

    c->n = 0;
    c->more = false;
    fdb_key_init(&base, p, FDB_KT_DS_LAYOUT_IDX);
    fdb_key_be32(&base, c->ds_id);
    err = idx_range_read(tr, &base, &c->cursor, (int)FDB_COORD_PAGE, &page);
    if (err != 0) {
        return (int)err;
    }
    for (i = 0; i < (uint32_t)page.count && n < FDB_COORD_PAGE; i++) {
        FDBKeyValue kv;
        const uint8_t *s;

        fdb_kv_at(page.kvs, (int)i, &kv);
        s = key_suffix(&base, &kv, FDB_DS_LAYOUT_IDX_TAIL);
        if (s == NULL) {
            kvs_release(&page);
            futures_drop(c->fs, 0, n);
            return FDB_ERR_PLATFORM_ERROR;
        }
        c->cand[n].clientid = fdb_get_u64(s);
        c->cand[n].fileid = fdb_get_u64(s + FDB_KEY_BE64_LEN);
        key_sid(&k, p, FDB_KT_LAYOUT_STATE, s + 2U * FDB_KEY_BE64_LEN);
        c->fs[n] = fdb_txn_get_start(tr, &k, false);
        n++;
    }
    if (page.count > 0) {
        FDBKeyValue kv;

        fdb_kv_at(page.kvs, page.count - 1, &kv);
        cursor_set(&c->cursor, &kv);
        c->more = page.more;
    }
    kvs_release(&page);
    for (i = 0; i < n; i++) {
        struct rval rv;
        fdb_error_t e2 = raw_wait(c->fs[i], &rv);

        c->fs[i] = NULL;
        if (e2 != 0) {
            if (err == 0) {
                err = e2;
            }
            continue;
        }
        if (err == 0 && rv.found) {
            struct fdb_layout_hdr hdr;

            if (fdb_layout_decode(rv.val, rv.len, &hdr, NULL, 0) &&
                hdr.clientid == c->cand[i].clientid && hdr.fileid == c->cand[i].fileid &&
                fdb_layout_ds_contains(rv.val, rv.len, c->ds_id) &&
                (!have_last || last.clientid != hdr.clientid || last.fileid != hdr.fileid)) {
                c->page[c->n] = c->cand[i];
                c->n++;
                last = c->cand[i];
                have_last = true;
            }
        }
        raw_release(&rv);
    }
    if (err != 0) {
        c->n = 0;
        return (int)err;
    }
    c->last = last;
    c->have_last = have_last;
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static enum mds_status fdb_ds_layout_idx_scan(struct mds_catalogue *cat, uint32_t ds_id,
                                              mds_coord_ds_layout_cb cb, void *ctx)
{
    struct ds_idx_ctx *c;
    enum mds_status st = MDS_OK;

    if (be_of(cat) == NULL || cb == NULL) {
        return MDS_ERR_INVAL;
    }
    c = calloc(1, sizeof(*c));
    if (c == NULL) {
        return MDS_ERR_NOMEM;
    }
    c->b = be_of(cat);
    c->ds_id = ds_id;
    for (;;) {
        uint32_t i;

        st = fdb_run_txn(c->b, FDB_TXN_READONLY, "ds_layout_idx_scan", ds_idx_body, c);
        if (st != MDS_OK) {
            break;
        }
        for (i = 0; i < c->n; i++) {
            if (cb(c->page[i].clientid, c->page[i].fileid, ctx) != 0) {
                goto out;
            }
        }
        if (!c->more) {
            break;
        }
    }
out:
    free(c);
    return st;
}

/* --- layout_iter_file -------------------------------------------------- */

struct holder {
    uint64_t            clientid;
    struct nfs4_stateid sid;
    uint32_t            iomode;
};

/* Heap allocated once per call (the page arrays). */
struct iter_file_ctx {
    struct fdb_backend *b;
    uint64_t            fileid;
    struct fdb_key      cursor;
    bool                more;
    uint32_t            n;
    struct holder       page[FDB_COORD_PAGE];
    FDBFuture          *fs[FDB_COORD_PAGE];
};

static int iter_file_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct iter_file_ctx *c = arg;
    const struct fdb_key_prefix *p = &c->b->prefix;
    struct fdb_key base;
    struct fdb_key k;
    struct kvs page;
    uint32_t n = 0;
    uint32_t i;
    fdb_error_t err;

    c->n = 0;
    c->more = false;
    key_u64_prefix(&base, p, FDB_KT_LAYOUT_BY_FILE, c->fileid);
    err = idx_range_read(tr, &base, &c->cursor, (int)FDB_COORD_PAGE, &page);
    if (err != 0) {
        return (int)err;
    }
    for (i = 0; i < (uint32_t)page.count && n < FDB_COORD_PAGE; i++) {
        FDBKeyValue kv;
        const uint8_t *other;

        fdb_kv_at(page.kvs, (int)i, &kv);
        other = key_suffix(&base, &kv, NFS4_OTHER_SIZE);
        if (other == NULL) {
            kvs_release(&page);
            futures_drop(c->fs, 0, n);
            return FDB_ERR_PLATFORM_ERROR;
        }
        memset(&c->page[n], 0, sizeof(c->page[n]));
        memcpy(c->page[n].sid.other, other, NFS4_OTHER_SIZE);
        key_sid(&k, p, FDB_KT_LAYOUT_STATE, other);
        c->fs[n] = fdb_txn_get_start(tr, &k, false);
        n++;
    }
    if (page.count > 0) {
        FDBKeyValue kv;

        fdb_kv_at(page.kvs, page.count - 1, &kv);
        cursor_set(&c->cursor, &kv);
        c->more = page.more;
    }
    kvs_release(&page);
    for (i = 0; i < n; i++) {
        struct rval rv;
        fdb_error_t e2 = raw_wait(c->fs[i], &rv);

        c->fs[i] = NULL;
        if (e2 != 0) {
            if (err == 0) {
                err = e2;
            }
            continue;
        }
        if (err == 0 && rv.found) {
            struct fdb_layout_hdr hdr;

            if (fdb_layout_decode(rv.val, rv.len, &hdr, NULL, 0) && hdr.fileid == c->fileid) {
                /* Compact in place: c->n <= i always holds. */
                struct holder *h = &c->page[c->n];

                if (c->n != i) {
                    memcpy(h->sid.other, c->page[i].sid.other, NFS4_OTHER_SIZE);
                }
                h->sid.seqid = hdr.seqid;
                h->clientid = hdr.clientid;
                h->iomode = hdr.iomode;
                c->n++;
            }
        }
        raw_release(&rv);
    }
    if (err != 0) {
        c->n = 0;
        return (int)err;
    }
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static enum mds_status fdb_layout_iter_file(struct mds_catalogue *cat, uint64_t fileid,
                                            mds_coord_layout_file_iter_cb cb, void *ctx)
{
    struct iter_file_ctx *c;
    enum mds_status st = MDS_OK;

    if (be_of(cat) == NULL || cb == NULL) {
        return MDS_ERR_INVAL;
    }
    c = calloc(1, sizeof(*c));
    if (c == NULL) {
        return MDS_ERR_NOMEM;
    }
    c->b = be_of(cat);
    c->fileid = fileid;
    for (;;) {
        uint32_t i;

        st = fdb_run_txn(c->b, FDB_TXN_READONLY, "layout_iter_file", iter_file_body, c);
        if (st != MDS_OK) {
            break;
        }
        for (i = 0; i < c->n; i++) {
            if (cb(c->page[i].clientid, &c->page[i].sid, c->page[i].iomode, ctx) != 0) {
                goto out;
            }
        }
        if (!c->more) {
            break;
        }
    }
out:
    free(c);
    return st;
}

/* --- layoutget_fused --------------------------------------------------- */

struct fused_ctx {
    struct layout_ctx        *lc;       /**< Grant to persist (heap). */
    uint64_t                  fileid;
    struct fdb_stripe_hdr_val hdr;
    struct mds_ds_map_entry  *entries;  /**< Heap, hdr.stripe_count x mirror_count. */
};

/* Decode the stripe entry rows of the range page into c->entries (one
 * per ordinal, ordinals contiguous from 0). */
static int fused_collect_entries(struct fused_ctx *c, const struct fdb_key *ent_base,
                                 const struct kvs *page, uint32_t n)
{
    uint32_t i;

    free(c->entries);
    c->entries = calloc(n, sizeof(*c->entries));
    if (c->entries == NULL) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    for (i = 0; i < n; i++) {
        FDBKeyValue kv;
        const uint8_t *s;

        fdb_kv_at(page->kvs, (int)i, &kv);
        s = key_suffix(ent_base, &kv, FDB_KEY_BE32_LEN); /* the ordinal */
        if (s == NULL || fdb_get_u32(s) != i ||
            !fdb_stripe_ent_decode(kv.value, (size_t)kv.value_length, &c->entries[i])) {
            return FDB_ERR_PLATFORM_ERROR; /* corrupt or incomplete stripe map */
        }
    }
    return 0;
}

static int fused_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct fused_ctx *c = arg;
    struct layout_ctx *lc = c->lc;
    const struct fdb_key_prefix *p = &lc->b->prefix;
    struct fdb_key k;
    struct fdb_key ent_base;
    struct fdb_key_range r;
    struct rval hdr_rv;
    struct kvs ents;
    struct layout_ds_id_list ids;
    FDBFuture *f_hdr;
    uint32_t n;
    fdb_error_t err;
    int rc;

    /* Header and entries in one wave. */
    fdb_key_stripe_hdr(&k, p, c->fileid);
    f_hdr = fdb_txn_get_start(tr, &k, false);
    fdb_key_stripe_ent_prefix(&ent_base, p, c->fileid);
    ents.f = NULL;
    ents.kvs = NULL;
    ents.count = 0;
    ents.more = false;
    if (fdb_key_range_prefix(&r, &ent_base)) {
        ents.f = fdb_txn_get_range_start(tr, &r, (int)FDB_LAYOUT_DS_MAX + 1, false, false);
    }
    err = raw_wait(f_hdr, &hdr_rv);
    if (err != 0) {
        if (ents.f != NULL) {
            fdb_future_destroy(ents.f);
        }
        return (int)err;
    }
    if (!hdr_rv.found) {
        raw_release(&hdr_rv);
        if (ents.f != NULL) {
            fdb_future_destroy(ents.f);
        }
        *st_out = MDS_ERR_NOTFOUND; /* no stripe map: nothing granted */
        return FDB_BODY_DONE;
    }
    if (!fdb_stripe_hdr_decode(hdr_rv.val, hdr_rv.len, &c->hdr)) {
        raw_release(&hdr_rv);
        if (ents.f != NULL) {
            fdb_future_destroy(ents.f);
        }
        return FDB_ERR_PLATFORM_ERROR;
    }
    raw_release(&hdr_rv);
    n = c->hdr.stripe_count * c->hdr.mirror_count;
    err = fdb_txn_get_range_wait(ents.f, &ents.kvs, &ents.count, &ents.more);
    if (err != 0) {
        if (ents.f != NULL) {
            fdb_future_destroy(ents.f);
        }
        return (int)err;
    }
    if ((uint32_t)ents.count != n || ents.more) {
        kvs_release(&ents);
        return FDB_ERR_PLATFORM_ERROR; /* header and entries disagree */
    }
    rc = fused_collect_entries(c, &ent_base, &ents, n);
    kvs_release(&ents);
    if (rc != 0) {
        return rc;
    }
    if (layout_ds_id_list_from_entries(&ids, c->entries, n) != MDS_OK) {
        *st_out = MDS_ERR_INVAL; /* a phantom stripe: the caller's fallback decides */
        return FDB_BODY_DONE;
    }
    lc->want.ds_count = ids.count;
    rc = layout_write(tr, p, lc->other, &lc->want, ids.ids, lc->enc, sizeof(lc->enc));
    layout_ds_id_list_destroy(&ids);
    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static enum mds_status fdb_layoutget_fused(struct mds_catalogue *cat, uint64_t fileid,
                                           uint32_t *stripe_count, uint32_t *stripe_unit,
                                           uint32_t *mirror_count,
                                           struct mds_ds_map_entry **entries,
                                           const struct nfs4_stateid *stateid,
                                           uint64_t clientid, uint32_t iomode,
                                           uint64_t offset, uint64_t length, uint32_t mds_id)
{
    struct fused_ctx c;
    enum mds_status st;

    if (entries != NULL) {
        *entries = NULL;
    }
    if (be_of(cat) == NULL || stateid == NULL || stripe_count == NULL || stripe_unit == NULL ||
        mirror_count == NULL || entries == NULL) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.lc = calloc(1, sizeof(*c.lc));
    if (c.lc == NULL) {
        return MDS_ERR_NOMEM;
    }
    c.fileid = fileid;
    st = layout_ctx_prepare(c.lc, be_of(cat), clientid, fileid, iomode, offset, length,
                            stateid, NULL, 0);
    if (st == MDS_OK) {
        c.lc->want.grant_owner_mds_id = mds_id;
        st = fdb_run_txn(c.lc->b, FDB_TXN_MUTATING, "layoutget_fused", fused_body, &c);
    }
    if (st == MDS_OK) {
        *stripe_count = c.hdr.stripe_count;
        *stripe_unit = c.hdr.stripe_unit;
        *mirror_count = c.hdr.mirror_count;
        *entries = c.entries; /* caller frees */
    } else {
        free(c.entries);
    }
    free(c.lc);
    return st;
}

/* -----------------------------------------------------------------------
 * Client recovery
 * ----------------------------------------------------------------------- */

struct recovery_ctx {
    struct fdb_backend     *b;
    uint64_t                clientid;
    struct fdb_recovery_val val;   /**< put: the row to write; get: the row read. */
    uint8_t                 enc[FDB_RECOVERY_ENC_MAX];
};

/* Read the RECOVERY row of c->clientid into *out. */
static fdb_error_t recovery_read(FDBTransaction *tr, const struct recovery_ctx *c,
                                 struct fdb_recovery_val *out, bool *found)
{
    struct fdb_key k;
    struct rval rv;
    fdb_error_t err;

    *found = false;
    key_u64_prefix(&k, &c->b->prefix, FDB_KT_RECOVERY, c->clientid);
    err = raw_get(tr, &k, &rv);
    if (err != 0) {
        return err;
    }
    if (rv.found) {
        if (!fdb_recovery_decode(rv.val, rv.len, out)) {
            raw_release(&rv);
            return FDB_ERR_PLATFORM_ERROR;
        }
        *found = true;
    }
    raw_release(&rv);
    return 0;
}

static int recovery_put_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct recovery_ctx *c = arg;
    const struct fdb_key_prefix *p = &c->b->prefix;
    struct fdb_recovery_val old;
    struct fdb_key k;
    size_t len = 0;
    bool found = false;
    fdb_error_t err;

    err = recovery_read(tr, c, &old, &found);
    if (err != 0) {
        return (int)err;
    }
    if (found && old.owner_mds_id != c->val.owner_mds_id) {
        key_recovery_by_owner(&k, p, old.owner_mds_id, c->clientid);
        fdb_txn_clear(tr, &k);
    }
    if (!fdb_recovery_encode(&c->val, c->enc, sizeof(c->enc), &len)) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    key_u64_prefix(&k, p, FDB_KT_RECOVERY, c->clientid);
    fdb_txn_set(tr, &k, c->enc, len);
    key_recovery_by_owner(&k, p, c->val.owner_mds_id, c->clientid);
    set_empty(tr, &k);
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static int recovery_del_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct recovery_ctx *c = arg;
    const struct fdb_key_prefix *p = &c->b->prefix;
    struct fdb_key k;
    bool found = false;
    fdb_error_t err;

    err = recovery_read(tr, c, &c->val, &found);
    if (err != 0) {
        return (int)err;
    }
    *st_out = MDS_OK; /* idempotent: a missing row is fine */
    if (!found) {
        return FDB_BODY_DONE;
    }
    key_u64_prefix(&k, p, FDB_KT_RECOVERY, c->clientid);
    fdb_txn_clear(tr, &k);
    key_recovery_by_owner(&k, p, c->val.owner_mds_id, c->clientid);
    fdb_txn_clear(tr, &k);
    return FDB_BODY_COMMIT;
}

static int recovery_get_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct recovery_ctx *c = arg;
    bool found = false;
    fdb_error_t err;

    err = recovery_read(tr, c, &c->val, &found);
    if (err != 0) {
        return (int)err;
    }
    *st_out = found ? MDS_OK : MDS_ERR_NOTFOUND;
    return FDB_BODY_COMMIT;
}

/* The owner stamped on a recovery row is this handle's identity; the
 * epoch is the registry epoch node_register recorded (0 before). */
static enum mds_status fdb_recovery_put(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                        uint64_t clientid, const uint8_t *co_ownerid,
                                        uint32_t co_ownerid_len, const uint8_t verifier[8])
{
    struct recovery_ctx *c;
    enum mds_status st;

    (void)txn;
    if (be_of(cat) == NULL || verifier == NULL || co_ownerid_len > FDB_RECOVERY_CO_MAX ||
        (co_ownerid_len > 0 && co_ownerid == NULL)) {
        return MDS_ERR_INVAL;
    }
    c = calloc(1, sizeof(*c));
    if (c == NULL) {
        return MDS_ERR_NOMEM;
    }
    c->b = be_of(cat);
    c->clientid = clientid;
    c->val.owner_mds_id = c->b->mds_id;
    c->val.owner_boot_epoch = atomic_load(&c->b->boot_epoch);
    c->val.co_ownerid_len = co_ownerid_len;
    if (co_ownerid_len > 0) {
        memcpy(c->val.co_ownerid, co_ownerid, co_ownerid_len);
    }
    memcpy(c->val.verifier, verifier, sizeof(c->val.verifier));
    st = fdb_run_txn(c->b, FDB_TXN_MUTATING, "recovery_put", recovery_put_body, c);
    free(c);
    return st;
}

static enum mds_status fdb_recovery_del(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                        uint64_t clientid)
{
    struct recovery_ctx *c;
    enum mds_status st;

    (void)txn;
    if (be_of(cat) == NULL) {
        return MDS_ERR_INVAL;
    }
    c = calloc(1, sizeof(*c));
    if (c == NULL) {
        return MDS_ERR_NOMEM;
    }
    c->b = be_of(cat);
    c->clientid = clientid;
    st = fdb_run_txn(c->b, FDB_TXN_MUTATING, "recovery_del", recovery_del_body, c);
    free(c);
    return st;
}

static enum mds_status fdb_recovery_get(struct mds_catalogue *cat, uint64_t clientid,
                                        uint8_t *co_ownerid, uint32_t *co_ownerid_len,
                                        uint8_t verifier[8])
{
    struct recovery_ctx *c;
    enum mds_status st;

    if (be_of(cat) == NULL) {
        return MDS_ERR_INVAL;
    }
    c = calloc(1, sizeof(*c));
    if (c == NULL) {
        return MDS_ERR_NOMEM;
    }
    c->b = be_of(cat);
    c->clientid = clientid;
    st = fdb_run_txn(c->b, FDB_TXN_READONLY, "recovery_get", recovery_get_body, c);
    if (st == MDS_OK) {
        if (co_ownerid != NULL && c->val.co_ownerid_len > 0) {
            memcpy(co_ownerid, c->val.co_ownerid, c->val.co_ownerid_len);
        }
        if (co_ownerid_len != NULL) {
            *co_ownerid_len = c->val.co_ownerid_len;
        }
        if (verifier != NULL) {
            memcpy(verifier, c->val.verifier, sizeof(c->val.verifier));
        }
    }
    free(c);
    return st;
}

/* --- recovery_list ------------------------------------------------------ */

struct recovery_row {
    uint64_t clientid;
    uint32_t owner_mds_id;
    uint64_t owner_boot_epoch;
};

/* Heap allocated once per call (the page arrays). */
struct recovery_list_ctx {
    struct fdb_backend  *b;
    uint32_t             owner;     /**< Index scan: the owner prefix. */
    bool                 table;     /**< Scan the RECOVERY table itself. */
    struct fdb_key       cursor;
    bool                 more;
    uint32_t             n;
    struct recovery_row  page[FDB_COORD_PAGE];
    FDBFuture           *fs[FDB_COORD_PAGE];
};

/* One page of RECOVERY rows straight from the table (owner filter 0). */
static int recovery_table_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct recovery_list_ctx *c = arg;
    struct fdb_key base;
    struct kvs page;
    uint32_t i;
    fdb_error_t err;

    c->n = 0;
    c->more = false;
    fdb_key_init(&base, &c->b->prefix, FDB_KT_RECOVERY);
    err = idx_range_read(tr, &base, &c->cursor, (int)FDB_COORD_PAGE, &page);
    if (err != 0) {
        return (int)err;
    }
    for (i = 0; i < (uint32_t)page.count && c->n < FDB_COORD_PAGE; i++) {
        FDBKeyValue kv;
        const uint8_t *s;
        struct fdb_recovery_val v;

        fdb_kv_at(page.kvs, (int)i, &kv);
        s = key_suffix(&base, &kv, FDB_KEY_BE64_LEN); /* the clientid */
        if (s == NULL || !fdb_recovery_decode(kv.value, (size_t)kv.value_length, &v)) {
            kvs_release(&page);
            c->n = 0;
            return FDB_ERR_PLATFORM_ERROR;
        }
        c->page[c->n].clientid = fdb_get_u64(s);
        c->page[c->n].owner_mds_id = v.owner_mds_id;
        c->page[c->n].owner_boot_epoch = v.owner_boot_epoch;
        c->n++;
    }
    if (page.count > 0) {
        FDBKeyValue kv;

        fdb_kv_at(page.kvs, page.count - 1, &kv);
        cursor_set(&c->cursor, &kv);
        c->more = page.more;
    }
    kvs_release(&page);
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

/* One page of RECOVERY_BY_OWNER + owner: keys give the clientids, the
 * rows (one wave) give the epoch and confirm the owner. */
static int recovery_index_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct recovery_list_ctx *c = arg;
    const struct fdb_key_prefix *p = &c->b->prefix;
    struct fdb_key base;
    struct fdb_key k;
    struct kvs page;
    uint32_t n = 0;
    uint32_t i;
    fdb_error_t err;

    c->n = 0;
    c->more = false;
    fdb_key_init(&base, p, FDB_KT_RECOVERY_BY_OWNER);
    fdb_key_be32(&base, c->owner);
    err = idx_range_read(tr, &base, &c->cursor, (int)FDB_COORD_PAGE, &page);
    if (err != 0) {
        return (int)err;
    }
    for (i = 0; i < (uint32_t)page.count && n < FDB_COORD_PAGE; i++) {
        FDBKeyValue kv;
        const uint8_t *s;

        fdb_kv_at(page.kvs, (int)i, &kv);
        s = key_suffix(&base, &kv, FDB_KEY_BE64_LEN); /* the clientid */
        if (s == NULL) {
            kvs_release(&page);
            futures_drop(c->fs, 0, n);
            return FDB_ERR_PLATFORM_ERROR;
        }
        c->page[n].clientid = fdb_get_u64(s);
        key_u64_prefix(&k, p, FDB_KT_RECOVERY, c->page[n].clientid);
        c->fs[n] = fdb_txn_get_start(tr, &k, false);
        n++;
    }
    if (page.count > 0) {
        FDBKeyValue kv;

        fdb_kv_at(page.kvs, page.count - 1, &kv);
        cursor_set(&c->cursor, &kv);
        c->more = page.more;
    }
    kvs_release(&page);
    for (i = 0; i < n; i++) {
        struct rval rv;
        fdb_error_t e2 = raw_wait(c->fs[i], &rv);

        c->fs[i] = NULL;
        if (e2 != 0) {
            if (err == 0) {
                err = e2;
            }
            continue;
        }
        if (err == 0 && rv.found) {
            struct fdb_recovery_val v;

            if (fdb_recovery_decode(rv.val, rv.len, &v) && v.owner_mds_id == c->owner) {
                c->page[c->n].clientid = c->page[i].clientid; /* c->n <= i */
                c->page[c->n].owner_mds_id = v.owner_mds_id;
                c->page[c->n].owner_boot_epoch = v.owner_boot_epoch;
                c->n++;
            }
        }
        raw_release(&rv);
    }
    if (err != 0) {
        c->n = 0;
        return (int)err;
    }
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

/* Run one paged scan (table or one owner prefix); *stopped when the
 * callback asked to stop. */
static enum mds_status recovery_list_scan(struct recovery_list_ctx *c, mds_recovery_list_cb cb,
                                          void *ctx, bool *stopped)
{
    c->cursor.len = 0;
    c->cursor.overflow = false;
    for (;;) {
        enum mds_status st;
        uint32_t i;

        st = fdb_run_txn(c->b, FDB_TXN_READONLY, "recovery_list",
                         c->table ? recovery_table_body : recovery_index_body, c);
        if (st != MDS_OK) {
            return st;
        }
        for (i = 0; i < c->n; i++) {
            if (cb(c->page[i].clientid, c->page[i].owner_mds_id, c->page[i].owner_boot_epoch,
                   ctx) != 0) {
                *stopped = true;
                return MDS_OK;
            }
        }
        if (!c->more) {
            return MDS_OK;
        }
    }
}

/* The rows owned by @p owner_mds_id plus the unassigned ones (owner 0);
 * 0 lists every row.  Rows explicitly owned by another MDS are never
 * returned (memdb / RonDB filter). */
static enum mds_status fdb_recovery_list(struct mds_catalogue *cat, uint32_t owner_mds_id,
                                         mds_recovery_list_cb cb, void *ctx)
{
    struct recovery_list_ctx *c;
    enum mds_status st;
    bool stopped = false;

    if (be_of(cat) == NULL || cb == NULL) {
        return MDS_ERR_INVAL;
    }
    c = calloc(1, sizeof(*c));
    if (c == NULL) {
        return MDS_ERR_NOMEM;
    }
    c->b = be_of(cat);
    if (owner_mds_id == 0) {
        c->table = true;
        st = recovery_list_scan(c, cb, ctx, &stopped);
    } else {
        c->owner = owner_mds_id;
        st = recovery_list_scan(c, cb, ctx, &stopped);
        if (st == MDS_OK && !stopped) {
            c->owner = 0;
            st = recovery_list_scan(c, cb, ctx, &stopped);
        }
    }
    free(c);
    return st;
}

/* -----------------------------------------------------------------------
 * Shared 2PC journal
 * ----------------------------------------------------------------------- */

struct journal_ctx {
    struct fdb_backend                    *b;
    uint64_t                               txn_id;
    uint8_t                                role;
    const struct mds_coord_journal_record *in;    /**< put */
    struct mds_coord_journal_record       *out;   /**< get */
    uint8_t                                enc[FDB_JOURNAL_ENC_MAX];
};

static int journal_put_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct journal_ctx *c = arg;
    struct fdb_key k;
    size_t len = 0;

    if (!fdb_journal_encode(c->in, c->enc, sizeof(c->enc), &len)) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    key_journal(&k, &c->b->prefix, c->in->txn_id, c->in->role);
    fdb_txn_set(tr, &k, c->enc, len);
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static int journal_get_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct journal_ctx *c = arg;
    struct fdb_key k;
    struct rval rv;
    fdb_error_t err;
    bool ok;

    key_journal(&k, &c->b->prefix, c->txn_id, c->role);
    err = raw_get(tr, &k, &rv);
    if (err != 0) {
        return (int)err;
    }
    if (!rv.found) {
        raw_release(&rv);
        *st_out = MDS_ERR_NOTFOUND;
        return FDB_BODY_DONE;
    }
    ok = fdb_journal_decode(rv.val, rv.len, c->txn_id, c->role, c->out);
    raw_release(&rv);
    if (!ok) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static int journal_del_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct journal_ctx *c = arg;
    struct fdb_key k;
    struct rval rv;
    fdb_error_t err;

    key_journal(&k, &c->b->prefix, c->txn_id, c->role);
    err = raw_get(tr, &k, &rv);
    if (err != 0) {
        return (int)err;
    }
    if (!rv.found) {
        raw_release(&rv);
        *st_out = MDS_ERR_NOTFOUND;
        return FDB_BODY_DONE;
    }
    raw_release(&rv);
    fdb_txn_clear(tr, &k);
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static enum mds_status fdb_journal_put(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                       const struct mds_coord_journal_record *record)
{
    struct journal_ctx *c;
    enum mds_status st;

    (void)txn;
    if (be_of(cat) == NULL || record == NULL) {
        return MDS_ERR_INVAL;
    }
    c = calloc(1, sizeof(*c));
    if (c == NULL) {
        return MDS_ERR_NOMEM;
    }
    c->b = be_of(cat);
    c->in = record;
    st = fdb_run_txn(c->b, FDB_TXN_MUTATING, "journal_put", journal_put_body, c);
    free(c);
    return st;
}

static enum mds_status fdb_journal_get(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                       uint64_t txn_id, uint8_t role,
                                       struct mds_coord_journal_record *record)
{
    struct journal_ctx *c;
    enum mds_status st;

    (void)txn;
    if (be_of(cat) == NULL || record == NULL) {
        return MDS_ERR_INVAL;
    }
    c = calloc(1, sizeof(*c));
    if (c == NULL) {
        return MDS_ERR_NOMEM;
    }
    c->b = be_of(cat);
    c->txn_id = txn_id;
    c->role = role;
    c->out = record;
    st = fdb_run_txn(c->b, FDB_TXN_READONLY, "journal_get", journal_get_body, c);
    free(c);
    return st;
}

static enum mds_status fdb_journal_del(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                       uint64_t txn_id, uint8_t role)
{
    struct journal_ctx *c;
    enum mds_status st;

    (void)txn;
    if (be_of(cat) == NULL) {
        return MDS_ERR_INVAL;
    }
    c = calloc(1, sizeof(*c));
    if (c == NULL) {
        return MDS_ERR_NOMEM;
    }
    c->b = be_of(cat);
    c->txn_id = txn_id;
    c->role = role;
    st = fdb_run_txn(c->b, FDB_TXN_MUTATING, "journal_del", journal_del_body, c);
    free(c);
    return st;
}

/* --- journal_scan -------------------------------------------------------- */

struct journal_key {
    uint64_t created_at_ns;
    uint64_t txn_id;
    uint8_t  role;
};

struct journal_scan_ctx {
    struct fdb_backend              *b;
    /* Phase 1: sort keys. */
    struct fdb_key                   cursor;
    bool                             more;
    bool                             overflow;
    uint32_t                         n_keys;
    struct journal_key              *keys;      /**< FDB_JOURNAL_SCAN_MAX. */
    struct mds_coord_journal_record  scratch;
    /* Phase 3: one batch of records. */
    uint32_t                         first;     /**< keys[first ..] in this batch. */
    uint32_t                         n_recs;
    bool                             present[FDB_JOURNAL_FETCH];
    struct mds_coord_journal_record *recs;      /**< FDB_JOURNAL_FETCH. */
    FDBFuture                       *fs[FDB_JOURNAL_FETCH];
};

/* Ascending created_at_ns, then (txn_id, role): a total order, so the
 * sort is deterministic whatever qsort does with equal stamps. */
static int journal_key_cmp(const void *a, const void *b)
{
    const struct journal_key *ka = a;
    const struct journal_key *kb = b;

    if (ka->created_at_ns != kb->created_at_ns) {
        return ka->created_at_ns < kb->created_at_ns ? -1 : 1;
    }
    if (ka->txn_id != kb->txn_id) {
        return ka->txn_id < kb->txn_id ? -1 : 1;
    }
    if (ka->role != kb->role) {
        return ka->role < kb->role ? -1 : 1;
    }
    return 0;
}

static int journal_keys_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct journal_scan_ctx *c = arg;
    struct fdb_key base;
    struct kvs page;
    uint32_t i;
    fdb_error_t err;

    c->more = false;
    fdb_key_init(&base, &c->b->prefix, FDB_KT_JOURNAL);
    err = idx_range_read(tr, &base, &c->cursor, (int)FDB_COORD_PAGE, &page);
    if (err != 0) {
        return (int)err;
    }
    for (i = 0; i < (uint32_t)page.count; i++) {
        FDBKeyValue kv;
        const uint8_t *s;
        struct journal_key *jk;

        fdb_kv_at(page.kvs, (int)i, &kv);
        s = key_suffix(&base, &kv, FDB_JOURNAL_KEY_TAIL);
        if (s == NULL) {
            kvs_release(&page);
            return FDB_ERR_PLATFORM_ERROR;
        }
        if (c->n_keys >= FDB_JOURNAL_SCAN_MAX) {
            c->overflow = true;
            break;
        }
        jk = &c->keys[c->n_keys];
        jk->txn_id = fdb_get_u64(s);
        jk->role = s[FDB_KEY_BE64_LEN];
        if (!fdb_journal_decode(kv.value, (size_t)kv.value_length, jk->txn_id, jk->role,
                                &c->scratch)) {
            kvs_release(&page);
            return FDB_ERR_PLATFORM_ERROR;
        }
        jk->created_at_ns = c->scratch.created_at_ns;
        c->n_keys++;
        cursor_set(&c->cursor, &kv);
    }
    if (!c->overflow) {
        c->more = page.more;
    }
    kvs_release(&page);
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

/* Fetch keys[first .. first + n_recs) in one wave; a record deleted since
 * phase 1 is marked absent and skipped by the caller. */
static int journal_fetch_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct journal_scan_ctx *c = arg;
    struct fdb_key k;
    uint32_t i;
    fdb_error_t err = 0;

    for (i = 0; i < c->n_recs; i++) {
        const struct journal_key *jk = &c->keys[c->first + i];

        c->present[i] = false;
        key_journal(&k, &c->b->prefix, jk->txn_id, jk->role);
        c->fs[i] = fdb_txn_get_start(tr, &k, false);
    }
    for (i = 0; i < c->n_recs; i++) {
        const struct journal_key *jk = &c->keys[c->first + i];
        struct rval rv;
        fdb_error_t e2 = raw_wait(c->fs[i], &rv);

        c->fs[i] = NULL;
        if (e2 != 0) {
            if (err == 0) {
                err = e2;
            }
            continue;
        }
        if (err == 0 && rv.found) {
            if (!fdb_journal_decode(rv.val, rv.len, jk->txn_id, jk->role, &c->recs[i])) {
                err = FDB_ERR_PLATFORM_ERROR;
            } else {
                c->present[i] = true;
            }
        }
        raw_release(&rv);
    }
    if (err != 0) {
        return (int)err;
    }
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

/* Phase 1: every sort key, page by page; then the sort. */
static enum mds_status journal_scan_collect(struct journal_scan_ctx *c)
{
    enum mds_status st;

    do {
        st = fdb_run_txn(c->b, FDB_TXN_READONLY, "journal_scan", journal_keys_body, c);
    } while (st == MDS_OK && c->more);
    if (st != MDS_OK) {
        return st;
    }
    if (c->overflow) {
        MDS_LOG_ERROR(LOG_COMP_CAT, "fdb journal_scan: more than %u journal records; "
                      "refusing an unordered scan", FDB_JOURNAL_SCAN_MAX);
        return MDS_ERR_NOSPC;
    }
    if (c->n_keys > 1) {
        qsort(c->keys, c->n_keys, sizeof(*c->keys), journal_key_cmp);
    }
    return MDS_OK;
}

/* Phase 3: the records, oldest first, one wave per batch, delivered
 * after each batch's transaction ended. */
static enum mds_status journal_scan_deliver(struct journal_scan_ctx *c,
                                            mds_coord_journal_scan_cb cb, void *ctx)
{
    for (c->first = 0; c->first < c->n_keys; c->first += c->n_recs) {
        enum mds_status st;
        uint32_t i;

        c->n_recs = c->n_keys - c->first;
        if (c->n_recs > FDB_JOURNAL_FETCH) {
            c->n_recs = FDB_JOURNAL_FETCH;
        }
        st = fdb_run_txn(c->b, FDB_TXN_READONLY, "journal_scan", journal_fetch_body, c);
        if (st != MDS_OK) {
            return st;
        }
        for (i = 0; i < c->n_recs; i++) {
            if (c->present[i] && cb(&c->recs[i], ctx) != 0) {
                return MDS_OK;
            }
        }
    }
    return MDS_OK;
}

static enum mds_status fdb_journal_scan(struct mds_catalogue *cat, mds_coord_journal_scan_cb cb,
                                        void *ctx)
{
    struct journal_scan_ctx *c;
    enum mds_status st = MDS_ERR_NOMEM;

    if (be_of(cat) == NULL || cb == NULL) {
        return MDS_ERR_INVAL;
    }
    c = calloc(1, sizeof(*c));
    if (c == NULL) {
        return MDS_ERR_NOMEM;
    }
    c->b = be_of(cat);
    c->keys = calloc(FDB_JOURNAL_SCAN_MAX, sizeof(*c->keys));
    c->recs = calloc(FDB_JOURNAL_FETCH, sizeof(*c->recs));
    if (c->keys != NULL && c->recs != NULL) {
        st = journal_scan_collect(c);
        if (st == MDS_OK) {
            st = journal_scan_deliver(c, cb, ctx);
        }
    }
    free(c->keys);
    free(c->recs);
    free(c);
    return st;
}

/* -----------------------------------------------------------------------
 * Generic index scan -> row wave used by the open / deleg / session /
 * lock-owner scans: one page of index keys, the rows in one wave, the
 * decoded rows that still match materialised for delivery.
 * ----------------------------------------------------------------------- */

/* Longest row-key suffix an index carries (session ids and lock keys). */
#define FDB_IDX_SUFFIX_MAX 16U

/* Embedded in a heap-allocated scan context (the page arrays). */
struct idx_scan_ctx {
    struct fdb_backend *b;
    struct fdb_key      base;      /**< Index prefix. */
    size_t              suffix;    /**< Row-key bytes after the prefix. */
    enum fdb_key_type   row_type;  /**< Row table; its key is type + suffix. */
    struct fdb_key      cursor;
    bool                more;
    uint32_t            n;
    /* Filled per page. */
    uint8_t             suffixes[FDB_COORD_PAGE * FDB_IDX_SUFFIX_MAX];
    const uint8_t      *vals[FDB_COORD_PAGE]; /**< Per row, valid while fs[i] lives. */
    size_t              lens[FDB_COORD_PAGE];
    FDBFuture          *fs[FDB_COORD_PAGE];
};

_Static_assert(NFS4_OTHER_SIZE <= FDB_IDX_SUFFIX_MAX, "stateid suffix must fit");

/* Read one index page and the rows it names (one wave).  On success
 * vals[i] / lens[i] address row i's value (NULL when absent) until the
 * caller calls idx_scan_release(). */
static fdb_error_t idx_scan_page(FDBTransaction *tr, struct idx_scan_ctx *c)
{
    struct fdb_key k;
    struct kvs page;
    uint32_t n = 0;
    uint32_t i;
    fdb_error_t err;

    c->n = 0;
    c->more = false;
    err = idx_range_read(tr, &c->base, &c->cursor, (int)FDB_COORD_PAGE, &page);
    if (err != 0) {
        return err;
    }
    for (i = 0; i < (uint32_t)page.count && n < FDB_COORD_PAGE; i++) {
        FDBKeyValue kv;
        const uint8_t *s;

        fdb_kv_at(page.kvs, (int)i, &kv);
        s = key_suffix(&c->base, &kv, c->suffix);
        if (s == NULL) {
            kvs_release(&page);
            futures_drop(c->fs, 0, n);
            return FDB_ERR_PLATFORM_ERROR;
        }
        memcpy(c->suffixes + (size_t)n * c->suffix, s, c->suffix);
        fdb_key_init(&k, &c->b->prefix, c->row_type);
        fdb_key_bytes(&k, s, c->suffix);
        c->fs[n] = fdb_txn_get_start(tr, &k, false);
        n++;
    }
    if (page.count > 0) {
        FDBKeyValue kv;

        fdb_kv_at(page.kvs, page.count - 1, &kv);
        cursor_set(&c->cursor, &kv);
        c->more = page.more;
    }
    kvs_release(&page);
    for (i = 0; i < n; i++) {
        struct rval rv;
        fdb_error_t e2 = raw_wait(c->fs[i], &rv);

        c->vals[i] = NULL;
        c->lens[i] = 0;
        if (e2 != 0) {
            c->fs[i] = NULL;
            if (err == 0) {
                err = e2;
            }
            continue;
        }
        /* Keep the future alive: the value is decoded by the caller. */
        c->fs[i] = rv.f;
        if (rv.found) {
            c->vals[i] = rv.val;
            c->lens[i] = rv.len;
        }
    }
    c->n = n;
    if (err != 0) {
        futures_drop(c->fs, 0, n);
        c->n = 0;
    }
    return err;
}

static void idx_scan_release(struct idx_scan_ctx *c)
{
    futures_drop(c->fs, 0, c->n);
}

/* @p c is part of a zeroed heap context; suffix <= FDB_IDX_SUFFIX_MAX. */
static void idx_scan_init(struct idx_scan_ctx *c, struct fdb_backend *b, size_t suffix,
                          enum fdb_key_type row_type)
{
    c->b = b;
    c->suffix = suffix;
    c->row_type = row_type;
}

/* -----------------------------------------------------------------------
 * Open / share state
 * ----------------------------------------------------------------------- */

struct open_ctx {
    struct fdb_backend              *b;
    const uint8_t                   *other;
    const struct mds_coord_open_row *in;
    struct mds_coord_open_row        row;
    uint8_t                          enc[FDB_OPEN_ENC_MAX];
};

static int open_put_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct open_ctx *c = arg;
    const struct fdb_key_prefix *p = &c->b->prefix;
    struct fdb_key k;
    size_t len = 0;

    if (!fdb_open_encode(c->in, c->enc, sizeof(c->enc), &len)) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    key_sid(&k, p, FDB_KT_OPEN, c->in->stateid_other);
    fdb_txn_set(tr, &k, c->enc, len);
    key_u64_sid(&k, p, FDB_KT_OPEN_BY_FILE, c->in->fileid, c->in->stateid_other);
    set_empty(tr, &k);
    key_u64_sid(&k, p, FDB_KT_OPEN_BY_CLIENT, c->in->clientid, c->in->stateid_other);
    set_empty(tr, &k);
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

/* Read the open row of c->other into c->row. */
static fdb_error_t open_read(FDBTransaction *tr, struct open_ctx *c, bool *found)
{
    struct fdb_key k;
    struct rval rv;
    fdb_error_t err;

    *found = false;
    key_sid(&k, &c->b->prefix, FDB_KT_OPEN, c->other);
    err = raw_get(tr, &k, &rv);
    if (err != 0) {
        return err;
    }
    if (rv.found) {
        if (!fdb_open_decode(rv.val, rv.len, c->other, &c->row)) {
            raw_release(&rv);
            return FDB_ERR_PLATFORM_ERROR;
        }
        *found = true;
    }
    raw_release(&rv);
    return 0;
}

static int open_get_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct open_ctx *c = arg;
    bool found = false;
    fdb_error_t err = open_read(tr, c, &found);

    if (err != 0) {
        return (int)err;
    }
    *st_out = found ? MDS_OK : MDS_ERR_NOTFOUND;
    return FDB_BODY_COMMIT;
}

static int open_del_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct open_ctx *c = arg;
    const struct fdb_key_prefix *p = &c->b->prefix;
    struct fdb_key k;
    bool found = false;
    fdb_error_t err = open_read(tr, c, &found);

    if (err != 0) {
        return (int)err;
    }
    if (!found) {
        *st_out = MDS_ERR_NOTFOUND;
        return FDB_BODY_DONE;
    }
    key_sid(&k, p, FDB_KT_OPEN, c->other);
    fdb_txn_clear(tr, &k);
    key_u64_sid(&k, p, FDB_KT_OPEN_BY_FILE, c->row.fileid, c->other);
    fdb_txn_clear(tr, &k);
    key_u64_sid(&k, p, FDB_KT_OPEN_BY_CLIENT, c->row.clientid, c->other);
    fdb_txn_clear(tr, &k);
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static enum mds_status fdb_open_put(struct mds_catalogue *cat,
                                    const struct mds_coord_open_row *row)
{
    struct open_ctx c;

    if (be_of(cat) == NULL || row == NULL || row->open_owner_len > sizeof(row->open_owner)) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.b = be_of(cat);
    c.in = row;
    return fdb_run_txn(c.b, FDB_TXN_MUTATING, "open_put", open_put_body, &c);
}

static enum mds_status fdb_open_get(struct mds_catalogue *cat, const uint8_t stateid_other[12],
                                    struct mds_coord_open_row *row)
{
    struct open_ctx c;
    enum mds_status st;

    if (be_of(cat) == NULL || stateid_other == NULL || row == NULL) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.b = be_of(cat);
    c.other = stateid_other;
    st = fdb_run_txn(c.b, FDB_TXN_READONLY, "open_get", open_get_body, &c);
    if (st == MDS_OK) {
        *row = c.row;
    }
    return st;
}

static enum mds_status fdb_open_del(struct mds_catalogue *cat, const uint8_t stateid_other[12])
{
    struct open_ctx c;

    if (be_of(cat) == NULL || stateid_other == NULL) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.b = be_of(cat);
    c.other = stateid_other;
    return fdb_run_txn(c.b, FDB_TXN_MUTATING, "open_del", open_del_body, &c);
}

/* Heap allocated once per call (the page arrays). */
struct open_scan_ctx {
    struct idx_scan_ctx        idx;
    const uint64_t            *fileid;    /**< Filter (one of the two). */
    const uint64_t            *clientid;
    uint32_t                   n;
    struct mds_coord_open_row  page[FDB_COORD_PAGE];
};

static int open_scan_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct open_scan_ctx *c = arg;
    uint32_t i;
    fdb_error_t err;

    c->n = 0;
    err = idx_scan_page(tr, &c->idx);
    if (err != 0) {
        return (int)err;
    }
    for (i = 0; i < c->idx.n; i++) {
        const uint8_t *other = c->idx.suffixes + (size_t)i * NFS4_OTHER_SIZE;
        struct mds_coord_open_row *row = &c->page[c->n];

        if (c->idx.vals[i] != NULL &&
            fdb_open_decode(c->idx.vals[i], c->idx.lens[i], other, row) &&
            (c->fileid == NULL || row->fileid == *c->fileid) &&
            (c->clientid == NULL || row->clientid == *c->clientid)) {
            c->n++;
        }
    }
    idx_scan_release(&c->idx);
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static enum mds_status open_scan_run(const struct mds_catalogue *cat, const uint64_t *fileid,
                                     const uint64_t *clientid, mds_coord_open_scan_cb cb,
                                     void *ctx)
{
    struct open_scan_ctx *c;
    enum mds_status st = MDS_OK;

    if (be_of(cat) == NULL || cb == NULL) {
        return MDS_ERR_INVAL;
    }
    c = calloc(1, sizeof(*c));
    if (c == NULL) {
        return MDS_ERR_NOMEM;
    }
    idx_scan_init(&c->idx, be_of(cat), NFS4_OTHER_SIZE, FDB_KT_OPEN);
    c->fileid = fileid;
    c->clientid = clientid;
    if (fileid != NULL) {
        key_u64_prefix(&c->idx.base, &c->idx.b->prefix, FDB_KT_OPEN_BY_FILE, *fileid);
    } else {
        key_u64_prefix(&c->idx.base, &c->idx.b->prefix, FDB_KT_OPEN_BY_CLIENT, *clientid);
    }
    for (;;) {
        uint32_t i;

        st = fdb_run_txn(c->idx.b, FDB_TXN_READONLY, "open_scan", open_scan_body, c);
        if (st != MDS_OK) {
            break;
        }
        for (i = 0; i < c->n; i++) {
            if (cb(&c->page[i], ctx) != 0) {
                goto out;
            }
        }
        if (!c->idx.more) {
            break;
        }
    }
out:
    free(c);
    return st;
}

static enum mds_status fdb_open_scan_file(struct mds_catalogue *cat, uint64_t fileid,
                                          mds_coord_open_scan_cb cb, void *ctx)
{
    return open_scan_run(cat, &fileid, NULL, cb, ctx);
}

static enum mds_status fdb_open_scan_client(struct mds_catalogue *cat, uint64_t clientid,
                                            mds_coord_open_scan_cb cb, void *ctx)
{
    return open_scan_run(cat, NULL, &clientid, cb, ctx);
}

/* -----------------------------------------------------------------------
 * Byte-range locks
 * ----------------------------------------------------------------------- */

struct lock_ctx {
    struct fdb_backend              *b;
    uint64_t                         fileid;
    uint64_t                         lock_id;
    const struct mds_coord_lock_row *in;
    struct mds_coord_lock_row        row;
    uint8_t                          enc[FDB_LOCK_ENC_MAX];
};

static int lock_put_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct lock_ctx *c = arg;
    struct fdb_key k;
    size_t len = 0;

    if (!fdb_lock_encode(c->in, c->enc, sizeof(c->enc), &len)) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    key_lock(&k, &c->b->prefix, c->in->fileid, c->in->lock_id);
    fdb_txn_set(tr, &k, c->enc, len);
    key_lock_by_owner(&k, &c->b->prefix, c->in);
    if (!fdb_key_ok(&k)) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    set_empty(tr, &k);
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static int lock_del_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct lock_ctx *c = arg;
    struct fdb_key k;
    struct rval rv;
    fdb_error_t err;
    bool ok;

    key_lock(&k, &c->b->prefix, c->fileid, c->lock_id);
    err = raw_get(tr, &k, &rv);
    if (err != 0) {
        return (int)err;
    }
    if (!rv.found) {
        raw_release(&rv);
        *st_out = MDS_ERR_NOTFOUND;
        return FDB_BODY_DONE;
    }
    ok = fdb_lock_decode(rv.val, rv.len, c->fileid, c->lock_id, &c->row);
    raw_release(&rv);
    if (!ok) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    fdb_txn_clear(tr, &k);
    key_lock_by_owner(&k, &c->b->prefix, &c->row);
    if (fdb_key_ok(&k)) {
        fdb_txn_clear(tr, &k);
    }
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static enum mds_status fdb_lock_put(struct mds_catalogue *cat,
                                    const struct mds_coord_lock_row *row)
{
    struct lock_ctx c;

    if (be_of(cat) == NULL || row == NULL || row->owner_len > sizeof(row->owner)) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.b = be_of(cat);
    c.in = row;
    return fdb_run_txn(c.b, FDB_TXN_MUTATING, "lock_put", lock_put_body, &c);
}

static enum mds_status fdb_lock_del(struct mds_catalogue *cat, uint64_t fileid, uint64_t lock_id)
{
    struct lock_ctx c;

    if (be_of(cat) == NULL) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.b = be_of(cat);
    c.fileid = fileid;
    c.lock_id = lock_id;
    return fdb_run_txn(c.b, FDB_TXN_MUTATING, "lock_del", lock_del_body, &c);
}

/* Exclusive end of a lock range; 0 and UINT64_MAX mean "to EOF". */
static uint64_t lock_end(uint64_t offset, uint64_t length)
{
    if (length == 0 || length == UINT64_MAX || length > UINT64_MAX - offset) {
        return UINT64_MAX;
    }
    return offset + length;
}

static bool lock_is_read(uint32_t lock_type)
{
    return lock_type == FDB_READ_LT || lock_type == FDB_READW_LT;
}

static bool lock_owner_eq(const struct mds_coord_lock_row *row, uint64_t clientid,
                          const uint8_t *owner, uint32_t owner_len)
{
    if (row->clientid != clientid || row->owner_len != owner_len) {
        return false;
    }
    return owner_len == 0 || memcmp(row->owner, owner, owner_len) == 0;
}

struct lock_test_ctx {
    struct fdb_backend        *b;
    uint64_t                   fileid;
    uint32_t                   lock_type;
    uint64_t                   offset;
    uint64_t                   length;
    uint64_t                   clientid;
    const uint8_t             *owner;
    uint32_t                   owner_len;
    bool                       conflict_found;
    struct mds_coord_lock_row  conflict;
};

/* True when the stored @p row conflicts with the request in @p c: a
 * DIFFERENT (clientid, owner) whose range overlaps the request and is
 * not read-vs-read (the holder never conflicts with itself). */
static bool lock_row_conflicts(const struct lock_test_ctx *c, const struct mds_coord_lock_row *row)
{
    uint64_t end = lock_end(c->offset, c->length);
    uint64_t r_end = lock_end(row->offset, row->length);

    if (lock_owner_eq(row, c->clientid, c->owner, c->owner_len)) {
        return false;
    }
    if (row->offset >= end || c->offset >= r_end) {
        return false; /* disjoint */
    }
    return !(lock_is_read(c->lock_type) && lock_is_read(row->lock_type));
}

/* Examine one page of LOCK rows; *hit when a conflict was copied into
 * c->conflict.  The cursor advances to the page's last key. */
static fdb_error_t lock_test_page(FDBTransaction *tr, struct lock_test_ctx *c,
                                  const struct fdb_key *base, struct fdb_key *cursor,
                                  bool *more, bool *hit)
{
    struct kvs page;
    uint32_t i;
    fdb_error_t err;

    *hit = false;
    *more = false;
    err = idx_range_read(tr, base, cursor, (int)FDB_COORD_PAGE, &page);
    if (err != 0) {
        return err;
    }
    for (i = 0; i < (uint32_t)page.count && !*hit; i++) {
        FDBKeyValue kv;
        const uint8_t *s;
        struct mds_coord_lock_row row;

        fdb_kv_at(page.kvs, (int)i, &kv);
        s = key_suffix(base, &kv, FDB_KEY_BE64_LEN); /* the lock_id */
        if (s == NULL || !fdb_lock_decode(kv.value, (size_t)kv.value_length, c->fileid,
                                          fdb_get_u64(s), &row)) {
            kvs_release(&page);
            return FDB_ERR_PLATFORM_ERROR;
        }
        if (lock_row_conflicts(c, &row)) {
            c->conflict = row;
            *hit = true;
        }
    }
    if (page.count > 0) {
        FDBKeyValue kv;

        fdb_kv_at(page.kvs, page.count - 1, &kv);
        cursor_set(cursor, &kv);
        *more = page.more;
    }
    kvs_release(&page);
    return 0;
}

/* LOCKT against the persisted rows of the file, page by page inside one
 * read-only transaction (one consistent snapshot). */
static int lock_test_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct lock_test_ctx *c = arg;
    struct fdb_key base;
    struct fdb_key cursor;
    uint32_t pages = 0;
    bool more = false;
    bool hit = false;

    c->conflict_found = false;
    cursor.len = 0;
    cursor.overflow = false;
    key_u64_prefix(&base, &c->b->prefix, FDB_KT_LOCK, c->fileid);
    do {
        fdb_error_t err = lock_test_page(tr, c, &base, &cursor, &more, &hit);

        if (err != 0) {
            return (int)err;
        }
        if (hit) {
            c->conflict_found = true;
            *st_out = MDS_ERR_EXISTS;
            return FDB_BODY_COMMIT;
        }
        if (more && ++pages >= FDB_LOCK_TEST_MAX_PAGES) {
            MDS_LOG_ERROR(LOG_COMP_CAT, "fdb lock_test: file %llu holds more than %u lock "
                          "rows", (unsigned long long)c->fileid,
                          FDB_LOCK_TEST_MAX_PAGES * FDB_COORD_PAGE);
            *st_out = MDS_ERR_IO;
            return FDB_BODY_DONE;
        }
    } while (more);
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

/* MDS_OK = the range is free (*conflict zeroed); MDS_ERR_EXISTS = a
 * conflicting lock exists and *conflict holds its row. */
static enum mds_status fdb_lock_test(struct mds_catalogue *cat, uint64_t fileid,
                                     uint32_t lock_type, uint64_t offset, uint64_t length,
                                     uint64_t clientid, const uint8_t *owner,
                                     uint32_t owner_len, struct mds_coord_lock_row *conflict)
{
    struct lock_test_ctx c;
    enum mds_status st;

    if (be_of(cat) == NULL || (owner_len > 0 && owner == NULL) || conflict == NULL) {
        return MDS_ERR_INVAL;
    }
    memset(conflict, 0, sizeof(*conflict));
    memset(&c, 0, sizeof(c));
    c.b = be_of(cat);
    c.fileid = fileid;
    c.lock_type = lock_type;
    c.offset = offset;
    c.length = length;
    c.clientid = clientid;
    c.owner = owner;
    c.owner_len = owner_len;
    st = fdb_run_txn(c.b, FDB_TXN_READONLY, "lock_test", lock_test_body, &c);
    if (st == MDS_ERR_EXISTS && c.conflict_found) {
        *conflict = c.conflict;
    }
    return st;
}

/* --- lock_scan_file ------------------------------------------------------ */

/* Heap allocated once per call (the page array). */
struct lock_scan_ctx {
    struct fdb_backend        *b;
    uint64_t                   fileid;
    struct fdb_key             cursor;
    bool                       more;
    uint32_t                   n;
    struct mds_coord_lock_row  page[FDB_COORD_PAGE];
};

static int lock_scan_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct lock_scan_ctx *c = arg;
    struct fdb_key base;
    struct kvs page;
    uint32_t i;
    fdb_error_t err;

    c->n = 0;
    c->more = false;
    key_u64_prefix(&base, &c->b->prefix, FDB_KT_LOCK, c->fileid);
    err = idx_range_read(tr, &base, &c->cursor, (int)FDB_COORD_PAGE, &page);
    if (err != 0) {
        return (int)err;
    }
    for (i = 0; i < (uint32_t)page.count && c->n < FDB_COORD_PAGE; i++) {
        FDBKeyValue kv;
        const uint8_t *s;

        fdb_kv_at(page.kvs, (int)i, &kv);
        s = key_suffix(&base, &kv, FDB_KEY_BE64_LEN); /* the lock_id */
        if (s == NULL || !fdb_lock_decode(kv.value, (size_t)kv.value_length, c->fileid,
                                          fdb_get_u64(s), &c->page[c->n])) {
            kvs_release(&page);
            c->n = 0;
            return FDB_ERR_PLATFORM_ERROR;
        }
        c->n++;
    }
    if (page.count > 0) {
        FDBKeyValue kv;

        fdb_kv_at(page.kvs, page.count - 1, &kv);
        cursor_set(&c->cursor, &kv);
        c->more = page.more;
    }
    kvs_release(&page);
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static enum mds_status fdb_lock_scan_file(struct mds_catalogue *cat, uint64_t fileid,
                                          mds_coord_lock_scan_cb cb, void *ctx)
{
    struct lock_scan_ctx *c;
    enum mds_status st = MDS_OK;

    if (be_of(cat) == NULL || cb == NULL) {
        return MDS_ERR_INVAL;
    }
    c = calloc(1, sizeof(*c));
    if (c == NULL) {
        return MDS_ERR_NOMEM;
    }
    c->b = be_of(cat);
    c->fileid = fileid;
    for (;;) {
        uint32_t i;

        st = fdb_run_txn(c->b, FDB_TXN_READONLY, "lock_scan_file", lock_scan_body, c);
        if (st != MDS_OK) {
            break;
        }
        for (i = 0; i < c->n; i++) {
            if (cb(&c->page[i], ctx) != 0) {
                goto out;
            }
        }
        if (!c->more) {
            break;
        }
    }
out:
    free(c);
    return st;
}

/* --- lock_scan_owner ----------------------------------------------------- */

/* Heap allocated once per call (the page arrays). */
struct lock_owner_ctx {
    struct idx_scan_ctx        idx;
    uint64_t                   clientid;
    const uint8_t             *owner;
    uint32_t                   owner_len;
    uint32_t                   n;
    struct mds_coord_lock_row  page[FDB_COORD_PAGE];
};

static int lock_owner_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct lock_owner_ctx *c = arg;
    uint32_t i;
    fdb_error_t err;

    c->n = 0;
    err = idx_scan_page(tr, &c->idx);
    if (err != 0) {
        return (int)err;
    }
    for (i = 0; i < c->idx.n; i++) {
        const uint8_t *s = c->idx.suffixes + (size_t)i * FDB_LOCK_KEY_TAIL;
        struct mds_coord_lock_row *row = &c->page[c->n];

        if (c->idx.vals[i] != NULL &&
            fdb_lock_decode(c->idx.vals[i], c->idx.lens[i], fdb_get_u64(s),
                            fdb_get_u64(s + FDB_KEY_BE64_LEN), row) &&
            lock_owner_eq(row, c->clientid, c->owner, c->owner_len)) {
            c->n++;
        }
    }
    idx_scan_release(&c->idx);
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

/* Every row of one lock-owner (LOCKU lookup). */
static enum mds_status fdb_lock_scan_owner(struct mds_catalogue *cat, uint64_t clientid,
                                           const uint8_t *owner, uint32_t owner_len,
                                           mds_coord_lock_scan_cb cb, void *ctx)
{
    struct lock_owner_ctx *c;
    enum mds_status st = MDS_OK;

    if (be_of(cat) == NULL || cb == NULL || (owner_len > 0 && owner == NULL) ||
        owner_len > 128U) {
        return MDS_ERR_INVAL;
    }
    c = calloc(1, sizeof(*c));
    if (c == NULL) {
        return MDS_ERR_NOMEM;
    }
    idx_scan_init(&c->idx, be_of(cat), FDB_LOCK_KEY_TAIL, FDB_KT_LOCK);
    c->clientid = clientid;
    c->owner = owner;
    c->owner_len = owner_len;
    key_lock_owner_prefix(&c->idx.base, &c->idx.b->prefix, clientid, owner, owner_len);
    for (;;) {
        uint32_t i;

        st = fdb_run_txn(c->idx.b, FDB_TXN_READONLY, "lock_scan_owner", lock_owner_body, c);
        if (st != MDS_OK) {
            break;
        }
        for (i = 0; i < c->n; i++) {
            if (cb(&c->page[i], ctx) != 0) {
                goto out;
            }
        }
        if (!c->idx.more) {
            break;
        }
    }
out:
    free(c);
    return st;
}

/* --- lock_reap_client ---------------------------------------------------- */

struct lock_reap_ctx {
    struct fdb_backend *b;
    uint64_t            clientid;
    bool                more;
    FDBFuture          *fs[FDB_COORD_DEL_BATCH];
    struct fdb_key      idx_keys[FDB_COORD_DEL_BATCH];
    uint64_t            fileids[FDB_COORD_DEL_BATCH];
    uint64_t            lock_ids[FDB_COORD_DEL_BATCH];
};

/* Parse the (be32 owner_len, owner, be64 fileid, be64 lock_id) tail of
 * a LOCK_BY_OWNER key under the per-client prefix @p base. */
static bool lock_owner_key_parse(const struct fdb_key *base, const FDBKeyValue *kv,
                                 uint64_t *fileid, uint64_t *lock_id)
{
    const uint8_t *s;
    size_t rest;
    uint32_t owner_len;

    if (kv->key_length <= 0 ||
        (size_t)kv->key_length < base->len + FDB_KEY_BE32_LEN + FDB_LOCK_KEY_TAIL) {
        return false;
    }
    s = kv->key + base->len;
    rest = (size_t)kv->key_length - base->len;
    owner_len = fdb_get_u32(s);
    if (owner_len > 128U || rest != FDB_KEY_BE32_LEN + (size_t)owner_len + FDB_LOCK_KEY_TAIL) {
        return false;
    }
    s += FDB_KEY_BE32_LEN + owner_len;
    *fileid = fdb_get_u64(s);
    *lock_id = fdb_get_u64(s + FDB_KEY_BE64_LEN);
    return true;
}

/* Start the row reads of one by-owner page; returns the count started. */
static fdb_error_t lock_reap_start(FDBTransaction *tr, struct lock_reap_ctx *c,
                                   const struct fdb_key *base, const struct kvs *page,
                                   uint32_t *n_out)
{
    struct fdb_key k;
    uint32_t n = 0;
    uint32_t i;

    for (i = 0; i < (uint32_t)page->count && n < FDB_COORD_DEL_BATCH; i++) {
        FDBKeyValue kv;

        fdb_kv_at(page->kvs, (int)i, &kv);
        if (!lock_owner_key_parse(base, &kv, &c->fileids[n], &c->lock_ids[n])) {
            futures_drop(c->fs, 0, n);
            return FDB_ERR_PLATFORM_ERROR;
        }
        cursor_set(&c->idx_keys[n], &kv);
        key_lock(&k, &c->b->prefix, c->fileids[n], c->lock_ids[n]);
        c->fs[n] = fdb_txn_get_start(tr, &k, false);
        n++;
    }
    *n_out = n;
    return 0;
}

/* Row @p i of the wave: a row still held by the client is cleared with
 * its by-owner key; the scanned index key goes in any case. */
static fdb_error_t lock_reap_row(FDBTransaction *tr, struct lock_reap_ctx *c, uint32_t i,
                                 const struct rval *rv)
{
    const struct fdb_key_prefix *p = &c->b->prefix;
    struct fdb_key k;

    if (rv->found) {
        struct mds_coord_lock_row row;

        if (!fdb_lock_decode(rv->val, rv->len, c->fileids[i], c->lock_ids[i], &row)) {
            return FDB_ERR_PLATFORM_ERROR;
        }
        if (row.clientid == c->clientid) {
            key_lock(&k, p, c->fileids[i], c->lock_ids[i]);
            fdb_txn_clear(tr, &k);
            key_lock_by_owner(&k, p, &row);
            if (fdb_key_ok(&k)) {
                fdb_txn_clear(tr, &k);
            }
        }
    }
    fdb_txn_clear(tr, &c->idx_keys[i]);
    return 0;
}

/* One batch: the first FDB_COORD_DEL_BATCH by-owner keys of the client
 * and their rows in one wave. */
static int lock_reap_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct lock_reap_ctx *c = arg;
    struct fdb_key base;
    struct fdb_key cursor;
    struct kvs page;
    uint32_t n = 0;
    uint32_t i;
    fdb_error_t err;

    cursor.len = 0;
    cursor.overflow = false;
    key_u64_prefix(&base, &c->b->prefix, FDB_KT_LOCK_BY_OWNER, c->clientid);
    err = idx_range_read(tr, &base, &cursor, (int)FDB_COORD_DEL_BATCH, &page);
    if (err != 0) {
        return (int)err;
    }
    c->more = page.more;
    err = lock_reap_start(tr, c, &base, &page, &n);
    kvs_release(&page);
    if (err != 0) {
        return (int)err;
    }
    for (i = 0; i < n; i++) {
        struct rval rv;
        fdb_error_t e2 = raw_wait(c->fs[i], &rv);

        c->fs[i] = NULL;
        if (e2 == 0) {
            if (err == 0) {
                err = lock_reap_row(tr, c, i, &rv);
            }
            raw_release(&rv);
        } else if (err == 0) {
            err = e2;
        }
    }
    if (err != 0) {
        return (int)err;
    }
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static enum mds_status fdb_lock_reap_client(struct mds_catalogue *cat, uint64_t clientid)
{
    struct lock_reap_ctx *c;
    enum mds_status st;
    uint32_t batches = 0;

    if (be_of(cat) == NULL) {
        return MDS_ERR_INVAL;
    }
    c = calloc(1, sizeof(*c));
    if (c == NULL) {
        return MDS_ERR_NOMEM;
    }
    c->b = be_of(cat);
    c->clientid = clientid;
    do {
        c->more = false;
        st = fdb_run_txn(c->b, FDB_TXN_MUTATING, "lock_reap_client", lock_reap_body, c);
        batches++;
    } while (st == MDS_OK && c->more && batches < FDB_COORD_DEL_MAX_BATCHES);
    if (st == MDS_OK && c->more) {
        MDS_LOG_ERROR(LOG_COMP_CAT, "fdb lock_reap_client: client %llu still has lock rows "
                      "after %u batches", (unsigned long long)clientid, batches);
        st = MDS_ERR_DELAY;
    }
    free(c);
    return st;
}

/* -----------------------------------------------------------------------
 * Delegations
 * ----------------------------------------------------------------------- */

struct deleg_ctx {
    struct fdb_backend               *b;
    const uint8_t                    *other;
    const struct mds_coord_deleg_row *in;
    struct mds_coord_deleg_row        row;
    uint8_t                           enc[FDB_DELEG_ENC_SIZE];
};

static int deleg_put_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct deleg_ctx *c = arg;
    const struct fdb_key_prefix *p = &c->b->prefix;
    struct fdb_key k;
    size_t len = 0;

    if (!fdb_deleg_encode(c->in, c->enc, sizeof(c->enc), &len)) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    key_sid(&k, p, FDB_KT_DELEG, c->in->stateid_other);
    fdb_txn_set(tr, &k, c->enc, len);
    key_u64_sid(&k, p, FDB_KT_DELEG_BY_FILE, c->in->fileid, c->in->stateid_other);
    set_empty(tr, &k);
    key_u64_sid(&k, p, FDB_KT_DELEG_BY_CLIENT, c->in->clientid, c->in->stateid_other);
    set_empty(tr, &k);
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static fdb_error_t deleg_read(FDBTransaction *tr, struct deleg_ctx *c, bool *found)
{
    struct fdb_key k;
    struct rval rv;
    fdb_error_t err;

    *found = false;
    key_sid(&k, &c->b->prefix, FDB_KT_DELEG, c->other);
    err = raw_get(tr, &k, &rv);
    if (err != 0) {
        return err;
    }
    if (rv.found) {
        if (!fdb_deleg_decode(rv.val, rv.len, c->other, &c->row)) {
            raw_release(&rv);
            return FDB_ERR_PLATFORM_ERROR;
        }
        *found = true;
    }
    raw_release(&rv);
    return 0;
}

static int deleg_get_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct deleg_ctx *c = arg;
    bool found = false;
    fdb_error_t err = deleg_read(tr, c, &found);

    if (err != 0) {
        return (int)err;
    }
    *st_out = found ? MDS_OK : MDS_ERR_NOTFOUND;
    return FDB_BODY_COMMIT;
}

static int deleg_del_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct deleg_ctx *c = arg;
    const struct fdb_key_prefix *p = &c->b->prefix;
    struct fdb_key k;
    bool found = false;
    fdb_error_t err = deleg_read(tr, c, &found);

    if (err != 0) {
        return (int)err;
    }
    if (!found) {
        *st_out = MDS_ERR_NOTFOUND;
        return FDB_BODY_DONE;
    }
    key_sid(&k, p, FDB_KT_DELEG, c->other);
    fdb_txn_clear(tr, &k);
    key_u64_sid(&k, p, FDB_KT_DELEG_BY_FILE, c->row.fileid, c->other);
    fdb_txn_clear(tr, &k);
    key_u64_sid(&k, p, FDB_KT_DELEG_BY_CLIENT, c->row.clientid, c->other);
    fdb_txn_clear(tr, &k);
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static enum mds_status fdb_deleg_put(struct mds_catalogue *cat,
                                     const struct mds_coord_deleg_row *row)
{
    struct deleg_ctx c;

    if (be_of(cat) == NULL || row == NULL) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.b = be_of(cat);
    c.in = row;
    return fdb_run_txn(c.b, FDB_TXN_MUTATING, "deleg_put", deleg_put_body, &c);
}

static enum mds_status fdb_deleg_get(struct mds_catalogue *cat, const uint8_t stateid_other[12],
                                     struct mds_coord_deleg_row *row)
{
    struct deleg_ctx c;
    enum mds_status st;

    if (be_of(cat) == NULL || stateid_other == NULL || row == NULL) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.b = be_of(cat);
    c.other = stateid_other;
    st = fdb_run_txn(c.b, FDB_TXN_READONLY, "deleg_get", deleg_get_body, &c);
    if (st == MDS_OK) {
        *row = c.row;
    }
    return st;
}

static enum mds_status fdb_deleg_del(struct mds_catalogue *cat, const uint8_t stateid_other[12])
{
    struct deleg_ctx c;

    if (be_of(cat) == NULL || stateid_other == NULL) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.b = be_of(cat);
    c.other = stateid_other;
    return fdb_run_txn(c.b, FDB_TXN_MUTATING, "deleg_del", deleg_del_body, &c);
}

/* Heap allocated once per call (the page arrays). */
struct deleg_scan_ctx {
    struct idx_scan_ctx         idx;
    const uint64_t             *fileid;
    const uint64_t             *clientid;
    uint32_t                    n;
    struct mds_coord_deleg_row  page[FDB_COORD_PAGE];
};

static int deleg_scan_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct deleg_scan_ctx *c = arg;
    uint32_t i;
    fdb_error_t err;

    c->n = 0;
    err = idx_scan_page(tr, &c->idx);
    if (err != 0) {
        return (int)err;
    }
    for (i = 0; i < c->idx.n; i++) {
        const uint8_t *other = c->idx.suffixes + (size_t)i * NFS4_OTHER_SIZE;
        struct mds_coord_deleg_row *row = &c->page[c->n];

        if (c->idx.vals[i] != NULL &&
            fdb_deleg_decode(c->idx.vals[i], c->idx.lens[i], other, row) &&
            (c->fileid == NULL || row->fileid == *c->fileid) &&
            (c->clientid == NULL || row->clientid == *c->clientid)) {
            c->n++;
        }
    }
    idx_scan_release(&c->idx);
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static enum mds_status deleg_scan_run(const struct mds_catalogue *cat, const uint64_t *fileid,
                                      const uint64_t *clientid, mds_coord_deleg_scan_cb cb,
                                      void *ctx)
{
    struct deleg_scan_ctx *c;
    enum mds_status st = MDS_OK;

    if (be_of(cat) == NULL || cb == NULL) {
        return MDS_ERR_INVAL;
    }
    c = calloc(1, sizeof(*c));
    if (c == NULL) {
        return MDS_ERR_NOMEM;
    }
    idx_scan_init(&c->idx, be_of(cat), NFS4_OTHER_SIZE, FDB_KT_DELEG);
    c->fileid = fileid;
    c->clientid = clientid;
    if (fileid != NULL) {
        key_u64_prefix(&c->idx.base, &c->idx.b->prefix, FDB_KT_DELEG_BY_FILE, *fileid);
    } else {
        key_u64_prefix(&c->idx.base, &c->idx.b->prefix, FDB_KT_DELEG_BY_CLIENT, *clientid);
    }
    for (;;) {
        uint32_t i;

        st = fdb_run_txn(c->idx.b, FDB_TXN_READONLY, "deleg_scan", deleg_scan_body, c);
        if (st != MDS_OK) {
            break;
        }
        for (i = 0; i < c->n; i++) {
            if (cb(&c->page[i], ctx) != 0) {
                goto out;
            }
        }
        if (!c->idx.more) {
            break;
        }
    }
out:
    free(c);
    return st;
}

static enum mds_status fdb_deleg_scan_file(struct mds_catalogue *cat, uint64_t fileid,
                                           mds_coord_deleg_scan_cb cb, void *ctx)
{
    return deleg_scan_run(cat, &fileid, NULL, cb, ctx);
}

static enum mds_status fdb_deleg_scan_client(struct mds_catalogue *cat, uint64_t clientid,
                                             mds_coord_deleg_scan_cb cb, void *ctx)
{
    return deleg_scan_run(cat, NULL, &clientid, cb, ctx);
}

/* -----------------------------------------------------------------------
 * Client identity
 * ----------------------------------------------------------------------- */

struct client_ctx {
    struct fdb_backend                *b;
    uint64_t                           clientid;
    const struct mds_coord_client_row *in;
    struct mds_coord_client_row        row;
    uint8_t                            enc[FDB_CLIENT_ENC_MAX];
};

static int client_put_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct client_ctx *c = arg;
    struct fdb_key k;
    size_t len = 0;

    if (!fdb_client_encode(c->in, c->enc, sizeof(c->enc), &len)) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    key_u64_prefix(&k, &c->b->prefix, FDB_KT_CLIENT, c->in->clientid);
    fdb_txn_set(tr, &k, c->enc, len);
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static fdb_error_t client_read(FDBTransaction *tr, struct client_ctx *c, bool *found)
{
    struct fdb_key k;
    struct rval rv;
    fdb_error_t err;

    *found = false;
    key_u64_prefix(&k, &c->b->prefix, FDB_KT_CLIENT, c->clientid);
    err = raw_get(tr, &k, &rv);
    if (err != 0) {
        return err;
    }
    if (rv.found) {
        if (!fdb_client_decode(rv.val, rv.len, c->clientid, &c->row)) {
            raw_release(&rv);
            return FDB_ERR_PLATFORM_ERROR;
        }
        *found = true;
    }
    raw_release(&rv);
    return 0;
}

static int client_get_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct client_ctx *c = arg;
    bool found = false;
    fdb_error_t err = client_read(tr, c, &found);

    if (err != 0) {
        return (int)err;
    }
    *st_out = found ? MDS_OK : MDS_ERR_NOTFOUND;
    return FDB_BODY_COMMIT;
}

static int client_del_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct client_ctx *c = arg;
    struct fdb_key k;
    bool found = false;
    fdb_error_t err = client_read(tr, c, &found);

    if (err != 0) {
        return (int)err;
    }
    if (!found) {
        *st_out = MDS_ERR_NOTFOUND;
        return FDB_BODY_DONE;
    }
    key_u64_prefix(&k, &c->b->prefix, FDB_KT_CLIENT, c->clientid);
    fdb_txn_clear(tr, &k);
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static enum mds_status fdb_client_put(struct mds_catalogue *cat,
                                      const struct mds_coord_client_row *row)
{
    struct client_ctx *c;
    enum mds_status st;

    if (be_of(cat) == NULL || row == NULL || row->co_ownerid_len > sizeof(row->co_ownerid)) {
        return MDS_ERR_INVAL;
    }
    c = calloc(1, sizeof(*c));
    if (c == NULL) {
        return MDS_ERR_NOMEM;
    }
    c->b = be_of(cat);
    c->in = row;
    st = fdb_run_txn(c->b, FDB_TXN_MUTATING, "client_put", client_put_body, c);
    free(c);
    return st;
}

static enum mds_status fdb_client_get(struct mds_catalogue *cat, uint64_t clientid,
                                      struct mds_coord_client_row *row)
{
    struct client_ctx *c;
    enum mds_status st;

    if (be_of(cat) == NULL || row == NULL) {
        return MDS_ERR_INVAL;
    }
    c = calloc(1, sizeof(*c));
    if (c == NULL) {
        return MDS_ERR_NOMEM;
    }
    c->b = be_of(cat);
    c->clientid = clientid;
    st = fdb_run_txn(c->b, FDB_TXN_READONLY, "client_get", client_get_body, c);
    if (st == MDS_OK) {
        *row = c->row;
    }
    free(c);
    return st;
}

static enum mds_status fdb_client_del(struct mds_catalogue *cat, uint64_t clientid)
{
    struct client_ctx *c;
    enum mds_status st;

    if (be_of(cat) == NULL) {
        return MDS_ERR_INVAL;
    }
    c = calloc(1, sizeof(*c));
    if (c == NULL) {
        return MDS_ERR_NOMEM;
    }
    c->b = be_of(cat);
    c->clientid = clientid;
    st = fdb_run_txn(c->b, FDB_TXN_MUTATING, "client_del", client_del_body, c);
    free(c);
    return st;
}

/* -----------------------------------------------------------------------
 * Sessions
 * ----------------------------------------------------------------------- */

struct session_ctx {
    struct fdb_backend                 *b;
    const uint8_t                      *session_id;
    const struct mds_coord_session_row *in;
    struct mds_coord_session_row        row;
    uint8_t                             enc[FDB_SESSION_ENC_SIZE];
};

static int session_put_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct session_ctx *c = arg;
    const struct fdb_key_prefix *p = &c->b->prefix;
    struct fdb_key k;
    size_t len = 0;

    if (!fdb_session_encode(c->in, c->enc, sizeof(c->enc), &len)) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    key_session(&k, p, c->in->session_id);
    fdb_txn_set(tr, &k, c->enc, len);
    key_session_by_client(&k, p, c->in->clientid, c->in->session_id);
    set_empty(tr, &k);
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static fdb_error_t session_read(FDBTransaction *tr, struct session_ctx *c, bool *found)
{
    struct fdb_key k;
    struct rval rv;
    fdb_error_t err;

    *found = false;
    key_session(&k, &c->b->prefix, c->session_id);
    err = raw_get(tr, &k, &rv);
    if (err != 0) {
        return err;
    }
    if (rv.found) {
        if (!fdb_session_decode(rv.val, rv.len, c->session_id, &c->row)) {
            raw_release(&rv);
            return FDB_ERR_PLATFORM_ERROR;
        }
        *found = true;
    }
    raw_release(&rv);
    return 0;
}

static int session_get_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct session_ctx *c = arg;
    bool found = false;
    fdb_error_t err = session_read(tr, c, &found);

    if (err != 0) {
        return (int)err;
    }
    *st_out = found ? MDS_OK : MDS_ERR_NOTFOUND;
    return FDB_BODY_COMMIT;
}

static int session_del_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct session_ctx *c = arg;
    const struct fdb_key_prefix *p = &c->b->prefix;
    struct fdb_key k;
    bool found = false;
    fdb_error_t err = session_read(tr, c, &found);

    if (err != 0) {
        return (int)err;
    }
    if (!found) {
        *st_out = MDS_ERR_NOTFOUND;
        return FDB_BODY_DONE;
    }
    key_session(&k, p, c->session_id);
    fdb_txn_clear(tr, &k);
    key_session_by_client(&k, p, c->row.clientid, c->session_id);
    fdb_txn_clear(tr, &k);
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static enum mds_status fdb_session_put(struct mds_catalogue *cat,
                                       const struct mds_coord_session_row *row)
{
    struct session_ctx c;

    if (be_of(cat) == NULL || row == NULL) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.b = be_of(cat);
    c.in = row;
    return fdb_run_txn(c.b, FDB_TXN_MUTATING, "session_put", session_put_body, &c);
}

static enum mds_status fdb_session_get(struct mds_catalogue *cat, const uint8_t session_id[16],
                                       struct mds_coord_session_row *row)
{
    struct session_ctx c;
    enum mds_status st;

    if (be_of(cat) == NULL || session_id == NULL || row == NULL) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.b = be_of(cat);
    c.session_id = session_id;
    st = fdb_run_txn(c.b, FDB_TXN_READONLY, "session_get", session_get_body, &c);
    if (st == MDS_OK) {
        *row = c.row;
    }
    return st;
}

static enum mds_status fdb_session_del(struct mds_catalogue *cat, const uint8_t session_id[16])
{
    struct session_ctx c;

    if (be_of(cat) == NULL || session_id == NULL) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.b = be_of(cat);
    c.session_id = session_id;
    return fdb_run_txn(c.b, FDB_TXN_MUTATING, "session_del", session_del_body, &c);
}

/* Heap allocated once per call (the page arrays). */
struct session_scan_ctx {
    struct idx_scan_ctx           idx;
    uint64_t                      clientid;
    uint32_t                      n;
    struct mds_coord_session_row  page[FDB_COORD_PAGE];
};

static int session_scan_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct session_scan_ctx *c = arg;
    uint32_t i;
    fdb_error_t err;

    c->n = 0;
    err = idx_scan_page(tr, &c->idx);
    if (err != 0) {
        return (int)err;
    }
    for (i = 0; i < c->idx.n; i++) {
        const uint8_t *sid = c->idx.suffixes + (size_t)i * 16U;
        struct mds_coord_session_row *row = &c->page[c->n];

        if (c->idx.vals[i] != NULL &&
            fdb_session_decode(c->idx.vals[i], c->idx.lens[i], sid, row) &&
            row->clientid == c->clientid) {
            c->n++;
        }
    }
    idx_scan_release(&c->idx);
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static enum mds_status fdb_session_scan_client(struct mds_catalogue *cat, uint64_t clientid,
                                               mds_coord_session_scan_cb cb, void *ctx)
{
    struct session_scan_ctx *c;
    enum mds_status st = MDS_OK;

    if (be_of(cat) == NULL || cb == NULL) {
        return MDS_ERR_INVAL;
    }
    c = calloc(1, sizeof(*c));
    if (c == NULL) {
        return MDS_ERR_NOMEM;
    }
    idx_scan_init(&c->idx, be_of(cat), 16U, FDB_KT_SESSION);
    c->clientid = clientid;
    key_u64_prefix(&c->idx.base, &c->idx.b->prefix, FDB_KT_SESSION_BY_CLIENT, clientid);
    for (;;) {
        uint32_t i;

        st = fdb_run_txn(c->idx.b, FDB_TXN_READONLY, "session_scan_client", session_scan_body,
                         c);
        if (st != MDS_OK) {
            break;
        }
        for (i = 0; i < c->n; i++) {
            if (cb(&c->page[i], ctx) != 0) {
                goto out;
            }
        }
        if (!c->idx.more) {
            break;
        }
    }
out:
    free(c);
    return st;
}

/* -----------------------------------------------------------------------
 * DRC slots
 * ----------------------------------------------------------------------- */

struct slot_ctx {
    struct fdb_backend *b;
    const uint8_t      *session_id;
    uint32_t            slot_id;
    struct fdb_slot_hdr hdr;
    const uint8_t      *reply_in;   /**< put */
    uint8_t            *reply_out;  /**< get: heap copy for the caller */
    uint8_t             enc[FDB_SLOT_ENC_MAX];
};

static int slot_put_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct slot_ctx *c = arg;
    struct fdb_key k;
    size_t len = 0;

    c->hdr.last_used_ns = realtime_ns();
    if (!fdb_slot_encode(&c->hdr, c->reply_in, c->enc, sizeof(c->enc), &len)) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    key_slot(&k, &c->b->prefix, c->session_id, c->slot_id);
    fdb_txn_set(tr, &k, c->enc, len);
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static int slot_get_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct slot_ctx *c = arg;
    struct fdb_key k;
    struct rval rv;
    const uint8_t *reply = NULL;
    fdb_error_t err;

    free(c->reply_out); /* a previous attempt's copy */
    c->reply_out = NULL;
    key_slot(&k, &c->b->prefix, c->session_id, c->slot_id);
    err = raw_get(tr, &k, &rv);
    if (err != 0) {
        return (int)err;
    }
    if (!rv.found) {
        raw_release(&rv);
        *st_out = MDS_ERR_NOTFOUND;
        return FDB_BODY_DONE;
    }
    if (!fdb_slot_decode(rv.val, rv.len, &c->hdr, &reply)) {
        raw_release(&rv);
        return FDB_ERR_PLATFORM_ERROR;
    }
    if (c->hdr.reply_len > 0) {
        c->reply_out = malloc(c->hdr.reply_len);
        if (c->reply_out == NULL) {
            raw_release(&rv);
            *st_out = MDS_ERR_NOMEM;
            return FDB_BODY_DONE;
        }
        memcpy(c->reply_out, reply, c->hdr.reply_len);
    }
    raw_release(&rv);
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static enum mds_status fdb_slot_put(struct mds_catalogue *cat, const uint8_t session_id[16],
                                    uint32_t slot_id, uint32_t seq_id, const void *cached_reply,
                                    uint32_t reply_len)
{
    struct slot_ctx *c;
    enum mds_status st;

    if (be_of(cat) == NULL || session_id == NULL || (reply_len > 0 && cached_reply == NULL)) {
        return MDS_ERR_INVAL;
    }
    if (reply_len > FDB_SLOT_REPLY_MAX) {
        return MDS_ERR_NOSPC; /* the store's bound; never truncated silently */
    }
    c = calloc(1, sizeof(*c));
    if (c == NULL) {
        return MDS_ERR_NOMEM;
    }
    c->b = be_of(cat);
    c->session_id = session_id;
    c->slot_id = slot_id;
    c->hdr.seq_id = seq_id;
    c->hdr.reply_len = reply_len;
    c->reply_in = cached_reply;
    st = fdb_run_txn(c->b, FDB_TXN_MUTATING, "slot_put", slot_put_body, c);
    free(c);
    return st;
}

/* row->cached_reply is a heap copy the caller frees (NULL when empty). */
static enum mds_status fdb_slot_get(struct mds_catalogue *cat, const uint8_t session_id[16],
                                    uint32_t slot_id, struct mds_coord_drc_slot_row *row)
{
    struct slot_ctx *c;
    enum mds_status st;

    if (be_of(cat) == NULL || session_id == NULL || row == NULL) {
        return MDS_ERR_INVAL;
    }
    c = calloc(1, sizeof(*c));
    if (c == NULL) {
        return MDS_ERR_NOMEM;
    }
    c->b = be_of(cat);
    c->session_id = session_id;
    c->slot_id = slot_id;
    st = fdb_run_txn(c->b, FDB_TXN_READONLY, "slot_get", slot_get_body, c);
    if (st == MDS_OK) {
        memset(row, 0, sizeof(*row));
        memcpy(row->session_id, session_id, sizeof(row->session_id));
        row->slot_id = slot_id;
        row->seq_id = c->hdr.seq_id;
        row->cached_reply = c->reply_out;
        row->reply_len = c->hdr.reply_len;
        row->last_used_ns = c->hdr.last_used_ns;
        c->reply_out = NULL;
    }
    free(c->reply_out);
    free(c);
    return st;
}

/* -----------------------------------------------------------------------
 * Vtable
 * ----------------------------------------------------------------------- */

const struct mds_coordination_ops fdb_coordination_ops = {
    .journal_put               = fdb_journal_put,
    .journal_get               = fdb_journal_get,
    .journal_del               = fdb_journal_del,
    .journal_scan              = fdb_journal_scan,
    .layout_grant              = fdb_layout_grant,
    .layout_grant_union        = fdb_layout_grant_union,
    .layoutget_fused           = fdb_layoutget_fused,
    .layout_return             = fdb_layout_return,
    .layout_get_by_stateid     = fdb_layout_get_by_stateid,
    .layout_scan_for_file      = fdb_layout_scan_for_file,
    .layout_del_all_for_client = fdb_layout_del_all_for_client,
    .ds_layout_idx_scan        = fdb_ds_layout_idx_scan,
    .layout_iter_file          = fdb_layout_iter_file,
    .recovery_put              = fdb_recovery_put,
    .recovery_del              = fdb_recovery_del,
    .recovery_get              = fdb_recovery_get,
    .recovery_list             = fdb_recovery_list,
    .open_put                  = fdb_open_put,
    .open_get                  = fdb_open_get,
    .open_del                  = fdb_open_del,
    .open_scan_file            = fdb_open_scan_file,
    .open_scan_client          = fdb_open_scan_client,
    .lock_put                  = fdb_lock_put,
    .lock_del                  = fdb_lock_del,
    .lock_test                 = fdb_lock_test,
    .lock_scan_file            = fdb_lock_scan_file,
    .lock_scan_owner           = fdb_lock_scan_owner,
    .lock_reap_client          = fdb_lock_reap_client,
    .deleg_put                 = fdb_deleg_put,
    .deleg_get                 = fdb_deleg_get,
    .deleg_del                 = fdb_deleg_del,
    .deleg_scan_file           = fdb_deleg_scan_file,
    .deleg_scan_client         = fdb_deleg_scan_client,
    .client_put                = fdb_client_put,
    .client_get                = fdb_client_get,
    .client_del                = fdb_client_del,
    .session_put               = fdb_session_put,
    .session_get               = fdb_session_get,
    .session_del               = fdb_session_del,
    .session_scan_client       = fdb_session_scan_client,
    .slot_put                  = fdb_slot_put,
    .slot_get                  = fdb_slot_get,
};
