/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * catalogue_daos_coord.c -- DAOS backend: coordination slots.
 *
 * Every slot is ONE daos_run_txn (daos_txn.h); the mds_cat_txn token is
 * never enlisted (C6).  Reads that can be issued together are started
 * as one parallel wave on the transaction's event queue and finished
 * afterwards, so a slot pays one round-trip latency per dependent wave,
 * not per row.  Enumerating slots materialise a bounded page inside a
 * read-only transaction and deliver it after daos_run_txn returned
 * (C1/C2): a callback may re-enter this handle and a retried attempt
 * never redelivers.  Statuses follow the FoundationDB unit
 * (catalogue_fdb_coord.c) and through it the memdb reference
 * implementation, the parity oracle for every semantic detail.
 *
 * Rows and their indexes (daos_keys.h; values in fdb_codec.h):
 *   LAYOUT_STATE dkey stateid         INODE 'L', CLIENT 'L' (akey stateid),
 *                                     DS_LAYOUT_IDX(ds) dkey (clientid,
 *                                     fileid, stateid), one per DS in the row
 *   OPEN dkey stateid                 INODE 'O', CLIENT 'O'
 *   INODE 'K' akey lock_id            CLIENT 'K' akey (owner, fileid, lock_id)
 *   DELEG dkey stateid                INODE 'D', CLIENT 'D'
 *   CLIENT 'c'                        --
 *   SESSION dkey session_id akey 's'  CLIENT 'S' (akey session_id)
 *   SESSION akey "slot" + be32 slot   --
 *   CLIENT 'R'                        RECOVERY_BY_OWNER(owner) dkey clientid
 *   JOURNAL dkey (txn_id, role)       --
 *
 * Index entries are one-byte markers written and punched in the SAME
 * transaction as their row and authoritative only together with it:
 * every index-driven scan fetches the rows in one parallel wave and
 * skips an entry whose row is absent or names another (fileid,
 * clientid) -- such an orphan can only be left by a re-put that rebinds
 * a key the protocol never rebinds (a stateid belongs to one file and
 * one client for its life).  layout_scan_for_file alone trusts the
 * INODE 'L' index without the row read: a false positive can only cause
 * an over-recall, which is safe.  The hot-path puts (layout_grant,
 * open / lock / deleg / client / session / slot put) are therefore blind
 * writes with no read.
 *
 * Index markers and DS index rows are written through handles opened
 * for the one operation (daos_obj_open_tag): the write is buffered in
 * the transaction, which keeps its own reference to the object, so the
 * handle closes at once.  This keeps the attempt's tracked-handle table
 * (DAOS_TXN_OBJ_MAX) for the row objects while a grant may touch one
 * DS_LAYOUT_IDX object per data server and a bulk delete one INODE
 * object per row.  Waves over rows that live in different objects (lock
 * rows, recovery rows) open one such handle per row for the fetch.
 *
 * No key is written twice in one transaction: a rebind punches only the
 * markers whose key changes and lets the row update overwrite the rest,
 * DS lists are deduplicated before their markers are written, and the
 * bulk deletes punch a scanned index key exactly once.  A body never
 * reads what it wrote (daos_txn.h: no read-your-writes); the delete
 * slots that need no index key (journal_del, client_del) are one
 * conditional punch (DAOS_COND_PUNCH) and let the store report the
 * absent row as -DER_NONEXIST -> MDS_ERR_NOTFOUND.
 *
 * LAYOUT_STATE is keyed by stateid.other alone (memdb keys the row by
 * (fileid, stateid)): a grant or union under a stateid already bound to
 * a different fileid rebinds it, clearing the old file's index keys in
 * the same transaction; layout_return with fileid != 0 reports
 * MDS_ERR_NOTFOUND when the row names another file (memdb parity) and
 * matches the stateid alone when fileid == 0.
 *
 * Hashed enumeration.  The by-file / by-client indexes are hashed akeys
 * and the spread objects (JOURNAL, DS_LAYOUT_IDX) hashed dkeys: a
 * listing is a stable total order with strict start-after resume, not
 * a sorted one.  Hence journal_scan collects every sort key before
 * delivering (as the FoundationDB unit does), ds_layout_idx_scan
 * deduplicates (clientid, fileid) pairs through a bounded set at
 * delivery time instead of relying on contiguous keys, and
 * lock_scan_owner filters the client's whole lock index by the owner
 * prefix.  recovery_list(0) has no table to range-read: it visits the
 * RECOVERY_BY_OWNER objects of every owner 0..MDS_MAX_NODES, the id
 * space of the node registry.
 *
 * Transaction shapes (F = fetch RPC, L = key listing, U = buffered
 * update, P = buffered punch, || = one parallel wave, -> = dependent
 * wave; every mutating transaction ends in one commit RPC; "(none)" is
 * a single operation without a transaction handle):
 *   journal_put            U row                                     (blind)
 *   journal_get            F row                                     (none)
 *   journal_del            P row dkey, DAOS_COND_PUNCH    (NOTFOUND if absent)
 *   journal_scan           per page: L JOURNAL dkeys -> F || ... || F rows
 *                          (sort keys); then per batch of 16: F || ... || F
 *                          rows -> deliver oldest first              (read-only)
 *   layout_grant           U row, U INODE 'L', U CLIENT 'L', U ds_idx x n (blind)
 *   layout_grant_union     F row; absent: as grant / present: U union row,
 *                          [P + U client-keyed indexes when the clientid
 *                          moved] / other fileid: P old INODE 'L' + ds_idx
 *                          [+ CLIENT 'L'] + as grant
 *   layoutget_fused        F INODE 's' map blob [-> F again into a buffer of
 *                          the reported size]; U row + indexes (DS list
 *                          derived from the entries)
 *   layout_return          F row; P row + INODE 'L' + CLIENT 'L' + ds_idx
 *   layout_get_by_stateid  F row                                     (none)
 *   layout_scan_for_file   L INODE 'L' nr=1                          (none)
 *   layout_del_all_for_client per batch of 32: L CLIENT 'L' -> F || ... || F
 *                          rows; P rows + INODE 'L' + ds_idx + CLIENT 'L'
 *                          (one commit per batch)
 *   ds_layout_idx_scan     per page: L ds_idx dkeys -> F || ... || F rows
 *                          (validate clientid, fileid, DS membership);
 *                          pairs deduplicated at delivery            (read-only)
 *   layout_iter_file       per page: L INODE 'L' -> F || ... || F rows (read-only)
 *   recovery_put           F row; U row, U by_owner [P old by_owner]
 *   recovery_del           F row; P row + by_owner                   (absent: OK)
 *   recovery_get           F row                                     (none)
 *   recovery_list          per page: L by_owner(f) -> F || ... || F CLIENT 'R'
 *                          rows, then the same for owner 0; f == 0: every
 *                          owner 0..MDS_MAX_NODES                    (read-only)
 *   open_put / deleg_put   U row, U INODE idx, U CLIENT idx            (blind)
 *   open_get / deleg_get   F row                                     (none)
 *   open_del / deleg_del   F row; P row + INODE idx + CLIENT idx
 *   open/deleg_scan_file, _scan_client
 *                          per page: L index -> F || ... || F rows    (read-only)
 *   lock_put               U INODE 'K' row, U CLIENT 'K' index         (blind)
 *   lock_del               F row; P row + CLIENT 'K' index
 *   lock_test              per page: L INODE 'K' -> ONE F of the page's akeys
 *                          (paged inside one read-only transaction)
 *   lock_scan_file         per page: L INODE 'K' -> ONE F             (read-only)
 *   lock_scan_owner        per page: L CLIENT 'K' (owner filter) ->
 *                          F || ... || F INODE 'K' rows               (read-only)
 *   lock_reap_client       per batch of 64: L CLIENT 'K' -> F || ... || F
 *                          rows; P rows + indexes
 *   client_put             U row                                     (blind)
 *   client_get             F row                                     (none)
 *   client_del             P row dkey, DAOS_COND_PUNCH    (NOTFOUND if absent)
 *   session_put            U row, U CLIENT 'S'                         (blind)
 *   session_get            F row                                     (none)
 *   session_del            F row; P row akey + CLIENT 'S'
 *   session_scan_client    per page: L CLIENT 'S' -> F || ... || F rows (read-only)
 *   slot_put               U slot akey                               (blind)
 *   slot_get               F slot akey                               (none)
 */

#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "catalogue_daos.h"
#include "catalogue_daos_internal.h"
#include "catalogue_internal.h"
#include "daos_codec.h"
#include "daos_keys.h"
#include "daos_txn.h"
#include "fdb_codec.h"
#include "layout_ds_ids.h"
#include "layout_range.h"
#include "mds_log.h"
#include "open_state.h"

/* Index entries materialised per read-only page (one transaction each). */
#define DAOS_COORD_PAGE            256U
/* Rows per page of the layout-row waves: a layout row can carry
 * FDB_LAYOUT_DS_MAX ids (FDB_LAYOUT_ENC_MAX bytes), so the value buffers
 * of a page are sized for the widest row and the page is kept small. */
#define DAOS_LAYOUT_PAGE           32U
/* Rows cleared per mutating batch of the lock reap. */
#define DAOS_COORD_DEL_BATCH       64U
/* Bulk deletes stop after this many batches: a store that gains rows
 * faster than a batch clears them (a grant storm on an expiring client)
 * must not turn the caller into an unbounded loop; MDS_ERR_DELAY then. */
#define DAOS_COORD_DEL_MAX_BATCHES 1024U
/* Lock rows examined per LOCKT before the slot gives up (MDS_ERR_IO):
 * DAOS_COORD_PAGE rows per listing. */
#define DAOS_LOCK_TEST_MAX_PAGES   256U
/* Records a journal scan can order.  The scan sorts by created_at_ns
 * (memdb parity), which needs every sort key in memory before the first
 * delivery; the bound (16384 x 24 bytes) is far above any live 2PC
 * population and keeps that memory fixed.  Beyond it the scan refuses
 * with MDS_ERR_NOSPC rather than deliver an unordered subset. */
#define DAOS_JOURNAL_SCAN_MAX      16384U
/* Journal rows listed and fetched per read-only page of the key pass. */
#define DAOS_JOURNAL_PAGE          64U
/* Journal records fetched per read-only batch after sorting. */
#define DAOS_JOURNAL_FETCH         16U
/* Distinct (clientid, fileid) pairs one ds_layout_idx_scan delivers.
 * The DS index is hashed, so the stateids of one pair are not adjacent
 * and "delivered once" needs the delivered set in memory; the bound is
 * twice what the recall path consumes (layout_recall.c stops at 4096).
 * Beyond it the scan refuses with MDS_ERR_NOSPC rather than repeat a
 * pair. */
#define DAOS_DS_IDX_PAIRS_MAX      8192U
#define DAOS_DS_IDX_SEEN_SLOTS     (2U * DAOS_DS_IDX_PAIRS_MAX)
/* First-tier buffer of the fused LAYOUTGET stripe-map read: a 128-stripe
 * map with 4 mirrors and 60-byte handles is about 40 KB, so one fetch
 * serves nearly every grant; a wider map costs a second fetch into a
 * buffer of the exact size the client reported. */
#define DAOS_FUSED_MAP_FIRST       65536U
/* A DS_LAYOUT_IDX dkey: be64 clientid + be64 fileid + stateid_other. */
#define DAOS_DS_IDX_DKEY_LEN       (2U * DAOS_KEY_BE64_LEN + NFS4_OTHER_SIZE)
/* A JOURNAL dkey: be64 txn_id + u8 role. */
#define DAOS_JOURNAL_DKEY_LEN      (DAOS_KEY_BE64_LEN + DAOS_KEY_U8_LEN)
/* Tail of a CLIENT 'K' akey after the owner: be64 fileid + be64 lock_id. */
#define DAOS_LOCK_IDX_TAIL         (2U * DAOS_KEY_BE64_LEN)
/* Longest lock owner (mds_coord_lock_row.owner). */
#define DAOS_LOCK_OWNER_MAX        128U

/* LAYOUTIOMODE4_RW: the union keeps a stateid ever granted RW at RW
 * (compound.h drags in the RPC headers; same local constant as memdb). */
#define DAOS_LAYOUTIOMODE_RW       2U

/* RFC 8881 nfs_lock_type4; blocking variants normalise to their plain
 * type for conflict purposes (lock_state.h drags in compound.h). */
#define DAOS_READ_LT               1U
#define DAOS_WRITE_LT              2U
#define DAOS_READW_LT              3U
#define DAOS_WRITEW_LT             4U

_Static_assert(FDB_LAYOUT_DS_MAX == MDS_LAYOUT_DS_ID_MAX,
               "layout row DS bound must equal the dispatcher ceiling");
_Static_assert((DAOS_DS_IDX_SEEN_SLOTS & (DAOS_DS_IDX_SEEN_SLOTS - 1U)) == 0,
               "the delivered-pair set is indexed by mask");
_Static_assert(DAOS_JOURNAL_FETCH <= DAOS_JOURNAL_PAGE, "one wave serves both journal phases");
_Static_assert(DAOS_COORD_DEL_BATCH <= DAOS_COORD_PAGE, "a delete batch is at most one page");
_Static_assert(DAOS_LOCK_OWNER_MAX == sizeof(((struct mds_coord_lock_row *)0)->owner),
               "lock owner bound must match the row");

/* -----------------------------------------------------------------------
 * Small helpers
 * ----------------------------------------------------------------------- */

static struct daos_backend *be_of(const struct mds_catalogue *cat)
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

/* Fetch one single-value akey into a codec-bounded buffer.  A value
 * larger than @p cap is a corrupt or oversized row (-DER_IO), never a
 * reason to retry with a bigger buffer. */
static int row_fetch(struct daos_txn *t, daos_handle_t oh, const struct daos_key *dkey,
                     struct daos_key *akey, void *buf, size_t cap, size_t *len, bool *found)
{
    int rc = daos_fetch_single(t, oh, dkey, akey, buf, cap, len, found);

    return (rc == -DER_REC2BIG) ? -DER_IO : rc;
}

/* Set the index marker @p akey under @p dkey of the object (@p tag,
 * @p id) through a handle opened for this one buffered operation (see
 * the file comment). */
static int idx_set(struct daos_txn *t, enum daos_tag tag, uint64_t id,
                   const struct daos_key *dkey, struct daos_key *akey)
{
    daos_handle_t oh;
    int rc = daos_obj_open_tag(t->b, tag, id, &oh);

    if (rc != 0) {
        return rc;
    }
    rc = daos_marker_set(t, oh, dkey, akey);
    daos_obj_close_h(&oh);
    return rc;
}

/* Punch the akey @p akey under @p dkey of the object (@p tag, @p id);
 * an absent key is a no-op. */
static int idx_clear(struct daos_txn *t, enum daos_tag tag, uint64_t id,
                     const struct daos_key *dkey, struct daos_key *akey)
{
    daos_handle_t oh;
    int rc = daos_obj_open_tag(t->b, tag, id, &oh);

    if (rc != 0) {
        return rc;
    }
    rc = daos_marker_clear(t, oh, dkey, akey);
    daos_obj_close_h(&oh);
    return rc;
}

/* Punch the whole dkey @p dkey of the object (@p tag, @p id). */
static int dkey_clear(struct daos_txn *t, enum daos_tag tag, uint64_t id, struct daos_key *dkey)
{
    daos_handle_t oh;
    int rc = daos_obj_open_tag(t->b, tag, id, &oh);

    if (rc != 0) {
        return rc;
    }
    rc = daos_punch_dkeys(t, oh, dkey, 1, 0);
    daos_obj_close_h(&oh);
    return rc;
}

/* -----------------------------------------------------------------------
 * CLIENT 'K' index akey: be32 owner_len + owner + be64 fileid + be64
 * lock_id.  Built here rather than with daos_akey_lock_owner(): that
 * builder refuses an empty owner, which the lock row format
 * (fdb_lock_encode) and the other backends accept.
 * ----------------------------------------------------------------------- */

static void lock_idx_akey(struct daos_key *k, const uint8_t *owner, uint32_t owner_len,
                          uint64_t fileid, uint64_t lock_id)
{
    daos_key_init(k);
    if (owner_len > DAOS_LOCK_OWNER_MAX || (owner_len > 0 && owner == NULL)) {
        k->overflow = true;
        return;
    }
    daos_key_be32(k, owner_len);
    daos_key_bytes(k, owner, owner_len);
    daos_key_be64(k, fileid);
    daos_key_be64(k, lock_id);
}

/* Parse a CLIENT 'K' akey; false for a malformed key. */
static bool lock_idx_key_parse(const uint8_t *key, size_t len, uint32_t *owner_len,
                               const uint8_t **owner, uint64_t *fileid, uint64_t *lock_id)
{
    uint32_t ol;

    if (len < DAOS_KEY_BE32_LEN + DAOS_LOCK_IDX_TAIL) {
        return false;
    }
    ol = fdb_get_u32(key);
    if (ol > DAOS_LOCK_OWNER_MAX || len != DAOS_KEY_BE32_LEN + (size_t)ol + DAOS_LOCK_IDX_TAIL) {
        return false;
    }
    *owner_len = ol;
    *owner = key + DAOS_KEY_BE32_LEN;
    *fileid = fdb_get_u64(key + DAOS_KEY_BE32_LEN + ol);
    *lock_id = fdb_get_u64(key + DAOS_KEY_BE32_LEN + ol + DAOS_KEY_BE64_LEN);
    return true;
}

static bool owner_bytes_eq(const uint8_t *a, uint32_t alen, const uint8_t *b, uint32_t blen)
{
    if (alen != blen) {
        return false;
    }
    return alen == 0 || memcmp(a, b, alen) == 0;
}

/* -----------------------------------------------------------------------
 * One page of keys (daos_txn.h listing) with its strict start-after
 * cursor.  The cursor advances only after the page has been fully
 * processed, so a re-run of the body that lists it starts from the same
 * position.
 * ----------------------------------------------------------------------- */

struct keypage {
    daos_key_desc_t *kds;
    uint8_t         *buf;
    struct daos_list l;
    struct daos_key  cursor;   /**< Last key of the previous page; empty = start. */
    uint32_t         cap;
};

static void keypage_free(struct keypage *p)
{
    free(p->kds);
    free(p->buf);
    p->kds = NULL;
    p->buf = NULL;
}

static enum mds_status keypage_alloc(struct keypage *p, uint32_t cap)
{
    memset(p, 0, sizeof(*p));
    p->kds = calloc(cap, sizeof(*p->kds));
    p->buf = calloc(cap, DAOS_KEY_MAX);
    if (p->kds == NULL || p->buf == NULL) {
        keypage_free(p);
        return MDS_ERR_NOMEM;
    }
    p->cap = cap;
    daos_key_init(&p->cursor);
    return MDS_OK;
}

static void keypage_rewind(struct keypage *p)
{
    daos_key_init(&p->cursor);
}

/* List the next page after the cursor: the akeys of @p dkey, or the
 * dkeys of the object when @p dkey is NULL. */
static int keypage_list(struct daos_txn *t, daos_handle_t oh, struct daos_key *dkey,
                        struct keypage *p, uint32_t limit)
{
    struct daos_key *resume = (p->cursor.len > 0) ? &p->cursor : NULL;

    daos_list_init(&p->l, p->kds, limit, p->buf, (size_t)p->cap * DAOS_KEY_MAX);
    if (dkey != NULL) {
        return daos_list_akeys(t, oh, dkey, resume, &p->l);
    }
    return daos_list_dkeys(t, oh, resume, &p->l);
}

/* Key @p i of the page when the keys are walked in order: @p off is the
 * running offset (0 before key 0), advanced past the returned key. */
static const uint8_t *keypage_key(const struct keypage *p, uint32_t i, size_t *off, size_t *len)
{
    const uint8_t *k = p->buf + *off;

    *len = (size_t)p->kds[i].kd_key_len;
    *off += *len;
    return k;
}

/* Move the cursor past the page; true when another page follows. */
static bool keypage_next(struct keypage *p)
{
    const uint8_t *k = NULL;
    size_t off = 0;
    size_t len = 0;
    uint32_t i;

    if (p->l.nr == 0) {
        return false;
    }
    for (i = 0; i < p->l.nr; i++) {
        k = keypage_key(p, i, &off, &len);
    }
    daos_key_init(&p->cursor);
    daos_key_bytes(&p->cursor, k, len);
    return !p->l.eof;
}

/* -----------------------------------------------------------------------
 * One parallel fetch wave: N single-value reads, one dkey/akey each,
 * started together and finished together (daos_txn.h).  The rows live
 * either in one object (@p oh valid: the spread row objects) or one per
 * entry (@p oh invalid: the object (@p tag, ents[i].id), opened for the
 * fetch and closed after it).
 * ----------------------------------------------------------------------- */

struct wave_ent {
    struct daos_fetch_small f;
    struct daos_key         dkey;
    struct daos_key         akey;
    daos_handle_t           oh;
    uint64_t                id;
    bool                    started;
};

struct wave {
    struct wave_ent *ents;
    uint8_t         *vals;
    size_t           val_max;
    uint32_t         cap;
    uint32_t         n;
};

static void wave_free(struct wave *w)
{
    free(w->ents);
    free(w->vals);
    w->ents = NULL;
    w->vals = NULL;
}

static enum mds_status wave_alloc(struct wave *w, uint32_t cap, size_t val_max)
{
    memset(w, 0, sizeof(*w));
    w->ents = calloc(cap, sizeof(*w->ents));
    w->vals = malloc((size_t)cap * val_max);
    if (w->ents == NULL || w->vals == NULL) {
        wave_free(w);
        return MDS_ERR_NOMEM;
    }
    w->cap = cap;
    w->val_max = val_max;
    return MDS_OK;
}

static void wave_reset(struct wave *w)
{
    w->n = 0;
}

/* Add one request (object id @p id); the caller fills dkey and akey.
 * NULL when the wave is full. */
static struct wave_ent *wave_add(struct wave *w, uint64_t id)
{
    struct wave_ent *e;

    if (w->n >= w->cap) {
        return NULL;
    }
    e = &w->ents[w->n];
    w->n++;
    e->oh = DAOS_HDL_INVAL;
    e->id = id;
    e->started = false;
    daos_key_init(&e->dkey);
    daos_key_init(&e->akey);
    return e;
}

static uint8_t *wave_val(const struct wave *w, uint32_t i)
{
    return w->vals + (size_t)i * w->val_max;
}

/* Stored size of row @p i after a finished wave (0 = absent). */
static size_t wave_len(const struct wave *w, uint32_t i)
{
    return daos_fetch_size(&w->ents[i].f.f, 0);
}

/* Issue and finish every request.  The first error wins; a value larger
 * than val_max is a corrupt row (-DER_IO). */
static int wave_run(struct daos_txn *t, struct wave *w, daos_handle_t oh, enum daos_tag tag)
{
    bool per_obj = !daos_handle_is_valid(oh);
    uint32_t i;
    int rc = 0;

    for (i = 0; i < w->n && rc == 0; i++) {
        struct wave_ent *e = &w->ents[i];

        e->oh = oh;
        if (per_obj) {
            rc = daos_obj_open_tag(t->b, tag, e->id, &e->oh);
            if (rc != 0) {
                break;
            }
        }
        daos_fetch_small_init(&e->f, &e->dkey);
        if (!daos_fetch_add(&e->f.f, &e->akey, wave_val(w, i), w->val_max)) {
            rc = -DER_INVAL;
            break;
        }
        rc = daos_fetch_start(t, e->oh, &e->f.f);
        e->started = (rc == 0);
    }
    for (i = 0; i < w->n; i++) {
        struct wave_ent *e = &w->ents[i];

        if (e->started) {
            int rc2 = daos_fetch_finish(t, &e->f.f);

            e->started = false;
            if (rc == 0) {
                rc = (rc2 == -DER_REC2BIG) ? -DER_IO : rc2;
            }
        }
        if (per_obj) {
            daos_obj_close_h(&e->oh);
        }
    }
    return rc;
}

/* -----------------------------------------------------------------------
 * Index page -> row wave, the shape of the open / deleg / session
 * scans: one page of the @p idx_dkey akeys of the object (@p idx_tag,
 * @p idx_id), each key (@p key_len bytes) being the dkey of one row of
 * the spread object @p row_tag whose value lives under @p row_akey.
 * After a page, keypage_key() walks the keys and wave_val() / wave_len()
 * give row i (0 = absent).
 * ----------------------------------------------------------------------- */

struct idx_scan {
    enum daos_tag  idx_tag;
    uint64_t       idx_id;
    uint8_t        idx_dkey;
    enum daos_tag  row_tag;
    uint8_t        row_akey;
    size_t         key_len;
    struct keypage kp;
    struct wave    w;
    bool           more;
};

static void idx_scan_free(struct idx_scan *s)
{
    keypage_free(&s->kp);
    wave_free(&s->w);
}

static enum mds_status idx_scan_alloc(struct idx_scan *s, uint32_t cap, size_t val_max)
{
    if (keypage_alloc(&s->kp, cap) != MDS_OK) {
        return MDS_ERR_NOMEM;
    }
    if (wave_alloc(&s->w, cap, val_max) != MDS_OK) {
        keypage_free(&s->kp);
        return MDS_ERR_NOMEM;
    }
    return MDS_OK;
}

static int idx_scan_page(struct daos_txn *t, struct idx_scan *s, uint32_t limit)
{
    struct daos_key dk;
    daos_handle_t oh_idx;
    daos_handle_t oh_row;
    size_t off = 0;
    uint32_t i;
    int rc;

    s->more = false;
    rc = daos_txn_obj(t, s->idx_tag, s->idx_id, &oh_idx);
    if (rc != 0) {
        return rc;
    }
    rc = daos_txn_obj(t, s->row_tag, 0, &oh_row);
    if (rc != 0) {
        return rc;
    }
    daos_key_init(&dk);
    daos_key_u8(&dk, s->idx_dkey);
    rc = keypage_list(t, oh_idx, &dk, &s->kp, limit);
    if (rc != 0) {
        return rc;
    }
    wave_reset(&s->w);
    for (i = 0; i < s->kp.l.nr; i++) {
        size_t len;
        const uint8_t *key = keypage_key(&s->kp, i, &off, &len);
        struct wave_ent *e;

        if (len != s->key_len) {
            return -DER_IO; /* corrupt index key */
        }
        e = wave_add(&s->w, 0);
        if (e == NULL) {
            return -DER_INVAL;
        }
        daos_key_bytes(&e->dkey, key, len);
        daos_key_u8(&e->akey, s->row_akey);
    }
    rc = wave_run(t, &s->w, oh_row, s->row_tag);
    if (rc != 0) {
        return rc;
    }
    s->more = keypage_next(&s->kp);
    return 0;
}

/* -----------------------------------------------------------------------
 * Layout state
 * ----------------------------------------------------------------------- */

/* Working state of the layout slots that read or write whole rows.  The
 * DS list of a row can carry FDB_LAYOUT_DS_MAX ids, so the struct is
 * heap allocated once per call (never on a worker stack). */
struct layout_ctx {
    struct daos_backend  *b;
    uint8_t               other[NFS4_OTHER_SIZE];
    struct fdb_layout_hdr want;         /**< The grant being written. */
    const uint32_t       *want_ds;      /**< Caller's DS list (borrowed). */
    uint64_t              match_fileid; /**< layout_return: 0 = any file. */
    struct fdb_layout_hdr row;          /**< The stored row, once read. */
    uint32_t              row_ds[FDB_LAYOUT_DS_MAX];
    uint32_t              uniq[FDB_LAYOUT_DS_MAX]; /**< Sorted unique DS list scratch. */
    uint8_t               enc[FDB_LAYOUT_ENC_MAX];
};

static int u32_cmp(const void *a, const void *b)
{
    uint32_t x = *(const uint32_t *)a;
    uint32_t y = *(const uint32_t *)b;

    return (x > y) - (x < y);
}

/* Sorted copy of @p ids without duplicates in c->uniq; returns its
 * length.  One DS index key is written (or punched) once per
 * transaction, whatever the caller's list repeats. */
static uint32_t ds_ids_unique(struct layout_ctx *c, const uint32_t *ids, uint32_t n)
{
    uint32_t m = 0;
    uint32_t i;

    if (n == 0 || ids == NULL) {
        return 0;
    }
    if (n > FDB_LAYOUT_DS_MAX) {
        n = FDB_LAYOUT_DS_MAX;
    }
    memcpy(c->uniq, ids, (size_t)n * sizeof(*ids));
    qsort(c->uniq, n, sizeof(*c->uniq), u32_cmp);
    for (i = 0; i < n; i++) {
        if (m == 0 || c->uniq[m - 1] != c->uniq[i]) {
            c->uniq[m] = c->uniq[i];
            m++;
        }
    }
    return m;
}

/* The INODE 'L' marker of (fileid, stateid). */
static int layout_file_marker(struct daos_txn *t, const struct layout_ctx *c, uint64_t fileid,
                              bool set)
{
    struct daos_key dk;
    struct daos_key ak;

    daos_dkey_inode(&dk, DAOS_INODE_DKEY_LAYOUTS);
    daos_akey_stateid(&ak, c->other);
    return set ? idx_set(t, DAOS_TAG_INODE, fileid, &dk, &ak)
               : idx_clear(t, DAOS_TAG_INODE, fileid, &dk, &ak);
}

/* The CLIENT 'L' marker of (clientid, stateid). */
static int layout_client_marker(struct daos_txn *t, const struct layout_ctx *c,
                                uint64_t clientid, bool set)
{
    struct daos_key dk;
    struct daos_key ak;

    daos_dkey_client(&dk, DAOS_CLIENT_DKEY_LAYOUTS);
    daos_akey_stateid(&ak, c->other);
    return set ? idx_set(t, DAOS_TAG_CLIENT, clientid, &dk, &ak)
               : idx_clear(t, DAOS_TAG_CLIENT, clientid, &dk, &ak);
}

/* The DS_LAYOUT_IDX rows of @p h: one per unique id of @p ds_ids. */
static int layout_ds_rows(struct daos_txn *t, struct layout_ctx *c, const struct fdb_layout_hdr *h,
                          const uint32_t *ds_ids, bool set)
{
    struct daos_key dk;
    struct daos_key ak;
    uint32_t n = ds_ids_unique(c, ds_ids, h->ds_count);
    uint32_t i;

    daos_dkey_ds_layout_idx(&dk, h->clientid, h->fileid, c->other);
    daos_akey_marker(&ak);
    for (i = 0; i < n; i++) {
        int rc = set ? idx_set(t, DAOS_TAG_DS_LAYOUT_IDX, c->uniq[i], &dk, &ak)
                     : dkey_clear(t, DAOS_TAG_DS_LAYOUT_IDX, c->uniq[i], &dk);

        if (rc != 0) {
            return rc;
        }
    }
    return 0;
}

/* Write the row and every index key of @p h (blind). */
static int layout_write(struct daos_txn *t, struct layout_ctx *c, const struct fdb_layout_hdr *h,
                        const uint32_t *ds_ids)
{
    struct daos_key dk;
    struct daos_key ak;
    daos_handle_t oh;
    size_t len = 0;
    int rc;

    if (!fdb_layout_encode(h, ds_ids, c->enc, sizeof(c->enc), &len)) {
        return -DER_INVAL;
    }
    rc = daos_txn_obj(t, DAOS_TAG_LAYOUT_STATE, 0, &oh);
    if (rc != 0) {
        return rc;
    }
    daos_dkey_stateid(&dk, c->other);
    daos_akey_value(&ak);
    rc = daos_update_single(t, oh, &dk, &ak, c->enc, len, 0, 0);
    if (rc == 0) {
        rc = layout_file_marker(t, c, h->fileid, true);
    }
    if (rc == 0) {
        rc = layout_client_marker(t, c, h->clientid, true);
    }
    if (rc == 0) {
        rc = layout_ds_rows(t, c, h, ds_ids, true);
    }
    return rc;
}

/* Punch the LAYOUT_STATE row of c->other. */
static int layout_clear_row(struct daos_txn *t, const struct layout_ctx *c)
{
    struct daos_key dk;
    daos_handle_t oh;
    int rc = daos_txn_obj(t, DAOS_TAG_LAYOUT_STATE, 0, &oh);

    if (rc != 0) {
        return rc;
    }
    daos_dkey_stateid(&dk, c->other);
    return daos_punch_dkeys(t, oh, &dk, 1, 0);
}

/* Punch the stored row c->row with its INODE 'L' marker and DS rows; the
 * CLIENT 'L' marker too unless the caller punches it itself
 * (layout_del_all_for_client punches the scanned key exactly once). */
static int layout_clear_stored(struct daos_txn *t, struct layout_ctx *c, bool client_marker)
{
    int rc = layout_clear_row(t, c);

    if (rc == 0) {
        rc = layout_file_marker(t, c, c->row.fileid, false);
    }
    if (rc == 0 && client_marker) {
        rc = layout_client_marker(t, c, c->row.clientid, false);
    }
    if (rc == 0) {
        rc = layout_ds_rows(t, c, &c->row, c->row_ds, false);
    }
    return rc;
}

/* Read the stored row into c->row / c->row_ds; *found false when absent. */
static int layout_read_row(struct daos_txn *t, struct layout_ctx *c, bool *found)
{
    struct daos_key dk;
    struct daos_key ak;
    daos_handle_t oh;
    size_t len = 0;
    int rc;

    *found = false;
    rc = daos_txn_obj(t, DAOS_TAG_LAYOUT_STATE, 0, &oh);
    if (rc != 0) {
        return rc;
    }
    daos_dkey_stateid(&dk, c->other);
    daos_akey_value(&ak);
    rc = row_fetch(t, oh, &dk, &ak, c->enc, sizeof(c->enc), &len, found);
    if (rc != 0) {
        return rc;
    }
    if (*found && !fdb_layout_decode(c->enc, len, &c->row, c->row_ds, FDB_LAYOUT_DS_MAX)) {
        return -DER_IO; /* corrupt row */
    }
    return 0;
}

static int layout_grant_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct layout_ctx *c = arg;
    int rc = layout_write(t, c, &c->want, c->want_ds);

    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

/* The stateid was bound to another file: the old file's marker and DS
 * rows go; the CLIENT marker only when the client changes, since the
 * grant rewrites it otherwise (one write per key per transaction). */
static int layout_rebind_clear(struct daos_txn *t, struct layout_ctx *c)
{
    int rc = layout_file_marker(t, c, c->row.fileid, false);

    if (rc == 0 && c->row.clientid != c->want.clientid) {
        rc = layout_client_marker(t, c, c->row.clientid, false);
    }
    if (rc == 0) {
        rc = layout_ds_rows(t, c, &c->row, c->row_ds, false);
    }
    return rc;
}

/* Renewal union (C3: read, decide and write in one transaction).  The
 * present row keeps a SUPERSET of every range granted under the
 * stateid, a monotonic seqid, an RW-dominant iomode, the newest
 * clientid and the DS list of the first grant (memdb / RonDB parity). */
static int layout_union_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct layout_ctx *c = arg;
    struct fdb_layout_hdr merged;
    bool found = false;
    int rc;

    rc = layout_read_row(t, c, &found);
    if (rc != 0) {
        return rc;
    }
    if (!found || c->row.fileid != c->want.fileid) {
        if (found) {
            rc = layout_rebind_clear(t, c);
            if (rc != 0) {
                return rc;
            }
        }
        rc = layout_write(t, c, &c->want, c->want_ds);
        if (rc != 0) {
            return rc;
        }
        *st_out = MDS_OK;
        return DAOS_BODY_COMMIT;
    }
    merged = c->row;
    layout_range_union_saturating(c->row.offset, c->row.length, c->want.offset,
                                  c->want.length, &merged.offset, &merged.length);
    if (c->want.seqid > merged.seqid) {
        merged.seqid = c->want.seqid;
    }
    merged.iomode = (c->row.iomode == DAOS_LAYOUTIOMODE_RW ||
                     c->want.iomode == DAOS_LAYOUTIOMODE_RW) ? DAOS_LAYOUTIOMODE_RW
                                                             : c->want.iomode;
    merged.clientid = c->want.clientid;
    if (merged.clientid != c->row.clientid) {
        /* The client-keyed entries move: the old client's marker and DS
         * rows (keyed by the old clientid) are punched, the new ones are
         * written below. */
        rc = layout_client_marker(t, c, c->row.clientid, false);
        if (rc == 0) {
            rc = layout_ds_rows(t, c, &c->row, c->row_ds, false);
        }
        if (rc != 0) {
            return rc;
        }
    }
    rc = layout_write(t, c, &merged, c->row_ds);
    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

static int layout_return_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct layout_ctx *c = arg;
    bool found = false;
    int rc;

    rc = layout_read_row(t, c, &found);
    if (rc != 0) {
        return rc;
    }
    if (!found || (c->match_fileid != 0 && c->row.fileid != c->match_fileid)) {
        *st_out = MDS_ERR_NOTFOUND;
        return DAOS_BODY_DONE;
    }
    rc = layout_clear_stored(t, c, true);
    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

/* Argument checks shared by the two grant slots; fills c->want. */
static enum mds_status layout_ctx_prepare(struct layout_ctx *c, struct daos_backend *b,
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
                                        daos_txn_body body, const char *op)
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
        st = daos_run_txn(c->b, DAOS_TXN_MUTATING, op, body, c);
    }
    free(c);
    return st;
}

static enum mds_status daos_layout_grant(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                         uint64_t clientid, uint64_t fileid, uint32_t iomode,
                                         uint64_t offset, uint64_t length,
                                         const struct nfs4_stateid *stateid,
                                         const uint32_t *ds_ids, uint32_t ds_count)
{
    (void)txn;
    return layout_grant_run(cat, clientid, fileid, iomode, offset, length, stateid, ds_ids,
                            ds_count, layout_grant_body, "layout_grant");
}

static enum mds_status daos_layout_grant_union(struct mds_catalogue *cat,
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

static enum mds_status daos_layout_return(struct mds_catalogue *cat, struct mds_cat_txn *txn,
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
    st = daos_run_txn(c->b, DAOS_TXN_MUTATING, "layout_return", layout_return_body, c);
    free(c);
    return st;
}

/* --- layout_get_by_stateid --------------------------------------------- */

/* Heap allocated once per call (the row buffer). */
struct layout_get_ctx {
    struct daos_backend  *b;
    const uint8_t        *other;
    struct fdb_layout_hdr hdr;
    uint8_t               enc[FDB_LAYOUT_ENC_MAX];
};

static int layout_get_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct layout_get_ctx *c = arg;
    struct daos_key dk;
    struct daos_key ak;
    daos_handle_t oh;
    size_t len = 0;
    bool found = false;
    int rc;

    rc = daos_txn_obj(t, DAOS_TAG_LAYOUT_STATE, 0, &oh);
    if (rc != 0) {
        return rc;
    }
    daos_dkey_stateid(&dk, c->other);
    daos_akey_value(&ak);
    rc = row_fetch(t, oh, &dk, &ak, c->enc, sizeof(c->enc), &len, &found);
    if (rc != 0) {
        return rc;
    }
    if (!found) {
        *st_out = MDS_ERR_NOTFOUND;
        return DAOS_BODY_DONE;
    }
    if (!fdb_layout_decode(c->enc, len, &c->hdr, NULL, 0)) {
        return -DER_IO;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

static enum mds_status daos_layout_get_by_stateid(struct mds_catalogue *cat,
                                                  const uint8_t stateid_other[12],
                                                  uint64_t *clientid, uint64_t *fileid,
                                                  uint32_t *iomode, uint64_t *offset,
                                                  uint64_t *length, uint32_t *seqid)
{
    struct layout_get_ctx *c;
    enum mds_status st;

    if (be_of(cat) == NULL || stateid_other == NULL) {
        return MDS_ERR_INVAL;
    }
    c = calloc(1, sizeof(*c));
    if (c == NULL) {
        return MDS_ERR_NOMEM;
    }
    c->b = be_of(cat);
    c->other = stateid_other;
    st = daos_run_txn(c->b, DAOS_TXN_NONE, "layout_get_by_stateid", layout_get_body, c);
    if (st == MDS_OK) {
        if (clientid != NULL) {
            *clientid = c->hdr.clientid;
        }
        if (fileid != NULL) {
            *fileid = c->hdr.fileid;
        }
        if (iomode != NULL) {
            *iomode = c->hdr.iomode;
        }
        if (offset != NULL) {
            *offset = c->hdr.offset;
        }
        if (length != NULL) {
            *length = c->hdr.length;
        }
        if (seqid != NULL) {
            *seqid = c->hdr.seqid;
        }
    }
    free(c);
    return st;
}

/* --- layout_scan_for_file ---------------------------------------------- */

struct layout_scan_ctx {
    struct daos_backend *b;
    uint64_t             fileid;
    bool                 has_layout;
};

/* A one-key listing of the file's 'L' index: the index is trusted
 * without the row read (file comment). */
static int layout_scan_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct layout_scan_ctx *c = arg;
    daos_key_desc_t kds[1];
    uint8_t buf[DAOS_KEY_MAX];
    struct daos_list l;
    struct daos_key dk;
    daos_handle_t oh;
    int rc;

    rc = daos_txn_obj(t, DAOS_TAG_INODE, c->fileid, &oh);
    if (rc != 0) {
        return rc;
    }
    daos_dkey_inode(&dk, DAOS_INODE_DKEY_LAYOUTS);
    daos_list_init(&l, kds, 1, buf, sizeof(buf));
    rc = daos_list_akeys(t, oh, &dk, NULL, &l);
    if (rc != 0) {
        return rc;
    }
    c->has_layout = (l.nr > 0);
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

static enum mds_status daos_layout_scan_for_file(struct mds_catalogue *cat, uint64_t fileid,
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
    st = daos_run_txn(c.b, DAOS_TXN_NONE, "layout_scan_for_file", layout_scan_body, &c);
    if (st == MDS_OK) {
        *has_layout = c.has_layout;
    }
    return st;
}

/* --- layout_del_all_for_client ----------------------------------------- */

struct layout_del_ctx {
    struct layout_ctx *lc;       /**< Row decode scratch (heap). */
    uint64_t           clientid;
    struct keypage     kp;       /**< CLIENT 'L' akeys of one batch. */
    struct wave        w;        /**< Their LAYOUT_STATE rows. */
    bool               more;
};

/* Build the row wave of the listed CLIENT 'L' page. */
static int layout_del_collect(struct layout_del_ctx *c)
{
    size_t off = 0;
    uint32_t i;

    wave_reset(&c->w);
    for (i = 0; i < c->kp.l.nr; i++) {
        size_t len;
        const uint8_t *key = keypage_key(&c->kp, i, &off, &len);
        struct wave_ent *e;

        if (len != NFS4_OTHER_SIZE) {
            return -DER_IO; /* corrupt index key */
        }
        e = wave_add(&c->w, 0);
        if (e == NULL) {
            return -DER_INVAL;
        }
        daos_key_bytes(&e->dkey, key, len);
        daos_akey_value(&e->akey);
    }
    return 0;
}

/* One batch: the first DAOS_LAYOUT_PAGE by-client index keys, their rows
 * in one parallel wave, then every punch.  Re-runnable: each attempt
 * re-reads the index from the start. */
static int layout_del_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct layout_del_ctx *c = arg;
    struct daos_key dk;
    struct daos_key ak;
    daos_handle_t oh_c;
    daos_handle_t oh_l;
    size_t off = 0;
    uint32_t i;
    int rc;

    c->more = false;
    keypage_rewind(&c->kp);
    rc = daos_txn_obj(t, DAOS_TAG_CLIENT, c->clientid, &oh_c);
    if (rc == 0) {
        rc = daos_txn_obj(t, DAOS_TAG_LAYOUT_STATE, 0, &oh_l);
    }
    if (rc != 0) {
        return rc;
    }
    daos_dkey_client(&dk, DAOS_CLIENT_DKEY_LAYOUTS);
    rc = keypage_list(t, oh_c, &dk, &c->kp, DAOS_LAYOUT_PAGE);
    if (rc == 0) {
        rc = layout_del_collect(c);
    }
    if (rc == 0) {
        rc = wave_run(t, &c->w, oh_l, DAOS_TAG_LAYOUT_STATE);
    }
    if (rc != 0) {
        return rc;
    }
    c->more = !c->kp.l.eof;
    for (i = 0; i < c->kp.l.nr; i++) {
        size_t len;
        const uint8_t *other = keypage_key(&c->kp, i, &off, &len);
        size_t vlen = wave_len(&c->w, i);

        memcpy(c->lc->other, other, NFS4_OTHER_SIZE);
        if (vlen != 0) {
            if (!fdb_layout_decode(wave_val(&c->w, i), vlen, &c->lc->row, c->lc->row_ds,
                                   FDB_LAYOUT_DS_MAX)) {
                return -DER_IO;
            }
            if (c->lc->row.clientid == c->clientid) {
                rc = layout_clear_stored(t, c->lc, false);
                if (rc != 0) {
                    return rc;
                }
            }
            /* A row naming another client is that client's live
             * layout: only the orphan key below goes. */
        }
        /* The scanned key itself, also when its row is gone or names
         * another client (an orphan). */
        daos_akey_stateid(&ak, other);
        rc = daos_marker_clear(t, oh_c, &dk, &ak);
        if (rc != 0) {
            return rc;
        }
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

static enum mds_status daos_layout_del_all_for_client(struct mds_catalogue *cat,
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
    if (c->lc == NULL || keypage_alloc(&c->kp, DAOS_LAYOUT_PAGE) != MDS_OK ||
        wave_alloc(&c->w, DAOS_LAYOUT_PAGE, FDB_LAYOUT_ENC_MAX) != MDS_OK) {
        st = MDS_ERR_NOMEM;
        goto out;
    }
    c->lc->b = be_of(cat);
    c->clientid = clientid;
    do {
        c->more = false;
        st = daos_run_txn(c->lc->b, DAOS_TXN_MUTATING, "layout_del_all_for_client",
                          layout_del_body, c);
        batches++;
    } while (st == MDS_OK && c->more && batches < DAOS_COORD_DEL_MAX_BATCHES);
    if (st == MDS_OK && c->more) {
        MDS_LOG_ERROR(LOG_COMP_CAT, "daos layout_del_all_for_client: client %llu still has "
                      "layout rows after %u batches", (unsigned long long)clientid, batches);
        st = MDS_ERR_DELAY;
    }
out:
    keypage_free(&c->kp);
    wave_free(&c->w);
    free(c->lc);
    free(c);
    return st;
}

/* --- ds_layout_idx_scan ------------------------------------------------ */

struct ds_pair {
    uint64_t clientid;
    uint64_t fileid;
};

struct ds_seen {
    uint64_t clientid;
    uint64_t fileid;
    bool     used;
};

/* Heap allocated once per call (the page arrays and the delivered set). */
struct ds_idx_ctx {
    struct daos_backend *b;
    uint32_t             ds_id;
    struct keypage       kp;      /**< DS_LAYOUT_IDX dkeys of one page. */
    struct wave          w;       /**< Their LAYOUT_STATE rows. */
    uint32_t             n;
    struct ds_pair       page[DAOS_LAYOUT_PAGE];
    bool                 more;
    struct ds_seen      *seen;    /**< Delivered pairs; DAOS_DS_IDX_SEEN_SLOTS. */
    uint32_t             seen_n;
};

static uint64_t ds_pair_hash(uint64_t clientid, uint64_t fileid)
{
    uint64_t x = clientid ^ (fileid * 0x9E3779B97F4A7C15ULL);

    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33;
    return x;
}

/* Record (clientid, fileid) as delivered; false when it already was.
 * *full reports a set at capacity (nothing recorded). */
static bool ds_seen_insert(struct ds_idx_ctx *c, uint64_t clientid, uint64_t fileid, bool *full)
{
    uint64_t h = ds_pair_hash(clientid, fileid);
    uint32_t i;

    *full = false;
    for (i = 0; i < DAOS_DS_IDX_SEEN_SLOTS; i++) {
        struct ds_seen *s = &c->seen[(h + i) & (DAOS_DS_IDX_SEEN_SLOTS - 1U)];

        if (!s->used) {
            if (c->seen_n >= DAOS_DS_IDX_PAIRS_MAX) {
                *full = true;
                return false;
            }
            s->used = true;
            s->clientid = clientid;
            s->fileid = fileid;
            c->seen_n++;
            return true;
        }
        if (s->clientid == clientid && s->fileid == fileid) {
            return false;
        }
    }
    *full = true; /* unreachable while seen_n < PAIRS_MAX < SLOTS */
    return false;
}

/* Build the row wave of the listed index page: each dkey names the
 * stateid of its row. */
static int ds_idx_collect(struct ds_idx_ctx *c)
{
    size_t off = 0;
    uint32_t i;

    wave_reset(&c->w);
    for (i = 0; i < c->kp.l.nr; i++) {
        size_t len;
        const uint8_t *key = keypage_key(&c->kp, i, &off, &len);
        struct wave_ent *e;

        if (len != DAOS_DS_IDX_DKEY_LEN) {
            return -DER_IO; /* corrupt index key */
        }
        e = wave_add(&c->w, 0);
        if (e == NULL) {
            return -DER_INVAL;
        }
        daos_dkey_stateid(&e->dkey, key + 2U * DAOS_KEY_BE64_LEN);
        daos_akey_value(&e->akey);
    }
    return 0;
}

/* Every (clientid, fileid) candidate of one page whose row still names
 * the pair and @p ds_id; the delivered-once filter runs at delivery
 * time, outside the transaction, so a re-run of this body cannot drop a
 * pair. */
static int ds_idx_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct ds_idx_ctx *c = arg;
    daos_handle_t oh_i;
    daos_handle_t oh_l;
    size_t off = 0;
    uint32_t i;
    int rc;

    c->n = 0;
    c->more = false;
    rc = daos_txn_obj(t, DAOS_TAG_DS_LAYOUT_IDX, c->ds_id, &oh_i);
    if (rc == 0) {
        rc = daos_txn_obj(t, DAOS_TAG_LAYOUT_STATE, 0, &oh_l);
    }
    if (rc == 0) {
        rc = keypage_list(t, oh_i, NULL, &c->kp, DAOS_LAYOUT_PAGE);
    }
    if (rc == 0) {
        rc = ds_idx_collect(c);
    }
    if (rc == 0) {
        rc = wave_run(t, &c->w, oh_l, DAOS_TAG_LAYOUT_STATE);
    }
    if (rc != 0) {
        return rc;
    }
    for (i = 0; i < c->kp.l.nr; i++) {
        size_t len;
        const uint8_t *key = keypage_key(&c->kp, i, &off, &len);
        size_t vlen = wave_len(&c->w, i);
        struct ds_pair cand;
        struct fdb_layout_hdr hdr;

        cand.clientid = fdb_get_u64(key);
        cand.fileid = fdb_get_u64(key + DAOS_KEY_BE64_LEN);
        if (vlen != 0 && fdb_layout_decode(wave_val(&c->w, i), vlen, &hdr, NULL, 0) &&
            hdr.clientid == cand.clientid && hdr.fileid == cand.fileid &&
            fdb_layout_ds_contains(wave_val(&c->w, i), vlen, c->ds_id)) {
            c->page[c->n] = cand;
            c->n++;
        }
    }
    c->more = keypage_next(&c->kp);
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

static enum mds_status daos_ds_layout_idx_scan(struct mds_catalogue *cat, uint32_t ds_id,
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
    c->seen = calloc(DAOS_DS_IDX_SEEN_SLOTS, sizeof(*c->seen));
    if (c->seen == NULL || keypage_alloc(&c->kp, DAOS_LAYOUT_PAGE) != MDS_OK ||
        wave_alloc(&c->w, DAOS_LAYOUT_PAGE, FDB_LAYOUT_ENC_MAX) != MDS_OK) {
        st = MDS_ERR_NOMEM;
        goto out;
    }
    c->b = be_of(cat);
    c->ds_id = ds_id;
    for (;;) {
        uint32_t i;

        st = daos_run_txn(c->b, DAOS_TXN_READONLY, "ds_layout_idx_scan", ds_idx_body, c);
        if (st != MDS_OK) {
            break;
        }
        for (i = 0; i < c->n; i++) {
            bool full = false;

            if (!ds_seen_insert(c, c->page[i].clientid, c->page[i].fileid, &full)) {
                if (full) {
                    MDS_LOG_ERROR(LOG_COMP_CAT, "daos ds_layout_idx_scan: DS %u names more "
                                  "than %u (client, file) pairs; refusing to repeat one",
                                  (unsigned)ds_id, DAOS_DS_IDX_PAIRS_MAX);
                    st = MDS_ERR_NOSPC;
                    goto out;
                }
                continue; /* another stateid of a delivered pair */
            }
            if (cb(c->page[i].clientid, c->page[i].fileid, ctx) != 0) {
                goto out;
            }
        }
        if (!c->more) {
            break;
        }
    }
out:
    keypage_free(&c->kp);
    wave_free(&c->w);
    free(c->seen);
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
    struct daos_backend *b;
    uint64_t             fileid;
    struct keypage       kp;      /**< INODE 'L' akeys of one page. */
    struct wave          w;       /**< Their LAYOUT_STATE rows. */
    uint32_t             n;
    struct holder        page[DAOS_LAYOUT_PAGE];
    bool                 more;
};

static int iter_file_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct iter_file_ctx *c = arg;
    struct daos_key dk;
    daos_handle_t oh_f;
    daos_handle_t oh_l;
    size_t off = 0;
    uint32_t i;
    int rc;

    c->n = 0;
    c->more = false;
    rc = daos_txn_obj(t, DAOS_TAG_INODE, c->fileid, &oh_f);
    if (rc == 0) {
        rc = daos_txn_obj(t, DAOS_TAG_LAYOUT_STATE, 0, &oh_l);
    }
    if (rc != 0) {
        return rc;
    }
    daos_dkey_inode(&dk, DAOS_INODE_DKEY_LAYOUTS);
    rc = keypage_list(t, oh_f, &dk, &c->kp, DAOS_LAYOUT_PAGE);
    if (rc != 0) {
        return rc;
    }
    wave_reset(&c->w);
    for (i = 0; i < c->kp.l.nr; i++) {
        size_t len;
        const uint8_t *key = keypage_key(&c->kp, i, &off, &len);
        struct wave_ent *e;

        if (len != NFS4_OTHER_SIZE) {
            return -DER_IO;
        }
        e = wave_add(&c->w, 0);
        if (e == NULL) {
            return -DER_INVAL;
        }
        daos_key_bytes(&e->dkey, key, len);
        daos_akey_value(&e->akey);
    }
    rc = wave_run(t, &c->w, oh_l, DAOS_TAG_LAYOUT_STATE);
    if (rc != 0) {
        return rc;
    }
    off = 0;
    for (i = 0; i < c->kp.l.nr; i++) {
        size_t len;
        const uint8_t *other = keypage_key(&c->kp, i, &off, &len);
        size_t vlen = wave_len(&c->w, i);
        struct fdb_layout_hdr hdr;

        if (vlen != 0 && fdb_layout_decode(wave_val(&c->w, i), vlen, &hdr, NULL, 0) &&
            hdr.fileid == c->fileid) {
            struct holder *h = &c->page[c->n];

            memcpy(h->sid.other, other, NFS4_OTHER_SIZE);
            h->sid.seqid = hdr.seqid;
            h->clientid = hdr.clientid;
            h->iomode = hdr.iomode;
            c->n++;
        }
    }
    c->more = keypage_next(&c->kp);
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

static enum mds_status daos_layout_iter_file(struct mds_catalogue *cat, uint64_t fileid,
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
    if (keypage_alloc(&c->kp, DAOS_LAYOUT_PAGE) != MDS_OK ||
        wave_alloc(&c->w, DAOS_LAYOUT_PAGE, FDB_LAYOUT_ENC_MAX) != MDS_OK) {
        st = MDS_ERR_NOMEM;
        goto out;
    }
    c->b = be_of(cat);
    c->fileid = fileid;
    for (;;) {
        uint32_t i;

        st = daos_run_txn(c->b, DAOS_TXN_READONLY, "layout_iter_file", iter_file_body, c);
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
    keypage_free(&c->kp);
    wave_free(&c->w);
    free(c);
    return st;
}

/* --- layoutget_fused --------------------------------------------------- */

struct fused_ctx {
    struct layout_ctx       *lc;       /**< Grant to persist (heap). */
    uint64_t                 fileid;
    uint32_t                 sc;
    uint32_t                 su;
    uint32_t                 mc;
    struct mds_ds_map_entry *entries;  /**< Heap (decoder); the caller's on success. */
    uint8_t                 *map;      /**< First-tier read buffer (heap). */
};

/* Read and decode the stripe-map blob of c->fileid: one fetch into the
 * first-tier buffer and, for a map wider than it, a second fetch into a
 * buffer of the size the client reported.  *found false when the file
 * has no map.  Both reads are in the one transaction, so the second
 * sees the same value as the first. */
static int fused_read_map(struct daos_txn *t, daos_handle_t oh, struct fused_ctx *c, bool *found)
{
    struct daos_key dk;
    struct daos_key ak;
    uint8_t *big = NULL;
    const uint8_t *buf = c->map;
    size_t len = 0;
    int rc;

    daos_dkey_inode(&dk, DAOS_INODE_DKEY_STRIPE);
    daos_akey_stripe_map(&ak);
    rc = daos_fetch_single(t, oh, &dk, &ak, c->map, DAOS_FUSED_MAP_FIRST, &len, found);
    if (rc == -DER_REC2BIG) {
        size_t need = (len != 0) ? len : (size_t)DAOS_STRIPE_MAP_ENC_MAX;

        if (need > DAOS_STRIPE_MAP_ENC_MAX) {
            return -DER_IO; /* wider than any encodable map: corrupt */
        }
        big = malloc(need);
        if (big == NULL) {
            return -DER_NOMEM;
        }
        rc = daos_fetch_single(t, oh, &dk, &ak, big, need, &len, found);
        if (rc == -DER_REC2BIG) {
            rc = -DER_IO; /* the snapshot is fixed: a second overflow is corrupt */
        }
        buf = big;
    }
    if (rc == 0 && *found &&
        !daos_stripe_map_decode(buf, len, &c->sc, &c->su, &c->mc, &c->entries)) {
        rc = -DER_IO; /* corrupt map (or no memory for the entries) */
    }
    free(big);
    return rc;
}

static int fused_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct fused_ctx *c = arg;
    struct layout_ctx *lc = c->lc;
    struct layout_ds_id_list ids;
    daos_handle_t oh;
    bool found = false;
    int rc;

    free(c->entries); /* a previous attempt's decode */
    c->entries = NULL;
    rc = daos_txn_obj(t, DAOS_TAG_INODE, c->fileid, &oh);
    if (rc != 0) {
        return rc;
    }
    rc = fused_read_map(t, oh, c, &found);
    if (rc != 0) {
        return rc;
    }
    if (!found) {
        *st_out = MDS_ERR_NOTFOUND; /* no stripe map: nothing granted */
        return DAOS_BODY_DONE;
    }
    if (layout_ds_id_list_from_entries(&ids, c->entries, c->sc * c->mc) != MDS_OK) {
        *st_out = MDS_ERR_INVAL; /* a phantom stripe: the caller's fallback decides */
        return DAOS_BODY_DONE;
    }
    lc->want.ds_count = ids.count;
    rc = layout_write(t, lc, &lc->want, ids.ids);
    layout_ds_id_list_destroy(&ids);
    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

static enum mds_status daos_layoutget_fused(struct mds_catalogue *cat, uint64_t fileid,
                                            uint32_t *stripe_count, uint32_t *stripe_unit,
                                            uint32_t *mirror_count,
                                            struct mds_ds_map_entry **entries,
                                            const struct nfs4_stateid *stateid,
                                            uint64_t clientid, uint32_t iomode,
                                            uint64_t offset, uint64_t length, uint32_t mds_id)
{
    struct fused_ctx c;
    enum mds_status st = MDS_ERR_NOMEM;

    if (entries != NULL) {
        *entries = NULL;
    }
    if (be_of(cat) == NULL || stateid == NULL || stripe_count == NULL || stripe_unit == NULL ||
        mirror_count == NULL || entries == NULL) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.lc = calloc(1, sizeof(*c.lc));
    c.map = malloc(DAOS_FUSED_MAP_FIRST);
    c.fileid = fileid;
    if (c.lc != NULL && c.map != NULL) {
        st = layout_ctx_prepare(c.lc, be_of(cat), clientid, fileid, iomode, offset, length,
                                stateid, NULL, 0);
    }
    if (st == MDS_OK) {
        c.lc->want.grant_owner_mds_id = mds_id;
        st = daos_run_txn(c.lc->b, DAOS_TXN_MUTATING, "layoutget_fused", fused_body, &c);
    }
    if (st == MDS_OK) {
        *stripe_count = c.sc;
        *stripe_unit = c.su;
        *mirror_count = c.mc;
        *entries = c.entries; /* caller frees */
    } else {
        free(c.entries);
    }
    free(c.map);
    free(c.lc);
    return st;
}

/* -----------------------------------------------------------------------
 * Client recovery
 * ----------------------------------------------------------------------- */

/* Heap allocated once per call (the row holds a 1 KB owner id). */
struct recovery_ctx {
    struct daos_backend    *b;
    uint64_t                clientid;
    struct fdb_recovery_val val;   /**< put: the row to write; get: the row read. */
    uint8_t                 enc[FDB_RECOVERY_ENC_MAX];
};

/* Read the CLIENT 'R' row of c->clientid into *out. */
static int recovery_read(struct daos_txn *t, struct recovery_ctx *c,
                         struct fdb_recovery_val *out, bool *found)
{
    struct daos_key dk;
    struct daos_key ak;
    daos_handle_t oh;
    size_t len = 0;
    int rc;

    *found = false;
    rc = daos_txn_obj(t, DAOS_TAG_CLIENT, c->clientid, &oh);
    if (rc != 0) {
        return rc;
    }
    daos_dkey_client(&dk, DAOS_CLIENT_DKEY_RECOVERY);
    daos_akey_value(&ak);
    rc = row_fetch(t, oh, &dk, &ak, c->enc, sizeof(c->enc), &len, found);
    if (rc != 0) {
        return rc;
    }
    if (*found && !fdb_recovery_decode(c->enc, len, out)) {
        return -DER_IO;
    }
    return 0;
}

/* Punch the RECOVERY_BY_OWNER(@p owner) entry of c->clientid. */
static int recovery_owner_clear(struct daos_txn *t, const struct recovery_ctx *c, uint32_t owner)
{
    struct daos_key dk;
    daos_handle_t oh;
    int rc = daos_txn_obj(t, DAOS_TAG_RECOVERY_BY_OWNER, owner, &oh);

    if (rc != 0) {
        return rc;
    }
    daos_dkey_be64(&dk, c->clientid);
    return daos_punch_dkeys(t, oh, &dk, 1, 0);
}

static int recovery_put_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct recovery_ctx *c = arg;
    struct fdb_recovery_val old;
    struct daos_key dk;
    struct daos_key ak;
    daos_handle_t oh;
    size_t len = 0;
    bool found = false;
    int rc;

    rc = recovery_read(t, c, &old, &found);
    if (rc != 0) {
        return rc;
    }
    if (found && old.owner_mds_id != c->val.owner_mds_id) {
        rc = recovery_owner_clear(t, c, old.owner_mds_id);
        if (rc != 0) {
            return rc;
        }
    }
    if (!fdb_recovery_encode(&c->val, c->enc, sizeof(c->enc), &len)) {
        return -DER_INVAL;
    }
    rc = daos_txn_obj(t, DAOS_TAG_CLIENT, c->clientid, &oh);
    if (rc != 0) {
        return rc;
    }
    daos_dkey_client(&dk, DAOS_CLIENT_DKEY_RECOVERY);
    daos_akey_value(&ak);
    rc = daos_update_single(t, oh, &dk, &ak, c->enc, len, 0, 0);
    if (rc != 0) {
        return rc;
    }
    rc = daos_txn_obj(t, DAOS_TAG_RECOVERY_BY_OWNER, c->val.owner_mds_id, &oh);
    if (rc != 0) {
        return rc;
    }
    daos_dkey_be64(&dk, c->clientid);
    daos_akey_marker(&ak);
    rc = daos_marker_set(t, oh, &dk, &ak);
    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

static int recovery_del_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct recovery_ctx *c = arg;
    struct daos_key dk;
    daos_handle_t oh;
    bool found = false;
    int rc;

    rc = recovery_read(t, c, &c->val, &found);
    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK; /* idempotent: a missing row is fine */
    if (!found) {
        return DAOS_BODY_DONE;
    }
    rc = daos_txn_obj(t, DAOS_TAG_CLIENT, c->clientid, &oh);
    if (rc != 0) {
        return rc;
    }
    daos_dkey_client(&dk, DAOS_CLIENT_DKEY_RECOVERY);
    rc = daos_punch_dkeys(t, oh, &dk, 1, 0);
    if (rc == 0) {
        rc = recovery_owner_clear(t, c, c->val.owner_mds_id);
    }
    if (rc != 0) {
        return rc;
    }
    return DAOS_BODY_COMMIT;
}

static int recovery_get_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct recovery_ctx *c = arg;
    bool found = false;
    int rc;

    rc = recovery_read(t, c, &c->val, &found);
    if (rc != 0) {
        return rc;
    }
    *st_out = found ? MDS_OK : MDS_ERR_NOTFOUND;
    return DAOS_BODY_COMMIT;
}

/* The owner stamped on a recovery row is this handle's identity; the
 * epoch is the registry epoch node_register recorded (0 before). */
static enum mds_status daos_recovery_put(struct mds_catalogue *cat, struct mds_cat_txn *txn,
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
    st = daos_run_txn(c->b, DAOS_TXN_MUTATING, "recovery_put", recovery_put_body, c);
    free(c);
    return st;
}

static enum mds_status daos_recovery_del(struct mds_catalogue *cat, struct mds_cat_txn *txn,
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
    st = daos_run_txn(c->b, DAOS_TXN_MUTATING, "recovery_del", recovery_del_body, c);
    free(c);
    return st;
}

static enum mds_status daos_recovery_get(struct mds_catalogue *cat, uint64_t clientid,
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
    st = daos_run_txn(c->b, DAOS_TXN_NONE, "recovery_get", recovery_get_body, c);
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
    struct daos_backend *b;
    uint32_t             owner;    /**< The RECOVERY_BY_OWNER object scanned. */
    struct keypage       kp;       /**< Its be64 clientid dkeys, one page. */
    struct wave          w;        /**< The CLIENT 'R' rows, one object each. */
    uint32_t             n;
    struct recovery_row  page[DAOS_COORD_PAGE];
    bool                 more;
};

/* One page of RECOVERY_BY_OWNER(owner): the keys give the clientids, the
 * rows (one wave over the CLIENT objects) give the epoch and confirm the
 * owner. */
static int recovery_index_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct recovery_list_ctx *c = arg;
    daos_handle_t oh;
    size_t off = 0;
    uint32_t i;
    int rc;

    c->n = 0;
    c->more = false;
    rc = daos_txn_obj(t, DAOS_TAG_RECOVERY_BY_OWNER, c->owner, &oh);
    if (rc != 0) {
        return rc;
    }
    rc = keypage_list(t, oh, NULL, &c->kp, DAOS_COORD_PAGE);
    if (rc != 0) {
        return rc;
    }
    wave_reset(&c->w);
    for (i = 0; i < c->kp.l.nr; i++) {
        size_t len;
        const uint8_t *key = keypage_key(&c->kp, i, &off, &len);
        struct wave_ent *e;

        if (len != DAOS_KEY_BE64_LEN) {
            return -DER_IO; /* corrupt index key */
        }
        e = wave_add(&c->w, fdb_get_u64(key));
        if (e == NULL) {
            return -DER_INVAL;
        }
        daos_dkey_client(&e->dkey, DAOS_CLIENT_DKEY_RECOVERY);
        daos_akey_value(&e->akey);
    }
    rc = wave_run(t, &c->w, DAOS_HDL_INVAL, DAOS_TAG_CLIENT);
    if (rc != 0) {
        return rc;
    }
    for (i = 0; i < c->w.n; i++) {
        size_t vlen = wave_len(&c->w, i);
        struct fdb_recovery_val v;

        if (vlen != 0 && fdb_recovery_decode(wave_val(&c->w, i), vlen, &v) &&
            v.owner_mds_id == c->owner) {
            c->page[c->n].clientid = c->w.ents[i].id;
            c->page[c->n].owner_mds_id = v.owner_mds_id;
            c->page[c->n].owner_boot_epoch = v.owner_boot_epoch;
            c->n++;
        }
    }
    c->more = keypage_next(&c->kp);
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

/* Run one paged scan of the owner object c->owner; *stopped when the
 * callback asked to stop. */
static enum mds_status recovery_list_scan(struct recovery_list_ctx *c, mds_recovery_list_cb cb,
                                          void *ctx, bool *stopped)
{
    keypage_rewind(&c->kp);
    for (;;) {
        enum mds_status st;
        uint32_t i;

        st = daos_run_txn(c->b, DAOS_TXN_READONLY, "recovery_list", recovery_index_body, c);
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
 * returned (memdb / RonDB filter).  There is no table of recovery rows
 * to range-read: the full listing visits the owner objects of the node
 * registry's id space, 0..MDS_MAX_NODES. */
static enum mds_status daos_recovery_list(struct mds_catalogue *cat, uint32_t owner_mds_id,
                                          mds_recovery_list_cb cb, void *ctx)
{
    struct recovery_list_ctx *c;
    enum mds_status st = MDS_OK;
    bool stopped = false;

    if (be_of(cat) == NULL || cb == NULL) {
        return MDS_ERR_INVAL;
    }
    c = calloc(1, sizeof(*c));
    if (c == NULL) {
        return MDS_ERR_NOMEM;
    }
    if (keypage_alloc(&c->kp, DAOS_COORD_PAGE) != MDS_OK ||
        wave_alloc(&c->w, DAOS_COORD_PAGE, FDB_RECOVERY_ENC_MAX) != MDS_OK) {
        st = MDS_ERR_NOMEM;
        goto out;
    }
    c->b = be_of(cat);
    if (owner_mds_id == 0) {
        uint32_t o;

        for (o = 0; o <= (uint32_t)MDS_MAX_NODES && st == MDS_OK && !stopped; o++) {
            c->owner = o;
            st = recovery_list_scan(c, cb, ctx, &stopped);
        }
    } else {
        c->owner = owner_mds_id;
        st = recovery_list_scan(c, cb, ctx, &stopped);
        if (st == MDS_OK && !stopped) {
            c->owner = 0;
            st = recovery_list_scan(c, cb, ctx, &stopped);
        }
    }
out:
    keypage_free(&c->kp);
    wave_free(&c->w);
    free(c);
    return st;
}

/* -----------------------------------------------------------------------
 * Shared 2PC journal
 * ----------------------------------------------------------------------- */

/* Heap allocated once per call (the row buffer). */
struct journal_ctx {
    struct daos_backend                   *b;
    uint64_t                               txn_id;
    uint8_t                                role;
    const struct mds_coord_journal_record *in;    /**< put */
    struct mds_coord_journal_record       *out;   /**< get */
    uint8_t                                enc[FDB_JOURNAL_ENC_MAX];
};

static int journal_put_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct journal_ctx *c = arg;
    struct daos_key dk;
    struct daos_key ak;
    daos_handle_t oh;
    size_t len = 0;
    int rc;

    if (!fdb_journal_encode(c->in, c->enc, sizeof(c->enc), &len)) {
        return -DER_INVAL;
    }
    rc = daos_txn_obj(t, DAOS_TAG_JOURNAL, 0, &oh);
    if (rc != 0) {
        return rc;
    }
    daos_dkey_journal(&dk, c->in->txn_id, c->in->role);
    daos_akey_value(&ak);
    rc = daos_update_single(t, oh, &dk, &ak, c->enc, len, 0, 0);
    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

static int journal_get_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct journal_ctx *c = arg;
    struct daos_key dk;
    struct daos_key ak;
    daos_handle_t oh;
    size_t len = 0;
    bool found = false;
    int rc;

    rc = daos_txn_obj(t, DAOS_TAG_JOURNAL, 0, &oh);
    if (rc != 0) {
        return rc;
    }
    daos_dkey_journal(&dk, c->txn_id, c->role);
    daos_akey_value(&ak);
    rc = row_fetch(t, oh, &dk, &ak, c->enc, sizeof(c->enc), &len, &found);
    if (rc != 0) {
        return rc;
    }
    if (!found) {
        *st_out = MDS_ERR_NOTFOUND;
        return DAOS_BODY_DONE;
    }
    if (!fdb_journal_decode(c->enc, len, c->txn_id, c->role, c->out)) {
        return -DER_IO;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

/* One conditional punch: the store refuses an absent row with
 * -DER_NONEXIST (at the call or at commit), which the runner reports as
 * MDS_ERR_NOTFOUND -- no read before the write. */
static int journal_del_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct journal_ctx *c = arg;
    struct daos_key dk;
    daos_handle_t oh;
    int rc;

    rc = daos_txn_obj(t, DAOS_TAG_JOURNAL, 0, &oh);
    if (rc != 0) {
        return rc;
    }
    daos_dkey_journal(&dk, c->txn_id, c->role);
    rc = daos_punch_dkeys(t, oh, &dk, 1, DAOS_COND_PUNCH);
    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

static enum mds_status daos_journal_put(struct mds_catalogue *cat, struct mds_cat_txn *txn,
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
    st = daos_run_txn(c->b, DAOS_TXN_MUTATING, "journal_put", journal_put_body, c);
    free(c);
    return st;
}

static enum mds_status daos_journal_get(struct mds_catalogue *cat, struct mds_cat_txn *txn,
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
    st = daos_run_txn(c->b, DAOS_TXN_NONE, "journal_get", journal_get_body, c);
    free(c);
    return st;
}

static enum mds_status daos_journal_del(struct mds_catalogue *cat, struct mds_cat_txn *txn,
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
    st = daos_run_txn(c->b, DAOS_TXN_MUTATING, "journal_del", journal_del_body, c);
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
    struct daos_backend             *b;
    struct keypage                   kp;        /**< JOURNAL dkeys, one page. */
    struct wave                      w;         /**< Rows of a page / a batch. */
    /* Phase 1: sort keys. */
    bool                             more;
    bool                             overflow;
    uint32_t                         n_keys;
    struct journal_key              *keys;      /**< DAOS_JOURNAL_SCAN_MAX. */
    struct mds_coord_journal_record  scratch;
    /* Phase 3: one batch of records. */
    uint32_t                         first;     /**< keys[first ..] in this batch. */
    uint32_t                         n_recs;
    bool                             present[DAOS_JOURNAL_FETCH];
    struct mds_coord_journal_record *recs;      /**< DAOS_JOURNAL_FETCH. */
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

/* One page of JOURNAL dkeys and their rows (the sort key is in the
 * value).  The keys are appended only after every store operation of
 * the attempt succeeded, so a re-run never appends a page twice. */
static int journal_keys_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct journal_scan_ctx *c = arg;
    daos_handle_t oh;
    size_t off = 0;
    uint32_t i;
    int rc;

    c->more = false;
    rc = daos_txn_obj(t, DAOS_TAG_JOURNAL, 0, &oh);
    if (rc != 0) {
        return rc;
    }
    rc = keypage_list(t, oh, NULL, &c->kp, DAOS_JOURNAL_PAGE);
    if (rc != 0) {
        return rc;
    }
    wave_reset(&c->w);
    for (i = 0; i < c->kp.l.nr; i++) {
        size_t len;
        const uint8_t *key = keypage_key(&c->kp, i, &off, &len);
        struct wave_ent *e;

        if (len != DAOS_JOURNAL_DKEY_LEN) {
            return -DER_IO;
        }
        e = wave_add(&c->w, 0);
        if (e == NULL) {
            return -DER_INVAL;
        }
        daos_key_bytes(&e->dkey, key, len);
        daos_akey_value(&e->akey);
    }
    rc = wave_run(t, &c->w, oh, DAOS_TAG_JOURNAL);
    if (rc != 0) {
        return rc;
    }
    off = 0;
    for (i = 0; i < c->kp.l.nr; i++) {
        size_t len;
        const uint8_t *key = keypage_key(&c->kp, i, &off, &len);
        size_t vlen = wave_len(&c->w, i);
        struct journal_key *jk;

        if (c->n_keys >= DAOS_JOURNAL_SCAN_MAX) {
            c->overflow = true;
            break;
        }
        jk = &c->keys[c->n_keys];
        jk->txn_id = fdb_get_u64(key);
        jk->role = key[DAOS_KEY_BE64_LEN];
        if (vlen == 0 || !fdb_journal_decode(wave_val(&c->w, i), vlen, jk->txn_id, jk->role,
                                             &c->scratch)) {
            return -DER_IO; /* a listed key without its row: corrupt */
        }
        jk->created_at_ns = c->scratch.created_at_ns;
        c->n_keys++;
    }
    if (!c->overflow) {
        c->more = keypage_next(&c->kp);
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

/* Fetch keys[first .. first + n_recs) in one wave; a record deleted since
 * phase 1 is marked absent and skipped by the caller. */
static int journal_fetch_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct journal_scan_ctx *c = arg;
    daos_handle_t oh;
    uint32_t i;
    int rc;

    rc = daos_txn_obj(t, DAOS_TAG_JOURNAL, 0, &oh);
    if (rc != 0) {
        return rc;
    }
    wave_reset(&c->w);
    for (i = 0; i < c->n_recs; i++) {
        const struct journal_key *jk = &c->keys[c->first + i];
        struct wave_ent *e = wave_add(&c->w, 0);

        c->present[i] = false;
        if (e == NULL) {
            return -DER_INVAL;
        }
        daos_dkey_journal(&e->dkey, jk->txn_id, jk->role);
        daos_akey_value(&e->akey);
    }
    rc = wave_run(t, &c->w, oh, DAOS_TAG_JOURNAL);
    if (rc != 0) {
        return rc;
    }
    for (i = 0; i < c->n_recs; i++) {
        const struct journal_key *jk = &c->keys[c->first + i];
        size_t vlen = wave_len(&c->w, i);

        if (vlen == 0) {
            continue;
        }
        if (!fdb_journal_decode(wave_val(&c->w, i), vlen, jk->txn_id, jk->role, &c->recs[i])) {
            return -DER_IO;
        }
        c->present[i] = true;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

/* Phase 1: every sort key, page by page; then the sort. */
static enum mds_status journal_scan_collect(struct journal_scan_ctx *c)
{
    enum mds_status st;

    keypage_rewind(&c->kp);
    do {
        st = daos_run_txn(c->b, DAOS_TXN_READONLY, "journal_scan", journal_keys_body, c);
    } while (st == MDS_OK && c->more);
    if (st != MDS_OK) {
        return st;
    }
    if (c->overflow) {
        MDS_LOG_ERROR(LOG_COMP_CAT, "daos journal_scan: more than %u journal records; "
                      "refusing an unordered scan", DAOS_JOURNAL_SCAN_MAX);
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
        if (c->n_recs > DAOS_JOURNAL_FETCH) {
            c->n_recs = DAOS_JOURNAL_FETCH;
        }
        st = daos_run_txn(c->b, DAOS_TXN_READONLY, "journal_scan", journal_fetch_body, c);
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

static enum mds_status daos_journal_scan(struct mds_catalogue *cat,
                                         mds_coord_journal_scan_cb cb, void *ctx)
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
    c->keys = calloc(DAOS_JOURNAL_SCAN_MAX, sizeof(*c->keys));
    c->recs = calloc(DAOS_JOURNAL_FETCH, sizeof(*c->recs));
    if (c->keys != NULL && c->recs != NULL && keypage_alloc(&c->kp, DAOS_JOURNAL_PAGE) == MDS_OK &&
        wave_alloc(&c->w, DAOS_JOURNAL_PAGE, FDB_JOURNAL_ENC_MAX) == MDS_OK) {
        st = journal_scan_collect(c);
        if (st == MDS_OK) {
            st = journal_scan_deliver(c, cb, ctx);
        }
    }
    keypage_free(&c->kp);
    wave_free(&c->w);
    free(c->keys);
    free(c->recs);
    free(c);
    return st;
}

/* -----------------------------------------------------------------------
 * Open / share state
 * ----------------------------------------------------------------------- */

struct open_ctx {
    struct daos_backend             *b;
    const uint8_t                   *other;
    const struct mds_coord_open_row *in;
    struct mds_coord_open_row        row;
    uint8_t                          enc[FDB_OPEN_ENC_MAX];
};

/* The INODE 'O' and CLIENT 'O' markers of an open row. */
static int open_markers(struct daos_txn *t, uint64_t fileid, uint64_t clientid,
                        const uint8_t other[NFS4_OTHER_SIZE], bool set)
{
    struct daos_key dk;
    struct daos_key ak;
    int rc;

    daos_akey_stateid(&ak, other);
    daos_dkey_inode(&dk, DAOS_INODE_DKEY_OPENS);
    rc = set ? idx_set(t, DAOS_TAG_INODE, fileid, &dk, &ak)
             : idx_clear(t, DAOS_TAG_INODE, fileid, &dk, &ak);
    if (rc != 0) {
        return rc;
    }
    daos_dkey_client(&dk, DAOS_CLIENT_DKEY_OPENS);
    return set ? idx_set(t, DAOS_TAG_CLIENT, clientid, &dk, &ak)
               : idx_clear(t, DAOS_TAG_CLIENT, clientid, &dk, &ak);
}

static int open_put_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct open_ctx *c = arg;
    struct daos_key dk;
    struct daos_key ak;
    daos_handle_t oh;
    size_t len = 0;
    int rc;

    if (!fdb_open_encode(c->in, c->enc, sizeof(c->enc), &len)) {
        return -DER_INVAL;
    }
    rc = daos_txn_obj(t, DAOS_TAG_OPEN, 0, &oh);
    if (rc != 0) {
        return rc;
    }
    daos_dkey_stateid(&dk, c->in->stateid_other);
    daos_akey_value(&ak);
    rc = daos_update_single(t, oh, &dk, &ak, c->enc, len, 0, 0);
    if (rc == 0) {
        rc = open_markers(t, c->in->fileid, c->in->clientid, c->in->stateid_other, true);
    }
    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

/* Read the open row of c->other into c->row. */
static int open_read(struct daos_txn *t, struct open_ctx *c, bool *found)
{
    struct daos_key dk;
    struct daos_key ak;
    daos_handle_t oh;
    size_t len = 0;
    int rc;

    *found = false;
    rc = daos_txn_obj(t, DAOS_TAG_OPEN, 0, &oh);
    if (rc != 0) {
        return rc;
    }
    daos_dkey_stateid(&dk, c->other);
    daos_akey_value(&ak);
    rc = row_fetch(t, oh, &dk, &ak, c->enc, sizeof(c->enc), &len, found);
    if (rc != 0) {
        return rc;
    }
    if (*found && !fdb_open_decode(c->enc, len, c->other, &c->row)) {
        return -DER_IO;
    }
    return 0;
}

static int open_get_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct open_ctx *c = arg;
    bool found = false;
    int rc = open_read(t, c, &found);

    if (rc != 0) {
        return rc;
    }
    *st_out = found ? MDS_OK : MDS_ERR_NOTFOUND;
    return DAOS_BODY_COMMIT;
}

static int open_del_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct open_ctx *c = arg;
    struct daos_key dk;
    daos_handle_t oh;
    bool found = false;
    int rc = open_read(t, c, &found);

    if (rc != 0) {
        return rc;
    }
    if (!found) {
        *st_out = MDS_ERR_NOTFOUND;
        return DAOS_BODY_DONE;
    }
    rc = daos_txn_obj(t, DAOS_TAG_OPEN, 0, &oh);
    if (rc != 0) {
        return rc;
    }
    daos_dkey_stateid(&dk, c->other);
    rc = daos_punch_dkeys(t, oh, &dk, 1, 0);
    if (rc == 0) {
        rc = open_markers(t, c->row.fileid, c->row.clientid, c->other, false);
    }
    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

static enum mds_status daos_open_put(struct mds_catalogue *cat,
                                     const struct mds_coord_open_row *row)
{
    struct open_ctx c;

    if (be_of(cat) == NULL || row == NULL || row->open_owner_len > sizeof(row->open_owner)) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.b = be_of(cat);
    c.in = row;
    return daos_run_txn(c.b, DAOS_TXN_MUTATING, "open_put", open_put_body, &c);
}

static enum mds_status daos_open_get(struct mds_catalogue *cat, const uint8_t stateid_other[12],
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
    st = daos_run_txn(c.b, DAOS_TXN_NONE, "open_get", open_get_body, &c);
    if (st == MDS_OK) {
        *row = c.row;
    }
    return st;
}

static enum mds_status daos_open_del(struct mds_catalogue *cat, const uint8_t stateid_other[12])
{
    struct open_ctx c;

    if (be_of(cat) == NULL || stateid_other == NULL) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.b = be_of(cat);
    c.other = stateid_other;
    return daos_run_txn(c.b, DAOS_TXN_MUTATING, "open_del", open_del_body, &c);
}

/* Heap allocated once per call (the page arrays). */
struct open_scan_ctx {
    struct idx_scan            idx;
    const uint64_t            *fileid;    /**< Filter (one of the two). */
    const uint64_t            *clientid;
    uint32_t                   n;
    struct mds_coord_open_row  page[DAOS_COORD_PAGE];
};

static int open_scan_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct open_scan_ctx *c = arg;
    size_t off = 0;
    uint32_t i;
    int rc;

    c->n = 0;
    rc = idx_scan_page(t, &c->idx, DAOS_COORD_PAGE);
    if (rc != 0) {
        return rc;
    }
    for (i = 0; i < c->idx.kp.l.nr; i++) {
        size_t len;
        const uint8_t *other = keypage_key(&c->idx.kp, i, &off, &len);
        size_t vlen = wave_len(&c->idx.w, i);
        struct mds_coord_open_row *row = &c->page[c->n];

        if (vlen != 0 && fdb_open_decode(wave_val(&c->idx.w, i), vlen, other, row) &&
            (c->fileid == NULL || row->fileid == *c->fileid) &&
            (c->clientid == NULL || row->clientid == *c->clientid)) {
            c->n++;
        }
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
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
    if (idx_scan_alloc(&c->idx, DAOS_COORD_PAGE, FDB_OPEN_ENC_MAX) != MDS_OK) {
        free(c);
        return MDS_ERR_NOMEM;
    }
    c->idx.row_tag = DAOS_TAG_OPEN;
    c->idx.row_akey = (uint8_t)'v';
    c->idx.key_len = NFS4_OTHER_SIZE;
    c->fileid = fileid;
    c->clientid = clientid;
    if (fileid != NULL) {
        c->idx.idx_tag = DAOS_TAG_INODE;
        c->idx.idx_id = *fileid;
        c->idx.idx_dkey = (uint8_t)DAOS_INODE_DKEY_OPENS;
    } else {
        c->idx.idx_tag = DAOS_TAG_CLIENT;
        c->idx.idx_id = *clientid;
        c->idx.idx_dkey = (uint8_t)DAOS_CLIENT_DKEY_OPENS;
    }
    for (;;) {
        uint32_t i;

        st = daos_run_txn(be_of(cat), DAOS_TXN_READONLY, "open_scan", open_scan_body, c);
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
    idx_scan_free(&c->idx);
    free(c);
    return st;
}

static enum mds_status daos_open_scan_file(struct mds_catalogue *cat, uint64_t fileid,
                                           mds_coord_open_scan_cb cb, void *ctx)
{
    return open_scan_run(cat, &fileid, NULL, cb, ctx);
}

static enum mds_status daos_open_scan_client(struct mds_catalogue *cat, uint64_t clientid,
                                             mds_coord_open_scan_cb cb, void *ctx)
{
    return open_scan_run(cat, NULL, &clientid, cb, ctx);
}

/* -----------------------------------------------------------------------
 * Byte-range locks
 * ----------------------------------------------------------------------- */

struct lock_ctx {
    struct daos_backend             *b;
    uint64_t                         fileid;
    uint64_t                         lock_id;
    const struct mds_coord_lock_row *in;
    struct mds_coord_lock_row        row;
    uint8_t                          enc[FDB_LOCK_ENC_MAX];
};

/* The CLIENT 'K' index entry of @p row. */
static int lock_index(struct daos_txn *t, const struct mds_coord_lock_row *row, bool set)
{
    struct daos_key dk;
    struct daos_key ak;

    daos_dkey_client(&dk, DAOS_CLIENT_DKEY_LOCKS);
    lock_idx_akey(&ak, row->owner, row->owner_len, row->fileid, row->lock_id);
    if (!daos_key_ok(&ak)) {
        return -DER_INVAL;
    }
    return set ? idx_set(t, DAOS_TAG_CLIENT, row->clientid, &dk, &ak)
               : idx_clear(t, DAOS_TAG_CLIENT, row->clientid, &dk, &ak);
}

static int lock_put_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct lock_ctx *c = arg;
    struct daos_key dk;
    struct daos_key ak;
    daos_handle_t oh;
    size_t len = 0;
    int rc;

    if (!fdb_lock_encode(c->in, c->enc, sizeof(c->enc), &len)) {
        return -DER_INVAL;
    }
    rc = daos_txn_obj(t, DAOS_TAG_INODE, c->in->fileid, &oh);
    if (rc != 0) {
        return rc;
    }
    daos_dkey_inode(&dk, DAOS_INODE_DKEY_LOCKS);
    daos_akey_lock(&ak, c->in->lock_id);
    rc = daos_update_single(t, oh, &dk, &ak, c->enc, len, 0, 0);
    if (rc == 0) {
        rc = lock_index(t, c->in, true);
    }
    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

static int lock_del_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct lock_ctx *c = arg;
    struct daos_key dk;
    struct daos_key ak;
    daos_handle_t oh;
    size_t len = 0;
    bool found = false;
    int rc;

    rc = daos_txn_obj(t, DAOS_TAG_INODE, c->fileid, &oh);
    if (rc != 0) {
        return rc;
    }
    daos_dkey_inode(&dk, DAOS_INODE_DKEY_LOCKS);
    daos_akey_lock(&ak, c->lock_id);
    rc = row_fetch(t, oh, &dk, &ak, c->enc, sizeof(c->enc), &len, &found);
    if (rc != 0) {
        return rc;
    }
    if (!found) {
        *st_out = MDS_ERR_NOTFOUND;
        return DAOS_BODY_DONE;
    }
    if (!fdb_lock_decode(c->enc, len, c->fileid, c->lock_id, &c->row)) {
        return -DER_IO;
    }
    rc = daos_punch_akeys(t, oh, &dk, &ak, 1, 0);
    if (rc == 0) {
        rc = lock_index(t, &c->row, false);
    }
    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

static enum mds_status daos_lock_put(struct mds_catalogue *cat,
                                     const struct mds_coord_lock_row *row)
{
    struct lock_ctx c;

    if (be_of(cat) == NULL || row == NULL || row->owner_len > sizeof(row->owner)) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.b = be_of(cat);
    c.in = row;
    return daos_run_txn(c.b, DAOS_TXN_MUTATING, "lock_put", lock_put_body, &c);
}

static enum mds_status daos_lock_del(struct mds_catalogue *cat, uint64_t fileid,
                                     uint64_t lock_id)
{
    struct lock_ctx c;

    if (be_of(cat) == NULL) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.b = be_of(cat);
    c.fileid = fileid;
    c.lock_id = lock_id;
    return daos_run_txn(c.b, DAOS_TXN_MUTATING, "lock_del", lock_del_body, &c);
}

/* --- one page of a file's lock rows -------------------------------------- */

/* The INODE 'K' akeys of one page and their rows, read in ONE fetch:
 * one dkey, many akeys, the listed keys referenced in place. */
struct lock_page {
    struct keypage    kp;
    daos_iod_t       *iods;
    d_sg_list_t      *sgls;
    d_iov_t          *iovs;
    uint8_t          *vals;    /**< cap x FDB_LOCK_ENC_MAX. */
    struct daos_fetch f;
    uint32_t          cap;
};

static void lock_page_free(struct lock_page *p)
{
    keypage_free(&p->kp);
    free(p->iods);
    free(p->sgls);
    free(p->iovs);
    free(p->vals);
    p->iods = NULL;
    p->sgls = NULL;
    p->iovs = NULL;
    p->vals = NULL;
}

static enum mds_status lock_page_alloc(struct lock_page *p, uint32_t cap)
{
    memset(p, 0, sizeof(*p));
    if (keypage_alloc(&p->kp, cap) != MDS_OK) {
        return MDS_ERR_NOMEM;
    }
    p->iods = calloc(cap, sizeof(*p->iods));
    p->sgls = calloc(cap, sizeof(*p->sgls));
    p->iovs = calloc(cap, sizeof(*p->iovs));
    p->vals = malloc((size_t)cap * FDB_LOCK_ENC_MAX);
    if (p->iods == NULL || p->sgls == NULL || p->iovs == NULL || p->vals == NULL) {
        lock_page_free(p);
        return MDS_ERR_NOMEM;
    }
    p->cap = cap;
    return MDS_OK;
}

/* List the next page of INODE 'K' akeys of @p oh and fetch their rows. */
static int lock_page_read(struct daos_txn *t, daos_handle_t oh, struct lock_page *p)
{
    struct daos_key dk;
    size_t off = 0;
    uint32_t i;
    int rc;

    daos_dkey_inode(&dk, DAOS_INODE_DKEY_LOCKS);
    rc = keypage_list(t, oh, &dk, &p->kp, p->cap);
    if (rc != 0 || p->kp.l.nr == 0) {
        return rc;
    }
    daos_fetch_init(&p->f, &dk, p->iods, p->sgls, p->iovs, p->cap);
    for (i = 0; i < p->kp.l.nr; i++) {
        size_t len = (size_t)p->kp.kds[i].kd_key_len;
        uint8_t *key = p->kp.buf + off;

        off += len;
        if (len != DAOS_KEY_BE64_LEN ||
            !daos_fetch_add_raw(&p->f, key, len, p->vals + (size_t)i * FDB_LOCK_ENC_MAX,
                                FDB_LOCK_ENC_MAX)) {
            return -DER_IO; /* corrupt lock key */
        }
    }
    rc = daos_fetch_run(t, oh, &p->f);
    return (rc == -DER_REC2BIG) ? -DER_IO : rc;
}

/* Decode row @p i of a read page (the lock_id is its akey). */
static int lock_page_row(const struct lock_page *p, uint32_t i, size_t *off, uint64_t fileid,
                         struct mds_coord_lock_row *row)
{
    size_t len;
    const uint8_t *key = keypage_key(&p->kp, i, off, &len);
    size_t vlen = daos_fetch_size(&p->f, i);

    if (vlen == 0 || !fdb_lock_decode(p->vals + (size_t)i * FDB_LOCK_ENC_MAX, vlen, fileid,
                                      fdb_get_u64(key), row)) {
        return -DER_IO; /* a listed lock without its row: corrupt */
    }
    return 0;
}

/* --- lock_test ----------------------------------------------------------- */

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
    return lock_type == DAOS_READ_LT || lock_type == DAOS_READW_LT;
}

static bool lock_owner_eq(const struct mds_coord_lock_row *row, uint64_t clientid,
                          const uint8_t *owner, uint32_t owner_len)
{
    if (row->clientid != clientid) {
        return false;
    }
    return owner_bytes_eq(row->owner, row->owner_len, owner, owner_len);
}

struct lock_test_ctx {
    struct daos_backend       *b;
    uint64_t                   fileid;
    uint32_t                   lock_type;
    uint64_t                   offset;
    uint64_t                   length;
    uint64_t                   clientid;
    const uint8_t             *owner;
    uint32_t                   owner_len;
    bool                       conflict_found;
    struct mds_coord_lock_row  conflict;
    struct lock_page           lp;
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

/* Examine the read page; *hit when a conflict was copied into c->conflict. */
static int lock_test_page(struct lock_test_ctx *c, bool *hit)
{
    size_t off = 0;
    uint32_t i;

    *hit = false;
    for (i = 0; i < c->lp.kp.l.nr && !*hit; i++) {
        struct mds_coord_lock_row row;
        int rc = lock_page_row(&c->lp, i, &off, c->fileid, &row);

        if (rc != 0) {
            return rc;
        }
        if (lock_row_conflicts(c, &row)) {
            c->conflict = row;
            *hit = true;
        }
    }
    return 0;
}

/* LOCKT against the persisted rows of the file, page by page inside one
 * read-only transaction (one consistent snapshot). */
static int lock_test_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct lock_test_ctx *c = arg;
    daos_handle_t oh;
    uint32_t pages = 0;
    bool more;
    int rc;

    c->conflict_found = false;
    keypage_rewind(&c->lp.kp);
    rc = daos_txn_obj(t, DAOS_TAG_INODE, c->fileid, &oh);
    if (rc != 0) {
        return rc;
    }
    do {
        bool hit = false;

        rc = lock_page_read(t, oh, &c->lp);
        if (rc == 0) {
            rc = lock_test_page(c, &hit);
        }
        if (rc != 0) {
            return rc;
        }
        if (hit) {
            c->conflict_found = true;
            *st_out = MDS_ERR_EXISTS;
            return DAOS_BODY_COMMIT;
        }
        more = keypage_next(&c->lp.kp);
        if (more && ++pages >= DAOS_LOCK_TEST_MAX_PAGES) {
            MDS_LOG_ERROR(LOG_COMP_CAT, "daos lock_test: file %llu holds more than %u lock "
                          "rows", (unsigned long long)c->fileid,
                          DAOS_LOCK_TEST_MAX_PAGES * DAOS_COORD_PAGE);
            *st_out = MDS_ERR_IO;
            return DAOS_BODY_DONE;
        }
    } while (more);
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

/* MDS_OK = the range is free (*conflict zeroed); MDS_ERR_EXISTS = a
 * conflicting lock exists and *conflict holds its row. */
static enum mds_status daos_lock_test(struct mds_catalogue *cat, uint64_t fileid,
                                      uint32_t lock_type, uint64_t offset, uint64_t length,
                                      uint64_t clientid, const uint8_t *owner,
                                      uint32_t owner_len, struct mds_coord_lock_row *conflict)
{
    struct lock_test_ctx *c;
    enum mds_status st;

    if (be_of(cat) == NULL || (owner_len > 0 && owner == NULL) || conflict == NULL) {
        return MDS_ERR_INVAL;
    }
    memset(conflict, 0, sizeof(*conflict));
    c = calloc(1, sizeof(*c));
    if (c == NULL) {
        return MDS_ERR_NOMEM;
    }
    if (lock_page_alloc(&c->lp, DAOS_COORD_PAGE) != MDS_OK) {
        free(c);
        return MDS_ERR_NOMEM;
    }
    c->b = be_of(cat);
    c->fileid = fileid;
    c->lock_type = lock_type;
    c->offset = offset;
    c->length = length;
    c->clientid = clientid;
    c->owner = owner;
    c->owner_len = owner_len;
    st = daos_run_txn(c->b, DAOS_TXN_READONLY, "lock_test", lock_test_body, c);
    if (st == MDS_ERR_EXISTS && c->conflict_found) {
        *conflict = c->conflict;
    }
    lock_page_free(&c->lp);
    free(c);
    return st;
}

/* --- lock_scan_file ------------------------------------------------------ */

/* Heap allocated once per call (the page array). */
struct lock_scan_ctx {
    struct daos_backend       *b;
    uint64_t                   fileid;
    struct lock_page           lp;
    bool                       more;
    uint32_t                   n;
    struct mds_coord_lock_row  page[DAOS_COORD_PAGE];
};

static int lock_scan_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct lock_scan_ctx *c = arg;
    daos_handle_t oh;
    size_t off = 0;
    uint32_t i;
    int rc;

    c->n = 0;
    c->more = false;
    rc = daos_txn_obj(t, DAOS_TAG_INODE, c->fileid, &oh);
    if (rc != 0) {
        return rc;
    }
    rc = lock_page_read(t, oh, &c->lp);
    if (rc != 0) {
        return rc;
    }
    for (i = 0; i < c->lp.kp.l.nr; i++) {
        rc = lock_page_row(&c->lp, i, &off, c->fileid, &c->page[c->n]);
        if (rc != 0) {
            c->n = 0;
            return rc;
        }
        c->n++;
    }
    c->more = keypage_next(&c->lp.kp);
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

static enum mds_status daos_lock_scan_file(struct mds_catalogue *cat, uint64_t fileid,
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
    if (lock_page_alloc(&c->lp, DAOS_COORD_PAGE) != MDS_OK) {
        free(c);
        return MDS_ERR_NOMEM;
    }
    c->b = be_of(cat);
    c->fileid = fileid;
    for (;;) {
        uint32_t i;

        st = daos_run_txn(c->b, DAOS_TXN_READONLY, "lock_scan_file", lock_scan_body, c);
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
    lock_page_free(&c->lp);
    free(c);
    return st;
}

/* --- lock_scan_owner ----------------------------------------------------- */

/* Heap allocated once per call (the page arrays). */
struct lock_owner_ctx {
    struct daos_backend       *b;
    uint64_t                   clientid;
    const uint8_t             *owner;
    uint32_t                   owner_len;
    struct keypage             kp;        /**< CLIENT 'K' akeys of one page. */
    struct wave                w;         /**< The INODE 'K' rows, one object each. */
    uint64_t                   lock_ids[DAOS_COORD_PAGE]; /**< Per wave entry. */
    uint32_t                   n;
    struct mds_coord_lock_row  page[DAOS_COORD_PAGE];
    bool                       more;
};

/* Wave entry for the lock row (fileid, lock_id) read from a CLIENT 'K' key. */
static int lock_wave_add(struct wave *w, uint64_t fileid, uint64_t lock_id)
{
    struct wave_ent *e = wave_add(w, fileid);

    if (e == NULL) {
        return -DER_INVAL;
    }
    daos_dkey_inode(&e->dkey, DAOS_INODE_DKEY_LOCKS);
    daos_akey_lock(&e->akey, lock_id);
    return 0;
}

/* One page of the client's lock index filtered by the owner prefix; the
 * rows of the matching keys in one wave. */
static int lock_owner_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct lock_owner_ctx *c = arg;
    struct daos_key dk;
    daos_handle_t oh;
    size_t off = 0;
    uint32_t i;
    int rc;

    c->n = 0;
    c->more = false;
    rc = daos_txn_obj(t, DAOS_TAG_CLIENT, c->clientid, &oh);
    if (rc != 0) {
        return rc;
    }
    daos_dkey_client(&dk, DAOS_CLIENT_DKEY_LOCKS);
    rc = keypage_list(t, oh, &dk, &c->kp, DAOS_COORD_PAGE);
    if (rc != 0) {
        return rc;
    }
    wave_reset(&c->w);
    for (i = 0; i < c->kp.l.nr; i++) {
        size_t len;
        const uint8_t *key = keypage_key(&c->kp, i, &off, &len);
        const uint8_t *owner;
        uint32_t owner_len;
        uint64_t fileid;
        uint64_t lock_id;

        if (!lock_idx_key_parse(key, len, &owner_len, &owner, &fileid, &lock_id)) {
            return -DER_IO; /* corrupt index key */
        }
        if (!owner_bytes_eq(owner, owner_len, c->owner, c->owner_len)) {
            continue;
        }
        c->lock_ids[c->w.n] = lock_id;
        rc = lock_wave_add(&c->w, fileid, lock_id);
        if (rc != 0) {
            return rc;
        }
    }
    rc = wave_run(t, &c->w, DAOS_HDL_INVAL, DAOS_TAG_INODE);
    if (rc != 0) {
        return rc;
    }
    for (i = 0; i < c->w.n; i++) {
        size_t vlen = wave_len(&c->w, i);
        struct mds_coord_lock_row *row = &c->page[c->n];

        if (vlen != 0 &&
            fdb_lock_decode(wave_val(&c->w, i), vlen, c->w.ents[i].id, c->lock_ids[i], row) &&
            lock_owner_eq(row, c->clientid, c->owner, c->owner_len)) {
            c->n++;
        }
    }
    c->more = keypage_next(&c->kp);
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

/* Every row of one lock-owner (LOCKU lookup). */
static enum mds_status daos_lock_scan_owner(struct mds_catalogue *cat, uint64_t clientid,
                                            const uint8_t *owner, uint32_t owner_len,
                                            mds_coord_lock_scan_cb cb, void *ctx)
{
    struct lock_owner_ctx *c;
    enum mds_status st = MDS_OK;

    if (be_of(cat) == NULL || cb == NULL || (owner_len > 0 && owner == NULL) ||
        owner_len > DAOS_LOCK_OWNER_MAX) {
        return MDS_ERR_INVAL;
    }
    c = calloc(1, sizeof(*c));
    if (c == NULL) {
        return MDS_ERR_NOMEM;
    }
    if (keypage_alloc(&c->kp, DAOS_COORD_PAGE) != MDS_OK ||
        wave_alloc(&c->w, DAOS_COORD_PAGE, FDB_LOCK_ENC_MAX) != MDS_OK) {
        st = MDS_ERR_NOMEM;
        goto out;
    }
    c->b = be_of(cat);
    c->clientid = clientid;
    c->owner = owner;
    c->owner_len = owner_len;
    for (;;) {
        uint32_t i;

        st = daos_run_txn(c->b, DAOS_TXN_READONLY, "lock_scan_owner", lock_owner_body, c);
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
    keypage_free(&c->kp);
    wave_free(&c->w);
    free(c);
    return st;
}

/* --- lock_reap_client ---------------------------------------------------- */

struct lock_reap_ctx {
    struct daos_backend *b;
    uint64_t             clientid;
    struct keypage       kp;       /**< CLIENT 'K' akeys of one batch. */
    struct wave          w;        /**< Their INODE 'K' rows, one object each. */
    uint64_t             lock_ids[DAOS_COORD_DEL_BATCH];
    bool                 more;
};

/* True when an earlier key of the batch names the same lock row
 * (fileid, lock_id): an orphaned owner key of a re-put lock lists the
 * row twice, and its punch is already buffered. */
static bool lock_reap_row_seen(const struct lock_reap_ctx *c, uint32_t i)
{
    uint32_t j;

    for (j = 0; j < i; j++) {
        if (c->w.ents[j].id == c->w.ents[i].id && c->lock_ids[j] == c->lock_ids[i]) {
            return true;
        }
    }
    return false;
}

/* True when @p k is one of the batch's scanned index keys, which are
 * punched as scanned keys, exactly once. */
static bool lock_reap_key_listed(const struct lock_reap_ctx *c, const struct daos_key *k)
{
    size_t off = 0;
    uint32_t i;

    for (i = 0; i < c->kp.l.nr; i++) {
        size_t len;
        const uint8_t *key = keypage_key(&c->kp, i, &off, &len);

        if (daos_key_equals(k, key, len)) {
            return true;
        }
    }
    return false;
}

/* Row @p i of the wave: a row still held by the client is punched with
 * its by-owner key; the scanned index key goes in any case.  Every key
 * is punched once per batch (file comment): a row named twice is
 * punched on its first key, and a by-owner key derived from a row that
 * the batch also lists is left to its own turn. */
static int lock_reap_row(struct daos_txn *t, struct lock_reap_ctx *c, daos_handle_t oh_c,
                         uint32_t i, const uint8_t *key, size_t key_len)
{
    struct daos_key dk;
    struct daos_key ak;
    size_t vlen = wave_len(&c->w, i);
    int rc;

    daos_dkey_client(&dk, DAOS_CLIENT_DKEY_LOCKS);
    if (vlen != 0 && !lock_reap_row_seen(c, i)) {
        struct mds_coord_lock_row row;

        if (!fdb_lock_decode(wave_val(&c->w, i), vlen, c->w.ents[i].id, c->lock_ids[i], &row)) {
            return -DER_IO;
        }
        if (row.clientid == c->clientid) {
            struct daos_key dk_row;

            daos_dkey_inode(&dk_row, DAOS_INODE_DKEY_LOCKS);
            daos_akey_lock(&ak, row.lock_id);
            rc = idx_clear(t, DAOS_TAG_INODE, row.fileid, &dk_row, &ak);
            if (rc != 0) {
                return rc;
            }
            lock_idx_akey(&ak, row.owner, row.owner_len, row.fileid, row.lock_id);
            if (daos_key_ok(&ak) && !lock_reap_key_listed(c, &ak)) {
                rc = daos_marker_clear(t, oh_c, &dk, &ak);
                if (rc != 0) {
                    return rc;
                }
            }
        }
    }
    daos_key_init(&ak);
    daos_key_bytes(&ak, key, key_len);
    return daos_marker_clear(t, oh_c, &dk, &ak);
}

/* One batch: the first DAOS_COORD_DEL_BATCH by-owner keys of the client
 * and their rows in one wave.  Re-runnable: each attempt re-reads the
 * index from the start. */
static int lock_reap_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct lock_reap_ctx *c = arg;
    struct daos_key dk;
    daos_handle_t oh;
    size_t off = 0;
    uint32_t i;
    int rc;

    c->more = false;
    keypage_rewind(&c->kp);
    rc = daos_txn_obj(t, DAOS_TAG_CLIENT, c->clientid, &oh);
    if (rc != 0) {
        return rc;
    }
    daos_dkey_client(&dk, DAOS_CLIENT_DKEY_LOCKS);
    rc = keypage_list(t, oh, &dk, &c->kp, DAOS_COORD_DEL_BATCH);
    if (rc != 0) {
        return rc;
    }
    wave_reset(&c->w);
    for (i = 0; i < c->kp.l.nr; i++) {
        size_t len;
        const uint8_t *key = keypage_key(&c->kp, i, &off, &len);
        const uint8_t *owner;
        uint32_t owner_len;
        uint64_t fileid;
        uint64_t lock_id;

        if (!lock_idx_key_parse(key, len, &owner_len, &owner, &fileid, &lock_id)) {
            return -DER_IO; /* corrupt index key */
        }
        c->lock_ids[i] = lock_id;
        rc = lock_wave_add(&c->w, fileid, lock_id);
        if (rc != 0) {
            return rc;
        }
    }
    rc = wave_run(t, &c->w, DAOS_HDL_INVAL, DAOS_TAG_INODE);
    if (rc != 0) {
        return rc;
    }
    c->more = !c->kp.l.eof;
    off = 0;
    for (i = 0; i < c->kp.l.nr; i++) {
        size_t len;
        const uint8_t *key = keypage_key(&c->kp, i, &off, &len);

        rc = lock_reap_row(t, c, oh, i, key, len);
        if (rc != 0) {
            return rc;
        }
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

static enum mds_status daos_lock_reap_client(struct mds_catalogue *cat, uint64_t clientid)
{
    struct lock_reap_ctx *c;
    enum mds_status st = MDS_OK;
    uint32_t batches = 0;

    if (be_of(cat) == NULL) {
        return MDS_ERR_INVAL;
    }
    c = calloc(1, sizeof(*c));
    if (c == NULL) {
        return MDS_ERR_NOMEM;
    }
    if (keypage_alloc(&c->kp, DAOS_COORD_DEL_BATCH) != MDS_OK ||
        wave_alloc(&c->w, DAOS_COORD_DEL_BATCH, FDB_LOCK_ENC_MAX) != MDS_OK) {
        st = MDS_ERR_NOMEM;
        goto out;
    }
    c->b = be_of(cat);
    c->clientid = clientid;
    do {
        c->more = false;
        st = daos_run_txn(c->b, DAOS_TXN_MUTATING, "lock_reap_client", lock_reap_body, c);
        batches++;
    } while (st == MDS_OK && c->more && batches < DAOS_COORD_DEL_MAX_BATCHES);
    if (st == MDS_OK && c->more) {
        MDS_LOG_ERROR(LOG_COMP_CAT, "daos lock_reap_client: client %llu still has lock rows "
                      "after %u batches", (unsigned long long)clientid, batches);
        st = MDS_ERR_DELAY;
    }
out:
    keypage_free(&c->kp);
    wave_free(&c->w);
    free(c);
    return st;
}

/* -----------------------------------------------------------------------
 * Delegations
 * ----------------------------------------------------------------------- */

struct deleg_ctx {
    struct daos_backend              *b;
    const uint8_t                    *other;
    const struct mds_coord_deleg_row *in;
    struct mds_coord_deleg_row        row;
    uint8_t                           enc[FDB_DELEG_ENC_SIZE];
};

/* The INODE 'D' and CLIENT 'D' markers of a delegation row. */
static int deleg_markers(struct daos_txn *t, uint64_t fileid, uint64_t clientid,
                         const uint8_t other[NFS4_OTHER_SIZE], bool set)
{
    struct daos_key dk;
    struct daos_key ak;
    int rc;

    daos_akey_stateid(&ak, other);
    daos_dkey_inode(&dk, DAOS_INODE_DKEY_DELEGS);
    rc = set ? idx_set(t, DAOS_TAG_INODE, fileid, &dk, &ak)
             : idx_clear(t, DAOS_TAG_INODE, fileid, &dk, &ak);
    if (rc != 0) {
        return rc;
    }
    daos_dkey_client(&dk, DAOS_CLIENT_DKEY_DELEGS);
    return set ? idx_set(t, DAOS_TAG_CLIENT, clientid, &dk, &ak)
               : idx_clear(t, DAOS_TAG_CLIENT, clientid, &dk, &ak);
}

static int deleg_put_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct deleg_ctx *c = arg;
    struct daos_key dk;
    struct daos_key ak;
    daos_handle_t oh;
    size_t len = 0;
    int rc;

    if (!fdb_deleg_encode(c->in, c->enc, sizeof(c->enc), &len)) {
        return -DER_INVAL;
    }
    rc = daos_txn_obj(t, DAOS_TAG_DELEG, 0, &oh);
    if (rc != 0) {
        return rc;
    }
    daos_dkey_stateid(&dk, c->in->stateid_other);
    daos_akey_value(&ak);
    rc = daos_update_single(t, oh, &dk, &ak, c->enc, len, 0, 0);
    if (rc == 0) {
        rc = deleg_markers(t, c->in->fileid, c->in->clientid, c->in->stateid_other, true);
    }
    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

static int deleg_read(struct daos_txn *t, struct deleg_ctx *c, bool *found)
{
    struct daos_key dk;
    struct daos_key ak;
    daos_handle_t oh;
    size_t len = 0;
    int rc;

    *found = false;
    rc = daos_txn_obj(t, DAOS_TAG_DELEG, 0, &oh);
    if (rc != 0) {
        return rc;
    }
    daos_dkey_stateid(&dk, c->other);
    daos_akey_value(&ak);
    rc = row_fetch(t, oh, &dk, &ak, c->enc, sizeof(c->enc), &len, found);
    if (rc != 0) {
        return rc;
    }
    if (*found && !fdb_deleg_decode(c->enc, len, c->other, &c->row)) {
        return -DER_IO;
    }
    return 0;
}

static int deleg_get_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct deleg_ctx *c = arg;
    bool found = false;
    int rc = deleg_read(t, c, &found);

    if (rc != 0) {
        return rc;
    }
    *st_out = found ? MDS_OK : MDS_ERR_NOTFOUND;
    return DAOS_BODY_COMMIT;
}

static int deleg_del_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct deleg_ctx *c = arg;
    struct daos_key dk;
    daos_handle_t oh;
    bool found = false;
    int rc = deleg_read(t, c, &found);

    if (rc != 0) {
        return rc;
    }
    if (!found) {
        *st_out = MDS_ERR_NOTFOUND;
        return DAOS_BODY_DONE;
    }
    rc = daos_txn_obj(t, DAOS_TAG_DELEG, 0, &oh);
    if (rc != 0) {
        return rc;
    }
    daos_dkey_stateid(&dk, c->other);
    rc = daos_punch_dkeys(t, oh, &dk, 1, 0);
    if (rc == 0) {
        rc = deleg_markers(t, c->row.fileid, c->row.clientid, c->other, false);
    }
    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

static enum mds_status daos_deleg_put(struct mds_catalogue *cat,
                                      const struct mds_coord_deleg_row *row)
{
    struct deleg_ctx c;

    if (be_of(cat) == NULL || row == NULL) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.b = be_of(cat);
    c.in = row;
    return daos_run_txn(c.b, DAOS_TXN_MUTATING, "deleg_put", deleg_put_body, &c);
}

static enum mds_status daos_deleg_get(struct mds_catalogue *cat, const uint8_t stateid_other[12],
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
    st = daos_run_txn(c.b, DAOS_TXN_NONE, "deleg_get", deleg_get_body, &c);
    if (st == MDS_OK) {
        *row = c.row;
    }
    return st;
}

static enum mds_status daos_deleg_del(struct mds_catalogue *cat, const uint8_t stateid_other[12])
{
    struct deleg_ctx c;

    if (be_of(cat) == NULL || stateid_other == NULL) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.b = be_of(cat);
    c.other = stateid_other;
    return daos_run_txn(c.b, DAOS_TXN_MUTATING, "deleg_del", deleg_del_body, &c);
}

/* Heap allocated once per call (the page arrays). */
struct deleg_scan_ctx {
    struct idx_scan             idx;
    const uint64_t             *fileid;
    const uint64_t             *clientid;
    uint32_t                    n;
    struct mds_coord_deleg_row  page[DAOS_COORD_PAGE];
};

static int deleg_scan_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct deleg_scan_ctx *c = arg;
    size_t off = 0;
    uint32_t i;
    int rc;

    c->n = 0;
    rc = idx_scan_page(t, &c->idx, DAOS_COORD_PAGE);
    if (rc != 0) {
        return rc;
    }
    for (i = 0; i < c->idx.kp.l.nr; i++) {
        size_t len;
        const uint8_t *other = keypage_key(&c->idx.kp, i, &off, &len);
        size_t vlen = wave_len(&c->idx.w, i);
        struct mds_coord_deleg_row *row = &c->page[c->n];

        if (vlen != 0 && fdb_deleg_decode(wave_val(&c->idx.w, i), vlen, other, row) &&
            (c->fileid == NULL || row->fileid == *c->fileid) &&
            (c->clientid == NULL || row->clientid == *c->clientid)) {
            c->n++;
        }
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
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
    if (idx_scan_alloc(&c->idx, DAOS_COORD_PAGE, FDB_DELEG_ENC_SIZE) != MDS_OK) {
        free(c);
        return MDS_ERR_NOMEM;
    }
    c->idx.row_tag = DAOS_TAG_DELEG;
    c->idx.row_akey = (uint8_t)'v';
    c->idx.key_len = NFS4_OTHER_SIZE;
    c->fileid = fileid;
    c->clientid = clientid;
    if (fileid != NULL) {
        c->idx.idx_tag = DAOS_TAG_INODE;
        c->idx.idx_id = *fileid;
        c->idx.idx_dkey = (uint8_t)DAOS_INODE_DKEY_DELEGS;
    } else {
        c->idx.idx_tag = DAOS_TAG_CLIENT;
        c->idx.idx_id = *clientid;
        c->idx.idx_dkey = (uint8_t)DAOS_CLIENT_DKEY_DELEGS;
    }
    for (;;) {
        uint32_t i;

        st = daos_run_txn(be_of(cat), DAOS_TXN_READONLY, "deleg_scan", deleg_scan_body, c);
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
    idx_scan_free(&c->idx);
    free(c);
    return st;
}

static enum mds_status daos_deleg_scan_file(struct mds_catalogue *cat, uint64_t fileid,
                                            mds_coord_deleg_scan_cb cb, void *ctx)
{
    return deleg_scan_run(cat, &fileid, NULL, cb, ctx);
}

static enum mds_status daos_deleg_scan_client(struct mds_catalogue *cat, uint64_t clientid,
                                              mds_coord_deleg_scan_cb cb, void *ctx)
{
    return deleg_scan_run(cat, NULL, &clientid, cb, ctx);
}

/* -----------------------------------------------------------------------
 * Client identity
 * ----------------------------------------------------------------------- */

/* Heap allocated once per call (the row holds a 1 KB owner id). */
struct client_ctx {
    struct daos_backend                *b;
    uint64_t                            clientid;
    const struct mds_coord_client_row  *in;
    struct mds_coord_client_row         row;
    uint8_t                             enc[FDB_CLIENT_ENC_MAX];
};

static int client_put_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct client_ctx *c = arg;
    struct daos_key dk;
    struct daos_key ak;
    daos_handle_t oh;
    size_t len = 0;
    int rc;

    if (!fdb_client_encode(c->in, c->enc, sizeof(c->enc), &len)) {
        return -DER_INVAL;
    }
    rc = daos_txn_obj(t, DAOS_TAG_CLIENT, c->in->clientid, &oh);
    if (rc != 0) {
        return rc;
    }
    daos_dkey_client(&dk, DAOS_CLIENT_DKEY_ROW);
    daos_akey_value(&ak);
    rc = daos_update_single(t, oh, &dk, &ak, c->enc, len, 0, 0);
    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

static int client_get_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct client_ctx *c = arg;
    struct daos_key dk;
    struct daos_key ak;
    daos_handle_t oh;
    size_t len = 0;
    bool found = false;
    int rc;

    rc = daos_txn_obj(t, DAOS_TAG_CLIENT, c->clientid, &oh);
    if (rc != 0) {
        return rc;
    }
    daos_dkey_client(&dk, DAOS_CLIENT_DKEY_ROW);
    daos_akey_value(&ak);
    rc = row_fetch(t, oh, &dk, &ak, c->enc, sizeof(c->enc), &len, &found);
    if (rc != 0) {
        return rc;
    }
    if (!found) {
        *st_out = MDS_ERR_NOTFOUND;
        return DAOS_BODY_DONE;
    }
    if (!fdb_client_decode(c->enc, len, c->clientid, &c->row)) {
        return -DER_IO;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

/* One conditional punch (see journal_del_body). */
static int client_del_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct client_ctx *c = arg;
    struct daos_key dk;
    daos_handle_t oh;
    int rc;

    rc = daos_txn_obj(t, DAOS_TAG_CLIENT, c->clientid, &oh);
    if (rc != 0) {
        return rc;
    }
    daos_dkey_client(&dk, DAOS_CLIENT_DKEY_ROW);
    rc = daos_punch_dkeys(t, oh, &dk, 1, DAOS_COND_PUNCH);
    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

static enum mds_status daos_client_put(struct mds_catalogue *cat,
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
    st = daos_run_txn(c->b, DAOS_TXN_MUTATING, "client_put", client_put_body, c);
    free(c);
    return st;
}

static enum mds_status daos_client_get(struct mds_catalogue *cat, uint64_t clientid,
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
    st = daos_run_txn(c->b, DAOS_TXN_NONE, "client_get", client_get_body, c);
    if (st == MDS_OK) {
        *row = c->row;
    }
    free(c);
    return st;
}

static enum mds_status daos_client_del(struct mds_catalogue *cat, uint64_t clientid)
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
    st = daos_run_txn(c->b, DAOS_TXN_MUTATING, "client_del", client_del_body, c);
    free(c);
    return st;
}

/* -----------------------------------------------------------------------
 * Sessions
 * ----------------------------------------------------------------------- */

struct session_ctx {
    struct daos_backend                 *b;
    const uint8_t                       *session_id;
    const struct mds_coord_session_row  *in;
    struct mds_coord_session_row         row;
    uint8_t                              enc[FDB_SESSION_ENC_SIZE];
};

/* The CLIENT 'S' marker of a session. */
static int session_marker(struct daos_txn *t, uint64_t clientid, const uint8_t session_id[16],
                          bool set)
{
    struct daos_key dk;
    struct daos_key ak;

    daos_dkey_client(&dk, DAOS_CLIENT_DKEY_SESSIONS);
    daos_akey_session(&ak, session_id);
    return set ? idx_set(t, DAOS_TAG_CLIENT, clientid, &dk, &ak)
               : idx_clear(t, DAOS_TAG_CLIENT, clientid, &dk, &ak);
}

static int session_put_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct session_ctx *c = arg;
    struct daos_key dk;
    struct daos_key ak;
    daos_handle_t oh;
    size_t len = 0;
    int rc;

    if (!fdb_session_encode(c->in, c->enc, sizeof(c->enc), &len)) {
        return -DER_INVAL;
    }
    rc = daos_txn_obj(t, DAOS_TAG_SESSION, 0, &oh);
    if (rc != 0) {
        return rc;
    }
    daos_dkey_session(&dk, c->in->session_id);
    daos_akey_session_row(&ak);
    rc = daos_update_single(t, oh, &dk, &ak, c->enc, len, 0, 0);
    if (rc == 0) {
        rc = session_marker(t, c->in->clientid, c->in->session_id, true);
    }
    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

static int session_read(struct daos_txn *t, struct session_ctx *c, bool *found)
{
    struct daos_key dk;
    struct daos_key ak;
    daos_handle_t oh;
    size_t len = 0;
    int rc;

    *found = false;
    rc = daos_txn_obj(t, DAOS_TAG_SESSION, 0, &oh);
    if (rc != 0) {
        return rc;
    }
    daos_dkey_session(&dk, c->session_id);
    daos_akey_session_row(&ak);
    rc = row_fetch(t, oh, &dk, &ak, c->enc, sizeof(c->enc), &len, found);
    if (rc != 0) {
        return rc;
    }
    if (*found && !fdb_session_decode(c->enc, len, c->session_id, &c->row)) {
        return -DER_IO;
    }
    return 0;
}

static int session_get_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct session_ctx *c = arg;
    bool found = false;
    int rc = session_read(t, c, &found);

    if (rc != 0) {
        return rc;
    }
    *st_out = found ? MDS_OK : MDS_ERR_NOTFOUND;
    return DAOS_BODY_COMMIT;
}

/* Only the 's' akey goes: the DRC slots of the dkey stay, as the
 * FoundationDB and memdb units leave their slot rows. */
static int session_del_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct session_ctx *c = arg;
    struct daos_key dk;
    struct daos_key ak;
    daos_handle_t oh;
    bool found = false;
    int rc = session_read(t, c, &found);

    if (rc != 0) {
        return rc;
    }
    if (!found) {
        *st_out = MDS_ERR_NOTFOUND;
        return DAOS_BODY_DONE;
    }
    rc = daos_txn_obj(t, DAOS_TAG_SESSION, 0, &oh);
    if (rc != 0) {
        return rc;
    }
    daos_dkey_session(&dk, c->session_id);
    daos_akey_session_row(&ak);
    rc = daos_punch_akeys(t, oh, &dk, &ak, 1, 0);
    if (rc == 0) {
        rc = session_marker(t, c->row.clientid, c->session_id, false);
    }
    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

static enum mds_status daos_session_put(struct mds_catalogue *cat,
                                        const struct mds_coord_session_row *row)
{
    struct session_ctx c;

    if (be_of(cat) == NULL || row == NULL) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.b = be_of(cat);
    c.in = row;
    return daos_run_txn(c.b, DAOS_TXN_MUTATING, "session_put", session_put_body, &c);
}

static enum mds_status daos_session_get(struct mds_catalogue *cat, const uint8_t session_id[16],
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
    st = daos_run_txn(c.b, DAOS_TXN_NONE, "session_get", session_get_body, &c);
    if (st == MDS_OK) {
        *row = c.row;
    }
    return st;
}

static enum mds_status daos_session_del(struct mds_catalogue *cat, const uint8_t session_id[16])
{
    struct session_ctx c;

    if (be_of(cat) == NULL || session_id == NULL) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.b = be_of(cat);
    c.session_id = session_id;
    return daos_run_txn(c.b, DAOS_TXN_MUTATING, "session_del", session_del_body, &c);
}

/* Heap allocated once per call (the page arrays). */
struct session_scan_ctx {
    struct idx_scan               idx;
    uint64_t                      clientid;
    uint32_t                      n;
    struct mds_coord_session_row  page[DAOS_COORD_PAGE];
};

static int session_scan_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct session_scan_ctx *c = arg;
    size_t off = 0;
    uint32_t i;
    int rc;

    c->n = 0;
    rc = idx_scan_page(t, &c->idx, DAOS_COORD_PAGE);
    if (rc != 0) {
        return rc;
    }
    for (i = 0; i < c->idx.kp.l.nr; i++) {
        size_t len;
        const uint8_t *sid = keypage_key(&c->idx.kp, i, &off, &len);
        size_t vlen = wave_len(&c->idx.w, i);
        struct mds_coord_session_row *row = &c->page[c->n];

        if (vlen != 0 && fdb_session_decode(wave_val(&c->idx.w, i), vlen, sid, row) &&
            row->clientid == c->clientid) {
            c->n++;
        }
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

static enum mds_status daos_session_scan_client(struct mds_catalogue *cat, uint64_t clientid,
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
    if (idx_scan_alloc(&c->idx, DAOS_COORD_PAGE, FDB_SESSION_ENC_SIZE) != MDS_OK) {
        free(c);
        return MDS_ERR_NOMEM;
    }
    c->idx.idx_tag = DAOS_TAG_CLIENT;
    c->idx.idx_id = clientid;
    c->idx.idx_dkey = (uint8_t)DAOS_CLIENT_DKEY_SESSIONS;
    c->idx.row_tag = DAOS_TAG_SESSION;
    c->idx.row_akey = (uint8_t)'s';
    c->idx.key_len = 16U;
    c->clientid = clientid;
    for (;;) {
        uint32_t i;

        st = daos_run_txn(be_of(cat), DAOS_TXN_READONLY, "session_scan_client",
                          session_scan_body, c);
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
    idx_scan_free(&c->idx);
    free(c);
    return st;
}

/* -----------------------------------------------------------------------
 * DRC slots
 * ----------------------------------------------------------------------- */

/* Heap allocated once per call (a slot value is up to 64 KB). */
struct slot_ctx {
    struct daos_backend *b;
    const uint8_t       *session_id;
    uint32_t             slot_id;
    struct fdb_slot_hdr  hdr;
    const uint8_t       *reply_in;   /**< put */
    uint8_t             *reply_out;  /**< get: heap copy for the caller */
    uint8_t              enc[FDB_SLOT_ENC_MAX];
};

static int slot_put_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct slot_ctx *c = arg;
    struct daos_key dk;
    struct daos_key ak;
    daos_handle_t oh;
    size_t len = 0;
    int rc;

    c->hdr.last_used_ns = realtime_ns();
    if (!fdb_slot_encode(&c->hdr, c->reply_in, c->enc, sizeof(c->enc), &len)) {
        return -DER_INVAL;
    }
    rc = daos_txn_obj(t, DAOS_TAG_SESSION, 0, &oh);
    if (rc != 0) {
        return rc;
    }
    daos_dkey_session(&dk, c->session_id);
    daos_akey_drc_slot(&ak, c->slot_id);
    rc = daos_update_single(t, oh, &dk, &ak, c->enc, len, 0, 0);
    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

static int slot_get_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct slot_ctx *c = arg;
    struct daos_key dk;
    struct daos_key ak;
    daos_handle_t oh;
    const uint8_t *reply = NULL;
    size_t len = 0;
    bool found = false;
    int rc;

    free(c->reply_out); /* a previous attempt's copy */
    c->reply_out = NULL;
    rc = daos_txn_obj(t, DAOS_TAG_SESSION, 0, &oh);
    if (rc != 0) {
        return rc;
    }
    daos_dkey_session(&dk, c->session_id);
    daos_akey_drc_slot(&ak, c->slot_id);
    rc = row_fetch(t, oh, &dk, &ak, c->enc, sizeof(c->enc), &len, &found);
    if (rc != 0) {
        return rc;
    }
    if (!found) {
        *st_out = MDS_ERR_NOTFOUND;
        return DAOS_BODY_DONE;
    }
    if (!fdb_slot_decode(c->enc, len, &c->hdr, &reply)) {
        return -DER_IO;
    }
    if (c->hdr.reply_len > 0) {
        c->reply_out = malloc(c->hdr.reply_len);
        if (c->reply_out == NULL) {
            *st_out = MDS_ERR_NOMEM;
            return DAOS_BODY_DONE;
        }
        memcpy(c->reply_out, reply, c->hdr.reply_len);
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

static enum mds_status daos_slot_put(struct mds_catalogue *cat, const uint8_t session_id[16],
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
    st = daos_run_txn(c->b, DAOS_TXN_MUTATING, "slot_put", slot_put_body, c);
    free(c);
    return st;
}

/* row->cached_reply is a heap copy the caller frees (NULL when empty). */
static enum mds_status daos_slot_get(struct mds_catalogue *cat, const uint8_t session_id[16],
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
    st = daos_run_txn(c->b, DAOS_TXN_NONE, "slot_get", slot_get_body, c);
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

const struct mds_coordination_ops daos_coordination_ops = {
    .journal_put               = daos_journal_put,
    .journal_get               = daos_journal_get,
    .journal_del               = daos_journal_del,
    .journal_scan              = daos_journal_scan,
    .layout_grant              = daos_layout_grant,
    .layout_grant_union        = daos_layout_grant_union,
    .layoutget_fused           = daos_layoutget_fused,
    .layout_return             = daos_layout_return,
    .layout_get_by_stateid     = daos_layout_get_by_stateid,
    .layout_scan_for_file      = daos_layout_scan_for_file,
    .layout_del_all_for_client = daos_layout_del_all_for_client,
    .ds_layout_idx_scan        = daos_ds_layout_idx_scan,
    .layout_iter_file          = daos_layout_iter_file,
    .recovery_put              = daos_recovery_put,
    .recovery_del              = daos_recovery_del,
    .recovery_get              = daos_recovery_get,
    .recovery_list             = daos_recovery_list,
    .open_put                  = daos_open_put,
    .open_get                  = daos_open_get,
    .open_del                  = daos_open_del,
    .open_scan_file            = daos_open_scan_file,
    .open_scan_client          = daos_open_scan_client,
    .lock_put                  = daos_lock_put,
    .lock_del                  = daos_lock_del,
    .lock_test                 = daos_lock_test,
    .lock_scan_file            = daos_lock_scan_file,
    .lock_scan_owner           = daos_lock_scan_owner,
    .lock_reap_client          = daos_lock_reap_client,
    .deleg_put                 = daos_deleg_put,
    .deleg_get                 = daos_deleg_get,
    .deleg_del                 = daos_deleg_del,
    .deleg_scan_file           = daos_deleg_scan_file,
    .deleg_scan_client         = daos_deleg_scan_client,
    .client_put                = daos_client_put,
    .client_get                = daos_client_get,
    .client_del                = daos_client_del,
    .session_put               = daos_session_put,
    .session_get               = daos_session_get,
    .session_del               = daos_session_del,
    .session_scan_client       = daos_session_scan_client,
    .slot_put                  = daos_slot_put,
    .slot_get                  = daos_slot_get,
};
