/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * catalogue_daos_ext.c -- DAOS backend: extended authority slots.
 *
 * The authority slots outside the namespace: inline data, extended
 * attributes, stripe maps, the DS registry and provisioning, quota,
 * the GC queue, the async-REMOVE delete manifest, shard routing,
 * cross-shard dirents and link anchors.  Same rules as
 * catalogue_daos_ns.c: every slot is ONE daos_run_txn (daos_txn.h), the
 * mds_cat_txn token is never enlisted (C6), reads that do not depend
 * on each other are issued as one parallel wave on the transaction's
 * event queue and finished together, every body reads first and then
 * writes blindly (a DAOS transaction has no read-your-writes), and an
 * enumerating slot materialises a bounded page inside a read-only
 * transaction and delivers it after daos_run_txn returned (C1/C2).
 * Statuses follow the memdb reference implementation
 * (catalogue_memdb.c) through the FoundationDB unit
 * (catalogue_fdb_ext.c); the deviations are listed at the end.
 *
 * Transaction shapes (F = fetch RPC, L = key listing, U = buffered
 * update, P = buffered punch, CP = conditional punch (DAOS_COND_PUNCH),
 * "wave" = parallel fetches finished together, "page" = one read-only
 * transaction per page; every mutating transaction ends in one commit
 * RPC; "(none)" = a single operation without a transaction handle):
 *   inline_get                    F 'i'/'d' [-> F of the exact size]   (read-only)
 *   inline_put                    U 'i'/'d' (blind)
 *   inline_del                    CP dkey 'i'
 *   xattr_get                     F 'x'/name [-> F of the exact size]  (read-only)
 *   xattr_exists                  F 'x'/name, one-byte probe           (none)
 *   xattr_put                     F 'a' blob + this thread's counter shard;
 *                                 U 'x'/name, inode touch (directory: U
 *                                 shard; file: U blob with ctime/change bumped)
 *   xattr_del                     F 'a' blob + shard; CP 'x'/name, inode touch
 *   xattr_list                    page: L 'x' akeys after the last name
 *   stripe_map_get                F 's'/'m' [-> F of the exact size]   (read-only)
 *   stripe_map_put                U 's'/'m' blob, U STRIPE_IDX marker (blind)
 *   stripe_map_del                P dkey 's', P STRIPE_IDX marker (blind)
 *   stripe_map_scan               page: L STRIPE_IDX dkeys -> wave F 's'/'m' of
 *                                 every file [-> wave F of the exact size for the
 *                                 maps larger than the first buffer]
 *   ds_get / ds_put / ds_del      F (none) / U / CP dkey
 *   ds_list                       L DS 'd' dkeys -> wave F rows        (read-only)
 *   ds_provision_get/put/del      F (none) / U / CP dkey
 *   quota_rule_get / _put         F (none) / U
 *   quota_usage_get / _put        F (none) / U (absolute upsert)
 *   gc_enqueue                    seq from the GC_SEQ batch allocator; U GC row,
 *                                 U GC_BY_OWNER marker (blind)
 *   gc_peek / gc_peek_batch       mds_id != 0: L GC_BY_OWNER(mds_id), L
 *                                 GC_BY_OWNER(0) (cap keys each) -> merge by
 *                                 seq -> waves F GC rows; mds_id == 0: L GC
 *                                 dkeys (cap) -> waves F rows         (read-only)
 *   gc_dequeue                    F GC row -> P row + P GC_BY_OWNER marker
 *   gc_count                      mds_id != 0: page: L GC_BY_OWNER(mds_id), then
 *                                 (0), count keys; mds_id == 0: page: L GC
 *                                 dkeys, count
 *   remove_pending_enqueue        seq from the REMOVE_SEQ allocator; U row
 *   remove_pending_enqueue_unlink F dirent || F child blob; U child blob
 *                                 (DELETE_PENDING), P 'N' akey + 'S'+seq dkey,
 *                                 U manifest row
 *   remove_pending_peek_batch     page: L REMOVE_PENDING dkeys -> wave F rows,
 *                                 claim filter
 *   remove_pending_claim          F -> U (claim decided and written in the same
 *                                 transaction, C3)
 *   remove_pending_complete       P dkey (blind, idempotent)
 *   remove_pending_bump_retry     F -> U
 *   remove_pending_count          page: L dkeys, count
 *   remove_pending_scan_all       page: L dkeys -> wave F rows
 *   shard_fileid_get/put/del      F (none) / U / CP dkey 'h'
 *   ext_dirent_get/put/del        F (none) / U / CP 'E'/name
 *   link_anchor_put / _del        U / CP dkey
 *
 * Conditional clears.  Where the FoundationDB unit reads a row's
 * presence and then clears it, this unit punches with DAOS_COND_PUNCH:
 * an absent row fails the call or the commit with -DER_NONEXIST, which
 * the runner reports as MDS_ERR_NOTFOUND with nothing applied -- the
 * predicate is enforced by the store (one commit, no read).  xattr_del
 * combines it with the inode read its touch needs: a missing name
 * aborts the whole transaction, touch included.
 *
 * Empty values.  DAOS treats a zero-length single-value update as a
 * punch, and both the inline data and an xattr may legitimately hold an
 * empty value (memdb semantics).  The stored form of INODE 'i'/'d' and
 * of every INODE 'x'/name value is therefore one version byte
 * (DAOS_EXT_BLOB_VERSION) followed by the bytes; this refines the
 * "raw bytes" of the daos_keys.h layout table.  A foreign version byte
 * is a corrupt row (MDS_ERR_IO).
 *
 * Adaptive reads.  A fetch into a buffer smaller than the value returns
 * -DER_REC2BIG together with the stored size and transfers nothing, and
 * a fetch buffer at or above the client's bulk threshold costs an RDMA
 * registration whatever the value's size.  The variable-size reads
 * (xattr_get, stripe_map_get, inline_get, the scan's first wave) use a
 * first buffer below that threshold and fetch a larger value once more
 * into a buffer of exactly its size; both fetches run under one
 * read-only transaction, so the size cannot change in between.
 *
 * GC visibility.  gc_peek, gc_peek_batch and gc_count see the rows
 * whose owner_mds_id is 0 (legacy) or this MDS's id, exactly like the
 * RonDB and FoundationDB backends (every row when this MDS's id is 0);
 * gc_enqueue and the fused remove (catalogue_daos_ns.c) stamp the
 * owner.  ds_gc.c drops a peeked row whose ds_id is not mounted on the
 * draining MDS, so on a store shared by several daemons an unfiltered
 * peek would let a peer drain -- and drop -- rows it cannot service.  A
 * dead MDS's rows therefore wait for its restart under the same id;
 * reaping foreign rows is a recovery-time job, not a peek-time one.
 * The filter is structural, not a scan: every row has a GC_BY_OWNER
 * marker (daos_keys.h) written and cleared with it, GC_BY_OWNER is a
 * uint64-keyed object whose dkeys enumerate in sequence order, and a
 * filtered peek lists the two owner objects it may drain (its own id
 * and 0), merges them by seq and fetches the rows -- O(rows it may
 * drain) whatever a dead peer's backlog is.  The GC rows themselves
 * live in a spread object (hash order), so the id-0 view -- every row
 * -- delivers the first cap rows of that order rather than the lowest
 * sequences; a hash order is a stable total order, so the id-0 drain
 * still visits every row exactly once.
 *
 * Counts.  gc_count and remove_pending_count are exact paged counts
 * (RonDB semantics) that saturate at DAOS_EXT_COUNT_MAX examined keys so
 * the metrics gauge they feed stays bounded in time.  A maintained META
 * counter is not an option: catalogue_daos_ns.c's fused remove writes
 * GC rows directly and would drift it.
 *
 * Sequences.  GC and manifest sequences come from the META batch
 * allocators (daos_backend_alloc_id), so their order is the order the
 * thread-local batches were reserved: strictly increasing within one
 * thread, not a global enqueue-time order across threads.
 *
 * Deviations from memdb, all deliberate: stripe_map_put refuses a zero
 * stripe_count, mirror_count or stripe_unit (MDS_ERR_INVAL; the blob
 * codec cannot represent it and no caller passes it); ds_put and
 * ds_provision_put refuse ds_id >= MDS_MAX_DS_NODES (MDS_ERR_INVAL) so
 * ds_list is bounded by construction; GC rows carry this MDS's owner id
 * and the owner filter above applies; xattr_list delivers names in the
 * store's hash order of the 'x' akeys (a stable total order, every name
 * once, not byte order); the id-0 GC view is in hash order and its
 * count does not decode the rows.
 */

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
#include "fdb_codec.h"
#include "quota.h"

/* Stored form of an xattr value and of the inline data: one version
 * byte, then the bytes (see "Empty values" above). */
#define DAOS_EXT_BLOB_VERSION     1U
#define DAOS_EXT_BLOB_PREFIX      1U
#define DAOS_EXT_INLINE_ENC_MAX   (DAOS_EXT_BLOB_PREFIX + MDS_INLINE_DATA_MAX)
#define DAOS_EXT_XATTR_ENC_MAX    (DAOS_EXT_BLOB_PREFIX + MDS_XATTR_VAL_MAX)

/* First buffer of the adaptive reads (see "Adaptive reads" above): a
 * value that fits costs one fetch with an inline reply. */
#define DAOS_EXT_SMALL_FETCH      3968U

/* Names per xattr_list page. */
#define DAOS_EXT_XATTR_PAGE       256U

/* stripe_map_scan: files per page (one parallel wave each). */
#define DAOS_EXT_SCAN_PAGE        64U

/* Parallel row fetches in flight per wave. */
#define DAOS_EXT_WAVE             128U

/* Queue walks: keys listed per page, the examine cap of a filtered peek
 * and the saturation point of the counting slots. */
#define DAOS_EXT_QUEUE_PAGE       DAOS_EXT_WAVE
#define DAOS_EXT_COUNT_PAGE       1024U
#define DAOS_EXT_COUNT_MAX        (1U << 20)
#define DAOS_EXT_EXAMINE_CAP(cap) ((uint64_t)(cap) * 256U + 4096U)

/* remove_pending_scan_all rows materialised per page. */
#define DAOS_EXT_RP_SCAN_PAGE     256U

/* Largest batch one GC peek materialises (ds_gc asks for at most 4096,
 * its DS_GC_MAX_BATCH_SIZE).  A larger cap is served up to this bound:
 * fewer rows than asked for is always a legal answer, and the bound
 * keeps the per-call scratch fixed. */
#define DAOS_EXT_GC_PEEK_MAX      4096U

/* Largest row the wave fetch slots read (a DS registry row is the widest). */
#define DAOS_EXT_ROW_BUF          FDB_DS_INFO_ENC_MAX

/* Bytes reserved per listed key of a sequence-keyed object (8-byte keys). */
#define DAOS_EXT_SEQ_KEY_BUF      16U

/* Length of a DS registry dkey ('d' + be32 ds_id). */
#define DAOS_EXT_DS_DKEY_LEN      (1U + DAOS_KEY_BE32_LEN)

_Static_assert(MDS_XATTR_NAME_MAX <= MDS_MAX_NAME, "xattr names must fit daos_key_name");
_Static_assert(DAOS_EXT_ROW_BUF >= FDB_GC_ENC_MAX && DAOS_EXT_ROW_BUF >= FDB_REMOVE_PENDING_ENC_MAX,
               "wave slots must hold every row they read");
_Static_assert(DAOS_EXT_SMALL_FETCH < DAOS_EXT_XATTR_ENC_MAX &&
               DAOS_EXT_SMALL_FETCH < DAOS_STRIPE_MAP_ENC_MAX,
               "the first buffer of an adaptive read is smaller than its layout's bound");
_Static_assert(DAOS_EXT_SEQ_KEY_BUF >= DAOS_KEY_BE64_LEN, "a sequence key is 8 bytes");
_Static_assert(DAOS_EXT_COUNT_MAX <= UINT32_MAX, "counts are reported as uint32_t");
_Static_assert(DAOS_EXT_GC_PEEK_MAX <= UINT32_MAX / DAOS_EXT_SEQ_KEY_BUF, "peek scratch fits");

/* -----------------------------------------------------------------------
 * Small helpers
 * ----------------------------------------------------------------------- */

static struct daos_backend *be_of(const struct mds_catalogue *cat)
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

/* Heap copy of @p val (@p len bytes; NULL allowed when 0) in its stored
 * form; *enc_len receives the stored length.  NULL when out of memory. */
static uint8_t *blob_wrap(const void *val, size_t len, size_t *enc_len)
{
    uint8_t *enc = malloc(len + DAOS_EXT_BLOB_PREFIX);

    if (enc == NULL) {
        return NULL;
    }
    enc[0] = (uint8_t)DAOS_EXT_BLOB_VERSION;
    if (len > 0) {
        memcpy(enc + DAOS_EXT_BLOB_PREFIX, val, len);
    }
    *enc_len = len + DAOS_EXT_BLOB_PREFIX;
    return enc;
}

/* The bytes of a stored value; false for a foreign version byte or a
 * value longer than @p max (a corrupt row). */
static bool blob_unwrap(const uint8_t *enc, size_t enc_len, size_t max, const uint8_t **val,
                        size_t *len)
{
    if (enc_len < DAOS_EXT_BLOB_PREFIX || enc[0] != (uint8_t)DAOS_EXT_BLOB_VERSION ||
        enc_len - DAOS_EXT_BLOB_PREFIX > max) {
        return false;
    }
    *val = enc + DAOS_EXT_BLOB_PREFIX;
    *len = enc_len - DAOS_EXT_BLOB_PREFIX;
    return true;
}

/*
 * Read the single value (@p dkey, @p akey) of @p oh adaptively.  The
 * first fetch goes into the caller's @p first (@p first_cap bytes); a
 * value that does not fit is reported with -DER_REC2BIG and its stored
 * size and is fetched again into a heap buffer of exactly that size
 * (*big, freed by the caller).  Both fetches run under the body's
 * read-only transaction, so the second sees the same epoch and the size
 * cannot have changed.  *val / *len describe the value (in @p first or
 * *big); *found reports its presence.
 *
 * Returns 0; -DER_IO for a value above @p max (larger than its layout
 * allows); -DER_NOMEM; a -DER_* code from DAOS.
 */
static int fetch_adaptive(struct daos_txn *t, daos_handle_t oh, const struct daos_key *dkey,
                          struct daos_key *akey, uint8_t *first, size_t first_cap, size_t max,
                          uint8_t **big, const uint8_t **val, size_t *len, bool *found)
{
    size_t need = 0;
    int rc;

    *big = NULL;
    *val = first;
    *len = 0;
    rc = daos_fetch_single(t, oh, dkey, akey, first, first_cap, &need, found);
    if (rc == 0) {
        *len = need;
        return 0;
    }
    if (rc != -DER_REC2BIG) {
        return rc;
    }
    if (need > max) {
        return -DER_IO;
    }
    if (need <= first_cap) {
        need = max; /* size not reported: the layout's bound is always enough */
    }
    *big = malloc(need);
    if (*big == NULL) {
        return -DER_NOMEM;
    }
    rc = daos_fetch_single(t, oh, dkey, akey, *big, need, len, found);
    if (rc == -DER_REC2BIG) {
        rc = -DER_IO; /* grown under a read-only epoch: cannot happen */
    }
    if (rc != 0) {
        free(*big);
        *big = NULL;
        *len = 0;
        return rc;
    }
    *val = *big;
    return 0;
}

/* -----------------------------------------------------------------------
 * Rows: one single-value akey under one dkey of one object.  Most slots
 * of this unit read, write or clear exactly one row.
 * ----------------------------------------------------------------------- */

struct row_ref {
    enum daos_tag   tag;
    uint64_t        id;        /**< oid.lo of the object. */
    struct daos_key dkey;
    struct daos_key akey;
    bool            akey_row;  /**< The dkey is shared: a clear punches the akey alone. */
};

static void row_init(struct row_ref *r, enum daos_tag tag, uint64_t id, bool akey_row)
{
    memset(r, 0, sizeof(*r));
    r->tag = tag;
    r->id = id;
    r->akey_row = akey_row;
    daos_akey_value(&r->akey);
}

static bool row_ok(const struct row_ref *r)
{
    return daos_key_ok(&r->dkey) && daos_key_ok(&r->akey);
}

/* INODE 'i' / 'd': the inline data. */
static void row_inline(struct row_ref *r, uint64_t fileid)
{
    row_init(r, DAOS_TAG_INODE, fileid, false);
    daos_dkey_inode(&r->dkey, DAOS_INODE_DKEY_INLINE);
    daos_akey_inline(&r->akey);
}

/* INODE 'h' / 'v': the shard routing row. */
static void row_shard(struct row_ref *r, uint64_t fileid)
{
    row_init(r, DAOS_TAG_INODE, fileid, false);
    daos_dkey_inode(&r->dkey, DAOS_INODE_DKEY_SHARD);
}

/* INODE 'E' / name: one cross-shard dirent of the directory @p parent. */
static void row_ext_dirent(struct row_ref *r, uint64_t parent, const char *name)
{
    row_init(r, DAOS_TAG_INODE, parent, true);
    daos_dkey_inode(&r->dkey, DAOS_INODE_DKEY_EXT_DIRENTS);
    daos_akey_name(&r->akey, name);
}

/* DS 'd' + be32 ds_id / 'v': a registry row. */
static void row_ds_info(struct row_ref *r, uint32_t ds_id)
{
    row_init(r, DAOS_TAG_DS, 0, false);
    daos_dkey_ds_info(&r->dkey, ds_id);
}

/* DS 'p' + be32 ds_id / 'v': a provisioning row. */
static void row_ds_provision(struct row_ref *r, uint32_t ds_id)
{
    row_init(r, DAOS_TAG_DS, 0, false);
    daos_dkey_ds_provision(&r->dkey, ds_id);
}

/* QUOTA 'r' + u8 scope_type + be64 scope_id / 'v'. */
static void row_quota_rule(struct row_ref *r, uint8_t scope_type, uint64_t scope_id)
{
    row_init(r, DAOS_TAG_QUOTA, 0, false);
    daos_dkey_quota_rule(&r->dkey, scope_type, scope_id);
}

/* QUOTA 'u' + u8 usage_type + be64 scope_id / 'v'. */
static void row_quota_usage(struct row_ref *r, uint8_t usage_type, uint64_t scope_id)
{
    row_init(r, DAOS_TAG_QUOTA, 0, false);
    daos_dkey_quota_usage(&r->dkey, usage_type, scope_id);
}

/* REMOVE_PENDING uint64 remove_seq / 'v': a manifest row. */
static void row_remove_pending(struct row_ref *r, uint64_t seq)
{
    row_init(r, DAOS_TAG_REMOVE_PENDING, 0, false);
    daos_dkey_u64(&r->dkey, seq);
}

/* LINK_ANCHOR be64 anchor_id / 'v'. */
static void row_link_anchor(struct row_ref *r, uint64_t anchor_id)
{
    row_init(r, DAOS_TAG_LINK_ANCHOR, 0, false);
    daos_dkey_be64(&r->dkey, anchor_id);
}

/* GC be64 gc_seq / 'v': a GC queue row. */
static void row_gc(struct row_ref *r, uint64_t gc_seq)
{
    row_init(r, DAOS_TAG_GC, 0, false);
    daos_dkey_be64(&r->dkey, gc_seq);
}

/* Fetch the row into @p buf (@p cap bytes).  A value larger than its
 * layout's buffer (-DER_REC2BIG) is a corrupt row: -DER_IO. */
static int row_fetch(struct daos_txn *t, struct row_ref *r, uint8_t *buf, size_t cap, size_t *len,
                     bool *found)
{
    daos_handle_t oh;
    int rc;

    rc = daos_txn_obj(t, r->tag, r->id, &oh);
    if (rc != 0) {
        return rc;
    }
    rc = daos_fetch_single(t, oh, &r->dkey, &r->akey, buf, cap, len, found);
    return (rc == -DER_REC2BIG) ? -DER_IO : rc;
}

/* Blind update of the row. */
static int row_update(struct daos_txn *t, struct row_ref *r, const void *val, size_t len)
{
    daos_handle_t oh;
    int rc;

    rc = daos_txn_obj(t, r->tag, r->id, &oh);
    if (rc != 0) {
        return rc;
    }
    return daos_update_single(t, oh, &r->dkey, &r->akey, val, len, 0, 0);
}

/* Punch the row: its akey when the dkey is shared, the whole dkey
 * otherwise.  @p flags is 0 or DAOS_COND_PUNCH. */
static int row_punch(struct daos_txn *t, struct row_ref *r, uint64_t flags)
{
    daos_handle_t oh;
    int rc;

    rc = daos_txn_obj(t, r->tag, r->id, &oh);
    if (rc != 0) {
        return rc;
    }
    if (r->akey_row) {
        return daos_punch_akeys(t, oh, &r->dkey, &r->akey, 1, flags);
    }
    return daos_punch_dkeys(t, oh, &r->dkey, 1, flags);
}

/* -----------------------------------------------------------------------
 * Generic one-row bodies: blind set, bounded get and clear.  Most slots
 * of this unit are one of these.
 * ----------------------------------------------------------------------- */

struct kv_ctx {
    struct row_ref  row;
    const uint8_t  *val;   /**< set: value bytes (caller-owned, immutable). */
    size_t          len;   /**< set: value length / get: length read. */
    uint8_t        *buf;   /**< get: destination. */
    size_t          cap;   /**< get: capacity of buf. */
    bool            found; /**< get: presence. */
    bool            cond;  /**< clear: DAOS_COND_PUNCH (absent -> MDS_ERR_NOTFOUND). */
};

static int body_set(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct kv_ctx *c = arg;
    int rc = row_update(t, &c->row, c->val, c->len);

    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

static int body_get(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct kv_ctx *c = arg;
    int rc = row_fetch(t, &c->row, c->buf, c->cap, &c->len, &c->found);

    if (rc != 0) {
        return rc;
    }
    *st_out = c->found ? MDS_OK : MDS_ERR_NOTFOUND;
    return DAOS_BODY_COMMIT;
}

/* With c->cond the punch is conditional: an absent row fails the call
 * or the commit with -DER_NONEXIST, which the runner reports as
 * MDS_ERR_NOTFOUND with nothing applied. */
static int body_clear(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct kv_ctx *c = arg;
    int rc = row_punch(t, &c->row, c->cond ? (uint64_t)DAOS_COND_PUNCH : 0U);

    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

static enum mds_status run_set(struct daos_backend *b, const char *op, const struct row_ref *r,
                               const uint8_t *val, size_t len)
{
    struct kv_ctx c;

    if (!row_ok(r) || len == 0) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.row = *r;
    c.val = val;
    c.len = len;
    return daos_run_txn(b, DAOS_TXN_MUTATING, op, body_set, &c);
}

/* Read the row into @p buf with one fetch and no transaction; MDS_OK /
 * MDS_ERR_NOTFOUND / the runner's status.  A value longer than @p cap is
 * MDS_ERR_IO (corrupt row). */
static enum mds_status run_get(struct daos_backend *b, const char *op, const struct row_ref *r,
                               uint8_t *buf, size_t cap, size_t *len)
{
    struct kv_ctx c;
    enum mds_status st;

    *len = 0;
    if (!row_ok(r)) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.row = *r;
    c.buf = buf;
    c.cap = cap;
    st = daos_run_txn(b, DAOS_TXN_NONE, op, body_get, &c);
    if (st == MDS_OK) {
        *len = c.len;
    }
    return st;
}

static enum mds_status run_clear(struct daos_backend *b, const char *op, const struct row_ref *r,
                                 bool require_present)
{
    struct kv_ctx c;

    if (!row_ok(r)) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.row = *r;
    c.cond = require_present;
    return daos_run_txn(b, DAOS_TXN_MUTATING, op, body_clear, &c);
}

/* -----------------------------------------------------------------------
 * Inode touch (the inode primitives it builds on are shared with the
 * namespace unit: catalogue_daos_internal.h)
 * ----------------------------------------------------------------------- */

/* One fetch of dkey 'a': the blob plus the counter shard this thread's
 * mutations use (daos_dir_shard_pick), the same dkey hence one RPC. */
struct inode_probe {
    struct mds_inode      ino;
    struct daos_dir_shard shard;        /**< Zero when absent or not a directory. */
    uint32_t              shard_idx;
    bool                  found;
    bool                  shard_present;
};

static int inode_probe_read(struct daos_txn *t, daos_handle_t oh, struct inode_probe *p)
{
    struct daos_inode_req r;
    int rc;

    memset(p, 0, sizeof(*p));
    p->shard_idx = daos_dir_shard_pick(t->b);
    rc = daos_inode_req_init(&r, t->b, (int32_t)p->shard_idx);
    if (rc != 0) {
        return rc;
    }
    rc = daos_inode_read_start(t, oh, &r);
    if (rc == 0) {
        rc = daos_inode_read_finish(t, &r, &p->ino, &p->found);
    }
    if (rc == 0 && p->found && !daos_inode_req_shard(&r, &p->shard, &p->shard_present)) {
        rc = -DER_IO;
    }
    daos_inode_req_fini(&r);
    return rc;
}

/* change += 1, ctime = now: the thread's counter shard of a directory
 * (its mtime stays -- an attribute change does not modify the
 * directory's contents), the blob of a file.  The xattr mutations use
 * it like memdb's inode touch. */
static int inode_touch(struct daos_txn *t, daos_handle_t oh, struct inode_probe *p,
                       struct timespec now)
{
    if (fdb_inode_is_dir(&p->ino)) {
        p->shard.change += 1;
        p->shard.ctime_ns = fdb_ts_to_ns(now);
        return daos_dir_shard_write(t, oh, p->shard_idx, &p->shard);
    }
    p->ino.ctime = now;
    p->ino.change++;
    return daos_inode_write_blob(t, oh, &p->ino);
}

/* -----------------------------------------------------------------------
 * Inline data
 * ----------------------------------------------------------------------- */

struct inline_get_ctx {
    uint64_t  fileid;
    uint8_t  *buf;        /**< Caller's destination. */
    size_t    cap;
    uint8_t  *first;      /**< First fetch buffer: min(cap, MDS_INLINE_DATA_MAX) + prefix. */
    size_t    first_cap;
    size_t    copied;
};

/* The caller's buffer may be shorter than the value: copy what fits
 * (memdb semantics; no caller needs the full length). */
static int inline_get_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct inline_get_ctx *c = arg;
    struct daos_key dk;
    struct daos_key ak;
    daos_handle_t oh;
    uint8_t *big = NULL;
    const uint8_t *enc = NULL;
    const uint8_t *val = NULL;
    size_t enc_len = 0;
    size_t vlen = 0;
    bool found = false;
    int rc;

    c->copied = 0;
    rc = daos_txn_obj(t, DAOS_TAG_INODE, c->fileid, &oh);
    if (rc != 0) {
        return rc;
    }
    daos_dkey_inode(&dk, DAOS_INODE_DKEY_INLINE);
    daos_akey_inline(&ak);
    rc = fetch_adaptive(t, oh, &dk, &ak, c->first, c->first_cap, DAOS_EXT_INLINE_ENC_MAX, &big,
                        &enc, &enc_len, &found);
    if (rc != 0) {
        return rc;
    }
    if (!found) {
        *st_out = MDS_ERR_NOTFOUND;
        return DAOS_BODY_DONE;
    }
    if (!blob_unwrap(enc, enc_len, MDS_INLINE_DATA_MAX, &val, &vlen)) {
        free(big);
        return -DER_IO; /* corrupt row */
    }
    if (vlen > c->cap) {
        vlen = c->cap;
    }
    if (vlen > 0) {
        memcpy(c->buf, val, vlen);
    }
    free(big);
    c->copied = vlen;
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

static enum mds_status daos_inline_get(struct mds_catalogue *cat, uint64_t fileid, void *buf,
                                       uint32_t buflen, uint32_t *outlen)
{
    struct daos_backend *b = be_of(cat);
    struct inline_get_ctx c;
    enum mds_status st;

    if (b == NULL || outlen == NULL || (buflen > 0 && buf == NULL)) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.fileid = fileid;
    c.buf = buf;
    c.cap = buflen;
    /* The caller's capacity sizes the first fetch: a value that fits it
     * is read in one RPC, a longer one (truncated anyway) in two. */
    c.first_cap = (buflen < MDS_INLINE_DATA_MAX ? buflen : MDS_INLINE_DATA_MAX) +
                  DAOS_EXT_BLOB_PREFIX;
    c.first = malloc(c.first_cap);
    if (c.first == NULL) {
        return MDS_ERR_NOMEM;
    }
    st = daos_run_txn(b, DAOS_TXN_READONLY, "inline_get", inline_get_body, &c);
    free(c.first);
    if (st == MDS_OK) {
        *outlen = (uint32_t)c.copied;
    }
    return st;
}

static enum mds_status daos_inline_put(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                       uint64_t fileid, const void *buf, uint32_t len)
{
    struct daos_backend *b = be_of(cat);
    struct row_ref r;
    uint8_t *enc;
    size_t enc_len = 0;
    enum mds_status st;

    (void)txn;
    if (b == NULL || len > MDS_INLINE_DATA_MAX || (len > 0 && buf == NULL)) {
        return MDS_ERR_INVAL;
    }
    enc = blob_wrap(buf, len, &enc_len);
    if (enc == NULL) {
        return MDS_ERR_NOMEM;
    }
    row_inline(&r, fileid);
    st = run_set(b, "inline_put", &r, enc, enc_len);
    free(enc); /* the client copied the value when the update was issued */
    return st;
}

static enum mds_status daos_inline_del(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                       uint64_t fileid)
{
    struct daos_backend *b = be_of(cat);
    struct row_ref r;

    (void)txn;
    if (b == NULL) {
        return MDS_ERR_INVAL;
    }
    row_inline(&r, fileid);
    return run_clear(b, "inline_del", &r, true);
}

/* -----------------------------------------------------------------------
 * Extended attributes
 * ----------------------------------------------------------------------- */

struct xattr_get_ctx {
    uint64_t        fileid;
    struct daos_key ak;
    uint8_t        *first;  /**< DAOS_EXT_SMALL_FETCH bytes. */
    uint8_t        *big;    /**< Exact-size second fetch (heap). */
    const uint8_t  *val;    /**< Into first or big. */
    size_t          vlen;
};

static int xattr_get_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct xattr_get_ctx *c = arg;
    struct daos_key dk;
    daos_handle_t oh;
    const uint8_t *enc = NULL;
    size_t enc_len = 0;
    bool found = false;
    int rc;

    /* A re-run attempt starts clean. */
    free(c->big);
    c->big = NULL;
    c->val = NULL;
    c->vlen = 0;
    rc = daos_txn_obj(t, DAOS_TAG_INODE, c->fileid, &oh);
    if (rc != 0) {
        return rc;
    }
    daos_dkey_inode(&dk, DAOS_INODE_DKEY_XATTRS);
    rc = fetch_adaptive(t, oh, &dk, &c->ak, c->first, DAOS_EXT_SMALL_FETCH, DAOS_EXT_XATTR_ENC_MAX,
                        &c->big, &enc, &enc_len, &found);
    if (rc != 0) {
        return rc;
    }
    if (!found) {
        *st_out = MDS_ERR_NOTFOUND;
        return DAOS_BODY_DONE;
    }
    if (!blob_unwrap(enc, enc_len, MDS_XATTR_VAL_MAX, &c->val, &c->vlen)) {
        return -DER_IO; /* corrupt row */
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

static enum mds_status daos_xattr_get(struct mds_catalogue *cat, uint64_t fileid,
                                      const char *name, void **val, uint32_t *vallen)
{
    struct daos_backend *b = be_of(cat);
    struct xattr_get_ctx c;
    uint8_t *copy;
    enum mds_status st;

    if (b == NULL || name == NULL || val == NULL || vallen == NULL) {
        return MDS_ERR_INVAL;
    }
    if (!name_len_ok(name, MDS_XATTR_NAME_MAX)) {
        return MDS_ERR_NOTFOUND; /* no such name can exist */
    }
    memset(&c, 0, sizeof(c));
    c.fileid = fileid;
    daos_akey_name(&c.ak, name);
    if (!daos_key_ok(&c.ak)) {
        return MDS_ERR_INVAL;
    }
    c.first = malloc(DAOS_EXT_SMALL_FETCH);
    if (c.first == NULL) {
        return MDS_ERR_NOMEM;
    }
    st = daos_run_txn(b, DAOS_TXN_READONLY, "xattr_get", xattr_get_body, &c);
    if (st == MDS_OK) {
        /* A zero-length value still hands back a freeable buffer so the
         * caller's contract (free *val) holds uniformly, as on memdb. */
        copy = malloc(c.vlen > 0 ? c.vlen : 1U);
        if (copy == NULL) {
            st = MDS_ERR_NOMEM;
        } else {
            if (c.vlen > 0) {
                memcpy(copy, c.val, c.vlen);
            }
            *val = copy;
            *vallen = (uint32_t)c.vlen;
        }
    }
    free(c.big);
    free(c.first);
    return st;
}

struct xattr_mut_ctx {
    uint64_t        fileid;
    struct daos_key ak;        /**< The name akey. */
    const uint8_t  *enc;       /**< put: the stored form (caller-owned for the call). */
    size_t          enc_len;
    bool            del;
};

/* put: F blob+shard; U xattr, touch.  del: F blob+shard; CP xattr,
 * touch -- an absent name fails the commit with -DER_NONEXIST
 * (MDS_ERR_NOTFOUND, the touch not applied).  A missing inode is
 * tolerated (the xattr row is still written / cleared), as on memdb. */
static int xattr_mut_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct xattr_mut_ctx *c = arg;
    struct inode_probe p;
    struct daos_key dk;
    daos_handle_t oh;
    int rc;

    rc = daos_txn_obj(t, DAOS_TAG_INODE, c->fileid, &oh);
    if (rc != 0) {
        return rc;
    }
    rc = inode_probe_read(t, oh, &p);
    if (rc != 0) {
        return rc;
    }
    daos_dkey_inode(&dk, DAOS_INODE_DKEY_XATTRS);
    if (c->del) {
        rc = daos_punch_akeys(t, oh, &dk, &c->ak, 1, DAOS_COND_PUNCH);
    } else {
        rc = daos_update_single(t, oh, &dk, &c->ak, c->enc, c->enc_len, 0, 0);
    }
    if (rc != 0) {
        return rc;
    }
    if (p.found) {
        rc = inode_touch(t, oh, &p, now_ts());
        if (rc != 0) {
            return rc;
        }
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

static enum mds_status daos_xattr_put(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                      uint64_t fileid, const char *name, const void *val,
                                      uint32_t vallen)
{
    struct daos_backend *b = be_of(cat);
    struct xattr_mut_ctx c;
    uint8_t *enc;
    enum mds_status st;

    (void)txn;
    if (b == NULL || !name_len_ok(name, MDS_XATTR_NAME_MAX) || vallen > MDS_XATTR_VAL_MAX ||
        (vallen > 0 && val == NULL)) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.fileid = fileid;
    daos_akey_name(&c.ak, name);
    if (!daos_key_ok(&c.ak)) {
        return MDS_ERR_INVAL;
    }
    enc = blob_wrap(val, vallen, &c.enc_len);
    if (enc == NULL) {
        return MDS_ERR_NOMEM;
    }
    c.enc = enc;
    st = daos_run_txn(b, DAOS_TXN_MUTATING, "xattr_put", xattr_mut_body, &c);
    free(enc);
    return st;
}

static enum mds_status daos_xattr_del(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                      uint64_t fileid, const char *name)
{
    struct daos_backend *b = be_of(cat);
    struct xattr_mut_ctx c;

    (void)txn;
    if (b == NULL || name == NULL) {
        return MDS_ERR_INVAL;
    }
    if (!name_len_ok(name, MDS_XATTR_NAME_MAX)) {
        return MDS_ERR_NOTFOUND;
    }
    memset(&c, 0, sizeof(c));
    c.fileid = fileid;
    daos_akey_name(&c.ak, name);
    if (!daos_key_ok(&c.ak)) {
        return MDS_ERR_INVAL;
    }
    c.del = true;
    return daos_run_txn(b, DAOS_TXN_MUTATING, "xattr_del", xattr_mut_body, &c);
}

struct xattr_exists_ctx {
    uint64_t        fileid;
    struct daos_key ak;
    bool            present;
};

/* Presence without the value: a fetch into a one-byte buffer.  An
 * empty xattr (its prefix byte alone) fits; any other value is reported
 * with -DER_REC2BIG, which is exactly "present". */
static int xattr_exists_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct xattr_exists_ctx *c = arg;
    struct daos_key dk;
    daos_handle_t oh;
    uint8_t probe[DAOS_EXT_BLOB_PREFIX];
    size_t len = 0;
    bool found = false;
    int rc;

    c->present = false;
    rc = daos_txn_obj(t, DAOS_TAG_INODE, c->fileid, &oh);
    if (rc != 0) {
        return rc;
    }
    daos_dkey_inode(&dk, DAOS_INODE_DKEY_XATTRS);
    rc = daos_fetch_single(t, oh, &dk, &c->ak, probe, sizeof(probe), &len, &found);
    if (rc == -DER_REC2BIG) {
        c->present = true;
    } else if (rc != 0) {
        return rc;
    } else {
        c->present = found;
    }
    *st_out = c->present ? MDS_OK : MDS_ERR_NOTFOUND;
    return DAOS_BODY_COMMIT;
}

static enum mds_status daos_xattr_exists(struct mds_catalogue *cat, uint64_t fileid,
                                         const char *name)
{
    struct daos_backend *b = be_of(cat);
    struct xattr_exists_ctx c;

    if (b == NULL || name == NULL) {
        return MDS_ERR_INVAL;
    }
    if (!name_len_ok(name, MDS_XATTR_NAME_MAX)) {
        return MDS_ERR_NOTFOUND;
    }
    memset(&c, 0, sizeof(c));
    c.fileid = fileid;
    daos_akey_name(&c.ak, name);
    if (!daos_key_ok(&c.ak)) {
        return MDS_ERR_INVAL;
    }
    return daos_run_txn(b, DAOS_TXN_NONE, "xattr_exists", xattr_exists_body, &c);
}

struct xattr_list_ctx {
    uint64_t          fileid;
    bool              have_after;
    char              after[MDS_XATTR_NAME_MAX + 1]; /**< Last name delivered. */
    char            (*names)[MDS_XATTR_NAME_MAX + 1]; /**< DAOS_EXT_XATTR_PAGE slots. */
    daos_key_desc_t  *kds;
    uint8_t          *keybuf;
    uint32_t          n;
    bool              more;
};

/* One page of 'x' akeys, strictly after the last delivered name, in
 * the store's hash order (the cursor resumes in that same order). */
static int xattr_list_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct xattr_list_ctx *c = arg;
    struct daos_list l;
    struct daos_key dk;
    struct daos_key resume;
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
    daos_dkey_inode(&dk, DAOS_INODE_DKEY_XATTRS);
    if (c->have_after) {
        daos_akey_name(&resume, c->after);
    }
    daos_list_init(&l, c->kds, DAOS_EXT_XATTR_PAGE, c->keybuf,
                   (size_t)DAOS_EXT_XATTR_PAGE * DAOS_KEY_MAX);
    rc = daos_list_akeys(t, oh, &dk, c->have_after ? &resume : NULL, &l);
    if (rc != 0) {
        return rc;
    }
    for (i = 0; i < l.nr; i++) {
        size_t klen = (size_t)l.kds[i].kd_key_len;

        if (klen == 0 || klen > MDS_XATTR_NAME_MAX) {
            return -DER_IO; /* corrupt key */
        }
        memcpy(c->names[c->n], l.buf + off, klen);
        c->names[c->n][klen] = '\0';
        off += klen;
        c->n++;
    }
    c->more = !l.eof;
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

static enum mds_status daos_xattr_list(struct mds_catalogue *cat, uint64_t fileid,
                                       mds_xattr_list_cb cb, void *ctx)
{
    struct daos_backend *b = be_of(cat);
    struct xattr_list_ctx c;
    enum mds_status st = MDS_OK;

    if (b == NULL || cb == NULL) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.fileid = fileid;
    c.names = calloc(DAOS_EXT_XATTR_PAGE, sizeof(*c.names));
    c.kds = calloc(DAOS_EXT_XATTR_PAGE, sizeof(*c.kds));
    c.keybuf = calloc(DAOS_EXT_XATTR_PAGE, DAOS_KEY_MAX);
    if (c.names == NULL || c.kds == NULL || c.keybuf == NULL) {
        st = MDS_ERR_NOMEM;
        goto out;
    }
    for (;;) {
        uint32_t i;

        st = daos_run_txn(b, DAOS_TXN_READONLY, "xattr_list", xattr_list_body, &c);
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
    free(c.kds);
    free(c.keybuf);
    return st;
}

/* -----------------------------------------------------------------------
 * Stripe maps: INODE 's' / 'm' holds header and entries in ONE value
 * (daos_codec.h), written and cleared together with the file's
 * STRIPE_IDX marker (catalogue_daos_internal.h).
 * ----------------------------------------------------------------------- */

struct stripe_get_ctx {
    uint64_t                 fileid;
    uint8_t                 *first;   /**< DAOS_EXT_SMALL_FETCH bytes. */
    uint32_t                 sc;
    uint32_t                 su;
    uint32_t                 mc;
    struct mds_ds_map_entry *entries; /**< Decoded (heap). */
};

static int stripe_get_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct stripe_get_ctx *c = arg;
    struct daos_key dk;
    struct daos_key ak;
    daos_handle_t oh;
    uint8_t *big = NULL;
    const uint8_t *enc = NULL;
    size_t enc_len = 0;
    bool found = false;
    bool ok;
    int rc;

    free(c->entries);
    c->entries = NULL;
    rc = daos_txn_obj(t, DAOS_TAG_INODE, c->fileid, &oh);
    if (rc != 0) {
        return rc;
    }
    daos_dkey_inode(&dk, DAOS_INODE_DKEY_STRIPE);
    daos_akey_stripe_map(&ak);
    rc = fetch_adaptive(t, oh, &dk, &ak, c->first, DAOS_EXT_SMALL_FETCH, DAOS_STRIPE_MAP_ENC_MAX,
                        &big, &enc, &enc_len, &found);
    if (rc != 0) {
        return rc;
    }
    if (!found) {
        *st_out = MDS_ERR_NOTFOUND;
        return DAOS_BODY_DONE;
    }
    /* The decoder reports a malformed value and an allocation failure
     * alike; both end the slot, the former is by far the likelier. */
    ok = daos_stripe_map_decode(enc, enc_len, &c->sc, &c->su, &c->mc, &c->entries);
    free(big);
    if (!ok) {
        return -DER_IO;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

static enum mds_status daos_stripe_map_get(struct mds_catalogue *cat, uint64_t fileid,
                                           uint32_t *sc, uint32_t *su, uint32_t *mc,
                                           struct mds_ds_map_entry **entries)
{
    struct daos_backend *b = be_of(cat);
    struct stripe_get_ctx c;
    enum mds_status st;

    if (entries != NULL) {
        *entries = NULL;
    }
    if (b == NULL) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.fileid = fileid;
    c.first = malloc(DAOS_EXT_SMALL_FETCH);
    if (c.first == NULL) {
        return MDS_ERR_NOMEM;
    }
    st = daos_run_txn(b, DAOS_TXN_READONLY, "stripe_map_get", stripe_get_body, &c);
    free(c.first);
    if (st != MDS_OK) {
        free(c.entries);
        return st;
    }
    if (sc != NULL) {
        *sc = c.sc;
    }
    if (su != NULL) {
        *su = c.su;
    }
    if (mc != NULL) {
        *mc = c.mc;
    }
    if (entries != NULL) {
        *entries = c.entries;
    } else {
        free(c.entries);
    }
    return MDS_OK;
}

struct stripe_put_ctx {
    uint64_t                       fileid;
    uint32_t                       sc;
    uint32_t                       su;
    uint32_t                       mc;
    const struct mds_ds_map_entry *entries;
};

/* A single-value update replaces the whole blob, so a narrower re-put
 * leaves nothing stale; the STRIPE_IDX marker is re-set (idempotent). */
static int stripe_put_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct stripe_put_ctx *c = arg;
    daos_handle_t oh;
    int rc;

    rc = daos_txn_obj(t, DAOS_TAG_INODE, c->fileid, &oh);
    if (rc != 0) {
        return rc;
    }
    rc = daos_stripe_map_write(t, oh, c->fileid, c->sc, c->su, c->mc, c->entries);
    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

static enum mds_status daos_stripe_map_put(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                           uint64_t fileid, uint32_t sc, uint32_t su, uint32_t mc,
                                           const struct mds_ds_map_entry *entries)
{
    struct daos_backend *b = be_of(cat);
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
    c.fileid = fileid;
    c.sc = sc;
    c.su = su;
    c.mc = mc;
    c.entries = entries;
    return daos_run_txn(b, DAOS_TXN_MUTATING, "stripe_map_put", stripe_put_body, &c);
}

struct stripe_del_ctx {
    uint64_t fileid;
};

/* Idempotent: deleting an absent map is MDS_OK (ds_gc repeats it). */
static int stripe_del_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct stripe_del_ctx *c = arg;
    daos_handle_t oh;
    int rc;

    rc = daos_txn_obj(t, DAOS_TAG_INODE, c->fileid, &oh);
    if (rc != 0) {
        return rc;
    }
    rc = daos_stripe_map_clear(t, oh, c->fileid);
    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

static enum mds_status daos_stripe_map_del(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                           uint64_t fileid)
{
    struct daos_backend *b = be_of(cat);
    struct stripe_del_ctx c;

    (void)txn;
    if (b == NULL) {
        return MDS_ERR_INVAL;
    }
    c.fileid = fileid;
    return daos_run_txn(b, DAOS_TXN_MUTATING, "stripe_map_del", stripe_del_body, &c);
}

/* One file of a stripe_map_scan page. */
struct scan_file {
    uint64_t                 fileid;
    daos_handle_t            oh;       /**< Untracked INODE handle, closed per page. */
    struct daos_fetch_small  f;
    struct daos_key          ak;
    uint8_t                 *buf;      /**< This file's slice of the page's first buffers. */
    uint8_t                 *big;      /**< Heap: a map larger than the slice (second wave). */
    size_t                   len;      /**< Stored size; 0 = the map is gone. */
    uint32_t                 sc;
    uint32_t                 su;
    uint32_t                 mc;
    struct mds_ds_map_entry *entries;  /**< Decoded (heap). */
    bool                     started;
    bool                     oversize; /**< First wave: -DER_REC2BIG. */
};

struct stripe_scan_ctx {
    bool              have_cursor;
    uint64_t          cursor;   /**< Last fileid listed by the previous page. */
    struct scan_file *files;    /**< DAOS_EXT_SCAN_PAGE slots. */
    uint8_t          *bufs;     /**< DAOS_EXT_SCAN_PAGE x DAOS_EXT_SMALL_FETCH. */
    daos_key_desc_t  *kds;
    uint8_t          *keybuf;
    uint32_t          n;
    bool              more;
};

static void scan_page_reset(struct stripe_scan_ctx *c)
{
    uint32_t i;

    for (i = 0; i < DAOS_EXT_SCAN_PAGE; i++) {
        struct scan_file *sf = &c->files[i];

        free(sf->big);
        free(sf->entries);
        memset(sf, 0, sizeof(*sf));
        sf->oh = DAOS_HDL_INVAL;
        sf->buf = c->bufs + (size_t)i * DAOS_EXT_SMALL_FETCH;
    }
    c->n = 0;
    c->more = false;
}

/* Phase 1: the next page of STRIPE_IDX dkeys (fileids with a map). */
static int scan_collect(struct daos_txn *t, struct stripe_scan_ctx *c)
{
    struct daos_list l;
    struct daos_key resume;
    daos_handle_t oh;
    size_t off = 0;
    uint32_t i;
    int rc;

    rc = daos_txn_obj(t, DAOS_TAG_STRIPE_IDX, 0, &oh);
    if (rc != 0) {
        return rc;
    }
    if (c->have_cursor) {
        daos_dkey_be64(&resume, c->cursor);
    }
    daos_list_init(&l, c->kds, DAOS_EXT_SCAN_PAGE, c->keybuf,
                   (size_t)DAOS_EXT_SCAN_PAGE * DAOS_KEY_MAX);
    rc = daos_list_dkeys(t, oh, c->have_cursor ? &resume : NULL, &l);
    if (rc != 0) {
        return rc;
    }
    for (i = 0; i < l.nr; i++) {
        size_t klen = (size_t)l.kds[i].kd_key_len;

        if (klen != DAOS_KEY_BE64_LEN) {
            return -DER_IO; /* corrupt index key */
        }
        c->files[i].fileid = fdb_get_u64(l.buf + off);
        off += klen;
        c->n++;
    }
    c->more = !l.eof;
    return 0;
}

/* Start the fetch of one file's 's'/'m' blob into @p dst (@p cap bytes). */
static int scan_fetch_start(struct daos_txn *t, struct scan_file *sf, uint8_t *dst, size_t cap)
{
    struct daos_key dk;
    int rc;

    daos_dkey_inode(&dk, DAOS_INODE_DKEY_STRIPE);
    daos_fetch_small_init(&sf->f, &dk);
    daos_akey_stripe_map(&sf->ak);
    if (!daos_fetch_add(&sf->f.f, &sf->ak, dst, cap)) {
        return -DER_INVAL;
    }
    rc = daos_fetch_start(t, sf->oh, &sf->f.f);
    sf->started = (rc == 0);
    return rc;
}

/* Phase 2: every file's map into its first buffer, one wave.  A map
 * larger than the buffer comes back as -DER_REC2BIG with its stored
 * size and is left for the second wave; a size above the layout's bound
 * is a corrupt row.  Every started fetch is finished, even after an
 * error, so none is left in flight; the first error wins. */
static int scan_wave_first(struct daos_txn *t, struct stripe_scan_ctx *c)
{
    uint32_t i;
    int rc = 0;

    for (i = 0; i < c->n && rc == 0; i++) {
        struct scan_file *sf = &c->files[i];

        rc = daos_obj_open_tag(t->b, DAOS_TAG_INODE, sf->fileid, &sf->oh);
        if (rc == 0) {
            rc = scan_fetch_start(t, sf, sf->buf, DAOS_EXT_SMALL_FETCH);
        }
    }
    for (i = 0; i < c->n; i++) {
        struct scan_file *sf = &c->files[i];
        int rc2;

        if (!sf->started) {
            continue;
        }
        rc2 = daos_fetch_finish(t, &sf->f.f);
        sf->started = false;
        sf->len = daos_fetch_size(&sf->f.f, 0);
        if (rc2 == -DER_REC2BIG) {
            sf->oversize = true;
            rc2 = 0;
            if (sf->len > DAOS_STRIPE_MAP_ENC_MAX) {
                rc2 = -DER_IO;
            } else if (sf->len <= DAOS_EXT_SMALL_FETCH) {
                sf->len = DAOS_STRIPE_MAP_ENC_MAX; /* size not reported: the bound */
            }
        }
        if (rc == 0) {
            rc = rc2;
        }
    }
    return rc;
}

/* Phase 3: the oversized maps again, into buffers of their reported
 * size (the read-only epoch guarantees it), one wave. */
static int scan_wave_second(struct daos_txn *t, struct stripe_scan_ctx *c)
{
    uint32_t i;
    int rc = 0;

    for (i = 0; i < c->n && rc == 0; i++) {
        struct scan_file *sf = &c->files[i];

        if (!sf->oversize) {
            continue;
        }
        sf->big = malloc(sf->len);
        if (sf->big == NULL) {
            rc = -DER_NOMEM;
            break;
        }
        rc = scan_fetch_start(t, sf, sf->big, sf->len);
    }
    for (i = 0; i < c->n; i++) {
        struct scan_file *sf = &c->files[i];
        int rc2;

        if (!sf->started) {
            continue;
        }
        rc2 = daos_fetch_finish(t, &sf->f.f);
        sf->started = false;
        if (rc2 == -DER_REC2BIG) {
            rc2 = -DER_IO; /* grown under a read-only epoch: cannot happen */
        }
        if (rc2 == 0) {
            sf->len = daos_fetch_size(&sf->f.f, 0);
        }
        if (rc == 0) {
            rc = rc2;
        }
    }
    return rc;
}

static void scan_close(struct stripe_scan_ctx *c)
{
    uint32_t i;

    for (i = 0; i < c->n; i++) {
        daos_obj_close_h(&c->files[i].oh);
    }
}

/* Phase 4: decode every map that exists.  A fileid whose map vanished
 * (len 0) is skipped: marker and blob are written and cleared in one
 * transaction and this is one snapshot, so only a foreign writer could
 * produce one. */
static int scan_decode(struct stripe_scan_ctx *c)
{
    uint32_t i;

    for (i = 0; i < c->n; i++) {
        struct scan_file *sf = &c->files[i];
        const uint8_t *src = sf->big != NULL ? sf->big : sf->buf;

        if (sf->len == 0) {
            continue;
        }
        if (!daos_stripe_map_decode(src, sf->len, &sf->sc, &sf->su, &sf->mc, &sf->entries)) {
            return -DER_IO; /* corrupt blob */
        }
        free(sf->big);
        sf->big = NULL;
    }
    return 0;
}

static int stripe_scan_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct stripe_scan_ctx *c = arg;
    int rc;

    scan_page_reset(c);
    rc = scan_collect(t, c);
    if (rc == 0 && c->n > 0) {
        rc = scan_wave_first(t, c);
    }
    if (rc == 0 && c->n > 0) {
        rc = scan_wave_second(t, c);
    }
    scan_close(c);
    if (rc == 0) {
        rc = scan_decode(c);
    }
    if (rc != 0) {
        return rc;
    }
    if (c->n > 0) {
        c->cursor = c->files[c->n - 1].fileid;
        c->have_cursor = true;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

static enum mds_status daos_stripe_map_scan(struct mds_catalogue *cat,
                                            mds_cat_stripe_map_scan_cb cb, void *ctx)
{
    struct daos_backend *b = be_of(cat);
    struct stripe_scan_ctx c;
    enum mds_status st = MDS_OK;

    if (b == NULL || cb == NULL) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.files = calloc(DAOS_EXT_SCAN_PAGE, sizeof(*c.files));
    c.bufs = calloc(DAOS_EXT_SCAN_PAGE, DAOS_EXT_SMALL_FETCH);
    c.kds = calloc(DAOS_EXT_SCAN_PAGE, sizeof(*c.kds));
    c.keybuf = calloc(DAOS_EXT_SCAN_PAGE, DAOS_KEY_MAX);
    if (c.files == NULL || c.bufs == NULL || c.kds == NULL || c.keybuf == NULL) {
        st = MDS_ERR_NOMEM;
        goto out;
    }
    for (;;) {
        uint32_t i;

        st = daos_run_txn(b, DAOS_TXN_READONLY, "stripe_map_scan", stripe_scan_body, &c);
        if (st != MDS_OK) {
            break;
        }
        for (i = 0; i < c.n; i++) {
            const struct scan_file *sf = &c.files[i];

            if (sf->len > 0 && cb(sf->fileid, sf->sc, sf->su, sf->mc, sf->entries, ctx) != 0) {
                goto out;
            }
        }
        if (!c.more || c.n == 0) {
            break;
        }
    }
out:
    if (c.files != NULL && c.bufs != NULL) {
        scan_page_reset(&c);
    }
    free(c.files);
    free(c.bufs);
    free(c.kds);
    free(c.keybuf);
    return st;
}

/* -----------------------------------------------------------------------
 * Row waves: the 'v' values of N dkeys of one object, DAOS_EXT_WAVE
 * parallel fetches at a time (a dkey is one fetch; a listing hands out
 * keys only).  Shared by ds_list, the GC peeks and the queue walks.
 * ----------------------------------------------------------------------- */

struct row_fetch {
    struct daos_fetch_small f;
    struct daos_key         dk;
    struct daos_key         ak;
    uint8_t                 buf[DAOS_EXT_ROW_BUF];
    bool                    started;
};

/** Name item @p i's dkey. */
typedef void (*row_key_fn)(void *arg, uint32_t i, struct daos_key *dk);

/** Consume item @p i's value (@p len 0 = absent); 0, or -DER_IO for a
 *  corrupt row. */
typedef int (*row_consume_fn)(void *arg, uint32_t i, const uint8_t *val, size_t len);

static int row_fetch_start(struct daos_txn *t, daos_handle_t oh, struct row_fetch *w)
{
    int rc;

    daos_fetch_small_init(&w->f, &w->dk);
    daos_akey_value(&w->ak);
    if (!daos_fetch_add(&w->f.f, &w->ak, w->buf, sizeof(w->buf))) {
        return -DER_INVAL;
    }
    rc = daos_fetch_start(t, oh, &w->f.f);
    w->started = (rc == 0);
    return rc;
}

/* Finish every started slot of a wave; the first error wins and a row
 * larger than every layout (-DER_REC2BIG) is corrupt. */
static int row_wave_finish(struct daos_txn *t, struct row_fetch *wave, uint32_t cnt)
{
    uint32_t i;
    int rc = 0;

    for (i = 0; i < cnt; i++) {
        int rc2;

        if (!wave[i].started) {
            continue;
        }
        rc2 = daos_fetch_finish(t, &wave[i].f.f);
        wave[i].started = false;
        if (rc2 == -DER_REC2BIG) {
            rc2 = -DER_IO;
        }
        if (rc == 0) {
            rc = rc2;
        }
    }
    return rc;
}

/* Items [0, n): @p key names item i's dkey, @p consume receives its
 * value, in item order, once its wave completed. */
static int row_wave_run(struct daos_txn *t, daos_handle_t oh, struct row_fetch *wave, uint32_t n,
                        row_key_fn key, row_consume_fn consume, void *arg)
{
    uint32_t done = 0;

    while (done < n) {
        uint32_t cnt = n - done;
        uint32_t i;
        int rc = 0;

        if (cnt > DAOS_EXT_WAVE) {
            cnt = DAOS_EXT_WAVE;
        }
        for (i = 0; i < cnt && rc == 0; i++) {
            key(arg, done + i, &wave[i].dk);
            rc = row_fetch_start(t, oh, &wave[i]);
        }
        {
            int rc2 = row_wave_finish(t, wave, cnt);

            if (rc == 0) {
                rc = rc2;
            }
        }
        if (rc != 0) {
            return rc;
        }
        for (i = 0; i < cnt; i++) {
            rc = consume(arg, done + i, wave[i].buf, daos_fetch_size(&wave[i].f.f, 0));
            if (rc != 0) {
                return rc;
            }
        }
        done += cnt;
    }
    return 0;
}

/* -----------------------------------------------------------------------
 * DS registry and provisioning
 * ----------------------------------------------------------------------- */

static enum mds_status daos_ds_get(struct mds_catalogue *cat, uint32_t ds_id,
                                   struct mds_ds_info *info)
{
    struct daos_backend *b = be_of(cat);
    uint8_t buf[FDB_DS_INFO_ENC_MAX];
    size_t len = 0;
    struct row_ref r;
    enum mds_status st;

    if (b == NULL || info == NULL) {
        return MDS_ERR_INVAL;
    }
    row_ds_info(&r, ds_id);
    st = run_get(b, "ds_get", &r, buf, sizeof(buf), &len);
    if (st != MDS_OK) {
        return st;
    }
    return fdb_ds_info_decode(buf, len, info) ? MDS_OK : MDS_ERR_IO;
}

static enum mds_status daos_ds_put(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                   const struct mds_ds_info *info)
{
    struct daos_backend *b = be_of(cat);
    uint8_t enc[FDB_DS_INFO_ENC_MAX];
    size_t len = 0;
    struct row_ref r;

    (void)txn;
    if (b == NULL || info == NULL || info->ds_id >= MDS_MAX_DS_NODES ||
        !fdb_ds_info_encode(info, enc, sizeof(enc), &len)) {
        return MDS_ERR_INVAL;
    }
    row_ds_info(&r, info->ds_id);
    return run_set(b, "ds_put", &r, enc, len);
}

static enum mds_status daos_ds_del(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                   uint32_t ds_id)
{
    struct daos_backend *b = be_of(cat);
    struct row_ref r;

    (void)txn;
    if (b == NULL) {
        return MDS_ERR_INVAL;
    }
    row_ds_info(&r, ds_id);
    return run_clear(b, "ds_del", &r, true);
}

struct ds_list_ctx {
    struct mds_ds_info *list;    /**< Heap, handed to the caller. */
    uint32_t            count;
    uint32_t           *ids;     /**< MDS_MAX_DS_NODES + 1 slots. */
    daos_key_desc_t    *kds;
    uint8_t            *keybuf;
    struct row_fetch   *wave;
};

/* The 'd' + be32 dkeys of the DS object in ds_id order (lexically they
 * precede the 'p' range; the bare prefix 'd' is the resume key, strictly
 * before every registry key).  One row more than the bound is accepted
 * so a registry beyond MDS_MAX_DS_NODES (only a writer bypassing
 * ds_put's id check could produce it) is reported as corrupt rather
 * than silently truncated. */
static int ds_list_collect(struct daos_txn *t, daos_handle_t oh, struct ds_list_ctx *c,
                           uint32_t *n_out)
{
    struct daos_list l;
    struct daos_key resume;
    uint32_t n = 0;
    bool at_end = false;
    int rc;

    daos_key_init(&resume);
    daos_key_u8(&resume, (uint8_t)'d');
    while (!at_end) {
        size_t off = 0;
        uint32_t i;

        daos_list_init(&l, c->kds, MDS_MAX_DS_NODES + 1U - n, c->keybuf,
                       (size_t)(MDS_MAX_DS_NODES + 1U) * DAOS_KEY_MAX);
        rc = daos_list_dkeys(t, oh, &resume, &l);
        if (rc != 0) {
            return rc;
        }
        for (i = 0; i < l.nr; i++) {
            const uint8_t *key = l.buf + off;
            size_t klen = (size_t)l.kds[i].kd_key_len;

            if (klen != DAOS_EXT_DS_DKEY_LEN || key[0] != (uint8_t)'d') {
                at_end = true; /* the provisioning range starts here */
                break;
            }
            if (n >= MDS_MAX_DS_NODES) {
                return -DER_IO;
            }
            c->ids[n++] = fdb_get_u32(key + 1);
            daos_key_init(&resume);
            daos_key_bytes(&resume, key, klen);
            off += klen;
        }
        if (l.eof || l.nr == 0) {
            at_end = true;
        }
    }
    *n_out = n;
    return 0;
}

static void ds_row_key(void *arg, uint32_t i, struct daos_key *dk)
{
    struct ds_list_ctx *c = arg;

    daos_dkey_ds_info(dk, c->ids[i]);
}

/* A listed dkey without its row cannot exist (ds_del punches the dkey). */
static int ds_row_consume(void *arg, uint32_t i, const uint8_t *val, size_t len)
{
    struct ds_list_ctx *c = arg;

    if (len == 0 || !fdb_ds_info_decode(val, len, &c->list[i])) {
        return -DER_IO;
    }
    return 0;
}

static int ds_list_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct ds_list_ctx *c = arg;
    daos_handle_t oh;
    uint32_t n = 0;
    int rc;

    free(c->list);
    c->list = NULL;
    c->count = 0;
    rc = daos_txn_obj(t, DAOS_TAG_DS, 0, &oh);
    if (rc != 0) {
        return rc;
    }
    rc = ds_list_collect(t, oh, c, &n);
    if (rc != 0) {
        return rc;
    }
    /* A non-NULL array even when empty (memdb hands one out too). */
    c->list = calloc(n > 0 ? n : 1U, sizeof(*c->list));
    if (c->list == NULL) {
        *st_out = MDS_ERR_NOMEM;
        return DAOS_BODY_DONE;
    }
    rc = row_wave_run(t, oh, c->wave, n, ds_row_key, ds_row_consume, c);
    if (rc != 0) {
        return rc;
    }
    c->count = n;
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

static enum mds_status daos_ds_list(struct mds_catalogue *cat, struct mds_ds_info **list,
                                    uint32_t *count)
{
    struct daos_backend *b = be_of(cat);
    struct ds_list_ctx c;
    enum mds_status st;

    if (b == NULL || list == NULL || count == NULL) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.ids = calloc(MDS_MAX_DS_NODES + 1U, sizeof(*c.ids));
    c.kds = calloc(MDS_MAX_DS_NODES + 1U, sizeof(*c.kds));
    c.keybuf = calloc(MDS_MAX_DS_NODES + 1U, DAOS_KEY_MAX);
    c.wave = calloc(DAOS_EXT_WAVE, sizeof(*c.wave));
    if (c.ids == NULL || c.kds == NULL || c.keybuf == NULL || c.wave == NULL) {
        st = MDS_ERR_NOMEM;
        goto out;
    }
    st = daos_run_txn(b, DAOS_TXN_READONLY, "ds_list", ds_list_body, &c);
    if (st == MDS_OK) {
        *list = c.list;
        *count = c.count;
        c.list = NULL;
    }
out:
    free(c.list);
    free(c.ids);
    free(c.kds);
    free(c.keybuf);
    free(c.wave);
    return st;
}

static enum mds_status daos_ds_provision_get(struct mds_catalogue *cat, uint32_t ds_id,
                                             uint8_t *secret, uint32_t secret_len,
                                             uint64_t *epoch)
{
    struct daos_backend *b = be_of(cat);
    struct fdb_ds_provision_val v;
    uint8_t buf[FDB_DS_PROVISION_ENC_MAX];
    size_t len = 0;
    struct row_ref r;
    uint32_t copy;
    enum mds_status st;

    if (b == NULL || (secret_len > 0 && secret == NULL) || epoch == NULL) {
        return MDS_ERR_INVAL;
    }
    row_ds_provision(&r, ds_id);
    st = run_get(b, "ds_provision_get", &r, buf, sizeof(buf), &len);
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

static enum mds_status daos_ds_provision_put(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                             uint32_t ds_id, const uint8_t *secret,
                                             uint32_t secret_len, uint64_t epoch)
{
    struct daos_backend *b = be_of(cat);
    struct fdb_ds_provision_val v;
    uint8_t enc[FDB_DS_PROVISION_ENC_MAX];
    size_t len = 0;
    struct row_ref r;

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
    row_ds_provision(&r, ds_id);
    return run_set(b, "ds_provision_put", &r, enc, len);
}

static enum mds_status daos_ds_provision_del(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                             uint32_t ds_id)
{
    struct daos_backend *b = be_of(cat);
    struct row_ref r;

    (void)txn;
    if (b == NULL) {
        return MDS_ERR_INVAL;
    }
    row_ds_provision(&r, ds_id);
    return run_clear(b, "ds_provision_del", &r, true);
}

/* -----------------------------------------------------------------------
 * Quota rules and usage (keyed scope_type + scope_id; absolute upserts)
 * ----------------------------------------------------------------------- */

static enum mds_status daos_quota_rule_get(struct mds_catalogue *cat, uint8_t scope_type,
                                           uint64_t scope_id, struct mds_quota_rule *rule)
{
    struct daos_backend *b = be_of(cat);
    uint8_t buf[FDB_QUOTA_RULE_ENC_SIZE];
    size_t len = 0;
    struct row_ref r;
    enum mds_status st;

    if (b == NULL || rule == NULL) {
        return MDS_ERR_INVAL;
    }
    row_quota_rule(&r, scope_type, scope_id);
    st = run_get(b, "quota_rule_get", &r, buf, sizeof(buf), &len);
    if (st != MDS_OK) {
        return st;
    }
    return fdb_quota_rule_decode(buf, len, rule) ? MDS_OK : MDS_ERR_IO;
}

static enum mds_status daos_quota_rule_put(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                           uint8_t scope_type, uint64_t scope_id,
                                           const struct mds_quota_rule *rule)
{
    struct daos_backend *b = be_of(cat);
    uint8_t enc[FDB_QUOTA_RULE_ENC_SIZE];
    size_t len = 0;
    struct row_ref r;

    (void)txn;
    if (b == NULL || rule == NULL || !fdb_quota_rule_encode(rule, enc, sizeof(enc), &len)) {
        return MDS_ERR_INVAL;
    }
    row_quota_rule(&r, scope_type, scope_id);
    return run_set(b, "quota_rule_put", &r, enc, len);
}

static enum mds_status daos_quota_usage_get(struct mds_catalogue *cat, uint8_t usage_type,
                                            uint64_t scope_id, struct mds_quota_usage *usage)
{
    struct daos_backend *b = be_of(cat);
    uint8_t buf[FDB_QUOTA_USAGE_ENC_SIZE];
    size_t len = 0;
    struct row_ref r;
    enum mds_status st;

    if (b == NULL || usage == NULL) {
        return MDS_ERR_INVAL;
    }
    row_quota_usage(&r, usage_type, scope_id);
    st = run_get(b, "quota_usage_get", &r, buf, sizeof(buf), &len);
    if (st != MDS_OK) {
        return st;
    }
    return fdb_quota_usage_decode(buf, len, usage) ? MDS_OK : MDS_ERR_IO;
}

static enum mds_status daos_quota_usage_put(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                            uint8_t usage_type, uint64_t scope_id,
                                            const struct mds_quota_usage *usage)
{
    struct daos_backend *b = be_of(cat);
    uint8_t enc[FDB_QUOTA_USAGE_ENC_SIZE];
    size_t len = 0;
    struct row_ref r;

    (void)txn;
    if (b == NULL || usage == NULL || !fdb_quota_usage_encode(usage, enc, sizeof(enc), &len)) {
        return MDS_ERR_INVAL;
    }
    row_quota_usage(&r, usage_type, scope_id);
    return run_set(b, "quota_usage_put", &r, enc, len);
}

/* -----------------------------------------------------------------------
 * Sequence-keyed queues (the manifest, one owner's GC index, the GC
 * rows): a paged, read-only walk over the dkeys of one object in the
 * object's enumeration order -- numeric for the uint64-keyed queues,
 * the stable hash order of the GC spread object.  Every key is handed
 * to a visitor, with its row when the walk fetches rows; the visitor
 * filters, copies out and says when the caller's capacity is reached.
 * A page is one transaction; a retried page re-runs from the position
 * saved at its start, so nothing is counted or copied twice.
 * ----------------------------------------------------------------------- */

enum queue_keys {
    QUEUE_KEYS_U64,  /**< Native uint64 dkeys (GC_BY_OWNER, REMOVE_PENDING). */
    QUEUE_KEYS_BE64, /**< be64 dkeys (GC). */
};

struct queue_pos {
    bool     have_cursor;
    uint64_t cursor;    /**< Last examined sequence. */
    uint64_t examined;
    uint32_t n;         /**< Rows accepted by the visitor so far. */
};

struct queue_ctx;

/** 0 = continue, 1 = capacity reached (stop), < 0 = corrupt row.
 *  @p val is NULL (len 0) on a keys-only walk. */
typedef int (*queue_visit_fn)(struct queue_ctx *c, uint64_t seq, const uint8_t *val,
                              size_t vlen);

struct queue_ctx {
    enum daos_tag     tag;
    uint64_t          id;
    enum queue_keys   keys;
    uint32_t          page;        /**< Keys per listing. */
    bool              fetch_rows;  /**< Fetch every listed key's 'v' row. */
    struct queue_pos  start;       /**< Position at the start of the page. */
    struct queue_pos  cur;         /**< Position reached by the page. */
    bool              more;
    bool              stop;
    queue_visit_fn    visit;
    void             *arg;
    uint64_t         *seqs;        /**< page slots: the listed keys. */
    daos_key_desc_t  *kds;
    uint8_t          *keybuf;      /**< page x DAOS_EXT_SEQ_KEY_BUF. */
    struct row_fetch *wave;        /**< DAOS_EXT_WAVE slots when fetch_rows. */
    uint32_t          nkeys;       /**< Keys listed by the current page. */
};

static bool queue_key_parse(const struct queue_ctx *c, const uint8_t *key, size_t len,
                            uint64_t *seq)
{
    if (c->keys == QUEUE_KEYS_U64) {
        return daos_dkey_u64_parse(key, len, seq);
    }
    if (len != DAOS_KEY_BE64_LEN) {
        return false;
    }
    *seq = fdb_get_u64(key);
    return true;
}

static void queue_key_build(const struct queue_ctx *c, uint64_t seq, struct daos_key *k)
{
    if (c->keys == QUEUE_KEYS_U64) {
        daos_dkey_u64(k, seq);
    } else {
        daos_dkey_be64(k, seq);
    }
}

static void queue_free(struct queue_ctx *c)
{
    free(c->seqs);
    free(c->kds);
    free(c->keybuf);
    free(c->wave);
    c->seqs = NULL;
    c->kds = NULL;
    c->keybuf = NULL;
    c->wave = NULL;
}

static enum mds_status queue_init(struct queue_ctx *c, enum daos_tag tag, uint64_t id,
                                  enum queue_keys keys, uint32_t page, bool fetch_rows,
                                  queue_visit_fn visit, void *arg)
{
    memset(c, 0, sizeof(*c));
    c->tag = tag;
    c->id = id;
    c->keys = keys;
    c->page = page;
    c->fetch_rows = fetch_rows;
    c->visit = visit;
    c->arg = arg;
    c->seqs = calloc(page, sizeof(*c->seqs));
    c->kds = calloc(page, sizeof(*c->kds));
    c->keybuf = calloc(page, DAOS_EXT_SEQ_KEY_BUF);
    if (fetch_rows) {
        c->wave = calloc(DAOS_EXT_WAVE, sizeof(*c->wave));
    }
    if (c->seqs == NULL || c->kds == NULL || c->keybuf == NULL ||
        (fetch_rows && c->wave == NULL)) {
        queue_free(c);
        return MDS_ERR_NOMEM;
    }
    return MDS_OK;
}

/* List the next page of keys, strictly after the cursor, into c->seqs. */
static int queue_collect(struct daos_txn *t, daos_handle_t oh, struct queue_ctx *c)
{
    struct daos_list l;
    struct daos_key resume;
    size_t off = 0;
    uint32_t i;
    int rc;

    if (c->cur.have_cursor) {
        queue_key_build(c, c->cur.cursor, &resume);
    }
    daos_list_init(&l, c->kds, c->page, c->keybuf, (size_t)c->page * DAOS_EXT_SEQ_KEY_BUF);
    rc = daos_list_dkeys(t, oh, c->cur.have_cursor ? &resume : NULL, &l);
    if (rc != 0) {
        return rc;
    }
    for (i = 0; i < l.nr; i++) {
        size_t klen = (size_t)l.kds[i].kd_key_len;

        if (!queue_key_parse(c, l.buf + off, klen, &c->seqs[i])) {
            return -DER_IO; /* corrupt key */
        }
        off += klen;
    }
    c->nkeys = l.nr;
    c->more = !l.eof;
    return 0;
}

/* Hand one key (and its row) to the visitor and advance the cursor
 * past it.  After a stop the rest of the page stays unexamined, so the
 * next page re-lists it from the cursor. */
static int queue_visit_one(struct queue_ctx *c, uint64_t seq, const uint8_t *val, size_t vlen)
{
    int rc;

    if (c->stop) {
        return 0;
    }
    rc = c->visit(c, seq, val, vlen);
    if (rc < 0) {
        return -DER_IO; /* corrupt row */
    }
    c->cur.examined++;
    c->cur.cursor = seq;
    c->cur.have_cursor = true;
    if (rc > 0) {
        c->stop = true;
    }
    return 0;
}

static void queue_row_key(void *arg, uint32_t i, struct daos_key *dk)
{
    struct queue_ctx *c = arg;

    queue_key_build(c, c->seqs[i], dk);
}

/* A key whose row is absent is skipped but still examined (the cursor
 * moves past it): key and row are written and cleared in one
 * transaction and this is one snapshot, so only a foreign writer could
 * produce one. */
static int queue_row_consume(void *arg, uint32_t i, const uint8_t *val, size_t len)
{
    struct queue_ctx *c = arg;

    if (len == 0) {
        if (!c->stop) {
            c->cur.examined++;
            c->cur.cursor = c->seqs[i];
            c->cur.have_cursor = true;
        }
        return 0;
    }
    return queue_visit_one(c, c->seqs[i], val, len);
}

static int queue_page_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct queue_ctx *c = arg;
    daos_handle_t oh;
    uint32_t i;
    int rc;

    c->cur = c->start;
    c->more = false;
    c->stop = false;
    c->nkeys = 0;
    rc = daos_txn_obj(t, c->tag, c->id, &oh);
    if (rc != 0) {
        return rc;
    }
    rc = queue_collect(t, oh, c);
    if (rc != 0) {
        return rc;
    }
    if (c->fetch_rows) {
        rc = row_wave_run(t, oh, c->wave, c->nkeys, queue_row_key, queue_row_consume, c);
    } else {
        for (i = 0; i < c->nkeys && rc == 0; i++) {
            rc = queue_visit_one(c, c->seqs[i], NULL, 0);
        }
    }
    if (rc != 0) {
        return rc;
    }
    c->more = c->more || c->stop;
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

/* Walk pages until the visitor stops, the object's keys end or
 * @p examine_cap keys were examined. */
static enum mds_status queue_walk(struct daos_backend *b, struct queue_ctx *c, const char *op,
                                  uint64_t examine_cap)
{
    for (;;) {
        enum mds_status st = daos_run_txn(b, DAOS_TXN_READONLY, op, queue_page_body, c);

        if (st != MDS_OK) {
            return st;
        }
        c->start = c->cur;
        if (c->stop || !c->more || c->cur.examined >= examine_cap) {
            return MDS_OK;
        }
    }
}

/* -----------------------------------------------------------------------
 * GC queue
 *
 * Two views (header comment, "GC visibility").  An MDS with an id
 * drains its own rows and the legacy rows (owner 0) and reaches them
 * through GC_BY_OWNER; an MDS with id 0 sees every row and lists the GC
 * object itself.  Both fetch the rows in waves.
 * ----------------------------------------------------------------------- */

struct gc_enqueue_ctx {
    struct mds_gc_entry e;
};

static int gc_enqueue_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct gc_enqueue_ctx *c = arg;
    int rc = daos_gc_row_set(t, &c->e);

    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

static enum mds_status daos_gc_enqueue(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                       uint64_t fileid, uint32_t ds_id, const uint8_t *nfs_fh,
                                       uint32_t fh_len, uint32_t sweep_hint)
{
    struct daos_backend *b = be_of(cat);
    struct gc_enqueue_ctx c;
    uint64_t seq = 0;
    enum mds_status st;

    (void)txn;
    if (b == NULL || fh_len > MDS_NFS_FH_MAX || (fh_len > 0 && nfs_fh == NULL)) {
        return MDS_ERR_INVAL;
    }
    /* The sequence is minted once per call: a retried attempt rewrites
     * the same row and the same index marker. */
    st = daos_backend_alloc_id(b, DAOS_META_GC_SEQ, &seq);
    if (st != MDS_OK) {
        return st;
    }
    memset(&c, 0, sizeof(c));
    c.e.gc_seq = seq;
    c.e.fileid = fileid;
    c.e.ds_id = ds_id;
    c.e.owner_mds_id = b->mds_id;
    c.e.sweep_hint = sweep_hint;
    c.e.nfs_fh_len = fh_len;
    if (fh_len > 0) {
        memcpy(c.e.nfs_fh, nfs_fh, fh_len);
    }
    return daos_run_txn(b, DAOS_TXN_MUTATING, "gc_enqueue", gc_enqueue_body, &c);
}

/* --- peek -------------------------------------------------------------- */

/* One owner's index page while it is merged. */
struct gc_idx_page {
    uint64_t *seqs;   /**< want slots. */
    uint32_t  count;
    uint32_t  pos;
    bool      eof;    /**< The listing reached the owner's last key. */
};

/* Heap allocated once per call. */
struct gc_peek_ctx {
    struct mds_gc_entry *entries;   /**< Caller's array; want slots are used. */
    uint32_t             want;      /**< Rows asked for (<= DAOS_EXT_GC_PEEK_MAX). */
    uint32_t             n;         /**< Rows delivered. */
    uint64_t            *seqs;      /**< want slots: the candidates, in delivery order. */
    uint32_t             ncand;
    struct gc_idx_page   own;
    struct gc_idx_page   legacy;
    daos_key_desc_t     *kds;       /**< want slots. */
    uint8_t             *keybuf;    /**< want x DAOS_EXT_SEQ_KEY_BUF. */
    struct row_fetch    *wave;
};

static void gc_peek_free(struct gc_peek_ctx *c)
{
    if (c == NULL) {
        return;
    }
    free(c->seqs);
    free(c->own.seqs);
    free(c->legacy.seqs);
    free(c->kds);
    free(c->keybuf);
    free(c->wave);
    free(c);
}

static struct gc_peek_ctx *gc_peek_alloc(struct mds_gc_entry *entries, uint32_t want)
{
    struct gc_peek_ctx *c = calloc(1, sizeof(*c));

    if (c == NULL) {
        return NULL;
    }
    c->entries = entries;
    c->want = want;
    c->seqs = calloc(want, sizeof(*c->seqs));
    c->own.seqs = calloc(want, sizeof(*c->own.seqs));
    c->legacy.seqs = calloc(want, sizeof(*c->legacy.seqs));
    c->kds = calloc(want, sizeof(*c->kds));
    c->keybuf = calloc(want, DAOS_EXT_SEQ_KEY_BUF);
    c->wave = calloc(DAOS_EXT_WAVE, sizeof(*c->wave));
    if (c->seqs == NULL || c->own.seqs == NULL || c->legacy.seqs == NULL || c->kds == NULL ||
        c->keybuf == NULL || c->wave == NULL) {
        gc_peek_free(c);
        return NULL;
    }
    return c;
}

/* The first c->want index keys of @p owner, in sequence order. */
static int gc_idx_list(struct daos_txn *t, struct gc_peek_ctx *c, uint32_t owner,
                       struct gc_idx_page *pg)
{
    struct daos_list l;
    daos_handle_t oh;
    size_t off = 0;
    uint32_t i;
    int rc;

    pg->count = 0;
    pg->pos = 0;
    pg->eof = false;
    rc = daos_txn_obj(t, DAOS_TAG_GC_BY_OWNER, owner, &oh);
    if (rc != 0) {
        return rc;
    }
    daos_list_init(&l, c->kds, c->want, c->keybuf, (size_t)c->want * DAOS_EXT_SEQ_KEY_BUF);
    rc = daos_list_dkeys(t, oh, NULL, &l);
    if (rc != 0) {
        return rc;
    }
    for (i = 0; i < l.nr; i++) {
        size_t klen = (size_t)l.kds[i].kd_key_len;

        if (!daos_dkey_u64_parse(l.buf + off, klen, &pg->seqs[i])) {
            return -DER_IO; /* corrupt index key */
        }
        off += klen;
    }
    pg->count = l.nr;
    pg->eof = l.eof;
    return 0;
}

/* Merge the two owner pages by ascending seq into c->seqs, at most
 * c->want of them.  Each page holds the smallest seqs of its owner, so
 * the merged prefix is the smallest seqs of both together -- as long as
 * a page that ran out before its owner's end (a listing may return
 * fewer keys than asked for) is not merged past: the merge stops there
 * and the next peek continues. */
static void gc_idx_merge(struct gc_peek_ctx *c)
{
    struct gc_idx_page *own = &c->own;
    struct gc_idx_page *leg = &c->legacy;
    uint32_t n = 0;

    while (n < c->want) {
        bool have_own = own->pos < own->count;
        bool have_leg = leg->pos < leg->count;

        if ((!have_own && !have_leg) || (!have_own && !own->eof) || (!have_leg && !leg->eof)) {
            break;
        }
        if (have_own && (!have_leg || own->seqs[own->pos] <= leg->seqs[leg->pos])) {
            c->seqs[n++] = own->seqs[own->pos++];
        } else {
            c->seqs[n++] = leg->seqs[leg->pos++];
        }
    }
    c->ncand = n;
}

static void gc_row_key(void *arg, uint32_t i, struct daos_key *dk)
{
    struct gc_peek_ctx *c = arg;

    daos_dkey_be64(dk, c->seqs[i]);
}

/* Append the row of candidate @p i (gc_seq set from the key).  A key
 * whose row is absent is skipped: row and marker are written and
 * cleared in one transaction and this is one snapshot, so only a
 * foreign writer could produce one. */
static int gc_row_consume(void *arg, uint32_t i, const uint8_t *val, size_t len)
{
    struct gc_peek_ctx *c = arg;
    struct mds_gc_entry e;

    if (len == 0) {
        return 0;
    }
    if (!fdb_gc_decode(val, len, &e)) {
        return -DER_IO; /* corrupt row */
    }
    e.gc_seq = c->seqs[i];
    c->entries[c->n] = e;
    c->n++;
    return 0;
}

/* The owner view: two index listings, a merge, then the row waves. */
static int gc_idx_peek_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct gc_peek_ctx *c = arg;
    daos_handle_t oh;
    int rc;

    c->n = 0;
    c->ncand = 0;
    rc = gc_idx_list(t, c, t->b->mds_id, &c->own);
    if (rc == 0) {
        rc = gc_idx_list(t, c, 0, &c->legacy);
    }
    if (rc != 0) {
        return rc;
    }
    gc_idx_merge(c);
    rc = daos_txn_obj(t, DAOS_TAG_GC, 0, &oh);
    if (rc != 0) {
        return rc;
    }
    rc = row_wave_run(t, oh, c->wave, c->ncand, gc_row_key, gc_row_consume, c);
    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

/* The id-0 view: the first c->want dkeys of the GC object in its
 * (hash) enumeration order, then the row waves. */
static int gc_all_peek_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct gc_peek_ctx *c = arg;
    struct daos_list l;
    daos_handle_t oh;
    size_t off = 0;
    uint32_t i;
    int rc;

    c->n = 0;
    c->ncand = 0;
    rc = daos_txn_obj(t, DAOS_TAG_GC, 0, &oh);
    if (rc != 0) {
        return rc;
    }
    daos_list_init(&l, c->kds, c->want, c->keybuf, (size_t)c->want * DAOS_EXT_SEQ_KEY_BUF);
    rc = daos_list_dkeys(t, oh, NULL, &l);
    if (rc != 0) {
        return rc;
    }
    for (i = 0; i < l.nr; i++) {
        size_t klen = (size_t)l.kds[i].kd_key_len;

        if (klen != DAOS_KEY_BE64_LEN) {
            return -DER_IO; /* corrupt key */
        }
        c->seqs[i] = fdb_get_u64(l.buf + off);
        off += klen;
    }
    c->ncand = l.nr;
    rc = row_wave_run(t, oh, c->wave, c->ncand, gc_row_key, gc_row_consume, c);
    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

/* One read-only transaction: the listings, then the row waves. */
static enum mds_status daos_gc_peek_batch(struct mds_catalogue *cat, struct mds_gc_entry *entries,
                                          uint32_t cap, uint32_t *n_out)
{
    struct daos_backend *b = be_of(cat);
    struct gc_peek_ctx *c;
    enum mds_status st;

    if (n_out != NULL) {
        *n_out = 0;
    }
    if (b == NULL || entries == NULL || cap == 0 || n_out == NULL) {
        return MDS_ERR_INVAL;
    }
    c = gc_peek_alloc(entries, (cap < DAOS_EXT_GC_PEEK_MAX) ? cap : DAOS_EXT_GC_PEEK_MAX);
    if (c == NULL) {
        return MDS_ERR_NOMEM;
    }
    st = daos_run_txn(b, DAOS_TXN_READONLY, "gc_peek_batch",
                      (b->mds_id == 0) ? gc_all_peek_body : gc_idx_peek_body, c);
    if (st == MDS_OK) {
        *n_out = c->n;
    }
    gc_peek_free(c);
    return st;
}

static enum mds_status daos_gc_peek(struct mds_catalogue *cat, struct mds_gc_entry *entry)
{
    uint32_t n = 0;
    enum mds_status st;

    if (entry == NULL) {
        return MDS_ERR_INVAL;
    }
    st = daos_gc_peek_batch(cat, entry, 1, &n);
    if (st != MDS_OK) {
        return st;
    }
    return (n == 1) ? MDS_OK : MDS_ERR_NOTFOUND;
}

/* --- dequeue ------------------------------------------------------------ */

struct gc_dequeue_ctx {
    uint64_t gc_seq;
};

/* The row is read for its owner, then row and index marker are cleared
 * together.  An absent row is MDS_ERR_NOTFOUND with nothing written (a
 * repeated dequeue); a row that does not decode is corrupt
 * (MDS_ERR_IO), as a peek over it reports. */
static int gc_dequeue_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct gc_dequeue_ctx *c = arg;
    struct mds_gc_entry e;
    struct row_ref r;
    uint8_t buf[FDB_GC_ENC_MAX];
    size_t len = 0;
    bool found = false;
    int rc;

    row_gc(&r, c->gc_seq);
    rc = row_fetch(t, &r, buf, sizeof(buf), &len, &found);
    if (rc != 0) {
        return rc;
    }
    if (!found) {
        *st_out = MDS_ERR_NOTFOUND;
        return DAOS_BODY_DONE;
    }
    if (!fdb_gc_decode(buf, len, &e)) {
        return -DER_IO;
    }
    rc = daos_gc_row_clear(t, c->gc_seq, e.owner_mds_id);
    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

static enum mds_status daos_gc_dequeue(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                       uint64_t gc_seq)
{
    struct daos_backend *b = be_of(cat);
    struct gc_dequeue_ctx c;

    (void)txn;
    if (b == NULL) {
        return MDS_ERR_INVAL;
    }
    c.gc_seq = gc_seq;
    return daos_run_txn(b, DAOS_TXN_MUTATING, "gc_dequeue", gc_dequeue_body, &c);
}

/* --- count -------------------------------------------------------------- */

struct count_arg {
    uint64_t count;
};

/* Every key counts (an owner's index, the GC object, the manifest). */
static int count_visit(struct queue_ctx *c, uint64_t seq, const uint8_t *val, size_t vlen)
{
    struct count_arg *a = c->arg;

    (void)seq;
    (void)val;
    (void)vlen;
    a->count++;
    return 0;
}

static uint32_t count_clamp(uint64_t n)
{
    return (n > UINT32_MAX) ? UINT32_MAX : (uint32_t)n;
}

/* Add the number of keys of the object (@p tag, @p id) to *count,
 * examining at most @p budget of them. */
static enum mds_status keys_count(struct daos_backend *b, enum daos_tag tag, uint64_t id,
                                  enum queue_keys keys, const char *op, uint64_t budget,
                                  uint64_t *count)
{
    struct queue_ctx q;
    struct count_arg a = { 0 };
    enum mds_status st;

    st = queue_init(&q, tag, id, keys, DAOS_EXT_COUNT_PAGE, false, count_visit, &a);
    if (st != MDS_OK) {
        return st;
    }
    st = queue_walk(b, &q, op, budget);
    queue_free(&q);
    if (st == MDS_OK) {
        *count += a.count;
    }
    return st;
}

static enum mds_status daos_gc_count(struct mds_catalogue *cat, uint32_t *count)
{
    struct daos_backend *b = be_of(cat);
    uint64_t total = 0;
    enum mds_status st;

    if (b == NULL || count == NULL) {
        return MDS_ERR_INVAL;
    }
    if (b->mds_id != 0) {
        /* Own keys first; the legacy keys get what is left of the
         * examine budget, so the two together saturate like one walk. */
        st = keys_count(b, DAOS_TAG_GC_BY_OWNER, b->mds_id, QUEUE_KEYS_U64, "gc_count",
                        DAOS_EXT_COUNT_MAX, &total);
        if (st == MDS_OK && total < DAOS_EXT_COUNT_MAX) {
            st = keys_count(b, DAOS_TAG_GC_BY_OWNER, 0, QUEUE_KEYS_U64, "gc_count",
                            DAOS_EXT_COUNT_MAX - total, &total);
        }
    } else {
        st = keys_count(b, DAOS_TAG_GC, 0, QUEUE_KEYS_BE64, "gc_count", DAOS_EXT_COUNT_MAX,
                        &total);
    }
    if (st == MDS_OK) {
        *count = count_clamp(total);
    }
    return st;
}

/* -----------------------------------------------------------------------
 * Async-REMOVE delete manifest
 * ----------------------------------------------------------------------- */

/* Encode a fresh manifest row into @p enc. */
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

static enum mds_status daos_remove_pending_enqueue(struct mds_catalogue *cat,
                                                   struct mds_cat_txn *txn, uint64_t dir_fileid,
                                                   const char *name, uint64_t child_fileid,
                                                   uint64_t child_generation, uint64_t *seq_out)
{
    struct daos_backend *b = be_of(cat);
    uint8_t enc[FDB_REMOVE_PENDING_ENC_MAX];
    size_t len = 0;
    struct row_ref r;
    uint64_t seq = 0;
    enum mds_status st;

    (void)txn;
    if (b == NULL || !name_len_ok(name, MDS_MAX_NAME) || seq_out == NULL) {
        return MDS_ERR_INVAL;
    }
    st = daos_backend_alloc_id(b, DAOS_META_REMOVE_SEQ, &seq);
    if (st != MDS_OK) {
        return st;
    }
    if (!rp_row_build(dir_fileid, name, child_fileid, child_generation, enc, sizeof(enc), &len)) {
        return MDS_ERR_INVAL;
    }
    row_remove_pending(&r, seq);
    st = run_set(b, "remove_pending_enqueue", &r, enc, len);
    if (st == MDS_OK) {
        *seq_out = seq;
    }
    return st;
}

struct rp_unlink_ctx {
    uint64_t    dir;
    const char *name;
    uint64_t    child;
    uint64_t    generation;
    uint64_t    seq;
    uint8_t     row[FDB_REMOVE_PENDING_ENC_MAX];
    size_t      row_len;
};

/* Wave: the dirent of the parent and the blob of the child, two
 * objects, one round trip. */
static int rp_unlink_wave(struct daos_txn *t, daos_handle_t oh_p, daos_handle_t oh_c,
                          const struct rp_unlink_ctx *c, struct fdb_dirent_val *dv,
                          bool *dirent_found, struct mds_inode *child, bool *child_found)
{
    struct daos_dirent_req rd;
    struct daos_inode_req rb;
    bool sd = false;
    bool sb = false;
    int rc;
    int rc2;

    *dirent_found = false;
    *child_found = false;
    rc = daos_inode_req_init_blob(&rb);
    if (rc != 0) {
        return rc;
    }
    rc = daos_dirent_read_start(t, oh_p, c->name, &rd);
    sd = (rc == 0);
    if (rc == 0) {
        rc = daos_inode_read_start(t, oh_c, &rb);
        sb = (rc == 0);
    }
    if (sd) {
        rc2 = daos_dirent_read_finish(t, &rd, dv, dirent_found);
        rc = (rc == 0) ? rc2 : rc;
    }
    if (sb) {
        rc2 = daos_inode_read_finish(t, &rb, child, child_found);
        rc = (rc == 0) ? rc2 : rc;
    }
    daos_inode_req_fini(&rb);
    return rc;
}

/* Delete-at-ack: manifest row + guarded dirent delete + DELETE_PENDING
 * flag in one transaction.  MDS_ERR_STALE when the dirent no longer
 * resolves to (child, generation).  The parent is deliberately not
 * touched: the ack path folds that delta into its aggregator (memdb
 * does the same). */
static int rp_unlink_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct rp_unlink_ctx *c = arg;
    struct fdb_dirent_val dv;
    struct mds_inode child;
    struct row_ref r;
    daos_handle_t oh_p;
    daos_handle_t oh_c;
    bool dirent_found = false;
    bool child_found = false;
    int rc;

    rc = daos_txn_obj(t, DAOS_TAG_INODE, c->dir, &oh_p);
    if (rc == 0) {
        rc = daos_txn_obj(t, DAOS_TAG_INODE, c->child, &oh_c);
    }
    if (rc == 0) {
        rc = rp_unlink_wave(t, oh_p, oh_c, c, &dv, &dirent_found, &child, &child_found);
    }
    if (rc != 0) {
        return rc;
    }
    if (!dirent_found || dv.child_fileid != c->child || !child_found ||
        child.generation != c->generation) {
        *st_out = MDS_ERR_STALE;
        return DAOS_BODY_DONE;
    }
    child.flags |= MDS_IFLAG_DELETE_PENDING;
    rc = daos_inode_write_blob(t, oh_c, &child);
    if (rc == 0) {
        rc = daos_dirent_clear(t, oh_p, c->name, dv.seq);
    }
    if (rc == 0) {
        row_remove_pending(&r, c->seq);
        rc = row_update(t, &r, c->row, c->row_len);
    }
    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

static enum mds_status daos_remove_pending_enqueue_unlink(struct mds_catalogue *cat,
                                                          struct mds_cat_txn *txn,
                                                          uint64_t dir_fileid, const char *name,
                                                          uint64_t child_fileid,
                                                          uint64_t child_generation,
                                                          uint64_t *seq_out)
{
    struct daos_backend *b = be_of(cat);
    struct rp_unlink_ctx c;
    enum mds_status st;

    (void)txn;
    if (b == NULL || !name_len_ok(name, MDS_MAX_NAME) || seq_out == NULL) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    st = daos_backend_alloc_id(b, DAOS_META_REMOVE_SEQ, &c.seq);
    if (st != MDS_OK) {
        return st;
    }
    c.dir = dir_fileid;
    c.name = name;
    c.child = child_fileid;
    c.generation = child_generation;
    if (!rp_row_build(dir_fileid, name, child_fileid, child_generation, c.row, sizeof(c.row),
                      &c.row_len)) {
        return MDS_ERR_INVAL;
    }
    st = daos_run_txn(b, DAOS_TXN_MUTATING, "remove_pending_enqueue_unlink", rp_unlink_body, &c);
    if (st == MDS_OK) {
        *seq_out = c.seq;
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

static enum mds_status daos_remove_pending_peek_batch(struct mds_catalogue *cat,
                                                      uint64_t now_ns,
                                                      struct mds_remove_pending_entry *entries,
                                                      uint32_t cap, uint32_t *n_out)
{
    struct daos_backend *b = be_of(cat);
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
    st = queue_init(&q, DAOS_TAG_REMOVE_PENDING, 0, QUEUE_KEYS_U64, DAOS_EXT_QUEUE_PAGE, true,
                    rp_peek_visit, &a);
    if (st != MDS_OK) {
        return st;
    }
    st = queue_walk(b, &q, "remove_pending_peek_batch", DAOS_EXT_EXAMINE_CAP(cap));
    if (st == MDS_OK) {
        *n_out = q.cur.n;
    }
    queue_free(&q);
    return st;
}

struct rp_claim_ctx {
    uint64_t remove_seq;
    uint32_t mds_id;
    uint64_t boot_epoch;
    uint64_t now_ns;
    uint64_t claim_ttl_ns;
};

/* The claim test and the claim write are one transaction (C3): two
 * drainers racing for a row conflict on its read, the loser's commit
 * restarts, its re-run sees the live claim. */
static int rp_claim_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct rp_claim_ctx *c = arg;
    struct mds_remove_pending_entry e;
    struct row_ref r;
    uint8_t buf[FDB_REMOVE_PENDING_ENC_MAX];
    size_t len = 0;
    bool found = false;
    int rc;

    row_remove_pending(&r, c->remove_seq);
    rc = row_fetch(t, &r, buf, sizeof(buf), &len, &found);
    if (rc != 0) {
        return rc;
    }
    if (!found) {
        *st_out = MDS_ERR_NOTFOUND;
        return DAOS_BODY_DONE;
    }
    if (!fdb_remove_pending_decode(buf, len, &e)) {
        return -DER_IO;
    }
    if (e.claim_mds_id != 0 && e.claim_expires_ns >= c->now_ns) {
        *st_out = MDS_ERR_NOTFOUND; /* another drainer holds a live claim */
        return DAOS_BODY_DONE;
    }
    e.claim_mds_id = c->mds_id;
    e.claim_boot = c->boot_epoch;
    if (c->claim_ttl_ns > UINT64_MAX - c->now_ns) {
        e.claim_expires_ns = UINT64_MAX; /* saturate like memdb */
    } else {
        e.claim_expires_ns = c->now_ns + c->claim_ttl_ns;
    }
    if (!fdb_remove_pending_encode(&e, buf, sizeof(buf), &len)) {
        return -DER_IO;
    }
    rc = row_update(t, &r, buf, len);
    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

static enum mds_status daos_remove_pending_claim(struct mds_catalogue *cat, uint64_t remove_seq,
                                                 uint32_t mds_id, uint64_t boot_epoch,
                                                 uint64_t now_ns, uint64_t claim_ttl_ns)
{
    struct daos_backend *b = be_of(cat);
    struct rp_claim_ctx c;

    if (b == NULL) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.remove_seq = remove_seq;
    c.mds_id = mds_id;
    c.boot_epoch = boot_epoch;
    c.now_ns = now_ns;
    c.claim_ttl_ns = claim_ttl_ns;
    return daos_run_txn(b, DAOS_TXN_MUTATING, "remove_pending_claim", rp_claim_body, &c);
}

/* Idempotent: a completed row may be completed again (blind punch). */
static enum mds_status daos_remove_pending_complete(struct mds_catalogue *cat,
                                                    uint64_t remove_seq)
{
    struct daos_backend *b = be_of(cat);
    struct row_ref r;

    if (b == NULL) {
        return MDS_ERR_INVAL;
    }
    row_remove_pending(&r, remove_seq);
    return run_clear(b, "remove_pending_complete", &r, false);
}

/* retries += 1 when the row exists; MDS_OK either way (memdb). */
static int rp_bump_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct kv_ctx *c = arg;
    struct mds_remove_pending_entry e;
    uint8_t buf[FDB_REMOVE_PENDING_ENC_MAX];
    size_t len = 0;
    bool found = false;
    int rc;

    rc = row_fetch(t, &c->row, buf, sizeof(buf), &len, &found);
    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    if (!found) {
        return DAOS_BODY_DONE;
    }
    if (!fdb_remove_pending_decode(buf, len, &e)) {
        return -DER_IO;
    }
    e.retries++;
    if (!fdb_remove_pending_encode(&e, buf, sizeof(buf), &len)) {
        return -DER_IO;
    }
    rc = row_update(t, &c->row, buf, len);
    if (rc != 0) {
        return rc;
    }
    return DAOS_BODY_COMMIT;
}

static enum mds_status daos_remove_pending_bump_retry(struct mds_catalogue *cat,
                                                      uint64_t remove_seq)
{
    struct daos_backend *b = be_of(cat);
    struct kv_ctx c;

    if (b == NULL) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    row_remove_pending(&c.row, remove_seq);
    return daos_run_txn(b, DAOS_TXN_MUTATING, "remove_pending_bump_retry", rp_bump_body, &c);
}

static enum mds_status daos_remove_pending_count(struct mds_catalogue *cat, uint32_t *count)
{
    struct daos_backend *b = be_of(cat);
    uint64_t total = 0;
    enum mds_status st;

    if (b == NULL || count == NULL) {
        return MDS_ERR_INVAL;
    }
    st = keys_count(b, DAOS_TAG_REMOVE_PENDING, 0, QUEUE_KEYS_U64, "remove_pending_count",
                    DAOS_EXT_COUNT_MAX, &total);
    if (st == MDS_OK) {
        *count = count_clamp(total);
    }
    return st;
}

struct rp_scan_arg {
    struct mds_remove_pending_entry *page; /**< DAOS_EXT_RP_SCAN_PAGE slots. */
};

static int rp_scan_visit(struct queue_ctx *c, uint64_t seq, const uint8_t *val, size_t vlen)
{
    struct rp_scan_arg *a = c->arg;

    if (c->cur.n >= DAOS_EXT_RP_SCAN_PAGE ||
        !fdb_remove_pending_decode(val, vlen, &a->page[c->cur.n])) {
        return -1;
    }
    a->page[c->cur.n].remove_seq = seq;
    c->cur.n++;
    return 0;
}

/* One page per transaction, delivered after it ended (C1/C2). */
static enum mds_status daos_remove_pending_scan_all(struct mds_catalogue *cat,
                                                    mds_cat_remove_pending_scan_cb cb, void *ctx)
{
    struct daos_backend *b = be_of(cat);
    struct queue_ctx q;
    struct rp_scan_arg a;
    enum mds_status st;

    if (b == NULL || cb == NULL) {
        return MDS_ERR_INVAL;
    }
    a.page = calloc(DAOS_EXT_RP_SCAN_PAGE, sizeof(*a.page));
    if (a.page == NULL) {
        return MDS_ERR_NOMEM;
    }
    st = queue_init(&q, DAOS_TAG_REMOVE_PENDING, 0, QUEUE_KEYS_U64, DAOS_EXT_RP_SCAN_PAGE, true,
                    rp_scan_visit, &a);
    if (st != MDS_OK) {
        free(a.page);
        return st;
    }
    for (;;) {
        uint32_t i;

        q.start.n = 0;
        st = daos_run_txn(b, DAOS_TXN_READONLY, "remove_pending_scan_all", queue_page_body, &q);
        if (st != MDS_OK) {
            break;
        }
        for (i = 0; i < q.cur.n; i++) {
            if (cb(&a.page[i], ctx) != 0) {
                goto out;
            }
        }
        if (!q.more || q.nkeys == 0) {
            break;
        }
        q.start = q.cur;
    }
out:
    queue_free(&q);
    free(a.page);
    return st;
}

/* -----------------------------------------------------------------------
 * Shard routing, cross-shard dirents, link anchors
 * ----------------------------------------------------------------------- */

static enum mds_status daos_shard_fileid_get(struct mds_catalogue *cat, uint64_t fileid,
                                             uint32_t *shard_id)
{
    struct daos_backend *b = be_of(cat);
    uint8_t buf[sizeof(uint32_t)]; /* INODE 'h' value: one LE u32 */
    size_t len = 0;
    struct row_ref r;
    enum mds_status st;

    if (b == NULL || shard_id == NULL) {
        return MDS_ERR_INVAL;
    }
    row_shard(&r, fileid);
    st = run_get(b, "shard_fileid_get", &r, buf, sizeof(buf), &len);
    if (st != MDS_OK) {
        return st;
    }
    return fdb_le32_decode(buf, len, shard_id) ? MDS_OK : MDS_ERR_IO;
}

static enum mds_status daos_shard_fileid_put(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                             uint64_t fileid, uint32_t shard_id)
{
    struct daos_backend *b = be_of(cat);
    uint8_t enc[sizeof(uint32_t)]; /* INODE 'h' value: one LE u32 */
    struct row_ref r;

    (void)txn;
    if (b == NULL) {
        return MDS_ERR_INVAL;
    }
    fdb_le32_put(enc, shard_id);
    row_shard(&r, fileid);
    return run_set(b, "shard_fileid_put", &r, enc, sizeof(enc));
}

static enum mds_status daos_shard_fileid_del(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                             uint64_t fileid)
{
    struct daos_backend *b = be_of(cat);
    struct row_ref r;

    (void)txn;
    if (b == NULL) {
        return MDS_ERR_INVAL;
    }
    row_shard(&r, fileid);
    return run_clear(b, "shard_fileid_del", &r, true);
}

static enum mds_status daos_ext_dirent_get(struct mds_catalogue *cat, uint64_t parent,
                                           const char *name, uint32_t *owner_mds_id,
                                           uint64_t *target_fileid, uint8_t *target_type,
                                           uint64_t *anchor_id)
{
    struct daos_backend *b = be_of(cat);
    struct fdb_ext_dirent_val v;
    uint8_t buf[FDB_EXT_DIRENT_ENC_SIZE];
    size_t len = 0;
    struct row_ref r;
    enum mds_status st;

    if (b == NULL || name == NULL) {
        return MDS_ERR_INVAL;
    }
    if (!name_len_ok(name, MDS_MAX_NAME)) {
        return MDS_ERR_NOTFOUND;
    }
    row_ext_dirent(&r, parent, name);
    st = run_get(b, "ext_dirent_get", &r, buf, sizeof(buf), &len);
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
static enum mds_status daos_ext_dirent_put(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                           uint64_t parent, const char *name,
                                           uint32_t owner_mds_id, uint64_t target_fileid,
                                           uint8_t target_type, uint64_t anchor_id)
{
    struct daos_backend *b = be_of(cat);
    struct fdb_ext_dirent_val v;
    uint8_t enc[FDB_EXT_DIRENT_ENC_SIZE];
    size_t len = 0;
    struct row_ref r;

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
    row_ext_dirent(&r, parent, name);
    return run_set(b, "ext_dirent_put", &r, enc, len);
}

static enum mds_status daos_ext_dirent_del(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                           uint64_t parent, const char *name)
{
    struct daos_backend *b = be_of(cat);
    struct row_ref r;

    (void)txn;
    if (b == NULL || name == NULL) {
        return MDS_ERR_INVAL;
    }
    if (!name_len_ok(name, MDS_MAX_NAME)) {
        return MDS_ERR_NOTFOUND;
    }
    row_ext_dirent(&r, parent, name);
    return run_clear(b, "ext_dirent_del", &r, true);
}

/* Upsert keyed anchor_id; the name may be empty (memdb parity). */
static enum mds_status daos_link_anchor_put(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                            uint64_t anchor_id, uint32_t remote_mds_id,
                                            uint64_t parent_fileid, const char *name)
{
    struct daos_backend *b = be_of(cat);
    struct fdb_link_anchor_val v;
    uint8_t enc[FDB_LINK_ANCHOR_ENC_MAX];
    size_t len = 0;
    struct row_ref r;

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
    row_link_anchor(&r, anchor_id);
    return run_set(b, "link_anchor_put", &r, enc, len);
}

static enum mds_status daos_link_anchor_del(struct mds_catalogue *cat, struct mds_cat_txn *txn,
                                            uint64_t anchor_id)
{
    struct daos_backend *b = be_of(cat);
    struct row_ref r;

    (void)txn;
    if (b == NULL) {
        return MDS_ERR_INVAL;
    }
    row_link_anchor(&r, anchor_id);
    return run_clear(b, "link_anchor_del", &r, true);
}

/* -----------------------------------------------------------------------
 * Registration.  prealloc_pool_* stay NULL (as on memdb and
 * FoundationDB: the DS prealloc engine then runs in-memory only) and
 * backend_client_stats is filled by catalogue_daos.c from the
 * transaction runner's counters.
 * ----------------------------------------------------------------------- */

void catalogue_daos_ext_register(struct mds_authority_ops *ops)
{
    if (ops == NULL) {
        return;
    }
    ops->inline_get                    = daos_inline_get;
    ops->inline_put                    = daos_inline_put;
    ops->inline_del                    = daos_inline_del;
    ops->xattr_get                     = daos_xattr_get;
    ops->xattr_put                     = daos_xattr_put;
    ops->xattr_del                     = daos_xattr_del;
    ops->xattr_list                    = daos_xattr_list;
    ops->xattr_exists                  = daos_xattr_exists;
    ops->stripe_map_get                = daos_stripe_map_get;
    ops->stripe_map_put                = daos_stripe_map_put;
    ops->stripe_map_del                = daos_stripe_map_del;
    ops->stripe_map_scan               = daos_stripe_map_scan;
    ops->ds_get                        = daos_ds_get;
    ops->ds_put                        = daos_ds_put;
    ops->ds_del                        = daos_ds_del;
    ops->ds_list                       = daos_ds_list;
    ops->ds_provision_get              = daos_ds_provision_get;
    ops->ds_provision_put              = daos_ds_provision_put;
    ops->ds_provision_del              = daos_ds_provision_del;
    ops->quota_rule_get                = daos_quota_rule_get;
    ops->quota_rule_put                = daos_quota_rule_put;
    ops->quota_usage_get               = daos_quota_usage_get;
    ops->quota_usage_put               = daos_quota_usage_put;
    ops->gc_enqueue                    = daos_gc_enqueue;
    ops->gc_peek                       = daos_gc_peek;
    ops->gc_dequeue                    = daos_gc_dequeue;
    ops->gc_count                      = daos_gc_count;
    ops->gc_peek_batch                 = daos_gc_peek_batch;
    ops->remove_pending_enqueue        = daos_remove_pending_enqueue;
    ops->remove_pending_enqueue_unlink = daos_remove_pending_enqueue_unlink;
    ops->remove_pending_peek_batch     = daos_remove_pending_peek_batch;
    ops->remove_pending_claim          = daos_remove_pending_claim;
    ops->remove_pending_complete       = daos_remove_pending_complete;
    ops->remove_pending_bump_retry     = daos_remove_pending_bump_retry;
    ops->remove_pending_count          = daos_remove_pending_count;
    ops->remove_pending_scan_all       = daos_remove_pending_scan_all;
    ops->shard_fileid_get              = daos_shard_fileid_get;
    ops->shard_fileid_put              = daos_shard_fileid_put;
    ops->shard_fileid_del              = daos_shard_fileid_del;
    ops->ext_dirent_get                = daos_ext_dirent_get;
    ops->ext_dirent_put                = daos_ext_dirent_put;
    ops->ext_dirent_del                = daos_ext_dirent_del;
    ops->link_anchor_put               = daos_link_anchor_put;
    ops->link_anchor_del               = daos_link_anchor_del;
}
