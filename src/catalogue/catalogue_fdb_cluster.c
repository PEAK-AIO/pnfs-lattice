/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * catalogue_fdb_cluster.c -- FoundationDB backend: cluster slots.
 *
 * The node registry and the partition map of mds_cluster.h (the Phase
 * 1b contract), one fdb_run_txn per slot.  Every conditional write is a
 * read-then-set inside ONE optimistic transaction: the row read adds a
 * read conflict range, so two daemons racing on the same mds_id or
 * partition serialise at the resolver and the loser re-runs against
 * the winner's committed row -- the epoch and owner rules are applied
 * to what is actually stored, never to a stale snapshot.
 *
 * Rows (fdb_keys.h; values in fdb_codec.h):
 *   NODE_REGISTRY + be32 mds_id   -> fdb_node codec (boot_epoch, ports,
 *                                    state, last_heartbeat_ns,
 *                                    witness_epoch, sw_version, hostname)
 *   PARTITION_MAP + be32 id       -> fdb_partition codec (owner, state, path)
 *
 * witness_epoch is the registering process's WITNESS key epoch
 * (fdb_txn.h): the open-time sweep of this mds_id's witness rows
 * (catalogue_fdb.c, witness_clear_body) clears only rows strictly below
 * it, so a daemon that takes over a still-running mds_id never erases
 * the live process's in-flight commit witnesses.  node_register writes
 * it; node_heartbeat re-encodes the decoded row, so it travels along.
 *
 * Timestamps are the writer's CLOCK_REALTIME nanoseconds (mds_cluster.h
 * clock-domain rule).  Enumerations materialise a bounded page in a
 * read-only transaction and deliver it afterwards (C1/C2); the registry
 * holds at most MDS_MAX_NODES rows, the map the configured partitions.
 *
 * Transaction shapes (R = point read, RR = range read, W = set/clear;
 * every mutating transaction adds the runner's witness set + READ
 * conflict range and commits once):
 *   node_register     R row; absent or lower epoch: W row (this process's
 *                     witness_epoch included) / else EXISTS
 *   node_heartbeat    R row; NOTFOUND / STALE / W row with the new stamp
 *   node_deregister   R row; absent: OK / STALE / W clear
 *   node_list         per page: RR NODE_REGISTRY (MDS_MAX_NODES rows)
 *   node_scan_stale   per page: RR NODE_REGISTRY, filter heartbeat < threshold
 *   partition_list    per page: RR PARTITION_MAP (64 rows)
 *   partition_put     R row; exists && insert_only: EXISTS / W row
 *   partition_cas     R row; NOTFOUND / owner mismatch: STALE / W row
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "catalogue_fdb.h"
#include "catalogue_internal.h"
#include "fdb_codec.h"
#include "fdb_keys.h"
#include "fdb_txn.h"

/* Registry rows per read-only page; the registry never holds more. */
#define FDB_NODE_PAGE      ((uint32_t)MDS_MAX_NODES)
/* Partition rows per read-only page (a row carries up to MDS_MAX_PATH). */
#define FDB_PARTITION_PAGE 64U

/* Node registry state of a registered daemon (RonDB column encoding). */
#define FDB_NODE_STATE_ACTIVE 0U

_Static_assert(FDB_NODE_PAGE <= 0x7FFFFFFFU, "page bound must fit the fdb_c int limit");

/* -----------------------------------------------------------------------
 * Helpers
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

static void key_node(struct fdb_key *k, const struct fdb_key_prefix *p, uint32_t mds_id)
{
    fdb_key_init(k, p, FDB_KT_NODE_REGISTRY);
    fdb_key_be32(k, mds_id);
}

static void key_partition(struct fdb_key *k, const struct fdb_key_prefix *p, uint32_t id)
{
    fdb_key_init(k, p, FDB_KT_PARTITION_MAP);
    fdb_key_be32(k, id);
}

/* Point read of @p k decoded through @p decode straight from the
 * client's buffer; *found false when absent; a value @p decode rejects
 * is a corrupt row (FDB_ERR_PLATFORM_ERROR -> MDS_ERR_IO). */
static fdb_error_t read_row(FDBTransaction *tr, const struct fdb_key *k,
                            bool (*decode)(const uint8_t *, size_t, void *), void *out,
                            bool *found)
{
    FDBFuture *f;
    fdb_bool_t present = 0;
    const uint8_t *val = NULL;
    int vlen = 0;
    fdb_error_t err;

    *found = false;
    f = fdb_txn_get_start(tr, k, false);
    if (f == NULL) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    err = fdb_txn_wait(f);
    if (err == 0) {
        err = fdb_future_get_value(f, &present, &val, &vlen);
    }
    if (err == 0 && present != 0) {
        if (vlen < 0 || !decode(val, (size_t)vlen, out)) {
            err = FDB_ERR_PLATFORM_ERROR;
        } else {
            *found = true;
        }
    }
    fdb_future_destroy(f);
    return err;
}

static bool node_decode_cb(const uint8_t *buf, size_t len, void *out)
{
    return fdb_node_decode(buf, len, out);
}

static bool partition_decode_cb(const uint8_t *buf, size_t len, void *out)
{
    return fdb_partition_decode(buf, len, out);
}

/* -----------------------------------------------------------------------
 * Node registry
 * ----------------------------------------------------------------------- */

struct node_ctx {
    struct fdb_backend *b;
    uint32_t            mds_id;
    uint64_t            boot_epoch;
    struct fdb_node_val want;   /**< register: the row to write */
    struct fdb_node_val row;    /**< the stored row, once read */
    uint8_t             enc[FDB_NODE_ENC_MAX];
};

static int node_write(FDBTransaction *tr, struct node_ctx *c, const struct fdb_node_val *v)
{
    struct fdb_key k;
    size_t len = 0;

    if (!fdb_node_encode(v, c->enc, sizeof(c->enc), &len)) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    key_node(&k, &c->b->prefix, c->mds_id);
    fdb_txn_set(tr, &k, c->enc, len);
    return 0;
}

/* Conditional upsert: insert when absent, replace when the stored
 * boot_epoch is lower, MDS_ERR_EXISTS otherwise (a duplicate live
 * mds_id is split-brain and is refused). */
static int node_register_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct node_ctx *c = arg;
    struct fdb_key k;
    bool found = false;
    fdb_error_t err;
    int rc;

    key_node(&k, &c->b->prefix, c->mds_id);
    err = read_row(tr, &k, node_decode_cb, &c->row, &found);
    if (err != 0) {
        return (int)err;
    }
    if (found && c->row.boot_epoch >= c->want.boot_epoch) {
        *st_out = MDS_ERR_EXISTS;
        return FDB_BODY_DONE;
    }
    c->want.last_heartbeat_ns = realtime_ns();
    rc = node_write(tr, c, &c->want);
    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

/* Update only when the row exists AND its epoch matches: NOTFOUND when
 * absent, STALE on a mismatch (an old incarnation never overwrites its
 * replacement); only the timestamp changes -- the decoded row is
 * written back, so its witness_epoch is preserved. */
static int node_heartbeat_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct node_ctx *c = arg;
    struct fdb_key k;
    bool found = false;
    fdb_error_t err;
    int rc;

    key_node(&k, &c->b->prefix, c->mds_id);
    err = read_row(tr, &k, node_decode_cb, &c->row, &found);
    if (err != 0) {
        return (int)err;
    }
    if (!found) {
        *st_out = MDS_ERR_NOTFOUND;
        return FDB_BODY_DONE;
    }
    if (c->row.boot_epoch != c->boot_epoch) {
        *st_out = MDS_ERR_STALE;
        return FDB_BODY_DONE;
    }
    c->row.last_heartbeat_ns = realtime_ns();
    rc = node_write(tr, c, &c->row);
    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

/* Delete only on an epoch match; an absent row is MDS_OK (a retried
 * shutdown is harmless), a mismatch is STALE and deletes nothing. */
static int node_deregister_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct node_ctx *c = arg;
    struct fdb_key k;
    bool found = false;
    fdb_error_t err;

    key_node(&k, &c->b->prefix, c->mds_id);
    err = read_row(tr, &k, node_decode_cb, &c->row, &found);
    if (err != 0) {
        return (int)err;
    }
    if (!found) {
        *st_out = MDS_OK;
        return FDB_BODY_DONE;
    }
    if (c->row.boot_epoch != c->boot_epoch) {
        *st_out = MDS_ERR_STALE;
        return FDB_BODY_DONE;
    }
    fdb_txn_clear(tr, &k);
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static enum mds_status fdb_node_register(struct mds_catalogue *cat, uint32_t mds_id,
                                         uint64_t boot_epoch, const char *hostname,
                                         uint16_t nfs_port, uint16_t grpc_port)
{
    struct node_ctx *c;
    enum mds_status st;

    if (be_of(cat) == NULL || hostname == NULL || hostname[0] == '\0' ||
        strnlen(hostname, FDB_NODE_HOST_MAX + 1U) > FDB_NODE_HOST_MAX) {
        return MDS_ERR_INVAL;
    }
    c = calloc(1, sizeof(*c));
    if (c == NULL) {
        return MDS_ERR_NOMEM;
    }
    c->b = be_of(cat);
    c->mds_id = mds_id;
    c->boot_epoch = boot_epoch;
    c->want.boot_epoch = boot_epoch;
    c->want.witness_epoch = c->b->witness_epoch;
    c->want.nfs_port = nfs_port;
    c->want.grpc_port = grpc_port;
    c->want.state = FDB_NODE_STATE_ACTIVE;
    (void)snprintf(c->want.sw_version, sizeof(c->want.sw_version), "%u.%u.%u",
                   (unsigned)PNFS_MDS_VERSION_MAJOR, (unsigned)PNFS_MDS_VERSION_MINOR,
                   (unsigned)PNFS_MDS_VERSION_PATCH);
    (void)snprintf(c->want.hostname, sizeof(c->want.hostname), "%s", hostname);
    st = fdb_run_txn(c->b, FDB_TXN_MUTATING, "node_register", node_register_body, c);
    free(c);
    if (st == MDS_OK) {
        /* This incarnation owns the registry row: rows that carry an
         * owner epoch (client recovery) stamp this epoch from now on. */
        (void)fdb_backend_set_boot_epoch(cat, boot_epoch);
    }
    return st;
}

static enum mds_status fdb_node_heartbeat(struct mds_catalogue *cat, uint32_t mds_id,
                                          uint64_t boot_epoch)
{
    struct node_ctx *c;
    enum mds_status st;

    if (be_of(cat) == NULL) {
        return MDS_ERR_INVAL;
    }
    c = calloc(1, sizeof(*c));
    if (c == NULL) {
        return MDS_ERR_NOMEM;
    }
    c->b = be_of(cat);
    c->mds_id = mds_id;
    c->boot_epoch = boot_epoch;
    st = fdb_run_txn(c->b, FDB_TXN_MUTATING, "node_heartbeat", node_heartbeat_body, c);
    free(c);
    return st;
}

static enum mds_status fdb_node_deregister(struct mds_catalogue *cat, uint32_t mds_id,
                                           uint64_t boot_epoch)
{
    struct node_ctx *c;
    enum mds_status st;

    if (be_of(cat) == NULL) {
        return MDS_ERR_INVAL;
    }
    c = calloc(1, sizeof(*c));
    if (c == NULL) {
        return MDS_ERR_NOMEM;
    }
    c->b = be_of(cat);
    c->mds_id = mds_id;
    c->boot_epoch = boot_epoch;
    st = fdb_run_txn(c->b, FDB_TXN_MUTATING, "node_deregister", node_deregister_body, c);
    free(c);
    return st;
}

/* --- node_list / node_scan_stale ------------------------------------------ */

struct node_row {
    uint32_t            mds_id;
    struct fdb_node_val val;
};

struct node_scan_ctx {
    struct fdb_backend *b;
    uint64_t            stale_before;  /**< 0 = every row. */
    struct fdb_key      cursor;
    bool                more;
    uint32_t            n;
    struct node_row    *page;          /**< FDB_NODE_PAGE rows. */
};

/* One page of registry rows after the cursor; stale_before != 0 keeps
 * only rows whose heartbeat is older. */
static int node_scan_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct node_scan_ctx *c = arg;
    struct fdb_key base;
    struct fdb_key_range r;
    FDBFuture *f;
    const FDBKeyValue *kvs = NULL;
    int count = 0;
    int i;
    fdb_error_t err;

    c->n = 0;
    c->more = false;
    fdb_key_init(&base, &c->b->prefix, FDB_KT_NODE_REGISTRY);
    if (!fdb_key_range_prefix(&r, &base)) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    if (c->cursor.len > 0) {
        r.begin = c->cursor;
        fdb_key_u8(&r.begin, 0);
    }
    f = fdb_txn_get_range_start(tr, &r, (int)FDB_NODE_PAGE, false, false);
    err = fdb_txn_get_range_wait(f, &kvs, &count, &c->more);
    if (err != 0) {
        if (f != NULL) {
            fdb_future_destroy(f);
        }
        return (int)err;
    }
    for (i = 0; i < count && c->n < FDB_NODE_PAGE; i++) {
        FDBKeyValue kv;
        struct node_row *row = &c->page[c->n];

        fdb_kv_at(kvs, i, &kv);
        if (kv.key_length != (int)base.len + 4 ||
            !fdb_node_decode(kv.value, (size_t)kv.value_length, &row->val)) {
            fdb_future_destroy(f);
            c->n = 0;
            return FDB_ERR_PLATFORM_ERROR; /* corrupt row */
        }
        row->mds_id = fdb_get_u32(kv.key + base.len);
        if (c->stale_before == 0 || row->val.last_heartbeat_ns < c->stale_before) {
            c->n++;
        }
    }
    if (count > 0) {
        FDBKeyValue kv;

        fdb_kv_at(kvs, count - 1, &kv);
        c->cursor.len = 0;
        c->cursor.overflow = false;
        fdb_key_bytes(&c->cursor, kv.key, (size_t)kv.key_length);
    }
    fdb_future_destroy(f);
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static enum mds_status fdb_node_list(struct mds_catalogue *cat, mds_cluster_node_cb cb,
                                     void *ctx)
{
    struct node_scan_ctx c;
    enum mds_status st = MDS_OK;

    if (be_of(cat) == NULL || cb == NULL) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.b = be_of(cat);
    c.page = calloc(FDB_NODE_PAGE, sizeof(*c.page));
    if (c.page == NULL) {
        return MDS_ERR_NOMEM;
    }
    for (;;) {
        uint32_t i;

        st = fdb_run_txn(c.b, FDB_TXN_READONLY, "node_list", node_scan_body, &c);
        if (st != MDS_OK) {
            break;
        }
        for (i = 0; i < c.n; i++) {
            const struct node_row *row = &c.page[i];

            if (cb(row->mds_id, row->val.boot_epoch, row->val.hostname, row->val.nfs_port,
                   row->val.grpc_port, row->val.last_heartbeat_ns, ctx) != 0) {
                goto out;
            }
        }
        if (!c.more) {
            break;
        }
    }
out:
    free(c.page);
    return st;
}

/* Rows whose last_heartbeat_ns < threshold_ns (same CLOCK_REALTIME
 * domain the heartbeat writes); nothing is older than the epoch. */
static enum mds_status fdb_node_scan_stale(struct mds_catalogue *cat, uint64_t threshold_ns,
                                           mds_cluster_stale_cb cb, void *ctx)
{
    struct node_scan_ctx c;
    enum mds_status st = MDS_OK;

    if (be_of(cat) == NULL || cb == NULL) {
        return MDS_ERR_INVAL;
    }
    if (threshold_ns == 0) {
        return MDS_OK;
    }
    memset(&c, 0, sizeof(c));
    c.b = be_of(cat);
    c.stale_before = threshold_ns;
    c.page = calloc(FDB_NODE_PAGE, sizeof(*c.page));
    if (c.page == NULL) {
        return MDS_ERR_NOMEM;
    }
    for (;;) {
        uint32_t i;

        st = fdb_run_txn(c.b, FDB_TXN_READONLY, "node_scan_stale", node_scan_body, &c);
        if (st != MDS_OK) {
            break;
        }
        for (i = 0; i < c.n; i++) {
            const struct node_row *row = &c.page[i];

            if (cb(row->mds_id, row->val.boot_epoch, row->val.last_heartbeat_ns, ctx) != 0) {
                goto out;
            }
        }
        if (!c.more) {
            break;
        }
    }
out:
    free(c.page);
    return st;
}

/* -----------------------------------------------------------------------
 * Partition map
 * ----------------------------------------------------------------------- */

struct partition_ctx {
    struct fdb_backend       *b;
    uint32_t                  partition_id;
    bool                      insert_only;
    uint32_t                  expected_owner;  /**< cas */
    struct fdb_partition_val  want;            /**< put: the row to write */
    struct fdb_partition_val  row;             /**< the stored row, once read */
    uint8_t                   enc[FDB_PARTITION_ENC_MAX];
};

static int partition_write(FDBTransaction *tr, struct partition_ctx *c,
                           const struct fdb_partition_val *v)
{
    struct fdb_key k;
    size_t len = 0;

    if (!fdb_partition_encode(v, c->enc, sizeof(c->enc), &len)) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    key_partition(&k, &c->b->prefix, c->partition_id);
    fdb_txn_set(tr, &k, c->enc, len);
    return 0;
}

/* insert_only: EXISTS when the row exists (the root claim); otherwise
 * an upsert, reserved for seeding a never-owned initial layout. */
static int partition_put_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct partition_ctx *c = arg;
    struct fdb_key k;
    bool found = false;
    fdb_error_t err;
    int rc;

    key_partition(&k, &c->b->prefix, c->partition_id);
    err = read_row(tr, &k, partition_decode_cb, &c->row, &found);
    if (err != 0) {
        return (int)err;
    }
    if (found && c->insert_only) {
        *st_out = MDS_ERR_EXISTS;
        return FDB_BODY_DONE;
    }
    rc = partition_write(tr, c, &c->want);
    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

/* Compare-and-swap of the owner: NOTFOUND when absent, STALE when the
 * stored owner is not the expected one (nothing written), else the
 * owner and state move and the path is kept. */
static int partition_cas_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct partition_ctx *c = arg;
    struct fdb_key k;
    bool found = false;
    fdb_error_t err;
    int rc;

    key_partition(&k, &c->b->prefix, c->partition_id);
    err = read_row(tr, &k, partition_decode_cb, &c->row, &found);
    if (err != 0) {
        return (int)err;
    }
    if (!found) {
        *st_out = MDS_ERR_NOTFOUND;
        return FDB_BODY_DONE;
    }
    if (c->row.owner_mds_id != c->expected_owner) {
        *st_out = MDS_ERR_STALE;
        return FDB_BODY_DONE;
    }
    c->row.owner_mds_id = c->want.owner_mds_id;
    c->row.state = c->want.state;
    rc = partition_write(tr, c, &c->row);
    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

/* --- partition_list -------------------------------------------------------- */

struct partition_row {
    uint32_t                 id;
    struct fdb_partition_val val;
};

struct partition_scan_ctx {
    struct fdb_backend   *b;
    struct fdb_key        cursor;
    bool                  more;
    uint32_t              n;
    struct partition_row *page;   /**< FDB_PARTITION_PAGE rows. */
};

static int partition_scan_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct partition_scan_ctx *c = arg;
    struct fdb_key base;
    struct fdb_key_range r;
    FDBFuture *f;
    const FDBKeyValue *kvs = NULL;
    int count = 0;
    int i;
    fdb_error_t err;

    c->n = 0;
    c->more = false;
    fdb_key_init(&base, &c->b->prefix, FDB_KT_PARTITION_MAP);
    if (!fdb_key_range_prefix(&r, &base)) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    if (c->cursor.len > 0) {
        r.begin = c->cursor;
        fdb_key_u8(&r.begin, 0);
    }
    f = fdb_txn_get_range_start(tr, &r, (int)FDB_PARTITION_PAGE, false, false);
    err = fdb_txn_get_range_wait(f, &kvs, &count, &c->more);
    if (err != 0) {
        if (f != NULL) {
            fdb_future_destroy(f);
        }
        return (int)err;
    }
    for (i = 0; i < count && c->n < FDB_PARTITION_PAGE; i++) {
        FDBKeyValue kv;
        struct partition_row *row = &c->page[c->n];

        fdb_kv_at(kvs, i, &kv);
        if (kv.key_length != (int)base.len + 4 ||
            !fdb_partition_decode(kv.value, (size_t)kv.value_length, &row->val)) {
            fdb_future_destroy(f);
            c->n = 0;
            return FDB_ERR_PLATFORM_ERROR; /* corrupt row */
        }
        row->id = fdb_get_u32(kv.key + base.len);
        c->n++;
    }
    if (count > 0) {
        FDBKeyValue kv;

        fdb_kv_at(kvs, count - 1, &kv);
        c->cursor.len = 0;
        c->cursor.overflow = false;
        fdb_key_bytes(&c->cursor, kv.key, (size_t)kv.key_length);
    }
    fdb_future_destroy(f);
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static enum mds_status fdb_partition_list(struct mds_catalogue *cat,
                                          mds_cluster_partition_cb cb, void *ctx)
{
    struct partition_scan_ctx c;
    enum mds_status st = MDS_OK;

    if (be_of(cat) == NULL || cb == NULL) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.b = be_of(cat);
    c.page = calloc(FDB_PARTITION_PAGE, sizeof(*c.page));
    if (c.page == NULL) {
        return MDS_ERR_NOMEM;
    }
    for (;;) {
        uint32_t i;

        st = fdb_run_txn(c.b, FDB_TXN_READONLY, "partition_list", partition_scan_body, &c);
        if (st != MDS_OK) {
            break;
        }
        for (i = 0; i < c.n; i++) {
            const struct partition_row *row = &c.page[i];

            if (cb(row->id, row->val.owner_mds_id, row->val.state, row->val.subtree_path,
                   ctx) != 0) {
                goto out;
            }
        }
        if (!c.more) {
            break;
        }
    }
out:
    free(c.page);
    return st;
}

static enum mds_status fdb_partition_put(struct mds_catalogue *cat, uint32_t partition_id,
                                         uint32_t owner_mds_id, uint8_t state,
                                         const char *subtree_path, bool insert_only)
{
    struct partition_ctx *c;
    enum mds_status st;

    if (be_of(cat) == NULL || subtree_path == NULL || subtree_path[0] == '\0' ||
        strnlen(subtree_path, MDS_MAX_PATH) >= MDS_MAX_PATH) {
        return MDS_ERR_INVAL;
    }
    c = calloc(1, sizeof(*c));
    if (c == NULL) {
        return MDS_ERR_NOMEM;
    }
    c->b = be_of(cat);
    c->partition_id = partition_id;
    c->insert_only = insert_only;
    c->want.owner_mds_id = owner_mds_id;
    c->want.state = state;
    (void)snprintf(c->want.subtree_path, sizeof(c->want.subtree_path), "%s", subtree_path);
    st = fdb_run_txn(c->b, FDB_TXN_MUTATING, "partition_put", partition_put_body, c);
    free(c);
    return st;
}

/* The failover takeover's persistence step (mds_cluster.h): the owner
 * moves only while the row still names @p expected_owner. */
static enum mds_status fdb_partition_cas(struct mds_catalogue *cat, uint32_t partition_id,
                                         uint32_t expected_owner, uint32_t new_owner,
                                         uint8_t new_state)
{
    struct partition_ctx *c;
    enum mds_status st;

    if (be_of(cat) == NULL) {
        return MDS_ERR_INVAL;
    }
    c = calloc(1, sizeof(*c));
    if (c == NULL) {
        return MDS_ERR_NOMEM;
    }
    c->b = be_of(cat);
    c->partition_id = partition_id;
    c->expected_owner = expected_owner;
    c->want.owner_mds_id = new_owner;
    c->want.state = new_state;
    st = fdb_run_txn(c->b, FDB_TXN_MUTATING, "partition_cas", partition_cas_body, c);
    free(c);
    return st;
}

/* -----------------------------------------------------------------------
 * Vtable.  Every cluster slot is populated, so mds_cluster_supported()
 * is true for this backend (caps carry MDS_CAT_CAP_MULTI_PROCESS).
 * ----------------------------------------------------------------------- */

const struct mds_cluster_ops fdb_cluster_ops = {
    .node_register   = fdb_node_register,
    .node_heartbeat  = fdb_node_heartbeat,
    .node_deregister = fdb_node_deregister,
    .node_list       = fdb_node_list,
    .node_scan_stale = fdb_node_scan_stale,
    .partition_list  = fdb_partition_list,
    .partition_put   = fdb_partition_put,
    .partition_cas   = fdb_partition_cas,
};
