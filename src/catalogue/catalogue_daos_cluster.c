/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * catalogue_daos_cluster.c -- DAOS backend: cluster slots.
 *
 * The node registry and the partition map of mds_cluster.h (the Phase
 * 1b contract), one daos_run_txn per slot.  Every epoch- or owner-guarded
 * write is a read-then-update inside ONE serializable transaction: the
 * row read is part of the transaction's read set, so two daemons racing
 * on the same mds_id or partition conflict at commit, the loser re-runs
 * (-DER_TX_RESTART) against the winner's committed row, and the epoch
 * and owner rules are applied to what is actually stored, never to a
 * stale snapshot.  The one guard the store can evaluate itself -- "the
 * row must not exist" of the insert-only partition_put -- is a
 * conditional insert (DAOS_COND_DKEY_INSERT) with no read at all.
 *
 * Rows (daos_keys.h; values in fdb_codec.h):
 *   NODE_REGISTRY dkey be32 mds_id  akey 'v' -> fdb_node codec (boot_epoch,
 *                                   ports, state, last_heartbeat_ns,
 *                                   witness_epoch, sw_version, hostname)
 *   PARTITION_MAP dkey be32 id      akey 'v' -> fdb_partition codec (owner,
 *                                   state, path)
 *
 * witness_epoch is written as 0: this backend has no commit-outcome
 * witness protocol (daos_txn.h), the field only keeps the row format
 * shared with the FoundationDB unit.  node_heartbeat re-encodes the
 * decoded row, so whatever a row carries travels along.
 *
 * Timestamps are the writer's CLOCK_REALTIME nanoseconds (mds_cluster.h
 * clock-domain rule).  Enumerations list the sorted be32 dkeys of the
 * object and fetch every row of the page in one parallel wave inside a
 * read-only transaction, then deliver the page (C1/C2); the registry
 * holds at most MDS_MAX_NODES rows, the map the configured partitions.
 *
 * Transaction shapes (F = fetch RPC, L = key listing, U = buffered
 * update, P = buffered punch, || = one parallel wave; every mutating
 * transaction ends in one commit RPC):
 *   node_register     F row; absent or lower epoch: U row (witness_epoch 0)
 *                     / else EXISTS
 *   node_heartbeat    F row; NOTFOUND / STALE / U row with the new stamp
 *   node_deregister   F row; absent: OK / STALE / P row dkey
 *   node_list         per page: L NODE_REGISTRY dkeys -> F || ... || F rows
 *                     (MDS_MAX_NODES rows)                          (read-only)
 *   node_scan_stale   the same, filter heartbeat < threshold          (read-only)
 *   partition_list    per page: L PARTITION_MAP dkeys -> F || ... || F rows
 *                     (64 rows)                                     (read-only)
 *   partition_put     insert_only: U row, DAOS_COND_DKEY_INSERT (EXISTS when
 *                     the row exists) / else U row
 *   partition_cas     F row; NOTFOUND / owner mismatch: STALE / U row
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "catalogue_daos.h"
#include "catalogue_daos_internal.h"
#include "catalogue_internal.h"
#include "daos_keys.h"
#include "daos_txn.h"
#include "fdb_codec.h"
#include "mds_log.h"

/* Registry rows per read-only page; the registry never holds more. */
#define DAOS_NODE_PAGE      ((uint32_t)MDS_MAX_NODES)
/* Partition rows per read-only page (a row carries up to MDS_MAX_PATH). */
#define DAOS_PARTITION_PAGE 64U

/* Node registry state of a registered daemon (RonDB column encoding). */
#define DAOS_NODE_STATE_ACTIVE 0U

/* -----------------------------------------------------------------------
 * Helpers
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

/* Fetch the 'v' row of the be32 dkey @p id on @p oh into @p buf and
 * decode it through @p decode; *found false when absent.  A value the
 * decoder rejects or larger than @p cap is a corrupt row (-DER_IO). */
static int row_read(struct daos_txn *t, daos_handle_t oh, uint32_t id, uint8_t *buf, size_t cap,
                    bool (*decode)(const uint8_t *, size_t, void *), void *out, bool *found)
{
    struct daos_key dk;
    struct daos_key ak;
    size_t len = 0;
    int rc;

    *found = false;
    daos_dkey_be32(&dk, id);
    daos_akey_value(&ak);
    rc = daos_fetch_single(t, oh, &dk, &ak, buf, cap, &len, found);
    if (rc == -DER_REC2BIG) {
        return -DER_IO;
    }
    if (rc != 0) {
        return rc;
    }
    if (*found && !decode(buf, len, out)) {
        return -DER_IO;
    }
    return 0;
}

/* Blind update of the 'v' row of the be32 dkey @p id with @p flags
 * (0 or DAOS_COND_DKEY_INSERT). */
static int row_write(struct daos_txn *t, daos_handle_t oh, uint32_t id, const uint8_t *enc,
                     size_t len, uint64_t flags)
{
    struct daos_key dk;
    struct daos_key ak;

    daos_dkey_be32(&dk, id);
    daos_akey_value(&ak);
    return daos_update_single(t, oh, &dk, &ak, enc, len, flags, 0);
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
 * One page of a be32-keyed registry object: the sorted dkeys after the
 * cursor and the 'v' value of each, fetched in one parallel wave
 * (daos_txn.h).  Heap allocated once per call; the page is reused
 * across the read-only transactions of one scan.
 * ----------------------------------------------------------------------- */

struct reg_page {
    daos_key_desc_t         *kds;      /**< cap descriptors. */
    uint8_t                 *keys;     /**< cap x DAOS_KEY_MAX listed key bytes. */
    struct daos_fetch_small *reqs;     /**< cap requests. */
    struct daos_key         *dkeys;    /**< cap dkeys (referenced by the requests). */
    uint8_t                 *vals;     /**< cap x val_max row bytes. */
    struct daos_key          akey;     /**< 'v', shared by every request. */
    size_t                   val_max;
    uint32_t                 cap;
    struct daos_list         l;
    struct daos_key          cursor;   /**< Last key of the previous page; empty = start. */
    bool                     more;
};

static void reg_page_free(struct reg_page *p)
{
    free(p->kds);
    free(p->keys);
    free(p->reqs);
    free(p->dkeys);
    free(p->vals);
    p->kds = NULL;
    p->keys = NULL;
    p->reqs = NULL;
    p->dkeys = NULL;
    p->vals = NULL;
}

static enum mds_status reg_page_alloc(struct reg_page *p, uint32_t cap, size_t val_max)
{
    memset(p, 0, sizeof(*p));
    p->kds = calloc(cap, sizeof(*p->kds));
    p->keys = calloc(cap, DAOS_KEY_MAX);
    p->reqs = calloc(cap, sizeof(*p->reqs));
    p->dkeys = calloc(cap, sizeof(*p->dkeys));
    p->vals = malloc((size_t)cap * val_max);
    if (p->kds == NULL || p->keys == NULL || p->reqs == NULL || p->dkeys == NULL ||
        p->vals == NULL) {
        reg_page_free(p);
        return MDS_ERR_NOMEM;
    }
    p->cap = cap;
    p->val_max = val_max;
    daos_akey_value(&p->akey);
    daos_key_init(&p->cursor);
    return MDS_OK;
}

/* Start the row fetch of every listed key; every key must be a be32 id. */
static int reg_page_start(struct daos_txn *t, daos_handle_t oh, struct reg_page *p,
                          uint32_t *started)
{
    size_t off = 0;
    uint32_t i;
    int rc = 0;

    *started = 0;
    for (i = 0; i < p->l.nr && rc == 0; i++) {
        size_t len = (size_t)p->kds[i].kd_key_len;
        struct daos_fetch_small *r = &p->reqs[i];

        if (len != DAOS_KEY_BE32_LEN) {
            return -DER_IO; /* a foreign or corrupt key */
        }
        daos_key_init(&p->dkeys[i]);
        daos_key_bytes(&p->dkeys[i], p->keys + off, len);
        off += len;
        daos_fetch_small_init(r, &p->dkeys[i]);
        if (!daos_fetch_add(&r->f, &p->akey, p->vals + (size_t)i * p->val_max, p->val_max)) {
            return -DER_INVAL;
        }
        rc = daos_fetch_start(t, oh, &r->f);
        if (rc == 0) {
            (*started)++;
        }
    }
    return rc;
}

/* List the next page of dkeys of @p oh after the cursor and fetch their
 * rows.  On success p->l.nr rows are readable through reg_page_row() and
 * the cursor has advanced past the page; p->more reports a next page. */
static int reg_page_read(struct daos_txn *t, daos_handle_t oh, struct reg_page *p)
{
    struct daos_key *resume = (p->cursor.len > 0) ? &p->cursor : NULL;
    uint32_t started = 0;
    uint32_t i;
    int rc;

    p->more = false;
    daos_list_init(&p->l, p->kds, p->cap, p->keys, (size_t)p->cap * DAOS_KEY_MAX);
    rc = daos_list_dkeys(t, oh, resume, &p->l);
    if (rc != 0) {
        return rc;
    }
    rc = reg_page_start(t, oh, p, &started);
    for (i = 0; i < started; i++) {
        int rc2 = daos_fetch_finish(t, &p->reqs[i].f);

        if (rc == 0) {
            rc = (rc2 == -DER_REC2BIG) ? -DER_IO : rc2;
        }
    }
    if (rc != 0) {
        return rc;
    }
    if (p->l.nr > 0) {
        p->cursor = p->dkeys[p->l.nr - 1];
        p->more = !p->l.eof;
    }
    return 0;
}

/* Row @p i of a read page: its be32 id and value (len 0 = absent). */
static void reg_page_row(const struct reg_page *p, uint32_t i, uint32_t *id,
                         const uint8_t **val, size_t *len)
{
    *id = fdb_get_u32(p->dkeys[i].buf);
    *val = p->vals + (size_t)i * p->val_max;
    *len = daos_fetch_size(&p->reqs[i].f, 0);
}

/* -----------------------------------------------------------------------
 * Node registry
 * ----------------------------------------------------------------------- */

struct node_ctx {
    struct daos_backend *b;
    uint32_t             mds_id;
    uint64_t             boot_epoch;
    struct fdb_node_val  want;   /**< register: the row to write */
    struct fdb_node_val  row;    /**< the stored row, once read */
    uint8_t              enc[FDB_NODE_ENC_MAX];
};

static int node_write(struct daos_txn *t, daos_handle_t oh, struct node_ctx *c,
                      const struct fdb_node_val *v)
{
    size_t len = 0;

    if (!fdb_node_encode(v, c->enc, sizeof(c->enc), &len)) {
        return -DER_INVAL;
    }
    return row_write(t, oh, c->mds_id, c->enc, len, 0);
}

/* Conditional upsert: insert when absent, replace when the stored
 * boot_epoch is lower, MDS_ERR_EXISTS otherwise (a duplicate live
 * mds_id is split-brain and is refused). */
static int node_register_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct node_ctx *c = arg;
    daos_handle_t oh;
    bool found = false;
    int rc;

    rc = daos_txn_obj(t, DAOS_TAG_NODE_REGISTRY, 0, &oh);
    if (rc != 0) {
        return rc;
    }
    rc = row_read(t, oh, c->mds_id, c->enc, sizeof(c->enc), node_decode_cb, &c->row, &found);
    if (rc != 0) {
        return rc;
    }
    if (found && c->row.boot_epoch >= c->want.boot_epoch) {
        *st_out = MDS_ERR_EXISTS;
        return DAOS_BODY_DONE;
    }
    c->want.last_heartbeat_ns = realtime_ns();
    rc = node_write(t, oh, c, &c->want);
    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

/* Update only when the row exists AND its epoch matches: NOTFOUND when
 * absent, STALE on a mismatch (an old incarnation never overwrites its
 * replacement); only the timestamp changes -- the decoded row is
 * written back, so every other field travels along. */
static int node_heartbeat_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct node_ctx *c = arg;
    daos_handle_t oh;
    bool found = false;
    int rc;

    rc = daos_txn_obj(t, DAOS_TAG_NODE_REGISTRY, 0, &oh);
    if (rc != 0) {
        return rc;
    }
    rc = row_read(t, oh, c->mds_id, c->enc, sizeof(c->enc), node_decode_cb, &c->row, &found);
    if (rc != 0) {
        return rc;
    }
    if (!found) {
        *st_out = MDS_ERR_NOTFOUND;
        return DAOS_BODY_DONE;
    }
    if (c->row.boot_epoch != c->boot_epoch) {
        *st_out = MDS_ERR_STALE;
        return DAOS_BODY_DONE;
    }
    c->row.last_heartbeat_ns = realtime_ns();
    rc = node_write(t, oh, c, &c->row);
    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

/* Delete only on an epoch match; an absent row is MDS_OK (a retried
 * shutdown is harmless), a mismatch is STALE and deletes nothing. */
static int node_deregister_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct node_ctx *c = arg;
    struct daos_key dk;
    daos_handle_t oh;
    bool found = false;
    int rc;

    rc = daos_txn_obj(t, DAOS_TAG_NODE_REGISTRY, 0, &oh);
    if (rc != 0) {
        return rc;
    }
    rc = row_read(t, oh, c->mds_id, c->enc, sizeof(c->enc), node_decode_cb, &c->row, &found);
    if (rc != 0) {
        return rc;
    }
    if (!found) {
        *st_out = MDS_OK;
        return DAOS_BODY_DONE;
    }
    if (c->row.boot_epoch != c->boot_epoch) {
        *st_out = MDS_ERR_STALE;
        return DAOS_BODY_DONE;
    }
    daos_dkey_be32(&dk, c->mds_id);
    rc = daos_punch_dkeys(t, oh, &dk, 1, 0);
    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

static enum mds_status daos_node_register(struct mds_catalogue *cat, uint32_t mds_id,
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
    c->want.witness_epoch = 0; /* no witness protocol on this backend */
    c->want.nfs_port = nfs_port;
    c->want.grpc_port = grpc_port;
    c->want.state = DAOS_NODE_STATE_ACTIVE;
    (void)snprintf(c->want.sw_version, sizeof(c->want.sw_version), "%u.%u.%u",
                   (unsigned)PNFS_MDS_VERSION_MAJOR, (unsigned)PNFS_MDS_VERSION_MINOR,
                   (unsigned)PNFS_MDS_VERSION_PATCH);
    (void)snprintf(c->want.hostname, sizeof(c->want.hostname), "%s", hostname);
    st = daos_run_txn(c->b, DAOS_TXN_MUTATING, "node_register", node_register_body, c);
    free(c);
    if (st == MDS_OK) {
        /* This incarnation owns the registry row: rows that carry an
         * owner epoch (client recovery) stamp this epoch from now on. */
        (void)daos_backend_set_boot_epoch(cat, boot_epoch);
    }
    return st;
}

static enum mds_status daos_node_heartbeat(struct mds_catalogue *cat, uint32_t mds_id,
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
    st = daos_run_txn(c->b, DAOS_TXN_MUTATING, "node_heartbeat", node_heartbeat_body, c);
    free(c);
    return st;
}

static enum mds_status daos_node_deregister(struct mds_catalogue *cat, uint32_t mds_id,
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
    st = daos_run_txn(c->b, DAOS_TXN_MUTATING, "node_deregister", node_deregister_body, c);
    free(c);
    return st;
}

/* --- node_list / node_scan_stale ------------------------------------------ */

struct node_row {
    uint32_t            mds_id;
    struct fdb_node_val val;
};

struct node_scan_ctx {
    struct daos_backend *b;
    uint64_t             stale_before;  /**< 0 = every row. */
    struct reg_page      pg;
    uint32_t             n;
    struct node_row     *page;          /**< DAOS_NODE_PAGE rows. */
};

/* One page of registry rows after the cursor; stale_before != 0 keeps
 * only rows whose heartbeat is older. */
static int node_scan_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct node_scan_ctx *c = arg;
    daos_handle_t oh;
    uint32_t i;
    int rc;

    c->n = 0;
    rc = daos_txn_obj(t, DAOS_TAG_NODE_REGISTRY, 0, &oh);
    if (rc != 0) {
        return rc;
    }
    rc = reg_page_read(t, oh, &c->pg);
    if (rc != 0) {
        return rc;
    }
    for (i = 0; i < c->pg.l.nr; i++) {
        struct node_row *row = &c->page[c->n];
        const uint8_t *val;
        size_t len;

        reg_page_row(&c->pg, i, &row->mds_id, &val, &len);
        if (len == 0 || !fdb_node_decode(val, len, &row->val)) {
            c->n = 0;
            return -DER_IO; /* a listed key without a valid row: corrupt */
        }
        if (c->stale_before == 0 || row->val.last_heartbeat_ns < c->stale_before) {
            c->n++;
        }
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

static enum mds_status node_scan_prepare(const struct mds_catalogue *cat, uint64_t stale_before,
                                         struct node_scan_ctx *c)
{
    memset(c, 0, sizeof(*c));
    c->b = be_of(cat);
    c->stale_before = stale_before;
    c->page = calloc(DAOS_NODE_PAGE, sizeof(*c->page));
    if (c->page == NULL) {
        return MDS_ERR_NOMEM;
    }
    if (reg_page_alloc(&c->pg, DAOS_NODE_PAGE, FDB_NODE_ENC_MAX) != MDS_OK) {
        free(c->page);
        c->page = NULL;
        return MDS_ERR_NOMEM;
    }
    return MDS_OK;
}

static void node_scan_release(struct node_scan_ctx *c)
{
    reg_page_free(&c->pg);
    free(c->page);
    c->page = NULL;
}

static enum mds_status daos_node_list(struct mds_catalogue *cat, mds_cluster_node_cb cb,
                                      void *ctx)
{
    struct node_scan_ctx c;
    enum mds_status st;

    if (be_of(cat) == NULL || cb == NULL) {
        return MDS_ERR_INVAL;
    }
    st = node_scan_prepare(cat, 0, &c);
    if (st != MDS_OK) {
        return st;
    }
    for (;;) {
        uint32_t i;

        st = daos_run_txn(c.b, DAOS_TXN_READONLY, "node_list", node_scan_body, &c);
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
        if (!c.pg.more) {
            break;
        }
    }
out:
    node_scan_release(&c);
    return st;
}

/* Rows whose last_heartbeat_ns < threshold_ns (same CLOCK_REALTIME
 * domain the heartbeat writes); nothing is older than the epoch. */
static enum mds_status daos_node_scan_stale(struct mds_catalogue *cat, uint64_t threshold_ns,
                                            mds_cluster_stale_cb cb, void *ctx)
{
    struct node_scan_ctx c;
    enum mds_status st;

    if (be_of(cat) == NULL || cb == NULL) {
        return MDS_ERR_INVAL;
    }
    if (threshold_ns == 0) {
        return MDS_OK;
    }
    st = node_scan_prepare(cat, threshold_ns, &c);
    if (st != MDS_OK) {
        return st;
    }
    for (;;) {
        uint32_t i;

        st = daos_run_txn(c.b, DAOS_TXN_READONLY, "node_scan_stale", node_scan_body, &c);
        if (st != MDS_OK) {
            break;
        }
        for (i = 0; i < c.n; i++) {
            const struct node_row *row = &c.page[i];

            if (cb(row->mds_id, row->val.boot_epoch, row->val.last_heartbeat_ns, ctx) != 0) {
                goto out;
            }
        }
        if (!c.pg.more) {
            break;
        }
    }
out:
    node_scan_release(&c);
    return st;
}

/* -----------------------------------------------------------------------
 * Partition map
 * ----------------------------------------------------------------------- */

/* Heap allocated once per call (a row carries up to MDS_MAX_PATH). */
struct partition_ctx {
    struct daos_backend      *b;
    uint32_t                  partition_id;
    bool                      insert_only;
    uint32_t                  expected_owner;  /**< cas */
    struct fdb_partition_val  want;            /**< put: the row to write */
    struct fdb_partition_val  row;             /**< the stored row, once read */
    uint8_t                   enc[FDB_PARTITION_ENC_MAX];
};

static int partition_write(struct daos_txn *t, daos_handle_t oh, struct partition_ctx *c,
                           const struct fdb_partition_val *v, uint64_t flags)
{
    size_t len = 0;

    if (!fdb_partition_encode(v, c->enc, sizeof(c->enc), &len)) {
        return -DER_INVAL;
    }
    return row_write(t, oh, c->partition_id, c->enc, len, flags);
}

/* insert_only: the root claim -- a conditional insert the store refuses
 * with -DER_EXIST (at the call or at commit) when the row exists, which
 * the runner reports as MDS_ERR_EXISTS; no read.  Otherwise an upsert,
 * reserved for seeding a never-owned initial layout. */
static int partition_put_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct partition_ctx *c = arg;
    daos_handle_t oh;
    int rc;

    rc = daos_txn_obj(t, DAOS_TAG_PARTITION_MAP, 0, &oh);
    if (rc != 0) {
        return rc;
    }
    rc = partition_write(t, oh, c, &c->want,
                         c->insert_only ? (uint64_t)DAOS_COND_DKEY_INSERT : 0U);
    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

/* Compare-and-swap of the owner: NOTFOUND when absent, STALE when the
 * stored owner is not the expected one (nothing written), else the
 * owner and state move and the path is kept. */
static int partition_cas_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct partition_ctx *c = arg;
    daos_handle_t oh;
    bool found = false;
    int rc;

    rc = daos_txn_obj(t, DAOS_TAG_PARTITION_MAP, 0, &oh);
    if (rc != 0) {
        return rc;
    }
    rc = row_read(t, oh, c->partition_id, c->enc, sizeof(c->enc), partition_decode_cb, &c->row,
                  &found);
    if (rc != 0) {
        return rc;
    }
    if (!found) {
        *st_out = MDS_ERR_NOTFOUND;
        return DAOS_BODY_DONE;
    }
    if (c->row.owner_mds_id != c->expected_owner) {
        *st_out = MDS_ERR_STALE;
        return DAOS_BODY_DONE;
    }
    c->row.owner_mds_id = c->want.owner_mds_id;
    c->row.state = c->want.state;
    rc = partition_write(t, oh, c, &c->row, 0);
    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

/* --- partition_list -------------------------------------------------------- */

struct partition_row {
    uint32_t                 id;
    struct fdb_partition_val val;
};

struct partition_scan_ctx {
    struct daos_backend  *b;
    struct reg_page       pg;
    uint32_t              n;
    struct partition_row *page;   /**< DAOS_PARTITION_PAGE rows. */
};

static int partition_scan_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct partition_scan_ctx *c = arg;
    daos_handle_t oh;
    uint32_t i;
    int rc;

    c->n = 0;
    rc = daos_txn_obj(t, DAOS_TAG_PARTITION_MAP, 0, &oh);
    if (rc != 0) {
        return rc;
    }
    rc = reg_page_read(t, oh, &c->pg);
    if (rc != 0) {
        return rc;
    }
    for (i = 0; i < c->pg.l.nr; i++) {
        struct partition_row *row = &c->page[c->n];
        const uint8_t *val;
        size_t len;

        reg_page_row(&c->pg, i, &row->id, &val, &len);
        if (len == 0 || !fdb_partition_decode(val, len, &row->val)) {
            c->n = 0;
            return -DER_IO; /* corrupt row */
        }
        c->n++;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

static enum mds_status daos_partition_list(struct mds_catalogue *cat,
                                           mds_cluster_partition_cb cb, void *ctx)
{
    struct partition_scan_ctx c;
    enum mds_status st = MDS_OK;

    if (be_of(cat) == NULL || cb == NULL) {
        return MDS_ERR_INVAL;
    }
    memset(&c, 0, sizeof(c));
    c.b = be_of(cat);
    c.page = calloc(DAOS_PARTITION_PAGE, sizeof(*c.page));
    if (c.page == NULL) {
        return MDS_ERR_NOMEM;
    }
    if (reg_page_alloc(&c.pg, DAOS_PARTITION_PAGE, FDB_PARTITION_ENC_MAX) != MDS_OK) {
        free(c.page);
        return MDS_ERR_NOMEM;
    }
    for (;;) {
        uint32_t i;

        st = daos_run_txn(c.b, DAOS_TXN_READONLY, "partition_list", partition_scan_body, &c);
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
        if (!c.pg.more) {
            break;
        }
    }
out:
    reg_page_free(&c.pg);
    free(c.page);
    return st;
}

static enum mds_status daos_partition_put(struct mds_catalogue *cat, uint32_t partition_id,
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
    st = daos_run_txn(c->b, DAOS_TXN_MUTATING, "partition_put", partition_put_body, c);
    free(c);
    return st;
}

/* The failover takeover's persistence step (mds_cluster.h): the owner
 * moves only while the row still names @p expected_owner. */
static enum mds_status daos_partition_cas(struct mds_catalogue *cat, uint32_t partition_id,
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
    st = daos_run_txn(c->b, DAOS_TXN_MUTATING, "partition_cas", partition_cas_body, c);
    free(c);
    return st;
}

/* -----------------------------------------------------------------------
 * Vtable.  Every cluster slot is populated, so mds_cluster_supported()
 * is true for this backend (caps carry MDS_CAT_CAP_MULTI_PROCESS).
 * ----------------------------------------------------------------------- */

const struct mds_cluster_ops daos_cluster_ops = {
    .node_register   = daos_node_register,
    .node_heartbeat  = daos_node_heartbeat,
    .node_deregister = daos_node_deregister,
    .node_list       = daos_node_list,
    .node_scan_stale = daos_node_scan_stale,
    .partition_list  = daos_partition_list,
    .partition_put   = daos_partition_put,
    .partition_cas   = daos_partition_cas,
};
