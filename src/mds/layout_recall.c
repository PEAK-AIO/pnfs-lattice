/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * layout_recall.c — Layout recall / revocation coordinator.
 *
 * DS-failure path: best-effort CB_LAYOUTRECALL, authoritative revoke.
 *
 * When a DS goes offline (ds_health callback fires):
 *   1. Scan ds_layout_idx for all (clientid, fileid) on the DS.
 *   2. Collect unique clientids.
 *   3. If a session table is set, snapshot callback targets under the
 *      session table lock, then attempt CB_LAYOUTRECALL for each.
 *   4. Regardless of CB outcome, revoke all affected layouts.
 *
 * Callback failure (no backchannel, send error, timeout) is non-fatal.
 * The revoke is authoritative — clients discover revocation on their
 * next SEQUENCE or LAYOUTGET.
 *
 * See include/layout_recall.h for API and docs/architecture.md §15.
 */

#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stdio.h>
#include <unistd.h>

#include "layout_recall.h"
#include "mds_catalogue.h"
#include "mds_coordination.h"
#include "commit_queue.h"
#include "open_state.h"
#include "session.h"
#include "nfs4_cb.h"
#include "rpc_server.h"
#include "mds_shard.h"

/* Default revoke timeout — used as CB send timeout per client. */
#define DEFAULT_REVOKE_MS  30000

/* Maximum entries collected per DS scan. */
#define MAX_RECALL_ENTRIES  4096

/* Maximum unique clientids we track per recall batch. */
#define MAX_RECALL_CLIENTS  256

struct layout_recall {
    struct mds_catalogue    *cat;
    struct commit_queue     *cq;
    struct session_table    *st;       /* Optional: for CB delivery */
    uint32_t                 revoke_ms;
};

/* -----------------------------------------------------------------------
 * Scan context — collects (clientid, fileid) pairs from ds_layout_idx
 * ----------------------------------------------------------------------- */

struct recall_entry {
    uint64_t clientid;
    uint64_t fileid;
    uint8_t  stateid_other[12]; /* real stateid for precise revoke */
};

struct recall_scan_ctx {
    struct recall_entry *entries;
    uint32_t             count;
    uint32_t             capacity;
};

static int recall_scan_cb(uint64_t clientid, uint64_t fileid, void *ctx)
{
    struct recall_scan_ctx *sc = ctx;

    if (sc->count >= sc->capacity) {
        return 1; /* stop scan — capacity reached */
    }

    sc->entries[sc->count].clientid = clientid;
    sc->entries[sc->count].fileid = fileid;
    sc->count++;
    return 0;
}

/* -----------------------------------------------------------------------
 * Callback target snapshot — collected under session table lock
 * ----------------------------------------------------------------------- */

struct cb_target {
    uint64_t clientid;
    uint8_t  session_id[SESSION_ID_SIZE];
    uint32_t cb_prog;
    uint32_t cb_sec_flavor;
    int      fd;             /* dup'd fd — caller must close */
    uint32_t slot_seq_id;
};

struct cb_target_list {
    struct cb_target targets[MAX_RECALL_CLIENTS];
    uint32_t         count;
    uint64_t         filter_clientids[MAX_RECALL_CLIENTS];
    uint32_t         filter_count;
};

/**
 * Session enumeration callback — runs under session table lock.
 * Extracts fd via rpc_conn_get_fd() and dup()s it so it survives
 * after the lock is released.
 */
static int snap_cb_target(const struct session_cb_snap *snap, void *ctx)
{
    struct cb_target_list *tl = ctx;
    uint32_t i;
    int fd;

    if (tl->count >= MAX_RECALL_CLIENTS) {
        return 1; /* stop — full */
    }

    /* Only snapshot sessions for clientids we care about. */
    for (i = 0; i < tl->filter_count; i++) {
        if (tl->filter_clientids[i] == snap->clientid) {
            break;
        }
    }
    if (i >= tl->filter_count) {
        return 0; /* not in our filter set — skip */
    }

    fd = rpc_conn_get_fd(snap->cb_conn);
    if (fd < 0) {
        return 0; /* no valid fd — skip */
    }

    int dup_fd = dup(fd);
    if (dup_fd < 0) {
        return 0; /* resource exhaustion — skip */
    }

    struct cb_target *t = &tl->targets[tl->count];
    t->clientid = snap->clientid;
    memcpy(t->session_id, snap->session_id, SESSION_ID_SIZE);
    t->cb_prog = snap->cb_prog;
    t->cb_sec_flavor = snap->cb_sec_flavor;
    t->fd = dup_fd;
    t->slot_seq_id = snap->slot_seq_id;
    tl->count++;

    return 0;
}

/* -----------------------------------------------------------------------
 * Best-effort CB_LAYOUTRECALL send
 * ----------------------------------------------------------------------- */

/**
 * Build a temporary nfs4_session from snapshot data and call
 * nfs4_cb_layoutrecall().  Best-effort: failures are logged, not fatal.
 */
static void attempt_cb_for_target(const struct cb_target *t,
                                  uint32_t timeout_ms)
{
    struct nfs4_cb_layoutrecall_args args;

    /* Send LAYOUTRECALL4_ALL — the DS is offline, recall everything. */
    memset(&args, 0, sizeof(args));
    args.layout_type = 1;  /* LAYOUT4_NFSV4_1_FILES */
    args.iomode = 3;       /* LAYOUTIOMODE4_ANY */
    args.recall_type = LAYOUTRECALL4_ALL;

    int rc = nfs4_cb_layoutrecall_fd(t->fd, t->session_id,
                                      t->cb_prog, t->slot_seq_id,
                                      1, &args, timeout_ms);
    if (rc != 0) {
        (void)fprintf(stderr, "layout_recall: CB_LAYOUTRECALL to "
                "clientid=%lu failed (rc=%d) — will revoke\n",
                (unsigned long)t->clientid, rc);
    }
}


/* -----------------------------------------------------------------------
 * Revoke (force-delete) a single layout + index entry via CQ or direct
 * ----------------------------------------------------------------------- */

/**
 * Revoke a single layout via CQ (replicated) or catalogue vtable.
 *
 * @param stateid_other  Real 12-byte stateid.other from the layout grant.
 *                       Must not be NULL for RonDB path (exact PK match).
 * @param ds_id          DS to clean up in the reverse index (0 = no DS cleanup).
 */
static void revoke_layout(struct layout_recall *lr,
                          const uint8_t stateid_other[12],
                          uint64_t clientid, uint64_t fileid,
                          uint32_t ds_id)
{
    if (lr->cq != NULL) {
        struct commit_op cop;
        struct commit_op_layout_del *ld = &cop.args.layout_del;

        memset(&cop, 0, sizeof(cop));
        cop.type = COMMIT_OP_LAYOUT_STATE_DEL;
        ld->clientid = clientid;
        ld->fileid = fileid;
        memcpy(ld->stateid_other, stateid_other, 12);
        /* Borrow the stack-local ds_id arg.  Per the
         * commit_queue_submit() lifetime contract, the borrowed
         * pointer only needs to outlive the synchronous submit
         * call below; the function's own stack frame is still
         * live at that point. */
        ld->ds_ids = (ds_id != 0) ? &ds_id : NULL;
        ld->ds_count = (ds_id != 0) ? 1 : 0;
        (void)commit_queue_submit(lr->cq, &cop);
    } else if (lr->cat != NULL) {
        (void)mds_coord_layout_return(lr->cat, NULL, stateid_other,
                                       clientid, fileid,
                                       (ds_id != 0) ? &ds_id : NULL,
                                       (ds_id != 0) ? 1 : 0);
    }
}

/* Forward declaration: defined below in the file-recall section. */
static int file_holder_cb(uint64_t clientid,
                          const struct nfs4_stateid *stateid,
                          uint32_t iomode, void *ctx);
struct ds_layout_match_ctx {
    uint64_t clientid;
    uint64_t fileid;
    bool found;
};

static int ds_layout_match_cb(uint64_t clientid, uint64_t fileid, void *ctx)
{
    struct ds_layout_match_ctx *match = ctx;

    if (match != NULL &&
        match->clientid == clientid &&
        match->fileid == fileid) {
        match->found = true;
        return 1; /* stop scan */
    }
    return 0;
}

/**
 * Resolve stateid_other for recall entries collected from ds_layout_idx.
 * ds_layout_idx only stores (clientid, fileid); the real stateid must be
 * looked up from layout_state via the file iterator.
 *
 * For multi-holder files, iterates all holders and matches by clientid.
 */
static void resolve_stateids_for_ds_entries(
    struct mds_catalogue *cat,
    struct recall_entry *entries, uint32_t count)
{
    for (uint32_t i = 0; i < count; i++) {
        struct recall_scan_ctx inner;
        struct recall_entry tmp[MAX_RECALL_CLIENTS];

        inner.entries = tmp;
        inner.count = 0;
        inner.capacity = MAX_RECALL_CLIENTS;

        if (mds_coord_layout_iter_file(cat, entries[i].fileid,
                                        file_holder_cb, &inner) != MDS_OK) {
            continue;
        }

        /* Match by clientid to get the correct stateid. */
        for (uint32_t j = 0; j < inner.count; j++) {
            if (tmp[j].clientid == entries[i].clientid) {
                memcpy(entries[i].stateid_other,
                       tmp[j].stateid_other, 12);
                break;
            }
        }
    }
}

static void revoke_layout_for_file_entry(struct layout_recall *lr,
                                         const struct recall_entry *entry)
{
    struct mds_ds_info *ds_list = NULL;
    uint32_t ds_count = 0;
    bool revoked_any = false;
    enum mds_status st;

    if (lr == NULL || entry == NULL) {
        return;
    }

    if (lr->cat == NULL) {
        revoke_layout(lr, entry->stateid_other,
                      entry->clientid, entry->fileid, 0);
        return;
    }

    st = mds_cat_ds_list(lr->cat, &ds_list, &ds_count);
    if (st != MDS_OK || ds_list == NULL || ds_count == 0) {
        free(ds_list);
        revoke_layout(lr, entry->stateid_other,
                      entry->clientid, entry->fileid, 0);
        return;
    }

    for (uint32_t i = 0; i < ds_count; i++) {
        struct ds_layout_match_ctx match;

        memset(&match, 0, sizeof(match));
        match.clientid = entry->clientid;
        match.fileid = entry->fileid;

        st = mds_coord_ds_layout_idx_scan(lr->cat, ds_list[i].ds_id,
                                          ds_layout_match_cb, &match);
        if (st == MDS_OK && match.found) {
            revoke_layout(lr, entry->stateid_other,
                          entry->clientid, entry->fileid,
                          ds_list[i].ds_id);
            revoked_any = true;
        }
    }

    free(ds_list);

    if (!revoked_any) {
        revoke_layout(lr, entry->stateid_other,
                      entry->clientid, entry->fileid, 0);
    }
}

/* -----------------------------------------------------------------------
 * Unique clientid collection helper
 * ----------------------------------------------------------------------- */

static uint32_t collect_unique_clientids(const struct recall_entry *entries,
                                          uint32_t entry_count,
                                          uint64_t *out,
                                          uint32_t out_capacity)
{
    uint32_t count = 0;
    uint32_t i, j;

    for (i = 0; i < entry_count && count < out_capacity; i++) {
        for (j = 0; j < count; j++) {
            if (out[j] == entries[i].clientid) {
                break;
            }
        }
        if (j == count) {
            out[count++] = entries[i].clientid;
        }
    }
    return count;
}

/* -----------------------------------------------------------------------
 * Public API
 * ----------------------------------------------------------------------- */

int layout_recall_init(const struct mds_catalogue *cat,
                       struct commit_queue *cq,
                       uint32_t revoke_ms,
                       struct layout_recall **out)
{
    struct layout_recall *lr;

    if (cat == NULL || out == NULL) {
        return -EINVAL;
    }

    lr = calloc(1, sizeof(*lr));
    if (lr == NULL) {
        return -ENOMEM;
    }

    lr->cat = (struct mds_catalogue *)cat;
    lr->cq = cq;
    lr->st = NULL;
    lr->revoke_ms = (revoke_ms > 0) ? revoke_ms : DEFAULT_REVOKE_MS;

    *out = lr;
    return 0;
}

void layout_recall_destroy(struct layout_recall *lr)
{
    free(lr);
}

void layout_recall_set_session_table(struct layout_recall *lr,
                                     struct session_table *st)
{
    if (lr != NULL) {
        lr->st = st;
    }
}

void layout_recall_set_shard(struct layout_recall *lr,
                             const struct mds_shard *shard)
{
    if (lr == NULL) {
        return;
    }
    if (shard != NULL) {
        lr->cq = shard->cq;
    } else {
        lr->cq = NULL;
    }
}

int layout_recall_for_ds(struct layout_recall *lr, uint32_t ds_id)
{
    struct recall_scan_ctx sc;
    struct recall_entry *entries = NULL;
    enum mds_status st;
    uint32_t i;

    if (lr == NULL) {
        return -EINVAL;
    }
    /* Step 1: Scan the reverse index for all layouts on this DS. */
    entries = calloc(MAX_RECALL_ENTRIES, sizeof(*entries));
    if (entries == NULL) {
        return -ENOMEM;
    }

    sc.entries = entries;
    sc.count = 0;
    sc.capacity = MAX_RECALL_ENTRIES;

    st = mds_coord_ds_layout_idx_scan(lr->cat, ds_id,
                                       recall_scan_cb, &sc);
    if (st != MDS_OK) {
        free(entries);
        return -EIO;
    }

    if (sc.count == 0) {
        free(entries);
        return 0; /* No layouts affected. */
    }

    /*
     * Step 2: Best-effort CB_LAYOUTRECALL to affected clients.
     *
     * If a session table is available, collect unique clientids,
     * snapshot callback targets, and attempt CB_LAYOUTRECALL.
     * Failures are non-fatal — we always proceed to revoke.
     */
    if (lr->st != NULL) {
        struct cb_target_list tl;
        memset(&tl, 0, sizeof(tl));

        tl.filter_count = collect_unique_clientids(
            entries, sc.count,
            tl.filter_clientids, MAX_RECALL_CLIENTS);

        /* Snapshot CB targets under session table lock. */
        session_for_each_with_cb(lr->st, snap_cb_target, &tl);

        /* Send CB_LAYOUTRECALL to each target (outside lock). */
        for (i = 0; i < tl.count; i++) {
            attempt_cb_for_target(&tl.targets[i], lr->revoke_ms);
            close(tl.targets[i].fd);
        }
    }

    /*
     * Step 3: Authoritative revoke — delete all affected layouts.
     *
     * This runs regardless of whether CB_LAYOUTRECALL succeeded.
     * Clients that acknowledged the recall will have already
     * returned their layouts; for those, the delete is a no-op.
     * Clients that did not respond discover revocation on their
     * next SEQUENCE or LAYOUTGET.
     */
    /* Resolve stateids for RonDB path — ds_layout_idx only has
     * (clientid, fileid), not stateid_other. */
    if (lr->cat != NULL && lr->cq == NULL) {
        resolve_stateids_for_ds_entries(lr->cat, entries, sc.count);
    }

    for (i = 0; i < sc.count; i++) {
        revoke_layout(lr, entries[i].stateid_other,
                      entries[i].clientid,
                      entries[i].fileid, ds_id);
    }

    free(entries);
    return 0;
}

/* Callback for mds_coord_layout_iter_file — collects holders with stateids. */
static int file_holder_cb(uint64_t clientid,
                          const struct nfs4_stateid *stateid,
                          uint32_t iomode, void *ctx)
{
    struct recall_scan_ctx *sc = ctx;

    (void)iomode;

    if (sc->count < sc->capacity) {
        sc->entries[sc->count].clientid = clientid;
        sc->entries[sc->count].fileid = 0; /* filled by caller */
        if (stateid != NULL) {
            memcpy(sc->entries[sc->count].stateid_other,
                   stateid->other, 12);
        }
        sc->count++;
    }
    return 0;
}

int layout_recall_for_file(struct layout_recall *lr, uint64_t fileid)
{
    struct recall_entry *entries = NULL;
    struct recall_scan_ctx scan;
    enum mds_status st;

    if (lr == NULL) {
        return -EINVAL;
    }

    /*
     * Step 1: Enumerate every layout holder for this file using
     * the iterator API.  This replaces the old first-match-only
     * probe with a full holder scan.
     */
    entries = calloc(MAX_RECALL_ENTRIES, sizeof(*entries));
    if (entries == NULL) {
        return -ENOMEM;
    }

    scan.entries  = entries;
    scan.count    = 0;
    scan.capacity = MAX_RECALL_ENTRIES;

    st = mds_coord_layout_iter_file(lr->cat, fileid,
                                     file_holder_cb, &scan);
    if (st != MDS_OK && st != MDS_ERR_NOTFOUND) {
        free(entries);
        return -EIO;
    }

    /* Fill in fileid for all entries. */
    for (uint32_t i = 0; i < scan.count; i++) {
        entries[i].fileid = fileid;
    }

    if (scan.count == 0) {
        free(entries);
        return 0; /* No layouts on this file. */
    }

    /*
     * Step 2: Best-effort CB_LAYOUTRECALL to each unique holder.
     */
    if (lr->st != NULL) {
        struct cb_target_list tl;

        memset(&tl, 0, sizeof(tl));
        tl.filter_count = collect_unique_clientids(
            entries, scan.count,
            tl.filter_clientids, MAX_RECALL_CLIENTS);

        session_for_each_with_cb(lr->st, snap_cb_target, &tl);

        for (uint32_t i = 0; i < tl.count; i++) {
            attempt_cb_for_target(&tl.targets[i], lr->revoke_ms);
            close(tl.targets[i].fd);
        }
    }

    /*
     * Step 3: Authoritative revoke — delete all layouts for this file.
     */
    for (uint32_t ri = 0; ri < scan.count; ri++) {
        revoke_layout_for_file_entry(lr, &entries[ri]);
    }

    free(entries);
    return 0;
}
