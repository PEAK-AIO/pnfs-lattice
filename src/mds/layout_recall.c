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
#include "layout_types.h"  /* LAYOUT4_FLEX_FILES default */
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
    /*
     * Default layout_type echoed in CB_LAYOUTRECALL.clora_type for
     * recall paths that do not have a per-grant layout_type to plumb
     * through (currently only the DS-failure / LAYOUTRECALL4_ALL
     * path).  The catalogue does not persist per-grant layout_type,
     * so we need a coordinator-level default in homogeneous
     * deployments.  Initialised to LAYOUT4_FLEX_FILES at
     * layout_recall_init() time — production deployments grant
     * flexfiles layouts (RFC 8435 §5) and the prior hard-coded
     * LAYOUT4_NFSV4_1_FILES value caused Linux flexfiles clients to
     * reject the recall.
     */
    uint32_t                 default_layout_type;
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
    uint32_t minorversion;   /* RFC 8881 §20.1 — CB_COMPOUND minor */
    struct nfs4_cb_sec cb_sec; /* RFC 8881 §2.10.8.3 — captured sec parms */
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
    t->cb_sec = snap->cb_sec;
    t->fd = dup_fd;
    t->slot_seq_id = snap->slot_seq_id;
    t->minorversion = snap->minorversion;
    tl->count++;

    return 0;
}

/* -----------------------------------------------------------------------
 * Best-effort CB_LAYOUTRECALL send
 * ----------------------------------------------------------------------- */

/**
 * Build a temporary nfs4_session from snapshot data and call
 * nfs4_cb_layoutrecall().  Best-effort: failures are logged, not fatal.
 *
 * @param layout_type  Layout type to advertise in clora_type.  The
 *                     DS-failure path uses the recall coordinator's
 *                     configured default (set by
 *                     layout_recall_set_default_layout_type, default
 *                     LAYOUT4_FLEX_FILES).
 */
static void attempt_cb_for_target(const struct cb_target *t,
                                  uint32_t layout_type,
                                  uint32_t timeout_ms)
{
    struct nfs4_cb_layoutrecall_args args;

    /* Send LAYOUTRECALL4_ALL — the DS is offline, recall everything.
     * RFC 8881 §20.3.4 still requires clora_type even for ALL/FSID;
     * the value MUST match the holder's layout type or the kernel
     * client will reject the CB — see Mark's two-client harness
     * report (bugs from mark/STRICT_ANALYSIS.md).  We therefore
     * advertise the coordinator's default layout_type rather than
     * the previous hard-coded LAYOUT4_NFSV4_1_FILES (1). */
    memset(&args, 0, sizeof(args));
    args.layout_type = layout_type;
    args.iomode = 3;       /* LAYOUTIOMODE4_ANY */
    args.recall_type = LAYOUTRECALL4_ALL;

    int rc = nfs4_cb_layoutrecall_fd(t->fd, t->session_id,
                                      t->cb_prog, t->slot_seq_id,
                                      1, t->minorversion, &t->cb_sec,
                                      &args, timeout_ms);
    if (rc != 0) {
        (void)fprintf(stderr, "layout_recall: CB_LAYOUTRECALL to "
                "clientid=%lu failed (rc=%d) \u2014 will revoke\n",
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
    /* Production deployments grant flexfiles layouts (RFC 8435 §5).
     * Operators with a files-layout deployment can override via
     * layout_recall_set_default_layout_type(). */
    lr->default_layout_type = LAYOUT4_FLEX_FILES;

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

void layout_recall_set_default_layout_type(struct layout_recall *lr,
                                           uint32_t layout_type)
{
    if (lr != NULL && layout_type != 0) {
        lr->default_layout_type = layout_type;
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
            attempt_cb_for_target(&tl.targets[i],
                                  lr->default_layout_type,
                                  lr->revoke_ms);
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

/* -----------------------------------------------------------------------
 * Byte-range conflict-recall (op_layoutget)
 *
 * Mark's bug `bugs from mark/mds_byte_range_layoutrecall.md`: the MDS
 * historically only emitted whole-file CB_LAYOUTRECALL even when the
 * conflicting LAYOUTGET request only touched a sub-range of the
 * holder's grant.  This blocks Slice 3 of the strict N-to-1 kernel
 * patch series, which depends on partial recalls so concurrent writers
 * to disjoint ranges of the same shared file don't ping-pong each
 * other's whole layout.
 *
 * The helper below mirrors layout_recall_for_file but:
 *   - takes (offset, length, iomode) from the requesting compound;
 *   - filters the holder set by iomode-conflict;
 *   - computes the intersection of [req_off, req_off + req_len) with
 *     each holder's stored (offset, length);
 *   - emits CB_LAYOUTRECALL with recall_type = LAYOUTRECALL4_FILE and
 *     the intersection;
 *   - revokes the holder's authoritative layout-state row.
 *
 * v1 conservative behaviour: full-revoke the holder's grant when the
 * intersection is non-empty.  RFC permits emitting a byte-range CB
 * even when the server tracks whole-file granularity — the kernel
 * client invalidates only the recalled sub-range of its cache, which
 * is what Slice 3 needs.  Splitting the layout-state row into two
 * adjacent ranges is a Phase 2 follow-up.
 * ----------------------------------------------------------------------- */

/* RFC 8881 §12.4.4: layout iomode constants (mirrored from
 * include/compound.h to avoid pulling that header here). */
#define LAYOUTIOMODE4_READ  1
#define LAYOUTIOMODE4_RW    2
#define LAYOUTIOMODE4_ANY   3

static bool iomode_conflicts(uint32_t holder_iomode, uint32_t requester_iomode)
{
    /*
     * RFC 8881 §12.5.5: a layout conflicts with another if at least
     * one side has write semantics (RW).  Two READs do not conflict.
     * ANY behaves as RW for safety.
     */
    bool holder_w = (holder_iomode == LAYOUTIOMODE4_RW) ||
                    (holder_iomode == LAYOUTIOMODE4_ANY);
    bool req_w    = (requester_iomode == LAYOUTIOMODE4_RW) ||
                    (requester_iomode == LAYOUTIOMODE4_ANY);
    return holder_w || req_w;
}

/*
 * Compute [out_off, out_off + out_len) = [a_off, a_off + a_len)
 *                                       ∩ [b_off, b_off + b_len).
 * Length values of UINT64_MAX mean "to EOF" per RFC 8881 §12.5.5.
 * Returns false on empty intersection (caller skips the recall).
 */
static bool range_intersect(uint64_t a_off, uint64_t a_len,
                            uint64_t b_off, uint64_t b_len,
                            uint64_t *out_off, uint64_t *out_len)
{
    uint64_t a_end;
    uint64_t b_end;
    uint64_t lo;
    uint64_t hi;

    /* Saturating add: a_off + a_len, capping at UINT64_MAX. */
    a_end = (a_len == UINT64_MAX || a_len > UINT64_MAX - a_off)
              ? UINT64_MAX : a_off + a_len;
    b_end = (b_len == UINT64_MAX || b_len > UINT64_MAX - b_off)
              ? UINT64_MAX : b_off + b_len;

    lo = (a_off > b_off) ? a_off : b_off;
    hi = (a_end < b_end) ? a_end : b_end;
    if (lo >= hi) {
        return false;
    }
    *out_off = lo;
    /* Preserve UINT64_MAX as "to EOF" so the wire CB carries the
     * RFC-canonical sentinel rather than a numerically-identical but
     * structurally-different absolute byte count. */
    *out_len = (hi == UINT64_MAX) ? UINT64_MAX : (hi - lo);
    return true;
}

struct byte_range_holder {
    uint64_t            clientid;
    uint64_t            fileid;
    struct nfs4_stateid stateid;
    uint32_t            iomode;
    uint64_t            offset;
    uint64_t            length;
    uint64_t            recall_offset;
    uint64_t            recall_length;
    /*
     * Layout type echoed in CB_LAYOUTRECALL.clora_type for this
     * holder.  The catalogue layer does not persist per-grant
     * layout_type, so we copy the requesting LAYOUTGET's
     * a->layout_type into every holder — op_layoutget validates
     * that value against { LAYOUT4_NFSV4_1_FILES, LAYOUT4_FLEX_FILES }
     * before reaching this scan, and our grant policy never mixes
     * layout types within the same fileid (compound_layout.c
     * always echoes a->layout_type in r->layout_type).
     */
    uint32_t            layout_type;
};

#define LAYOUT_BYTE_RANGE_MAX_HOLDERS 256

struct byte_range_collect_ctx {
    struct byte_range_holder *holders;
    uint32_t                  count;
    uint32_t                  capacity;
    uint64_t                  fileid;
    uint64_t                  req_clientid;
    uint32_t                  req_iomode;
    uint64_t                  req_offset;
    uint64_t                  req_length;
    uint32_t                  req_layout_type;
    struct mds_catalogue     *cat;
};

/*
 * Per-holder iterator callback for mds_coord_layout_iter_file.  We
 * receive (clientid, stateid, iomode) and look up (offset, length)
 * via mds_coord_layout_get_by_stateid; the latter is a single NDB
 * read keyed on stateid_other so the cost is bounded.
 */
static int byte_range_collect_cb(uint64_t clientid,
                                 const struct nfs4_stateid *stateid,
                                 uint32_t iomode, void *ctx)
{
    struct byte_range_collect_ctx *c = ctx;
    uint64_t hold_off = 0;
    uint64_t hold_len = 0;
    uint64_t inter_off = 0;
    uint64_t inter_len = 0;
    uint64_t scratch_clientid = 0;
    uint64_t scratch_fileid = 0;
    uint32_t scratch_iomode = 0;
    uint32_t scratch_seqid = 0;
    enum mds_status st;

    if (c == NULL || stateid == NULL) {
        return 0;
    }
    if (clientid == c->req_clientid) {
        return 0; /* self — skip */
    }
    if (!iomode_conflicts(iomode, c->req_iomode)) {
        return 0;
    }
    if (c->count >= c->capacity) {
        return 1; /* stop scan; defer surplus to next call */
    }

    if (c->cat != NULL) {
        st = mds_coord_layout_get_by_stateid(c->cat,
                                              stateid->other,
                                              &scratch_clientid,
                                              &scratch_fileid,
                                              &scratch_iomode,
                                              &hold_off, &hold_len,
                                              &scratch_seqid);
        if (st != MDS_OK) {
            /* Stale or partially-persisted state — fall back to
             * whole-file recall to be safe. */
            hold_off = 0;
            hold_len = UINT64_MAX;
        }
    } else {
        hold_off = 0;
        hold_len = UINT64_MAX;
    }

    if (!range_intersect(c->req_offset, c->req_length,
                          hold_off, hold_len,
                          &inter_off, &inter_len)) {
        return 0; /* disjoint ranges — no conflict */
    }

    c->holders[c->count].clientid       = clientid;
    c->holders[c->count].fileid         = c->fileid;
    c->holders[c->count].stateid        = *stateid;
    c->holders[c->count].iomode         = iomode;
    c->holders[c->count].offset         = hold_off;
    c->holders[c->count].length         = hold_len;
    c->holders[c->count].recall_offset  = inter_off;
    c->holders[c->count].recall_length  = inter_len;
    c->holders[c->count].layout_type    = c->req_layout_type;
    c->count++;
    return 0;
}

/*
 * Per-holder CB context.  Only one holder is sent per
 * session_for_each_with_cb_for_clientid() invocation — the iterator
 * stops at the first session it visits for the holder's clientid.
 */
struct byte_range_one_cb_ctx {
    const struct byte_range_holder *holder;
    uint32_t                        timeout_ms;
};

static int byte_range_cb_one_holder(const struct session_cb_snap *snap,
                                    void *ctx)
{
    struct byte_range_one_cb_ctx *c = ctx;
    struct nfs4_cb_layoutrecall_args args;
    int fd;
    int dup_fd;
    int rc;

    if (snap == NULL || c == NULL || c->holder == NULL) {
        return 0;
    }
    /* Defensive: per-clientid iterator already filters by clientid,
     * but a same-process race could in theory invoke this from the
     * global iterator path — keep the explicit check. */
    if (c->holder->clientid != snap->clientid) {
        return 0;
    }
    fd = rpc_conn_get_fd(snap->cb_conn);
    if (fd < 0) {
        return 0;
    }
    dup_fd = dup(fd);
    if (dup_fd < 0) {
        return 0;
    }

    memset(&args, 0, sizeof(args));
    args.layout_type = c->holder->layout_type;
    args.iomode      = c->holder->iomode;
    args.recall_type = LAYOUTRECALL4_FILE;
    args.fileid      = c->holder->fileid;
    args.stateid     = c->holder->stateid;
    args.offset      = c->holder->recall_offset;
    args.length      = c->holder->recall_length;

    rc = nfs4_cb_layoutrecall_fd(dup_fd, snap->session_id,
                                 snap->cb_prog,
                                 snap->slot_seq_id,
                                 1, snap->minorversion,
                                 &snap->cb_sec,
                                 &args, c->timeout_ms);
    if (rc != 0) {
        (void)fprintf(stderr,
            "layout_recall: byte-range CB_LAYOUTRECALL "
            "clientid=%llu fileid=%llu off=%llu len=%llu "
            "rc=%d \u2014 revoking\n",
            (unsigned long long)c->holder->clientid,
            (unsigned long long)c->holder->fileid,
            (unsigned long long)args.offset,
            (unsigned long long)args.length, rc);
    }
    (void)close(dup_fd);

    /*
     * Return 1 ("snap consumed") so session_invoke_cb_locked()
     * commits the slot-0 sequenceid advance before releasing the
     * session-table lock.  Without this, a second CB on the same
     * session would reuse sa_sequenceid and the kernel would treat
     * it as a slot replay (RFC 8881 §18.46.4).  We commit even on
     * CB send failure: the kernel's contract is +1 per CB attempt,
     * not per CB success.
     */
    return 1;
}

int layout_recall_byte_range_for_holders(struct layout_recall *lr,
                                         uint64_t fileid,
                                         uint64_t req_clientid,
                                         uint32_t req_iomode,
                                         uint64_t req_offset,
                                         uint64_t req_length,
                                         uint32_t req_layout_type,
                                         uint32_t *recalled_out)
{
    struct byte_range_holder holders[LAYOUT_BYTE_RANGE_MAX_HOLDERS];
    struct byte_range_collect_ctx col;
    enum mds_status st;
    uint32_t i;

    if (recalled_out != NULL) {
        *recalled_out = 0;
    }
    if (lr == NULL) {
        return -EINVAL;
    }
    if (lr->cat == NULL) {
        /* No catalogue — no holders to query; treat as no-conflict. */
        return 0;
    }

    memset(&col, 0, sizeof(col));
    col.holders         = holders;
    col.capacity        = LAYOUT_BYTE_RANGE_MAX_HOLDERS;
    col.fileid          = fileid;
    col.req_clientid    = req_clientid;
    col.req_iomode      = req_iomode;
    col.req_offset      = req_offset;
    col.req_length      = req_length;
    col.req_layout_type = req_layout_type;
    col.cat             = lr->cat;

    st = mds_coord_layout_iter_file(lr->cat, fileid,
                                     byte_range_collect_cb, &col);
    if (st != MDS_OK && st != MDS_ERR_NOTFOUND) {
        return -EIO;
    }
    if (col.count == 0) {
        return 0;
    }

    /*
     * For each holder, dispatch one CB_LAYOUTRECALL via the per-
     * clientid session iterator, which selects the most-recently
     * created session for the holder (Q2(a) of Mark's bug report).
     * The iterator commits the slot-0 sequenceid advance under the
     * session-table lock when our callback returns 1.
     */
    if (lr->st != NULL) {
        for (i = 0; i < col.count; i++) {
            struct byte_range_one_cb_ctx one_ctx = {
                .holder = &holders[i],
                .timeout_ms = lr->revoke_ms,
            };
            (void)session_for_each_with_cb_for_clientid(
                lr->st, holders[i].clientid,
                byte_range_cb_one_holder, &one_ctx);
        }
    }

    /*
     * Authoritative revoke: drop each holder's layout-state row.
     * v1 full-revokes the whole grant whenever the intersection is
     * non-empty.  Splitting the holder's row into two adjacent
     * ranges is a Phase 2 follow-up; the on-wire CB is already
     * byte-range correct, which is what the kernel client cares
     * about.
     */
    for (i = 0; i < col.count; i++) {
        revoke_layout(lr, holders[i].stateid.other,
                      holders[i].clientid, holders[i].fileid, 0);
    }

    if (recalled_out != NULL) {
        *recalled_out = col.count;
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
            attempt_cb_for_target(&tl.targets[i],
                                  lr->default_layout_type,
                                  lr->revoke_ms);
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
