/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * layout_recall.h -- Layout recall / revocation coordinator.
 *
 * Coordinates layout recall and revocation for DS failure and
 * per-file recall (migration, rebalance, tiering).  Best-effort
 * CB_LAYOUTRECALL is sent via nfs4_cb when a session table is
 * attached; authoritative revoke follows regardless of callback
 * outcome.  Client ack tracking and timeout/retry are still TODO.
 *
 * See docs/architecture.md S15 for design.
 */

#ifndef LAYOUT_RECALL_H
#define LAYOUT_RECALL_H

#include <stdint.h>

struct mds_catalogue;
struct commit_queue;

/* Opaque handle. */
struct layout_recall;

/**
 * Initialise the layout recall coordinator.
 *
 * @param cat           Catalogue handle (for ds_layout_idx scan).
 * @param cq            Commit queue (for revoke/cleanup; may be NULL).
 * @param revoke_ms     Reserved for future callback grace period (0 = default).
 * @param out           Receives handle.
 * @return 0 on success, -errno on failure.
 */
int layout_recall_init(const struct mds_catalogue *cat,
                       struct commit_queue *cq,
                       uint32_t revoke_ms,
                       struct layout_recall **out);

/**
 * Destroy the recall coordinator and free resources.
 *
 * @param lr  Handle (NULL tolerated).
 */
void layout_recall_destroy(struct layout_recall *lr);

/**
 * Revoke all layouts referencing a given DS.
 *
 * Called by ds_health failure callback when a DS goes offline.
 * Scans ds_layout_idx for the DS and immediately revokes (deletes)
 * all affected layouts.  No callbacks are sent in this phase.
 *
 * This function is synchronous but does not sleep.
 *
 * @param lr     Recall coordinator handle.
 * @param ds_id  Data server that failed.
 * @return 0 on success, -errno on failure.
 */
int layout_recall_for_ds(struct layout_recall *lr, uint32_t ds_id);

/**
 * Recall and revoke all layouts for a specific file.
 *
 * Enumerates every layout holder via catalogue layout iteration,
 * sends best-effort CB_LAYOUTRECALL to each (if session table is set),
 * then authoritatively deletes all layout state for the file.
 *
 * @param lr      Recall coordinator handle.
 * @param fileid  File whose layouts should be recalled.
 * @return 0 on success, -errno on failure.
 */
int layout_recall_for_file(struct layout_recall *lr, uint64_t fileid);

/**
 * Recall and revoke every layout for a final unlink.
 *
 * Final unlink is different from an ordinary byte-range conflict:
 * once the last namespace entry is removed, no outstanding layout for
 * that fileid may remain valid.  This helper sends best-effort
 * whole-file CB_LAYOUTRECALL4_FILE callbacks while the fileid is still
 * resolvable, then unconditionally removes every matching layout-state
 * row regardless of callback status, including NFS4_OK,
 * NFS4ERR_DELAY, and NFS4ERR_RECALLCONFLICT.
 *
 * Keep this separate from layout_recall_byte_range_for_holders(), whose
 * normal layoutget conflict semantics intentionally preserve layout
 * rows for transient callback responses so the client's natural
 * LAYOUTRETURN can complete.
 *
 * @param lr            Recall coordinator handle.
 * @param fileid        File being finally unlinked.
 * @param recalled_out  Optional: receives number of layout rows found.
 * @return 0 on success, -errno on catalogue failure.
 */
int layout_recall_revoke_all_for_unlink(struct layout_recall *lr,
                                        uint64_t fileid,
                                        uint32_t *recalled_out);

/**
 * Byte-range conflict-recall on op_layoutget.
 *
 * Recalls only the holders whose existing layout overlaps the
 * requested (offset, length) AND whose iomode conflicts with the
 * requesting iomode (RW vs anything; READ vs RW).  For each such
 * holder we emit `CB_LAYOUTRECALL` with `recall_type = LAYOUTRECALL4_FILE`
 * and the *intersection* of (req_off, req_len) with (hold_off, hold_len)
 * -- RFC 8881 S12.5.5 byte-range partial recall.  The holder's
 * authoritative layout state is then revoked from the catalogue (full
 * stateid revoke; partial-revoke granularity is a Phase 2 follow-up).
 *
 * Same-client renew (clientid == requesting_clientid) is skipped -- a
 * client may upgrade or extend its own layout without recalling itself.
 *
 * Best-effort and idempotent.  CB delivery failure (no backchannel,
 * timeout, send error, terminal NFS4ERR_*) is non-fatal; the catalogue
 * revoke runs for those cases so the requesting LAYOUTGET can proceed.
 * Transient client statuses (NFS4_OK, NFS4ERR_DELAY, and
 * NFS4ERR_RECALLCONFLICT) intentionally preserve the layout row so the
 * client's natural LAYOUTRETURN can complete without BAD_STATEID.
 *
 * @param lr             Recall coordinator handle.
 * @param fileid         File targeted by the LAYOUTGET.
 * @param req_clientid   Requesting client (skip self).
 * @param req_iomode     LAYOUTIOMODE4_READ or LAYOUTIOMODE4_RW.
 * @param req_offset     Requested range start (bytes).
 * @param req_length     Requested range length (UINT64_MAX = to-EOF).
 * @param req_layout_type Layout type the requester is asking for
 *                       (e.g. LAYOUT4_FLEX_FILES).  This value is
 *                       echoed in the CB_LAYOUTRECALL `clora_type`
 *                       field -- the holder's grant uses the same
 *                       layout type since the catalogue does not
 *                       persist per-grant layout_type and we never
 *                       grant a mixed-type layout for a single file
 *                       (compound_layout.c:op_layoutget validates
 *                       a->layout_type in { LAYOUT4_NFSV4_1_FILES,
 *                       LAYOUT4_FLEX_FILES }).  Passing a stale or
 *                       wrong value would yield a CB the holder
 *                       client cannot decode -- Mark's bug.
 *
 *                       Pass 0 for callers that do not have an
 *                       active LAYOUTGET request (for example
 *                       op_setattr on SIZE); the helper then
 *                       substitutes the coordinator-level
 *                       default set by
 *                       layout_recall_set_default_layout_type()
 *                       (default LAYOUT4_FLEX_FILES).
 * @param recalled_out   Optional: receives the count of holders the
 *                       helper recalled.  Pass NULL to ignore.
 * @return 0 on success (zero or more recalls sent), -errno on
 *         catalogue failure (no recalls sent).
 */
int layout_recall_byte_range_for_holders(struct layout_recall *lr,
                                         uint64_t fileid,
                                         uint64_t req_clientid,
                                         uint32_t req_iomode,
                                         uint64_t req_offset,
                                         uint64_t req_length,
                                         uint32_t req_layout_type,
                                         uint32_t *recalled_out);

/**
 * Set the default layout type used by the DS-failure recall path.
 *
 * The DS-failure path (layout_recall_for_ds) emits LAYOUTRECALL4_ALL
 * which still carries a `clora_type` per RFC 8881 S20.3.4.  The
 * catalogue does not persist per-grant layout_type, so callers must
 * tell the coordinator which type was used to grant layouts on the
 * affected DS.  In a homogeneous deployment (all flexfiles or all
 * files-layout) this is a single value set once at startup.
 *
 * Default is LAYOUT4_FLEX_FILES (matches the production layout type).
 * Pass 0 to keep the current default.  NULL @lr is tolerated.
 */
void layout_recall_set_default_layout_type(struct layout_recall *lr,
                                           uint32_t layout_type);

struct session_table;

/**
 * Inject session table reference for callback delivery.
 *
 * When set, layout_recall_for_ds() will attempt best-effort
 * CB_LAYOUTRECALL to clients before revoking their layouts.
 * When NULL (default), revoke-only behavior is used.
 *
 * @param lr  Recall coordinator handle.
 * @param st  Session table (NULL to disable callbacks).
 */
void layout_recall_set_session_table(struct layout_recall *lr,
                                     struct session_table *st);

struct mds_shard;

/**
 * Bind to a default shard (sets db and cq from shard).
 *
 * @param lr     Layout recall handle.
 * @param shard  Default shard (NULL tolerated).
 */
void layout_recall_set_shard(struct layout_recall *lr,
                             const struct mds_shard *shard);

struct mds_proxy_ctx;

/**
 * Inject proxy I/O context for DS-side fencing.
 *
 * When set, the revoke path (byte_range_revoke_holders) chowns the
 * DS backing file to the fencing uid/gid after revoking the layout
 * state, per RFC 8435 §14.  Without this binding, revocation is
 * callback-only and a non-cooperating client can continue I/O.
 *
 * @param lr     Layout recall handle (NULL tolerated).
 * @param proxy  Proxy context (NULL to disable fencing).
 */
void layout_recall_set_proxy(struct layout_recall *lr,
                             struct mds_proxy_ctx *proxy);

/* -----------------------------------------------------------------------
 * Layout-stateid seqid tracker (compound_layout.c).
 *
 * These are defined in compound_layout.c and used by layout_recall.c
 * for CB_LAYOUTRECALL stateid management.  Not static because they
 * cross translation-unit boundaries within the same static library.
 * ----------------------------------------------------------------------- */
void layout_seqid_advance(const uint8_t other[12],
                          bool *hit, uint32_t *next_seqid);
void layout_seqid_record_at(const uint8_t other[12], uint32_t seqid);
bool layout_seqid_peek(const uint8_t other[12], uint32_t *cur_seqid);
void layout_seqid_remove(const uint8_t other[12]);

/* Diagnostics: current count of live entries in the layout-seqid
 * tracker (compound_layout.c).  Exported for metrics / admin
 * introspection. */
uint64_t layout_seqid_entry_count(void);

#endif /* LAYOUT_RECALL_H */
