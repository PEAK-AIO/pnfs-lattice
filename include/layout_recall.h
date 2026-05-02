/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * layout_recall.h — Layout recall / revocation coordinator.
 *
 * Coordinates layout recall and revocation for DS failure and
 * per-file recall (migration, rebalance, tiering).  Best-effort
 * CB_LAYOUTRECALL is sent via nfs4_cb when a session table is
 * attached; authoritative revoke follows regardless of callback
 * outcome.  Client ack tracking and timeout/retry are still TODO.
 *
 * See docs/architecture.md §15 for design.
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

#endif /* LAYOUT_RECALL_H */
