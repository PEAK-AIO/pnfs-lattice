/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * rebalance.h -- Admin-triggered mirror relocation (V1).
 *
 * Moves mirrored data from a source DS to a target DS by copying
 * the data and atomically updating the stripe map + GC-enqueueing
 * the old copy.  V1 constraints match resilver:
 *   - single-stripe files only
 *   - no active layouts/writers (skipped)
 *   - dense files only (sparse files skipped)
 *
 * See docs/architecture.md S3.8 for design.
 */

#ifndef REBALANCE_H
#define REBALANCE_H

#include <stdint.h>
#include "pnfs_mds.h"

struct mds_catalogue;
struct commit_queue;
struct mds_proxy_ctx;
struct open_state_table;
struct rebalance_worker;

/* Rebalance state constants. */
#define REBALANCE_IDLE      0
#define REBALANCE_SCANNING  1
#define REBALANCE_COPYING   2
#define REBALANCE_DONE      3
#define REBALANCE_ERROR     4

/**
 * Extended rebalance status with skip-reason counters.
 *
 * All counters are cumulative across all passes.
 */
struct rebalance_status_info {
    uint32_t state;                 /**< Current REBALANCE_* state. */
    uint32_t source_ds;             /**< Source data server ID. */
    uint32_t target_ds;             /**< Target data server ID. */
    uint32_t candidates_in_pass;    /**< Candidates found in latest scan. */
    uint32_t files_moved;           /**< Files successfully relocated. */
    uint64_t bytes_copied;          /**< Total bytes copied. */
    uint32_t pass_count;            /**< Number of scan passes completed. */
    uint32_t skipped_active_layout; /**< Files skipped: active pNFS layout. */
    uint32_t skipped_active_writer; /**< Files skipped: active proxy writer. */
    uint32_t skipped_sparse;        /**< Files skipped: sparse file. */
    uint32_t skipped_multi_stripe;  /**< Files skipped: multi-stripe layout. */
    uint32_t skipped_error;         /**< Files skipped: copy/commit error. */
};

/**
 * Initialise a rebalance worker (does not start the thread).
 *
 * @param cat       Catalogue handle.
 * @param cq        Commit queue (must not be NULL).
 * @param proxy_ctx Proxy I/O context with DS mounts.
 * @param ot        Open state table for writer fencing (may be NULL).
 * @param out       Receives allocated worker.
 * @return 0 on success, -1 on failure.
 */
int rebalance_init(const struct mds_catalogue *cat, struct commit_queue *cq,
                   struct mds_proxy_ctx *proxy_ctx,
                   struct open_state_table *ot,
                   struct rebalance_worker **out);

/**
 * Start rebalancing from source_ds to target_ds.
 *
 * Validates: source != target, both ONLINE, both mounted.
 * Launches the background thread.
 * Returns MDS_ERR_INVAL if source == target, MDS_ERR_IO if a DS is
 * offline or not mounted, MDS_ERR_EXISTS if already running.
 *
 * @param rw        Rebalance worker.
 * @param source_ds Source data server ID.
 * @param target_ds Target data server ID.
 * @return MDS_OK on success, specific mds_status on failure.
 */
enum mds_status rebalance_start(struct rebalance_worker *rw,
                                uint32_t source_ds, uint32_t target_ds);

/**
 * Stop a running rebalance (signal + join).
 *
 * Safe to call if not running.
 *
 * @param rw  Rebalance worker (may be NULL).
 */
void rebalance_stop(struct rebalance_worker *rw);

/**
 * Destroy the worker and free all resources.
 *
 * Calls rebalance_stop() if still running.
 *
 * @param rw  Rebalance worker (may be NULL).
 */
void rebalance_destroy(struct rebalance_worker *rw);

/**
 * Query extended rebalance status including skip reasons.
 *
 * @param rw    Rebalance worker (NULL tolerated -- zeroed output).
 * @param info  Receives all status fields.
 */
void rebalance_status_ext(const struct rebalance_worker *rw,
                           struct rebalance_status_info *info);

/**
 * Set the inter-pass retry delay (seconds).  Default is 5.
 * Useful for testing (set to 0 for fast tests).
 *
 * @param rw    Rebalance worker.
 * @param secs  Delay in seconds between retry passes.
 */
void rebalance_set_retry_delay(struct rebalance_worker *rw, uint32_t secs);

struct mds_shard;

/**
 * Bind to a default shard (sets db and cq from shard).
 *
 * @param rw     Rebalance worker.
 * @param shard  Default shard (NULL tolerated).
 */
void rebalance_set_shard(struct rebalance_worker *rw,
                         const struct mds_shard *shard);

#endif /* REBALANCE_H */
