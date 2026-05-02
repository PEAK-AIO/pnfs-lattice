/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * resilver.h — Background resilvering for degraded mirrored files.
 *
 * Rebuilds data on a replacement data server by copying from healthy
 * mirrors.  Admin-triggered via mds-admin CLI.  V1 constraints:
 *   - repair-only (existing mirrored files with OFFLINE mirror)
 *   - single-stripe files only
 *   - no active layouts/writers (skipped if layout_state or open_state exists)
 *   - dense files only (sparse files skipped)
 *
 * See docs/architecture.md §15.3 for design.
 */

#ifndef RESILVER_H
#define RESILVER_H

#include <stdint.h>

struct mds_catalogue;
struct commit_queue;
struct mds_proxy_ctx;
struct open_state_table;
struct resilver_worker;

/* Resilver state constants. */
#define RESILVER_IDLE      0
#define RESILVER_SCANNING  1
#define RESILVER_COPYING   2
#define RESILVER_DONE      3
#define RESILVER_ERROR     4

/**
 * Initialise a resilver worker (does not start the thread).
 *
 * @param cat       Catalogue handle.
 * @param cq        Commit queue (may be NULL; direct txn used).
 * @param proxy_ctx Proxy I/O context with DS mounts.
 * @param ot        Open state table for writer fencing (may be NULL).
 * @param out       Receives allocated worker.
 * @return 0 on success, -1 on failure.
 */
int resilver_init(const struct mds_catalogue *cat, struct commit_queue *cq,
                  struct mds_proxy_ctx *proxy_ctx,
                  struct open_state_table *ot,
                  struct resilver_worker **out);

/**
 * Start resilvering onto a target DS.
 *
 * Validates the DS is ONLINE.  Launches the background thread.
 * Returns -1 if already running or DS is invalid.
 *
 * @param rw        Resilver worker.
 * @param target_ds Target data server ID.
 * @return 0 on success, -1 on failure.
 */
int resilver_start(struct resilver_worker *rw, uint32_t target_ds);

/**
 * Stop a running resilver (signal + join).
 *
 * Safe to call if not running.
 *
 * @param rw  Resilver worker (may be NULL).
 */
void resilver_stop(struct resilver_worker *rw);

/**
 * Destroy the worker and free all resources.
 *
 * Calls resilver_stop() if still running.
 *
 * @param rw  Resilver worker (may be NULL).
 */
void resilver_destroy(struct resilver_worker *rw);

/**
 * Extended resilver status with skip-reason counters.
 *
 * All fields are optional (NULL to skip).  All counters are cumulative
 * across all passes.
 */
struct resilver_status_info {
    uint32_t state;                 /**< Current RESILVER_* state. */
    uint32_t files_total;           /**< Total degraded files found. */
    uint32_t files_done;            /**< Files successfully repaired. */
    uint64_t bytes_copied;          /**< Total bytes copied. */
    uint32_t pass_count;            /**< Number of scan passes completed. */
    uint32_t skipped_active_layout; /**< Files skipped: active pNFS layout. */
    uint32_t skipped_active_writer; /**< Files skipped: active proxy writer. */
    uint32_t skipped_sparse;        /**< Files skipped: sparse file. */
    uint32_t skipped_multi_stripe;  /**< Files skipped: multi-stripe layout. */
    uint32_t skipped_error;         /**< Files skipped: copy/commit error. */
};

/**
 * Query resilver progress (lock-free atomic reads).
 *
 * @param rw           Resilver worker.
 * @param files_total  Receives total degraded files found.
 * @param files_done   Receives files completed.
 * @param bytes_copied Receives total bytes copied.
 * @param state        Receives current state (RESILVER_*).
 */
void resilver_status(const struct resilver_worker *rw,
                     uint32_t *files_total, uint32_t *files_done,
                     uint64_t *bytes_copied, uint32_t *state);

/**
 * Query extended resilver status including skip reasons.
 *
 * @param rw    Resilver worker (NULL tolerated — zeroed output).
 * @param info  Receives all status fields.
 */
void resilver_status_ext(const struct resilver_worker *rw,
                          struct resilver_status_info *info);


/**
 * Set the inter-pass retry delay (seconds).  Default is 5.
 * Useful for testing (set to 0 for fast tests).
 *
 * @param rw    Resilver worker.
 * @param secs  Delay in seconds between retry passes.
 */
void resilver_set_retry_delay(struct resilver_worker *rw, uint32_t secs);

struct mds_shard;

/**
 * Bind to a default shard (sets db and cq from shard).
 *
 * @param rw     Resilver worker.
 * @param shard  Default shard (NULL tolerated).
 */
void resilver_set_shard(struct resilver_worker *rw,
                        const struct mds_shard *shard);

#endif /* RESILVER_H */
