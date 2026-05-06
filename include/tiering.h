/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * tiering.h -- DS storage tiering V1.
 *
 * Periodic full stripe-map scan that classifies files as promote/demote
 * based on I/O tracker scores, then moves them to the correct tier via
 * the shared mover helper.
 *
 * V1 scope: single-stripe, mirror_count >= 1, LAYOUTSTATS primary,
 * LAYOUTGET fallback, cooldown hysteresis.
 * See docs/architecture.md S3.9 for design.
 */

#ifndef TIERING_H
#define TIERING_H

#include <stdint.h>
#include "pnfs_mds.h"

struct mds_catalogue;
struct commit_queue;
struct mds_proxy_ctx;
struct open_state_table;
struct io_tracker;
struct tiering_worker;

/* Tiering state constants. */
#define TIERING_IDLE        0
#define TIERING_EVALUATING  1
#define TIERING_MOVING      2
#define TIERING_DONE        3
#define TIERING_ERROR       4

/** Configuration for the tiering evaluator/mover. */
struct tiering_config {
    uint32_t eval_interval_sec;    /**< Seconds between eval cycles (default 3600). */
    double   promote_threshold;    /**< Bytes/hour to trigger promotion (default 100 MiB/h). */
    double   demote_threshold;     /**< Bytes/hour below which to demote (default ~0.042 MiB/h). */
    uint32_t cooldown_evals;       /**< Min eval cycles between moves for same file (default 2). */
    uint32_t layoutget_bump_weight; /**< Synthetic bytes per LAYOUTGET bump (default 1 MiB). */
};

/** Extended tiering status with skip-reason counters. */
struct tiering_status_info {
    uint32_t state;                    /**< Current TIERING_* state. */
    uint32_t eval_count;               /**< Number of eval cycles completed. */
    uint32_t candidates_in_eval;       /**< Candidates found in latest eval. */
    uint32_t files_promoted;           /**< Files promoted (cumulative). */
    uint32_t files_demoted;            /**< Files demoted (cumulative). */
    uint64_t bytes_moved;              /**< Bytes copied (cumulative). */
    uint32_t skipped_active_layout;    /**< Skipped: active pNFS layout. */
    uint32_t skipped_active_writer;    /**< Skipped: active proxy writer. */
    uint32_t skipped_sparse;           /**< Skipped: sparse file. */
    uint32_t skipped_multi_stripe;     /**< Skipped: multi-stripe layout. */
    uint32_t skipped_cooldown;         /**< Skipped: cooldown hysteresis. */
    uint32_t skipped_no_target_ds;     /**< Skipped: no DS in target tier. */
    uint32_t skipped_error;            /**< Skipped: mover error. */
};

/**
 * Initialise a tiering worker (does not start the thread).
 *
 * @param cat       Catalogue handle.
 * @param cq        Commit queue.
 * @param proxy_ctx Proxy I/O context.
 * @param ot        Open state table (may be NULL).
 * @param iot       I/O tracker (must not be NULL).
 * @param out       Receives allocated worker.
 * @return 0 on success, -1 on failure.
 */
int tiering_init(const struct mds_catalogue *cat, struct commit_queue *cq,
                 struct mds_proxy_ctx *proxy_ctx,
                 struct open_state_table *ot,
                 struct io_tracker *iot,
                 struct tiering_worker **out);

/**
 * Start the tiering eval/move loop.
 *
 * @param tw   Tiering worker.
 * @param cfg  Configuration (copied).
 * @return MDS_OK on success, MDS_ERR_EXISTS if already running.
 */
enum mds_status tiering_start(struct tiering_worker *tw,
                               const struct tiering_config *cfg);

/**
 * Stop the tiering loop (signal + join).  Safe if not running.
 * @param tw  Tiering worker (NULL tolerated).
 */
void tiering_stop(struct tiering_worker *tw);

/**
 * Destroy the worker and free all resources.
 * Calls tiering_stop() if still running.
 * @param tw  Tiering worker (NULL tolerated).
 */
void tiering_destroy(struct tiering_worker *tw);

/**
 * Query tiering status.
 * @param tw    Tiering worker (NULL tolerated -- zeroed output).
 * @param info  Receives all status fields.
 */
void tiering_status(const struct tiering_worker *tw,
                    struct tiering_status_info *info);

/**
 * Set the eval interval (seconds).
 * Useful for testing (set to 0 for immediate eval).
 * @param tw    Tiering worker.
 * @param secs  Eval interval in seconds.
 */
void tiering_set_eval_interval(struct tiering_worker *tw, uint32_t secs);

/**
 * Limit the worker to at most @a n eval cycles.  0 = unlimited (default).
 * Useful for deterministic unit tests.
 */
void tiering_set_max_evals(struct tiering_worker *tw, uint32_t n);

struct mds_shard;

/**
 * Bind to a default shard (sets db and cq from shard).
 *
 * @param tw     Tiering worker.
 * @param shard  Default shard (NULL tolerated).
 */
void tiering_set_shard(struct tiering_worker *tw,
                       const struct mds_shard *shard);

#endif /* TIERING_H */
