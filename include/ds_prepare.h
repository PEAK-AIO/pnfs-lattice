/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * ds_prepare.h -- Async DS file preparation queue (Epic B).
 *
 * After namespace CREATE commits a stripe_map with DS_PENDING,
 * a background worker creates the DS file and captures its NFS
 * file handle.  LAYOUTGET checks the result: fast if done,
 * LAYOUTUNAVAILABLE if still pending, sync fallback if no queue.
 */

#ifndef DS_PREPARE_H
#define DS_PREPARE_H

#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>

#include "pnfs_mds.h"

struct mds_catalogue;
struct mds_proxy_ctx;

/* Job states. */
enum ds_prepare_state {
	DS_PREP_QUEUED      = 0,
	DS_PREP_IN_PROGRESS = 1,
	DS_PREP_DONE        = 2,
	DS_PREP_FAILED      = 3,
	DS_PREP_CANCELLED   = 4,
};

/* Per-file prepare job. */
struct ds_prepare_job {
	struct mds_catalogue  *cat;          /* Catalogue vtable for this file. */
	uint64_t              fileid;
	uint64_t              generation;   /* inode generation at enqueue */
	uint32_t              ds_id;
	uint32_t              stripe;
	uint32_t              mirror;
	_Atomic int           state;
	struct ds_prepare_job *hash_next;   /* fileid hash chain */
	struct ds_prepare_job *queue_next;  /* per-DS FIFO chain */
};

/* Opaque context. */
struct ds_prepare_ctx;

/* Observability counters. */
struct ds_prepare_stats {
	uint64_t completed;
	uint64_t failed;
	uint64_t cancelled;
	uint64_t queue_depth;
};

/**
 * @brief Create and start the DS-prepare queue.
 *
 * Spawns one worker thread per DS mount.
 *
 * @param cat        Catalogue handle (for stripe_map + inode updates).
 * @param proxy      Proxy I/O context (for DS file creation).
 * @param ds_count   Number of data servers.
 * @param queue_depth Maximum queued jobs (0 = default 4096).
 * @param[out] out   Receives the context.
 * @return 0 on success, -errno on failure.
 */
int ds_prepare_create(const struct mds_catalogue *cat,
		      struct mds_proxy_ctx *proxy,
		      uint32_t ds_count,
		      uint32_t queue_depth,
		      struct ds_prepare_ctx **out);

/**
 * @brief Enqueue a DS-prepare job after CREATE commits.
 *
 * Fire-and-forget from CREATE's perspective.  Returns 0 on success
 * or -ENOSPC if the queue is full (caller should not block).
 *
 * @param ctx        Prepare context.
 * @param cat        Catalogue vtable that owns fileid.
 * @param fileid     Committed fileid.
 * @param generation Inode generation at commit time.
 * @param ds_id      Target DS from stripe_map.
 * @param stripe     Stripe index.
 * @param mirror     Mirror index.
 * @return 0 on success, -ENOSPC if full, -EINVAL on bad args.
 */
int ds_prepare_enqueue(struct ds_prepare_ctx *ctx,
		       struct mds_catalogue *cat,
		       uint64_t fileid, uint64_t generation,
		       uint32_t ds_id, uint32_t stripe, uint32_t mirror);

/**
 * @brief Look up a job by fileid (for LAYOUTGET fast-check).
 *
 * Returns the job state.  Does NOT block.
 *
 * @param ctx     Prepare context.
 * @param fileid  File to check.
 * @return DS_PREP_DONE if ready, DS_PREP_QUEUED/IN_PROGRESS if
 *         pending, DS_PREP_FAILED/CANCELLED on error, or -1 if
 *         no job exists for this fileid.
 */
int ds_prepare_check(struct ds_prepare_ctx *ctx, uint64_t fileid);

/**
 * @brief Get observability counters.
 */
void ds_prepare_get_stats(const struct ds_prepare_ctx *ctx,
			  struct ds_prepare_stats *stats);

/**
 * @brief Drain the queue, join workers, and free resources.
 */
void ds_prepare_destroy(struct ds_prepare_ctx *ctx);

#endif /* DS_PREPARE_H */
