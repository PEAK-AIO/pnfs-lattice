/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * mover_util.h -- Shared file relocation helper for rebalance/tiering.
 *
 * Performs: revalidate stripe map -> fencing checks -> proxy copy ->
 * completeness verify -> atomic COMMIT_OP_REBALANCE_MOVE.
 */

#ifndef MOVER_UTIL_H
#define MOVER_UTIL_H

#include <stdint.h>
#include <stdatomic.h>

#include "pnfs_mds.h"

struct mds_catalogue;
struct commit_queue;
struct mds_proxy_ctx;
struct open_state_table;

/** Per-move skip/stat counters (caller-owned, updated atomically). */
struct mover_counters {
    _Atomic uint32_t skipped_active_layout;
    _Atomic uint32_t skipped_active_writer;
    _Atomic uint32_t skipped_sparse;
    _Atomic uint64_t bytes_copied;
    _Atomic uint32_t skipped_error;
};

/** Context passed to mover_relocate_file(). */
struct mover_ctx {
    struct mds_catalogue     *cat;       /**< Catalogue vtable. */
    struct commit_queue      *cq;
    struct mds_proxy_ctx     *proxy;
    struct open_state_table  *ot;
    _Atomic int              *running;   /**< Pointer to caller's stop flag. */
    struct mover_counters    *counters;  /**< Caller-owned stats. */
};

/**
 * Relocate a single file's stripe/mirror slot from one DS to another.
 *
 * Supports any stripe_count and mirror_count >= 1.
 *
 * @param ctx            Mover context.
 * @param fileid         File to move.
 * @param source_ds      Current DS holding the data.
 * @param source_mirror  Mirror index within the target stripe.
 * @param target_ds      Destination DS.
 * @param stripe_idx     Which stripe to relocate (0 for single-stripe).
 * @return  1 = moved successfully.
 *          0 = skipped (stale, fenced, target already present, etc.).
 *         -1 = hard error.
 */
int mover_relocate_file(const struct mover_ctx *ctx,
                        uint64_t fileid,
                        uint32_t source_ds,
                        uint32_t source_mirror,
                        uint32_t target_ds,
                        uint32_t stripe_idx);

#endif /* MOVER_UTIL_H */
