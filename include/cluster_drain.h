/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * cluster_drain.h -- Donor-local drain orchestrator (Seq 8).
 *
 * Evacuates all owned subtrees from the local node and transitions
 * to NODE_DRAINED.  Must be invoked on the node being drained.
 * Membership removal is a separate step performed from another node.
 */

#ifndef CLUSTER_DRAIN_H
#define CLUSTER_DRAIN_H

#include <stdint.h>
#include "pnfs_mds.h"

/* Forward declarations. */
struct cluster_membership;
struct subtree_map;
struct mds_catalogue;

/**
 * @brief Drain all subtrees from the local node.
 *
 * Transitions self from ACTIVE_SERVING -> DRAINING, migrates each
 * owned subtree to @p dest_mds_id, verifies zero remaining, and
 * transitions to NODE_DRAINED.
 *
 * Constraints:
 *   - Must be called on the donor node (self_id must match).
 *   - Rejects if self owns root subtree "/" (out of scope).
 *   - Non-transactional: completed migrations are not rolled back
 *     on partial failure.  On failure, lifecycle returns to
 *     ACTIVE_SERVING with potentially fewer subtrees.
 *
 * @param ctx         Membership handle.
 * @param map         Subtree map handle.
 * @param cat         Local catalogue handle.
 * @param dest_mds_id Destination MDS for all subtrees.
 * @return MDS_OK on full drain, MDS_ERR_PERM if preconditions fail,
 *         MDS_ERR_IO / other on migration failure (partial drain).
 */
enum mds_status cluster_drain_self(
    struct cluster_membership *ctx,
    struct subtree_map *map,
    struct mds_catalogue *cat,
    uint32_t dest_mds_id);

#endif /* CLUSTER_DRAIN_H */
