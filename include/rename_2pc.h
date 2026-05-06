/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * rename_2pc.h -- Cross-subtree rename two-phase commit API.
 */

#ifndef RENAME_2PC_H
#define RENAME_2PC_H

#include <stdint.h>
#include <stddef.h>

#include "pnfs_mds.h"

/* Forward declarations. */
struct mds_catalogue;
struct rename_2pc_transport;

/**
 * @brief Initiate a cross-subtree rename (coordinator side).
 *
 * @param cat             Catalogue handle.
 * @param transport       Transport callbacks for inter-MDS RPCs.
 * @param src_parent      Source parent fileid (local).
 * @param src_name        Source entry name.
 * @param dst_parent      Destination parent fileid (remote).
 * @param dst_name        Destination entry name.
 * @param remote_mds_id   Remote MDS node ID.
 * @return MDS_OK on success.
 */
enum mds_status rename_2pc_initiate(
    struct mds_catalogue *cat,
    const struct rename_2pc_transport *transport,
    uint64_t src_parent, const char *src_name,
    uint64_t dst_parent, const char *dst_name,
    uint32_t remote_mds_id);

/**
 * @brief Handle PrepareRename on the participant side.
 * @return 1 for VOTE_COMMIT, 0 for VOTE_ABORT.
 */
int rename_2pc_on_prepare(struct mds_catalogue *cat,
                          uint64_t txn_id,
                          uint64_t dst_parent,
                          const char *dst_name,
                          const void *inode_data,
                          size_t data_len,
                          uint32_t coordinator_mds_id);

/**
 * @brief Handle CommitRename on the participant side.
 *
 * Reads the inode snapshot from the PREPARE journal entry.
 * No inode_data parameter needed -- it is persisted in the journal.
 */
enum mds_status rename_2pc_on_commit(struct mds_catalogue *cat,
                                     uint64_t txn_id);

/**
 * @brief Handle AbortRename on the participant side.
 */
enum mds_status rename_2pc_on_abort(struct mds_catalogue *cat,
                                    uint64_t txn_id);

/**
 * @brief Recover incomplete 2PC transactions on startup.
 *
 * @param cat        Catalogue handle (for journal scan).
 * @param transport  Optional transport for commit retry (may be NULL).
 * @return MDS_OK if all entries resolved, MDS_ERR_DELAY if some remain.
 */
struct mds_shard_map;

/**
 * @param shard_map  Shard map for resolving local cross-shard recovery
 *                   (may be NULL for non-sharded deployments).
 */
enum mds_status rename_2pc_recover(struct mds_catalogue *cat,
                                   const struct rename_2pc_transport *transport,
                                   const struct mds_shard_map *shard_map);

struct mds_shard;

/**
 * @brief Cross-shard rename within the same MDS node (Phase 4).
 *
 * Moves a regular file from one local shard to another, preserving
 * its fileid.  Supports destination overwrite (POSIX semantics).
 * Directory cross-shard rename remains MDS_ERR_XDEV.
 *
 * @param root_cat        Root catalogue (for journal + fileid map).
 * @param src_shard       Source shard (saved_fh directory).
 * @param dst_shard       Destination shard (current_fh directory).
 * @param src_parent      Source parent fileid.
 * @param src_name        Source entry name.
 * @param dst_parent      Destination parent fileid.
 * @param dst_name        Destination entry name.
 * @return MDS_OK on success, MDS_ERR_XDEV if source is a directory.
 */
enum mds_status rename_2pc_initiate_local_shard(
    struct mds_catalogue *root_cat,
    const struct mds_shard *src_shard,
    const struct mds_shard *dst_shard,
    uint64_t src_parent, const char *src_name,
    uint64_t dst_parent, const char *dst_name);

#endif /* RENAME_2PC_H */
