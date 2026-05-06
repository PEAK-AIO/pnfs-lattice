/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * hardlink_2pc.h -- Cross-subtree hard link two-phase commit API.
 *
 * Modeled on rename_2pc: coordinator journal on directory-owning MDS,
 * participant journal on inode-owning MDS. Crash-safe and idempotent.
 */

#ifndef HARDLINK_2PC_H
#define HARDLINK_2PC_H

#include <stdint.h>
#include <stddef.h>

#include "pnfs_mds.h"

struct mds_catalogue;
struct cluster_membership;

/**
 * Initiate a cross-subtree hard link (coordinator / directory-owning MDS).
 *
 * 1. Allocates txn_id and anchor_id.
 * 2. Writes PREPARED journal entry.
 * 3. Sends PrepareLink to inode-owning MDS.
 * 4. On VOTE_COMMIT: creates external dirent, writes COMMITTED.
 * 5. Sends CommitLink to inode-owning MDS.
 */
enum mds_status hardlink_2pc_initiate(
    struct mds_catalogue *cat,
    const struct cluster_membership *membership,
    uint64_t dir_parent, const char *name,
    uint64_t target_fileid, uint8_t target_type,
    uint32_t target_owner_mds_id,
    uint32_t self_mds_id);

/**
 * Handle PrepareLink on the participant (inode-owning MDS).
 *
 * Validates target, reserves nlink+1, writes anchor + journal.
 * @return 1 for VOTE_COMMIT, 0 for VOTE_ABORT.
 */
int hardlink_2pc_on_prepare(struct mds_catalogue *cat,
                            uint64_t txn_id,
                            uint64_t target_fileid,
                            uint64_t anchor_id,
                            uint32_t remote_mds_id,
                            uint64_t remote_parent,
                            const char *remote_name);

/** Handle CommitLink on the participant. Finalizes anchor + nlink. */
enum mds_status hardlink_2pc_on_commit(struct mds_catalogue *cat,
                                       uint64_t txn_id);

/** Handle AbortLink on the participant. Rolls back nlink + anchor. */
enum mds_status hardlink_2pc_on_abort(struct mds_catalogue *cat,
                                      uint64_t txn_id);

/**
 * Recover incomplete hardlink 2PC transactions on startup.
 *
 * Scans the hardlink journal and resolves PREPARED entries.
 */
enum mds_status hardlink_2pc_recover(struct mds_catalogue *cat,
                                     const struct cluster_membership *membership);

#endif /* HARDLINK_2PC_H */
