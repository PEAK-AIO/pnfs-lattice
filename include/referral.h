/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * referral.h -- Junction directory and fs_locations referral API.
 */

#ifndef REFERRAL_H
#define REFERRAL_H

#include <stdint.h>
#include <stddef.h>

#include "pnfs_mds.h"

/* Forward declarations. */
struct mds_catalogue;
struct mds_fs_location;

/**
 * @brief Check if a fileid is a junction directory.
 * @param cat     Catalogue handle.
 * @param fileid  Inode to check.
 * @return 1 if junction, 0 if not, -1 on error.
 */
int referral_is_junction(struct mds_catalogue *cat, uint64_t fileid);

/**
 * @brief Convenience check for compound dispatch.
 * @return MDS_OK, MDS_ERR_MOVED, or MDS_ERR_IO.
 */
enum mds_status referral_check(struct mds_catalogue *cat, uint64_t fileid);

/**
 * @brief Create a junction directory.
 * @param cat             Catalogue handle.
 * @param parent_fileid   Parent directory.
 * @param name            Junction directory name.
 * @param target_mds_id   Target MDS ID (informational).
 * @return MDS_OK on success.
 */
enum mds_status referral_create_junction(struct mds_catalogue *cat,
                                         uint64_t parent_fileid,
                                         const char *name,
                                         uint32_t target_mds_id);

/**
 * @brief Create or repair a junction directory.
 *
 * Like referral_create_junction(), but if a plain directory already
 * exists at @name (no sticky bit), upgrades it to a junction instead
 * of failing with MDS_ERR_EXISTS.
 */
enum mds_status referral_ensure_junction(struct mds_catalogue *cat,
                                         uint64_t parent_fileid,
                                         const char *name,
                                         uint32_t target_mds_id);

/**
 * @brief Resolve a fileid to its absolute path via parent_fileid ancestry.
 * @param cat      Catalogue handle.
 * @param fileid   Starting inode fileid.
 * @param path_buf Receives the null-terminated absolute path.
 * @param buf_len  Capacity of path_buf.
 * @return MDS_OK on success.
 */
enum mds_status referral_resolve_path(struct mds_catalogue *cat,
				      uint64_t fileid,
				      char *path_buf, size_t buf_len);

enum mds_status referral_encode_fs_locations(
    const struct mds_fs_location *loc,
    void *xdr_out, size_t *out_len);

/**
 * @brief FSID major for a shard/partition owned by @owner_mds_id.
 *
 * Each MDS shard exports a distinct filesystem.  Junction referrals
 * advertise the target shard's FSID so clients treat cross-shard
 * access as a separate superblock.
 */
static inline uint64_t referral_fsid_major(uint32_t owner_mds_id)
{
	return (owner_mds_id == 0) ? 1 : (uint64_t)owner_mds_id;
}

static inline uint64_t referral_fsid_minor(uint32_t owner_mds_id)
{
	(void)owner_mds_id;
	return 0;
}

#endif /* REFERRAL_H */
