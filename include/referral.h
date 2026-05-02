/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * referral.h — Junction directory and fs_locations referral API.
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

#endif /* REFERRAL_H */
