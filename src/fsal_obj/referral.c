/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * referral.c -- Junction directory and fs_locations management.
 *
 * Subtree boundaries are marked with junction directories (sticky-bit).
 * When a client accesses a junction, we return NFS4ERR_MOVED +
 * fs_locations pointing to the owning MDS.
 *
 * referral_build() lives in subtree_map.c (it only needs the map).
 * This file contains the catalogue-backed junction helpers.
 */

#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <sys/stat.h>   /* S_ISVTX */

#include "pnfs_mds.h"
#include "referral.h"
#include "mds_catalogue.h"
#include "subtree_map.h"

/* -----------------------------------------------------------------------
 * Junction inode helpers
 *
 * A junction is a directory with the sticky bit (S_ISVTX, 01000)
 * set in its mode.  This is an in-band marker -- no extra catalogue DBI
 * is needed.
 * ----------------------------------------------------------------------- */

/**
 * @brief Check if a fileid is a junction directory.
 *
 * Reads the inode via catalogue and checks:
 *   1. type == MDS_FTYPE_DIR
 *   2. mode has S_ISVTX set
 *
 * @param cat     Catalogue handle.
 * @param fileid  Inode to check.
 * @return 1 if junction, 0 if not, -1 on error.
 */
int referral_is_junction(struct mds_catalogue *cat, uint64_t fileid)
{
    struct mds_inode inode;
    enum mds_status st;

    if (cat == NULL) {
        return 0;
    }
    st = mds_cat_ns_getattr(cat, fileid, &inode);
    if (st != MDS_OK) {
        return (st == MDS_ERR_NOTFOUND) ? 0 : -1;
    }
    if (inode.type != MDS_FTYPE_DIR) {
        return 0;
    }
    return (inode.mode & S_ISVTX) ? 1 : 0;
}

/**
 * @brief Check if current_fh is a junction; if so, return MDS_ERR_MOVED.
 *
 * Convenience helper for compound dispatch.  Returns MDS_OK if the
 * fileid is not a junction (normal processing continues).
 *
 * @param cat     Catalogue handle.
 * @param fileid  Current file handle fileid.
 * @return MDS_OK (not a junction), MDS_ERR_MOVED (is a junction),
 *         or MDS_ERR_IO on error.
 */
enum mds_status referral_check(struct mds_catalogue *cat, uint64_t fileid)
{
    int rc = referral_is_junction(cat, fileid);
    if (rc < 0) {
        return MDS_ERR_IO;
    }
    return rc ? MDS_ERR_MOVED : MDS_OK;
}

/**
 * @brief Create a junction directory via catalogue.
 *
 * Creates a directory with sticky bit (junction marker) using the
 * catalogue ns_create + ns_setattr API.  Works with any backend.
 *
 * @param cat             Catalogue handle.
 * @param parent_fileid   Parent directory.
 * @param name            Junction directory name.
 * @param target_mds_id   Target MDS ID (informational -- stored in
 *                        subtree map, not in the inode).
 * @return MDS_OK on success.
 */
enum mds_status referral_create_junction(struct mds_catalogue *cat,
                                         uint64_t parent_fileid,
                                         const char *name,
                                         uint32_t target_mds_id)
{
    (void)target_mds_id;
    enum mds_status st;
    struct mds_inode out;

    if (cat == NULL || name == NULL) {
        return MDS_ERR_INVAL;
    }

    /* Create a normal directory first. */
    st = mds_cat_ns_create(cat, NULL, parent_fileid, name,
                           MDS_FTYPE_DIR, 0777, 0, 0, NULL, &out);
    if (st != MDS_OK) {
        return st;
    }

    /* Set the sticky bit to mark it as a junction. */
    out.mode = 0777 | S_ISVTX;
    st = mds_cat_ns_setattr(cat, NULL, out.fileid, &out, MDS_ATTR_MODE);
    if (st != MDS_OK) {
        /* Best-effort: junction dir exists but unmarked.
         * A subsequent restart will retry. */
        return st;
    }
    return MDS_OK;
}

/**
 * @brief Encode fs_locations4 into a caller-provided buffer.
 *
 * Produces a minimal XDR-like encoding of the referral information.
 * This is a simplified encoding for the compound layer -- the full
 * XDR encoding is handled by xdr_codec.c when available.
 *
 * Format: [server_len u32][server bytes][rootpath_len u32][rootpath bytes]
 *
 * @param loc      Pre-built fs_location from referral_build().
 * @param xdr_out  Output buffer.
 * @param out_len  On entry: buffer capacity. On exit: bytes written.
 * @return MDS_OK on success, MDS_ERR_INVAL if buffer too small.
 */
enum mds_status referral_encode_fs_locations(
    const struct mds_fs_location *loc,
    void *xdr_out, size_t *out_len)
{
    if (loc == NULL || xdr_out == NULL || out_len == NULL) {
        return MDS_ERR_INVAL;
    }

    size_t slen = strlen(loc->server);
    size_t rlen = strlen(loc->rootpath);
    size_t needed = sizeof(uint32_t) + slen + sizeof(uint32_t) + rlen;

    if (*out_len < needed) {
        *out_len = needed;
        return MDS_ERR_INVAL;
    }

    uint8_t *p = xdr_out;
    uint32_t tmp;

    tmp = (uint32_t)slen;
    memcpy(p, &tmp, sizeof(tmp));
    p += sizeof(tmp);
    memcpy(p, loc->server, slen);
    p += slen;

    tmp = (uint32_t)rlen;
    memcpy(p, &tmp, sizeof(tmp));
    p += sizeof(tmp);
    memcpy(p, loc->rootpath, rlen);
    /* NOLINTNEXTLINE(clang-analyzer-deadcode.DeadStores) */
    p += rlen;

    *out_len = needed;
    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * Fileid-to-path resolution for junction referrals
 *
 * Walks the parent_fileid chain from @fileid to root, scanning dirents
 * at each level to recover the component name.  Produces an absolute
 * path like "/shard2" that can be passed to referral_build().
 *
 * Limitation: supports paths up to REFERRAL_PATH_MAX_DEPTH components
 * deep.  Junction directories are typically one level below root, so
 * this is generous.  Deeper paths are an ERROR, never a silently
 * truncated rootpath (clients would chase a wrong fs_locations).
 * ----------------------------------------------------------------------- */

/* 32 components x 256 bytes = 8 KiB of stack in
 * referral_resolve_path; safe on default thread stacks. */
#define REFERRAL_PATH_MAX_DEPTH 32

/**
 * Readdir callback: find the name of a child by fileid.
 */
struct find_name_arg {
	uint64_t target;
	char     name[256];
	bool     found;
};

static int find_name_cb(const struct mds_cat_dirent *entry, void *arg)
{
	struct find_name_arg *a = arg;
	if (entry->fileid == a->target) {
		(void)snprintf(a->name, sizeof(a->name), "%s", entry->name);
		a->found = true;
		return 1; /* stop */
	}
	return 0;
}

/**
 * @brief Resolve a fileid to its absolute path via parent_fileid ancestry.
 *
 * Walks from fileid up to MDS_FILEID_ROOT using mds_cat_ns_getattr
 * and mds_cat_ns_readdir to reconstruct the absolute path.
 *
 * @param cat      Catalogue handle.
 * @param fileid   Starting inode fileid.
 * @param path_buf Receives the null-terminated absolute path.
 * @param buf_len  Capacity of path_buf.
 * @return MDS_OK on success, MDS_ERR_INVAL if the ancestry is deeper
 *         than REFERRAL_PATH_MAX_DEPTH (no truncated path is ever
 *         returned as success).
 */
enum mds_status referral_resolve_path(struct mds_catalogue *cat,
				      uint64_t fileid,
				      char *path_buf, size_t buf_len)
{
	char components[REFERRAL_PATH_MAX_DEPTH][256];
	int depth = 0;
	uint64_t cur = fileid;

	if (cat == NULL || path_buf == NULL || buf_len == 0) {
		return MDS_ERR_INVAL;
	}

	while (cur != MDS_FILEID_ROOT && depth < REFERRAL_PATH_MAX_DEPTH) {
		struct mds_inode inode;
		enum mds_status st;

		st = mds_cat_ns_getattr(cat, cur, &inode);
		if (st != MDS_OK) {
			return MDS_ERR_NOTFOUND;
		}
		if (inode.parent_fileid == 0 ||
		    inode.parent_fileid == cur) {
			break;
		}

		struct find_name_arg fna = {
			.target = cur,
			.found = false,
		};
		st = mds_cat_ns_readdir(cat, inode.parent_fileid,
					NULL, 0, NULL,
					find_name_cb, &fna);
		if (st != MDS_OK || !fna.found) {
			return MDS_ERR_NOTFOUND;
		}

		(void)snprintf(components[depth], sizeof(components[depth]),
			       "%s", fna.name);
		depth++;
		cur = inode.parent_fileid;
	}

	/* Depth limit exhausted before reaching the root: returning the
	 * partial path as MDS_OK would send clients a wrong fs_locations
	 * rootpath.  Fail instead; callers already handle errors from
	 * this function (no referral is surfaced). */
	if (cur != MDS_FILEID_ROOT && depth >= REFERRAL_PATH_MAX_DEPTH) {
		return MDS_ERR_INVAL;
	}

	if (depth == 0) {
		(void)snprintf(path_buf, buf_len, "/");
		return MDS_OK;
	}

	size_t off = 0;
	for (int i = depth - 1; i >= 0; i--) {
		int n = snprintf(path_buf + off, buf_len - off,
				 "/%s", components[i]);
		if (n < 0 || (size_t)n >= buf_len - off) {
			return MDS_ERR_INVAL;
		}
		off += (size_t)n;
	}
	return MDS_OK;
}
