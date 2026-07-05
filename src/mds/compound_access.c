/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 */
/*
 * compound_access.c -- POSIX discretionary access control (DAC) for
 * NFSv4.1 compound operations.
 *
 * The MDS receives the caller's identity from the RPC credential
 * (AUTH_SYS: uid, gid, supplementary gids -- rpc_server.c copies them
 * into struct compound_data).  These helpers evaluate classic POSIX
 * mode-bit semantics against that identity so mutation handlers can
 * enforce:
 *
 *   - owner/group/other r/w/x bits on files and directories,
 *   - write+search on a directory for namespace mutations
 *     (CREATE / REMOVE / RENAME / LINK / OPEN(CREATE)),
 *   - the S_ISVTX (sticky) restricted-deletion rule,
 *   - the SETATTR permission matrix (chmod / chown / truncate /
 *     utimes), and
 *   - SUID/SGID clearing on chown, truncate, and write.
 *
 * Enforcement is gated on two conditions checked by
 * compound_dac_active():
 *
 *   1. cd->cfg_posix_dac -- operator knob (`posix_dac` in mds.conf,
 *      default true).  Disabling restores the historical permissive
 *      behaviour where any principal could mutate any object.
 *   2. cd->auth_flavor == AUTH_SYS -- other flavors (AUTH_NONE, GSS)
 *      carry no usable uid/gid mapping at this layer, so they keep
 *      the legacy behaviour rather than mis-enforcing against a
 *      zeroed identity.
 *
 * uid 0 (root) bypasses permission checks, matching local-filesystem
 * semantics (no root-squash support at this layer).  SUID/SGID
 * clearing on chown intentionally applies to root as well (Linux
 * clears the bits on chown regardless of privilege).
 *
 * Error mapping follows POSIX errno conventions used by the Linux
 * VFS: permission-bit failures return NFS4ERR_ACCESS (EACCES);
 * ownership-rule failures (chmod/chown/utimes by non-owner, sticky
 * deletes) return NFS4ERR_PERM (EPERM).
 */

#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <sys/stat.h>

#include "pnfs_mds.h"
#include "compound.h"
#include "compound_internal.h"

/* RPC credential flavor for AUTH_SYS (RFC 5531 section 8.2). */
#define COMPOUND_AUTH_SYS 1u

bool compound_dac_active(const struct compound_data *cd)
{
	if (cd == NULL) {
		return false;
	}
	return cd->cfg_posix_dac && cd->auth_flavor == COMPOUND_AUTH_SYS;
}

bool compound_cred_in_group(const struct compound_data *cd, uint32_t gid)
{
	uint32_t i;

	if (cd->cred_gid == gid) {
		return true;
	}
	for (i = 0; i < cd->aux_gid_count && i < 16; i++) {
		if (cd->aux_gids[i] == gid) {
			return true;
		}
	}
	return false;
}

/**
 * Evaluate POSIX mode bits for the caller against @a inode.
 *
 * @param may  Bitwise OR of COMPOUND_MAY_READ / _WRITE / _EXEC.
 * @return NFS4_OK when every requested permission is granted (or DAC
 *         is inactive / caller is root); NFS4ERR_ACCESS otherwise.
 */
enum nfs4_status compound_access_mode_check(const struct compound_data *cd,
					    const struct mds_inode *inode,
					    uint32_t may)
{
	uint32_t perm;

	if (!compound_dac_active(cd) || cd->cred_uid == 0) {
		return NFS4_OK;
	}
	if (cd->cred_uid == (uint32_t)inode->uid) {
		perm = (inode->mode >> 6) & 7u;
	} else if (compound_cred_in_group(cd, (uint32_t)inode->gid)) {
		perm = (inode->mode >> 3) & 7u;
	} else {
		perm = inode->mode & 7u;
	}
	if ((perm & may) != may) {
		return NFS4ERR_ACCESS;
	}
	return NFS4_OK;
}

enum nfs4_status compound_dir_mutate_check(const struct compound_data *cd,
					   const struct mds_inode *dir)
{
	return compound_access_mode_check(cd, dir,
					  COMPOUND_MAY_WRITE |
					  COMPOUND_MAY_EXEC);
}

enum nfs4_status compound_dir_search_check(const struct compound_data *cd,
					   const struct mds_inode *dir)
{
	return compound_access_mode_check(cd, dir, COMPOUND_MAY_EXEC);
}

/**
 * S_ISVTX restricted-deletion rule (POSIX unlink/rmdir/rename).
 *
 * When the containing directory carries the sticky bit, an entry may
 * only be removed (or renamed away / overwritten) by the entry's
 * owner, the directory's owner, or root.  Write permission on the
 * directory is checked separately by compound_dir_mutate_check().
 *
 * @return NFS4_OK or NFS4ERR_PERM.
 */
enum nfs4_status compound_sticky_delete_check(const struct compound_data *cd,
					      const struct mds_inode *dir,
					      const struct mds_inode *victim)
{
	if (!compound_dac_active(cd) || cd->cred_uid == 0) {
		return NFS4_OK;
	}
	if ((dir->mode & S_ISVTX) == 0) {
		return NFS4_OK;
	}
	if (cd->cred_uid == (uint32_t)victim->uid ||
	    cd->cred_uid == (uint32_t)dir->uid) {
		return NFS4_OK;
	}
	return NFS4ERR_PERM;
}

/**
 * Compute the post-write / post-truncate mode for @a inode.
 *
 * Linux clears S_ISUID unconditionally and S_ISGID only when the
 * group-execute bit is set (S_ISGID without S_IXGRP is mandatory
 * locking, not a privilege bit) whenever a regular file's contents
 * change and the writer lacks CAP_FSETID.  We approximate the
 * capability check with "caller is not root".
 *
 * @param mode_out  Receives the cleared mode when clearing applies.
 * @return true when the caller must persist @a mode_out with
 *         MDS_ATTR_MODE; false when no clearing is needed.
 */
bool compound_write_clears_setid(const struct compound_data *cd,
				 const struct mds_inode *inode,
				 uint32_t *mode_out)
{
	uint32_t mode;

	if (!compound_dac_active(cd) || cd->cred_uid == 0) {
		return false;
	}
	if (inode->type != MDS_FTYPE_REG) {
		return false;
	}
	mode = inode->mode;
	if ((mode & (S_ISUID | S_ISGID)) == 0) {
		return false;
	}
	mode &= ~(uint32_t)S_ISUID;
	if (mode & S_IXGRP) {
		mode &= ~(uint32_t)S_ISGID;
	}
	if (mode == inode->mode) {
		return false;
	}
	if (mode_out != NULL) {
		*mode_out = mode;
	}
	return true;
}

/**
 * SETATTR permission matrix + SUID/SGID adjustments.
 *
 * Validates the requested attribute changes in @a arg against POSIX
 * ownership rules using the pre-mutation inode @a pre, and computes
 * any implicit mode adjustments (SGID drop on non-member chmod,
 * SUID/SGID clearing on chown and truncate).
 *
 * On NFS4_OK the caller must persist @a *attrs_inout with
 * @a *mask_inout: this function may rewrite attrs_inout->mode and OR
 * MDS_ATTR_MODE into the mask.  On error nothing was modified.
 *
 * Rules enforced (caller uid != 0; root skips the permission gates
 * but still receives the chown SUID/SGID clearing adjustment):
 *   - MODE: caller must own the file (else PERM).  A caller outside
 *     the file's group silently loses S_ISGID from the requested mode.
 *   - UID: changing uid requires root (PERM).  A no-op uid set is
 *     allowed only for the owner.
 *   - GID: caller must own the file and belong to the target group
 *     (else PERM).  A no-op gid set by the owner is allowed.
 *   - SIZE: without an open stateid, requires write permission
 *     (ACCESS).  A real size change on a regular file clears
 *     SUID/SGID for non-root callers.
 *   - ATIME/MTIME (explicit times): owner only (else PERM).
 *   - ATIME_NOW/MTIME_NOW: owner, or write permission (else ACCESS).
 *   - chown/chgrp on a regular file clears S_ISUID and, when S_IXGRP
 *     is set, S_ISGID -- for every caller including root.
 */
/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
enum nfs4_status compound_setattr_dac_check(const struct compound_data *cd,
					    const struct mds_inode *pre,
					    const struct nfs4_arg_setattr *arg,
					    struct mds_inode *attrs_inout,
					    uint32_t *mask_inout)
{
	uint32_t mask = *mask_inout;
	bool is_owner;
	bool is_root;

	if (!compound_dac_active(cd)) {
		return NFS4_OK;
	}

	is_root = (cd->cred_uid == 0);
	is_owner = (cd->cred_uid == (uint32_t)pre->uid);

	if (!is_root) {
		if (mask & MDS_ATTR_MODE) {
			if (!is_owner) {
				return NFS4ERR_PERM;
			}
		}
		if (mask & MDS_ATTR_UID) {
			if (arg->attrs.uid != pre->uid) {
				return NFS4ERR_PERM;
			}
			if (!is_owner) {
				return NFS4ERR_PERM;
			}
		}
		if (mask & MDS_ATTR_GID) {
			if (!is_owner) {
				return NFS4ERR_PERM;
			}
			if (arg->attrs.gid != pre->gid &&
			    !compound_cred_in_group(
				    cd, (uint32_t)arg->attrs.gid)) {
				return NFS4ERR_PERM;
			}
		}
		if (mask & MDS_ATTR_SIZE) {
			if (!arg->has_stateid) {
				enum nfs4_status nst =
					compound_access_mode_check(
						cd, pre, COMPOUND_MAY_WRITE);
				if (nst != NFS4_OK) {
					return nst;
				}
			}
		}
		if (mask & (MDS_ATTR_ATIME | MDS_ATTR_MTIME)) {
			/* Explicit timestamps: owner only. */
			if (!is_owner) {
				return NFS4ERR_PERM;
			}
		}
		if (mask & (MDS_ATTR_ATIME_NOW | MDS_ATTR_MTIME_NOW)) {
			/* touch-to-now: owner or write permission. */
			if (!is_owner) {
				enum nfs4_status nst =
					compound_access_mode_check(
						cd, pre, COMPOUND_MAY_WRITE);
				if (nst != NFS4_OK) {
					return nst;
				}
			}
		}
	}

	/* ---- Implicit mode adjustments (post-gate) ---- */

	/*
	 * chmod by a non-root caller outside the file's (final) group
	 * silently drops S_ISGID from the requested mode (POSIX: "if
	 * the calling process does not have appropriate privileges and
	 * the group ID of the file does not match the effective group
	 * ID or one of the supplementary group IDs, S_ISGID is
	 * cleared").
	 */
	if ((mask & MDS_ATTR_MODE) && !is_root) {
		uint64_t final_gid = (mask & MDS_ATTR_GID)
				     ? arg->attrs.gid : pre->gid;
		if (!compound_cred_in_group(cd, (uint32_t)final_gid)) {
			attrs_inout->mode &= ~(uint32_t)S_ISGID;
		}
	}

	/*
	 * chown/chgrp kills SUID/SGID on every file type except
	 * directories.  Linux sets ATTR_KILL_SUID/SGID on EVERY
	 * chown-family syscall (fs/open.c chown_common) -- including
	 * no-op id values and root callers -- and notify_change kills
	 * the bits for any non-directory inode (pjdfstest chown/00
	 * verifies regular files, fifos, sockets, and device nodes;
	 * directories are the documented Linux exception).  NFS
	 * clients only carry the UID/GID attrs in SETATTRs generated
	 * by chown syscalls, so keying on the mask matches the
	 * syscall boundary.  When the same SETATTR also carries MODE,
	 * the clearing applies to the requested mode; otherwise we
	 * fold a MODE update derived from the pre-mutation mode into
	 * the same catalogue write.
	 */
	if ((mask & (MDS_ATTR_UID | MDS_ATTR_GID)) != 0 &&
	    pre->type != MDS_FTYPE_DIR) {
		uint32_t base = (mask & MDS_ATTR_MODE)
				? attrs_inout->mode : pre->mode;
		uint32_t cleared = base;

		cleared &= ~(uint32_t)S_ISUID;
		if (cleared & S_IXGRP) {
			cleared &= ~(uint32_t)S_ISGID;
		}
		if (cleared != base) {
			attrs_inout->mode = cleared;
			mask |= MDS_ATTR_MODE;
		}
	}

	/*
	 * Truncate (real size change) by a non-root caller clears
	 * SUID/SGID on regular files, mirroring Linux's
	 * should_remove_suid() on do_truncate.
	 */
	if ((mask & MDS_ATTR_SIZE) && !is_root &&
	    arg->attrs.size != pre->size &&
	    pre->type == MDS_FTYPE_REG &&
	    (mask & MDS_ATTR_MODE) == 0) {
		uint32_t cleared_mode = 0;

		if (compound_write_clears_setid(cd, pre, &cleared_mode)) {
			attrs_inout->mode = cleared_mode;
			mask |= MDS_ATTR_MODE;
		}
	}

	*mask_inout = mask;
	return NFS4_OK;
}
